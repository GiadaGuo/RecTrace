import random
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import logging
import uuid
import torch
from entities import Item
from interface.database import BookAndInteractionDBSession, UserDBSession
from interface.recommender_system import RecommenderSystem  # 推荐系统类
from interface.db_utils import *
from contextlib import asynccontextmanager
from utils import getItems

# ===== 全局推荐器 ======
# 应用启动时创建一个实例即可
recsys_model: RecommenderSystem = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """APP生命周期函数"""
    """应用启动时自动调用，初始化推荐系统类"""
    global recsys_model
    logging.info("Initializing RecSys model...")
    try:
        # 创建独立的会话（不使用依赖注入）
        async with BookAndInteractionDBSession() as book_and_interaction_session:
            async with UserDBSession() as user_session:
                # 从数据库读取数据
                df_items, df_interactions = await prepare_book_and_interaction_data(book_and_interaction_session)
                df_users = await prepare_user_data(user_session)
                print(f"读取到{len(df_items)}条物品数据，{len(df_users)}条用户数据,{len(df_interactions)}条交互记录")
                logging.info("Data loaded successfully from database.")
        # 初始化推荐系统
        recsys_model = RecommenderSystem(df_items, df_users, df_interactions)
        # 加载预训练好的权重进行推荐
        # 各模型权重路径
        twin_towers_model_weights_path = "model_weights/twin_towers_model.pth"
        light_gcn_weights_path = "model_weights/lightgcn.pth"
        three_towers_model_weights_path = "model_weights/three_towers_model.pth"
        multi_task_model_path = "model_weights/multi_task_model.pth"
        recsys_model.fit_with_weights(twin_towers_model_weights_path, light_gcn_weights_path,
                                      three_towers_model_weights_path, multi_task_model_path)
        logging.info("RecSys model initialized successfully.")
    except Exception as e:
        logging.error(f"Failed to initialize RecSys model: {e}")
        raise RuntimeError(f"Failed to start service due to model initialization error: {e}")
    # yield 控制权交给应用
    yield
    """应用关闭时自动释放空间"""
    logging.info("Shutting down RecSys service...")
    if recsys_model:
        del recsys_model
        torch.cuda.empty_cache()
    logging.info("Shutdown complete")

app = FastAPI(
    title="Recommendation Service with Database",
    description="A microservice for providing recommendations using the RecSys model.",
    version="3.0",
    lifespan=lifespan
)

# ===== 交互类 =====
class RecommendationRequest(BaseModel):
    user_id: str
    hour: int
    is_weekend: bool
    is_holiday: bool

class RecommendationResponse(BaseModel):
    status: str
    user_id: str
    recommendations: List[str]
    request_id: str

class FineTuningRequest(BaseModel):
    # 传入的是类似"2026-04-09 16:54:13"这样的字符串
    start_time:str
    end_time:str

class Response(BaseModel):
    status: str
    message: str


# ========= 核心推荐函数 ==========
async def get_recommendations_from_model(user_id: int, hour: int, is_weekend:bool, is_holiday:bool) -> List[int]:
    """
    调用已实例化的推荐器进行推荐
    Args:
        user_id: 用户ID
        hour: 当前时间(小时)
        is_weekend: 当前是否是周末
        is_holiday: 当前是否是节假日
    """
    global recsys_model
    if not recsys_model:
        raise RuntimeError("RecSys model is not initialized")

    logging.info(f"Generating recommendations for user '{user_id}' using the real model...")

    predicted_item_ids = recsys_model.recommend(user_id,hour,is_weekend,is_holiday)
    logging.info(f"Successfully generated {len(predicted_item_ids)} recommendations for user '{user_id}'.")
    return predicted_item_ids

# ========= 每日微调函数 ==========
async def models_fine_tuning(start_time,end_time)->bool:
    """
    Args:
        start_time:起始时间戳
        end_time:终止时间戳
    """
    global recsys_model
    if not recsys_model:
        raise RuntimeError("RecSys model is not initialized")
    logging.info("Model fine-tuning started...")

    try:
        async with BookAndInteractionDBSession() as book_and_interaction_session:
            df_items, df_interactions = await get_book_and_interaction_data_by_time(book_and_interaction_session,
                                                                                    start_time, end_time)
        async with UserDBSession() as user_session:
            df_users = await get_user_data_by_time(user_session, start_time, end_time)

        if df_items.empty:
            logging.info("微调时查询到的新物品列表为空")
        if df_users.empty:
            logging.info("微调时查询到的新用户列表为空")
        if df_interactions.empty:
            logging.info("微调时查询到的新交互记录为空")

        logging.info("loading new data from database finished")
    except Exception as e:
        logging.error(f"Error during loading new data from database: {str(e)}")
        return False

    try:
        items = await getItems(df_items)  # 当日新增的物品对象列表
        recsys_model.fine_tuning(df_items, df_users, df_interactions, items)
        logging.info("Model fine-tuning finished")
        return True
    except Exception as e:
        logging.error(f"Error during model fine-tuning: {str(e)}")
        return False
    # items = await getItems(df_items)  # 当日新增的物品对象列表
    # recsys_model.fine_tuning(df_items, df_users, df_interactions, items)
    # logging.info("Model fine-tuning finished")
    # return True


# =========== 接口路由 ===========
# 推荐接口
@app.post("/recommend",
          responses={400: {"model": Response}, 404: {"model": Response},500: {"model": Response}},
          summary="Get Recommendations for a User")
async def recommend(request: RecommendationRequest):
    user_id = request.user_id
    hour = request.hour
    is_weekend = request.is_weekend
    is_holiday = request.is_holiday

    if not (user_id and hour is not None):
        logging.error("recommend failed : Missing request parameters ")
        return Response(
            status = "error",
            message = "Missing request parameters"
        )

    try:
        # recommendations是推荐的物品ID列表，顺序是有意义的
        recommendations = await get_recommendations_from_model(
            user_id=user_id,
            hour=hour,
            is_weekend=is_weekend,
            is_holiday=is_holiday
        )
        return RecommendationResponse(
            status="success",
            user_id=user_id,
            recommendations=recommendations,
            request_id=str(uuid.uuid4())
        )
    except KeyError as e:
        logging.warning(f"User '{user_id}' not found in model data. Returning empty list or handling cold start.")
        return Response(status="error", message=f"User '{user_id}' not found in model data")

    except Exception as e:
        logging.error(f"Error during prediction for user '{user_id}': {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error during recommendation prediction.")


@app.post("/fine_tuning",
              response_model=Response,
          responses={400: {"model": Response}, 404: {"model": Response},500: {"model": Response}},
          summary="daily fine-tuning")
async def fine_tuning(request: FineTuningRequest):
    # 获取新数据的起止时间
    start_time = request.start_time
    end_time = request.end_time

    if not (start_time and end_time):
        logging.error("fine_tuning failed : Missing request parameters ")
        return Response(
            status = "error",
            message = "Missing request parameters"
        )

    status=await models_fine_tuning(start_time, end_time)
    if status:
        return Response(
            status="success",
            message="Daily fine-tuning success",
        )
    else:
        return Response(
            status="error",
            message=f"Daily fine-tuning failed",
        )

# 健康检查接口
@app.get("/health", summary="Health Check")
async def health_check():
    global recsys_model
    if not recsys_model:
        return {"status": "unhealthy", "reason": "Model not loaded",
                "timestamp": __import__('datetime').datetime.utcnow().isoformat()}
    return {"status": "healthy", "timestamp": __import__('datetime').datetime.utcnow().isoformat()}

if __name__ == "__main__":
    import uvicorn
    # 在这里替换为你自己的IP地址和端口号
    uvicorn.run(app, host="0.0.0.0", port=6006)