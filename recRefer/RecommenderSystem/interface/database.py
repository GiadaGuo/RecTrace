from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# ===== 数据库连接 URL =====
# 在这里替换为你自己的用户名、密码、数据库服务器IP
BOOK_INTERACTION_DATABASE_URL = "mysql+aiomysql://root:123321@127.0.0.1/kl_trade"
USER_DATABASE_URL = "mysql+aiomysql://root:123321@127.0.0.1/kl_user"

# ===== 创建异步引擎 =====
# 为三个数据库分别创建引擎
book_and_interaction_db_engine = create_async_engine(BOOK_INTERACTION_DATABASE_URL,
                                                     pool_size = 10,  # 连接池大小
                                                     max_overflow=20,  # 最大溢出连接数
                                                     pool_recycle=3600,  # 连接回收时间（秒）
                                                     pool_pre_ping=True,  # 连接预检测
                                                     pool_timeout=60,  # 获取连接的超时时间
                                                     echo=True)
user_db_engine = create_async_engine(USER_DATABASE_URL,
                                     pool_size=10,  # 连接池大小
                                     max_overflow=20,  # 最大溢出连接数
                                     pool_recycle=3600,  # 连接回收时间（秒）
                                     pool_pre_ping=True,  # 连接预检测
                                     pool_timeout=60,  # 获取连接的超时时间
                                     echo=True)

# 创建对应的会话工厂
BookAndInteractionDBSession = sessionmaker(bind=book_and_interaction_db_engine, class_=AsyncSession,autocommit=False, autoflush=False, expire_on_commit=False)
UserDBSession = sessionmaker(bind=user_db_engine, class_=AsyncSession,autocommit=False, autoflush=False, expire_on_commit=False)

# 得到数据库Session对象
async def get_book_and_interaction_db():
    async with BookAndInteractionDBSession() as session:
        yield session

async def get_user_db():
    async with UserDBSession() as session:
        yield session