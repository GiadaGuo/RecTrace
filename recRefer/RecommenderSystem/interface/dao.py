from sqlalchemy.ext.asyncio import AsyncSession
import interface.database_models as models
from sqlalchemy import select, between, and_
from datetime import datetime
from typing import List

async def get_all_user_profiles(user_db_session: AsyncSession) -> list[models.UserProfileBehavior]:
    """获取所有用户信息"""
    stmt = select(models.UserProfileBehavior) # 类定义中关联的有具体数据表
    result = await user_db_session.execute(stmt)
    return result.scalars().all()

async def get_all_book_infos(book_and_interaction_db_session: AsyncSession) -> list[models.BookInfo]:
    """获取所有书籍信息"""
    stmt = select(models.BookInfo)
    result = await book_and_interaction_db_session.execute(stmt)
    return result.scalars().all()

async def get_all_item_interactions(book_and_interaction_db_session: AsyncSession) -> list[models.ItemInteraction]:
    """获取所有书籍统计信息"""
    stmt = select(models.ItemInteraction)
    result = await book_and_interaction_db_session.execute(stmt)
    return result.scalars().all()

async def get_all_user_item_interactions(book_and_interaction_db_session: AsyncSession) -> list[models.UserItemInteraction]:
    """获取所有用户-书籍交互记录"""
    stmt = select(models.UserItemInteraction)
    result = await book_and_interaction_db_session.execute(stmt)
    return result.scalars().all()


async def get_user_profiles_by_time_range(
        user_db_session: AsyncSession,
        start_time,
        end_time
) -> List[models.UserProfileBehavior]:
    """
    根据时间范围获取用户信息
    Args:
        user_db_session: 数据库会话
        start_time: 开始时间
        end_time: 结束时间
    Returns:
        UserProfileBehavior对象列表
    """
    stmt = select(models.UserProfileBehavior).where(
        between(
            models.UserProfileBehavior.create_time,
            start_time,
            end_time
        )
    )

    result = await user_db_session.execute(stmt)
    return result.scalars().all()


async def get_item_data_by_time_range(
        db_session: AsyncSession,
        start_time,
        end_time
) -> List[dict]:
    """
    获取物品信息及其互动数据（联查book_info和item_interaction表）

    Args:
        db_session: 数据库会话
        start_time: 开始时间
        end_time: 结束时间

    Returns:
        包含物品信息和互动数据的字典列表
    """
    # 使用SQLAlchemy ORM进行联查
    stmt = select(
        # book_info表字段
        models.BookInfo.item_id,
        models.BookInfo.name,
        models.BookInfo.author,
        models.BookInfo.city,
        models.BookInfo.item_categories,
        models.BookInfo.item_keywords,
        models.BookInfo.price,
        models.BookInfo.image,
        models.BookInfo.description,
        models.BookInfo.create_time,
        # item_interaction表字段
        models.ItemInteraction.item_click_last3m,
        models.ItemInteraction.item_cart_last3m,
        models.ItemInteraction.item_buy_last3m,
        models.ItemInteraction.item_forward_last3m
    ).select_from(
        models.BookInfo.__table__.join(
            models.ItemInteraction.__table__,
            models.BookInfo.item_id == models.ItemInteraction.item_id
        )
    ).where(
        and_(
            between(models.BookInfo.create_time, start_time, end_time),
            models.BookInfo.item_id == models.ItemInteraction.item_id
        )
    )

    result = await db_session.execute(stmt)
    rows = result.fetchall()

    # 将结果转换为字典列表
    items_with_interactions = [
        {
            'item_id': row.item_id,
            'name': row.name,
            'author': row.author,
            'city': row.city,
            'item_categories': row.item_categories,
            'item_keywords': tuple(row.item_keywords.split(";")),
            'price': row.price,
            'image': row.image,
            'description': row.description,
            'create_time':row.create_time,
            'item_click_last3m': row.item_click_last3m,
            'item_cart_last3m': row.item_cart_last3m,
            'item_buy_last3m': row.item_buy_last3m,
            'item_forward_last3m': row.item_forward_last3m
        }
        for row in rows
    ]

    return items_with_interactions

async def get_interactions_by_time_range(
        db_session: AsyncSession,
        start_time,
        end_time
) -> List[models.UserItemInteraction]:
    """
    根据时间范围获取交互记录
    Args:
        db_session: 数据库会话
        start_time: 开始时间
        end_time: 结束时间
    Returns:
        UserItemInteraction对象列表
    """
    stmt = select(models.UserItemInteraction).where(
        between(
            models.UserItemInteraction.create_time,
            start_time,
            end_time
        )
    )

    result = await db_session.execute(stmt)
    return result.scalars().all()