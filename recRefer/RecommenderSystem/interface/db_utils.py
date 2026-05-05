import pandas as pd
import interface.dao as dao

async def prepare_book_and_interaction_data(db_session):
    """
    读取数据库数据，并将其转换为 Dataframe 对象
    Args:
        db_session:激活的数据库 Session 实例对象
    """
    print("book data and interactions data preparing from database...")

    db_books_raw = await dao.get_all_book_infos(db_session)

    db_item_interactions_raw = await dao.get_all_item_interactions(db_session)

    db_interactions_raw = await dao.get_all_user_item_interactions(db_session)

    books_data = []
    for book_obj in db_books_raw:
        keywords_str = book_obj.item_keywords if book_obj.item_keywords is not None else ""
        categories_str = book_obj.item_categories if book_obj.item_categories is not None else ""

        books_data.append({
            'item_id': book_obj.item_id,
            'name': book_obj.name,
            'author': book_obj.author,
            'city': book_obj.city,
            'item_categories': categories_str,
            'item_keywords': keywords_str,
            'create_time': book_obj.create_time,
            'price': book_obj.price,
            'image': book_obj.image,
            'description': book_obj.description,
        })
    df_books = pd.DataFrame(books_data)

    items_interactions_data = []
    for item_obj in db_item_interactions_raw:
        items_interactions_data.append({
            'item_id': item_obj.item_id,
            'item_click_last3m': item_obj.item_click_last3m,
            'item_cart_last3m': item_obj.item_cart_last3m,
            'item_forward_last3m': item_obj.item_forward_last3m,
            'item_buy_last3m': item_obj.item_buy_last3m,
        })
    df_item_interactions = pd.DataFrame(items_interactions_data)
    # 拼接成推荐系统需要的df_items
    df_items=pd.merge(df_books, df_item_interactions, on='item_id', how='left')

    interactions_data = []
    for interaction_obj in db_interactions_raw:
        interactions_data.append({
            'user_id': interaction_obj.user_id,
            'item_id': interaction_obj.item_id,
            'rating': interaction_obj.rating,
            'hour': interaction_obj.hour,
            'is_weekend': interaction_obj.is_weekend,
            'is_holiday': interaction_obj.is_holiday,
            'click': interaction_obj.click,
            'cart': interaction_obj.cart,
            'forward': interaction_obj.forward,
            'buy': interaction_obj.buy
        })
    df_interactions = pd.DataFrame(interactions_data)

    print("book data and interactions data finished\n")
    return df_items,df_interactions

async def prepare_user_data(db_session):
    """
    读取数据库数据，并将其转换为 Dataframe 对象
    Args:
        db_session:激活的数据库 Session 实例对象
    """
    print("user data preparing from database...")
    db_users_raw = await dao.get_all_user_profiles(db_session)
    users_data = []
    for user_obj in db_users_raw:
        user_categories_str = user_obj.user_categories if user_obj.user_categories is not None else ""
        user_keywords_str = user_obj.user_keywords if user_obj.user_keywords is not None else ""

        users_data.append({
            'user_id': user_obj.user_id,
            'gender': user_obj.gender,
            'age': user_obj.age,
            'user_categories': user_categories_str,
            'user_keywords': user_keywords_str,
            'user_click_last3m': user_obj.user_click_last3m,
            'user_cart_last3m': user_obj.user_cart_last3m,
            'user_buy_last3m': user_obj.user_buy_last3m,
            'user_forward_last3m': user_obj.user_forward_last3m,
        })
    df_users = pd.DataFrame(users_data)
    print("user data finished\n")

    return df_users

async def get_book_and_interaction_data_by_time(db_session,start_time,end_time):
    """
    按照起止时间读取数据库书籍和交互数据，并将其转换为 Dataframe 对象
    Args:
        db_session:激活的数据库 Session 实例对象
    """
    df_items=pd.DataFrame(await dao.get_item_data_by_time_range(db_session,start_time,end_time))

    interactions = await dao.get_interactions_by_time_range(db_session,start_time,end_time)
    interactions_data = []
    for interaction_obj in interactions:
        interactions_data.append({
            'user_id': interaction_obj.user_id,
            'item_id': interaction_obj.item_id,
            'rating': interaction_obj.rating,
            'hour': interaction_obj.hour,
            'is_weekend': interaction_obj.is_weekend,
            'is_holiday': interaction_obj.is_holiday,
            'click': interaction_obj.click,
            'cart': interaction_obj.cart,
            'forward': interaction_obj.forward,
            'buy': interaction_obj.buy
        })
    df_interactions = pd.DataFrame(interactions_data)

    return df_items,df_interactions


async def get_user_data_by_time(db_session,start_time,end_time):
    """
    按照起止时间读取数据库用户数据，并将其转换为 Dataframe 对象
    Args:
        db_session:激活的数据库 Session 实例对象
    """
    print("user data preparing from database with time range...")
    db_users_raw = await dao.get_user_profiles_by_time_range(db_session,start_time,end_time)
    users_data = []
    for user_obj in db_users_raw:
        user_categories_str = user_obj.user_categories if user_obj.user_categories is not None else ""
        user_keywords_str = user_obj.user_keywords if user_obj.user_keywords is not None else ""

        users_data.append({
            'user_id': user_obj.user_id,
            'gender': user_obj.gender,
            'age': user_obj.age,
            'user_categories': tuple(user_categories_str.split(";")),
            'user_keywords': tuple(user_keywords_str.split(";")),
            'user_click_last3m': user_obj.user_click_last3m,
            'user_cart_last3m': user_obj.user_cart_last3m,
            'user_buy_last3m': user_obj.user_buy_last3m,
            'user_forward_last3m': user_obj.user_forward_last3m,
        })
    df_users = pd.DataFrame(users_data)
    print("user data finished\n")

    return df_users