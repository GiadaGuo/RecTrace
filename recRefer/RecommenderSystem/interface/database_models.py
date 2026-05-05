from sqlalchemy import Column, Integer, String, Float, BigInteger, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# ========== 定义数据库关联类 ===========
class UserProfileBehavior(Base):
    __tablename__ = "user_profile_behavior"

    id = Column(BigInteger, primary_key=True, index=True)
    user_id = Column(String(50), unique=True, index=True)
    gender = Column(String)
    age = Column(Integer)
    user_categories = Column(String) # 是分号分隔的字符串
    user_keywords = Column(String)   # 是分号分隔的字符串
    create_time=Column(DateTime)
    # 统计信息
    user_click_last3m = Column(Integer)
    user_cart_last3m = Column(Integer)
    user_buy_last3m = Column(Integer)
    user_forward_last3m = Column(Integer)

class BookInfo(Base):
    __tablename__ = "book_info"

    item_id = Column(String(50), primary_key=True, index=True)
    name = Column(String)
    author = Column(String)
    city = Column(String)
    item_categories = Column(String) # 是分号分隔的字符串
    item_keywords = Column(String)   # 是分号分隔的字符串
    create_time = Column(DateTime)
    price = Column(Float)
    image = Column(String)
    description = Column(String)

class ItemInteraction(Base):
    __tablename__ = "item_interaction"

    item_id = Column(String(50), primary_key=True, index=True)
    item_click_last3m = Column(Integer)
    item_cart_last3m = Column(Integer)
    item_buy_last3m = Column(Integer)
    item_forward_last3m = Column(Integer)

class UserItemInteraction(Base):
    __tablename__ = "user_item_interaction"

    id = Column(BigInteger, primary_key=True, index=True)
    user_id = Column(String(50), index=True)
    item_id = Column(String(50), index=True)
    rating = Column(Integer)
    create_time = Column(DateTime)
    hour = Column(Integer)
    is_weekend = Column(Boolean)
    is_holiday = Column(Boolean)
    click = Column(Boolean)
    cart = Column(Boolean)
    forward = Column(Boolean)
    buy = Column(Boolean)


