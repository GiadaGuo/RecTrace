"""
feature_schema.py
-----------------
Dataclass definitions for user features, item features, and the combined
feature bundle used by the ranking model.
"""

from dataclasses import dataclass, field
from typing import List


@dataclass
class UserFeatures:
    """Global cumulative features for a single user (feat:user:{uid})."""
    uid: str = ""
    total_pv: int = 0
    total_click: int = 0
    total_cart: int = 0
    total_fav: int = 0
    total_buy: int = 0
    active_days: int = 0
    user_age: int = 0
    user_city: str = ""
    user_level: int = 0
    # Window stats (feat:user:stat:{uid})
    click_5min: float = 0.0
    click_1h: float = 0.0
    pv_5min: float = 0.0
    pv_1h: float = 0.0
    # Recent click sequence (seq:click:{uid}) — list of item_ids, newest first
    click_seq: List[str] = field(default_factory=list)


@dataclass
class ItemFeatures:
    """Global cumulative features for a single item (feat:item:{item_id})."""
    item_id: str = ""
    total_detail_click: int = 0
    total_buy: int = 0
    total_cart: int = 0
    total_fav: int = 0
    uv: int = 0
    conversion_rate: float = 0.0
    category_id: int = 0
    item_brand: str = ""
    item_price: float = 0.0
    # Window stats (feat:item:stat:{item_id})
    click_5min: float = 0.0
    click_1h: float = 0.0


@dataclass
class CrossFeatures:
    """User × category cross features with exponential decay (cross:{uid}:{cat_id})."""
    pv_cnt: float = 0.0
    click_cnt: float = 0.0
    cart_cnt: float = 0.0
    buy_cnt: float = 0.0


@dataclass
class UserItemWindowFeatures:
    """Home-channel 1-min window features (feat:user_item:{uid}:{cat_id})."""
    show_cnt: int = 0
    click_cnt: int = 0
    cart_cnt: int = 0
    buy_cnt: int = 0


@dataclass
class FeatureBundle:
    """All features assembled for one (user, item) pair at ranking time."""
    user: UserFeatures = field(default_factory=UserFeatures)
    item: ItemFeatures = field(default_factory=ItemFeatures)
    cross: CrossFeatures = field(default_factory=CrossFeatures)
    user_item_window: UserItemWindowFeatures = field(default_factory=UserItemWindowFeatures)
