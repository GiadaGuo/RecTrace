"""
base_model.py
-------------
Abstract interface for all ranking models.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List


class ScoredItem:
    """A ranked item with its score and per-feature contribution breakdown."""

    __slots__ = ("item_id", "score", "feature_contributions", "item_brand", "item_price", "category_id")

    def __init__(
        self,
        item_id: str,
        score: float,
        feature_contributions: Dict[str, float],
        item_brand: str = "",
        item_price: float = 0.0,
        category_id: int = 0,
    ):
        self.item_id = item_id
        self.score = score
        self.feature_contributions = feature_contributions
        self.item_brand = item_brand
        self.item_price = item_price
        self.category_id = category_id

    def to_dict(self) -> Dict[str, Any]:
        return {
            "item_id": self.item_id,
            "score": round(self.score, 6),
            "feature_contributions": {k: round(v, 4) for k, v in self.feature_contributions.items()},
            "item_brand": self.item_brand,
            "item_price": self.item_price,
            "category_id": self.category_id,
        }


class BaseModel(ABC):
    @abstractmethod
    def predict(
        self,
        uid: str,
        candidates: List[str],
        r=None,
    ) -> List[ScoredItem]:
        """
        Score and rank the candidate items for the given user.

        Parameters
        ----------
        uid        : user identifier
        candidates : list of candidate item_ids
        r          : optional Redis client (injected for testing)

        Returns
        -------
        List[ScoredItem] sorted descending by score.
        """
