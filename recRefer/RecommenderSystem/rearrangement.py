from collections import defaultdict
from typing import List
from sklearn.metrics.pairwise import cosine_similarity
from entities import *
import numpy as np

device = "cuda" if torch.cuda.is_available() else "cpu"

class MmrDiversity:
    def __init__(self):
        """
        基于MMR（最大边际相关性）的多样性重排算法
        Args:
            lambda_param: MMR平衡参数，控制相关性和多样性的权衡 (0 <= lambda <= 1)
            selection_count: 要选择的物品数量
            window_size: 滑动窗口大小
        """
        self.items = []
        self.lambda_param = 0.7
        self.selection_count = 50
        self.window_size = 30
        # 物品间的余弦相似度矩阵（基于内容）
        self.item_id_index = {} #在build_cosine_similarity_matrix填充，这里的index就是矩阵的index
        self.cosine_similarity_matrix = None

    def build_cosine_similarity_matrix(self, items):
        """
        离线计算所有物品间的余弦相似度（基于内容）
        Args:
            items: 全体物品列表
        """
        self.items = items
        n = len(self.items)

        # 创建物品ID到索引的映射
        self.item_id_index = {item.item_id: i for i, item in enumerate(items)}

        # 提取所有 content_feature 并堆叠成矩阵 [n, feature_dim]
        features_list = []
        for item in items:
            features_list.append(item.content_feature)

        # 堆叠成 [n, feature_dim] 矩阵
        all_features = np.vstack(features_list)

        # 使用sklearn批量计算余弦相似度矩阵
        self.cosine_similarity_matrix = cosine_similarity(all_features)

    def mmr_diversity_selection(self,items)->List[int]:
        """
        执行MMR算法
        Args:
            items: 精排传过来的item列表
        """
        if not items:
            raise TypeError("物品列表为空")
        if self.cosine_similarity_matrix is None:
            raise TypeError("请先调用build_cosine_similarity_matrix方法计算余弦相似度矩阵")

        # 初始化已选择物品列表
        selected_items = []
        remaining_items = items.copy()

        # 第一个物品选择相关性最高的
        first_item = max(remaining_items, key=lambda x: x.relevance_score)
        selected_items.append(first_item)
        remaining_items.remove(first_item)

        # 逐步选择剩余物品(考虑剩余物品不够选择的情况)
        for _ in range(min(self.selection_count - 1, len(remaining_items))):
            if not remaining_items:
                break
            # 确定当前窗口范围(考虑窗口超出已挑选物品列表的情况)
            current_window_size = min(self.window_size, len(selected_items))  # 实际的窗口大小，因为刚开始已挑选的物品列表较小
            current_window = selected_items[len(selected_items) - current_window_size:]
            # 计算每个候选物品的MMR得分
            mmr_scores = defaultdict(float)
            for item_r in remaining_items:
                # 计算与窗口中物品的最大相似度
                max_sim = -1
                for item_w in current_window:
                    sim = self.cosine_similarity_matrix[self.item_id_index[item_r.item_id],self.item_id_index[item_w.item_id]]
                    if sim > max_sim:
                        max_sim = sim
                # 挑选的relevance_score越高越好，max_sim越小越好
                mmr_scores[item_r] = self.lambda_param * item_r.relevance_score - (1 - self.lambda_param) * max_sim
            # 选择MMR得分最高的物品
            best_item = max(mmr_scores.items(), key=lambda x: x[1])[0]
            # 将选中的物品加入已选择列表
            selected_items.append(best_item)
            remaining_items.remove(best_item)

        selected_items = [item.item_id for item in selected_items]

        return selected_items