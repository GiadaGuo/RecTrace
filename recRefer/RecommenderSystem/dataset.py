import pandas as pd
import torch
from torch.utils.data import Dataset
import random
import numpy as np
from scipy.sparse import bmat, csr_matrix

# ----------------------------
# 数据集类（支持负采样 1:2）
# ----------------------------
class TwoTowerDataset(Dataset):
    def __init__(self, df_all, processor, user_discrete_cols, item_discrete_cols,
                 user_continuous_cols, item_continuous_cols, n_neg=2):
        """
        用于双塔模型的训练数据加载，核心功能是：
        正负样本构造,按 1:2 比例进行采样
        调用 FeatureProcessor 将原始样本转为模型输入张量
        :param df_all:包含所有正样本的 DataFrame（每行是一个用户-物品交互）
        :param processor:已训练好的 FeatureProcessor 实例
        :param *cols: 各类特征列名列表,与 FeatureProcessor 相同
        :param n_neg:每个正样本对应的负样本数量
        """
        self.df_all = df_all.reset_index(drop=True)
        self.processor = processor
        self.user_discrete_cols = user_discrete_cols
        self.item_discrete_cols = item_discrete_cols
        self.user_continuous_cols = user_continuous_cols
        self.item_continuous_cols = item_continuous_cols
        self.n_neg = n_neg
        self.all_item_ids = list(processor.item_id_vocab.values())

    def __len__(self):
        return len(self.df_all)

    def __getitem__(self, idx):
        row = self.df_all.iloc[idx]

        # 正样本特征
        pos_user_ids, pos_user_disc, pos_user_cont = self.processor.transform_user_features(
            pd.DataFrame([row]), self.user_discrete_cols, self.user_continuous_cols)
        pos_item_ids, pos_item_disc, pos_item_cont = self.processor.transform_item_features(
            pd.DataFrame([row]), self.item_discrete_cols, self.item_continuous_cols)

        # 负采样:从所有物品中随机挑，作为负样本
        neg_samples = []
        for _ in range(self.n_neg):
            neg_item_id = random.choice(self.all_item_ids)
            neg_row = row.copy()
            neg_row['item_id'] = list(self.processor.item_id_vocab.keys())[neg_item_id]
            neg_item_ids, neg_item_disc, neg_item_cont = self.processor.transform_item_features(
                pd.DataFrame([neg_row]), self.item_discrete_cols, self.item_continuous_cols)
            neg_samples.append((neg_item_ids, neg_item_disc, neg_item_cont))

        return {
            'pos': (pos_user_ids, pos_user_disc, pos_user_cont, pos_item_ids, pos_item_disc, pos_item_cont),
            'neg': neg_samples,
        }

class ThreeTowerDataset(Dataset):
    """三塔模型数据集(精排中的多目标模型复用)"""
    def __init__(self, df, processor,
                 user_discrete_cols, item_discrete_cols,
                 scene_discrete_cols, user_cont_cols, item_cont_cols, stat_cont_cols,
                 target_cols=None):
        self.df = df.reset_index(drop=True)
        self.processor = processor
        self.user_discrete_cols = user_discrete_cols
        self.item_discrete_cols = item_discrete_cols
        self.scene_discrete_cols = scene_discrete_cols
        self.user_cont_cols = user_cont_cols
        self.item_cont_cols = item_cont_cols
        self.stat_cont_cols = stat_cont_cols
        self.target_cols = target_cols

    def __len__(self):
        return len(self.df)

    def __getitem__(self, idx):
        row = self.df.iloc[idx]

        # 转换特征
        user_ids, user_discrete, user_continuous = \
            self.processor.transform_user_features(
                pd.DataFrame([row]), self.user_discrete_cols, self.user_cont_cols) # 离散特征ID转索引，连续特征归一化

        scene_discrete=self.processor.transform_scene_features(pd.DataFrame([row]),self.scene_discrete_cols)

        item_ids, item_discrete, item_continuous = \
            self.processor.transform_item_features(
                pd.DataFrame([row]), self.item_discrete_cols, self.item_cont_cols)

        stat_continuous = self.processor.transform_stat_features(
            pd.DataFrame([row]), self.stat_cont_cols)

        # 获取目标值
        targets = torch.tensor([row[col] for col in self.target_cols], dtype=torch.float32)

        return {
            'user_ids': user_ids.squeeze(),
            'user_discrete': user_discrete.squeeze(),
            'user_continuous': user_continuous.squeeze(),
            'scene_discrete': scene_discrete.squeeze(),
            'item_ids': item_ids.squeeze(),
            'item_discrete': item_discrete.squeeze(),
            'item_continuous': item_continuous.squeeze(),
            'stat_continuous': stat_continuous.squeeze(),
            'targets': targets
        }

class GraphDataset(Dataset):
    def __init__(self, users_ids,items_ids,df_interactions):
        self.user2idx={id:index for index,id in enumerate(users_ids)}
        self.item2idx={id:index for index,id in enumerate(items_ids)}
        self.user_ids=users_ids # 全体用户id列表
        self.interactions=df_interactions
        self.real_user_ids=df_interactions['user_id'].unique().tolist() # 存在交互记录的用户id列表
        self.n_users=len(users_ids)
        self.n_items=len(items_ids)
        self.norm_adj_matrix=self.build_norm_adj_matrix(df_interactions)
        # 以下数据用于训练时的正负样本采样
        self.user2items=df_interactions[['user_id','item_id']].groupby('user_id')['item_id'].apply(list).to_dict() # {user_id:[item_ids]}表示用户交互过的物品ids
        self.items_ids = items_ids

    def build_norm_adj_matrix(self,df_interactions:pd.DataFrame):
        user_indices = [self.user2idx[id] for id in df_interactions['user_id']]
        item_indices = [self.item2idx[id] for id in df_interactions['item_id']]
        R = csr_matrix((np.ones(len(user_indices)), (user_indices, item_indices)),
                       shape=(self.n_users, self.n_items))
        R.data = (R.data > 0).astype(float) # 有重复记录的话csr_matrix会自动相加，所以要做重新二值化

        zero_1=csr_matrix((self.n_users,self.n_users))
        zero_2=csr_matrix((self.n_items,self.n_items))
        A=bmat([[zero_1,R],[R.T,zero_2]],format='csr') # R是用户对物品的，R.T是物品对用户的，以CSR格式存储稀疏矩阵
        degree = np.array(A.sum(axis=1)).flatten()
        degree[degree==0]=1e-10 # 防止除0（孤立节点）
        # normalization
        row_idxs,col_idxs = A.nonzero()
        row_degrees,col_degrees = degree[row_idxs],degree[col_idxs]
        data=1.0/np.sqrt(row_degrees*col_degrees)
        A.data=data
        # 转tensor格式
        A_coo = A.tocoo()

        indices = torch.LongTensor(np.array([A_coo.row, A_coo.col]))
        values = torch.FloatTensor(A_coo.data)
        shape = torch.Size(A_coo.shape)
        # 禁用稀疏不变性检查（性能更好）
        torch.sparse.check_sparse_tensor_invariants.disable()
        norm_adj_tensor = torch.sparse_coo_tensor(indices, values, shape)

        return norm_adj_tensor

    def generate_batch(self,batch_size):
        batch_user_ids=random.sample(self.real_user_ids,batch_size)
        pos_item_idxs=[]
        neg_item_idxs=[]
        for user_id in batch_user_ids:
            pos_item_idx=self.item2idx[random.choice(self.user2items[user_id])]
            neg_item_idx=self.item2idx[random.choice(self.items_ids)]
            pos_item_idxs.append(pos_item_idx)
            neg_item_idxs.append(neg_item_idx)
        user_idxs=[self.user2idx[user_id] for user_id in batch_user_ids]

        return torch.LongTensor(user_idxs),torch.LongTensor(pos_item_idxs),torch.LongTensor(neg_item_idxs)

if __name__ == '__main__':
    # 准备物品数据
    df_items = pd.read_csv("data/items.csv", encoding="utf-8")
    # 准备用户数据
    df_users = pd.read_csv("data/users.csv", encoding="utf-8")
    # 准备交互数据
    df_interactions = pd.read_csv("data/interactions.csv", encoding="utf-8")
    graph_dataset=GraphDataset(list(df_users['user_id']),list(df_items['item_id']),df_interactions)




