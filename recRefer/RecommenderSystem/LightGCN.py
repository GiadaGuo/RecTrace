import os
import logging
import math
import torch
import torch.nn as nn
from torch import optim
import pandas as pd
from utils import bpr_loss
from dataset import GraphDataset
import faiss

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

class LightGCN(nn.Module):
    def __init__(self,n_layers,n_users,n_items,embed_dim,adj_matrix):
        """
        Args:
            n_layers:图卷积层数
            n_users:用户数
            n_items:物品数
            embed_dim:嵌入层输出维度
            adj_matrix:归一化后的邻接矩阵（sparse_coo_tensor）
        """
        super().__init__()
        self.n_layers=n_layers
        self.n_users=n_users
        self.n_items=n_items
        self.embed_dim=embed_dim
        self.adj_matrix=adj_matrix.to(device)
        self.user_embedding = nn.Parameter(torch.zeros(n_users,embed_dim)) # 没有显式的嵌入层，直接定义等价的查找表，这个相当于就是第一层embedding之后的结果
        self.item_embedding = nn.Parameter(torch.zeros(n_items,embed_dim)) # ID为基础的推荐系统embedding常见实现方式
        nn.init.xavier_uniform_(self.user_embedding) # Xavier均匀初始化,避免训练初始梯度消失
        nn.init.xavier_uniform_(self.item_embedding)

    def forward(self):
        all_embedding=torch.cat([self.user_embedding,self.item_embedding],dim=0) # 所有用户和所有物品
        all_emb_list=[all_embedding]
        for i in range(self.n_layers):
            all_embedding=torch.sparse.mm(self.adj_matrix,all_embedding)
            all_emb_list.append(all_embedding)
        final_all_embedding=sum(all_emb_list)/(self.n_layers+1) # 总共是层数+1个结果

        users_emb,items_emb=final_all_embedding[:self.n_users],final_all_embedding[self.n_users:]

        return users_emb,items_emb

class LightGCNRecommender(nn.Module):
    def __init__(self,item_idx2id):
        super().__init__()
        self.model = None
        self.dataset = None
        self.users_embedding = None
        self.items_embedding = None

        self.item_idx2id = item_idx2id
        self.topk=50
        self.faiss_index = None

    def train_light_gcn(self,n_layers,user_ids,item_ids,embed_dim,df_interactions):
        print("LightGCN training...")
        dataset=GraphDataset(user_ids,item_ids,df_interactions)
        model = LightGCN(n_layers, len(user_ids), len(item_ids), embed_dim, dataset.norm_adj_matrix).to(device)
        # train
        epochs = 50
        batch_size = 200
        batch_num = 10  # 注意这里不是所有batch加起来是对所有数据过了一遍，因为generate是随机采样
        optimizer = optim.Adam(model.parameters(), lr=0.01, weight_decay=0.001)  # weight_decay内置了L2正则化
        model.train()
        for i in range(epochs):
            sum_loss = 0.0
            for j in range(batch_num):
                user_emb, item_emb = model.forward()
                user_idxs, pos_item_idxs, neg_item_idxs = dataset.generate_batch(batch_size)
                user_embs = user_emb[user_idxs]  # [B,emb_dim]
                pos_item_embs = item_emb[pos_item_idxs]  # [B,emb_dim]
                neg_item_embs = item_emb[neg_item_idxs]  # [B,emb_dim]

                pos_scores = (user_embs * pos_item_embs).sum(dim=1)  # [B,1]
                neg_scores = (user_embs * neg_item_embs).sum(dim=1)  # [B,1]
                loss = bpr_loss(pos_scores, neg_scores)

                optimizer.zero_grad()
                loss.backward()
                optimizer.step()

                sum_loss += loss.item()

        self.model = model
        self.dataset = dataset
        # 保存模型权重
        torch.save(model.state_dict(),"model_weights/lightgcn.pth")

        print("LightGCN training finished")

    def fine_tune_model_and_update_index(self, df_items, df_users, df_interactions):
        """
        使用每日新数据对LightGCN模型进行微调，并更新 Faiss 索引
        Args:
            df_items: 全部物品数据
            df_users: 全部用户数据
            df_interactions: 当日新交互数据
        """
        print("开始LightGCN模型微调和索引更新...")
        # 目前的全部用户和物品ID
        all_user_ids = list(set(list(df_users['user_id'])))
        all_item_ids = list(set(list(df_items['item_id'])))

        # 目前的全部交互数据
        combined_interactions = df_interactions

        # 重建数据集（包含新数据）
        print("重建数据集...")
        new_dataset = GraphDataset(all_user_ids, all_item_ids, combined_interactions)

        # 微调模型
        print("开始模型微调...")
        # 扩展模型参数维度以适应新用户和新物品
        old_user_emb = self.model.user_embedding.data
        old_item_emb = self.model.item_embedding.data

        new_n_users = len(all_user_ids)
        new_n_items = len(all_item_ids)

        # 创建新的embedding参数
        new_user_embedding = nn.Parameter(torch.zeros(new_n_users, self.model.embed_dim).to(device))
        new_item_embedding = nn.Parameter(torch.zeros(new_n_items, self.model.embed_dim).to(device))

        # 复制旧参数
        old_n_users, old_n_items = old_user_emb.shape[0], old_item_emb.shape[0]
        new_user_embedding.data[:old_n_users] = old_user_emb
        new_item_embedding.data[:old_n_items] = old_item_emb

        # 初始化新参数
        nn.init.xavier_uniform_(new_user_embedding[old_n_users:])
        nn.init.xavier_uniform_(new_item_embedding[old_n_items:])

        # 替换模型参数
        self.model.user_embedding = new_user_embedding
        self.model.item_embedding = new_item_embedding
        self.model.n_users = new_n_users
        self.model.n_items = new_n_items
        self.model.adj_matrix = new_dataset.norm_adj_matrix.to(device)

        # 微调参数
        optimizer = optim.Adam(self.model.parameters(), lr=0.001, weight_decay=0.001)

        # 微调训练
        self.model.train()
        epochs = 10  # 微调轮数较少
        batch_size = 200
        batch_num = 5  # 微调时减少batch数量

        for i in range(epochs):
            sum_loss = 0.0
            for j in range(batch_num):
                user_emb, item_emb = self.model.forward()
                user_idxs, pos_item_idxs, neg_item_idxs = new_dataset.generate_batch(batch_size)
                user_embs = user_emb[user_idxs]  # [B,emb_dim]
                pos_item_embs = item_emb[pos_item_idxs]  # [B,emb_dim]
                neg_item_embs = item_emb[neg_item_idxs]  # [B,emb_dim]

                pos_scores = (user_embs * pos_item_embs).sum(dim=1)  # [B,1]
                neg_scores = (user_embs * neg_item_embs).sum(dim=1)  # [B,1]
                loss = bpr_loss(pos_scores, neg_scores)

                optimizer.zero_grad()
                loss.backward()
                optimizer.step()

                sum_loss += loss.item()

        # 更新dataset引用
        self.dataset = new_dataset
        # 保存模型权重
        # os.makedirs("model_weights", exist_ok=True)
        torch.save(self.model.state_dict(), "../model_weights/lightgcn.pth")

        print(f"模型微调完成")

        # 重新计算Embeddings
        print("重新计算用户和物品Embeddings...")
        self.users_embedding, self.items_embedding = self.model.forward()

        # 重建Faiss索引
        print("重建Faiss索引...")
        # 更新item_idx2id映射（如果新物品被添加）
        for idx, id in enumerate(all_item_ids):
            if idx not in self.item_idx2id:
                self.item_idx2id[idx] = id

        # 删除旧索引
        if hasattr(self, 'faiss_index') and self.faiss_index is not None:
            del self.faiss_index

        # 创建新索引
        self.initialize_faiss_index()

        print("LightGCN模型微调和索引更新完成！")

    def load_light_gcn(self,n_layers,user_ids,item_ids,embed_dim,df_interactions,path):
        """用训练好的权重直接载入模型"""
        print("LightGCN loading...")
        dataset = GraphDataset(user_ids, item_ids, df_interactions)
        model = LightGCN(n_layers, len(user_ids), len(item_ids), embed_dim, dataset.norm_adj_matrix).to(device)

        model.load_state_dict(torch.load(path))
        self.model=model

        print("LightGCN loading finished")

    def fit(self):
        self.users_embedding, self.items_embedding = self.model.forward()
        # 构建faiss索引
        self.initialize_faiss_index()

    def initialize_faiss_index(self):
        """
        初始化Faiss索引，将物品Embedding存入向量数据库
        """
        if self.items_embedding is None:
            raise ValueError("请先调用 compute_embeddings 计算Embedding")
        print("LightGCN faiss index building...")
        # 获取物品向量维度
        vector_dim = self.items_embedding.shape[1]
        n_vectors = self.items_embedding.shape[0]
        # 转换为numpy格式，faiss需要float32
        # LightGCN的Embedding通常是未经归一化的，需要进行L2归一化以实现余弦相似度
        items_embedding_numpy = self.items_embedding.detach().cpu().numpy().astype('float32')

        # L2归一化,下面再用IP内积就实现了余弦相似度
        faiss.normalize_L2(items_embedding_numpy)

        # 根据物品数量选择索引类型
        if n_vectors < 1000:  # 小数据量使用精确搜索
            index = faiss.IndexFlatIP(vector_dim)  # 余弦相似度(向量已归一化)
            index.add(items_embedding_numpy)
        else:  # 大数据量使用近似搜索
            # IVF (Inverted File) 参数
            n_cluster = int(math.sqrt(n_vectors))  # 聚类中心数量
            # PQ (Product Quantization) 参数
            # M 是切分成的段数，因此向量维度要能整除M，物品塔的输出维度是32
            M = 8
            # 创建量化器 (用于聚类)
            quantizer = faiss.IndexFlatIP(vector_dim)  # 使用内积，因为已经归一化
            # 创建索引
            index = faiss.IndexIVFPQ(quantizer, vector_dim, n_cluster, M, 8)
            index.train(items_embedding_numpy)
            index.add(items_embedding_numpy)

        # 保存索引
        self.faiss_index = index

        print("LightGCN faiss index finished")

    def recommend_for_user(self, user_idx, top_k=50):
        """
        使用Faiss向量数据库为用户推荐物品
        Args:
            user_idx: 用户索引
            top_k: 推荐物品数量
        Returns:
            推荐的物品ID集合
        """
        if not hasattr(self, 'faiss_index') or self.faiss_index is None:
            raise ValueError("请先调用 fit 函数构建Faiss索引")

        # 获取用户Embedding
        user_embedding = self.users_embedding[user_idx].detach().cpu().numpy().astype('float32')

        # 对用户Embedding也进行L2归一化，以匹配物品向量的相似度计算方式
        faiss.normalize_L2(user_embedding.reshape(1, -1))

        # 设置搜索参数 (nprobe越大越精确但越慢)
        self.faiss_index.nprobe = 5

        # 在Faiss索引中搜索最相似的物品
        scores, indices = self.faiss_index.search(user_embedding.reshape(1, -1), top_k)

        # 将返回的物品索引转换为原始物品ID
        recommended_item_ids = [self.item_idx2id[idx] for idx in indices[0]]

        return set(recommended_item_ids)

if __name__ == "__main__":
    # 加载训练数据
    print("data preparing...")
    # 准备物品数据
    df_items = pd.read_csv("data/items_new.csv", encoding="utf-8")
    df_items['item_keywords'] = df_items['item_keywords'].apply(lambda x: tuple(x.split(';')))
    item_idx2id={}
    for idx,id in enumerate(list(df_items['item_id'])):
        item_idx2id[idx] = id
    # 准备用户数据
    df_users = pd.read_csv("data/users_new.csv", encoding="utf-8")
    df_users['user_categories'] = df_users['user_categories'].fillna('').apply(lambda x: tuple(x.split(';')))
    df_users['user_keywords'] = df_users['user_keywords'].fillna('').apply(lambda x: tuple(x.split(';')))
    # 准备交互数据
    df_interactions = pd.read_csv("data/interactions_new.csv", encoding="utf-8")
    print("data finished\n")
    user_ids = list(df_users['user_id'])  # 用户ID列表
    item_ids = list(df_items['item_id'])  # 物品ID列表

    recommender = LightGCNRecommender(item_idx2id)
    recommender.train_light_gcn(2,user_ids,item_ids,64,df_interactions)
    """最佳参数: {'n_layers': 2, 'embed_dim': 64, 'lr': 0.01, 'weight_decay': 0.001, 'epochs': 50}"""