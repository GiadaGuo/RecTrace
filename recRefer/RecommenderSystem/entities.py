import logging
from collections import deque
from PIL import Image
from io import BytesIO
import requests
import torch
import clip

device=torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

class Item:
    """物品类，表示系统中的内容项"""
    def __init__(self, item_idx,item_id, name,author, categories, keywords,discrete_features,
                 continuous_features,create_time,image,description,content_feature=None,relevance_score=None):
        """
        初始化物品类
        Args:
            item_idx : 物品索引
            item_id: 物品ID
            name: 物品标题
            categories: 物品所属类目列表
            keywords: 物品包含的关键词列表
            discrete_features: 离散特征字典，如 {'category': 'A'}
            continuous_features: 连续特征字典，如 {'price': 50.0}
            create_time: 物品创建时间
            image: 物品图片url
            description: 物品文字描述
            content_feature: 基于内容的特征向量
        """
        self.item_idx=item_idx
        self.item_id = item_id
        self.name = name
        self.author=author
        self.categories = categories
        self.keywords = keywords
        self.create_time = create_time

        self.discrete_features = discrete_features or {}
        self.continuous_features = continuous_features or {}

        self.image = image
        self.description = description
        # 基于内容的向量表征
        self.content_feature = content_feature
        self.relevance_score = relevance_score

    def calculate_content_feature(self,model,preprocess):
        # 反爬措施，如果用的是自己的服务器可以不要
        headers = {
            # 设置 User-Agent，让服务器以为你是浏览器而不是脚本
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            # "Referer": "https://movie.douban.com/"
        }
        try:
            # 发送带有请求头的 GET 请求
            response = requests.get(self.image, stream=True, headers=headers)
            if response.status_code == 200:
                # 将字节数据包装成文件对象
                image_data = BytesIO(response.content)
                # 使用Pillow打开图像
                img = Image.open(image_data)
                # 强转RGB图像
                rgb_img = img.convert('RGB')
        except Exception as e:
            logging.error(f"request error, message:{e}")
            raise RuntimeError("获取图片失败")

        with torch.no_grad():#非训练不需要梯度，节省显存
            image_tensor = preprocess(rgb_img).unsqueeze(0).to(device) # torch.Size([1, 3, 224, 224])
            text_tensor = clip.tokenize(self.description).to(device) # torch.Size([1, 77])

            image_feature=model.encode_image(image_tensor) # torch.Size([1, 512])
            text_feature=model.encode_text(text_tensor) # torch.Size([1, 512])
            content_feature=torch.cat((image_feature,text_feature),dim=1)# torch.Size([1,1024])
            # ======== 内存优化 ========
            content_feature_cpu=content_feature.cpu() # 显存不够存储，转移到CPU上
            self.content_feature=content_feature_cpu
            del content_feature # 释放显存中的tensor空间

    def __repr__(self):
        return str({'item_id':self.item_id, 'name':self.name,'keywords':self.keywords,'description':self.description,'relevance_score':self.relevance_score})

class UserProfile:
    """用户画像类"""
    def __init__(self, user_idx,user_id, categories=None, keywords=None, discrete_features=None,
                 continuous_features=None,max_history=50):
        """
        初始化用户画像
        Args:
            user_idx: 用户索引 (0-n_users)
            user_id: 用户ID
            categories: 用户感兴趣的类目列表
            keywords: 用户感兴趣的关键词列表
            max_history: 用户历史交互队列的最大长度
            discrete_features: 离散特征字典，如 {'gender': 'M'}
            continuous_features: 连续特征字典，如 {'age': 25}
        """
        self.user_idx = user_idx
        self.user_id = user_id
        self.categories = categories  # 存储用户感兴趣的类目
        self.keywords = keywords  # 存储用户感兴趣的关键词
        self.interaction_history = deque(maxlen=max_history)  # 使用双端队列限制历史长度
        self.discrete_features = discrete_features or {}
        self.continuous_features = continuous_features or {}

    def __repr__(self):
        return str(self.__dict__)

    def add_interaction(self, item_id):
        """添加用户交互记录"""
        self.interaction_history.append(item_id)

    def get_last_n_interactions(self, n):
        """获取用户最近的n次交互记录"""
        return list(self.interaction_history)[-n:]