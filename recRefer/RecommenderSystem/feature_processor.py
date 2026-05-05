import torch
from sklearn.preprocessing import StandardScaler

device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

class FeatureProcessor:
    """统一特征处理器，同时支持双塔模型和三塔模型"""
    def __init__(self):
        # 双塔模型相关
        self.user_id_vocab = {}
        self.item_id_vocab = {}
        self.user_discrete_vocab = {}
        self.item_discrete_vocab = {}

        # 三塔模型新增
        self.scene_discrete_vocab = {}  # 场景离散特征
        self.user_cont_scaler = None
        self.item_cont_scaler = None
        self.stat_cont_scaler = None

        # 内部标记：当前模式
        self._mode = None  # 'two_tower' 或 'three_tower'

    def _validate_columns(self, required_cols, provided_cols, context=""):
        """验证列名是否完整"""
        missing = set(required_cols) - set(provided_cols)
        if missing:
            raise ValueError(f"{context} 缺少必要列: {missing}")

    def build_vocab_and_scale_two_tower(self, df,
                                        user_discrete_cols, item_discrete_cols,
                                        user_continuous_cols, item_continuous_cols):
        """为双塔模型构建词汇表和标准化器"""
        # 设置模式
        self._mode = 'two_tower'
        # 构建ID词汇表
        all_user_ids = set(df['user_id'])
        self.user_id_vocab = {uid: i for i, uid in enumerate(sorted(all_user_ids))}

        all_item_ids = set(df['item_id'])
        self.item_id_vocab = {iid: i for i, iid in enumerate(sorted(all_item_ids))}

        # 构建离散特征词汇表
        for col in user_discrete_cols:
            unique_vals = set(df[col].values)
            self.user_discrete_vocab[col] = {val: i for i, val in enumerate(sorted(unique_vals))}

        for col in item_discrete_cols:
            unique_vals = set(df[col].values)
            self.item_discrete_vocab[col] = {val: i for i, val in enumerate(sorted(unique_vals))}

        # 构建连续特征标准化器
        self.user_cont_scaler = StandardScaler()
        self.item_cont_scaler = StandardScaler()

        if user_continuous_cols:
            self.user_cont_scaler.fit(df[user_continuous_cols].values)
        if item_continuous_cols:
            self.item_cont_scaler.fit(df[item_continuous_cols].values)

    def build_vocab_and_scale_three_tower(self, df,
                                          user_discrete_cols, item_discrete_cols, scene_discrete_cols,
                                          user_cont_cols, item_cont_cols, stat_cont_cols):
        """为三塔模型构建词汇表和标准化器"""
        # 设置模式
        self._mode = 'three_tower'

        # 构建ID词汇表（复用双塔逻辑）
        all_user_ids = set(df['user_id'])
        self.user_id_vocab = {uid: i for i, uid in enumerate(sorted(all_user_ids))}

        all_item_ids = set(df['item_id'])
        self.item_id_vocab = {iid: i for i, iid in enumerate(sorted(all_item_ids))}

        # 构建所有离散特征词汇表
        for col in user_discrete_cols:
            unique_vals = set(df[col].values)
            self.user_discrete_vocab[col] = {val: i for i, val in enumerate(sorted(unique_vals))}

        for col in item_discrete_cols:
            unique_vals = set(df[col].values)
            self.item_discrete_vocab[col] = {val: i for i, val in enumerate(sorted(unique_vals))}

        for col in scene_discrete_cols:
            unique_vals = set(df[col].values)
            self.scene_discrete_vocab[col] = {val: i for i, val in enumerate(sorted(unique_vals))}

        # 构建所有连续特征标准化器
        self.user_cont_scaler = StandardScaler()
        self.item_cont_scaler = StandardScaler()
        self.stat_cont_scaler = StandardScaler()

        # 拟合标准化器
        if user_cont_cols:
            self.user_cont_scaler.fit(df[user_cont_cols].values)
        if item_cont_cols:
            self.item_cont_scaler.fit(df[item_cont_cols].values)
        if stat_cont_cols:
            self.stat_cont_scaler.fit(df[stat_cont_cols].values)

    def build_vocab_and_scale(self, df,
                              user_discrete_cols=None, item_discrete_cols=None,
                              user_continuous_cols=None, item_continuous_cols=None,
                              # 三塔新增参数（默认为None，保持向后兼容）
                              scene_discrete_cols=None,stat_cont_cols=None):
        """
        统一接口：根据参数自动判断是双塔还是三塔模式
        """
        # 检查是哪种模式
        is_three_tower = any([scene_discrete_cols, stat_cont_cols])

        if is_three_tower:
            # 三塔模式
            scene_discrete_cols = scene_discrete_cols or []
            stat_cont_cols = stat_cont_cols or []

            self.build_vocab_and_scale_three_tower(
                df,
                user_discrete_cols, item_discrete_cols, scene_discrete_cols,
                user_continuous_cols, item_continuous_cols, stat_cont_cols
            )
        else:
            # 双塔模式
            user_continuous_cols = user_continuous_cols or []
            item_continuous_cols = item_continuous_cols or []

            self.build_vocab_and_scale_two_tower(
                df,
                user_discrete_cols or [], item_discrete_cols or [],
                user_continuous_cols, item_continuous_cols
            )

    def transform_user_features(self, df_batch, user_discrete_cols, user_cont_cols):
        """转换用户特征（双塔和三塔通用）"""
        # 用户ID
        user_ids = torch.tensor([self.user_id_vocab.get(x, 0) for x in df_batch['user_id']])

        # 用户离散特征
        user_discrete_list = []
        for col in user_discrete_cols:
            col_vals = torch.tensor([self.user_discrete_vocab[col].get(x, 0) for x in df_batch[col]])
            user_discrete_list.append(col_vals)

        user_discrete = torch.stack(user_discrete_list, dim=1) if user_discrete_list else \
            torch.zeros((len(df_batch), 0), dtype=torch.long)

        # 用户连续特征
        if user_cont_cols and self.user_cont_scaler:
            user_cont_values = self.user_cont_scaler.transform(df_batch[user_cont_cols].values)
            user_continuous = torch.tensor(user_cont_values, dtype=torch.float32)
        else:
            user_continuous = torch.zeros((len(df_batch), 0), dtype=torch.float32)

        return user_ids.to(device), user_discrete.to(device), user_continuous.to(device)

    def transform_item_features(self, df_batch, item_discrete_cols, item_cont_cols):
        """转换物品特征（双塔和三塔通用）"""
        # 物品ID
        item_ids = torch.tensor([self.item_id_vocab.get(x, 0) for x in df_batch['item_id']])

        # 物品离散特征
        item_discrete_list = []
        for col in item_discrete_cols:
            col_vals = torch.tensor([self.item_discrete_vocab[col].get(x, 0) for x in df_batch[col]])
            item_discrete_list.append(col_vals)

        item_discrete = torch.stack(item_discrete_list, dim=1) if item_discrete_list else \
            torch.zeros((len(df_batch), 0), dtype=torch.long)

        # 物品连续特征
        if item_cont_cols and self.item_cont_scaler:
            item_cont_values = self.item_cont_scaler.transform(df_batch[item_cont_cols].values)
            item_continuous = torch.tensor(item_cont_values, dtype=torch.float32)
        else:
            item_continuous = torch.zeros((len(df_batch), 0), dtype=torch.float32)

        return item_ids.to(device), item_discrete.to(device), item_continuous.to(device)

    def transform_scene_features(self, df_batch, scene_discrete_cols):
        """转换场景特征（三塔专用）"""
        if not scene_discrete_cols:
            # 返回空张量
            scene_discrete = torch.zeros((len(df_batch), 0), dtype=torch.long)
            scene_continuous = torch.zeros((len(df_batch), 0), dtype=torch.float32)
            return scene_discrete, scene_continuous

        # 场景离散特征
        scene_discrete_list = []
        for col in scene_discrete_cols:
            col_vals = torch.tensor([self.scene_discrete_vocab[col].get(x, 0) for x in df_batch[col]])
            scene_discrete_list.append(col_vals)

        scene_discrete = torch.stack(scene_discrete_list, dim=1) if scene_discrete_list else \
            torch.zeros((len(df_batch), 0), dtype=torch.long)

        return scene_discrete.to(device)

    def transform_stat_features(self, df_batch, stat_cont_cols):
        """转换统计特征（三塔专用）"""
        if not stat_cont_cols or not self.stat_cont_scaler:
            return torch.zeros((len(df_batch), 0), dtype=torch.float32)

        stat_cont_values = self.stat_cont_scaler.transform(df_batch[stat_cont_cols].values)
        stat_continuous = torch.tensor(stat_cont_values, dtype=torch.float32)
        return stat_continuous.to(device)