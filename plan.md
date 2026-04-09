# 需求
从0开始帮我搭建1个java工程项目demo（maven项目），实现实时数据链路，核心目标是在本地就可以跑通。

技术栈（用当前最新的稳定版本）：kafka,flink,redis,java,Docker Compose,SQLite

# 数据

数据模拟脚本用python还是java要给出最佳选择。模拟出来的数据需要接近真实业务特征。

## 原始行为数据
Kafka: ods_user_behavior
└── 实时事件流（100条/秒，幂律分布热点，漏斗转化率）

参照淘宝用户购物行为数据集（本数据集包含了2017年11月25日至2017年12月3日之间，有行为的约一百万随机用户的所有行为（行为包括点击、购买、加购、喜欢）。数据集的组织形式和MovieLens-20M类似，即数据集的每一行表示一条用户行为，由用户ID、商品ID、商品类目ID、行为类型和时间戳组成，并以逗号分隔）

列名称	说明    
用户ID	整数类型，序列化后的用户ID    
商品ID	整数类型，序列化后的商品ID 
商品类目ID	整数类型，序列化后的商品所属类目ID   
行为类型	字符串，枚举类型，包括('pv', 'buy', 'cart', 'fav')   
时间戳	行为发生的unix时间戳   

模拟实时行为事件流
    考虑热门商品集中效应（20%的商品贡献80%的流量）
    考虑行为漏斗转化率（70%浏览，10%收藏，15%加购，5%购买）；
    生成完之后先简单验证数据分布符合预期
    可以模拟正常流量 + 可手动触发突发流量（用于反压测试）

## 维度数据
dim.db（SQLite）
├── dim_user：1000个用户（年龄/城市/等级）
└── dim_item：5000个商品（类目/品牌/价格）

Redis
├── dim:user:U0001 → hash
└── dim:item:I00001 → hash

先写入SQLite，再同步写入redis


# 项目流程

数据模拟层          采集层           处理层                    输出层
                                                        
Mock脚本      →   Kafka         →   Flink              →   Kafka
(行为事件)        (原始Topic)        (特征计算/维度拼接)       (特征Topic)
                                        ↑
                                  Redis（缓存）/SQLite（持久化）
                                   (维度数据)

# 数仓分层
Topic: ods_user_behavior          # 原始行为数据（输入）
        ↓
Topic: dwd_behavior_with_dim      # 关联维度后的明细数据
        ↓
Topic: dws_user_item_feature（交叉特征：以 user_id + category_id 为粒度）      # 聚合后的特征数据（输出）
      dws_user_feature（用户实时特征：需要维护用户历史状态）
      dws_item_feature（商品实时特征）
     


# flink job设计

Job1：维度拼接（学习异步IO + Redis）

Job2：窗口聚合（学习Window + State）

Job3：实时指标（学习KeyedState长期状态）



# 本地启动顺序
第一步：docker compose up
        启动 Kafka + Redis + SQLite + Flink集群

第二步：初始化维度数据
        向 Redis 写入用户/商品维度数据
        向 SQLite 写入维度表（作为备份）

第三步：创建 Kafka Topics
    

第四步：启动 Mock 数据脚本
        按泊松分布模拟真实流量（白天多/夜晚少）

第五步：提交 Flink Job（按顺序）
        Job1 维度拼接
        Job2 窗口聚合
        Job3 实时指标

第六步：调试验证
        Kafka console consumer 消费输出Topic验证数据
        Flink Web UI 观察各算子指标
        手动触发反压 / 模拟Checkpoint恢复