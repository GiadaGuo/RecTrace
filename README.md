# Real-time Data Pipeline Demo

本地可运行的实时数据链路 Demo，技术栈：Kafka + Flink + Redis + SQLite + Python + Docker Compose。

## 架构图

```
Mock脚本(Python)  →  Kafka(ods_user_behavior)  →  Flink Job1(维度拼接/AsyncIO+Redis)
                                                         ↓
                              Redis(维度缓存)  →  Kafka(dwd_behavior_with_dim)
                              SQLite(维度持久)         ↓                ↓
                                              Flink Job2(窗口聚合)  Flink Job3(实时指标)
                                                   ↓                    ↓          ↓
                                        dws_user_item_feature  dws_user_feature  dws_item_feature
```

## 版本

| 组件 | 版本 |
|------|------|
| Java | 11 |
| Flink | 1.20.3 |
| Kafka | 3.7.1 (KRaft) |
| Redis | 7.4 |
| Python | 3.8+ |

---

## 启动步骤

### 第一步：启动基础设施

```bash
docker compose up -d
```

等待所有服务就绪（约 30 秒），检查状态：

```bash
docker compose ps
```

### 第二步：安装 Python 依赖

```bash
pip install -r scripts/requirements.txt
```

### 第三步：创建 Kafka Topics

```bash
bash scripts/create_topics.sh
```

### 第四步：初始化维度数据

向 SQLite 和 Redis 写入 1000 个用户、5000 个商品的维度数据：

```bash
python scripts/init_dim_data.py
```

### 第五步：提交 Flink Jobs

先将 JAR 打包（首次或代码修改后）：

```bash
mvn clean package -DskipTests -pl flink-jobs
```

复制 JAR 到 Flink 服务：

```bash
docker cp flink-jobs/target/flink-jobs-1.0-SNAPSHOT.jar flink-jobmanager:/opt/flink/usrlib/
```

按顺序提交三个 Job：

```bash
# Job1: 维度拼接
docker exec flink-jobmanager flink run \
  -c com.demo.job1.DimJoinJob \
  /opt/flink/usrlib/flink-jobs-1.0-SNAPSHOT.jar

# Job2: 窗口聚合
docker exec flink-jobmanager flink run \
  -c com.demo.job2.WindowAggJob \
  /opt/flink/usrlib/flink-jobs-1.0-SNAPSHOT.jar

# Job3: 实时指标
docker exec flink-jobmanager flink run \
  -c com.demo.job3.RealtimeFeatureJob \
  /opt/flink/usrlib/flink-jobs-1.0-SNAPSHOT.jar
```

### 第六步：启动 Mock 数据生产者

```bash
# 正常模式（~100 条/秒）
python scripts/mock_producer.py

# 突发流量模式（500 条/秒，持续 30 秒，用于反压测试）
python scripts/mock_producer.py --burst
```

---

## 调试验证

### Flink Web UI
打开浏览器访问 http://localhost:8081，查看 Job 运行状态和算子指标。

### 消费 Kafka 输出 Topic

```bash
# 查看维度拼接结果
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic dwd_behavior_with_dim \
  --from-beginning \
  --max-messages 5

# 查看窗口聚合特征
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic dws_user_item_feature \
  --from-beginning \
  --max-messages 5

# 查看用户实时特征
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic dws_user_feature \
  --from-beginning \
  --max-messages 5

# 查看商品实时特征
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic dws_item_feature \
  --from-beginning \
  --max-messages 5
```

### 查询 Redis 维度数据

```bash
docker exec redis redis-cli hgetall "dim:user:U000001"
docker exec redis redis-cli hgetall "dim:item:I0000001"
```

### 查询 SQLite 维度数据

```bash
sqlite3 data/dim.db "SELECT * FROM dim_user LIMIT 5;"
sqlite3 data/dim.db "SELECT * FROM dim_item LIMIT 5;"
```

---

## 项目结构

```
real-time-demo/
├── docker-compose.yml               # 基础设施编排
├── pom.xml                          # 父 POM
├── scripts/
│   ├── requirements.txt             # Python 依赖
│   ├── init_dim_data.py             # 初始化维度数据
│   ├── mock_producer.py             # Mock 行为事件生产者
│   └── create_topics.sh             # 创建 Kafka Topics
├── data/
│   └── dim.db                       # SQLite（运行时生成）
└── flink-jobs/
    ├── pom.xml
    └── src/main/java/com/demo/
        ├── common/
        │   ├── model/               # POJO 数据模型
        │   ├── serde/               # JSON 序列化工具
        │   └── config/              # Kafka 配置常量
        ├── job1/DimJoinJob.java     # AsyncIO + Redis 维度拼接
        ├── job2/WindowAggJob.java   # 滚动窗口聚合特征
        └── job3/RealtimeFeatureJob.java  # KeyedState 实时指标
```

## 停止环境

```bash
docker compose down
```
