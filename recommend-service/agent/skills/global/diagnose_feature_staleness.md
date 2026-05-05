---
name: diagnose_feature_staleness
description: 诊断用户特征过期导致推荐质量下降的排查流程
tags: [feature_freshness, staleness, pipeline, health]
version: 1
---

## 适用场景
推荐结果与用户近期行为不符，用户反馈"刚买过的东西还在推"或推荐偏好未更新。

## 诊断步骤

### 1. 检查特征新鲜度
```
get_feature_freshness(uid=<目标用户ID>)
```
- `age_seconds > 600` → 特征已过期（stale）
- `update_ts = null` → 该用户从未被特征计算 Job 处理

### 2. 检查实时链路健康
```
get_pipeline_health()
```
关注 Flink 任务状态：
- `state = RUNNING`：任务正常，等待下次特征更新
- `state = FAILED`：任务挂了，需要重启 Flink Job
- `state = FAILING`：任务异常中，需要查看 Flink Dashboard

### 3. 确认特征计算 Job 状态
Flink Job 清单（任务名映射）：
| Job 名称 | 职责 |
|---------|------|
| UserFeatureFunction | 用户实时统计特征（Job3） |
| SequenceFeatureJob | 用户行为序列特征（Job4） |

### 4. 查看最近一次推荐快照（对比特征版本）
若有 req_id：
```
get_rec_snapshot(req_id=<请求ID>)
```
对比 snapshot 中的 user_features.update_ts 与当前 Redis 中 update_ts

## 解决建议
- Flink 任务 FAILED：检查 `/jobs/overview` 中 failure cause，重启任务
- 特征轻微滞后（< 5 分钟）：属于正常传输延迟，可忽略
- 特征严重滞后（> 30 分钟）：检查 Kafka topic 积压，排查消息消费瓶颈
