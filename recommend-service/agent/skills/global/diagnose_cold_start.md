---
name: diagnose_cold_start
description: 诊断冷启动用户推荐效果差的完整排查流程
tags: [cold_start, diagnose, user_features]
version: 1
---

## 适用场景
用户反馈新注册或长时间未访问后推荐质量差，不够个性化。

## 诊断步骤

### 1. 检查用户特征稀疏程度
```
get_user_features(uid=<目标用户ID>)
```
关注：
- 特征字段数量是否极少（< 5个字段）
- 统计类特征（点击次数、购买次数）是否为 0 或 null
- update_ts 是否缺失

### 2. 检查行为序列长度
```
get_user_sequence(uid=<目标用户ID>)
```
关注：
- sequence 列表长度，冷启动用户通常 < 5 条
- 若为空，行为特征全部退化到全局均值

### 3. 检查特征新鲜度
```
get_feature_freshness(uid=<目标用户ID>)
```
- 若 update_ts 缺失，表示 Flink Job3 从未处理该用户

### 4. 诊断结论
| 特征 | 序列 | 结论 |
|------|------|------|
| 稀疏 | 为空 | 典型冷启动，推荐退化到热门/兜底策略 |
| 稀疏 | 有序列 | 特征计算滞后，等待 Job3 更新 |
| 正常 | 有序列 | 非冷启动，排查其他问题 |

## 解决建议
- 冷启动：增加新用户引导问卷，提升初始偏好质量
- 计算滞后：检查 get_pipeline_health 确认 Flink 任务健康
