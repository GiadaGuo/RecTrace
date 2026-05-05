---
name: system_health_check
description: 推荐系统全链路健康巡检标准流程
tags: [health, pipeline, flink, feature_freshness, monitoring]
version: 1
---

## 适用场景
定期巡检、告警响应、上线前确认推荐系统整体运行状态。

## 巡检步骤

### 1. 检查 Flink 实时任务状态
```
get_pipeline_health()
```
期望结果：
- `flink.running == flink.total_jobs`（所有任务运行中）
- `flink.failed == 0`

若有 FAILED 任务：
- 查看任务名，定位是 Job1（数据收集）、Job3（特征计算）还是 Job4（序列特征）
- 通过 Flink Dashboard（默认 http://localhost:8081）查看详细错误日志

### 2. 抽查代表性用户特征新鲜度
选取活跃用户和近期新用户各一个：
```
get_feature_freshness(uid=U000001)
get_feature_freshness(uid=<新注册用户ID>)
```
- 活跃用户特征 age_seconds 应 < 300（5分钟内更新）
- 新用户首次特征更新延迟不应超过 10 分钟

### 3. 抽查最近推荐请求快照
找一个最近 5 分钟内的 req_id：
```
get_rec_snapshot(req_id=<最近请求ID>)
```
确认：
- candidates 列表不为空（召回正常）
- scores 不全为 0（精排正常）
- user_features 不为空（特征注入正常）

### 4. 快速综合诊断（有 req_id 时优先使用）
```
diagnose_recommendation_chain(req_id=<请求ID>)
```
一键获取快照 + 溯源 + 特征时效 + 链路健康完整报告。

## 健康状态评分参考
| 检查项 | 绿色 | 黄色 | 红色 |
|--------|------|------|------|
| Flink 任务 | 全部 RUNNING | 1个 FAILING | 有 FAILED |
| 特征新鲜度 | < 5 分钟 | 5-30 分钟 | > 30 分钟 |
| 快照完整性 | candidates > 10 | candidates 1-10 | candidates 为空 |
