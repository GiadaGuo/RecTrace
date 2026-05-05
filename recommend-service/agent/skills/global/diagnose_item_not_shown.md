---
name: diagnose_item_not_shown
description: 排查某商品未出现在推荐结果中的原因（过滤链路追溯）
tags: [item_missing, filter, lineage, diagnose]
version: 1
---

## 适用场景
运营/用户反馈某个商品应该被推荐但没出现，需要追溯是在哪个环节被过滤掉的。

## 诊断步骤

### 1. 先获取最近推荐快照
```
get_rec_snapshot(req_id=<最近请求ID>)
```
- 确认 candidates 列表中是否包含目标商品
- 若在 candidates 中：商品已进入精排，查看 scores 确认得分

### 2. 若不在 candidates 中，走溯源链路
```
run_lineage_trace(uid=<用户ID>, item_id=<商品ID>)
```
返回结果分析：
- `found_in_recall = false`：召回阶段未取到此商品（协同过滤/物料库覆盖问题）
- `found_in_recall = true, filtered_at = coarse_rank`：粗排过滤掉了
- `found_in_recall = true, filtered_at = fine_rank`：精排打分后被截断

### 3. 若有 ClickHouse（Step 6 之后），查看详细过滤原因
```
get_ranking_trace(req_id=<请求ID>)
```
查看每个 stage 的 filter_reason 列表

### 4. 查看该用户的历史过滤分布
```
get_filter_reason_stats(uid=<用户ID>)
```
确认是偶发问题还是系统性过滤

## 常见过滤原因及对应处理
| 过滤原因 | 可能原因 | 建议 |
|---------|---------|------|
| out_of_stock | 商品库存为 0 | 检查商品状态，非 Agent 可修复 |
| low_diversity | 相似商品过多 | 多样性控制参数过严 |
| low_score | 精排得分低 | 用户与商品相关性弱 |
| recall_miss | 召回未覆盖 | 协同过滤训练数据问题 |
