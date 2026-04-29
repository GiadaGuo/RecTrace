"""
prompts.py
----------
Centralised prompt templates and user-facing message strings for the agent
module.  All LLM prompts, SSE progress templates, and error messages that
were previously hard-coded in orchestrator.py and routers/agent.py are
collected here for easier maintenance and future extension.
"""

# ── LLM system prompt ────────────────────────────────────────────────────────

SYSTEM_PROMPT = """你是一个推荐系统的智能分析助手。当用户询问用户特征、行为序列、推荐结果或数据链路时，你**必须**调用相应工具获取实时数据，不能凭借自身知识回答：
- get_user_features：获取用户的实时特征（当问题涉及用户特征、画像时调用）
- get_user_sequence：获取用户的点击行为序列（当问题涉及用户行为时调用）
- get_rec_snapshot：查看某次推荐请求的快照数据（需要 req_id）
- get_feature_contributions：查看某商品的特征贡献分（需要 req_id + item_id）
- run_lineage_trace：追溯某商品被推荐的完整决策路径

请根据用户问题主动调用工具，并用中文给出完整回答。"""

# ── SSE progress templates (used by orchestrator.stream) ─────────────────────

TOOL_CALL_PROGRESS_TEMPLATE = "> 🔧 调用工具 `{tool_name}({args_preview})`\n"

TOOL_RESULT_HEADER_TEMPLATE = "> **工具 `{name}` 返回数据：**\n"

TOOL_RESULT_ITEM_TEMPLATE = "> - `{k}`: {v}"

TOOL_RESULT_FALLBACK_TEMPLATE = "> 工具 `{name}` 返回：{content}\n"

# ── Error / fallback messages ────────────────────────────────────────────────

LLM_ERROR_TEMPLATE = "抱歉，模型调用出现异常：{e}"

RECURSION_LIMIT_MSG = "抱歉，推理轮次已达上限，请尝试简化问题。"

NO_CONTENT_MSG = "抱歉，未能获取到完整回答，请重试。"
