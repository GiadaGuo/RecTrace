export interface RecommendRequest {
  uid: string
  top_k: number
}

export interface FeatureContributions {
  [key: string]: number
}

export interface RecommendItem {
  item_id: string
  score: number
  feature_contributions: FeatureContributions
  item_brand?: string
  item_price?: number
  category_id?: number
}

export interface RecommendResponse {
  req_id: string
  uid: string
  items: RecommendItem[]
}

export interface InjectRequest {
  uid: string
  item_id: string
  bhv_type: string
}

export async function fetchRecommend(req: RecommendRequest): Promise<RecommendResponse> {
  const res = await fetch('/recommend', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(req),
  })
  if (!res.ok) {
    throw new Error(`推荐接口请求失败：${res.status}`)
  }
  return res.json() as Promise<RecommendResponse>
}

export async function injectBehavior(req: InjectRequest): Promise<void> {
  const res = await fetch('/recommend/inject', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(req),
  })
  if (!res.ok) {
    throw new Error(`注入行为请求失败：${res.status}`)
  }
}

// ── Agent Chat API (SSE streaming) ────────────────────────────────────────────

export interface ChatRequest {
  message: string
  session_id?: string
}

export interface LineageResponse {
  uid: string
  item_id: string
  req_id?: string
  ts?: string
  seq_items: string[]
  contributing_features: Record<string, number>
}

/**
 * Stream agent chat via SSE.
 * Calls onChunk for each text chunk received, resolves when [DONE] is received.
 */
export async function streamAgentMessage(
  req: ChatRequest,
  onChunk: (chunk: string) => void,
): Promise<void> {
  const res = await fetch('/agent/chat', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(req),
  })
  if (!res.ok) {
    throw new Error(`Agent 请求失败：${res.status}`)
  }
  if (!res.body) {
    throw new Error('浏览器不支持 ReadableStream')
  }

  const reader = res.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''

  while (true) {
    const { done, value } = await reader.read()
    if (done) break

    buffer += decoder.decode(value, { stream: true })
    const lines = buffer.split('\n')
    // Keep the last incomplete line in buffer
    buffer = lines.pop() ?? ''

    for (const line of lines) {
      const trimmed = line.trim()
      if (!trimmed.startsWith('data: ')) continue
      const payload = trimmed.slice(6) // strip "data: "
      if (payload === '[DONE]') return
      onChunk(payload)
    }
  }
}

export async function fetchLineage(uid: string, itemId: string): Promise<LineageResponse> {
  const res = await fetch(`/lineage/${encodeURIComponent(uid)}/${encodeURIComponent(itemId)}`)
  if (!res.ok) {
    throw new Error(`溯源查询失败：${res.status}`)
  }
  return res.json() as Promise<LineageResponse>
}
