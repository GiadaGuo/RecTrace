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
