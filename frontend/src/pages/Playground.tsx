import { useState, useRef } from 'react'
import {
  fetchRecommend,
  injectBehavior,
  type RecommendItem,
  type FeatureContributions,
} from '../api/recommend'

// feature_contributions 字段的中文标签映射
const FEAT_LABEL: Record<string, string> = {
  conversion_rate: '转化率',
  cross_click_cnt: '品类点击兴趣',
  cross_buy_cnt: '品类购买意图',
  in_click_seq: '近期点击命中',
  item_click_1h: '近1小时热度',
  behavior_seq_attention: '序列注意力',
  category_match: '类目匹配',
  price_preference: '价格偏好',
  user_level: '用户等级',
}

function getFeatureLabel(key: string): string {
  return FEAT_LABEL[key] ?? key
}

/** 计算贡献值最高的 key */
function maxKey(contributions: FeatureContributions): string {
  return Object.entries(contributions).reduce(
    (best, [k, v]) => (v > best[1] ? [k, v] : best),
    ['', -Infinity]
  )[0]
}

// -------------------------------------------------------
// 特征贡献水平条组件
// -------------------------------------------------------
function FeatureBars({ contributions }: { contributions: FeatureContributions }) {
  const topKey = maxKey(contributions)
  return (
    <div style={{ marginTop: 8 }}>
      {Object.entries(contributions).map(([key, val]) => (
        <div key={key} style={{ marginBottom: 4 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, color: 'var(--color-text-tertiary)' }}>
            <span>{getFeatureLabel(key)}</span>
            <span>{(val * 100).toFixed(1)}%</span>
          </div>
          <div className="bar-wrap">
            <div
              className="bar"
              style={{
                width: `${Math.min(val * 100, 100)}%`,
                background: key === topKey ? '#1D9E75' : 'var(--color-text-tertiary)',
              }}
            />
          </div>
        </div>
      ))}
    </div>
  )
}

// -------------------------------------------------------
// 注入行为按钮组件
// -------------------------------------------------------
const BHV_TYPES = ['click', 'purchase', 'expose'] as const
type BhvType = typeof BHV_TYPES[number]

function InjectButton({
  uid,
  itemId,
  onInjected,
}: {
  uid: string
  itemId: string
  onInjected: () => void
}) {
  const [bhvType, setBhvType] = useState<BhvType>('click')
  const [loading, setLoading] = useState(false)
  const [status, setStatus] = useState<'idle' | 'ok' | 'err'>('idle')

  async function handleInject() {
    setLoading(true)
    setStatus('idle')
    try {
      await injectBehavior({ uid, item_id: itemId, bhv_type: bhvType })
      setStatus('ok')
      onInjected()
    } catch {
      setStatus('err')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginTop: 8 }}>
      <select
        value={bhvType}
        onChange={e => setBhvType(e.target.value as BhvType)}
        disabled={loading}
        style={{
          fontSize: 11,
          padding: '2px 6px',
          borderRadius: 'var(--border-radius-md)',
          border: '0.5px solid var(--color-border-tertiary)',
          background: 'var(--color-background-secondary)',
          color: 'var(--color-text-secondary)',
        }}
      >
        {BHV_TYPES.map(t => (
          <option key={t} value={t}>{t}</option>
        ))}
      </select>
      <button
        onClick={handleInject}
        disabled={loading}
        style={{
          fontSize: 11,
          padding: '2px 10px',
          borderRadius: 'var(--border-radius-md)',
          border: '0.5px solid var(--color-border-tertiary)',
          background: loading ? 'var(--color-background-secondary)' : '#E1F5EE',
          color: loading ? 'var(--color-text-tertiary)' : '#085041',
          fontWeight: 500,
        }}
      >
        {loading ? '注入中…' : '注入行为'}
      </button>
      {status === 'ok' && (
        <span style={{ fontSize: 11, color: '#085041' }}>已注入，排序刷新中…</span>
      )}
      {status === 'err' && (
        <span style={{ fontSize: 11, color: '#c0392b' }}>注入失败</span>
      )}
    </div>
  )
}

// -------------------------------------------------------
// 单个排名条目
// -------------------------------------------------------
function RankItem({
  item,
  rank,
  uid,
  onInjected,
}: {
  item: RecommendItem
  rank: number
  uid: string
  onInjected: () => void
}) {
  const [expanded, setExpanded] = useState(false)
  const scorePercent = Math.min(item.score * 100, 100)
  const barColor = item.score >= 0.7 ? '#1D9E75' : '#BA7517'

  return (
    <div className="rank-item" style={{ flexDirection: 'column', gap: 0 }}>
      <div style={{ display: 'flex', alignItems: 'flex-start', gap: 12, width: '100%' }}>
        <div className="rank-num">{rank}</div>
        <div className="rank-body">
          <div className="rank-title">{item.item_id}</div>
          <div className="rank-meta" style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
            {item.item_brand && <span className="tag tag-teal">{item.item_brand}</span>}
            {item.item_price != null && (
              <span className="tag tag-amber">¥{item.item_price.toFixed(0)}</span>
            )}
            <span style={{ color: 'var(--color-text-tertiary)' }}>{item.item_id}</span>
          </div>
          <div className="bar-wrap">
            <div className="bar" style={{ width: `${scorePercent}%`, background: barColor }} />
          </div>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 4 }}>
          <div className="rank-score">{item.score.toFixed(2)}</div>
          <button
            onClick={() => setExpanded(e => !e)}
            style={{
              fontSize: 11,
              padding: '2px 8px',
              borderRadius: 'var(--border-radius-md)',
              border: '0.5px solid var(--color-border-tertiary)',
              background: 'transparent',
              color: 'var(--color-text-secondary)',
            }}
          >
            {expanded ? '收起' : '特征贡献'}
          </button>
        </div>
      </div>

      {expanded && (
        <div style={{ paddingLeft: 32, width: '100%' }}>
          <FeatureBars contributions={item.feature_contributions} />
          <InjectButton uid={uid} itemId={item.item_id} onInjected={onInjected} />
        </div>
      )}
    </div>
  )
}

// -------------------------------------------------------
// 主页面：Playground
// -------------------------------------------------------
const DEFAULT_UID = 'U000001'
const POLL_INTERVAL_MS = 500
const POLL_MAX_TRIES = 10

export default function Playground() {
  const [uidInput, setUidInput] = useState(DEFAULT_UID)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [items, setItems] = useState<RecommendItem[]>([])
  const [currentUid, setCurrentUid] = useState<string>('')
  const [reqId, setReqId] = useState<string>('')
  const [polling, setPolling] = useState(false)
  const pollTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  async function doFetch(uid: string): Promise<RecommendItem[]> {
    const resp = await fetchRecommend({ uid, top_k: 10 })
    setReqId(resp.req_id)
    setItems(resp.items)
    return resp.items
  }

  async function handleFetch() {
    const uid = uidInput.trim()
    if (!uid) return
    setLoading(true)
    setError(null)
    try {
      await doFetch(uid)
      setCurrentUid(uid)
    } catch (e) {
      setError(e instanceof Error ? e.message : '请求失败')
    } finally {
      setLoading(false)
    }
  }

  function startPolling(uid: string, prevItems: RecommendItem[]) {
    setPolling(true)
    let tries = 0

    function poll() {
      tries++
      fetchRecommend({ uid, top_k: 10 })
        .then(resp => {
          const changed = resp.items.some((item, i) => item.item_id !== prevItems[i]?.item_id)
          setItems(resp.items)
          setReqId(resp.req_id)
          if (changed || tries >= POLL_MAX_TRIES) {
            setPolling(false)
          } else {
            pollTimerRef.current = setTimeout(poll, POLL_INTERVAL_MS)
          }
        })
        .catch(() => setPolling(false))
    }

    pollTimerRef.current = setTimeout(poll, POLL_INTERVAL_MS)
  }

  function handleInjected() {
    if (!currentUid) return
    // 清除已有轮询
    if (pollTimerRef.current) clearTimeout(pollTimerRef.current)
    startPolling(currentUid, items)
  }

  // 用户头像圆中的首字母/数字
  const avatarLetter = currentUid ? currentUid.replace(/\D/g, '').slice(-2) || currentUid[0] : '?'

  return (
    <div style={{ display: 'grid', gridTemplateColumns: '240px 1fr', gap: '2.5rem' }}>
      {/* -------- 左列：用户上下文 -------- */}
      <div>
        {/* uid 输入 */}
        <div style={{ marginBottom: '1rem' }}>
          <div className="section-label">用户 ID</div>
          <div style={{ display: 'flex', gap: 6 }}>
            <input
              type="text"
              value={uidInput}
              onChange={e => setUidInput(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && handleFetch()}
              placeholder="输入 uid，如 U000001"
              style={{
                flex: 1,
                fontSize: 13,
                padding: '5px 10px',
                borderRadius: 'var(--border-radius-md)',
                border: '0.5px solid var(--color-border-secondary)',
                background: 'var(--color-background-primary)',
                color: 'var(--color-text-primary)',
              }}
            />
            <button
              onClick={handleFetch}
              disabled={loading}
              style={{
                fontSize: 13,
                padding: '5px 12px',
                borderRadius: 'var(--border-radius-md)',
                border: '0.5px solid var(--color-border-secondary)',
                background: loading ? 'var(--color-background-secondary)' : '#E1F5EE',
                color: loading ? 'var(--color-text-tertiary)' : '#085041',
                fontWeight: 500,
                whiteSpace: 'nowrap',
              }}
            >
              {loading ? '加载中…' : '获取推荐'}
            </button>
          </div>
          {error && (
            <div style={{ fontSize: 12, color: '#c0392b', marginTop: 6 }}>{error}</div>
          )}
        </div>

        {/* 用户卡片 */}
        {currentUid && (
          <>
            <div className="section-label">User context</div>
            <div className="card">
              <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 12 }}>
                <div style={{
                  width: 36, height: 36, borderRadius: '50%',
                  background: '#E1F5EE',
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                  fontWeight: 500, fontSize: 13, color: '#085041',
                }}>
                  {avatarLetter}
                </div>
                <div>
                  <div style={{ fontSize: 14, fontWeight: 500 }}>{currentUid}</div>
                  <div style={{ fontSize: 11, color: 'var(--color-text-tertiary)' }}>
                    结果数：{items.length}
                  </div>
                </div>
              </div>
              <div className="feat-grid">
                <div className="feat-item"><div>结果数</div><div className="feat-val">{items.length}</div></div>
                <div className="feat-item"><div>最高分</div><div className="feat-val">{items[0]?.score.toFixed(2) ?? '-'}</div></div>
                <div className="feat-item"><div>最低分</div><div className="feat-val">{items[items.length - 1]?.score.toFixed(2) ?? '-'}</div></div>
                <div className="feat-item"><div>请求 ID</div><div className="feat-val" style={{ fontSize: 10, fontFamily: 'var(--font-mono)', wordBreak: 'break-all' }}>{reqId.slice(-8)}</div></div>
              </div>
            </div>

            {/* 特征快照 */}
            <div className="section-label" style={{ marginTop: 12 }}>Feature snapshot</div>
            <div className="card" style={{ fontSize: 12 }}>
              <div style={{ marginBottom: 6, color: 'var(--color-text-secondary)' }}>Top-1 特征贡献</div>
              {items[0] && (
                <FeatureBars contributions={items[0].feature_contributions} />
              )}
            </div>
          </>
        )}
      </div>

      {/* -------- 右列：排序列表 -------- */}
      <div>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
          <div className="section-label" style={{ marginBottom: 0 }}>排序结果</div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            {polling && (
              <span style={{ fontSize: 11, color: '#085041' }} className="tag tag-live">排序刷新中…</span>
            )}
            <span style={{ fontSize: 11, color: 'var(--color-text-tertiary)' }}>
              {items.length > 0 ? `共 ${items.length} 条` : '请输入 uid 获取推荐'}
            </span>
          </div>
        </div>

        {items.length === 0 && !loading && (
          <div style={{
            padding: '3rem 2rem',
            textAlign: 'center',
            color: 'var(--color-text-tertiary)',
            fontSize: 13,
            border: '0.5px dashed var(--color-border-tertiary)',
            borderRadius: 'var(--border-radius-lg)',
          }}>
            输入用户 ID 并点击「获取推荐」查看排序结果
          </div>
        )}

        {items.length > 0 && (
          <div className="card" style={{ padding: '0.75rem 1rem' }}>
            {items.map((item, idx) => (
              <RankItem
                key={item.item_id}
                item={item}
                rank={idx + 1}
                uid={currentUid}
                onInjected={handleInjected}
              />
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
