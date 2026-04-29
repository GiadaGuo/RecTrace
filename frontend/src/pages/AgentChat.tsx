import { useState, useRef, useCallback } from 'react'
import ReactMarkdown from 'react-markdown'
import { streamAgentMessage, fetchLineage, type LineageResponse } from '../api/recommend'

// ── Types ──────────────────────────────────────────────────────────────────────

interface ChatMessage {
  id: string
  role: 'user' | 'ai'
  content: string
}

interface Segment {
  type: 'tool' | 'text'
  text: string
}

/**
 * Split message content into tool-call progress blocks and normal text.
 * Tool blocks are consecutive blockquote lines starting with "> 🔧" or "> **工具".
 */
function parseSegments(content: string): Segment[] {
  if (!content) return []
  const lines = content.split('\n')
  const segments: Segment[] = []
  let currentType: 'tool' | 'text' | null = null
  let currentLines: string[] = []

  const isToolLine = (line: string) =>
    line.startsWith('> 🔧') || line.startsWith('> **工具')

  const flush = () => {
    if (currentType && currentLines.length > 0) {
      segments.push({ type: currentType, text: currentLines.join('\n') })
    }
    currentLines = []
  }

  for (const line of lines) {
    const toolLine = isToolLine(line)
    const lineType: 'tool' | 'text' = toolLine ? 'tool' : 'text'

    if (currentType !== lineType) {
      flush()
      currentType = lineType
    }
    currentLines.push(line)
  }
  flush()

  return segments
}

// ── Quick question presets ──────────────────────────────────────────────────────

const QUICK_QUESTIONS = [
  '为什么推荐了 item_0472？',
  '用户 U000001 的特征是什么？',
  '最近的推荐链路有异常吗？',
]

// ── Component ──────────────────────────────────────────────────────────────────

let _msgId = 0
function nextId(): string {
  return `msg_${++_msgId}`
}

export default function AgentChat() {
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [lineageData, setLineageData] = useState<LineageResponse | null>(null)
  const chatEndRef = useRef<HTMLDivElement>(null)

  const scrollToBottom = useCallback(() => {
    chatEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [])

  async function handleSend(overrideText?: string) {
    const text = (overrideText ?? input).trim()
    if (!text || loading) return

    setInput('')
    const userMsg: ChatMessage = { id: nextId(), role: 'user', content: text }
    const aiMsg: ChatMessage = { id: nextId(), role: 'ai', content: '' }

    setMessages(prev => [...prev, userMsg, aiMsg])
    setLoading(true)
    setLineageData(null)

    // Scroll after user message is added
    setTimeout(scrollToBottom, 0)

    try {
      await streamAgentMessage(
        { message: text },
        (chunk) => {
          setMessages(prev =>
            prev.map(m => (m.id === aiMsg.id ? { ...m, content: m.content + chunk } : m)),
          )
          setTimeout(scrollToBottom, 0)
        },
      )
    } catch (e) {
      setMessages(prev =>
        prev.map(m =>
          m.id === aiMsg.id
            ? { ...m, content: m.content || `请求失败：${e instanceof Error ? e.message : '未知错误'}` }
            : m,
        ),
      )
    } finally {
      setLoading(false)
    }
  }

  function handleKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSend()
    }
  }

  async function handleLineage() {
    // Try to extract uid and item_id from the last AI message
    const lastAi = [...messages].reverse().find(m => m.role === 'ai')
    const uidMatch = lastAi?.content.match(/[Uu]\d{6,}/)
    const itemMatch = lastAi?.content.match(/item[,_]\s?(\d{4,})/)
    // Simple heuristic: if we can't auto-detect, show hint
    const uid = uidMatch?.[0] ?? 'U000001'
    const itemId = itemMatch?.[1] ?? '0472'

    try {
      const result = await fetchLineage(uid, `I0000${itemId}`)
      setLineageData(result)
    } catch {
      setLineageData(null)
    }
  }

  return (
    <div>
      <div className="chat-wrap">
        {/* Chat body */}
        <div className="chat-body">
          {messages.length === 0 && (
            <div style={{ textAlign: 'center', color: 'var(--color-text-tertiary)', fontSize: 13, padding: '3rem 1rem' }}>
              向 Agent 提问推荐结果或数据链路相关问题
            </div>
          )}

          {messages.map(msg => (
            <div key={msg.id} className={`msg ${msg.role === 'user' ? 'msg-user' : 'msg-ai'}`}>
              <div className="msg-label">{msg.role === 'user' ? 'you' : 'agent'}</div>
              <div className="msg-bubble">
                {msg.role === 'user' ? (msg.content) : !msg.content && loading ? '...' : parseSegments(msg.content).map((seg, i) =>
                  seg.type === 'tool'
                    ? <details key={i} className="tool-block" open><summary>工具调用</summary>
                        <ReactMarkdown>{seg.text}</ReactMarkdown>
                      </details>
                    : <ReactMarkdown key={i}>{seg.text}</ReactMarkdown>
                )}
              </div>
            </div>
          ))}

          <div ref={chatEndRef} />
        </div>

        {/* Input row */}
        <div className="chat-input-row">
          <input
            type="text"
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="问推荐结果解释、特征归因、链路异常…"
            disabled={loading}
          />
          <button
            onClick={() => handleSend()}
            disabled={loading || !input.trim()}
            style={{
              fontSize: 13,
              padding: '5px 14px',
              borderRadius: 'var(--border-radius-md)',
              border: '0.5px solid var(--color-border-secondary)',
              background: loading || !input.trim() ? 'var(--color-background-secondary)' : '#E1F5EE',
              color: loading || !input.trim() ? 'var(--color-text-tertiary)' : '#085041',
              fontWeight: 500,
              whiteSpace: 'nowrap',
            }}
          >
            {loading ? '思考中…' : '发送'}
          </button>
        </div>
      </div>

      {/* Quick questions */}
      {messages.length === 0 && (
        <div style={{ display: 'flex', gap: 6, marginTop: 10, flexWrap: 'wrap' }}>
          {QUICK_QUESTIONS.map(q => (
            <button
              key={q}
              onClick={() => handleSend(q)}
              style={{
                fontSize: 12,
                padding: '4px 10px',
                borderRadius: 'var(--border-radius-md)',
                border: '0.5px solid var(--color-border-tertiary)',
                background: 'var(--color-background-secondary)',
                color: 'var(--color-text-secondary)',
              }}
            >
              {q}
            </button>
          ))}
        </div>
      )}

      {/* Lineage result */}
      {lineageData && (
        <div style={{ marginTop: 12 }}>
          <div className="section-label">溯源结果</div>
          <div className="card lineage-card">
            <div><strong>用户：</strong>{lineageData.uid}</div>
            <div><strong>商品：</strong>{lineageData.item_id}</div>
            {lineageData.req_id && <div><strong>请求 ID：</strong>{lineageData.req_id}</div>}
            {lineageData.ts && <div><strong>时间：</strong>{lineageData.ts}</div>}
            {lineageData.seq_items.length > 0 && (
              <div style={{ marginTop: 6 }}>
                <strong>相关行为序列：</strong>
                <div style={{ fontFamily: 'var(--font-mono)', fontSize: 11, color: 'var(--color-text-tertiary)', marginTop: 2 }}>
                  [{lineageData.seq_items.join(', ')}]
                </div>
              </div>
            )}
            {Object.keys(lineageData.contributing_features).length > 0 && (
              <div style={{ marginTop: 6 }}>
                <strong>特征贡献：</strong>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 4, marginTop: 4 }}>
                  {Object.entries(lineageData.contributing_features).map(([k, v]) => (
                    <div key={k} style={{ fontSize: 12 }}>
                      <span style={{ color: 'var(--color-text-secondary)' }}>{k}</span>{' '}
                      <span style={{ fontWeight: 500 }}>{(v * 100).toFixed(1)}%</span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Trace button */}
      {messages.length > 0 && !loading && (
        <div style={{ marginTop: 8 }}>
          <button
            onClick={handleLineage}
            style={{
              fontSize: 12,
              padding: '4px 12px',
              borderRadius: 'var(--border-radius-md)',
              border: '0.5px solid var(--color-border-tertiary)',
              background: 'var(--color-background-secondary)',
              color: 'var(--color-text-secondary)',
            }}
          >
            查看最近溯源
          </button>
        </div>
      )}
    </div>
  )
}
