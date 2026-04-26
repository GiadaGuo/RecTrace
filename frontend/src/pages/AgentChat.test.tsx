import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import AgentChat from './AgentChat'

// ── Helpers ────────────────────────────────────────────────────────────────────

/** Create a mock SSE stream from an array of data payloads */
function createMockSSEStream(chunks: string[]) {
  const encoder = new TextEncoder()
  const lines = chunks.map(c => `data: ${c}\n\n`).join('') + 'data: [DONE]\n\n'
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(encoder.encode(lines))
      controller.close()
    },
  })
  return stream
}

function mockFetchWithSSE(chunks: string[]) {
  const stream = createMockSSEStream(chunks)
  return vi.fn().mockResolvedValue({
    ok: true,
    body: stream,
    status: 200,
  })
}

// ── Tests ──────────────────────────────────────────────────────────────────────

describe('AgentChat', () => {
  beforeEach(() => {
    vi.restoreAllMocks()
  })

  it('renders empty chat state with placeholder', () => {
    render(<AgentChat />)
    expect(screen.getByText(/向 Agent 提问/)).toBeInTheDocument()
  })

  it('renders quick question buttons when no messages', () => {
    render(<AgentChat />)
    expect(screen.getByText('为什么推荐了 item_0472？')).toBeInTheDocument()
  })

  it('sends a message and displays user bubble', async () => {
    const user = userEvent.setup()
    const mockFetch = mockFetchWithSSE(['你好！'])
    vi.stubGlobal('fetch', mockFetch)

    render(<AgentChat />)
    const input = screen.getByPlaceholderText(/问推荐结果/)
    const sendBtn = screen.getByText('发送')

    await user.type(input, '测试消息')
    await user.click(sendBtn)

    expect(screen.getByText('测试消息')).toBeInTheDocument()
    expect(screen.getByText('you')).toBeInTheDocument()
  })

  it('receives SSE chunks and updates AI bubble', async () => {
    const user = userEvent.setup()
    const mockFetch = mockFetchWithSSE(['你好', '，这是', '测试回复'])
    vi.stubGlobal('fetch', mockFetch)

    render(<AgentChat />)
    const input = screen.getByPlaceholderText(/问推荐结果/)

    await user.type(input, 'hello{Enter}')

    await waitFor(() => {
      expect(screen.getByText(/你好，这是测试回复/)).toBeInTheDocument()
    })
  })

  it('shows loading indicator while streaming', async () => {
    const user = userEvent.setup()
    // Create a stream that delays
    const encoder = new TextEncoder()
    let controllerRef: ReadableStreamDefaultController | null = null
    const stream = new ReadableStream({
      start(ctrl) {
        controllerRef = ctrl
      },
    })
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      body: stream,
      status: 200,
    })
    vi.stubGlobal('fetch', mockFetch)

    render(<AgentChat />)
    const input = screen.getByPlaceholderText(/问推荐结果/)

    await user.type(input, 'test{Enter}')

    // The button should show "思考中…"
    expect(screen.getByText('思考中…')).toBeInTheDocument()

    // Complete the stream
    controllerRef!.enqueue(encoder.encode('data: 回复\n\ndata: [DONE]\n\n'))
    controllerRef!.close()

    await waitFor(() => {
      expect(screen.getByText('回复')).toBeInTheDocument()
    })
  })

  it('handles Enter key to send', async () => {
    const user = userEvent.setup()
    const mockFetch = mockFetchWithSSE(['ok'])
    vi.stubGlobal('fetch', mockFetch)

    render(<AgentChat />)
    const input = screen.getByPlaceholderText(/问推荐结果/)

    await user.type(input, '按回车发送{Enter}')

    expect(screen.getByText('按回车发送')).toBeInTheDocument()
  })

  it('shows error message on fetch failure', async () => {
    const user = userEvent.setup()
    const mockFetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 500,
    })
    vi.stubGlobal('fetch', mockFetch)

    render(<AgentChat />)
    const input = screen.getByPlaceholderText(/问推荐结果/)

    await user.type(input, 'error test{Enter}')

    await waitFor(() => {
      expect(screen.getByText(/Agent 请求失败：500/)).toBeInTheDocument()
    })
  })

  it('quick question button sends message', async () => {
    const user = userEvent.setup()
    const mockFetch = mockFetchWithSSE(['回答'])
    vi.stubGlobal('fetch', mockFetch)

    render(<AgentChat />)
    const btn = screen.getByText('为什么推荐了 item_0472？')

    await user.click(btn)

    expect(screen.getByText('为什么推荐了 item_0472？')).toBeInTheDocument()
  })
})
