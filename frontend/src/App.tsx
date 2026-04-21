import { useState } from 'react'
import Playground from './pages/Playground'

type PageKey = 'welcome' | 'pipeline' | 'playground' | 'agent'

const NAV_ITEMS: { key: PageKey; label: string }[] = [
  { key: 'welcome', label: 'Welcome' },
  { key: 'pipeline', label: 'Pipeline monitor' },
  { key: 'playground', label: 'Rec playground' },
  { key: 'agent', label: 'Agent' },
]

export default function App() {
  const [page, setPage] = useState<PageKey>('welcome')

  return (
    <div style={{ padding: '1rem 0' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.25rem' }}>
        <div>
          <span style={{ fontSize: 16, fontWeight: 500, color: 'var(--color-text-primary)' }}>RecTrace Playground</span>
          <span className="tag tag-live" style={{ marginLeft: 8 }}>● live</span>
        </div>
        <span style={{ fontSize: 12, color: 'var(--color-text-tertiary)' }}>Flink + LLM Ranking + Agent</span>
      </div>

      <nav className="nav">
        {NAV_ITEMS.map(item => (
          <button
            key={item.key}
            className={`nav-btn${page === item.key ? ' active' : ''}`}
            onClick={() => setPage(item.key)}
          >
            {item.label}
          </button>
        ))}
      </nav>

      {page === 'welcome' && (
        <div style={{ padding: '2rem', textAlign: 'center', color: 'var(--color-text-tertiary)', fontSize: 13 }}>
          欢迎使用 RecTrace Playground，请选择上方标签页开始使用
        </div>
      )}
      {page === 'playground' && <Playground />}
      {(page === 'pipeline' || page === 'agent') && (
        <div style={{ padding: '2rem', textAlign: 'center', color: 'var(--color-text-tertiary)', fontSize: 13 }}>
          该页面尚未实现
        </div>
      )}
    </div>
  )
}
