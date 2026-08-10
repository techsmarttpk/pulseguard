import type { FeedSummary } from "../types";

function timeAgo(iso: string | null): string {
  if (!iso) return "—";
  const seconds = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
  if (seconds < 60) return `${Math.round(seconds)}s ago`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m ago`;
  return `${Math.round(seconds / 3600)}h ago`;
}

export function FeedHealth({ feeds }: { feeds: FeedSummary[] }) {
  return (
    <div className="panel col-6">
      <h2>Feed Health</h2>
      {feeds.length === 0 && <div className="empty-state">No feeds reporting yet.</div>}
      {feeds.map((f) => (
        <div className="feed-row" key={f.feed}>
          <div>
            <span className={`dot state-${f.state}`} style={{ display: "inline-block", marginRight: 8 }} />
            <span className="feed-name">{f.feed}</span>
            <span className={`state-${f.state}`} style={{ marginLeft: 10, fontWeight: 600, fontSize: 12 }}>
              {f.state}
            </span>
          </div>
          <div style={{ textAlign: "right" }}>
            <div>{f.messages_per_second.toFixed(0)} msg/s</div>
            <div className="feed-meta">
              p99 {(f.p99_latency_seconds * 1000).toFixed(0)}ms · {timeAgo(f.since)}
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}
