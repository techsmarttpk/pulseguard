import type { MetricsSnapshot } from "../types";

export function SystemMetrics({ metrics }: { metrics: MetricsSnapshot | null }) {
  const tiles = [
    { label: "Messages/sec", value: metrics ? metrics.messages_per_second_total.toFixed(0) : "—" },
    { label: "P95 latency", value: metrics ? `${(metrics.p95_latency_seconds * 1000).toFixed(0)}ms` : "—" },
    { label: "P99 latency", value: metrics ? `${(metrics.p99_latency_seconds * 1000).toFixed(0)}ms` : "—" },
    { label: "Active anomalies (5m)", value: metrics ? metrics.active_anomalies_last_5m : "—" },
    { label: "Consumer lag", value: metrics ? metrics.consumer_lag_total.toFixed(0) : "—" },
    { label: "Rejected/sec", value: metrics ? metrics.rejected_rate_per_second.toFixed(1) : "—" },
  ];

  return (
    <div className="panel col-6">
      <h2>System Metrics</h2>
      <div className="metric-tiles">
        {tiles.map((t) => (
          <div className="metric-tile" key={t.label}>
            <div className="value">{t.value}</div>
            <div className="label">{t.label}</div>
          </div>
        ))}
      </div>
    </div>
  );
}
