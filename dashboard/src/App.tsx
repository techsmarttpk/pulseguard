import { useEffect, useRef, useState } from "react";
import { api } from "./api";
import { AlertsTable } from "./components/AlertsTable";
import { AnomalyChart, LatencyChart, ThroughputChart } from "./components/Charts";
import { FeedHealth } from "./components/FeedHealth";
import { SystemMetrics } from "./components/SystemMetrics";
import type { Alert, FeedSummary, MetricsSnapshot, SystemStatus, TimePoint } from "./types";

const POLL_MS = 4000;
const MAX_POINTS = 60;

export default function App() {
  const [feeds, setFeeds] = useState<FeedSummary[]>([]);
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [metrics, setMetrics] = useState<MetricsSnapshot | null>(null);
  const [status, setStatus] = useState<SystemStatus | null>(null);
  const [history, setHistory] = useState<TimePoint[]>([]);
  const [error, setError] = useState<string | null>(null);
  const timer = useRef<number | null>(null);

  async function refresh() {
    try {
      const [feedsRes, alertsRes, metricsRes, statusRes] = await Promise.all([
        api.getFeeds(),
        api.getAlerts(30),
        api.getMetrics(),
        api.getStatus(),
      ]);
      setFeeds(feedsRes);
      setAlerts(alertsRes);
      setMetrics(metricsRes);
      setStatus(statusRes);
      setError(null);

      setHistory((prev) => {
        const point: TimePoint = {
          t: Date.now(),
          messages_per_second: metricsRes.messages_per_second_total,
          p95_latency_ms: metricsRes.p95_latency_seconds * 1000,
          p99_latency_ms: metricsRes.p99_latency_seconds * 1000,
          anomalies_5m: metricsRes.active_anomalies_last_5m,
        };
        const next = [...prev, point];
        return next.length > MAX_POINTS ? next.slice(next.length - MAX_POINTS) : next;
      });
    } catch (e) {
      setError(
        `Could not reach the PulseGuard API. Is it running? (${(e as Error).message})`
      );
    }
  }

  useEffect(() => {
    refresh();
    timer.current = window.setInterval(refresh, POLL_MS);
    return () => {
      if (timer.current) window.clearInterval(timer.current);
    };
  }, []);

  return (
    <div className="app">
      <header className="app-header">
        <div>
          <h1>PulseGuard</h1>
          <div className="subtitle">Real-time market data reliability &amp; anomaly detection</div>
        </div>
        {status && (
          <div className={`status-chip state-${status.overall_state}`}>
            <span className={`dot state-${status.overall_state}`} />
            System {status.overall_state}
          </div>
        )}
      </header>

      {error && <div className="error-banner">{error}</div>}

      <div className="grid">
        <FeedHealth feeds={feeds} />
        <SystemMetrics metrics={metrics} />
        <ThroughputChart data={history} />
        <LatencyChart data={history} />
        <AnomalyChart data={history} />
        <AlertsTable alerts={alerts} />
      </div>
    </div>
  );
}
