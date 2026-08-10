export type FeedState = "HEALTHY" | "DEGRADED" | "STALE" | "OFFLINE";

export interface FeedSummary {
  feed: string;
  state: FeedState;
  reason: string | null;
  since: string | null;
  messages_per_second: number;
  p95_latency_seconds: number;
  p99_latency_seconds: number;
}

export interface Alert {
  alert_id: string;
  created_at: string;
  resolved_at: string | null;
  severity: "CRITICAL" | "HIGH" | "MEDIUM" | "LOW";
  alert_type: string;
  feed: string;
  symbol: string | null;
  description: string;
  metrics: Record<string, unknown>;
  detection_source: string;
  status: "ACTIVE" | "RESOLVED";
}

export interface Anomaly {
  anomaly_id: string;
  detected_at: string;
  symbol: string;
  detection_method: string;
  severity: string;
  anomaly_score: number;
  description: string;
  metrics: Record<string, unknown>;
  event_id: string | null;
}

export interface MetricsSnapshot {
  messages_per_second_total: number;
  p50_latency_seconds: number;
  p95_latency_seconds: number;
  p99_latency_seconds: number;
  active_anomalies_last_5m: number;
  consumer_lag_total: number;
  rejected_rate_per_second: number;
}

export interface SystemStatus {
  overall_state: FeedState;
  feeds_healthy: number;
  feeds_degraded: number;
  feeds_stale: number;
  feeds_offline: number;
  active_alerts_by_severity: Record<string, number>;
}

export interface TimePoint {
  t: number;
  messages_per_second: number;
  p95_latency_ms: number;
  p99_latency_ms: number;
  anomalies_5m: number;
}
