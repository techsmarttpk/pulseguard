import type { Alert, Anomaly, FeedSummary, MetricsSnapshot, SystemStatus } from "./types";

const BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

async function getJson<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`);
  if (!res.ok) {
    throw new Error(`${path} failed: ${res.status}`);
  }
  return (await res.json()) as T;
}

export const api = {
  getFeeds: () => getJson<FeedSummary[]>("/api/feeds"),
  getAlerts: (limit = 50) => getJson<Alert[]>(`/api/alerts?limit=${limit}`),
  getAnomalies: (limit = 50) => getJson<Anomaly[]>(`/api/anomalies?limit=${limit}`),
  getMetrics: () => getJson<MetricsSnapshot>("/api/metrics"),
  getStatus: () => getJson<SystemStatus>("/api/status"),
};
