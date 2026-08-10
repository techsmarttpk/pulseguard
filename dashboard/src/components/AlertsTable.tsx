import type { Alert } from "../types";

function formatTime(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleTimeString(undefined, { hour12: false });
}

export function AlertsTable({ alerts }: { alerts: Alert[] }) {
  return (
    <div className="panel col-12">
      <h2>Live Alerts</h2>
      {alerts.length === 0 ? (
        <div className="empty-state">No alerts yet — feed is healthy.</div>
      ) : (
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Severity</th>
              <th>Symbol / Feed</th>
              <th>Alert</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {alerts.map((a) => (
              <tr key={a.alert_id}>
                <td>{formatTime(a.created_at)}</td>
                <td>
                  <span className={`severity-badge severity-${a.severity}`}>{a.severity}</span>
                </td>
                <td>{a.symbol || a.feed}</td>
                <td>
                  <div style={{ fontWeight: 600 }}>{a.alert_type.replace(/_/g, " ")}</div>
                  <div className="feed-meta">{a.description}</div>
                </td>
                <td>
                  <span className={`status-pill status-${a.status}`}>{a.status}</span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
