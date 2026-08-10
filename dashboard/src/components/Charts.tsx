import {
  Area,
  AreaChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { TimePoint } from "../types";

const axisStyle = { fontSize: 11, fill: "#838d9c" };

function fmtTime(t: number) {
  return new Date(t).toLocaleTimeString(undefined, { hour12: false, minute: "2-digit", second: "2-digit" });
}

export function ThroughputChart({ data }: { data: TimePoint[] }) {
  return (
    <div className="panel col-4">
      <h2>Throughput over time</h2>
      <ResponsiveContainer width="100%" height={180}>
        <AreaChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke="#232a38" />
          <XAxis dataKey="t" tickFormatter={fmtTime} tick={axisStyle} minTickGap={40} />
          <YAxis tick={axisStyle} width={40} />
          <Tooltip
            labelFormatter={(v) => fmtTime(Number(v))}
            contentStyle={{ background: "#12161f", border: "1px solid #232a38", fontSize: 12 }}
          />
          <Area type="monotone" dataKey="messages_per_second" stroke="#4b9fff" fill="#4b9fff33" name="msg/sec" />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

export function LatencyChart({ data }: { data: TimePoint[] }) {
  return (
    <div className="panel col-4">
      <h2>Latency over time</h2>
      <ResponsiveContainer width="100%" height={180}>
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke="#232a38" />
          <XAxis dataKey="t" tickFormatter={fmtTime} tick={axisStyle} minTickGap={40} />
          <YAxis tick={axisStyle} width={40} unit="ms" />
          <Tooltip
            labelFormatter={(v) => fmtTime(Number(v))}
            contentStyle={{ background: "#12161f", border: "1px solid #232a38", fontSize: 12 }}
          />
          <Line type="monotone" dataKey="p95_latency_ms" stroke="#e0b84b" dot={false} name="p95 (ms)" />
          <Line type="monotone" dataKey="p99_latency_ms" stroke="#e0453f" dot={false} name="p99 (ms)" />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

export function AnomalyChart({ data }: { data: TimePoint[] }) {
  return (
    <div className="panel col-4">
      <h2>Anomaly count over time (5m window)</h2>
      <ResponsiveContainer width="100%" height={180}>
        <AreaChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke="#232a38" />
          <XAxis dataKey="t" tickFormatter={fmtTime} tick={axisStyle} minTickGap={40} />
          <YAxis tick={axisStyle} width={40} />
          <Tooltip
            labelFormatter={(v) => fmtTime(Number(v))}
            contentStyle={{ background: "#12161f", border: "1px solid #232a38", fontSize: 12 }}
          />
          <Area type="monotone" dataKey="anomalies_5m" stroke="#33c17a" fill="#33c17a33" name="anomalies" />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
