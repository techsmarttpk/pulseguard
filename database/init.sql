-- PulseGuard database schema.
--
-- Deliberate design decision: this schema does NOT store every market
-- tick. The high-volume stream lives in Kafka and is processed in-memory
-- by the services; only the outputs that matter for reliability
-- engineering — alerts, anomalies, feed state transitions, and periodic
-- aggregated metrics — are durably persisted. An optional, rate-limited
-- sample of raw events can be captured into `recent_events` for debugging
-- (disabled by default; see INGESTION_EVENT_SAMPLE_RATE).

CREATE TABLE IF NOT EXISTS alerts (
    alert_id            TEXT PRIMARY KEY,
    created_at          TIMESTAMPTZ NOT NULL,
    resolved_at         TIMESTAMPTZ,
    severity            TEXT NOT NULL CHECK (severity IN ('CRITICAL', 'HIGH', 'MEDIUM', 'LOW')),
    alert_type          TEXT NOT NULL,
    feed                TEXT NOT NULL,
    symbol              TEXT,
    description         TEXT NOT NULL,
    metrics             JSONB NOT NULL DEFAULT '{}'::jsonb,
    detection_source    TEXT NOT NULL,
    dedup_key           TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'RESOLVED'))
);

CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts (status);
CREATE INDEX IF NOT EXISTS idx_alerts_feed ON alerts (feed);
CREATE INDEX IF NOT EXISTS idx_alerts_dedup_key ON alerts (dedup_key);

CREATE TABLE IF NOT EXISTS anomalies (
    anomaly_id          TEXT PRIMARY KEY,
    detected_at         TIMESTAMPTZ NOT NULL,
    symbol              TEXT NOT NULL,
    detection_method    TEXT NOT NULL CHECK (detection_method IN ('statistical', 'isolation_forest', 'validation')),
    severity            TEXT NOT NULL CHECK (severity IN ('CRITICAL', 'HIGH', 'MEDIUM', 'LOW')),
    anomaly_score       DOUBLE PRECISION NOT NULL,
    description         TEXT NOT NULL,
    metrics             JSONB NOT NULL DEFAULT '{}'::jsonb,
    event_id            TEXT
);

CREATE INDEX IF NOT EXISTS idx_anomalies_detected_at ON anomalies (detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_symbol ON anomalies (symbol);
CREATE INDEX IF NOT EXISTS idx_anomalies_method ON anomalies (detection_method);

CREATE TABLE IF NOT EXISTS feed_status_transitions (
    id                  BIGSERIAL PRIMARY KEY,
    feed                TEXT NOT NULL,
    previous_state      TEXT,
    new_state           TEXT NOT NULL CHECK (new_state IN ('HEALTHY', 'DEGRADED', 'STALE', 'OFFLINE')),
    reason              TEXT,
    transitioned_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_feed_transitions_feed ON feed_status_transitions (feed, transitioned_at DESC);

CREATE TABLE IF NOT EXISTS aggregated_metrics (
    feed                TEXT NOT NULL,
    window_start        TIMESTAMPTZ NOT NULL,
    metrics             JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (feed, window_start)
);

CREATE INDEX IF NOT EXISTS idx_aggregated_metrics_window ON aggregated_metrics (window_start DESC);

-- Optional bounded recent-event window, disabled by default. If enabled
-- (INGESTION_EVENT_SAMPLE_RATE > 0), ingestion writes a sampled subset of
-- valid events here for ad-hoc debugging; a periodic cleanup deletes rows
-- older than the configured retention window so this table never grows
-- unbounded.
CREATE TABLE IF NOT EXISTS recent_events (
    event_id            TEXT PRIMARY KEY,
    symbol              TEXT NOT NULL,
    event_type          TEXT NOT NULL,
    price               DOUBLE PRECISION NOT NULL,
    quantity            DOUBLE PRECISION,
    bid                 DOUBLE PRECISION,
    ask                 DOUBLE PRECISION,
    producer_timestamp  TIMESTAMPTZ NOT NULL,
    received_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_recent_events_received_at ON recent_events (received_at DESC);
CREATE INDEX IF NOT EXISTS idx_recent_events_symbol ON recent_events (symbol);
