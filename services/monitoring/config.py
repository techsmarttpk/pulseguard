from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MonitoringConfig(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", populate_by_name=True)

    kafka_bootstrap_servers: str = Field("kafka:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    topic_market_data: str = Field("market-data", alias="KAFKA_TOPIC_MARKET_DATA")
    topic_alerts: str = Field("alerts", alias="KAFKA_TOPIC_ALERTS")
    consumer_group_suffix: str = Field("pulseguard-monitoring", alias="KAFKA_CONSUMER_GROUP_MONITORING")
    consumer_concurrency: int = Field(2, alias="KAFKA_CONSUMER_CONCURRENCY")

    database_url: str = Field(
        "postgresql://pulseguard:pulseguard_dev_password@postgres:5432/pulseguard",
        alias="DATABASE_URL",
    )

    healthy_min_msg_per_sec: float = Field(50, alias="MONITORING_HEALTHY_MIN_MSG_PER_SEC")
    degraded_min_msg_per_sec: float = Field(5, alias="MONITORING_DEGRADED_MIN_MSG_PER_SEC")
    stale_no_message_seconds: float = Field(5, alias="MONITORING_STALE_NO_MESSAGE_SECONDS")
    offline_no_message_seconds: float = Field(15, alias="MONITORING_OFFLINE_NO_MESSAGE_SECONDS")
    p99_latency_alert_seconds: float = Field(1.0, alias="MONITORING_P99_LATENCY_ALERT_SECONDS")
    error_rate_alert_threshold: float = Field(0.05, alias="MONITORING_ERROR_RATE_ALERT_THRESHOLD")
    alert_cooldown_seconds: float = Field(60.0, alias="MONITORING_ALERT_COOLDOWN_SECONDS")

    evaluation_interval_seconds: float = 1.0
    active_alerts_poll_interval_seconds: float = 5.0
    metrics_port: int = Field(9103, alias="METRICS_PORT_MONITORING")
    log_level: str = Field("INFO", alias="INGESTION_LOG_LEVEL")

    known_symbols: str = Field("AAPL,MSFT,NVDA,GOOGL,AMZN,TSLA,META", alias="SIMULATOR_SYMBOLS")

    @property
    def symbol_list(self) -> list[str]:
        return [s.strip() for s in self.known_symbols.split(",") if s.strip()]


def load_config() -> MonitoringConfig:
    return MonitoringConfig()
