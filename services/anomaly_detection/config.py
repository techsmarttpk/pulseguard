from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AnomalyConfig(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", populate_by_name=True)

    kafka_bootstrap_servers: str = Field("kafka:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    topic_market_data: str = Field("market-data", alias="KAFKA_TOPIC_MARKET_DATA")
    topic_alerts: str = Field("alerts", alias="KAFKA_TOPIC_ALERTS")
    consumer_group: str = Field("pulseguard-anomaly-detection", alias="KAFKA_CONSUMER_GROUP_ANOMALY")
    consumer_concurrency: int = Field(4, alias="KAFKA_CONSUMER_CONCURRENCY")

    database_url: str = Field(
        "postgresql://pulseguard:pulseguard_dev_password@postgres:5432/pulseguard",
        alias="DATABASE_URL",
    )

    zscore_threshold: float = Field(4.0, alias="ANOMALY_ZSCORE_THRESHOLD")
    pct_change_threshold: float = Field(0.03, alias="ANOMALY_PCT_CHANGE_THRESHOLD")
    rolling_window_size: int = Field(100, alias="ANOMALY_ROLLING_WINDOW_SIZE")

    isolation_forest_contamination: float = Field(0.02, alias="ANOMALY_ISOLATION_FOREST_CONTAMINATION")
    isolation_forest_retrain_every_n: int = Field(2000, alias="ANOMALY_ISOLATION_FOREST_RETRAIN_EVERY_N")
    isolation_forest_min_train_samples: int = Field(300, alias="ANOMALY_ISOLATION_FOREST_MIN_TRAIN_SAMPLES")

    alert_cooldown_seconds: float = Field(60.0, alias="MONITORING_ALERT_COOLDOWN_SECONDS")
    metrics_port: int = Field(9102, alias="METRICS_PORT_ANOMALY")
    log_level: str = Field("INFO", alias="INGESTION_LOG_LEVEL")


def load_config() -> AnomalyConfig:
    return AnomalyConfig()
