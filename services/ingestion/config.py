from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class IngestionConfig(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", populate_by_name=True)

    kafka_bootstrap_servers: str = Field("kafka:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    topic_market_data: str = Field("market-data", alias="KAFKA_TOPIC_MARKET_DATA")
    topic_dead_letter: str = Field("market-data-dead-letter", alias="KAFKA_TOPIC_DEAD_LETTER")
    topic_alerts: str = Field("alerts", alias="KAFKA_TOPIC_ALERTS")
    consumer_group: str = Field("pulseguard-ingestion", alias="KAFKA_CONSUMER_GROUP_INGESTION")
    consumer_concurrency: int = Field(4, alias="KAFKA_CONSUMER_CONCURRENCY")

    database_url: str = Field(
        "postgresql://pulseguard:pulseguard_dev_password@postgres:5432/pulseguard",
        alias="DATABASE_URL",
    )

    stale_threshold_seconds: float = Field(5.0, alias="INGESTION_STALE_THRESHOLD_SECONDS")
    duplicate_cache_size: int = Field(50000, alias="INGESTION_DUPLICATE_CACHE_SIZE")
    extreme_quantity_threshold: float = Field(1_000_000, alias="INGESTION_EXTREME_QUANTITY_THRESHOLD")
    alert_cooldown_seconds: float = Field(60.0, alias="MONITORING_ALERT_COOLDOWN_SECONDS")
    metrics_port: int = Field(9101, alias="METRICS_PORT_INGESTION")
    log_level: str = Field("INFO", alias="INGESTION_LOG_LEVEL")


def load_config() -> IngestionConfig:
    return IngestionConfig()
