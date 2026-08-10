"""Configuration for the market data simulator, entirely env-driven so the
same image can be pointed at different throughput / injection profiles from
docker-compose or a CI benchmark script without a code change.
"""
from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _split_csv(value: str) -> list[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


class SimulatorConfig(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", populate_by_name=True)

    symbols: str = Field("AAPL,MSFT,NVDA,GOOGL,AMZN,TSLA,META", alias="SIMULATOR_SYMBOLS")
    exchanges: str = Field("NASDAQ,NYSE", alias="SIMULATOR_EXCHANGES")
    throughput_msg_per_sec: int = Field(1000, alias="SIMULATOR_THROUGHPUT_MSG_PER_SEC")
    run_duration_seconds: int = Field(0, alias="SIMULATOR_RUN_DURATION_SECONDS")  # 0 = run forever

    inject_enabled: bool = Field(True, alias="SIMULATOR_INJECT_ENABLED")
    inject_price_spike_prob: float = Field(0.0008, alias="SIMULATOR_INJECT_PRICE_SPIKE_PROB")
    inject_price_crash_prob: float = Field(0.0008, alias="SIMULATOR_INJECT_PRICE_CRASH_PROB")
    inject_negative_price_prob: float = Field(0.0003, alias="SIMULATOR_INJECT_NEGATIVE_PRICE_PROB")
    inject_zero_price_prob: float = Field(0.0003, alias="SIMULATOR_INJECT_ZERO_PRICE_PROB")
    inject_bad_bid_ask_prob: float = Field(0.0005, alias="SIMULATOR_INJECT_BAD_BID_ASK_PROB")
    inject_extreme_quantity_prob: float = Field(0.0005, alias="SIMULATOR_INJECT_EXTREME_QUANTITY_PROB")
    inject_duplicate_prob: float = Field(0.0008, alias="SIMULATOR_INJECT_DUPLICATE_PROB")
    inject_sequence_gap_prob: float = Field(0.0008, alias="SIMULATOR_INJECT_SEQUENCE_GAP_PROB")
    inject_stale_event_prob: float = Field(0.0005, alias="SIMULATOR_INJECT_STALE_EVENT_PROB")
    inject_delayed_event_prob: float = Field(0.0008, alias="SIMULATOR_INJECT_DELAYED_EVENT_PROB")
    inject_corrupted_event_prob: float = Field(0.0003, alias="SIMULATOR_INJECT_CORRUPTED_EVENT_PROB")

    inject_burst_enabled: bool = Field(True, alias="SIMULATOR_INJECT_BURST_ENABLED")
    inject_burst_interval_seconds: int = Field(90, alias="SIMULATOR_INJECT_BURST_INTERVAL_SECONDS")
    inject_burst_duration_seconds: int = Field(5, alias="SIMULATOR_INJECT_BURST_DURATION_SECONDS")
    inject_burst_multiplier: int = Field(8, alias="SIMULATOR_INJECT_BURST_MULTIPLIER")

    inject_pause_enabled: bool = Field(True, alias="SIMULATOR_INJECT_PAUSE_ENABLED")
    inject_pause_interval_seconds: int = Field(120, alias="SIMULATOR_INJECT_PAUSE_INTERVAL_SECONDS")
    inject_pause_duration_seconds: int = Field(3, alias="SIMULATOR_INJECT_PAUSE_DURATION_SECONDS")

    inject_outage_enabled: bool = Field(False, alias="SIMULATOR_INJECT_OUTAGE_ENABLED")
    inject_outage_interval_seconds: int = Field(300, alias="SIMULATOR_INJECT_OUTAGE_INTERVAL_SECONDS")
    inject_outage_duration_seconds: int = Field(15, alias="SIMULATOR_INJECT_OUTAGE_DURATION_SECONDS")

    kafka_bootstrap_servers: str = Field("kafka:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic_market_data: str = Field("market-data", alias="KAFKA_TOPIC_MARKET_DATA")

    @property
    def symbol_list(self) -> list[str]:
        return _split_csv(self.symbols)

    @property
    def exchange_list(self) -> list[str]:
        return _split_csv(self.exchanges)


def load_config() -> SimulatorConfig:
    return SimulatorConfig()
