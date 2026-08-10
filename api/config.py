from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ApiConfig(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", populate_by_name=True)

    host: str = Field("0.0.0.0", alias="API_HOST")
    port: int = Field(8000, alias="API_PORT")
    cors_origins: str = Field("http://localhost:5173,http://localhost:3000", alias="API_CORS_ORIGINS")

    database_url: str = Field(
        "postgresql://pulseguard:pulseguard_dev_password@postgres:5432/pulseguard",
        alias="DATABASE_URL",
    )
    prometheus_url: str = Field("http://prometheus:9090", alias="PROMETHEUS_URL")
    known_symbols: str = Field("AAPL,MSFT,NVDA,GOOGL,AMZN,TSLA,META", alias="SIMULATOR_SYMBOLS")

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

    @property
    def symbol_list(self) -> list[str]:
        return [s.strip() for s in self.known_symbols.split(",") if s.strip()]


def load_config() -> ApiConfig:
    return ApiConfig()
