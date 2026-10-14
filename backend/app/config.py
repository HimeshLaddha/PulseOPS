from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    POSTGRES_USER: str = "pulseops"
    POSTGRES_PASSWORD: str = "pulseops_secret"
    POSTGRES_DB: str = "pulseops_db"
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432

    DB_POOL_MIN_SIZE: int = 5
    DB_POOL_MAX_SIZE: int = 20

    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_STREAM_KEY: str = "telemetry:stream"
    REDIS_CONSUMER_GROUP: str = "pulseops-ingest-group"
    REDIS_STREAM_MIN_IDLE_MS: int = 5000  # Minimum idle threshold (ms) before unacknowledged messages become eligible for reclaim via XAUTOCLAIM. Not a guaranteed lease; workers exceeding this threshold may trigger concurrent reclaim, handled safely via idempotent PostgreSQL writes.
    INGESTION_MODE: str = "direct"

    @property
    def database_url(self) -> str:
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
            f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def redis_url(self) -> str:
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache()
def get_settings() -> Settings:
    return Settings()
