"""Application configuration for PurpleBank backend.

This module centralizes runtime settings loaded from environment variables.
Keeping configuration in one place avoids hidden defaults spread across
routers/services and makes deployment behavior explicit.
"""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings for backend API and database connectivity."""

    app_name: str = "PurpleBank API"
    app_env: str = "dev"
    api_prefix: str = "/api/v1"

    mysql_host: str = "127.0.0.1"
    mysql_port: int = 3306
    mysql_user: str = "root"
    mysql_password: str = "root"
    mysql_database: str = "purple_bank"

    jwt_secret: str = Field(default="purple-bank-dev-secret", min_length=16)
    jwt_algorithm: str = "HS256"
    jwt_exp_minutes: int = 120

    frontend_origin: str = "http://localhost:3000"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @property
    def sqlalchemy_url(self) -> str:
        """Return SQLAlchemy DSN for PyMySQL driver."""

        return (
            f"mysql+pymysql://{self.mysql_user}:{self.mysql_password}"
            f"@{self.mysql_host}:{self.mysql_port}/{self.mysql_database}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached settings instance to avoid repeated env parsing."""

    return Settings()
