from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


class ConfigError(ValueError):
    """Raised when required environment configuration is invalid."""


@dataclass(slots=True)
class AppConfig:
    db_host: str
    db_port: int
    db_user: str
    db_password: str
    db_name: str
    db_charset: str
    openai_api_key: str
    openai_model: str
    seed_sql_path: str
    sql_max_rows: int
    agent_max_retries: int

    @property
    def database_url(self) -> str:
        return (
            f"mysql+pymysql://{self.db_user}:{self.db_password}@"
            f"{self.db_host}:{self.db_port}/{self.db_name}?charset={self.db_charset}"
        )


def _first_env(*keys: str, default: str | None = None) -> str | None:
    for key in keys:
        value = os.getenv(key)
        if value is not None and value != "":
            return value
    return default


def _int_env(keys: tuple[str, ...], default: int) -> int:
    raw = _first_env(*keys)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"Environment variable {keys[0]} must be an integer.") from exc


def load_config(env_file: str | Path = ".env") -> AppConfig:
    load_dotenv(dotenv_path=env_file)

    db_host = _first_env("MYSQL_HOST", "DB_HOST", default="localhost")
    db_port = _int_env(("MYSQL_PORT", "DB_PORT"), default=3306)
    db_user = _first_env("MYSQL_USER", "DB_USER")
    db_password = _first_env("MYSQL_PASSWORD", "DB_PASSWORD")
    db_name = _first_env("MYSQL_DATABASE", "MYSQL_DB", "DB_NAME", "DATABASE_NAME")
    db_charset = _first_env("MYSQL_CHARSET", "DB_CHARSET", default="utf8mb4")

    openai_api_key = _first_env("OPENAI_API_KEY")
    openai_model = _first_env("OPENAI_MODEL", default="gpt-4o-mini")

    seed_sql_path = _first_env("SEED_SQL_PATH", default="exmaple_data.sql")
    sql_max_rows = _int_env(("SQL_MAX_ROWS",), default=200)
    agent_max_retries = _int_env(("AGENT_MAX_RETRIES",), default=2)

    missing = []
    if not db_user:
        missing.append("MYSQL_USER/DB_USER")
    if not db_password:
        missing.append("MYSQL_PASSWORD/DB_PASSWORD")
    if not db_name:
        missing.append("MYSQL_DATABASE/MYSQL_DB/DB_NAME")
    if not openai_api_key:
        missing.append("OPENAI_API_KEY")

    if missing:
        raise ConfigError(f"Missing required environment variables: {', '.join(missing)}")

    return AppConfig(
        db_host=db_host or "localhost",
        db_port=db_port,
        db_user=db_user,
        db_password=db_password,
        db_name=db_name,
        db_charset=db_charset or "utf8mb4",
        openai_api_key=openai_api_key,
        openai_model=openai_model or "gpt-4o-mini",
        seed_sql_path=seed_sql_path or "exmaple_data.sql",
        sql_max_rows=sql_max_rows,
        agent_max_retries=agent_max_retries,
    )
