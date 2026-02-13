import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()
"""
_get(參數名, 預設值) -> 取得環境變數，若不存在或為空字串則回傳預設值
"""

def _get(key: str, default: str | None = None) -> str | None:
    v = os.getenv(key, default)
    if v is None:
        return None
    v = v.strip()
    return v if v != "" else default


@dataclass(frozen=True)
class Settings:
    llm_base_url: str
    llm_model: str
    llm_api_key: str

    temperature: float = 0.2  # default
    max_tokens: int | None = None  # optional

    db_host: str | None = None
    db_port: int = 3306
    db_user: str | None = None
    db_password: str | None = None
    db_name: str | None = None
    chart_output_dir: str = "artifacts/charts"

    @staticmethod
    def load() -> "Settings":
        base_url = _get("LLM_BASE_URL")
        model = _get("LLM_MODEL")
        api_key = _get("LLM_API_KEY", "empty")

        if not base_url or not model:
            missing = []
            if not base_url:
                missing.append("LLM_BASE_URL")
            if not model:
                missing.append("LLM_MODEL")
            raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

        # 可選：你之後也可以放在 .env，例如 LLM_TEMPERATURE、LLM_MAX_TOKENS
        temp = float(_get("LLM_TEMPERATURE", "0.2"))
        max_tokens_raw = _get("LLM_MAX_TOKENS", None)
        max_tokens = int(max_tokens_raw) if max_tokens_raw else None

        return Settings(
            llm_base_url=base_url,
            llm_model=model,
            llm_api_key=api_key or "empty",
            temperature=temp,
            max_tokens=max_tokens,
            db_host=_get("DB_HOST"),
            db_port=int(_get("DB_PORT", "3306") or "3306"),
            db_user=_get("DB_USER"),
            db_password=_get("DB_PASSWORD"),
            db_name=_get("DB_NAME"),
            chart_output_dir=_get("CHART_OUTPUT_DIR", "artifacts/charts") or "artifacts/charts",
        )
