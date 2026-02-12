from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class AskRequest(BaseModel):
    question: str = Field(..., min_length=1, description="Natural language analytics question")
    chart_type: Literal["auto", "bar", "line", "pie", "scatter"] = "auto"


class AskResponse(BaseModel):
    question: str
    sql: str
    columns: list[str]
    rows: list[dict[str, Any]]
    chart_spec: dict[str, Any] | None = None
    insight: str | None = None
    retries: int = 0
