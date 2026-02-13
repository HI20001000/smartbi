from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.query_executor import QueryResult


@dataclass(frozen=True)
class ChartSpec:
    chart_type: str
    x: str | None
    y: list[str]
    title: str


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def build_chart_spec(query_result: QueryResult, title: str = "SQL Query Result") -> ChartSpec:
    if not query_result.rows:
        return ChartSpec(chart_type="table", x=None, y=[], title=f"{title} (empty)")

    sample = query_result.rows[0]
    numeric_cols = [c for c, v in sample.items() if _is_number(v)]
    non_numeric_cols = [c for c in query_result.columns if c not in numeric_cols]

    if not numeric_cols:
        return ChartSpec(chart_type="table", x=None, y=[], title=title)

    # time-ish dimension first, otherwise first categorical
    x_candidates = [c for c in non_numeric_cols if any(k in c.lower() for k in ("date", "month", "time", "yyyy"))]
    if x_candidates:
        x_col = x_candidates[0]
        return ChartSpec(chart_type="line", x=x_col, y=[numeric_cols[0]], title=title)

    if non_numeric_cols:
        return ChartSpec(chart_type="bar", x=non_numeric_cols[0], y=[numeric_cols[0]], title=title)

    return ChartSpec(chart_type="table", x=None, y=numeric_cols[:3], title=title)
