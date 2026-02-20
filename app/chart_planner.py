from __future__ import annotations

from decimal import Decimal
from dataclasses import dataclass
from typing import Any

from app.query_executor import QueryResult


ROW_INDEX_X_KEY = "__row_index__"


@dataclass(frozen=True)
class ChartSpec:
    chart_type: str
    x: str | None
    y: list[str]
    title: str


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)


def build_chart_spec(query_result: QueryResult, title: str = "SQL Query Result") -> ChartSpec:
    if not query_result.rows:
        return ChartSpec(chart_type="table", x=None, y=[], title=f"{title} (empty)")

    numeric_cols = [
        c
        for c in query_result.columns
        if any(_is_number(row.get(c)) for row in query_result.rows)
    ]
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

    # only numeric columns are present: still render a bar chart using row index as x-axis
    return ChartSpec(chart_type="bar", x=ROW_INDEX_X_KEY, y=[numeric_cols[0]], title=title)
