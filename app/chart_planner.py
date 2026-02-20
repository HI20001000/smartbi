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


def _normalize_chart_type(value: str | None) -> str:
    raw = str(value or "").strip().lower()
    mapping = {
        "bar": "bar",
        "line": "line",
        "pie": "pie",
        "scatter": "scatter",
        "直條圖": "bar",
        "柱狀圖": "bar",
        "長條圖": "bar",
        "折線圖": "line",
        "線圖": "line",
        "圓餅圖": "pie",
        "餅圖": "pie",
        "餅形圖": "pie",
        "散佈圖": "scatter",
        "散点图": "scatter",
    }
    return mapping.get(raw, "")


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)


def build_chart_spec(
    query_result: QueryResult,
    title: str = "SQL Query Result",
    preferred_chart_type: str | None = None,
) -> ChartSpec:
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

    requested = _normalize_chart_type(preferred_chart_type)
    if requested == "pie" and non_numeric_cols:
        return ChartSpec(chart_type="pie", x=non_numeric_cols[0], y=[numeric_cols[0]], title=title)
    if requested == "scatter" and len(numeric_cols) >= 2:
        return ChartSpec(chart_type="scatter", x=numeric_cols[0], y=[numeric_cols[1]], title=title)
    if requested == "line":
        if non_numeric_cols:
            return ChartSpec(chart_type="line", x=non_numeric_cols[0], y=[numeric_cols[0]], title=title)
        return ChartSpec(chart_type="line", x=ROW_INDEX_X_KEY, y=[numeric_cols[0]], title=title)
    if requested == "bar":
        if non_numeric_cols:
            return ChartSpec(chart_type="bar", x=non_numeric_cols[0], y=[numeric_cols[0]], title=title)
        return ChartSpec(chart_type="bar", x=ROW_INDEX_X_KEY, y=[numeric_cols[0]], title=title)

    # time-ish dimension first, otherwise first categorical
    x_candidates = [c for c in non_numeric_cols if any(k in c.lower() for k in ("date", "month", "time", "yyyy"))]
    if x_candidates:
        x_col = x_candidates[0]
        return ChartSpec(chart_type="line", x=x_col, y=[numeric_cols[0]], title=title)

    if non_numeric_cols:
        return ChartSpec(chart_type="bar", x=non_numeric_cols[0], y=[numeric_cols[0]], title=title)

    if len(numeric_cols) >= 2:
        return ChartSpec(chart_type="scatter", x=numeric_cols[0], y=[numeric_cols[1]], title=title)

    # only numeric columns are present: still render a bar chart using row index as x-axis
    return ChartSpec(chart_type="bar", x=ROW_INDEX_X_KEY, y=[numeric_cols[0]], title=title)
