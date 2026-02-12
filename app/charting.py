from __future__ import annotations

from typing import Any, Literal

ChartType = Literal["auto", "bar", "line", "pie", "scatter"]


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _pick_fields(rows: list[dict[str, Any]], columns: list[str]) -> tuple[str | None, str | None]:
    if not rows or not columns:
        return None, None

    numeric_candidates = [c for c in columns if any(_is_number(r.get(c)) for r in rows)]
    dimension_candidates = [c for c in columns if c not in numeric_candidates]

    x_field = dimension_candidates[0] if dimension_candidates else columns[0]
    y_field = numeric_candidates[0] if numeric_candidates else (columns[1] if len(columns) > 1 else columns[0])
    return x_field, y_field


def build_chart_spec(rows: list[dict[str, Any]], columns: list[str], chart_type: ChartType) -> dict[str, Any] | None:
    if not rows:
        return None

    x_field, y_field = _pick_fields(rows, columns)
    if not x_field or not y_field:
        return None

    if chart_type == "auto":
        chart_type = "line" if "date" in x_field.lower() or "time" in x_field.lower() else "bar"

    if chart_type == "bar":
        mark = "bar"
    elif chart_type == "line":
        mark = "line"
    elif chart_type == "pie":
        return {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "data": {"values": rows},
            "mark": {"type": "arc", "innerRadius": 30},
            "encoding": {
                "theta": {"field": y_field, "type": "quantitative"},
                "color": {"field": x_field, "type": "nominal"},
            },
        }
    else:
        mark = "point"

    return {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "data": {"values": rows},
        "mark": mark,
        "encoding": {
            "x": {"field": x_field, "type": "nominal"},
            "y": {"field": y_field, "type": "quantitative"},
            "tooltip": [{"field": c} for c in columns],
        },
    }


def summarize_insight(rows: list[dict[str, Any]], columns: list[str]) -> str:
    if not rows:
        return "No rows returned by the query."

    row_count = len(rows)
    numeric_cols = [c for c in columns if any(_is_number(r.get(c)) for r in rows)]
    if not numeric_cols:
        return f"Returned {row_count} rows across {len(columns)} columns."

    col = numeric_cols[0]
    values = [r[col] for r in rows if _is_number(r.get(col))]
    if not values:
        return f"Returned {row_count} rows across {len(columns)} columns."

    return (
        f"Returned {row_count} rows. For '{col}', min={min(values):,.2f}, "
        f"max={max(values):,.2f}, avg={sum(values) / len(values):,.2f}."
    )
