from __future__ import annotations

import re
from typing import Any


_DATE_RANGE_PATTERN = re.compile(r"^\s*(\d{4}-\d{2}-\d{2})\s*(?:TO|to|~|-)\s*(\d{4}-\d{2}-\d{2})\s*$")
_YEAR_PATTERN = re.compile(r"^\s*(\d{4})\s*$")
_MONTH_TAG_PATTERN = re.compile(r"^\s*MONTH:(0?[1-9]|1[0-2])\s*$", re.IGNORECASE)


def _unique_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _build_time_filter(time_range: str) -> list[dict[str, Any]]:
    if not isinstance(time_range, str) or not time_range.strip():
        return []

    m = _DATE_RANGE_PATTERN.match(time_range)
    if m:
        start_date, end_date = m.group(1), m.group(2)
        return [
            {
                "field": "calendar.biz_date",
                "op": "between",
                "value": [start_date, end_date],
                "source": "step_b_time_range",
            }
        ]

    year_match = _YEAR_PATTERN.match(time_range)
    if year_match:
        year = year_match.group(1)
        return [
            {
                "field": "calendar.biz_date",
                "op": "between",
                "value": [f"{year}-01-01", f"{year}-12-31"],
                "source": "step_b_time_range_year",
            }
        ]

    month_match = _MONTH_TAG_PATTERN.match(time_range)
    if month_match:
        month = int(month_match.group(1))
        return [
            {
                "field": "calendar.month",
                "op": "=",
                "value": f"{month:02d}",
                "source": "step_b_time_range_month_tag",
            }
        ]

    return []


def _build_time_filter_from_bounds(time_start: str, time_end: str) -> list[dict[str, Any]]:
    if not isinstance(time_start, str) or not isinstance(time_end, str):
        return []
    start = time_start.strip()
    end = time_end.strip()
    if not start or not end:
        return []

    return [
        {
            "field": "calendar.biz_date",
            "op": "between",
            "value": [start, end],
            "source": "step_b_time_bounds",
        }
    ]


def build_semantic_plan(
    extracted_features: dict[str, Any],
    token_hits: dict[str, Any],
) -> dict[str, Any]:
    matches = token_hits.get("matches", []) or []

    selected_metrics = _unique_keep_order(
        [m.get("canonical_name", "") for m in matches if m.get("object_type") == "metric" and m.get("allowed") is not False and m.get("canonical_name")]
    )
    selected_dimensions = _unique_keep_order(
        [m.get("canonical_name", "") for m in matches if m.get("object_type") == "dimension" and m.get("allowed") is not False and m.get("canonical_name")]
    )

    dataset_candidates = _unique_keep_order(
        [m.get("dataset", "") for m in matches if m.get("dataset")]
    )

    selected_filters: list[dict[str, Any]] = []
    for f in extracted_features.get("filters", []) or []:
        if isinstance(f, str) and f.strip():
            selected_filters.append({"expr": f.strip(), "source": "step_b_filters"})

    time_filters = _build_time_filter_from_bounds(
        str(extracted_features.get("time_start", "") or ""),
        str(extracted_features.get("time_end", "") or ""),
    )
    if time_filters:
        selected_filters.extend(time_filters)
    else:
        selected_filters.extend(_build_time_filter(str(extracted_features.get("time_range", "") or "")))

    blocked = token_hits.get("blocked_matches", []) or []
    rejected_candidates = [
        {
            "canonical_name": b.get("canonical_name", ""),
            "reason": "sensitive_or_disallowed",
        }
        for b in blocked
        if b.get("canonical_name")
    ]

    needs_clarification = not selected_metrics and not selected_dimensions
    clarification_questions: list[str] = []
    if needs_clarification:
        clarification_questions.append("請補充要查詢的指標或維度名稱。")

    confidence = 0.0
    if selected_metrics or selected_dimensions:
        confidence = 0.8
    elif matches:
        confidence = 0.4

    return {
        "selected_metrics": selected_metrics,
        "selected_dimensions": selected_dimensions,
        "selected_filters": selected_filters,
        "selected_dataset_candidates": dataset_candidates,
        "rejected_candidates": rejected_candidates,
        "needs_clarification": needs_clarification,
        "clarification_questions": clarification_questions,
        "confidence": confidence,
    }
