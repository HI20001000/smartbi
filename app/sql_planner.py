from __future__ import annotations

from typing import Any


def _unique_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


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

    selected_filters.extend(
        _build_time_filter_from_bounds(
            str(extracted_features.get("time_start", "") or ""),
            str(extracted_features.get("time_end", "") or ""),
        )
    )

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
