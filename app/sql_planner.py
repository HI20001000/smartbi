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


def _canonical_candidates(matches: list[dict[str, Any]], object_type: str) -> list[str]:
    return _unique_keep_order(
        [
            m.get("canonical_name", "")
            for m in matches
            if m.get("object_type") == object_type and m.get("allowed") is not False and m.get("canonical_name")
        ]
    )


def _dataset_candidates(matches: list[dict[str, Any]]) -> list[str]:
    return _unique_keep_order([m.get("dataset", "") for m in matches if m.get("dataset")])


def _safe_selected_values(candidates: list[str], values: list[Any]) -> list[str]:
    candidate_set = set(candidates)
    out: list[str] = []
    for value in values:
        if not isinstance(value, str):
            continue
        normalized = value.strip()
        if not normalized or normalized not in candidate_set:
            continue
        if normalized not in out:
            out.append(normalized)
    return out


def build_semantic_plan(
    extracted_features: dict[str, Any],
    token_hits: dict[str, Any],
) -> dict[str, Any]:
    matches = token_hits.get("matches", []) or []

    selected_metrics = _canonical_candidates(matches, "metric")
    selected_dimensions = _canonical_candidates(matches, "dimension")
    dataset_candidates = _dataset_candidates(matches)

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


def merge_llm_selection_into_plan(
    draft_plan: dict[str, Any],
    llm_selection: dict[str, Any],
    token_hits: dict[str, Any],
) -> dict[str, Any]:
    matches = token_hits.get("matches", []) or []
    metric_candidates = _canonical_candidates(matches, "metric")
    dimension_candidates = _canonical_candidates(matches, "dimension")
    dataset_candidates = _dataset_candidates(matches)

    selected_metrics = _safe_selected_values(metric_candidates, llm_selection.get("selected_metrics", []) or [])
    selected_dimensions = _safe_selected_values(dimension_candidates, llm_selection.get("selected_dimensions", []) or [])
    selected_datasets = _safe_selected_values(dataset_candidates, llm_selection.get("selected_dataset_candidates", []) or [])

    if not selected_metrics:
        selected_metrics = list(draft_plan.get("selected_metrics", []) or [])
    if not selected_dimensions:
        selected_dimensions = list(draft_plan.get("selected_dimensions", []) or [])
    if not selected_datasets:
        selected_datasets = list(draft_plan.get("selected_dataset_candidates", []) or [])

    selected_filters = list(draft_plan.get("selected_filters", []) or [])
    llm_filters = llm_selection.get("selected_filters", []) or []
    if isinstance(llm_filters, list) and llm_filters:
        merged = [f for f in llm_filters if isinstance(f, dict)]
        if merged:
            selected_filters = merged

    rejected_candidates = list(draft_plan.get("rejected_candidates", []) or [])
    needs_clarification = not selected_metrics and not selected_dimensions
    clarification_questions: list[str] = []
    if needs_clarification:
        clarification_questions.append("請補充要查詢的指標或維度名稱。")

    confidence = llm_selection.get("confidence")
    if not isinstance(confidence, (int, float)):
        confidence = draft_plan.get("confidence", 0.0)

    return {
        "selected_metrics": selected_metrics,
        "selected_dimensions": selected_dimensions,
        "selected_filters": selected_filters,
        "selected_dataset_candidates": selected_datasets,
        "rejected_candidates": rejected_candidates,
        "needs_clarification": needs_clarification,
        "clarification_questions": clarification_questions,
        "confidence": float(confidence),
    }
