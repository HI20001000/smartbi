from __future__ import annotations

import re
from typing import Any


_NUMERIC_PATTERN = re.compile(r"^-?\d+(?:\.\d+)?$")


def _unique_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _parse_scalar_filter_value(raw_value: str) -> Any:
    value = raw_value.strip()
    if len(value) >= 2 and ((value[0] == value[-1] == "'") or (value[0] == value[-1] == '"')):
        return value[1:-1]
    if _NUMERIC_PATTERN.match(value):
        return float(value) if "." in value else int(value)
    return value


def _parse_filter_expr(filter_text: str) -> dict[str, Any]:
    text = filter_text.strip()
    source = "step_b_filters"
    if not text:
        return {"expr": text, "source": source}

    between_match = re.match(r"^(?P<field>.+?)\s+[bB][eE][tT][wW][eE][eE][nN]\s+(?P<start>.+?)\s+[aA][nN][dD]\s+(?P<end>.+)$", text)
    if between_match:
        return {
            "field": between_match.group("field").strip(),
            "op": "between",
            "value": [
                _parse_scalar_filter_value(between_match.group("start")),
                _parse_scalar_filter_value(between_match.group("end")),
            ],
            "source": source,
        }

    in_match = re.match(r"^(?P<field>.+?)\s+[iI][nN]\s*\((?P<values>.+)\)$", text)
    if in_match:
        raw_values = [v.strip() for v in in_match.group("values").split(",")]
        values = [_parse_scalar_filter_value(v) for v in raw_values if v]
        return {
            "field": in_match.group("field").strip(),
            "op": "in",
            "value": values,
            "source": source,
        }

    normalized_text = text.replace("＝", "=")
    for op in ("!=", ">=", "<=", "=", ">", "<"):
        if op not in normalized_text:
            continue
        lhs, rhs = normalized_text.split(op, 1)
        field = lhs.strip()
        value_text = rhs.strip()
        if field and value_text:
            return {
                "field": field,
                "op": op,
                "value": _parse_scalar_filter_value(value_text),
                "source": source,
            }
        break

    return {"expr": text, "source": source}


def _build_step_b_filters(extracted_features: dict[str, Any]) -> list[dict[str, Any]]:
    selected_filters: list[dict[str, Any]] = []
    for f in extracted_features.get("filters", []) or []:
        if isinstance(f, str) and f.strip():
            selected_filters.append(_parse_filter_expr(f))
    return selected_filters


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

    selected_filters = _build_step_b_filters(extracted_features)
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

    return {
        "selected_metrics": selected_metrics,
        "selected_dimensions": selected_dimensions,
        "selected_filters": selected_filters,
        "selected_dataset_candidates": dataset_candidates,
        "rejected_candidates": rejected_candidates,
        "needs_clarification": needs_clarification,
        "clarification_questions": clarification_questions,
    }


def merge_llm_selection_into_plan(
    llm_selection: dict[str, Any],
    token_hits: dict[str, Any],
    extracted_features: dict[str, Any],
) -> dict[str, Any]:
    """Deterministically assemble semantic plan from Step C candidates + LLM selection."""
    matches = token_hits.get("matches", []) or []
    metric_candidates = _canonical_candidates(matches, "metric")
    dimension_candidates = _canonical_candidates(matches, "dimension")
    dataset_candidates = _dataset_candidates(matches)

    selected_metrics = _safe_selected_values(metric_candidates, llm_selection.get("selected_metrics", []) or [])
    selected_dimensions = _safe_selected_values(dimension_candidates, llm_selection.get("selected_dimensions", []) or [])
    selected_datasets = _safe_selected_values(dataset_candidates, llm_selection.get("selected_dataset_candidates", []) or [])

    # fallback: if LLM did not select, use deterministic candidate order from Step C
    if not selected_metrics:
        selected_metrics = metric_candidates
    if not selected_dimensions:
        selected_dimensions = dimension_candidates
    if not selected_datasets:
        selected_datasets = dataset_candidates

    selected_filters: list[dict[str, Any]] = []
    llm_filters = llm_selection.get("selected_filters", []) or []
    if isinstance(llm_filters, list):
        selected_filters = [f for f in llm_filters if isinstance(f, dict)]

    if not selected_filters:
        selected_filters = _build_step_b_filters(extracted_features)

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

    return {
        "selected_metrics": selected_metrics,
        "selected_dimensions": selected_dimensions,
        "selected_filters": selected_filters,
        "selected_dataset_candidates": selected_datasets,
        "rejected_candidates": rejected_candidates,
        "needs_clarification": needs_clarification,
        "clarification_questions": clarification_questions,
    }
