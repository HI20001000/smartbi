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


def _normalize_key(value: str) -> str:
    return str(value or "").strip().lower()


def _normalize_phrase(value: str) -> str:
    text = _normalize_key(value).replace(" ", "")
    for token in ("總額", "總和", "合計", "總計"):
        text = text.replace(token, "")
    return text


def _canonical_candidates(matches: list[dict[str, Any]], object_type: str) -> list[str]:
    return _unique_keep_order(
        [
            m.get("canonical_name", "")
            for m in matches
            if m.get("object_type") == object_type and m.get("allowed") is not False and m.get("canonical_name")
        ]
    )


def _dimension_candidates(matches: list[dict[str, Any]]) -> list[str]:
    return _unique_keep_order(
        [
            m.get("canonical_name", "")
            for m in matches
            if m.get("object_type") in {"dimension", "field"}
            and m.get("allowed") is not False
            and m.get("canonical_name")
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


def _add_alias(alias_lookup: dict[str, str], alias: str, canonical: str) -> None:
    key = _normalize_key(alias)
    if key and key not in alias_lookup:
        alias_lookup[key] = canonical


def _build_field_alias_lookup(semantic_layer: dict[str, Any] | None, dataset_name: str) -> dict[str, str]:
    if semantic_layer is None:
        return {}

    alias_lookup: dict[str, str] = {}
    datasets = semantic_layer.get("datasets", {}) or {}
    entities = semantic_layer.get("entities", {}) or {}
    dataset = datasets.get(dataset_name, {}) or {}

    for dimension in dataset.get("dimensions", []) or []:
        name = str(dimension.get("name", "") or "").strip()
        if not name:
            continue
        canonical = f"{dataset_name}.{name}"
        _add_alias(alias_lookup, canonical, canonical)
        _add_alias(alias_lookup, name, canonical)
        for synonym in dimension.get("synonyms", []) or []:
            _add_alias(alias_lookup, str(synonym), canonical)

    for time_dimension in dataset.get("time_dimensions", []) or []:
        name = str(time_dimension.get("name", "") or "").strip()
        if not name:
            continue
        canonical = f"{dataset_name}.{name}"
        _add_alias(alias_lookup, canonical, canonical)
        _add_alias(alias_lookup, name, canonical)
        for synonym in time_dimension.get("synonyms", []) or []:
            _add_alias(alias_lookup, str(synonym), canonical)
        grain = str(time_dimension.get("grain", "") or "").strip().lower()
        if grain == "month":
            for month_alias in ("月份", "month", "month_id", "年月", "月度"):
                _add_alias(alias_lookup, month_alias, canonical)

    join_entities = [
        str(j.get("entity", "") or "").strip()
        for j in dataset.get("joins", []) or []
        if isinstance(j, dict) and str(j.get("entity", "") or "").strip()
    ]
    if not dataset_name:
        join_entities = list(entities.keys())
    for entity_name in join_entities:
        entity = entities.get(entity_name, {}) or {}
        for field in entity.get("fields", []) or []:
            name = str(field.get("name", "") or "").strip()
            if not name:
                continue
            canonical = f"{entity_name}.{name}"
            _add_alias(alias_lookup, canonical, canonical)
            _add_alias(alias_lookup, name, canonical)
            for synonym in field.get("synonyms", []) or []:
                _add_alias(alias_lookup, str(synonym), canonical)

    return alias_lookup


def _infer_metrics_from_features(
    extracted_features: dict[str, Any],
    semantic_layer: dict[str, Any] | None,
) -> list[str]:
    if semantic_layer is None:
        return []

    asked_metrics = [m for m in (extracted_features.get("metrics", []) or []) if isinstance(m, str) and m.strip()]
    if not asked_metrics:
        return []

    normalized_asks = [_normalize_phrase(m) for m in asked_metrics if _normalize_phrase(m)]
    if not normalized_asks:
        return []

    inferred: list[str] = []
    datasets = semantic_layer.get("datasets", {}) or {}
    for dataset_name, dataset in datasets.items():
        for metric in dataset.get("metrics", []) or []:
            metric_name = str(metric.get("name", "") or "").strip()
            if not metric_name:
                continue
            canonical = f"{dataset_name}.{metric_name}"
            aliases = [metric_name]
            aliases.extend(str(s) for s in (metric.get("synonyms", []) or []) if str(s).strip())
            normalized_aliases = [_normalize_phrase(a) for a in aliases if _normalize_phrase(a)]
            if not normalized_aliases:
                continue
            for ask in normalized_asks:
                if any((ask in alias) or (alias in ask) for alias in normalized_aliases):
                    inferred.append(canonical)
                    break

    return _unique_keep_order(inferred)


def _normalize_filter_field(parsed_filter: dict[str, Any], alias_lookup: dict[str, str], raw_filter_text: str) -> dict[str, Any]:
    field = parsed_filter.get("field")
    if not isinstance(field, str) or not field.strip():
        return parsed_filter

    normalized_key = _normalize_key(field)
    canonical = alias_lookup.get(normalized_key)
    if canonical:
        out = dict(parsed_filter)
        out["field"] = canonical
        return out

    # keep canonical-like field names (e.g. branch.region) as-is
    if "." in field.strip():
        return parsed_filter

    return {
        "expr": raw_filter_text.strip(),
        "source": parsed_filter.get("source", "step_b_filters"),
    }


def _build_step_b_filters(
    extracted_features: dict[str, Any],
    semantic_layer: dict[str, Any] | None,
    selected_dataset: str,
) -> list[dict[str, Any]]:
    alias_lookup = _build_field_alias_lookup(semantic_layer, selected_dataset)
    selected_filters: list[dict[str, Any]] = []
    for f in extracted_features.get("filters", []) or []:
        if not isinstance(f, str) or not f.strip():
            continue
        parsed = _parse_filter_expr(f)
        normalized = _normalize_filter_field(parsed, alias_lookup, f)
        selected_filters.append(normalized)
    return selected_filters


def _resolve_time_filter_field(selected_dataset: str, semantic_layer: dict[str, Any] | None) -> str:
    if not selected_dataset or semantic_layer is None:
        return "calendar.biz_date"
    dataset = (semantic_layer.get("datasets", {}) or {}).get(selected_dataset, {}) or {}
    for time_dimension in dataset.get("time_dimensions", []) or []:
        name = str(time_dimension.get("name", "") or "").strip()
        if name:
            return f"{selected_dataset}.{name}"
    return "calendar.biz_date"


def _resolve_time_dimension_grain(selected_dataset: str, semantic_layer: dict[str, Any] | None) -> str:
    if not selected_dataset or semantic_layer is None:
        return ""
    dataset = (semantic_layer.get("datasets", {}) or {}).get(selected_dataset, {}) or {}
    for time_dimension in dataset.get("time_dimensions", []) or []:
        grain = str(time_dimension.get("grain", "") or "").strip().lower()
        if grain:
            return grain
    return ""


def _normalize_time_bound_value(value: str, grain: str) -> str:
    text = value.strip()
    if grain == "month":
        month_match = re.match(r"^\d{4}-\d{2}(?:-\d{2})?$", text)
        if month_match:
            return text[:7]
    return text


def _extract_month_tokens(query_text: Any) -> list[str]:
    if not isinstance(query_text, str) or not query_text.strip():
        return []
    months = re.findall(r"(\d{4})[-年/](\d{1,2})", query_text)
    normalized: list[str] = []
    for year, month in months:
        mm = month.zfill(2)
        if 1 <= int(mm) <= 12:
            normalized.append(f"{year}-{mm}")
    return _unique_keep_order(normalized)


def _build_time_filter_from_bounds(
    time_start: str,
    time_end: str,
    selected_dataset: str,
    semantic_layer: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if not isinstance(time_start, str) or not isinstance(time_end, str):
        return []
    start = time_start.strip()
    end = time_end.strip()
    if not start or not end:
        return []

    grain = _resolve_time_dimension_grain(selected_dataset, semantic_layer)
    normalized_start = _normalize_time_bound_value(start, grain)
    normalized_end = _normalize_time_bound_value(end, grain)

    return [
        {
            "field": _resolve_time_filter_field(selected_dataset, semantic_layer),
            "op": "between",
            "value": [normalized_start, normalized_end],
            "source": "step_b_time_bounds",
        }
    ]


def _prune_conflicting_month_filters(selected_filters: list[dict[str, Any]], month_field: str) -> list[dict[str, Any]]:
    pruned: list[dict[str, Any]] = []
    for f in selected_filters:
        if not isinstance(f, dict):
            continue
        source = str(f.get("source", "") or "")
        field = str(f.get("field", "") or "")
        op = str(f.get("op", "") or "").strip().lower()
        expr = str(f.get("expr", "") or "")
        if source == "step_b_time_bounds":
            continue
        if field == month_field and op in {"=", "between", "in"} and source == "step_b_filters":
            continue
        if source == "step_b_filters" and "月份" in expr:
            continue
        pruned.append(f)
    return pruned


def build_semantic_plan(
    extracted_features: dict[str, Any],
    token_hits: dict[str, Any],
    semantic_layer: dict[str, Any] | None = None,
) -> dict[str, Any]:
    matches = token_hits.get("matches", []) or []

    selected_metrics = _canonical_candidates(matches, "metric")
    selected_dimensions = _dimension_candidates(matches)
    dataset_candidates = _dataset_candidates(matches)
    primary_dataset = dataset_candidates[0] if dataset_candidates else ""

    selected_filters = _build_step_b_filters(extracted_features, semantic_layer, primary_dataset)
    selected_filters.extend(
        _build_time_filter_from_bounds(
            str(extracted_features.get("time_start", "") or ""),
            str(extracted_features.get("time_end", "") or ""),
            primary_dataset,
            semantic_layer,
        )
    )

    grain = _resolve_time_dimension_grain(primary_dataset, semantic_layer)
    month_tokens = _extract_month_tokens(extracted_features.get("query_text", ""))
    if grain == "month" and len(month_tokens) >= 2:
        month_field = _resolve_time_filter_field(primary_dataset, semantic_layer)
        selected_filters = _prune_conflicting_month_filters(selected_filters, month_field)
        selected_filters.append(
            {
                "field": month_field,
                "op": "between",
                "value": [month_tokens[0], month_tokens[-1]],
                "source": "query_text_month_bounds",
            }
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


def _infer_dimensions_from_features(
    extracted_features: dict[str, Any],
    selected_dataset: str,
    semantic_layer: dict[str, Any] | None,
) -> list[str]:
    if not selected_dataset or semantic_layer is None:
        return []

    asked_dimensions = extracted_features.get("dimensions", []) or []
    if not isinstance(asked_dimensions, list) or not any(str(d).strip() for d in asked_dimensions):
        return []

    dataset = (semantic_layer.get("datasets", {}) or {}).get(selected_dataset, {}) or {}
    inferred: list[str] = []

    for time_dimension in dataset.get("time_dimensions", []) or []:
        name = str(time_dimension.get("name", "") or "").strip()
        if name:
            inferred.append(f"{selected_dataset}.{name}")
            break

    return inferred


def _canonicalize_dimensions_for_dataset(
    selected_dimensions: list[str],
    selected_dataset: str,
    semantic_layer: dict[str, Any] | None,
) -> list[str]:
    if not selected_dimensions or not selected_dataset or semantic_layer is None:
        return selected_dimensions

    dataset = (semantic_layer.get("datasets", {}) or {}).get(selected_dataset, {}) or {}
    time_dimensions = dataset.get("time_dimensions", []) or []
    if not time_dimensions:
        return selected_dimensions

    primary_time_name = str(time_dimensions[0].get("name", "") or "").strip()
    if not primary_time_name:
        return selected_dimensions
    dataset_time_canonical = f"{selected_dataset}.{primary_time_name}"

    normalized: list[str] = []
    for dim in selected_dimensions:
        if dim == "calendar.biz_date":
            normalized.append(dataset_time_canonical)
        else:
            normalized.append(dim)
    return _unique_keep_order(normalized)


def _filter_dimensions_for_dataset(
    selected_dimensions: list[str],
    selected_dataset: str,
    semantic_layer: dict[str, Any] | None,
) -> list[str]:
    if not selected_dimensions or not selected_dataset or semantic_layer is None:
        return selected_dimensions

    entities = semantic_layer.get("entities", {}) or {}
    filtered: list[str] = []
    for dim in selected_dimensions:
        if not isinstance(dim, str) or "." not in dim:
            continue
        owner = dim.split(".", 1)[0]
        if owner == selected_dataset or owner in entities:
            filtered.append(dim)
    return _unique_keep_order(filtered)


def _sanitize_llm_filters(
    llm_filters: list[Any],
    semantic_layer: dict[str, Any] | None,
    selected_dataset: str,
) -> list[dict[str, Any]]:
    alias_lookup = _build_field_alias_lookup(semantic_layer, selected_dataset)
    valid_canonical_fields = set(alias_lookup.values())
    allowed_binary_ops = {"between", "=", "!=", ">", ">=", "<", "<=", "in"}
    allowed_unary_ops = {"is null", "is not null"}
    sanitized: list[dict[str, Any]] = []

    for item in llm_filters:
        if not isinstance(item, dict):
            continue

        copied = dict(item)
        field = copied.get("field")
        canonical_field = ""
        if isinstance(field, str) and field.strip():
            normalized_field = _normalize_key(field)
            canonical = alias_lookup.get(normalized_field)
            if canonical:
                canonical_field = canonical
            elif "." in field.strip() and field.strip() in valid_canonical_fields:
                canonical_field = field.strip()
            else:
                continue
            copied["field"] = canonical_field

        op = str(copied.get("op", "") or "").strip().lower().replace("_", " ")
        if op:
            if op in allowed_unary_ops:
                if not canonical_field:
                    continue
                copied = {"field": canonical_field, "op": op}
            elif op not in allowed_binary_ops:
                continue

        sanitized.append(copied)

    return sanitized


def merge_llm_selection_into_plan(
    llm_selection: dict[str, Any],
    token_hits: dict[str, Any],
    extracted_features: dict[str, Any],
    semantic_layer: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Deterministically assemble semantic plan from Step C candidates + LLM selection."""
    matches = token_hits.get("matches", []) or []
    metric_candidates = _canonical_candidates(matches, "metric")
    dimension_candidates = _dimension_candidates(matches)
    dataset_candidates = _dataset_candidates(matches)

    selected_metrics = _safe_selected_values(metric_candidates, llm_selection.get("selected_metrics", []) or [])
    selected_dimensions = _safe_selected_values(dimension_candidates, llm_selection.get("selected_dimensions", []) or [])
    selected_datasets = _safe_selected_values(dataset_candidates, llm_selection.get("selected_dataset_candidates", []) or [])

    # fallback: if LLM did not select, use deterministic candidate order from Step C
    if not selected_metrics:
        selected_metrics = metric_candidates
    if not selected_metrics:
        selected_metrics = _infer_metrics_from_features(extracted_features, semantic_layer)
    if not selected_datasets:
        selected_datasets = dataset_candidates
    if not selected_datasets and selected_metrics:
        selected_datasets = _unique_keep_order([m.split(".", 1)[0] for m in selected_metrics if "." in m])

    primary_dataset = selected_datasets[0] if selected_datasets else ""

    if not selected_dimensions:
        selected_dimensions = dimension_candidates
    if not selected_dimensions:
        selected_dimensions = _infer_dimensions_from_features(extracted_features, primary_dataset, semantic_layer)
    selected_dimensions = _filter_dimensions_for_dataset(selected_dimensions, primary_dataset, semantic_layer)
    selected_dimensions = _canonicalize_dimensions_for_dataset(selected_dimensions, primary_dataset, semantic_layer)

    selected_filters: list[dict[str, Any]] = []
    llm_filters = llm_selection.get("selected_filters", []) or []
    if isinstance(llm_filters, list):
        selected_filters = _sanitize_llm_filters(llm_filters, semantic_layer, primary_dataset)

    if not selected_filters:
        selected_filters = _build_step_b_filters(extracted_features, semantic_layer, primary_dataset)

    selected_filters.extend(
        _build_time_filter_from_bounds(
            str(extracted_features.get("time_start", "") or ""),
            str(extracted_features.get("time_end", "") or ""),
            primary_dataset,
            semantic_layer,
        )
    )

    grain = _resolve_time_dimension_grain(primary_dataset, semantic_layer)
    month_tokens = _extract_month_tokens(extracted_features.get("query_text", ""))
    if grain == "month" and len(month_tokens) >= 2:
        month_field = _resolve_time_filter_field(primary_dataset, semantic_layer)
        selected_filters = _prune_conflicting_month_filters(selected_filters, month_field)
        selected_filters.append(
            {
                "field": month_field,
                "op": "between",
                "value": [month_tokens[0], month_tokens[-1]],
                "source": "query_text_month_bounds",
            }
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
