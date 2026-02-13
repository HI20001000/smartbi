from __future__ import annotations

from typing import Any


def _dataset_from_canonical_name(canonical_name: str) -> str | None:
    if not isinstance(canonical_name, str) or "." not in canonical_name:
        return None
    dataset, _ = canonical_name.split(".", 1)
    return dataset or None


def _collect_selected_datasets(enhanced_plan: dict[str, Any]) -> list[str]:
    candidates = enhanced_plan.get("selected_dataset_candidates", []) or []
    selected: list[str] = [c for c in candidates if isinstance(c, str) and c]

    for key in ("selected_metrics", "selected_dimensions"):
        for canonical_name in enhanced_plan.get(key, []) or []:
            dataset = _dataset_from_canonical_name(canonical_name)
            if dataset and dataset not in selected:
                selected.append(dataset)
    return selected


def _has_join_path(semantic_layer: dict[str, Any], selected_datasets: list[str]) -> bool:
    datasets = semantic_layer.get("datasets", {}) or {}
    join_entities_sets: list[set[str]] = []

    for dataset_name in selected_datasets:
        dataset_def = datasets.get(dataset_name, {}) or {}
        joins = dataset_def.get("joins", []) or []
        join_entities = {
            j.get("entity")
            for j in joins
            if isinstance(j, dict) and isinstance(j.get("entity"), str) and j.get("entity")
        }
        if not join_entities:
            return False
        join_entities_sets.append(join_entities)

    if not join_entities_sets:
        return True

    common_join_entities = set.intersection(*join_entities_sets)
    return bool(common_join_entities)


def validate_semantic_plan(
    enhanced_plan: dict[str, Any],
    token_hits: dict[str, Any],
    governance_limits: dict[str, Any],
    semantic_layer: dict[str, Any] | None = None,
) -> dict[str, Any]:
    errors: list[str] = []
    blocked = token_hits.get("blocked_matches", []) or []

    if blocked:
        errors.append("命中敏感欄位（allowed=false），請改用非敏感欄位或彙總指標。")

    require_time_filter = bool(governance_limits.get("require_time_filter", False))
    filters = enhanced_plan.get("selected_filters", []) or []
    if require_time_filter and not filters:
        errors.append("查詢需要時間條件（require_time_filter=true），請補充時間範圍。")

    time_axis = enhanced_plan.get("time_axis", {}) or {}
    if time_axis.get("has_time_filter"):
        if not time_axis.get("start_date") or not time_axis.get("end_date"):
            errors.append("時間軸解析不完整，缺少 start_date/end_date。")

    selected_metrics = enhanced_plan.get("selected_metrics", []) or []
    selected_dimensions = enhanced_plan.get("selected_dimensions", []) or []
    if not selected_metrics and not selected_dimensions:
        errors.append("尚未選到可用的指標/維度，請補充查詢條件。")

    selected_datasets = _collect_selected_datasets(enhanced_plan)
    if len(selected_datasets) > 1 and semantic_layer is not None:
        if not _has_join_path(semantic_layer, selected_datasets):
            ds_list = ", ".join(selected_datasets)
            errors.append(
                f"多資料集無法透過共同維度連接（datasets: {ds_list}），請改用同一資料集的指標/維度。"
            )

    return {
        "ok": len(errors) == 0,
        "errors": errors,
    }
