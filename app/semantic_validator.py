from __future__ import annotations

from typing import Any


def _dataset_from_canonical_name(canonical_name: str) -> str | None:
    if not isinstance(canonical_name, str) or "." not in canonical_name:
        return None
    dataset, _ = canonical_name.split(".", 1)
    return dataset or None


def _collect_selected_datasets(
    enhanced_plan: dict[str, Any],
    semantic_layer: dict[str, Any] | None = None,
) -> list[str]:
    candidates = enhanced_plan.get("selected_dataset_candidates", []) or []
    selected: list[str] = [c for c in candidates if isinstance(c, str) and c]

    datasets = ((semantic_layer or {}).get("datasets", {}) or {})
    entities = ((semantic_layer or {}).get("entities", {}) or {})

    for key in ("selected_metrics", "selected_dimensions"):
        for canonical_name in enhanced_plan.get(key, []) or []:
            dataset = _dataset_from_canonical_name(canonical_name)
            if not dataset:
                continue
            # only dataset prefixes should participate in multi-dataset checks;
            # entity dimensions (e.g. branch.branch_name) are valid joined fields.
            if entities and dataset in entities:
                continue
            if datasets and dataset not in datasets:
                continue
            if dataset not in selected:
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


def _build_valid_canonical_sets(semantic_layer: dict[str, Any]) -> tuple[set[str], set[str]]:
    metric_set: set[str] = set()
    dimension_set: set[str] = set()

    datasets = semantic_layer.get("datasets", {}) or {}
    entities = semantic_layer.get("entities", {}) or {}

    for dataset_name, dataset in datasets.items():
        for metric in dataset.get("metrics", []) or []:
            name = str(metric.get("name", "") or "").strip()
            if name:
                metric_set.add(f"{dataset_name}.{name}")
        for dimension in dataset.get("dimensions", []) or []:
            name = str(dimension.get("name", "") or "").strip()
            if name:
                dimension_set.add(f"{dataset_name}.{name}")
        for time_dimension in dataset.get("time_dimensions", []) or []:
            name = str(time_dimension.get("name", "") or "").strip()
            if name:
                dimension_set.add(f"{dataset_name}.{name}")

    for entity_name, entity in entities.items():
        for field in entity.get("fields", []) or []:
            name = str(field.get("name", "") or "").strip()
            if name:
                dimension_set.add(f"{entity_name}.{name}")

    return metric_set, dimension_set


def _has_compilable_select_item(enhanced_plan: dict[str, Any], semantic_layer: dict[str, Any]) -> bool:
    datasets = enhanced_plan.get("selected_dataset_candidates", []) or []
    dataset_name = str(datasets[0]).strip() if datasets else ""
    if not dataset_name:
        return False

    ds = (semantic_layer.get("datasets", {}) or {}).get(dataset_name, {}) or {}
    metric_names = {
        f"{dataset_name}.{str(metric.get('name', '')).strip()}"
        for metric in ds.get("metrics", []) or []
        if str(metric.get("name", "") or "").strip()
    }
    dimension_names = {
        f"{dataset_name}.{str(dimension.get('name', '')).strip()}"
        for dimension in ds.get("dimensions", []) or []
        if str(dimension.get("name", "") or "").strip()
    }

    selected_metrics = [x for x in enhanced_plan.get("selected_metrics", []) or [] if isinstance(x, str)]
    selected_dimensions = [x for x in enhanced_plan.get("selected_dimensions", []) or [] if isinstance(x, str)]

    if any(item in metric_names for item in selected_metrics):
        return True
    if any(item in dimension_names for item in selected_dimensions):
        return True
    return False


def validate_semantic_plan(
    enhanced_plan: dict[str, Any],
    token_hits: dict[str, Any],
    governance_limits: dict[str, Any],
    semantic_layer: dict[str, Any] | None = None,
) -> dict[str, Any]:
    errors: list[str] = []
    error_codes: list[str] = []
    blocked = token_hits.get("blocked_matches", []) or []

    def _add_error(code: str, message: str) -> None:
        if code not in error_codes:
            error_codes.append(code)
        errors.append(message)

    if blocked:
        _add_error("BLOCKED_MATCH", "命中敏感欄位（allowed=false），請改用非敏感欄位或彙總指標。")
        return {
            "ok": False,
            "errors": errors,
            "error_codes": error_codes,
        }

    require_time_filter = bool(governance_limits.get("require_time_filter", False))
    filters = enhanced_plan.get("selected_filters", []) or []
    has_selection_context = any(
        bool(enhanced_plan.get(key, []) or [])
        for key in ("selected_metrics", "selected_dimensions", "selected_dataset_candidates")
    )
    if require_time_filter and has_selection_context and not filters:
        _add_error("TIME_FILTER_REQUIRED", "查詢需要時間條件（require_time_filter=true），請補充時間範圍。")

    time_axis = enhanced_plan.get("time_axis", {}) or {}
    if time_axis.get("has_time_filter"):
        if not time_axis.get("start_date") or not time_axis.get("end_date"):
            _add_error("TIME_AXIS_INCOMPLETE", "時間軸解析不完整，缺少 start_date/end_date。")

    selected_metrics = enhanced_plan.get("selected_metrics", []) or []
    selected_dimensions = enhanced_plan.get("selected_dimensions", []) or []
    if not selected_metrics and not selected_dimensions:
        _add_error("EMPTY_SELECTION", "尚未選到可用的指標/維度，請補充查詢條件。")

    selected_datasets = _collect_selected_datasets(enhanced_plan, semantic_layer)
    if len(selected_datasets) > 1 and semantic_layer is not None:
        if not _has_join_path(semantic_layer, selected_datasets):
            ds_list = ", ".join(selected_datasets)
            _add_error(
                "MULTI_DATASET_NO_JOIN_PATH",
                f"多資料集無法透過共同維度連接（datasets: {ds_list}），請改用同一資料集的指標/維度。",
            )

    if semantic_layer is not None:
        valid_metrics, valid_dimensions = _build_valid_canonical_sets(semantic_layer)
        invalid_metrics = [m for m in selected_metrics if isinstance(m, str) and m not in valid_metrics]
        invalid_dimensions = [d for d in selected_dimensions if isinstance(d, str) and d not in valid_dimensions]
        invalid_filter_fields = []
        for f in filters:
            if not isinstance(f, dict):
                continue
            field = f.get("field")
            if not isinstance(field, str):
                continue
            normalized_field = field.strip()
            if not normalized_field or "." not in normalized_field:
                continue
            if normalized_field not in valid_dimensions:
                invalid_filter_fields.append(normalized_field)

        if invalid_metrics or invalid_dimensions or invalid_filter_fields:
            invalid_text = ", ".join(invalid_metrics + invalid_dimensions + invalid_filter_fields)
            _add_error("INVALID_CANONICAL_REF", f"選取了語意層不存在的欄位：{invalid_text}")

        first_dataset = ""
        datasets = enhanced_plan.get("selected_dataset_candidates", []) or []
        if datasets and isinstance(datasets[0], str):
            first_dataset = datasets[0]
        if first_dataset:
            entities = semantic_layer.get("entities", {}) or {}
            foreign_metrics = [
                m
                for m in selected_metrics
                if isinstance(m, str)
                and "." in m
                and m.split(".", 1)[0] != first_dataset
            ]
            foreign_dimensions = [
                d
                for d in selected_dimensions
                if isinstance(d, str)
                and "." in d
                and d.split(".", 1)[0] != first_dataset
                and d.split(".", 1)[0] not in entities
            ]
            if foreign_metrics or foreign_dimensions:
                _add_error("DATASET_MISMATCH", "已選欄位與 selected_dataset_candidates[0] 不一致，請改用同一資料集。")

        for idx, f in enumerate(filters):
            if not isinstance(f, dict):
                _add_error("INVALID_FILTER_SHAPE", f"第 {idx+1} 個過濾條件格式錯誤，需為物件。")
                continue
            op = str(f.get("op", "") or "").strip().lower()
            value = f.get("value")
            has_expr = isinstance(f.get("expr"), str) and bool(f.get("expr", "").strip())
            if op == "between":
                if not isinstance(value, list) or len(value) != 2:
                    _add_error("INVALID_FILTER_BETWEEN", f"第 {idx+1} 個 between 條件必須提供兩個值。")
            elif op in {"=", "!=", ">", ">=", "<", "<="}:
                if value is None:
                    _add_error("INVALID_FILTER_VALUE", f"第 {idx+1} 個過濾條件缺少 value。")
            elif op == "in":
                if not isinstance(value, list) or not value:
                    _add_error("INVALID_FILTER_VALUE", f"第 {idx+1} 個 in 條件需提供至少一個值。")
            elif op in {"is null", "is not null"}:
                pass
            elif not has_expr:
                _add_error("INVALID_FILTER_SHAPE", f"第 {idx+1} 個過濾條件缺少可用欄位（field/op/value 或 expr）。")

        if not _has_compilable_select_item(enhanced_plan, semantic_layer):
            _add_error("NO_COMPILABLE_SELECT", "目前選取內容無法編譯成有效 SELECT，請改選同資料集內的指標/維度。")

    return {
        "ok": len(errors) == 0,
        "errors": errors,
        "error_codes": error_codes,
    }
