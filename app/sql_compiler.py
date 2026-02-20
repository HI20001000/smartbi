from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SemanticLookup:
    dataset_name: str
    metric_expr_by_name: dict[str, str]
    metric_type_by_name: dict[str, str]
    dimension_expr_by_name: dict[str, str]
    first_time_expr: str
    from_clause: str
    join_clauses: list[tuple[str, str]]
    calendar_table: str
    calendar_join_on: str


def _quote_sql_value(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).replace("'", "''")
    return f"'{text}'"


def _build_semantic_lookup(dataset_name: str, semantic_layer: dict[str, Any]) -> SemanticLookup:
    datasets = semantic_layer.get("datasets", {}) or {}
    entities = semantic_layer.get("entities", {}) or {}
    dataset = datasets.get(dataset_name, {}) or {}

    metric_expr_by_name: dict[str, str] = {}
    metric_type_by_name: dict[str, str] = {}
    for metric in dataset.get("metrics", []) or []:
        canonical = f"{dataset_name}.{metric.get('name', '')}"
        expr = str(metric.get("expr", "") or "").strip()
        metric_type = str(metric.get("type", "") or "").strip().lower()
        if canonical and expr:
            metric_expr_by_name[canonical] = expr
            metric_type_by_name[canonical] = metric_type

    dimension_expr_by_name: dict[str, str] = {}
    for dimension in dataset.get("dimensions", []) or []:
        canonical = f"{dataset_name}.{dimension.get('name', '')}"
        expr = str(dimension.get("expr", "") or "").strip()
        if canonical and expr:
            dimension_expr_by_name[canonical] = expr

    for time_dimension in dataset.get("time_dimensions", []) or []:
        canonical = f"{dataset_name}.{time_dimension.get('name', '')}"
        expr = str(time_dimension.get("expr", "") or "").strip()
        if canonical and expr:
            dimension_expr_by_name[canonical] = expr

    first_time_expr = ""
    for time_dimension in dataset.get("time_dimensions", []) or []:
        expr = str(time_dimension.get("expr", "") or "").strip()
        if expr:
            first_time_expr = expr
            break

    for entity_name, entity in entities.items():
        for field in entity.get("fields", []) or []:
            canonical = f"{entity_name}.{field.get('name', '')}"
            expr = str(field.get("expr", "") or "").strip()
            if canonical and expr:
                dimension_expr_by_name[canonical] = expr

    join_clauses: list[tuple[str, str]] = []
    calendar_table = str((((semantic_layer.get("entities", {}) or {}).get("calendar", {}) or {}).get("table", "") or "")).strip()
    calendar_join_on = ""
    for join in dataset.get("joins", []) or []:
        entity_name = join.get("entity")
        on_raw = join.get("on")
        # YAML 1.1 may parse key `on` as boolean True when unquoted.
        if on_raw in (None, "") and True in join:
            on_raw = join.get(True)
        on_clause = str(on_raw or "").strip()
        if not entity_name or not on_clause:
            continue
        entity = entities.get(entity_name, {}) or {}
        table = str(entity.get("table", "") or "").strip()
        if not table:
            continue
        join_clauses.append((str(entity_name), f"LEFT JOIN {table} ON {on_clause}"))
        if str(entity_name) == "calendar":
            calendar_join_on = on_clause

    return SemanticLookup(
        dataset_name=dataset_name,
        metric_expr_by_name=metric_expr_by_name,
        metric_type_by_name=metric_type_by_name,
        dimension_expr_by_name=dimension_expr_by_name,
        first_time_expr=first_time_expr,
        from_clause=str(dataset.get("from", "") or "").strip(),
        join_clauses=join_clauses,
        calendar_table=calendar_table,
        calendar_join_on=calendar_join_on,
    )


def _normalize_metric_expr(expr: str, metric_type: str) -> str:
    metric_type = (metric_type or "").strip().lower()
    if metric_type == "sum":
        return f"SUM({expr})"
    if metric_type == "avg":
        return f"AVG({expr})"
    if metric_type == "count_distinct":
        return f"COUNT(DISTINCT {expr})"
    if metric_type == "count":
        return f"COUNT({expr})"
    return expr


def _should_use_calendar_skeleton(lookup: SemanticLookup, group_by_parts: list[str]) -> bool:
    if lookup.dataset_name != "deposit_balance_daily":
        return False
    if not lookup.calendar_table or not lookup.calendar_join_on:
        return False
    if not lookup.first_time_expr:
        return False
    return lookup.first_time_expr in group_by_parts


def compile_sql_from_semantic_plan(
    enhanced_plan: dict[str, Any],
    semantic_layer: dict[str, Any],
) -> str:
    datasets = enhanced_plan.get("selected_dataset_candidates", []) or []
    if not datasets:
        raise ValueError("No dataset candidates available for SQL compilation.")

    dataset_name = datasets[0]
    lookup = _build_semantic_lookup(dataset_name, semantic_layer)
    if not lookup.from_clause:
        raise ValueError(f"Dataset '{dataset_name}' has no from clause.")

    select_parts: list[str] = []
    group_by_parts: list[str] = []

    for canonical in enhanced_plan.get("selected_dimensions", []) or []:
        expr = lookup.dimension_expr_by_name.get(canonical)
        if not expr:
            continue
        alias = canonical.replace(".", "_")
        select_parts.append(f"{expr} AS {alias}")
        group_by_parts.append(expr)

    use_calendar_skeleton = _should_use_calendar_skeleton(lookup, group_by_parts)

    for canonical in enhanced_plan.get("selected_metrics", []) or []:
        expr = lookup.metric_expr_by_name.get(canonical)
        if not expr:
            continue
        metric_type = lookup.metric_type_by_name.get(canonical, "")
        metric_expr = _normalize_metric_expr(expr, metric_type)
        if use_calendar_skeleton and metric_type in {"sum", "avg", "count", "count_distinct"}:
            metric_expr = f"COALESCE({metric_expr}, 0)"
        alias = canonical.replace(".", "_")
        select_parts.append(f"{metric_expr} AS {alias}")

    if not select_parts:
        raise ValueError("No valid dimensions/metrics found for SELECT clause.")

    where_parts: list[str] = []
    for f in enhanced_plan.get("selected_filters", []) or []:
        if not isinstance(f, dict):
            continue
        field = str(f.get("field", "") or "").strip()
        op = str(f.get("op", "") or "").strip().lower()
        value = f.get("value")
        field_expr = lookup.dimension_expr_by_name.get(field, field)

        if op == "between" and isinstance(value, list) and len(value) == 2:
            where_parts.append(
                f"{field_expr} BETWEEN {_quote_sql_value(value[0])} AND {_quote_sql_value(value[1])}"
            )
        elif op in {"=", "!=", ">", ">=", "<", "<="}:
            where_parts.append(f"{field_expr} {op} {_quote_sql_value(value)}")
        elif op == "in" and isinstance(value, list) and value:
            value_sql = ", ".join(_quote_sql_value(v) for v in value)
            where_parts.append(f"{field_expr} IN ({value_sql})")
        elif isinstance(f.get("expr"), str) and f["expr"].strip():
            where_parts.append(f["expr"].strip())

    sql_lines = [f"SELECT {', '.join(select_parts)}"]
    if use_calendar_skeleton:
        sql_lines.append(f"FROM {lookup.calendar_table}")
        sql_lines.append(f"LEFT JOIN {lookup.from_clause} ON {lookup.calendar_join_on}")
        for entity_name, join_clause in lookup.join_clauses:
            if entity_name == "calendar":
                continue
            sql_lines.append(join_clause)
    else:
        sql_lines.append(f"FROM {lookup.from_clause}")
        sql_lines.extend(join_clause for _, join_clause in lookup.join_clauses)

    if where_parts:
        sql_lines.append(f"WHERE {' AND '.join(where_parts)}")

    if group_by_parts:
        sql_lines.append(f"GROUP BY {', '.join(group_by_parts)}")

    return "\n".join(sql_lines)
