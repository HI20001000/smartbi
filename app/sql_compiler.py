from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SemanticLookup:
    metric_expr_by_name: dict[str, str]
    dimension_expr_by_name: dict[str, str]
    from_clause: str
    join_clauses: list[str]


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
    for metric in dataset.get("metrics", []) or []:
        canonical = f"{dataset_name}.{metric.get('name', '')}"
        expr = str(metric.get("expr", "") or "").strip()
        if canonical and expr:
            metric_expr_by_name[canonical] = expr

    dimension_expr_by_name: dict[str, str] = {}
    for dimension in dataset.get("dimensions", []) or []:
        canonical = f"{dataset_name}.{dimension.get('name', '')}"
        expr = str(dimension.get("expr", "") or "").strip()
        if canonical and expr:
            dimension_expr_by_name[canonical] = expr

    for entity_name, entity in entities.items():
        for field in entity.get("fields", []) or []:
            canonical = f"{entity_name}.{field.get('name', '')}"
            expr = str(field.get("expr", "") or "").strip()
            if canonical and expr:
                dimension_expr_by_name[canonical] = expr

    join_clauses: list[str] = []
    for join in dataset.get("joins", []) or []:
        entity_name = join.get("entity")
        on_clause = str(join.get("on", "") or "").strip()
        if not entity_name or not on_clause:
            continue
        entity = entities.get(entity_name, {}) or {}
        table = str(entity.get("table", "") or "").strip()
        if not table:
            continue
        join_clauses.append(f"LEFT JOIN {table} ON {on_clause}")

    return SemanticLookup(
        metric_expr_by_name=metric_expr_by_name,
        dimension_expr_by_name=dimension_expr_by_name,
        from_clause=str(dataset.get("from", "") or "").strip(),
        join_clauses=join_clauses,
    )


def compile_sql_from_semantic_plan(
    enhanced_plan: dict[str, Any],
    semantic_layer: dict[str, Any],
    limit: int = 200,
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

    for canonical in enhanced_plan.get("selected_metrics", []) or []:
        expr = lookup.metric_expr_by_name.get(canonical)
        if not expr:
            continue
        alias = canonical.replace(".", "_")
        select_parts.append(f"{expr} AS {alias}")

    if not select_parts:
        raise ValueError("No valid dimensions/metrics found for SELECT clause.")

    where_parts: list[str] = []
    for f in enhanced_plan.get("selected_filters", []) or []:
        if not isinstance(f, dict):
            continue
        field = str(f.get("field", "") or "").strip()
        op = str(f.get("op", "") or "").strip().lower()
        value = f.get("value")

        if op == "between" and isinstance(value, list) and len(value) == 2:
            where_parts.append(
                f"{field} BETWEEN {_quote_sql_value(value[0])} AND {_quote_sql_value(value[1])}"
            )
        elif op in {"=", "!=", ">", ">=", "<", "<="}:
            where_parts.append(f"{field} {op} {_quote_sql_value(value)}")
        elif op == "in" and isinstance(value, list) and value:
            value_sql = ", ".join(_quote_sql_value(v) for v in value)
            where_parts.append(f"{field} IN ({value_sql})")
        elif isinstance(f.get("expr"), str) and f["expr"].strip():
            where_parts.append(f["expr"].strip())

    sql_lines = [f"SELECT {', '.join(select_parts)}", f"FROM {lookup.from_clause}"]
    sql_lines.extend(lookup.join_clauses)

    if where_parts:
        sql_lines.append(f"WHERE {' AND '.join(where_parts)}")

    if group_by_parts:
        sql_lines.append(f"GROUP BY {', '.join(group_by_parts)}")

    sql_lines.append(f"LIMIT {int(limit)}")
    return "\n".join(sql_lines)
