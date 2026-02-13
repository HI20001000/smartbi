from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class SemanticEntry:
    object_type: str
    canonical_name: str
    aliases: tuple[str, ...]
    dataset: str | None = None
    entity: str | None = None
    table: str | None = None
    allowed: bool = True


class SemanticTokenMatcher:
    """Token matcher over semantic-layer metadata.

    This matcher is intentionally deterministic: it performs exact token/alias
    matching against semantic objects extracted from the YAML semantic layer.
    """

    def __init__(self, semantic_yaml_path: str | Path):
        self.semantic_yaml_path = Path(semantic_yaml_path)
        (
            self.entries,
            self.metric_index,
            self.dimension_index,
            self.filter_field_index,
            self.time_index,
        ) = self._build_entries_and_indexes()

    def _build_entries_and_indexes(
        self,
    ) -> tuple[
        list[SemanticEntry],
        dict[str, dict[str, Any]],
        dict[str, dict[str, Any]],
        dict[str, dict[str, Any]],
        dict[str, dict[str, str]],
    ]:
        with self.semantic_yaml_path.open("r", encoding="utf-8") as f:
            semantic = yaml.safe_load(f) or {}

        layer = semantic.get("semantic_layer", {})
        entries: list[SemanticEntry] = []
        metric_index: dict[str, dict[str, Any]] = {}
        dimension_index: dict[str, dict[str, Any]] = {}
        filter_field_index: dict[str, dict[str, Any]] = {}
        time_index: dict[str, dict[str, str]] = {}

        entities = layer.get("entities", {})
        for entity_name, entity in entities.items():
            table = entity.get("table")

            for field in entity.get("fields", []):
                field_aliases = self._collect_aliases(field)
                entries.append(
                    SemanticEntry(
                        object_type="field",
                        canonical_name=f"{entity_name}.{field.get('name')}",
                        aliases=tuple(field_aliases),
                        entity=entity_name,
                        table=table,
                        allowed=True,
                    )
                )
                field_name = str(field.get("name", "") or "")
                field_expr = str(field.get("expr", "") or "")
                if field_name and field_expr:
                    dimension_payload = {"name": field_name, "expr": field_expr}
                    filter_payload = {"expr": field_expr}
                    for alias in field_aliases:
                        dimension_index.setdefault(alias, dimension_payload)
                        filter_field_index.setdefault(alias, filter_payload)

            for field in entity.get("sensitive_fields", []):
                entries.append(
                    SemanticEntry(
                        object_type="sensitive_field",
                        canonical_name=f"{entity_name}.{field.get('name')}",
                        aliases=tuple(self._collect_aliases(field)),
                        entity=entity_name,
                        table=table,
                        allowed=bool(field.get("allowed", False)),
                    )
                )

        datasets = layer.get("datasets", {})
        for dataset_name, dataset in datasets.items():
            for time_dimension in dataset.get("time_dimensions", []):
                time_aliases = self._collect_aliases(time_dimension)
                entries.append(
                    SemanticEntry(
                        object_type="time_dimension",
                        canonical_name=f"{dataset_name}.{time_dimension.get('name')}",
                        aliases=tuple(time_aliases),
                        dataset=dataset_name,
                        allowed=True,
                    )
                )
                time_expr = str(time_dimension.get("expr", "") or "")
                if dataset_name and time_expr and dataset_name not in time_index:
                    time_index[dataset_name] = {"time_field": time_expr}

            for metric in dataset.get("metrics", []):
                metric_aliases = self._collect_aliases(metric)
                entries.append(
                    SemanticEntry(
                        object_type="metric",
                        canonical_name=f"{dataset_name}.{metric.get('name')}",
                        aliases=tuple(metric_aliases),
                        dataset=dataset_name,
                        allowed=True,
                    )
                )
                metric_name = str(metric.get("name", "") or "")
                metric_agg = str(metric.get("type", "") or "")
                metric_expr = str(metric.get("expr", "") or "")
                if metric_name and metric_agg and metric_expr and dataset_name:
                    metric_payload = {
                        "dataset": dataset_name,
                        "name": metric_name,
                        "agg": metric_agg,
                        "expr": metric_expr,
                    }
                    for alias in metric_aliases:
                        metric_index.setdefault(alias, metric_payload)

            for dimension in dataset.get("dimensions", []):
                dimension_aliases = self._collect_aliases(dimension)
                entries.append(
                    SemanticEntry(
                        object_type="dimension",
                        canonical_name=f"{dataset_name}.{dimension.get('name')}",
                        aliases=tuple(dimension_aliases),
                        dataset=dataset_name,
                        allowed=True,
                    )
                )
                dimension_name = str(dimension.get("name", "") or "")
                dimension_expr = str(dimension.get("expr", "") or "")
                if dimension_name and dimension_expr:
                    dimension_payload = {
                        "name": dimension_name,
                        "expr": dimension_expr,
                    }
                    filter_payload = {"expr": dimension_expr}
                    for alias in dimension_aliases:
                        dimension_index.setdefault(alias, dimension_payload)
                        filter_field_index.setdefault(alias, filter_payload)

        return entries, metric_index, dimension_index, filter_field_index, time_index

    @staticmethod
    def _normalize(text: str) -> str:
        return text.strip().lower()

    def _collect_aliases(self, item: dict[str, Any]) -> list[str]:
        aliases = [item.get("name", "")]
        aliases.extend(item.get("synonyms", []) or [])
        expr = item.get("expr")
        if isinstance(expr, str) and expr:
            aliases.append(expr)
        return [self._normalize(a) for a in aliases if isinstance(a, str) and a.strip()]

    def _build_semantic_refs(
        self,
        extracted_features: dict[str, Any],
    ) -> dict[str, Any]:
        metric_refs: list[dict[str, str]] = []
        seen_metric_names: set[str] = set()
        for metric in extracted_features.get("metrics", []) or []:
            if not isinstance(metric, str) or not metric.strip():
                continue
            mapped = self.metric_index.get(self._normalize(metric))
            if not mapped:
                continue
            mapped_name = str(mapped.get("name", "") or "")
            if not mapped_name or mapped_name in seen_metric_names:
                continue
            seen_metric_names.add(mapped_name)
            metric_refs.append(
                {
                    "name": mapped_name,
                    "agg": str(mapped.get("agg", "") or ""),
                    "expr": str(mapped.get("expr", "") or ""),
                }
            )

        dimension_refs: list[dict[str, str]] = []
        seen_dimension_exprs: set[str] = set()
        for dimension in extracted_features.get("dimensions", []) or []:
            if not isinstance(dimension, str) or not dimension.strip():
                continue
            mapped = self.dimension_index.get(self._normalize(dimension))
            if not mapped:
                continue
            mapped_expr = str(mapped.get("expr", "") or "")
            if not mapped_expr or mapped_expr in seen_dimension_exprs:
                continue
            seen_dimension_exprs.add(mapped_expr)
            dimension_refs.append(
                {
                    "name": str(mapped.get("name", "") or ""),
                    "expr": mapped_expr,
                }
            )

        filter_refs: list[dict[str, str]] = []
        seen_filters: set[tuple[str, str, str]] = set()
        for filter_text in extracted_features.get("filters", []) or []:
            if not isinstance(filter_text, str) or "=" not in filter_text:
                continue
            lhs, rhs = filter_text.split("=", 1)
            field_key = self._normalize(lhs)
            mapped = self.filter_field_index.get(field_key)
            if not mapped:
                continue
            expr = str(mapped.get("expr", "") or "")
            value = rhs.strip()
            if not expr:
                continue
            dedupe_key = (expr, "=", value)
            if dedupe_key in seen_filters:
                continue
            seen_filters.add(dedupe_key)
            filter_refs.append({"expr": expr, "op": "=", "value": value})

        dataset = ""
        if metric_refs:
            first_metric_key = self._normalize(str((extracted_features.get("metrics", []) or [""])[0]))
            metric_mapped = self.metric_index.get(first_metric_key)
            if metric_mapped:
                dataset = str(metric_mapped.get("dataset", "") or "")
        if not dataset:
            for metric in extracted_features.get("metrics", []) or []:
                if not isinstance(metric, str):
                    continue
                metric_mapped = self.metric_index.get(self._normalize(metric))
                if metric_mapped:
                    dataset = str(metric_mapped.get("dataset", "") or "")
                    if dataset:
                        break
        if not dataset:
            for value in extracted_features.get("tokens", []) or []:
                if not isinstance(value, str):
                    continue
                maybe_metric = self.metric_index.get(self._normalize(value))
                if maybe_metric:
                    dataset = str(maybe_metric.get("dataset", "") or "")
                    if dataset:
                        break

        time_field = str(self.time_index.get(dataset, {}).get("time_field", "") or "") if dataset else ""

        return {
            "dataset": dataset,
            "time_field": time_field,
            "limit": extracted_features.get("limit"),
            "metrics": metric_refs,
            "dimensions": dimension_refs,
            "filters": filter_refs,
        }

    def match(self, extracted_features: dict[str, Any]) -> dict[str, Any]:
        semantic_refs = self._build_semantic_refs(extracted_features)
        return {
            "tokens": extracted_features.get("tokens", []) or [],
            "time_start": extracted_features.get("time_start", ""),
            "time_end": extracted_features.get("time_end", ""),
            "semantic_refs": semantic_refs,
        }
