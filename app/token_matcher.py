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
        self.entries = self._build_entries()

    def _build_entries(self) -> list[SemanticEntry]:
        with self.semantic_yaml_path.open("r", encoding="utf-8") as f:
            semantic = yaml.safe_load(f) or {}

        layer = semantic.get("semantic_layer", {})
        entries: list[SemanticEntry] = []

        entities = layer.get("entities", {})
        for entity_name, entity in entities.items():
            table = entity.get("table")

            for field in entity.get("fields", []):
                entries.append(
                    SemanticEntry(
                        object_type="field",
                        canonical_name=f"{entity_name}.{field.get('name')}",
                        aliases=tuple(self._collect_aliases(field)),
                        entity=entity_name,
                        table=table,
                        allowed=True,
                    )
                )

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
                entries.append(
                    SemanticEntry(
                        object_type="time_dimension",
                        canonical_name=f"{dataset_name}.{time_dimension.get('name')}",
                        aliases=tuple(self._collect_aliases(time_dimension)),
                        dataset=dataset_name,
                        allowed=True,
                    )
                )

            for metric in dataset.get("metrics", []):
                entries.append(
                    SemanticEntry(
                        object_type="metric",
                        canonical_name=f"{dataset_name}.{metric.get('name')}",
                        aliases=tuple(self._collect_aliases(metric)),
                        dataset=dataset_name,
                        allowed=True,
                    )
                )

            for dimension in dataset.get("dimensions", []):
                entries.append(
                    SemanticEntry(
                        object_type="dimension",
                        canonical_name=f"{dataset_name}.{dimension.get('name')}",
                        aliases=tuple(self._collect_aliases(dimension)),
                        dataset=dataset_name,
                        allowed=True,
                    )
                )

        return entries

    @staticmethod
    def _normalize(text: str) -> str:
        return text.strip().lower()

    @staticmethod
    def _unique_keep_order(values: list[str]) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for value in values:
            if value not in seen:
                seen.add(value)
                out.append(value)
        return out

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
        matches: list[dict[str, Any]],
    ) -> dict[str, Any]:
        metric_refs = self._unique_keep_order(
            [
                m.get("canonical_name", "")
                for m in matches
                if m.get("object_type") == "metric" and m.get("allowed") is not False and m.get("canonical_name")
            ]
        )
        dimension_refs = self._unique_keep_order(
            [
                m.get("canonical_name", "")
                for m in matches
                if m.get("object_type") == "dimension" and m.get("allowed") is not False and m.get("canonical_name")
            ]
        )
        dataset_candidates = self._unique_keep_order(
            [m.get("dataset", "") for m in matches if m.get("dataset")]
        )

        dataset = dataset_candidates[0] if dataset_candidates else ""
        if not dataset and metric_refs:
            dataset = metric_refs[0].split(".", 1)[0]
        if not dataset and dimension_refs:
            dataset = dimension_refs[0].split(".", 1)[0]

        filter_refs: list[dict[str, Any]] = []
        filters = extracted_features.get("filters", []) or []
        if dimension_refs:
            for filter_text in filters:
                if isinstance(filter_text, str) and filter_text.strip():
                    filter_refs.append(
                        {
                            "field": dimension_refs[0],
                            "op": "=",
                            "value": filter_text.strip(),
                        }
                    )

        time_start = str(extracted_features.get("time_start", "") or "").strip()
        time_end = str(extracted_features.get("time_end", "") or "").strip()
        time_ref: dict[str, Any] = {}
        time_field = ""
        for match in matches:
            if match.get("object_type") == "time_dimension" and match.get("allowed") is not False:
                time_field = str(match.get("canonical_name", "") or "")
                if time_field:
                    break
        if not time_field and dataset:
            time_field = f"{dataset}.biz_date"
        if time_field and time_start and time_end:
            time_ref = {
                "field": time_field,
                "start": time_start,
                "end": time_end,
            }

        signal_count = len(metric_refs) + len(dimension_refs)
        if time_ref:
            signal_count += 1
        confidence = min(0.99, round(0.45 + signal_count * 0.13, 2)) if signal_count else 0.0

        return {
            "dataset": dataset,
            "metric_refs": metric_refs,
            "dimension_refs": dimension_refs,
            "filter_refs": filter_refs,
            "time_ref": time_ref,
            "dataset_candidates": dataset_candidates,
            "confidence": confidence,
        }

    def match(self, extracted_features: dict[str, Any]) -> dict[str, Any]:
        raw_tokens: list[str] = []
        for key in ("tokens", "metrics", "dimensions", "filters"):
            values = extracted_features.get(key, []) or []
            for v in values:
                if isinstance(v, str) and v.strip():
                    raw_tokens.append(v)

        normalized_tokens = [self._normalize(t) for t in raw_tokens]

        matches: list[dict[str, Any]] = []
        for token in normalized_tokens:
            for entry in self.entries:
                if token in entry.aliases:
                    matches.append(
                        {
                            "token": token,
                            "object_type": entry.object_type,
                            "canonical_name": entry.canonical_name,
                            "dataset": entry.dataset,
                            "entity": entry.entity,
                            "table": entry.table,
                            "allowed": entry.allowed,
                        }
                    )

        blocked = [m for m in matches if m.get("allowed") is False]
        semantic_refs = self._build_semantic_refs(extracted_features, matches)
        extracted_features["semantic_refs"] = semantic_refs

        return {
            "input_tokens": normalized_tokens,
            "matches": matches,
            "blocked_matches": blocked,
            "needs_clarification": len(matches) == 0,
            "semantic_refs": semantic_refs,
        }
