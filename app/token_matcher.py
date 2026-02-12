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

    def _collect_aliases(self, item: dict[str, Any]) -> list[str]:
        aliases = [item.get("name", "")]
        aliases.extend(item.get("synonyms", []) or [])
        expr = item.get("expr")
        if isinstance(expr, str) and expr:
            aliases.append(expr)
        return [self._normalize(a) for a in aliases if isinstance(a, str) and a.strip()]

    def match(self, extracted_features: dict[str, Any]) -> dict[str, Any]:
        raw_tokens: list[str] = []
        for key in ("tokens", "metrics", "dimensions", "filters"):
            values = extracted_features.get(key, []) or []
            for v in values:
                if isinstance(v, str) and v.strip():
                    raw_tokens.append(v)

        time_range = extracted_features.get("time_range", "")
        if isinstance(time_range, str) and time_range.strip():
            raw_tokens.append(time_range)

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
        return {
            "input_tokens": normalized_tokens,
            "matches": matches,
            "blocked_matches": blocked,
            "need_clarification": len(matches) == 0,
        }
