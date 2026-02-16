from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib import request

import yaml
from langchain_openai import OpenAIEmbeddings


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

    It combines:
    1) deterministic exact alias matching
    2) semantic retrieval via embedding similarity + optional reranker
    """

    def __init__(
        self,
        semantic_yaml_path: str | Path,
        *,
        embedding_base_url: str | None = None,
        embedding_model: str | None = None,
        embedding_api_key: str = "empty",
        reranker_base_url: str | None = None,
        reranker_model: str | None = None,
        reranker_api_key: str = "empty",
    ):
        self.semantic_yaml_path = Path(semantic_yaml_path)
        self.embedding_base_url = (embedding_base_url or "").strip()
        self.embedding_model = (embedding_model or "").strip()
        self.embedding_api_key = (embedding_api_key or "empty").strip() or "empty"
        self.reranker_base_url = (reranker_base_url or "").strip()
        self.reranker_model = (reranker_model or "").strip()
        self.reranker_api_key = (reranker_api_key or "empty").strip() or "empty"
        self.embedding_client = self._build_embedding_client()

        (
            self.entries,
            self.metric_index,
            self.dimension_index,
            self.filter_field_index,
            self.time_index,
        ) = self._build_entries_and_indexes()
        self._entry_lookup: dict[str, SemanticEntry] = {e.canonical_name: e for e in self.entries}
        self._semantic_docs = self._build_semantic_docs()

    def _build_embedding_client(self) -> OpenAIEmbeddings | None:
        if not self.embedding_base_url or not self.embedding_model:
            return None
        try:
            return OpenAIEmbeddings(
                model=self.embedding_model,
                base_url=self.embedding_base_url,
                api_key=self.embedding_api_key,
            )
        except Exception:
            return None

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

    def _build_semantic_docs(self) -> list[dict[str, str]]:
        docs: list[dict[str, str]] = []
        for entry in self.entries:
            aliases = [a for a in entry.aliases if a]
            text = (
                f"type:{entry.object_type};canonical:{entry.canonical_name};"
                f"dataset:{entry.dataset or ''};entity:{entry.entity or ''};"
                f"aliases:{','.join(aliases)}"
            )
            docs.append({"canonical_name": entry.canonical_name, "text": text})
        return docs

    @staticmethod
    def _cosine_similarity(a: list[float], b: list[float]) -> float:
        dot = sum(x * y for x, y in zip(a, b))
        norm_a = math.sqrt(sum(x * x for x in a))
        norm_b = math.sqrt(sum(y * y for y in b))
        if norm_a == 0.0 or norm_b == 0.0:
            return 0.0
        return dot / (norm_a * norm_b)

    @staticmethod
    def _to_match_payload(entry: SemanticEntry, score: float | None = None, source: str = "exact") -> dict[str, Any]:
        payload: dict[str, Any] = {
            "object_type": entry.object_type,
            "canonical_name": entry.canonical_name,
            "dataset": entry.dataset or "",
            "entity": entry.entity or "",
            "table": entry.table or "",
            "allowed": entry.allowed,
            "source": source,
        }
        if score is not None:
            payload["score"] = round(score, 6)
        return payload

    def _build_exact_matches(self, extracted_features: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        values: list[str] = []
        for key in ("tokens", "metrics", "dimensions"):
            for v in extracted_features.get(key, []) or []:
                if isinstance(v, str) and v.strip():
                    values.append(self._normalize(v))

        seen: set[str] = set()
        matches: list[dict[str, Any]] = []
        blocked: list[dict[str, Any]] = []
        for value in values:
            for entry in self.entries:
                if value in entry.aliases:
                    if entry.canonical_name in seen:
                        continue
                    seen.add(entry.canonical_name)
                    payload = self._to_match_payload(entry, source="exact")
                    if entry.allowed:
                        matches.append(payload)
                    else:
                        blocked.append(payload)
        return matches, blocked

    def _semantic_retrieve(self, query: str, top_k: int = 8) -> list[dict[str, Any]]:
        if not query or not self.embedding_client or not self._semantic_docs:
            return []

        try:
            query_vector = self.embedding_client.embed_query(query)
            doc_vectors = self.embedding_client.embed_documents([d["text"] for d in self._semantic_docs])
        except Exception:
            return []

        scored: list[tuple[int, float]] = []
        for idx, vec in enumerate(doc_vectors):
            score = self._cosine_similarity(query_vector, vec)
            scored.append((idx, score))
        scored.sort(key=lambda x: x[1], reverse=True)
        top = scored[:top_k]

        out: list[dict[str, Any]] = []
        for idx, score in top:
            canonical = self._semantic_docs[idx]["canonical_name"]
            entry = self._entry_lookup.get(canonical)
            if not entry:
                continue
            out.append(self._to_match_payload(entry, score=score, source="embedding"))
        return out

    def _rerank(self, query: str, candidates: list[dict[str, Any]], top_k: int = 8) -> list[dict[str, Any]]:
        if not query or not candidates:
            return candidates[:top_k]
        if not self.reranker_base_url or not self.reranker_model:
            return candidates[:top_k]

        docs = [
            f"{c.get('object_type', '')} {c.get('canonical_name', '')} {c.get('dataset', '')} {c.get('entity', '')}"
            for c in candidates
        ]
        payload = {
            "model": self.reranker_model,
            "query": query,
            "documents": docs,
            "top_n": min(top_k, len(docs)),
        }

        endpoint = self.reranker_base_url.rstrip("/") + "/rerank"
        req = request.Request(
            endpoint,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.reranker_api_key}",
            },
            data=json.dumps(payload).encode("utf-8"),
        )

        try:
            with request.urlopen(req, timeout=10) as resp:
                body = json.loads(resp.read().decode("utf-8"))
            results = body.get("results", []) or []
            ranked: list[dict[str, Any]] = []
            for item in results:
                idx = int(item.get("index", -1))
                if idx < 0 or idx >= len(candidates):
                    continue
                c = dict(candidates[idx])
                c["score"] = float(item.get("relevance_score", c.get("score", 0.0)))
                c["source"] = "embedding+reranker"
                ranked.append(c)
            return ranked[:top_k] if ranked else candidates[:top_k]
        except Exception:
            return candidates[:top_k]

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
            "metrics": metric_refs,
            "dimensions": dimension_refs,
            "filters": filter_refs,
        }

    @staticmethod
    def _merge_matches(exact_matches: list[dict[str, Any]], retrieved: list[dict[str, Any]]) -> list[dict[str, Any]]:
        merged: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in exact_matches + retrieved:
            canonical = str(item.get("canonical_name", "") or "")
            if not canonical or canonical in seen:
                continue
            seen.add(canonical)
            merged.append(item)
        return merged

    def match(self, extracted_features: dict[str, Any]) -> dict[str, Any]:
        exact_matches, blocked = self._build_exact_matches(extracted_features)
        query_parts: list[str] = []
        for key in ("metrics", "dimensions", "tokens", "filters"):
            values = extracted_features.get(key, []) or []
            query_parts.extend([v for v in values if isinstance(v, str) and v.strip()])
        semantic_query = " ".join(query_parts).strip()

        embedding_hits = self._semantic_retrieve(semantic_query, top_k=8)
        reranked_hits = self._rerank(semantic_query, embedding_hits, top_k=8)

        retrieved_allowed = [item for item in reranked_hits if item.get("allowed") is not False]
        retrieved_blocked = [item for item in reranked_hits if item.get("allowed") is False]
        blocked = self._merge_matches(blocked, retrieved_blocked)

        matches = self._merge_matches(exact_matches, retrieved_allowed)
        return {
            "tokens": extracted_features.get("tokens", []) or [],
            "time_start": extracted_features.get("time_start", ""),
            "time_end": extracted_features.get("time_end", ""),
            "matches": matches,
            "blocked_matches": blocked,
        }
