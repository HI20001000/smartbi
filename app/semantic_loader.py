from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


SEMANTIC_YAML_PATH = "app/semantics/smartbi_demo_macau_banking_semantic.yaml"


def load_semantic_layer(path: str | Path = SEMANTIC_YAML_PATH) -> dict[str, Any]:
    semantic_path = Path(path)
    with semantic_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("semantic_layer", {})


def get_governance(semantic_layer: dict[str, Any]) -> dict[str, Any]:
    return semantic_layer.get("governance", {}).get("default_query_limits", {})
