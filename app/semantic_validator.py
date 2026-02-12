from __future__ import annotations

from typing import Any


def validate_semantic_plan(
    enhanced_plan: dict[str, Any],
    token_hits: dict[str, Any],
    governance_limits: dict[str, Any],
) -> dict[str, Any]:
    errors: list[str] = []
    blocked = token_hits.get("blocked_matches", []) or []

    if blocked:
        errors.append("命中敏感欄位（allowed=false），請改用非敏感欄位或彙總指標。")

    require_time_filter = bool(governance_limits.get("require_time_filter", False))
    filters = enhanced_plan.get("selected_filters", []) or []
    if require_time_filter and not filters:
        errors.append("查詢需要時間條件（require_time_filter=true），請補充時間範圍。")

    selected_metrics = enhanced_plan.get("selected_metrics", []) or []
    selected_dimensions = enhanced_plan.get("selected_dimensions", []) or []
    if not selected_metrics and not selected_dimensions:
        errors.append("尚未選到可用的指標/維度，請補充查詢條件。")

    return {
        "ok": len(errors) == 0,
        "errors": errors,
    }
