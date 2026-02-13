from app.sql_planner import build_semantic_plan


def test_build_semantic_plan_uses_time_bounds_when_present():
    extracted_features = {
        "tokens": [],
        "metrics": [],
        "dimensions": [],
        "filters": [],
        "time_start": "2026-01-01",
        "time_end": "2026-01-31",
    }
    token_hits = {"matches": [], "blocked_matches": []}

    plan = build_semantic_plan(extracted_features, token_hits)

    assert {
        "field": "calendar.biz_date",
        "op": "between",
        "value": ["2026-01-01", "2026-01-31"],
        "source": "step_b_time_bounds",
    } in plan["selected_filters"]


def test_build_semantic_plan_keeps_time_filter_empty_without_bounds():
    extracted_features = {
        "tokens": [],
        "metrics": [],
        "dimensions": [],
        "filters": [],
        "time_start": "",
        "time_end": "",
    }
    token_hits = {"matches": [], "blocked_matches": []}

    plan = build_semantic_plan(extracted_features, token_hits)

    assert all(f.get("source") != "step_b_time_bounds" for f in plan["selected_filters"])
