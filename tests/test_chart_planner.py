from app.chart_planner import build_chart_spec
from app.query_executor import QueryResult


def test_chart_planner_prefers_line_for_time_dimension():
    result = QueryResult(
        columns=["biz_date", "deposit_balance"],
        rows=[
            {"biz_date": "2026-01-01", "deposit_balance": 100.0},
            {"biz_date": "2026-01-02", "deposit_balance": 150.0},
        ],
    )

    spec = build_chart_spec(result, title="Balance Trend")
    assert spec.chart_type == "line"
    assert spec.x == "biz_date"
    assert spec.y == ["deposit_balance"]


def test_chart_planner_uses_bar_for_categorical_dimension():
    result = QueryResult(
        columns=["region", "txn_count"],
        rows=[
            {"region": "澳門半島", "txn_count": 88},
            {"region": "氹仔", "txn_count": 55},
        ],
    )

    spec = build_chart_spec(result)
    assert spec.chart_type == "bar"
    assert spec.x == "region"
    assert spec.y == ["txn_count"]
