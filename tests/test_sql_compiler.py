from app.semantic_loader import load_semantic_layer
from app.sql_compiler import compile_sql_from_semantic_plan


def test_compile_sql_from_semantic_plan_basic():
    semantic_layer = load_semantic_layer()
    plan = {
        "selected_metrics": ["deposit_balance_daily.deposit_end_balance"],
        "selected_dimensions": ["deposit_balance_daily.available_bal"],
        "selected_filters": [
            {
                "field": "calendar.biz_date",
                "op": "between",
                "value": ["2024-01-01", "2024-12-31"],
            }
        ],
        "selected_dataset_candidates": ["deposit_balance_daily"],
    }

    sql = compile_sql_from_semantic_plan(plan, semantic_layer, limit=100)

    assert "FROM fact_account_balance_daily as bal" in sql
    assert "bal.available_bal AS deposit_balance_daily_available_bal" in sql
    assert "bal.end_balance AS deposit_balance_daily_deposit_end_balance" in sql
    assert "calendar.biz_date BETWEEN '2024-01-01' AND '2024-12-31'" in sql
    assert "LIMIT 100" in sql
