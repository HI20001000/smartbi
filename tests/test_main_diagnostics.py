import unittest

from app.main import (
    _build_dataset_time_bounds_sql,
    _build_empty_result_hint,
    _compute_adjusted_time_range,
    _replace_time_between_filter,
)


class MainDiagnosticsTests(unittest.TestCase):
    def test_build_dataset_time_bounds_sql_uses_dataset_from_and_time_expr(self):
        semantic_layer = {
            "datasets": {
                "deposit_balance_daily": {
                    "from": "fact_account_balance_daily as bal",
                    "time_dimensions": [{"name": "biz_date", "expr": "bal.biz_date"}],
                }
            }
        }
        plan = {"selected_dataset_candidates": ["deposit_balance_daily"]}

        sql = _build_dataset_time_bounds_sql(plan, semantic_layer)

        self.assertEqual(
            sql,
            "SELECT MIN(bal.biz_date) AS min_biz_date, MAX(bal.biz_date) AS max_biz_date FROM fact_account_balance_daily as bal",
        )

    def test_compute_adjusted_time_range_uses_data_bounds_when_disjoint(self):
        adjusted = _compute_adjusted_time_range(
            "2024-01-01",
            "2024-12-31",
            "2026-01-01",
            "2026-01-31",
        )
        self.assertEqual(adjusted, ("2026-01-01", "2026-01-31"))

    def test_replace_time_between_filter_updates_plan_filter(self):
        plan = {
            "selected_filters": [
                {
                    "field": "deposit_balance_daily.biz_date",
                    "op": "between",
                    "value": ["2024-01-01", "2024-12-31"],
                    "source": "step_b_time_bounds",
                },
                {"field": "branch.region", "op": "=", "value": "澳門半島"},
            ]
        }

        updated = _replace_time_between_filter(plan, "2026-01-01", "2026-01-31")

        self.assertIsNotNone(updated)
        self.assertEqual(
            updated["selected_filters"][0],
            {
                "field": "deposit_balance_daily.biz_date",
                "op": "between",
                "value": ["2026-01-01", "2026-01-31"],
                "source": "auto_adjusted_time_bounds",
            },
        )
        self.assertEqual(updated["selected_filters"][1]["field"], "branch.region")

    def test_build_empty_result_hint_contains_auto_fix_message(self):
        hint = _build_empty_result_hint(
            requested_start="2024-01-01",
            requested_end="2024-12-31",
            data_start="2026-01-01",
            data_end="2026-01-31",
            adjusted_start="2026-01-01",
            adjusted_end="2026-01-31",
        )

        self.assertIn("查詢時間範圍可能超出資料可用區間", hint)
        self.assertIn("已自動改用可用時間範圍重新查詢", hint)
        self.assertIn("2026-01-01 ~ 2026-01-31", hint)


if __name__ == "__main__":
    unittest.main()
