import unittest

from app.main import _build_dataset_time_bounds_sql, _build_empty_result_hint


class _FakeExecutor:
    def __init__(self, rows):
        self._rows = rows

    def run(self, sql: str, max_rows: int = 1000):
        class _R:
            def __init__(self, rows):
                self.rows = rows

        return _R(self._rows)


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

    def test_build_empty_result_hint_returns_range_hint_when_requested_time_outside_data(self):
        plan = {
            "selected_dataset_candidates": ["deposit_balance_daily"],
            "selected_filters": [
                {
                    "field": "deposit_balance_daily.biz_date",
                    "op": "between",
                    "value": ["2024-01-01", "2024-12-31"],
                }
            ],
        }
        semantic_layer = {
            "datasets": {
                "deposit_balance_daily": {
                    "from": "fact_account_balance_daily as bal",
                    "time_dimensions": [{"name": "biz_date", "expr": "bal.biz_date"}],
                }
            }
        }
        executor = _FakeExecutor([{"min_biz_date": "2026-01-01", "max_biz_date": "2026-01-31"}])

        hint = _build_empty_result_hint(plan, semantic_layer, executor)

        self.assertIn("查詢時間範圍可能超出資料可用區間", hint)
        self.assertIn("2024-01-01 ~ 2024-12-31", hint)
        self.assertIn("2026-01-01 ~ 2026-01-31", hint)


if __name__ == "__main__":
    unittest.main()
