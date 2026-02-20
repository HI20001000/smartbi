from decimal import Decimal
import unittest

from app.chart_planner import ROW_INDEX_X_KEY, build_chart_spec
from app.query_executor import QueryResult


class ChartPlannerTests(unittest.TestCase):
    def test_build_chart_spec_supports_decimal_metric_for_bar_chart(self):
        result = QueryResult(
            columns=["branch_name", "total_amount"],
            rows=[
                {"branch_name": "A", "total_amount": Decimal("10.5")},
                {"branch_name": "B", "total_amount": Decimal("20.0")},
            ],
        )

        spec = build_chart_spec(result, title="t")

        self.assertEqual(spec.chart_type, "bar")
        self.assertEqual(spec.x, "branch_name")
        self.assertEqual(spec.y, ["total_amount"])

    def test_build_chart_spec_detects_numeric_columns_beyond_first_row(self):
        result = QueryResult(
            columns=["month", "total_amount"],
            rows=[
                {"month": "2024-01", "total_amount": None},
                {"month": "2024-02", "total_amount": 30},
            ],
        )

        spec = build_chart_spec(result, title="t")

        self.assertEqual(spec.chart_type, "line")
        self.assertEqual(spec.x, "month")
        self.assertEqual(spec.y, ["total_amount"])

    def test_build_chart_spec_uses_row_index_for_numeric_only_results(self):
        result = QueryResult(
            columns=["deposit_balance_daily_deposit_end_balance"],
            rows=[
                {"deposit_balance_daily_deposit_end_balance": Decimal("10.0")},
                {"deposit_balance_daily_deposit_end_balance": Decimal("20.0")},
            ],
        )

        spec = build_chart_spec(result, title="t")

        self.assertEqual(spec.chart_type, "bar")
        self.assertEqual(spec.x, ROW_INDEX_X_KEY)
        self.assertEqual(spec.y, ["deposit_balance_daily_deposit_end_balance"])


if __name__ == "__main__":
    unittest.main()
