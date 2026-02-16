import unittest

from app.semantic_validator import validate_semantic_plan
from app.sql_compiler import compile_sql_from_semantic_plan
from app.sql_planner import merge_llm_selection_into_plan


SEMANTIC_LAYER = {
    "entities": {
        "calendar": {
            "table": "dim_calendar",
            "fields": [
                {"name": "biz_date", "expr": "dim_calendar.biz_date"},
            ],
        }
    },
    "datasets": {
        "sales": {
            "from": "fact_sales as s",
            "metrics": [
                {"name": "revenue", "expr": "SUM(s.revenue)"},
                {"name": "orders", "expr": "COUNT(*)"},
            ],
            "dimensions": [
                {"name": "biz_date", "expr": "s.biz_date"},
            ],
            "joins": [
                {"entity": "calendar", "on": "s.biz_date = dim_calendar.biz_date"},
            ],
        }
    },
}


class SemanticPipelineTests(unittest.TestCase):
    def test_merge_llm_selection_keeps_only_candidates(self):
        draft = {
            "selected_metrics": ["sales.revenue"],
            "selected_dimensions": ["sales.biz_date"],
            "selected_filters": [{"field": "calendar.biz_date", "op": "between", "value": ["2024-01-01", "2024-01-31"]}],
            "selected_dataset_candidates": ["sales"],
            "rejected_candidates": [],
            "needs_clarification": False,
            "clarification_questions": [],
            "confidence": 0.8,
        }
        token_hits = {
            "matches": [
                {"object_type": "metric", "canonical_name": "sales.revenue", "dataset": "sales", "allowed": True},
                {"object_type": "dimension", "canonical_name": "sales.biz_date", "dataset": "sales", "allowed": True},
            ]
        }
        llm_selection = {
            "selected_metrics": ["sales.revenue", "sales.invalid_metric"],
            "selected_dimensions": ["sales.biz_date"],
            "selected_dataset_candidates": ["sales", "other_ds"],
            "selected_filters": [],
            "confidence": 0.6,
        }

        merged = merge_llm_selection_into_plan(draft, llm_selection, token_hits)

        self.assertEqual(merged["selected_metrics"], ["sales.revenue"])
        self.assertEqual(merged["selected_dimensions"], ["sales.biz_date"])
        self.assertEqual(merged["selected_dataset_candidates"], ["sales"])

    def test_validator_returns_error_code_for_invalid_canonical(self):
        plan = {
            "selected_metrics": ["sales.ghost_metric"],
            "selected_dimensions": [],
            "selected_filters": [{"field": "calendar.biz_date", "op": "between", "value": ["2024-01-01", "2024-01-31"]}],
            "selected_dataset_candidates": ["sales"],
        }
        token_hits = {"blocked_matches": []}
        governance = {"require_time_filter": True}

        result = validate_semantic_plan(plan, token_hits, governance, semantic_layer=SEMANTIC_LAYER)

        self.assertFalse(result["ok"])
        self.assertIn("INVALID_CANONICAL_REF", result["error_codes"])

    def test_compiler_builds_sql_deterministically(self):
        plan = {
            "selected_metrics": ["sales.revenue"],
            "selected_dimensions": ["sales.biz_date"],
            "selected_filters": [{"field": "calendar.biz_date", "op": "between", "value": ["2024-01-01", "2024-01-31"]}],
            "selected_dataset_candidates": ["sales"],
        }

        sql = compile_sql_from_semantic_plan(plan, SEMANTIC_LAYER, limit=100)

        self.assertIn("SELECT s.biz_date AS sales_biz_date, SUM(s.revenue) AS sales_revenue", sql)
        self.assertIn("FROM fact_sales as s", sql)
        self.assertIn("GROUP BY s.biz_date", sql)
        self.assertTrue(sql.strip().endswith("LIMIT 100"))


if __name__ == "__main__":
    unittest.main()
