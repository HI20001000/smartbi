import unittest

from app.semantic_validator import validate_semantic_plan
from app.sql_compiler import compile_sql_from_semantic_plan
from app.sql_planner import merge_llm_selection_into_plan


SEMANTIC_LAYER = {
    "entities": {
        "calendar": {
            "table": "dim_calendar",
            "fields": [
                {"name": "biz_date", "expr": "dim_calendar.biz_date", "synonyms": ["日期"]},
            ],
        },
        "branch": {
            "table": "dim_branch",
            "fields": [
                {"name": "region", "expr": "dim_branch.region", "synonyms": ["地區"]},
            ],
        },
    },
    "datasets": {
        "sales": {
            "from": "fact_sales as s",
            "metrics": [
                {"name": "revenue", "expr": "SUM(s.revenue)"},
                {"name": "orders", "expr": "COUNT(*)"},
            ],
            "time_dimensions": [
                {"name": "biz_date", "expr": "s.biz_date", "synonyms": ["交易日"]},
            ],
            "dimensions": [
                {"name": "biz_date", "expr": "s.biz_date", "synonyms": ["日期"]},
            ],
            "joins": [
                {"entity": "calendar", "on": "s.biz_date = dim_calendar.biz_date"},
                {"entity": "branch", "on": "s.branch_id = dim_branch.branch_id"},
            ],
        }
    },
}


class SemanticPipelineTests(unittest.TestCase):
    def test_merge_llm_selection_keeps_only_candidates(self):
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
        }
        features = {"filters": [], "time_start": "", "time_end": ""}

        merged = merge_llm_selection_into_plan(llm_selection, token_hits, features, semantic_layer=SEMANTIC_LAYER)

        self.assertEqual(merged["selected_metrics"], ["sales.revenue"])
        self.assertEqual(merged["selected_dimensions"], ["sales.biz_date"])
        self.assertEqual(merged["selected_dataset_candidates"], ["sales"])

    def test_merge_llm_selection_fallbacks_to_step_c_when_empty(self):
        token_hits = {
            "matches": [
                {"object_type": "metric", "canonical_name": "sales.revenue", "dataset": "sales", "allowed": True},
                {"object_type": "dimension", "canonical_name": "sales.biz_date", "dataset": "sales", "allowed": True},
            ]
        }
        llm_selection = {
            "selected_metrics": [],
            "selected_dimensions": [],
            "selected_dataset_candidates": [],
            "selected_filters": [],
        }
        features = {"filters": [], "time_start": "", "time_end": ""}

        merged = merge_llm_selection_into_plan(llm_selection, token_hits, features, semantic_layer=SEMANTIC_LAYER)

        self.assertEqual(merged["selected_metrics"], ["sales.revenue"])
        self.assertEqual(merged["selected_dimensions"], ["sales.biz_date"])
        self.assertEqual(merged["selected_dataset_candidates"], ["sales"])

    def test_merge_llm_selection_normalizes_step_b_filter_expr_to_canonical_field(self):
        token_hits = {
            "matches": [
                {"object_type": "metric", "canonical_name": "sales.revenue", "dataset": "sales", "allowed": True},
            ]
        }
        llm_selection = {
            "selected_metrics": ["sales.revenue"],
            "selected_dimensions": [],
            "selected_dataset_candidates": ["sales"],
            "selected_filters": [],
        }
        features = {"filters": ["地區=澳門半島"], "time_start": "", "time_end": ""}

        merged = merge_llm_selection_into_plan(llm_selection, token_hits, features, semantic_layer=SEMANTIC_LAYER)

        self.assertEqual(
            merged["selected_filters"],
            [{"field": "branch.region", "op": "=", "value": "澳門半島", "source": "step_b_filters"}],
        )

    def test_merge_llm_selection_uses_dataset_time_dimension_for_time_filter(self):
        token_hits = {
            "matches": [
                {"object_type": "metric", "canonical_name": "sales.revenue", "dataset": "sales", "allowed": True},
            ]
        }
        llm_selection = {
            "selected_metrics": ["sales.revenue"],
            "selected_dimensions": [],
            "selected_dataset_candidates": ["sales"],
            "selected_filters": [],
        }
        features = {"filters": [], "time_start": "2024-01-01", "time_end": "2024-01-31"}

        merged = merge_llm_selection_into_plan(llm_selection, token_hits, features, semantic_layer=SEMANTIC_LAYER)

        self.assertEqual(
            merged["selected_filters"],
            [{"field": "sales.biz_date", "op": "between", "value": ["2024-01-01", "2024-01-31"], "source": "step_b_time_bounds"}],
        )

    def test_validator_returns_error_code_for_invalid_canonical(self):
        plan = {
            "selected_metrics": ["sales.ghost_metric"],
            "selected_dimensions": [],
            "selected_filters": [{"field": "sales.biz_date", "op": "between", "value": ["2024-01-01", "2024-01-31"]}],
            "selected_dataset_candidates": ["sales"],
        }
        token_hits = {"blocked_matches": []}
        governance = {"require_time_filter": True}

        result = validate_semantic_plan(plan, token_hits, governance, semantic_layer=SEMANTIC_LAYER)

        self.assertFalse(result["ok"])
        self.assertIn("INVALID_CANONICAL_REF", result["error_codes"])

    def test_compiler_builds_sql_deterministically_from_canonical_filter_field(self):
        plan = {
            "selected_metrics": ["sales.revenue"],
            "selected_dimensions": ["sales.biz_date"],
            "selected_filters": [
                {"field": "branch.region", "op": "=", "value": "澳門半島"},
                {"field": "sales.biz_date", "op": "between", "value": ["2024-01-01", "2024-01-31"]},
            ],
            "selected_dataset_candidates": ["sales"],
        }

        sql = compile_sql_from_semantic_plan(plan, SEMANTIC_LAYER)

        self.assertIn("SELECT s.biz_date AS sales_biz_date, SUM(s.revenue) AS sales_revenue", sql)
        self.assertIn("FROM fact_sales as s", sql)
        self.assertIn("LEFT JOIN dim_branch ON s.branch_id = dim_branch.branch_id", sql)
        self.assertIn("dim_branch.region = '澳門半島'", sql)
        self.assertIn("s.biz_date BETWEEN '2024-01-01' AND '2024-01-31'", sql)
        self.assertIn("GROUP BY s.biz_date", sql)
        self.assertFalse(sql.strip().lower().endswith("limit 100"))


if __name__ == "__main__":
    unittest.main()
