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

    def test_validator_allows_entity_dimension_with_dataset_metric(self):
        plan = {
            "selected_metrics": ["sales.revenue"],
            "selected_dimensions": ["branch.region"],
            "selected_filters": [{"field": "sales.biz_date", "op": "between", "value": ["2024-01-01", "2024-01-31"]}],
            "selected_dataset_candidates": ["sales"],
        }
        token_hits = {"blocked_matches": []}
        governance = {"require_time_filter": True}

        result = validate_semantic_plan(plan, token_hits, governance, semantic_layer=SEMANTIC_LAYER)

        self.assertTrue(result["ok"])
        self.assertNotIn("MULTI_DATASET_NO_JOIN_PATH", result["error_codes"])

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


    def test_compiler_preserves_step_d2_time_between_values_without_rewrite(self):
        semantic_layer = {
            "entities": {
                "branch": {
                    "table": "dim_branch",
                    "fields": [{"name": "region", "expr": "dim_branch.region"}],
                }
            },
            "datasets": {
                "deposit_balance_daily": {
                    "from": "fact_account_balance_daily as bal",
                    "time_dimensions": [{"name": "biz_date", "expr": "bal.biz_date"}],
                    "metrics": [{"name": "deposit_end_balance", "expr": "bal.end_balance"}],
                    "dimensions": [],
                    "joins": [{"entity": "branch", "on": "bal.branch_id = dim_branch.branch_id"}],
                }
            },
        }
        plan = {
            "selected_metrics": ["deposit_balance_daily.deposit_end_balance"],
            "selected_dimensions": [],
            "selected_filters": [
                {"field": "branch.region", "op": "=", "value": "澳門半島"},
                {"field": "deposit_balance_daily.biz_date", "op": "between", "value": ["2024-01-01", "2024-12-31"]},
            ],
            "selected_dataset_candidates": ["deposit_balance_daily"],
        }

        sql = compile_sql_from_semantic_plan(plan, semantic_layer)

        self.assertIn("bal.biz_date BETWEEN '2024-01-01' AND '2024-12-31'", sql)
        self.assertNotIn("2026-01-01", sql)

    def test_compiler_supports_yaml_boolean_true_key_for_join_on(self):
        semantic_layer = {
            "entities": {
                "branch": {
                    "table": "dim_branch",
                    "fields": [
                        {"name": "region", "expr": "dim_branch.region"},
                    ],
                }
            },
            "datasets": {
                "deposit_balance_daily": {
                    "from": "fact_account_balance_daily as bal",
                    "metrics": [
                        {"name": "deposit_end_balance", "expr": "bal.end_balance"},
                    ],
                    "joins": [
                        {"entity": "branch", True: "bal.branch_id = dim_branch.branch_id"},
                    ],
                }
            },
        }
        plan = {
            "selected_metrics": ["deposit_balance_daily.deposit_end_balance"],
            "selected_dimensions": [],
            "selected_filters": [{"field": "branch.region", "op": "=", "value": "澳門半島"}],
            "selected_dataset_candidates": ["deposit_balance_daily"],
        }

        sql = compile_sql_from_semantic_plan(plan, semantic_layer)

        self.assertIn("LEFT JOIN dim_branch ON bal.branch_id = dim_branch.branch_id", sql)
        self.assertIn("dim_branch.region = '澳門半島'", sql)

    def test_compiler_uses_calendar_skeleton_for_deposit_daily_trend_and_coalesce(self):
        semantic_layer = {
            "entities": {
                "calendar": {"table": "dim_calendar", "fields": [{"name": "biz_date", "expr": "dim_calendar.biz_date"}]},
            },
            "datasets": {
                "deposit_balance_daily": {
                    "from": "fact_account_balance_daily as bal",
                    "time_dimensions": [{"name": "biz_date", "expr": "bal.biz_date"}],
                    "metrics": [{"name": "deposit_end_balance", "expr": "bal.end_balance", "type": "sum"}],
                    "dimensions": [{"name": "biz_date", "expr": "bal.biz_date"}],
                    "joins": [{"entity": "calendar", "on": "bal.biz_date = dim_calendar.biz_date"}],
                }
            },
        }
        plan = {
            "selected_metrics": ["deposit_balance_daily.deposit_end_balance"],
            "selected_dimensions": ["deposit_balance_daily.biz_date"],
            "selected_filters": [{"field": "deposit_balance_daily.biz_date", "op": "between", "value": ["2026-01-01", "2026-01-31"]}],
            "selected_dataset_candidates": ["deposit_balance_daily"],
        }

        sql = compile_sql_from_semantic_plan(plan, semantic_layer)

        self.assertIn("FROM dim_calendar", sql)
        self.assertIn("LEFT JOIN fact_account_balance_daily as bal ON bal.biz_date = dim_calendar.biz_date", sql)
        self.assertIn("COALESCE(SUM(bal.end_balance), 0) AS deposit_balance_daily_deposit_end_balance", sql)


    def test_merge_llm_selection_drops_invalid_llm_filter_fields(self):
        token_hits = {
            "matches": [
                {"object_type": "metric", "canonical_name": "deposit_balance_daily.deposit_end_balance", "dataset": "deposit_balance_daily", "allowed": True},
            ]
        }
        llm_selection = {
            "selected_metrics": ["deposit_balance_daily.deposit_end_balance"],
            "selected_dimensions": [],
            "selected_dataset_candidates": ["deposit_balance_daily"],
            "selected_filters": [
                {"field": "deposit_balance_daily.transaction_date", "op": ">=", "value": "2024-01-01"},
                {"field": "deposit_balance_daily.transaction_date", "op": "<", "value": "2024-02-01"},
            ],
        }
        features = {"filters": [], "time_start": "2024-01-01", "time_end": "2024-01-31"}

        semantic_layer = {
            "entities": {},
            "datasets": {
                "deposit_balance_daily": {
                    "from": "fact_account_balance_daily as bal",
                    "metrics": [{"name": "deposit_end_balance", "expr": "bal.end_balance"}],
                    "dimensions": [],
                    "time_dimensions": [{"name": "biz_date", "expr": "bal.biz_date"}],
                    "joins": [],
                }
            },
        }

        merged = merge_llm_selection_into_plan(llm_selection, token_hits, features, semantic_layer=semantic_layer)

        self.assertEqual(
            merged["selected_filters"],
            [{"field": "deposit_balance_daily.biz_date", "op": "between", "value": ["2024-01-01", "2024-01-31"], "source": "step_b_time_bounds"}],
        )

    def test_merge_llm_selection_infers_time_dimension_when_user_requests_date_trend(self):
        token_hits = {
            "matches": [
                {
                    "object_type": "metric",
                    "canonical_name": "deposit_balance_daily.deposit_end_balance",
                    "dataset": "deposit_balance_daily",
                    "allowed": True,
                },
                {
                    "object_type": "field",
                    "canonical_name": "calendar.biz_date",
                    "dataset": "",
                    "entity": "calendar",
                    "allowed": True,
                },
            ]
        }
        llm_selection = {
            "selected_metrics": ["deposit_balance_daily.deposit_end_balance"],
            "selected_dimensions": [],
            "selected_dataset_candidates": ["deposit_balance_daily"],
            "selected_filters": [],
        }
        features = {"filters": [], "dimensions": ["日期"], "time_start": "2026-01-01", "time_end": "2026-01-31"}

        semantic_layer = {
            "entities": {
                "calendar": {
                    "table": "dim_calendar",
                    "fields": [{"name": "biz_date", "expr": "dim_calendar.biz_date", "synonyms": ["日期"]}],
                }
            },
            "datasets": {
                "deposit_balance_daily": {
                    "from": "fact_account_balance_daily as bal",
                    "metrics": [{"name": "deposit_end_balance", "expr": "bal.end_balance"}],
                    "dimensions": [],
                    "time_dimensions": [{"name": "biz_date", "expr": "bal.biz_date", "synonyms": ["日期"]}],
                    "joins": [{"entity": "calendar", "on": "bal.biz_date = dim_calendar.biz_date"}],
                }
            },
        }

        merged = merge_llm_selection_into_plan(llm_selection, token_hits, features, semantic_layer=semantic_layer)

        self.assertIn("deposit_balance_daily.biz_date", merged["selected_dimensions"])

    def test_merge_llm_selection_infers_metric_from_feature_text_when_step_c_has_no_metric(self):
        token_hits = {
            "matches": [
                {"object_type": "field", "canonical_name": "branch.region", "dataset": "", "allowed": True},
            ]
        }
        llm_selection = {
            "selected_metrics": [],
            "selected_dimensions": [],
            "selected_dataset_candidates": [],
            "selected_filters": [],
        }
        features = {
            "metrics": ["存款餘額總額"],
            "dimensions": ["地區"],
            "filters": ["地區 in(澳門半島,氹仔,路氹城,路環)"],
            "time_start": "2026-01-01",
            "time_end": "2026-01-31",
        }

        semantic_layer = {
            "entities": {
                "branch": {
                    "table": "dim_branch",
                    "fields": [{"name": "region", "expr": "dim_branch.region", "synonyms": ["地區"]}],
                }
            },
            "datasets": {
                "deposit_balance_daily": {
                    "from": "fact_account_balance_daily as bal",
                    "metrics": [{"name": "deposit_end_balance", "expr": "bal.end_balance", "synonyms": ["存款餘額"]}],
                    "dimensions": [],
                    "time_dimensions": [{"name": "biz_date", "expr": "bal.biz_date"}],
                    "joins": [{"entity": "branch", "on": "bal.branch_id = dim_branch.branch_id"}],
                }
            },
        }

        merged = merge_llm_selection_into_plan(llm_selection, token_hits, features, semantic_layer=semantic_layer)

        self.assertEqual(merged["selected_metrics"], ["deposit_balance_daily.deposit_end_balance"])
        self.assertEqual(merged["selected_dataset_candidates"], ["deposit_balance_daily"])
        self.assertIn(
            {"field": "branch.region", "op": "in", "value": ["澳門半島", "氹仔", "路氹城", "路環"], "source": "step_b_filters"},
            merged["selected_filters"],
        )

    def test_validator_rejects_blocked_sensitive_fields(self):
        plan = {
            "selected_metrics": ["sales.revenue"],
            "selected_dimensions": [],
            "selected_filters": [{"field": "sales.biz_date", "op": "between", "value": ["2026-01-01", "2026-01-31"]}],
            "selected_dataset_candidates": ["sales"],
        }
        token_hits = {"blocked_matches": [{"canonical_name": "customer.id_no"}]}

        result = validate_semantic_plan(plan, token_hits, {"require_time_filter": True}, semantic_layer=SEMANTIC_LAYER)

        self.assertFalse(result["ok"])
        self.assertIn("BLOCKED_MATCH", result["error_codes"])

    def test_validator_requires_time_filter_when_governance_enabled(self):
        plan = {
            "selected_metrics": ["sales.revenue"],
            "selected_dimensions": [],
            "selected_filters": [],
            "selected_dataset_candidates": ["sales"],
        }

        result = validate_semantic_plan(plan, {"blocked_matches": []}, {"require_time_filter": True}, semantic_layer=SEMANTIC_LAYER)

        self.assertFalse(result["ok"])
        self.assertIn("TIME_FILTER_REQUIRED", result["error_codes"])

    def test_validator_rejects_incomplete_time_axis(self):
        plan = {
            "selected_metrics": ["sales.revenue"],
            "selected_dimensions": [],
            "selected_filters": [{"field": "sales.biz_date", "op": "between", "value": ["2026-01-01", "2026-01-31"]}],
            "selected_dataset_candidates": ["sales"],
            "time_axis": {"has_time_filter": True, "start_date": "2026-01-01", "end_date": ""},
        }

        result = validate_semantic_plan(plan, {"blocked_matches": []}, {"require_time_filter": True}, semantic_layer=SEMANTIC_LAYER)

        self.assertFalse(result["ok"])
        self.assertIn("TIME_AXIS_INCOMPLETE", result["error_codes"])

    def test_validator_rejects_empty_selection(self):
        plan = {
            "selected_metrics": [],
            "selected_dimensions": [],
            "selected_filters": [{"field": "sales.biz_date", "op": "between", "value": ["2026-01-01", "2026-01-31"]}],
            "selected_dataset_candidates": ["sales"],
        }

        result = validate_semantic_plan(plan, {"blocked_matches": []}, {"require_time_filter": True}, semantic_layer=SEMANTIC_LAYER)

        self.assertFalse(result["ok"])
        self.assertIn("EMPTY_SELECTION", result["error_codes"])

    def test_validator_rejects_multi_dataset_without_shared_join_entity(self):
        semantic_layer = {
            "entities": {
                "calendar": {"table": "dim_calendar", "fields": [{"name": "biz_date", "expr": "dim_calendar.biz_date"}]},
                "branch": {"table": "dim_branch", "fields": [{"name": "region", "expr": "dim_branch.region"}]},
            },
            "datasets": {
                "sales": {
                    "from": "fact_sales as s",
                    "metrics": [{"name": "revenue", "expr": "SUM(s.revenue)"}],
                    "time_dimensions": [{"name": "biz_date", "expr": "s.biz_date"}],
                    "joins": [{"entity": "calendar", "on": "s.biz_date = dim_calendar.biz_date"}],
                },
                "complaints": {
                    "from": "fact_complaint as c",
                    "metrics": [{"name": "case_cnt", "expr": "COUNT(*)"}],
                    "time_dimensions": [{"name": "biz_date", "expr": "c.biz_date"}],
                    "joins": [{"entity": "branch", "on": "c.branch_id = dim_branch.branch_id"}],
                },
            },
        }
        plan = {
            "selected_metrics": ["sales.revenue", "complaints.case_cnt"],
            "selected_dimensions": [],
            "selected_filters": [{"field": "sales.biz_date", "op": "between", "value": ["2026-01-01", "2026-01-31"]}],
            "selected_dataset_candidates": ["sales", "complaints"],
        }

        result = validate_semantic_plan(plan, {"blocked_matches": []}, {"require_time_filter": True}, semantic_layer=semantic_layer)

        self.assertFalse(result["ok"])
        self.assertIn("MULTI_DATASET_NO_JOIN_PATH", result["error_codes"])

    def test_validator_rejects_dataset_mismatch_against_primary_dataset(self):
        plan = {
            "selected_metrics": ["sales.revenue"],
            "selected_dimensions": ["other.region"],
            "selected_filters": [{"field": "sales.biz_date", "op": "between", "value": ["2026-01-01", "2026-01-31"]}],
            "selected_dataset_candidates": ["sales"],
        }

        result = validate_semantic_plan(plan, {"blocked_matches": []}, {"require_time_filter": True}, semantic_layer=SEMANTIC_LAYER)

        self.assertFalse(result["ok"])
        self.assertIn("DATASET_MISMATCH", result["error_codes"])

    def test_validator_rejects_invalid_filter_shapes(self):
        plan = {
            "selected_metrics": ["sales.revenue"],
            "selected_dimensions": [],
            "selected_filters": ["sales.biz_date between 2026-01-01 and 2026-01-31"],
            "selected_dataset_candidates": ["sales"],
        }

        result = validate_semantic_plan(plan, {"blocked_matches": []}, {"require_time_filter": False}, semantic_layer=SEMANTIC_LAYER)

        self.assertFalse(result["ok"])
        self.assertIn("INVALID_FILTER_SHAPE", result["error_codes"])

    def test_validator_rejects_between_with_invalid_value_count(self):
        plan = {
            "selected_metrics": ["sales.revenue"],
            "selected_dimensions": [],
            "selected_filters": [{"field": "sales.biz_date", "op": "between", "value": ["2026-01-01"]}],
            "selected_dataset_candidates": ["sales"],
        }

        result = validate_semantic_plan(plan, {"blocked_matches": []}, {"require_time_filter": False}, semantic_layer=SEMANTIC_LAYER)

        self.assertFalse(result["ok"])
        self.assertIn("INVALID_FILTER_BETWEEN", result["error_codes"])

    def test_validator_rejects_in_without_values(self):
        plan = {
            "selected_metrics": ["sales.revenue"],
            "selected_dimensions": [],
            "selected_filters": [{"field": "branch.region", "op": "in", "value": []}],
            "selected_dataset_candidates": ["sales"],
        }

        result = validate_semantic_plan(plan, {"blocked_matches": []}, {"require_time_filter": False}, semantic_layer=SEMANTIC_LAYER)

        self.assertFalse(result["ok"])
        self.assertIn("INVALID_FILTER_VALUE", result["error_codes"])

    def test_validator_rejects_when_no_compilable_select_exists(self):
        plan = {
            "selected_metrics": [],
            "selected_dimensions": ["branch.region"],
            "selected_filters": [{"field": "sales.biz_date", "op": "between", "value": ["2026-01-01", "2026-01-31"]}],
            "selected_dataset_candidates": ["sales"],
        }

        result = validate_semantic_plan(plan, {"blocked_matches": []}, {"require_time_filter": False}, semantic_layer=SEMANTIC_LAYER)

        self.assertFalse(result["ok"])
        self.assertIn("NO_COMPILABLE_SELECT", result["error_codes"])


if __name__ == "__main__":
    unittest.main()
