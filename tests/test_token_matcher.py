from app.token_matcher import SemanticTokenMatcher


def _build_matcher() -> SemanticTokenMatcher:
    return SemanticTokenMatcher("app/semantics/smartbi_demo_macau_banking_semantic.yaml")


def test_stepc_expanded_semantic_refs_and_time_passthrough():
    matcher = _build_matcher()
    step_b = {
        "tokens": ["近7天", "澳門分行", "交易筆數"],
        "metrics": ["交易筆數", "交易筆數"],
        "dimensions": ["分行", "分行"],
        "filters": ["分行=澳門", "分行=澳門"],
        "time_start": "2024-01-01",
        "time_end": "2024-12-31",
    }

    result = matcher.match(step_b)

    assert "limit" not in result
    assert "canonical_name" not in result["semantic_refs"]
    assert result["tokens"] == step_b["tokens"]
    assert result["time_start"] == "2024-01-01"
    assert result["time_end"] == "2024-12-31"

    assert result["semantic_refs"]["dataset"] == "transactions"
    assert result["semantic_refs"]["time_field"] == "tx.biz_date"
    assert result["semantic_refs"]["metrics"] == [
        {"name": "txn_count", "agg": "count", "expr": "tx.txn_id"}
    ]
    assert result["semantic_refs"]["dimensions"] == [
        {"name": "branch_name", "expr": "dim_branch.branch_name"}
    ]
    assert result["semantic_refs"]["filters"] == [
        {"expr": "dim_branch.branch_name", "op": "=", "value": "澳門"}
    ]


def test_stepc_missing_mapping_returns_empty_arrays_without_crash():
    matcher = _build_matcher()
    step_b = {
        "tokens": ["未知詞"],
        "metrics": ["不存在指標"],
        "dimensions": ["不存在維度"],
        "filters": ["不存在欄位=值"],
        "time_start": " 2024-02-01 ",
        "time_end": "2024-02-28 ",
    }

    result = matcher.match(step_b)

    assert result["semantic_refs"]["dataset"] == ""
    assert result["semantic_refs"]["time_field"] == ""
    assert result["semantic_refs"]["metrics"] == []
    assert result["semantic_refs"]["dimensions"] == []
    assert result["semantic_refs"]["filters"] == []

    # passthrough, no normalization/modification in StepC
    assert result["time_start"] == " 2024-02-01 "
    assert result["time_end"] == "2024-02-28 "
