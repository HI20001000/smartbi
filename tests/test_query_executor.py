from app.query_executor import SQLQueryExecutor


def test_only_select_allowed():
    assert SQLQueryExecutor._is_safe_select("SELECT 1")
    assert SQLQueryExecutor._is_safe_select("select col from t where id = 1")
    assert not SQLQueryExecutor._is_safe_select("DELETE FROM t")
    assert not SQLQueryExecutor._is_safe_select("SELECT * FROM t; DROP TABLE t")
