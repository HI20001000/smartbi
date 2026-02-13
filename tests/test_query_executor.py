from app.query_executor import SQLQueryExecutor


def test_only_select_allowed():
    assert SQLQueryExecutor._is_safe_select("SELECT 1")
    assert SQLQueryExecutor._is_safe_select("select col from t where id = 1")
    assert SQLQueryExecutor._is_safe_select("SELECT col FROM t;")
    assert not SQLQueryExecutor._is_safe_select("DELETE FROM t")
    assert not SQLQueryExecutor._is_safe_select("SELECT * FROM t; DROP TABLE t")
    assert not SQLQueryExecutor._is_safe_select("SELECT 1;   SELECT 2")


def test_normalize_single_select_sql_strips_single_trailing_semicolon():
    sql = "SELECT a, b FROM t WHERE id = 1;"
    normalized = SQLQueryExecutor._normalize_single_select_sql(sql)
    assert normalized == "SELECT a, b FROM t WHERE id = 1"


def test_normalize_single_select_sql_accepts_markdown_code_fence():
    sql = "```sql\nSELECT a, b FROM t WHERE id = 1;\n```"
    normalized = SQLQueryExecutor._normalize_single_select_sql(sql)
    assert normalized == "SELECT a, b FROM t WHERE id = 1"


def test_normalize_single_select_sql_accepts_quoted_escaped_newlines():
    sql = '"SELECT\\n  a, b\\nFROM t\\nWHERE id = 1;"'
    normalized = SQLQueryExecutor._normalize_single_select_sql(sql)
    assert normalized == "SELECT\n  a, b\nFROM t\nWHERE id = 1"
