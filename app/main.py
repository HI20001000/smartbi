import argparse
from datetime import datetime
from decimal import Decimal
import json
from pathlib import Path
import time

from dotenv import load_dotenv

from app.chart_planner import build_chart_spec
from app.chart_renderer import render_chart
from app.cli_ui import print_startup_ui
from app.config import Settings
from app.intent_router import IntentType, classify_intent
from app.llm_service import LLMChatSession
from app.query_executor import SQLQueryExecutor
from app.semantic_loader import get_governance, load_semantic_layer
from app.semantic_validator import validate_semantic_plan
from app.sql_compiler import compile_sql_from_semantic_plan
from app.sql_planner import merge_llm_selection_into_plan
from app.token_matcher import SemanticTokenMatcher


def _date_tag() -> str:
    return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")


def _pretty(data: object) -> str:
    def _json_fallback(value: object) -> object:
        if isinstance(value, Decimal):
            return float(value)
        return str(value)

    return json.dumps(data, ensure_ascii=False, indent=2, default=_json_fallback)


def _dark_log_block(text: str) -> str:
    # ANSI dark style block (fallback to plain text if terminal does not support ANSI)
    return f"\033[48;5;236m\033[97m{text}\033[0m"


def _detect_preferred_chart_type(features: dict) -> str | None:
    keywords = [
        *(features.get("tokens", []) or []),
        *(features.get("dimensions", []) or []),
        *(features.get("filters", []) or []),
    ]
    text = " ".join(str(k) for k in keywords)
    mapping = [
        (("圓餅圖", "餅圖", "餅形圖", "pie"), "pie"),
        (("散佈圖", "散点图", "scatter"), "scatter"),
        (("折線圖", "線圖", "line"), "line"),
        (("直條圖", "柱狀圖", "長條圖", "bar"), "bar"),
    ]
    for terms, chart_type in mapping:
        if any(term in text for term in terms):
            return chart_type
    return None


def _find_time_between_filter(enhanced_plan: dict) -> tuple[str, str] | None:
    for item in enhanced_plan.get("selected_filters", []) or []:
        if not isinstance(item, dict):
            continue
        op = str(item.get("op", "") or "").lower()
        field = str(item.get("field", "") or "")
        value = item.get("value")
        if op != "between" or not field.endswith(".biz_date"):
            continue
        if not isinstance(value, list) or len(value) != 2:
            continue
        start = str(value[0] or "").strip()
        end = str(value[1] or "").strip()
        if start and end:
            return start, end
    return None


def _build_dataset_time_bounds_sql(enhanced_plan: dict, semantic_layer: dict) -> str | None:
    datasets = enhanced_plan.get("selected_dataset_candidates", []) or []
    dataset_name = str(datasets[0]).strip() if datasets else ""
    if not dataset_name:
        return None

    dataset = ((semantic_layer or {}).get("datasets", {}) or {}).get(dataset_name, {}) or {}
    from_clause = str(dataset.get("from", "") or "").strip()
    time_dimensions = dataset.get("time_dimensions", []) or []
    if not from_clause or not time_dimensions:
        return None

    time_expr = str(time_dimensions[0].get("expr", "") or "").strip()
    if not time_expr:
        return None

    return (
        f"SELECT MIN({time_expr}) AS min_biz_date, MAX({time_expr}) AS max_biz_date "
        f"FROM {from_clause}"
    )


def _get_dataset_time_bounds(
    enhanced_plan: dict,
    semantic_layer: dict,
    executor: SQLQueryExecutor,
) -> tuple[str, str] | None:
    bounds_sql = _build_dataset_time_bounds_sql(enhanced_plan, semantic_layer)
    if not bounds_sql:
        return None

    try:
        bounds_result = executor.run(bounds_sql, max_rows=1)
    except Exception:
        return None

    if not bounds_result.rows:
        return None

    row = bounds_result.rows[0]
    min_date = row.get("min_biz_date")
    max_date = row.get("max_biz_date")
    if min_date is None or max_date is None:
        return None

    min_text = str(min_date).strip()
    max_text = str(max_date).strip()
    if not min_text or not max_text:
        return None
    return min_text, max_text


def _compute_adjusted_time_range(
    requested_start: str,
    requested_end: str,
    data_start: str,
    data_end: str,
) -> tuple[str, str] | None:
    overlap_start = max(requested_start, data_start)
    overlap_end = min(requested_end, data_end)
    if overlap_start <= overlap_end:
        return overlap_start, overlap_end

    # completely disjoint: fallback to full available range to guarantee non-empty opportunity
    if requested_end < data_start or requested_start > data_end:
        return data_start, data_end
    return None


def _replace_time_between_filter(enhanced_plan: dict, start: str, end: str) -> dict | None:
    filters = enhanced_plan.get("selected_filters", []) or []
    updated_filters: list[dict] = []
    replaced = False

    for item in filters:
        if not isinstance(item, dict):
            updated_filters.append(item)
            continue
        op = str(item.get("op", "") or "").lower()
        field = str(item.get("field", "") or "")
        if (not replaced) and op == "between" and field.endswith(".biz_date"):
            copied = dict(item)
            copied["value"] = [start, end]
            copied["source"] = "auto_adjusted_time_bounds"
            updated_filters.append(copied)
            replaced = True
            continue
        updated_filters.append(item)

    if not replaced:
        return None

    plan_copy = dict(enhanced_plan)
    plan_copy["selected_filters"] = updated_filters
    return plan_copy


def _build_empty_result_hint(
    requested_start: str,
    requested_end: str,
    data_start: str,
    data_end: str,
    adjusted_start: str,
    adjusted_end: str,
) -> str:
    return (
        "\n[診斷] 查詢時間範圍可能超出資料可用區間："
        f"請求 {requested_start} ~ {requested_end}；"
        f"資料約為 {data_start} ~ {data_end}。"
        "\n[修正] 已自動改用可用時間範圍重新查詢："
        f"{adjusted_start} ~ {adjusted_end}。"
    )


def _split_sql_script(script_text: str) -> list[str]:
    statements: list[str] = []
    chunks = script_text.split(";")
    for chunk in chunks:
        lines = []
        for line in chunk.splitlines():
            stripped = line.strip()
            if stripped.startswith("--"):
                continue
            lines.append(line)
        statement = "\n".join(lines).strip()
        if statement:
            statements.append(statement)
    return statements


def _run_sql_script_file(sql_file: str, settings: Settings) -> bool:
    file_path = Path(sql_file)
    if not file_path.exists() and file_path.name == "example_data.sql":
        typo_fallback = file_path.with_name("exmaple_data.sql")
        if typo_fallback.exists():
            file_path = typo_fallback

    if not file_path.exists():
        print(f"[Batch SQL] 檔案不存在：{file_path}")
        return False

    missing_db_fields = [
        name
        for name, value in (("db_host", settings.db_host), ("db_user", settings.db_user), ("db_name", settings.db_name))
        if not value
    ]
    if missing_db_fields:
        print(f"[Batch SQL] 無法執行 SQL 檔案，缺少 DB 設定：{', '.join(missing_db_fields)}")
        return False

    try:
        import pymysql
    except Exception as exc:
        print(f"[Batch SQL] 缺少 pymysql 依賴：{exc}")
        return False

    script_text = file_path.read_text(encoding="utf-8")
    statements = _split_sql_script(script_text)
    if not statements:
        print(f"[Batch SQL] 檔案無可執行語句：{file_path}")
        return True

    print(f"[Batch SQL] 開始執行：{file_path}（共 {len(statements)} 條語句）")
    try:
        conn = pymysql.connect(
            host=settings.db_host,
            port=settings.db_port,
            user=settings.db_user,
            password=settings.db_password or "",
            database=settings.db_name,
            autocommit=True,
        )
    except Exception as exc:
        print(f"[Batch SQL] 連線失敗：{exc}")
        return False

    try:
        with conn.cursor() as cursor:
            for idx, statement in enumerate(statements, start=1):
                try:
                    cursor.execute(statement)
                    print(f"[Batch SQL] ({idx}/{len(statements)}) OK")
                except Exception as exc:
                    print(f"[Batch SQL] ({idx}/{len(statements)}) FAILED: {exc}")
                    return False
    finally:
        conn.close()

    print("[Batch SQL] 執行完成。")
    return True


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SmartBI Chat CLI")
    parser.add_argument(
        "--sql-file",
        default=None,
        help="以批次模式執行 SQL 檔案（例如 sql/example_data.sql）",
    )
    return parser.parse_args()


def main():
    args = _parse_args()
    load_dotenv()
    settings = Settings.load()

    if args.sql_file:
        _run_sql_script_file(args.sql_file, settings)
        return

    session = LLMChatSession(settings)
    semantic_layer = load_semantic_layer()
    governance_limits = get_governance(semantic_layer)
    matcher = SemanticTokenMatcher(
        "app/semantics/smartbi_demo_macau_banking_semantic.yaml",
        embedding_base_url=settings.embedding_url,
        embedding_model=settings.embedding_model,
        embedding_api_key=settings.embedding_api_key,
        reranker_base_url=settings.reranker_url,
        reranker_model=settings.reranker_model,
        reranker_api_key=settings.reranker_api_key,
        reranker_score_threshold=settings.reranker_score_threshold,
    )

    print_startup_ui(
        model=settings.llm_model,
        base_url=settings.llm_base_url,
        version="1.0.0",
        app_name="SmartBI Chat CLI",
        framework="LangChain",
        clear_screen=True,
    )

    while True:
        try:
            user_input = input(f"{_date_tag()}You> ").strip()
        except (EOFError, KeyboardInterrupt) as e:
            print(f"\n[Input Error] :{e}. Exiting.")
            return

        if not user_input:
            continue

        intent_result = classify_intent(user_input, session)
        print(f"AI work in {intent_result.intent} intent (confidence: {intent_result.confidence:.2f})")
        if intent_result.intent == IntentType.EXIT:
            print("Bye!")
            return

        if intent_result.intent == IntentType.SQL:
            features = session.extract_sql_features_with_llm(user_input)
            print(f"\n{_date_tag()}AI> 已識別為 SQL 任務（Step A）。")

            token_hits = matcher.match(features)
            enhanced_plan = merge_llm_selection_into_plan(
                llm_selection={},
                token_hits=token_hits,
                extracted_features=features,
                semantic_layer=semantic_layer,
            )

            validation = validate_semantic_plan(
                enhanced_plan,
                token_hits,
                governance_limits,
                semantic_layer=semantic_layer,
            )

            generated_sql = ""
            compile_start = time.perf_counter()
            if validation.get("ok"):
                generated_sql = compile_sql_from_semantic_plan(
                    enhanced_plan=enhanced_plan,
                    semantic_layer=semantic_layer,
                )
            compile_ms = round((time.perf_counter() - compile_start) * 1000, 2)

            missing_db_fields = [
                name
                for name, value in (("db_host", settings.db_host), ("db_user", settings.db_user), ("db_name", settings.db_name))
                if not value
            ]
            chart_status = (
                "Step G/H/I 略過：缺少 DB 設定 " + ", ".join(missing_db_fields)
                if missing_db_fields
                else "Step G/H/I 略過：未啟用 SQL 執行。"
            )
            summary_status = "Step J 數據摘要：略過（尚無可用結果）"
            if not validation.get("ok"):
                failure_message = "; ".join(validation.get("errors", []) or []) or "規則校驗失敗"
                summary_text = session.summarize_failure_with_llm(user_input, failure_message)
                summary_status = _dark_log_block(f"Step J 數據摘要（錯誤修飾）：\n{summary_text}")
                chart_status = "Step G/H/I 略過：因 Step E 規則校驗失敗，停止後續步驟。"
            elif generated_sql and not missing_db_fields:
                try:
                    executor = SQLQueryExecutor(
                        host=settings.db_host,
                        port=settings.db_port,
                        user=settings.db_user,
                        password=settings.db_password or "",
                        database=settings.db_name,
                        read_timeout=governance_limits.get("timeout_seconds", 30),
                    )
                    result = executor.run(
                        generated_sql,
                        max_rows=governance_limits.get("max_rows", 1000),
                    )

                    preferred_chart_type = _detect_preferred_chart_type(features)
                    chart_spec = build_chart_spec(
                        result,
                        title="SmartBI SQL Result",
                        preferred_chart_type=preferred_chart_type,
                    )
                    chart_path = render_chart(
                        result,
                        chart_spec,
                        f"{settings.chart_output_dir}/query_chart.png",
                    )
                    chart_status = (
                        f"Step G SQL 執行筆數：{len(result.rows)}\n"
                        f"Step H 圖表規劃：{chart_spec}\n"
                        f"Step I 圖表輸出：{chart_path}"
                    )
                    zero_rows_notice = "[提醒] 查詢結果為 0 筆，當前條件下沒有可用數據。" if len(result.rows) == 0 else ""
                    try:
                        summary_text = session.summarize_query_result_with_llm(user_input, result.rows, max_rows=20)
                        summary_body = f"{zero_rows_notice}\n{summary_text}" if zero_rows_notice else summary_text
                        summary_status = _dark_log_block(f"Step J 數據摘要：\n{summary_body}")
                    except Exception as summary_exc:
                        if zero_rows_notice:
                            summary_status = f"Step J 數據摘要：\n{zero_rows_notice}\n（摘要生成失敗：{summary_exc}）"
                        else:
                            summary_status = f"Step J 數據摘要：略過（摘要生成失敗：{summary_exc}）"
                except Exception as exc:
                    failure_message = str(exc) or "Step G/H/I 執行失敗"
                    chart_status = f"Step G/H/I 略過或失敗：{failure_message}"
                    summary_text = session.summarize_failure_with_llm(user_input, failure_message)
                    summary_status = _dark_log_block(f"Step J 數據摘要（錯誤修飾）：\n{summary_text}")

            metrics_payload = {
                "validation_ok": validation.get("ok", False),
                "validation_error_codes": validation.get("error_codes", []),
                "selected_metrics_count": len(enhanced_plan.get("selected_metrics", []) or []),
                "selected_dimensions_count": len(enhanced_plan.get("selected_dimensions", []) or []),
                "selected_filters_count": len(enhanced_plan.get("selected_filters", []) or []),
                "compile_elapsed_ms": compile_ms,
                "sql_generated": bool(generated_sql),
            }

            sql_text = generated_sql if generated_sql else "[尚未生成，請先修正校驗錯誤]"
            print(
                "\n"
                f"Step B 特徵提取結果：\n{_pretty(features)}\n"
                f"Step C Token 命中結果：\n{_pretty(token_hits)}\n"
                f"Step D 合併後計畫（Deterministic）：\n{_pretty(enhanced_plan)}\n"
                f"Step E 規則校驗：\n{_pretty(validation)}\n"
                f"Step F SQL 生成結果：\n{sql_text}\n"
                f"Observability Metrics：\n{_pretty(metrics_payload)}\n"
                f"{chart_status}\n"
                f"{summary_status}\n"
            )
            continue

        try:
            reply = session.ask(user_input)
        except Exception as e:
            print(f"[ERROR] LLM call failed: {e}")
            continue

        print(f"{_date_tag()}AI> {reply}\n")


if __name__ == "__main__":
    main()
