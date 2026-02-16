from datetime import datetime

from dotenv import load_dotenv

from app.chart_planner import build_chart_spec
from app.chart_renderer import render_chart
from app.cli_ui import print_startup_ui
from app.config import Settings
from app.intent_router import IntentType, classify_intent
from app.llm_service import LLMChatSession
from app.query_executor import SQLQueryExecutor
from app.semantic_loader import load_semantic_layer, get_governance
from app.semantic_validator import validate_semantic_plan
from app.sql_compiler import compile_sql_from_semantic_plan
from app.sql_planner import build_semantic_plan
from app.token_matcher import SemanticTokenMatcher


def _date_tag() -> str:
    return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")


def main():
    load_dotenv()
    settings = Settings.load()
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
            print(
                f"\n\n{_date_tag()}AI> 已識別為 SQL 任務（Step A）。\n"
                f"Step B 特徵提取結果：{features}\n\n")
            
            token_hits = matcher.match(features)
            print(f"\n\nStep C Token 命中結果：{token_hits}\n\n")
            enhanced_plan = build_semantic_plan(
                extracted_features=features,
                token_hits=token_hits,
            )

            validation = validate_semantic_plan(
                enhanced_plan,
                token_hits,
                governance_limits,
                semantic_layer=semantic_layer,
            )

            generated_sql = ""
            if validation.get("ok"):
                try:
                    generated_sql = session.generate_sql_from_json_plan_with_llm(
                        user_input=user_input,
                        json_plan=enhanced_plan,
                        semantic_layer=semantic_layer,
                    )
                    if not generated_sql or "select *" in generated_sql.lower():
                        raise ValueError("LLM SQL invalid")
                except Exception:
                    generated_sql = compile_sql_from_semantic_plan(
                        enhanced_plan=enhanced_plan,
                        semantic_layer=semantic_layer,
                        limit=governance_limits.get("max_rows", 200),
                    )

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
            if generated_sql and not missing_db_fields:
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
                    chart_spec = build_chart_spec(result, title="SmartBI SQL Result")
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
                except Exception as exc:
                    chart_status = f"Step G/H/I 略過或失敗：{exc}"

            print(
                f"Step C Token 命中結果：{token_hits}\n"
                f"Step D 規劃結果（Deterministic）：{enhanced_plan}\n"
                f"Step E 規則校驗：{validation}\n"
                f"Step F SQL 生成結果：\n{generated_sql if generated_sql else '[尚未生成，請先修正校驗錯誤]'}\n"
                f"{chart_status}\n"
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
