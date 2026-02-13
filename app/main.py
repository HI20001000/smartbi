from datetime import datetime

from dotenv import load_dotenv

from app.cli_ui import print_startup_ui
from app.config import Settings
from app.intent_router import IntentType, classify_intent
from app.llm_service import LLMChatSession
from app.semantic_loader import load_semantic_layer, get_governance
from app.semantic_validator import validate_semantic_plan
from app.token_matcher import SemanticTokenMatcher


def _date_tag() -> str:
    return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")


def main():
    load_dotenv()
    settings = Settings.load()
    session = LLMChatSession(settings)
    semantic_layer = load_semantic_layer()
    governance_limits = get_governance(semantic_layer)
    matcher = SemanticTokenMatcher("app/semantics/smartbi_demo_macau_banking_semantic.yaml")

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
            token_hits = matcher.match(features)
            enhanced_plan = session.enhance_semantic_matches_with_llm(
                user_input=user_input,
                extracted_features=features,
                token_hits=token_hits,
                governance_limits=governance_limits,
            )
            time_axis = session.resolve_time_axis_with_llm(
                user_input=user_input,
                extracted_features=features,
                selected_dataset_candidates=enhanced_plan.get("selected_dataset_candidates", []) or [],
            )
            if time_axis.get("has_time_filter") and not (enhanced_plan.get("selected_filters") or []):
                enhanced_plan["selected_filters"] = [
                    {
                        "field": time_axis.get("time_dimension") or "calendar.biz_date",
                        "op": "between",
                        "value": [time_axis.get("start_date", ""), time_axis.get("end_date", "")],
                        "source": "llm_time_axis",
                    }
                ]
            enhanced_plan["time_axis"] = time_axis

            validation = validate_semantic_plan(enhanced_plan, token_hits, governance_limits)

            print(
                f"{_date_tag()}AI> 已識別為 SQL 任務（Step A）。\n"
                f"Step B 特徵提取結果：{features}\n"
                f"Step C Token 命中結果：{token_hits}\n"
                f"Step D LLM 輔助規劃：{enhanced_plan}\n"
                f"Step D.5 時間軸解析：{time_axis}\n"
                f"Step E 規則校驗：{validation}\n"
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
