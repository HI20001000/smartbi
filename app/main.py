from datetime import datetime

from dotenv import load_dotenv

from app.cli_ui import print_startup_ui
from app.config import Settings
from app.intent_router import IntentType, classify_intent
from app.llm_service import LLMChatSession


def _date_tag() -> str:
    return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")


def main():
    load_dotenv()
    settings = Settings.load()
    session = LLMChatSession(settings)

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
        if intent_result.intent == IntentType.EXIT:
            print("Bye!")
            return

        if intent_result.intent == IntentType.SQL:
            print(f"{_date_tag()}AI> 已識別為 SQL 任務（Step A）。請補充資料表/欄位與查詢條件。\n")
            continue

        try:
            reply = session.ask(user_input)
        except Exception as e:
            print(f"[ERROR] LLM call failed: {e}")
            continue

        print(f"{_date_tag()}AI> {reply}\n")


if __name__ == "__main__":
    main()
