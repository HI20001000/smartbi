from dotenv import load_dotenv

from app.cli_ui import print_startup_ui
from app.config import Settings
from app.llm_service import LLMChatSession


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
            user_input = input("You> ").strip()
        except (EOFError, KeyboardInterrupt) as e:
            print(f"\n[Input Error] :{e}. Exiting.")
            return

        if not user_input:
            continue
        if user_input in {"/exit", "exit", "quit"}:
            print("Bye!")
            return

        try:
            reply = session.ask(user_input)
        except Exception as e:
            print(f"[ERROR] LLM call failed: {e}")
            continue

        print(f"AI> {reply}\n")


if __name__ == "__main__":
    main()
