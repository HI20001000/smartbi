import json

from langchain_core.messages import SystemMessage, HumanMessage, AIMessage
from langchain_openai import ChatOpenAI

from app.config import Settings


class LLMChatSession:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = ChatOpenAI(
            base_url=settings.llm_base_url,
            api_key=settings.llm_api_key,
            model=settings.llm_model,
            temperature=settings.temperature,
            max_tokens=settings.max_tokens,
        )
        self.history = [
            SystemMessage(content="你是個助理，請用繁體中文回答，回答要清楚、簡潔。")
        ]

    def ask(self, user_input: str) -> str:
        self.history.append(HumanMessage(content=user_input))
        try:
            resp = self.client.invoke(self.history)
            reply = getattr(resp, "content", str(resp)).strip()
        except Exception:
            self.history.pop()
            raise

        self.history.append(AIMessage(content=reply))
        return reply

    def classify_intent_with_llm(self, user_input: str) -> str:
        prompt = [
            SystemMessage(
                content=(
                    "你是意圖分類器。請判斷使用者輸入意圖並輸出 JSON。"
                    "可用 intent 僅有 EXIT、SQL、CHAT。"
                    "輸出格式固定為："
                    '{"intent":"CHAT","confidence":0.0,"reason":"..."}'
                    "不要輸出任何 JSON 以外文字。"
                )
            ),
            HumanMessage(content=user_input),
        ]
        resp = self.client.invoke(prompt)
        return getattr(resp, "content", str(resp)).strip()


    def generate_sql_from_json_plan_with_llm(
        self,
        user_input: str,
        json_plan: dict,
        semantic_layer: dict,
    ) -> str:
        prompt = [
            SystemMessage(
                content=(
                    "你是 SQL 生成器。根據提供的 JSON plan 與 semantic layer 產生 SQL。"
                    "只輸出 SQL，不要任何解釋。"
                    "規則："
                    "1) 禁止使用未在 semantic layer 出現的資料表/欄位；"
                    "2) 禁止 SELECT *；"
                    "3) 優先使用 plan 中 selected_dataset_candidates[0]；"
                    "4) 若 plan 不足以產生 SQL，輸出 SELECT 1 WHERE 1=0。"
                )
            ),
            HumanMessage(
                content=(
                    f"user_input={user_input}\n"
                    f"json_plan={json.dumps(json_plan, ensure_ascii=False)}\n"
                    f"semantic_layer={json.dumps(semantic_layer, ensure_ascii=False)}"
                )
            ),
        ]
        resp = self.client.invoke(prompt)
        return getattr(resp, "content", str(resp)).strip()

    def extract_sql_features_with_llm(self, user_input: str) -> dict:
        prompt = [
            SystemMessage(
                content=(
                    "你是 SQL 查詢特徵提取器。"
                    "請從使用者自然語言提取 SQL 相關關鍵資訊，"
                    "並且只能輸出 JSON。"
                    "輸出格式固定為："
                    '{"tokens":[],"metrics":[],"dimensions":[],"filters":[],"time_range":"","time_start":"","time_end":"","needs_clarification":false}'
                    "說明："
                    "tokens 放通用關鍵詞；metrics 放指標詞；dimensions 放維度詞；"
                    "filters 放條件詞；time_range 放時間範圍（若無可留空字串）。"
                    "time_start/time_end 為時間範圍起訖，格式 yyyy-mm-dd；若無法確定可留空。"
                    "time_range 一律規範成可機器解析格式："
                    "1) 年份：例如『2024年』=> '2024-01-01 TO 2024-12-31'；"
                    "2) 年月：例如『2024年12月』=> '2024-12-01 TO 2024-12-31'；"
                    "3) 月份（無年份）：例如『12月各分行』=> 'MONTH:12'，表示12月內所有資料；"
                    "4) 區間：例如『2024年6月到2025年尾』=> '2024-06-01 TO 2025-12-31'。"
                    "不要輸出 JSON 以外文字。"
                )
            ),
            HumanMessage(content=user_input),
        ]

        try:
            resp = self.client.invoke(prompt)
            raw = getattr(resp, "content", str(resp)).strip()
            parsed = json.loads(raw)
        except Exception:
            return {
                "tokens": [],
                "metrics": [],
                "dimensions": [],
                "filters": [],
                "time_range": "",
                "time_start": "",
                "time_end": "",
                "needs_clarification": True,
            }

        return {
            "tokens": parsed.get("tokens", []) or [],
            "metrics": parsed.get("metrics", []) or [],
            "dimensions": parsed.get("dimensions", []) or [],
            "filters": parsed.get("filters", []) or [],
            "time_range": str(parsed.get("time_range", "") or ""),
            "time_start": str(parsed.get("time_start", "") or ""),
            "time_end": str(parsed.get("time_end", "") or ""),
            "needs_clarification": bool(parsed.get("needs_clarification", False)),
        }
