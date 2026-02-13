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

    def extract_sql_features_with_llm(self, user_input: str) -> dict:
        prompt = [
            SystemMessage(
                content=(
                    "你是 SQL 查詢特徵提取器。"
                    "請從使用者自然語言提取 SQL 相關關鍵資訊，"
                    "並且只能輸出 JSON。"
                    "輸出格式固定為："
                    '{"tokens":[],"metrics":[],"dimensions":[],"filters":[],"time_range":"","needs_clarification":false}'
                    "說明："
                    "tokens 放通用關鍵詞；metrics 放指標詞；dimensions 放維度詞；"
                    "filters 放條件詞；time_range 放時間範圍（若無可留空字串）。"
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
                "needs_clarification": True,
            }

        return {
            "tokens": parsed.get("tokens", []) or [],
            "metrics": parsed.get("metrics", []) or [],
            "dimensions": parsed.get("dimensions", []) or [],
            "filters": parsed.get("filters", []) or [],
            "time_range": str(parsed.get("time_range", "") or ""),
            "needs_clarification": bool(parsed.get("needs_clarification", False)),
        }

    def enhance_semantic_matches_with_llm(
        self,
        user_input: str,
        extracted_features: dict,
        token_hits: dict,
        governance_limits: dict,
    ) -> dict:
        prompt = [
            SystemMessage(
                content=(
                    "你是 SQL 語意規劃器。"
                    "你只能從提供的 token 命中候選中做選擇，不可杜撰欄位/指標/資料集。"
                    "若條件不足，請回傳 needs_clarification=true 並提供 clarification_questions。"
                    "若命中敏感欄位（allowed=false），請放入 rejected_candidates。"
                    "輸出格式固定為："
                    '{"selected_metrics":[],"selected_dimensions":[],"selected_filters":[],"selected_dataset_candidates":[],"rejected_candidates":[],"needs_clarification":false,"clarification_questions":[],"confidence":0.0}'
                    "不要輸出任何 JSON 以外文字。"
                )
            ),
            HumanMessage(
                content=json.dumps(
                    {
                        "user_input": user_input,
                        "extracted_features": extracted_features,
                        "token_hits": token_hits,
                        "governance_limits": governance_limits,
                    },
                    ensure_ascii=False,
                )
            ),
        ]

        try:
            resp = self.client.invoke(prompt)
            raw = getattr(resp, "content", str(resp)).strip()
            parsed = json.loads(raw)
        except Exception:
            return {
                "selected_metrics": [],
                "selected_dimensions": [],
                "selected_filters": [],
                "selected_dataset_candidates": [],
                "rejected_candidates": [],
                "needs_clarification": True,
                "clarification_questions": ["我暫時無法穩定判斷語意對象，請補充想查的指標、維度與時間。"],
                "confidence": 0.0,
            }

        return {
            "selected_metrics": parsed.get("selected_metrics", []) or [],
            "selected_dimensions": parsed.get("selected_dimensions", []) or [],
            "selected_filters": parsed.get("selected_filters", []) or [],
            "selected_dataset_candidates": parsed.get("selected_dataset_candidates", []) or [],
            "rejected_candidates": parsed.get("rejected_candidates", []) or [],
            "needs_clarification": bool(parsed.get("needs_clarification", False)),
            "clarification_questions": parsed.get("clarification_questions", []) or [],
            "confidence": float(parsed.get("confidence", 0.0) or 0.0),
        }

    def resolve_time_axis_with_llm(
        self,
        user_input: str,
        extracted_features: dict,
        selected_dataset_candidates: list[str],
    ) -> dict:
        prompt = [
            SystemMessage(
                content=(
                    "你是 SQL 時間軸解析器。"
                    "你需要把自然語言時間描述轉成固定格式 JSON。"
                    "若可解析，has_time_filter=true，並填入 start_date/end_date（YYYY-MM-DD）。"
                    "time_dimension 優先用 biz_date，若語境要求時間戳可用 txn_ts。"
                    "granularity 僅允許 day/month/quarter/year。"
                    "relative_type 僅允許 explicit_date_range/relative_days/relative_months/relative_years/calendar_period/none。"
                    "輸出格式固定為："
                    '{"has_time_filter":false,"time_dimension":"","start_date":"","end_date":"","granularity":"day","relative_type":"none","source_text":""}'
                    "不要輸出任何 JSON 以外文字。"
                )
            ),
            HumanMessage(
                content=json.dumps(
                    {
                        "user_input": user_input,
                        "time_range": extracted_features.get("time_range", ""),
                        "tokens": extracted_features.get("tokens", []),
                        "selected_dataset_candidates": selected_dataset_candidates,
                    },
                    ensure_ascii=False,
                )
            ),
        ]

        try:
            resp = self.client.invoke(prompt)
            raw = getattr(resp, "content", str(resp)).strip()
            parsed = json.loads(raw)
        except Exception:
            return {
                "has_time_filter": False,
                "time_dimension": "",
                "start_date": "",
                "end_date": "",
                "granularity": "day",
                "relative_type": "none",
                "source_text": str(extracted_features.get("time_range", "") or ""),
            }

        granularity = str(parsed.get("granularity", "day") or "day").lower()
        if granularity not in {"day", "month", "quarter", "year"}:
            granularity = "day"

        relative_type = str(parsed.get("relative_type", "none") or "none").lower()
        if relative_type not in {
            "explicit_date_range",
            "relative_days",
            "relative_months",
            "relative_years",
            "calendar_period",
            "none",
        }:
            relative_type = "none"

        return {
            "has_time_filter": bool(parsed.get("has_time_filter", False)),
            "time_dimension": str(parsed.get("time_dimension", "") or ""),
            "start_date": str(parsed.get("start_date", "") or ""),
            "end_date": str(parsed.get("end_date", "") or ""),
            "granularity": granularity,
            "relative_type": relative_type,
            "source_text": str(parsed.get("source_text", extracted_features.get("time_range", "")) or ""),
        }
