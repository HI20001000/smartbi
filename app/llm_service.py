import json
from decimal import Decimal

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

    def select_semantic_plan_with_llm(
        self,
        user_input: str,
        token_hits: dict,
    ) -> dict:
        matches = token_hits.get("matches", []) or []
        candidate_metrics = [
            m.get("canonical_name", "")
            for m in matches
            if m.get("object_type") == "metric" and m.get("allowed") is not False and m.get("canonical_name")
        ]
        candidate_dimensions = [
            m.get("canonical_name", "")
            for m in matches
            if m.get("object_type") == "dimension" and m.get("allowed") is not False and m.get("canonical_name")
        ]
        candidate_datasets = [
            m.get("dataset", "")
            for m in matches
            if isinstance(m.get("dataset"), str) and m.get("dataset")
        ]

        prompt = [
            SystemMessage(
                content=(
                    "你是 SmartBI 查詢規劃器。"
                    "任務：根據使用者需求，從候選清單中挑選指標/維度/資料集。"
                    "你不能輸出 SQL，只能輸出 JSON。"
                    "輸出格式固定為："
                    '{"selected_metrics":[],"selected_dimensions":[],"selected_filters":[],"selected_dataset_candidates":[]}'
                    "規則："
                    "1) selected_metrics / selected_dimensions 的值，必須來自候選清單；"
                    "2) selected_dataset_candidates 優先保留一個最適合資料集（可留空）；"
                    "3) selected_filters 可留空；若提供，僅使用 field/op/value 或 expr 結構；"
                    "4) 若無法判斷，輸出空陣列；"
                    "5) 只能輸出 JSON，不得輸出任何說明文字。"
                )
            ),
            HumanMessage(
                content=(
                    f"user_input={user_input}\n"
                    f"candidate_metrics={json.dumps(candidate_metrics, ensure_ascii=False)}\n"
                    f"candidate_dimensions={json.dumps(candidate_dimensions, ensure_ascii=False)}\n"
                    f"candidate_datasets={json.dumps(candidate_datasets, ensure_ascii=False)}"
                )
            ),
        ]
        try:
            resp = self.client.invoke(prompt)
            raw = getattr(resp, "content", str(resp)).strip()
            parsed = json.loads(raw)
        except Exception:
            parsed = {}

        def _string_list(value: object) -> list[str]:
            if not isinstance(value, list):
                return []
            out: list[str] = []
            seen: set[str] = set()
            for v in value:
                if not isinstance(v, str):
                    continue
                normalized = v.strip()
                if not normalized or normalized in seen:
                    continue
                seen.add(normalized)
                out.append(normalized)
            return out

        def _filter_list(value: object) -> list[dict]:
            if not isinstance(value, list):
                return []
            out: list[dict] = []
            for item in value:
                if not isinstance(item, dict):
                    continue
                normalized: dict[str, object] = {}
                for key in ("field", "op", "value", "expr", "source"):
                    if key in item:
                        normalized[key] = item.get(key)
                if normalized:
                    out.append(normalized)
            return out

        return {
            "selected_metrics": _string_list(parsed.get("selected_metrics")),
            "selected_dimensions": _string_list(parsed.get("selected_dimensions")),
            "selected_filters": _filter_list(parsed.get("selected_filters")),
            "selected_dataset_candidates": _string_list(parsed.get("selected_dataset_candidates")),
        }

    def extract_sql_features_with_llm(self, user_input: str) -> dict:
        prompt = [
            SystemMessage(
                content=(
                    "你是 SmartBI 查詢解析器（SQL/BI Query Feature Extractor）。"
                    "任務：從使用者輸入（中文/英文/混合）提取查詢特徵，並且【只能輸出 JSON】。"
                    "JSON 格式固定為："
                    "{\"tokens\":[],\"metrics\":[],\"dimensions\":[],\"filters\":[],\"time_start\":\"\",\"time_end\":\"\"}"
                    "\n\n"
                    "【輸出規則（嚴格遵守）】\n"
                    "1) 只能輸出以上 6 個欄位，不得新增欄位；不得輸出任何 JSON 以外文字。\n"
                    "2) tokens/metrics/dimensions/filters 必須是【字串陣列】；time_start/time_end 必須是字串。\n"
                    "3) time_start/time_end 格式必須為 yyyy-mm-dd；若無法判定則輸出空字串 \"\"。\n"
                    "4) 不要臆測：使用者沒提到的內容不要填；不確定就留空。\n"
                    "5) 去重：陣列內不得重複字串；保持由重要到次要的順序。\n"
                    "6) 若成功解析為具體日期（time_start/time_end 非空），時間詞不要再放入 tokens 或 filters。\n"
                    "\n"
                    "【欄位語義（SmartBI 導向）】\n"
                    "- metrics：可聚合的指標/度量（例如：銷售額、訂單數、GMV、利潤、DAU、轉化率、平均客單價、同比、環比）。\n"
                    "- dimensions：分組/切片維度（例如：日期、月份、地區、省、市、門店、渠道、品類、商品、用戶類型）。\n"
                    "- filters：限制條件，使用【可讀的條件片段】字串，不要求嚴格語法，但要清楚（例如：\"地區=華東\"、\"渠道 in(線上)\"、\"狀態=已支付\"、\"客單價>200\"）。\n"
                    "- tokens：其他關鍵詞（實體、別名、業務名詞、口語詞、主題詞），以及你無法判定是 metrics/dimensions/filters 的重要詞。\n"
                    "\n"
                    "【時間解析規則（優先級最高，必須執行）】\n"
                    "一、明確日期範圍：\n"
                    "- 例如 \"2024-01-01 到 2024-01-31\" => time_start=\"2024-01-01\", time_end=\"2024-01-31\"。\n"
                    "\n"
                    "二、年份：\n"
                    "- \"2024年\" => time_start=\"2024-01-01\", time_end=\"2024-12-31\"。\n"
                    "\n"
                    "三、月份：\n"
                    "- \"2024年1月\" 或 \"2024-01\" => time_start=\"2024-01-01\"，time_end=該月最後一天（需判斷閏年）。\n"
                    "\n"
                    "四、季度：\n"
                    "- \"2024年Q1\" 或 \"2024Q1\" => time_start=\"2024-01-01\", time_end=\"2024-03-31\"。\n"
                    "- Q2/Q3/Q4 依序為 04-01~06-30、07-01~09-30、10-01~12-31。\n"
                    "\n"
                    "五、相對時間（必須基於系統當前日期與系統時區計算，需輸出具體 yyyy-mm-dd）\n"
                    "- 今天 => time_start=today, time_end=today。\n"
                    "- 昨天 => time_start=today-1day, time_end=today-1day。\n"
                    "- 近7天/最近7天 => time_start=today-6days, time_end=today（包含今天共7天）。\n"
                    "- 近N天/最近N天 => time_start=today-(N-1)days, time_end=today。\n"
                    "- 最近一個月 => time_start=today-1month+1day, time_end=today。\n"
                    "- 本月 => time_start=本月第一天, time_end=today。\n"
                    "- 上月 => time_start=上月第一天, time_end=上月最後一天。\n"
                    "- 今年 => time_start=今年1月1日, time_end=today。\n"
                    "- 去年 => time_start=去年1月1日, time_end=去年12月31日。\n"
                    "若同時給出基準日期（例如：以2024-03-10為基準的近7天），則以該日期為基準計算。\n"
                    "只有在完全無法判斷時間範圍時，time_start/time_end 才允許為空字串 \"\"。\n"
                    "\n"
                    "【分類優先級（避免亂放）】\n"
                    "1) 能明確聚合/指標 => metrics\n"
                    "2) 能明確分組/枚舉 => dimensions\n"
                    "3) 明確條件限制（=、>、<、包含、topN、區間、in、between、是否、狀態）=> filters\n"
                    "4) 其餘重要詞 => tokens\n"
                    "\n"
                    "【常見指標詞映射（看見就優先進 metrics）】\n"
                    "- \"多少\"/\"幾\" + 名詞（訂單/用戶/人數/次數）=> 對應計數型 metrics（例如：\"訂單數\"）\n"
                    "- \"平均\"/\"人均\"/\"每\" => 平均類 metrics（例如：\"平均客單價\"、\"人均消費\"）\n"
                    "- \"增長\"/\"同比\"/\"環比\" => 增長類 metrics（例如：\"同比增長率\"）\n"
                    "\n"
                    "輸出要求：最終只輸出 JSON 物件字串（不要 markdown，不要解釋）。"
                )
            ),
            HumanMessage(content=user_input),
        ]

        try:
            resp = self.client.invoke(prompt)
            raw = getattr(resp, "content", str(resp)).strip()
            parsed = json.loads(raw)
        except Exception:
            parsed = {}

        def _string_list(value: object) -> list[str]:
            if not isinstance(value, list):
                return []
            return [v.strip() for v in value if isinstance(v, str) and v.strip()]

        def _date_or_empty(value: object) -> str:
            if not isinstance(value, str):
                return ""
            value = value.strip()
            if len(value) == 10 and value[4] == "-" and value[7] == "-":
                return value
            return ""

        return {
            "tokens": _string_list(parsed.get("tokens")),
            "metrics": _string_list(parsed.get("metrics")),
            "dimensions": _string_list(parsed.get("dimensions")),
            "filters": _string_list(parsed.get("filters")),
            "time_start": _date_or_empty(parsed.get("time_start")),
            "time_end": _date_or_empty(parsed.get("time_end")),
        }

    def summarize_query_result_with_llm(self, user_input: str, rows: list[dict], max_rows: int = 20) -> str:
        sample_rows = rows[: max(1, int(max_rows))]

        def _json_fallback(value: object) -> object:
            if isinstance(value, Decimal):
                return float(value)
            return str(value)

        prompt = [
            SystemMessage(
                content=(
                    "你是 SmartBI 報表摘要助手。"
                    "請根據使用者問題與查詢結果，輸出 2~4 句繁體中文摘要。"
                    "要求：聚焦關鍵數據、趨勢與可行觀察，不要杜撰資料。"
                    "若資料筆數很少，請直接點出樣本有限。"
                )
            ),
            HumanMessage(
                content=(
                    f"user_input={user_input}\n"
                    f"rows_json={json.dumps(sample_rows, ensure_ascii=False, default=_json_fallback)}"
                )
            ),
        ]

        try:
            resp = self.client.invoke(prompt)
            return getattr(resp, "content", str(resp)).strip()
        except Exception as exc:
            return f"（摘要生成失敗：{exc}）"
