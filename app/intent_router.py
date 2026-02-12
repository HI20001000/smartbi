import json
from dataclasses import dataclass
from enum import Enum

from app.llm_service import LLMChatSession


class IntentType(str, Enum):
    EXIT = "EXIT"
    SQL = "SQL"
    CHAT = "CHAT"


@dataclass(frozen=True)
class IntentResult:
    intent: IntentType
    confidence: float
    reason: str


def _rule_based_intent(user_input: str) -> IntentResult | None:
    text = user_input.strip().lower()
    if not text:
        return None

    exit_keywords = {
        "/exit",
        "exit",
        "quit",
        "bye",
        "退出",
        "結束",
        "离开",
        "離開",
    }
    if text in exit_keywords:
        return IntentResult(
            intent=IntentType.EXIT,
            confidence=1.0,
            reason="Matched local exit keyword.",
        )

    return None


def classify_intent(user_input: str, session: LLMChatSession) -> IntentResult:
    local_result = _rule_based_intent(user_input)
    if local_result:
        return local_result

    raw = session.classify_intent_with_llm(user_input)
    try:
        parsed = json.loads(raw)
        intent = IntentType(parsed.get("intent", "CHAT").upper())
        confidence = float(parsed.get("confidence", 0.5))
        reason = str(parsed.get("reason", "LLM classified intent."))
        return IntentResult(intent=intent, confidence=confidence, reason=reason)
    except (json.JSONDecodeError, ValueError, TypeError, KeyError):
        return IntentResult(
            intent=IntentType.CHAT,
            confidence=0.3,
            reason="Failed to parse LLM intent output; fallback to CHAT.",
        )
