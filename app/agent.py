from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.engine import Engine

from app.config import AppConfig
from app.sql_firewall import ensure_limit, validate_sql


@dataclass(slots=True)
class SQLGenerationResult:
    sql: str
    retries: int


class SQLAgentService:
    def __init__(self, config: AppConfig, engine: Engine):
        self.config = config
        self.engine = engine
        self._agent_executor = self._create_agent_executor()

    def _create_agent_executor(self):
        try:
            from langchain_community.agent_toolkits import create_sql_agent
            from langchain_community.utilities import SQLDatabase
            from langchain_openai import ChatOpenAI
        except ImportError as exc:
            raise RuntimeError(
                "LangChain SQL agent dependencies are missing. "
                "Install: langchain, langchain-community, langchain-openai"
            ) from exc

        db = SQLDatabase(self.engine)
        llm = ChatOpenAI(model=self.config.openai_model, api_key=self.config.openai_api_key, temperature=0)
        return create_sql_agent(llm=llm, db=db, agent_type="openai-tools", verbose=False)

    def _extract_sql(self, output: str) -> str:
        text = output.strip()
        if "```sql" in text:
            text = text.split("```sql", 1)[1].split("```", 1)[0].strip()
        elif "```" in text:
            text = text.split("```", 1)[1].split("```", 1)[0].strip()
        return text

    def generate_sql(self, question: str, max_rows: int) -> SQLGenerationResult:
        prompt = (
            "Generate a single safe MySQL SELECT query for this question. "
            "Return SQL only, no explanation.\n"
            f"Question: {question}"
        )

        last_error = ""
        for attempt in range(self.config.agent_max_retries + 1):
            result = self._agent_executor.invoke({"input": prompt})
            output = result.get("output", "") if isinstance(result, dict) else str(result)
            sql = ensure_limit(self._extract_sql(output), max_rows=max_rows)

            ok, reason = validate_sql(sql)
            if ok:
                return SQLGenerationResult(sql=sql, retries=attempt)

            last_error = reason or "Unknown SQL validation error"
            prompt = (
                "The previous SQL was invalid due to firewall policy. "
                f"Reason: {last_error}. Generate another SQL query.\n"
                f"Question: {question}"
            )

        raise ValueError(f"Failed to generate safe SQL after retries: {last_error}")
