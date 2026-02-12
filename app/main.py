from __future__ import annotations

from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text

from app.agent import SQLAgentService
from app.charting import build_chart_spec, summarize_insight
from app.config import ConfigError, load_config
from app.db_init import ensure_database_initialized
from app.schemas import AskRequest, AskResponse

app = FastAPI(title="SmartBI SQL Agent API")

_state: dict[str, object] = {}


@app.on_event("startup")
def startup() -> None:
    try:
        config = load_config()
    except ConfigError as exc:
        raise RuntimeError(f"Configuration error: {exc}") from exc

    engine = create_engine(config.database_url, pool_pre_ping=True)
    ensure_database_initialized(engine, seed_sql_path=config.seed_sql_path)

    _state["config"] = config
    _state["engine"] = engine
    _state["agent"] = SQLAgentService(config=config, engine=engine)


@app.post("/ask", response_model=AskResponse)
def ask(req: AskRequest) -> AskResponse:
    engine = _state.get("engine")
    agent = _state.get("agent")
    config = _state.get("config")

    if not engine or not agent or not config:
        raise HTTPException(status_code=503, detail="Service is not ready yet")

    sql_result = agent.generate_sql(req.question, max_rows=config.sql_max_rows)

    with engine.connect() as conn:
        rows = conn.execute(text(sql_result.sql)).mappings().all()

    result_rows = [dict(row) for row in rows]
    columns = list(result_rows[0].keys()) if result_rows else []
    chart_spec = build_chart_spec(result_rows, columns, req.chart_type)
    insight = summarize_insight(result_rows, columns)

    return AskResponse(
        question=req.question,
        sql=sql_result.sql,
        columns=columns,
        rows=result_rows,
        chart_spec=chart_spec,
        insight=insight,
        retries=sql_result.retries,
    )
