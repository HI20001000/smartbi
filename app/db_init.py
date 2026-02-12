from __future__ import annotations

from pathlib import Path

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine


def _split_sql_statements(raw_sql: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    in_single = False
    in_double = False

    for char in raw_sql:
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double

        if char == ";" and not in_single and not in_double:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            continue

        current.append(char)

    trailing = "".join(current).strip()
    if trailing:
        statements.append(trailing)
    return statements


def ensure_database_initialized(engine: Engine, seed_sql_path: str) -> dict[str, str | int]:
    inspector = inspect(engine)
    if inspector.get_table_names():
        return {"status": "already_initialized", "executed_statements": 0}

    path = Path(seed_sql_path)
    if not path.exists():
        raise FileNotFoundError(f"Seed SQL file not found: {seed_sql_path}")

    seed_sql = path.read_text(encoding="utf-8")
    statements = _split_sql_statements(seed_sql)

    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))

    return {"status": "seeded", "executed_statements": len(statements)}
