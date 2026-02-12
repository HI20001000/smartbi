from __future__ import annotations

import re

_ALLOWED_PREFIX = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)
_BLOCK_PATTERNS = [
    re.compile(r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|REPLACE|GRANT|REVOKE)\b", re.IGNORECASE),
    re.compile(r"--"),
    re.compile(r"/\*"),
    re.compile(r";\s*\S", re.IGNORECASE),
    re.compile(r"\bINTO\s+OUTFILE\b", re.IGNORECASE),
]


def validate_sql(sql: str) -> tuple[bool, str | None]:
    if not sql or not sql.strip():
        return False, "SQL is empty"
    if not _ALLOWED_PREFIX.search(sql):
        return False, "Only SELECT / WITH queries are allowed"
    for pattern in _BLOCK_PATTERNS:
        if pattern.search(sql):
            return False, f"Blocked by SQL firewall pattern: {pattern.pattern}"
    return True, None


def ensure_limit(sql: str, max_rows: int) -> str:
    cleaned = sql.strip().rstrip(";")
    if re.search(r"\bLIMIT\b", cleaned, flags=re.IGNORECASE):
        return cleaned
    return f"{cleaned}\nLIMIT {max_rows}"
