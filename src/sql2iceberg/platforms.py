"""Work out which SQL platform a code base targets, from imports, connection strings and env vars."""

from __future__ import annotations

import os
import re
from collections import Counter
from pathlib import Path

# platform -> (strong signals: drivers/URLs, weight 3; weak signals: SQL syntax, weight 1)
_STRONG: dict[str, list[str]] = {
    "snowflake": [r"snowflake\.connector", r"snowflake\.snowpark", r"snowflakecomputing\.com"],
    "postgres": [r"psycopg2?", r"asyncpg", r"postgres(ql)?://"],
    "mysql": [r"pymysql", r"mysql\.connector", r"mysqlclient", r"mysql://"],
    "mssql": [r"pyodbc", r"pymssql", r"mssql\+"],
    "oracle": [r"oracledb", r"cx_Oracle"],
}
_WEAK: dict[str, list[str]] = {
    "snowflake": [r"\bQUALIFY\b", r"\bIFF\s*\(", r"\bLATERAL\s+FLATTEN\b", r"\bVARIANT\b"],
    "postgres": [r"\bRETURNING\b", r"::\s*jsonb"],
    "mysql": [r"\bAUTO_INCREMENT\b"],
    "mssql": [r"\bNOLOCK\b", r"\bTOP\s+\d+\b", r"\bGETDATE\s*\("],
    "oracle": [r"\bROWNUM\b", r"\bSYSDATE\b", r"\bNVL\s*\(", r"\bFROM\s+DUAL\b"],
}
_ENV_SIGNALS = {
    "snowflake": "SNOWFLAKE_ACCOUNT", "postgres": "PGHOST", "mysql": "MYSQL_HOST",
    "mssql": "MSSQL_SERVER", "oracle": "ORACLE_DSN",
}
_SUFFIXES = {".py", ".sql", ".ipynb", ".yml", ".yaml", ".toml", ".cfg", ".ini", ".json"}
_SKIP_DIRS = {".venv", ".git", "node_modules", "__pycache__"}


def detect_platform(path: str | Path, env: dict[str, str] | None = None) -> dict:
    """Return ``{"platform": name | None, "scores": {...}, "evidence": [...]}``.

    Driver imports and connection strings count 3, SQL-syntax hints 1, a set env var 2.
    A tie or no signal gives ``platform=None``: the caller must then ask the user.
    """
    env = os.environ if env is None else env
    p = Path(path)
    files = [f for f in p.rglob("*") if f.is_file()] if p.is_dir() else [p]
    scores: Counter = Counter()
    evidence: list[str] = []
    for f in files:
        if f.suffix.lower() not in _SUFFIXES or _SKIP_DIRS & set(f.parts):
            continue
        try:
            text = f.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for table, weight in ((_STRONG, 3), (_WEAK, 1)):
            for platform, patterns in table.items():
                for pat in patterns:
                    if re.search(pat, text, re.IGNORECASE):
                        scores[platform] += weight
                        evidence.append(f"{f.name}: {platform} ~ {pat}")
    for platform, var in _ENV_SIGNALS.items():
        if env.get(var):
            scores[platform] += 2
            evidence.append(f"env {var} is set")
    ranked = scores.most_common(2)
    best = ranked[0][0] if ranked and (len(ranked) == 1 or ranked[0][1] > ranked[1][1]) else None
    return {"platform": best, "scores": dict(scores), "evidence": evidence[:20]}
