"""Publishers expose existing Iceberg tables to another platform (Snowflake, AWS Glue/Athena, BigQuery).

Every publisher first builds a plan (a list of Actions: SQL or an API call described as text) and only
changes anything in the target account when ``apply()`` is called. ``plan()`` never connects anywhere.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from ..tracing import span

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def ident(value: str, what: str = "identifier") -> str:
    """Validate a SQL object name. Names are interpolated into DDL, so reject anything unusual."""
    if not _IDENT.match(value or ""):
        raise ValueError(f"unsafe or empty {what}: {value!r} (letters, digits, _ and $ only)")
    return value


def lit(value: str) -> str:
    """Quote a SQL string literal."""
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


@dataclass
class IcebergTable:
    name: str                       # table name (no catalog/namespace)
    namespace: str = "db"
    metadata_location: str = ""     # full URI of the current metadata.json, e.g. s3://bucket/db/t/metadata/v3.metadata.json
    format_version: int = 2


@dataclass
class Action:
    target: str                     # snowflake | aws | gcp
    kind: str                       # sql | api
    description: str
    text: str                       # SQL text, or a description of the API call
    costs_money: bool = False       # warehouse / query usage possible
    done: bool = False
    error: str = ""
    output: str = ""                # rows returned by a check, as text


@dataclass
class PublishResult:
    actions: list[Action] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(a.done and not a.error for a in self.actions)

    def to_markdown(self) -> str:
        out = [f"# Publish plan ({len(self.actions)} actions)\n"]
        out += [f"- WARNING: {w}" for w in self.warnings]
        for i, a in enumerate(self.actions, 1):
            state = "applied" if a.done else ("FAILED: " + a.error if a.error else "planned")
            out.append(f"\n## {i}. {a.description} [{a.target}, {state}]")
            out.append(f"```{'sql' if a.kind == 'sql' else 'text'}\n{a.text}\n```")
            if a.output:
                out.append(f"Result:\n```\n{a.output}\n```")
        return "\n".join(out) + "\n"


class Publisher(ABC):
    target = ""

    def __init__(self) -> None:
        self.warnings: list[str] = []

    @abstractmethod
    def plan(self, tables: list[IcebergTable]) -> PublishResult: ...

    @abstractmethod
    def _run(self, action: Action) -> str | None:
        """Execute one action against the real account. Raise on failure; return any rows as text."""

    def apply(self, tables: list[IcebergTable], *, confirm: bool = False) -> PublishResult:
        """Run the plan. ``confirm=True`` is required so an accidental call cannot change an account."""
        if not confirm:
            raise PermissionError("apply() changes a real account: call it with confirm=True after reading plan()")
        result = self.plan(tables)
        for action in result.actions:
            try:
                with span("publish.action", target=action.target, step=action.description):
                    action.output = self._run(action) or ""
                action.done = True
            except Exception as exc:  # noqa: BLE001  report and stop: later steps depend on earlier ones
                action.error = f"{type(exc).__name__}: {exc}"[:1500]
                break
        return result


def publisher_from_config(cfg: dict) -> tuple[Publisher, list[IcebergTable]]:
    """Build a publisher and its tables from a plain dict (the JSON file the CLI reads).

    {"target": "snowflake" | "aws" | "gcp", "<target>": {...target fields...},
     "tables": [{"name": ..., "namespace": ..., "metadata_location": ..., "format_version": 2}]}
    """
    from .aws import GluePublisher, GlueTarget
    from .gcp import BigQueryPublisher, BigQueryTarget
    from .snowflake import SnowflakePublisher, SnowflakeTarget

    kind = cfg.get("target")
    tables = [IcebergTable(**t) for t in cfg.get("tables", [])]
    if not tables:
        raise ValueError("config has no tables")
    if kind == "snowflake":
        return SnowflakePublisher(SnowflakeTarget(**cfg["snowflake"])), tables
    if kind == "aws":
        return GluePublisher(GlueTarget(**cfg["aws"])), tables
    if kind == "gcp":
        return BigQueryPublisher(BigQueryTarget(**cfg["gcp"])), tables
    raise ValueError("target must be snowflake, aws or gcp")
