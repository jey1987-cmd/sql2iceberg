"""GCP: BigLake external Iceberg tables in BigQuery.

BigQuery reads Iceberg data from Cloud Storage, so the warehouse must be on ``gs://``. Each table is created
from its current metadata.json (a snapshot: re-run after writes). Needs a BigQuery connection resource with
read access to the bucket, and ``pip install sql2iceberg[gcp]`` to apply. Authentication is Application Default
Credentials (``gcloud auth application-default login`` or a service account), never stored here.
"""

from __future__ import annotations

from dataclasses import dataclass

from .base import Action, IcebergTable, Publisher, PublishResult, ident, lit


@dataclass
class BigQueryTarget:
    project: str
    dataset: str
    connection: str                  # "<project>.<location>.<connection_id>"
    location: str = "US"

    def __post_init__(self) -> None:
        for v, what in ((self.project.replace("-", "_"), "project"), (self.dataset, "dataset")):
            ident(v, what)
        if len(self.connection.split(".")) != 3:
            raise ValueError("connection must look like project.location.connection_id")


class BigQueryPublisher(Publisher):
    target = "gcp"

    def __init__(self, target: BigQueryTarget, client=None) -> None:
        super().__init__()
        self.t, self._client = target, client

    def plan(self, tables: list[IcebergTable]) -> PublishResult:
        t = self.t
        res = PublishResult()
        res.warnings.append("BigLake Iceberg tables are snapshots of one metadata file: re-run after each write.")
        if any(tb.format_version >= 3 for tb in tables):
            res.warnings.append("Some tables are Iceberg format-version 3; check BigQuery's supported versions "
                                "and write with --iceberg-version 2 if unsure.")
        res.actions.append(Action("gcp", "sql", "Create dataset",
                                  f"CREATE SCHEMA IF NOT EXISTS `{t.project}.{t.dataset}` OPTIONS (location = {lit(t.location)});"))
        for tb in tables:
            if not tb.metadata_location.startswith("gs://"):
                raise ValueError(f"{tb.name}: BigQuery needs a gs:// metadata_location, got {tb.metadata_location!r}")
            res.actions.append(Action(
                "gcp", "sql", f"Expose {tb.name}",
                f"CREATE OR REPLACE EXTERNAL TABLE `{t.project}.{t.dataset}.{ident(tb.name, 'table name')}`\n"
                f"  WITH CONNECTION `{t.connection}`\n"
                f"  OPTIONS (format = 'ICEBERG', uris = [{lit(tb.metadata_location)}]);"))
        res.actions.append(Action(
            "gcp", "sql", "Count rows (billed per bytes processed)",
            "\n".join(f"SELECT '{tb.name}' AS table_name, COUNT(*) AS row_count "
                      f"FROM `{t.project}.{t.dataset}.{tb.name}`;" for tb in tables), costs_money=True))
        return res

    def _run(self, action: Action) -> str | None:
        if self._client is None:
            try:
                from google.cloud import bigquery
            except ImportError as exc:
                raise ImportError("pip install 'sql2iceberg[gcp]'") from exc
            self._client = bigquery.Client(project=self.t.project, location=self.t.location)
        lines = []
        for stmt in [s for s in action.text.split(";\n") if s.strip()]:
            rows = self._client.query(stmt.rstrip(";")).result()
            if stmt.lstrip().upper().startswith("SELECT"):
                lines.extend(", ".join(str(v) for v in tuple(r)) for r in rows)
        return "\n".join(lines) or None
