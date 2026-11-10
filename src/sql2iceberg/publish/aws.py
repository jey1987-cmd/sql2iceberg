"""AWS: make Iceberg tables visible in the Glue Data Catalog (so Athena, EMR, Redshift Spectrum can query them).

If the tables were written through a Glue catalog (``catalog_type="glue"``) they are already registered and
``plan()`` only adds a verification step. If they live in a Hadoop catalog on S3, they are registered in Glue
with Iceberg's ``register_table`` procedure, run in a Spark session that has a Glue catalog configured.
Needs ``pip install sql2iceberg[aws]`` (boto3) for verification. Credentials come from the usual AWS
profile / environment, never from this library's config.
"""

from __future__ import annotations

from dataclasses import dataclass

from .base import Action, IcebergTable, Publisher, PublishResult, ident, lit


@dataclass
class GlueTarget:
    database: str                      # Glue database (Iceberg namespace)
    region: str = ""
    already_in_glue: bool = False      # tables were written through a Glue catalog
    spark_glue_catalog: str = "glue"   # name of the Spark catalog configured with GlueCatalog
    athena_workgroup: str = "primary"
    athena_output: str = ""            # s3://bucket/prefix/ for Athena results (needed to run the count check)

    def __post_init__(self) -> None:
        ident(self.database, "glue database")
        ident(self.spark_glue_catalog, "spark catalog name")


class GluePublisher(Publisher):
    target = "aws"

    def __init__(self, target: GlueTarget, spark=None, session=None) -> None:
        super().__init__()
        self.t, self._spark, self._session = target, spark, session

    def plan(self, tables: list[IcebergTable]) -> PublishResult:
        t = self.t
        res = PublishResult()
        add = res.actions.append
        if not t.already_in_glue:
            add(Action("aws", "sql", "Create Glue database via Spark",
                       f"CREATE NAMESPACE IF NOT EXISTS {t.spark_glue_catalog}.{t.database};"))
            for tb in tables:
                if not tb.metadata_location.startswith("s3"):
                    raise ValueError(f"{tb.name}: metadata_location must be an s3:// URI to register in Glue")
                add(Action("aws", "sql", f"Register {tb.name} in Glue",
                           f"CALL {t.spark_glue_catalog}.system.register_table(table => "
                           f"{lit(t.database + '.' + ident(tb.name, 'table name'))}, "
                           f"metadata_file => {lit(tb.metadata_location)});"))
        add(Action("aws", "api", "Verify tables in Glue",
                   "glue.get_table for: " + ", ".join(f"{t.database}.{tb.name}" for tb in tables)
                   + " (expects Parameters.table_type == ICEBERG)"))
        if t.athena_output:
            add(Action("aws", "sql", "Count rows with Athena (billed per data scanned)",
                       "\n".join(f'SELECT \'{tb.name}\' AS table_name, COUNT(*) FROM "{t.database}"."{tb.name}";'
                                 for tb in tables), costs_money=True))
        return res

    def _boto(self, service: str):
        if self._session is None:
            try:
                import boto3
            except ImportError as exc:
                raise ImportError("pip install 'sql2iceberg[aws]'") from exc
            self._session = boto3.Session(region_name=self.t.region or None)
        return self._session.client(service)

    def _run(self, action: Action) -> str | None:
        if action.kind == "sql" and action.costs_money:
            return self._athena(action.text)
        elif action.kind == "sql":
            if self._spark is None:
                raise RuntimeError("pass spark=<SparkSession with a Glue catalog> to register tables")
            for stmt in [s for s in action.text.split(";\n") if s.strip()]:
                self._spark.sql(stmt.rstrip(";"))
        else:
            self._verify_glue(action.text)

    def _verify_glue(self, text: str) -> None:
        glue = self._boto("glue")
        names = text.split("for: ")[1].split(" (")[0].split(", ")
        for full in names:
            db, name = full.split(".")
            tbl = glue.get_table(DatabaseName=db, Name=name)["Table"]
            if tbl.get("Parameters", {}).get("table_type", "").upper() != "ICEBERG":
                raise RuntimeError(f"{full} exists in Glue but is not an Iceberg table")

    def _athena(self, sql_text: str) -> str:
        import time

        athena = self._boto("athena")
        lines: list[str] = []
        for stmt in [s for s in sql_text.split(";\n") if s.strip()]:
            qid = athena.start_query_execution(
                QueryString=stmt.rstrip(";"), WorkGroup=self.t.athena_workgroup,
                ResultConfiguration={"OutputLocation": self.t.athena_output})["QueryExecutionId"]
            while True:
                state = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
                if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                    break
                time.sleep(1)
            if state != "SUCCEEDED":
                raise RuntimeError(f"Athena query {state}: {stmt[:80]}")
            rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"][1:]
            lines.extend(", ".join(c.get("VarCharValue", "") for c in r["Data"]) for r in rows)
        return "\n".join(lines)
