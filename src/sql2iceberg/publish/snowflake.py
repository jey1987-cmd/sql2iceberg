"""Snowflake: query Iceberg tables you manage elsewhere, as externally managed Iceberg tables.

Two ways to tell Snowflake where the table is:
  * ``glue``          catalog integration to AWS Glue; Snowflake follows the Glue table (CATALOG_TABLE_NAME)
  * ``object_store``  catalog integration on the storage itself; each table points at a metadata.json file

SQL follows the Snowflake documentation for CREATE EXTERNAL VOLUME, CREATE CATALOG INTEGRATION and
CREATE ICEBERG TABLE (externally managed). The table DDL matches the shape of a table already created on a live
account by an earlier session (object_store mode); Glue mode and the volume/integration DDL are unverified
until the live test in tests/test_live.py has been run.
Creating these objects needs a role with the matching privileges, and queries use warehouse credits.
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse

from .base import Action, IcebergTable, Publisher, PublishResult, ident, lit

_PROVIDERS = {"s3": "S3", "gs": "GCS", "gcs": "GCS", "azure": "AZURE", "abfss": "AZURE"}


@dataclass
class SnowflakeTarget:
    database: str
    schema: str
    base_url: str                      # storage root that contains the warehouse, e.g. s3://bucket/warehouse/
    mode: str = "object_store"         # object_store | glue
    external_volume: str = "SF2ICE_VOL"
    catalog_integration: str = "SF2ICE_CATALOG"
    aws_role_arn: str = ""             # role Snowflake assumes to read the bucket (S3) and Glue (glue mode)
    glue_catalog_id: str = ""          # AWS account id (glue mode)
    glue_region: str = ""
    glue_namespace: str = "db"
    azure_tenant_id: str = ""
    auto_refresh: bool = False         # glue mode only
    create_volume: bool = True         # False: reuse an existing external volume with this name
    create_integration: bool = True    # False: reuse an existing catalog integration with this name
    create_schema: bool = True

    def __post_init__(self) -> None:
        for field_name in ("database", "schema", "external_volume", "catalog_integration"):
            ident(getattr(self, field_name), field_name)
        if self.mode not in ("object_store", "glue"):
            raise ValueError("mode must be object_store or glue")
        scheme = urlparse(self.base_url).scheme
        if scheme not in _PROVIDERS:
            raise ValueError(f"base_url must start with one of {sorted(_PROVIDERS)}://")
        if scheme == "s3" and self.create_volume and not self.aws_role_arn:
            raise ValueError("aws_role_arn is required for an S3 external volume")
        if self.mode == "glue" and self.create_integration and not (
                self.glue_catalog_id and self.glue_region and self.aws_role_arn):
            raise ValueError("glue mode needs glue_catalog_id, glue_region and aws_role_arn")


class SnowflakePublisher(Publisher):
    target = "snowflake"

    def __init__(self, target: SnowflakeTarget, connector=None) -> None:
        super().__init__()
        self.t = target
        self._connector = connector

    def _volume_sql(self) -> str:
        t = self.t
        scheme = urlparse(t.base_url).scheme
        provider = _PROVIDERS[scheme]
        base = self._base()
        fields = ["NAME = 'loc1'", f"STORAGE_PROVIDER = '{provider}'", f"STORAGE_BASE_URL = {lit(base)}"]
        if provider == "S3":
            fields.append(f"STORAGE_AWS_ROLE_ARN = {lit(t.aws_role_arn)}")
        if provider == "AZURE":
            fields.append(f"AZURE_TENANT_ID = {lit(t.azure_tenant_id)}")
        return (f"CREATE EXTERNAL VOLUME IF NOT EXISTS {t.external_volume}\n  STORAGE_LOCATIONS = (( "
                + ", ".join(fields) + " ));")

    def _integration_sql(self) -> str:
        t = self.t
        if t.mode == "glue":
            return (f"CREATE CATALOG INTEGRATION IF NOT EXISTS {t.catalog_integration}\n"
                    f"  CATALOG_SOURCE = GLUE\n  CATALOG_NAMESPACE = {lit(t.glue_namespace)}\n  TABLE_FORMAT = ICEBERG\n"
                    f"  GLUE_AWS_ROLE_ARN = {lit(t.aws_role_arn)}\n  GLUE_CATALOG_ID = {lit(t.glue_catalog_id)}\n"
                    f"  GLUE_REGION = {lit(t.glue_region)}\n  ENABLED = TRUE;")
        return (f"CREATE CATALOG INTEGRATION IF NOT EXISTS {t.catalog_integration}\n"
                f"  CATALOG_SOURCE = OBJECT_STORE\n  TABLE_FORMAT = ICEBERG\n  ENABLED = TRUE;")

    def _base(self) -> str:
        base = self.t.base_url if self.t.base_url.endswith("/") else self.t.base_url + "/"
        return base.replace("gs://", "gcs://", 1)

    def _relative_path(self, uri: str) -> str:
        """Path of a metadata file relative to the external volume's base URL."""
        uri, base = uri.replace("gs://", "gcs://", 1), self._base()
        if not uri.startswith(base):
            raise ValueError(f"{uri} is not under the external volume base {base}")
        return uri[len(base):]

    def _table_sql(self, tb: IcebergTable) -> str:
        t = self.t
        head = f"CREATE ICEBERG TABLE IF NOT EXISTS {t.database}.{t.schema}.{ident(tb.name, 'table name')}\n"
        head += f"  EXTERNAL_VOLUME = {lit(t.external_volume)}\n  CATALOG = {lit(t.catalog_integration)}\n"
        if t.mode == "glue":
            return head + f"  CATALOG_TABLE_NAME = {lit(tb.name)}" + (
                "\n  AUTO_REFRESH = TRUE" if t.auto_refresh else "") + ";"
        if not tb.metadata_location:
            raise ValueError(f"{tb.name}: metadata_location is required in object_store mode")
        return head + f"  METADATA_FILE_PATH = {lit(self._relative_path(tb.metadata_location))};"

    def plan(self, tables: list[IcebergTable]) -> PublishResult:
        t = self.t
        res = PublishResult()
        if any(tb.format_version >= 3 for tb in tables):
            res.warnings.append("Some tables are Iceberg format-version 3. Check the current Snowflake docs for "
                                "v3 support in externally managed tables; use --iceberg-version 2 if unsure.")
        if t.mode == "object_store":
            res.warnings.append("object_store mode points at a fixed metadata file. Re-run after each write "
                                "(ALTER ICEBERG TABLE ... REFRESH 'new/metadata.json') or use glue mode.")
        add = res.actions.append
        if t.create_volume:
            add(Action("snowflake", "sql", "Create external volume", self._volume_sql()))
        if t.create_integration:
            add(Action("snowflake", "sql", "Create catalog integration", self._integration_sql()))
        if t.create_schema:
            add(Action("snowflake", "sql", "Create schema", f"CREATE SCHEMA IF NOT EXISTS {t.database}.{t.schema};"))
        for tb in tables:
            add(Action("snowflake", "sql", f"Expose {tb.name}", self._table_sql(tb)))
        add(Action("snowflake", "sql", "Count rows (uses a warehouse)",
                   "\n".join(f"SELECT '{tb.name}' AS table_name, COUNT(*) AS row_count "
                             f"FROM {t.database}.{t.schema}.{tb.name};" for tb in tables), costs_money=True))
        return res

    def _run(self, action: Action) -> str | None:
        if self._connector is None:
            from ..connectors import get_connector

            self._connector = get_connector("snowflake")
        lines = []
        for stmt in [x for x in action.text.split(";\n") if x.strip()]:
            rows = self._connector.query(stmt.rstrip(";"))
            if stmt.lstrip().upper().startswith("SELECT"):
                lines.extend(", ".join(str(v) for v in r) for r in rows)
        return "\n".join(lines) or None
