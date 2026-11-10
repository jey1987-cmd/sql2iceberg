## sql2iceberg 0.2.2

```bash
pip install --upgrade "sql2iceberg>=0.2.2"
```

### Fixed (non-Snowflake sources)
- Ordered string aggregation (Postgres `STRING_AGG ... ORDER BY`, MySQL `GROUP_CONCAT ... ORDER BY`, Oracle `LISTAGG ... WITHIN GROUP`) and
  `PERCENTILE_CONT ... WITHIN GROUP` are now rewritten for every source dialect. In 0.2.0 and 0.2.1 only Snowflake got these rewrites.
- SQL Server `DATEDIFF` counts calendar boundaries like SQL Server does.
- Oracle `ROWNUM` is flagged unsupported.

Postgres, MySQL, SQL Server and Oracle are still experimental: tested with unit tests, not against live servers.

**Full changelog:** https://github.com/jey1987-cmd/sql2iceberg/blob/main/CHANGELOG.md
