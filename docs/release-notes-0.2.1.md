## sql2iceberg 0.2.1

```bash
pip install --upgrade "sql2iceberg>=0.2.1"
```

### Fixed
- `pip install "sql2iceberg[spark]"` installed PySpark 4.3, which does not work with the Iceberg runtime jar (Spark 4.1) that the generated
  scripts use, so the scripts failed at start. The `spark`, `pytest` and `all` extras now require `pyspark>=3.5,<4.2`. If you already
  installed 0.2.0, run `pip install "pyspark>=4.0,<4.2"`.

See the [0.2.0 release notes](https://github.com/jey1987-cmd/sql2iceberg/blob/main/docs/release-notes-0.2.0.md) for everything new in 0.2.

**Full changelog:** https://github.com/jey1987-cmd/sql2iceberg/blob/main/CHANGELOG.md
