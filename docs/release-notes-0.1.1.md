## sql2iceberg 0.1.1

Documentation-only release. It improves the PyPI project page; there are **no code changes** since 0.1.0.

```bash
pip install --upgrade sql2iceberg
```

### What's new
- **Logo and badges** (PyPI version, Python versions, CI, license) at the top of the README.
- **Table of contents** in the README.
- **Fixed links:** README links to the docs, changelog, contributing guide and license were relative and did not resolve on the PyPI page. They are now absolute GitHub URLs.

### Still true from 0.1.0
Alpha. Verified live on Snowflake, AWS (Glue/Athena) and BigQuery with Iceberg format-version 2 (38 validation checks matched).
Postgres, MySQL, SQL Server and Oracle connectors are experimental. See the
[environments](https://github.com/jey1987-cmd/sql2iceberg/blob/main/docs/environments.md) and
[troubleshooting](https://github.com/jey1987-cmd/sql2iceberg/blob/main/docs/troubleshooting.md) pages.

**Full changelog:** https://github.com/jey1987-cmd/sql2iceberg/blob/main/CHANGELOG.md
