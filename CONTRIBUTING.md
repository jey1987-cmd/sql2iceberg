# Contributing

## Setup
```bash
pip install -e ".[pytest,dev]"
ruff check src tests
pytest -q -m "not spark"          # no cloud accounts needed
```
Spark tests: `SQL2ICE_ICEBERG=1 pytest -m spark` with the Iceberg jars in `./jars` (see docs/environments.md).

## Rules
- Never commit credentials, account IDs, bucket names, or real company SQL or data. Examples use made-up data.
- Untested platforms must stay labelled experimental in code and docs.
- Anything that changes a cloud account must be gated behind an explicit `--apply` / `confirm=True`.

## Reporting problems
Open an issue with `sql2iceberg issue --open` (pre-fills your environment: names and versions only).

## Security
Do not open a public issue for a vulnerability. Use GitHub's "Report a vulnerability" (private advisory) on the repository.
