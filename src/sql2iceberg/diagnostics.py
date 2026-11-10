"""``sql2iceberg doctor``: check the local environment, and build a pre-filled GitHub issue link.

Only names and versions are collected: never values of environment variables, never credentials.
"""

from __future__ import annotations

import importlib.metadata as md
import os
import platform
import re
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.parse import quote

from . import __version__

REPO = "jey1987-cmd/sql2iceberg"
ISSUES_URL = f"https://github.com/{REPO}/issues"

# (distribution name, needed for)
PACKAGES = [
    ("sqlglot", "core: SQL parsing and conversion"),
    ("pyspark", "Spark jobs, verify, tests (extra: spark)"),
    ("pytest", "pytest plugin (extra: pytest)"),
    ("chispa", "DataFrame comparison in tests (extra: pytest)"),
    ("snowflake-connector-python", "Snowflake connector (extra: snowflake)"),
    ("psycopg2-binary", "Postgres connector, experimental (extra: postgres)"),
    ("pymysql", "MySQL connector, experimental (extra: mysql)"),
    ("pyodbc", "SQL Server connector, experimental (extra: mssql)"),
    ("oracledb", "Oracle connector, experimental (extra: oracle)"),
    ("boto3", "AWS Glue/Athena publisher (extra: aws)"),
    ("google-cloud-bigquery", "BigQuery publisher (extra: gcp)"),
    ("python-dotenv", "optional: load a .env file in your own scripts"),
]
ENV_NAMES = [
    "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_PRIVATE_KEY_PATH",
    "SNOWFLAKE_AUTHENTICATOR", "SNOWFLAKE_WAREHOUSE", "AWS_PROFILE", "AWS_REGION", "AWS_CA_BUNDLE",
    "GOOGLE_APPLICATION_CREDENTIALS", "JAVA_HOME", "SPARK_HOME", "JDK_JAVA_OPTIONS", "JAVA_TOOL_OPTIONS",
    "SQL2ICEBERG_TRACE", "SQL2ICE_ICEBERG",
]


def _java_version(java: str) -> str | None:
    try:
        out = subprocess.run([java, "-version"], capture_output=True, text=True, timeout=15, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return None
    m = re.search(r'version "([^"]+)"', out.stderr + out.stdout)
    return m.group(1) if m else None


def _major(v: str | None) -> int | None:
    if not v:
        return None
    parts = v.split(".")
    return int(parts[1]) if parts[0] == "1" and len(parts) > 1 else int(re.match(r"\d+", parts[0]).group())


def collect() -> dict:
    """Return {"info": {...}, "checks": [(level, message)]}. Level is OK, WARN or FAIL."""
    checks: list[tuple[str, str]] = []
    info: dict = {"sql2iceberg": __version__, "python": sys.version.split()[0],
                  "os": f"{platform.system()} {platform.release()}"}
    pkgs = {}
    for dist, _ in PACKAGES:
        try:
            pkgs[dist] = md.version(dist)
        except md.PackageNotFoundError:
            pkgs[dist] = None
    info["packages"] = pkgs
    # Java and Spark
    on_path = shutil.which("java")
    home = os.environ.get("JAVA_HOME")
    path_v = _java_version(on_path) if on_path else None
    home_java = str(Path(home) / "bin" / "java") if home else None
    home_v = _java_version(home_java) if home_java and Path(home_java + (".exe" if os.name == "nt" else "")).exists() else None
    info["java_on_path"], info["java_home_version"] = path_v, home_v
    if pkgs.get("pyspark"):
        if not (path_v or home_v):
            checks.append(("FAIL", "pyspark is installed but no Java was found. Install JDK 17 and set JAVA_HOME."))
        for label, v in (("PATH", path_v), ("JAVA_HOME", home_v)):
            m = _major(v)
            if m is not None and m not in (17, 21) and pkgs["pyspark"].startswith("4"):
                checks.append(("WARN", f"Java on {label} is {v}; PySpark 4 needs Java 17 or 21."))
            elif m is not None and m not in (8, 11, 17) and pkgs["pyspark"].startswith("3.5"):
                checks.append(("WARN", f"Java on {label} is {v}; PySpark 3.5 supports Java 8, 11 and 17."))
        if path_v and home_v and _major(path_v) != _major(home_v):
            checks.append(("WARN", (f"JAVA_HOME ({home_v}) differs from java on PATH ({path_v}). Spark uses JAVA_HOME. "
                                    "See docs/troubleshooting.md: UnsupportedClassVersionError.")))
    sh = os.environ.get("SPARK_HOME")
    if sh and pkgs.get("pyspark") and "site-packages" not in sh.replace("\\", "/"):
        checks.append(("WARN", ("SPARK_HOME points outside the pip-installed PySpark and will override it. "
                                "Unset it (docs/troubleshooting.md: wrong Spark version).")))
    # network trust
    if os.environ.get("AWS_CA_BUNDLE") or os.environ.get("REQUESTS_CA_BUNDLE"):
        checks.append(("OK", ("A custom CA bundle is set for Python/AWS. Java needs its own truststore "
                              "(docs/troubleshooting.md: certificate_unknown).")))
    # jars
    jars = sorted(p.name for p in Path("jars").glob("*.jar")) if Path("jars").is_dir() else []
    info["local_jars"] = jars
    info["env_set"] = [n for n in ENV_NAMES if os.environ.get(n)]
    if not checks:
        checks.append(("OK", "No problems found"))
    return {"info": info, "checks": checks}


def to_markdown(result: dict) -> str:
    i = result["info"]
    lines = [f"sql2iceberg {i['sql2iceberg']} | Python {i['python']} | {i['os']}",
             f"Java on PATH: {i['java_on_path']} | JAVA_HOME java: {i['java_home_version']}", "", "Packages:"]
    lines += [f"- {k}: {v or 'not installed'}" for k, v in i["packages"].items()]
    lines += ["", "Local jars: " + (", ".join(i["local_jars"]) or "none"),
              "Environment variables set (names only): " + (", ".join(i["env_set"]) or "none"), "", "Checks:"]
    lines += [f"- [{lvl}] {msg}" for lvl, msg in result["checks"]]
    return "\n".join(lines) + "\n"


def issue_url(title: str = "", template: str = "bug_report.yml") -> str:
    body = "```\n" + to_markdown(collect()) + "```\n"
    return (f"{ISSUES_URL}/new?template={quote(template)}&title={quote(title)}"
            f"&environment={quote(body)}")
