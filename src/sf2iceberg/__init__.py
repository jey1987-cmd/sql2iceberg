"""sf2iceberg: detect Snowflake SQL, convert it for Spark/Iceberg, verify under pytest."""

from .convert import Finding, Result, convert_statement
from .detect import Statement, detect_path, detect_python, detect_sql_text

__all__ = [
    "Finding",
    "Result",
    "Statement",
    "convert_statement",
    "detect_path",
    "detect_python",
    "detect_sql_text",
]
__version__ = "0.1.0.dev0"
