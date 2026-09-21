"""Preprocess Databricks notebook SOURCE text into a Fabric-ready form.

Handles: parameter-cell injection, raw-table-to-PARQUET-path rewriting, legacy
widget comment-out, spark.sql f-string normalization, and other text rewrites
applied before the source is converted into a Fabric .ipynb (see
migrate_databricks_to_fabric.py / convert_databricks_source_to_ipynb).

Usage (as a library):
    from preprocess_notebook import preprocess_notebook
    processed_source = preprocess_notebook(raw_source, notebook_name)
"""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Optional

logger = logging.getLogger("preprocess_notebook")


# ---------------------------------------------------------------------------
# Environment loading
# ---------------------------------------------------------------------------

def _load_dotenv(env_path: Path) -> None:
    """Populate os.environ from a simple KEY=VALUE .env file (no overrides)."""
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


_load_dotenv(Path(__file__).resolve().parent / ".env")


def _get_env(name: str, default: str = "") -> str:
    return os.environ.get(name, default) or ""


def _get_env_list(name: str, default: str = "") -> list[str]:
    raw = _get_env(name, default)
    return [item.strip() for item in raw.split(",") if item.strip()]


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PARAMS_MARKER = "# __PARAMETERS_CELL__"

# DWH database name -> (prod CE db name, prod DA db name) mapping used when
# rewriting raw-table queries into PARQUET file paths.
_MAPPING_DB_DWH = _get_env_list(
    "MAPPING_DB_DWH",
    "whiteoak,arborvitae,magnolia,douglasfir,shagbark,caladium,silvermound,"
    "mayapple,munstead,anemone,harebells,turmeric,tamarack,irishflower,shagbark",
)
_MAPPING_DB_CE = _get_env_list(
    "MAPPING_DB_CE",
    "waerebo,atambua,maros,dompu,solok,canggah,sirimau,mayong,kemangkon,"
    "apitalawu,haruman,tritih,tunreng,inalipue,solok",
)
_MAPPING_DB_DA = _get_env_list(
    "MAPPING_DB_DA",
    "waterlily,allamanda,marigold,daisy,zepar,serrulata,marguerite,mithril,"
    "anvil,haapalaite,titania,tungsten,iolite,solok",
)
MAPPING_DB = dict(zip(_MAPPING_DB_DWH, zip(_MAPPING_DB_CE, _MAPPING_DB_DA)))

_RAW_TABLE_PATTERN = re.compile(
    r"(?i)(?P<indent>\s*)SELECT \*(?P<select_suffix>.*?)\s+FROM "
    r"(?:`(?P<catalog>[^`]+)`\.)?`?(?P<schema>[\w]+)`?\.(?P<table>[\w]+_raw)\s*\n"
    r"\s*WHERE ARRAY_JOIN\(SLICE\(SPLIT\(\s*_metadata\.file_path,\s*'/'\),\s*8,\s*4\),\s*'-'\)\s*=\s*"
    r"(?:\(DATE\(GETDATE\(\)\s*\+\s*INTERVAL 7 HOURS\)\)|date\(from_utc_timestamp\(current_timestamp\(\),\s*'Asia/Bangkok'\)\))\s*\|\|\s*'-(?P<folder_identifier>\{folder_identifier\}|[^']+)'"
    r"(?P<extra_filter>\s+AND\b.*)?"
)

COMMENT_OUT_PATTERNS = [
    r'\s*.*\bdbutils\b.*$',
    r'\s*print\(f"Folder data yang digunakan: \{folder_identifier\}"\)\s*$',
    r'\s*spark\.sql\("USE CATALOG.*"\)\s*$',
    r'\s*spark\.sql\("USE \w+"\)\s*$',
]


# ---------------------------------------------------------------------------
# Preprocessing rules
# ---------------------------------------------------------------------------

def _comment_matching_lines(source_text: str, patterns: Optional[list[str]]) -> str:
    compiled = [re.compile(pattern) for pattern in patterns or []]
    result = []
    for line in source_text.splitlines():
        stripped = line.lstrip()
        if compiled and not stripped.startswith("#") and any(pattern.match(line) for pattern in compiled):
            line = line[: len(line) - len(stripped)] + "# " + stripped
        result.append(line)
    return "\n".join(result)


def _injected_parameter_cells(is_sql_notebook: bool, include_auto_merge: bool) -> str:
    parameters = (
        f"{PARAMS_MARKER}\n"
        "from datetime import datetime as dt\n\n"
        "year   = dt.now().strftime('%Y')\n"
        "month  = dt.now().strftime('%m')\n"
        "day    = dt.now().strftime('%d')\n"
        "folder_identifier  = \"02\"\n"
        "payload_filter     = (\n"
        "    f\"payload >= '{dt.now():%Y-%m-%d} 00:00:00' and \"\n"
        "    f\"payload <= '{dt.now():%Y-%m-%d} 11:00:00'\"\n"
        ")\n"
        "main_dealer_id     = 1\n"
        "# COMMAND ----------\n"
    )
    if is_sql_notebook:
        config = (
            "spark.conf.set(\"year\", str(year))\n"
            "spark.conf.set(\"month\", str(month))\n"
            "spark.conf.set(\"day\", str(day))\n"
            "spark.conf.set(\"folder_identifier\", str(folder_identifier))\n"
            "spark.conf.set(\"payload_filter\", payload_filter)\n"
        )
    else:
        config = "spark.conf.set('spark.sql.parser.quotedRegexColumnNames', 'true')\n"
    if include_auto_merge:
        config += "spark.conf.set('spark.databricks.delta.schema.autoMerge.enabled', 'true')\n"
    return parameters + config + "# COMMAND ----------\n"


def _replace_magic_command(line: str, replacements: dict) -> str:
    stripped = line.lstrip()
    if not stripped.startswith("# MAGIC %"):
        return line
    indent = line[: len(line) - len(stripped)]
    command, separator, rest = stripped[len("# MAGIC "):].partition(" ")
    return indent + "# MAGIC " + replacements.get(command, command) + (separator + rest if separator else "")


def rewrite_raw_table_filter_to_parquet_path(source_text: str, is_sql_notebook: bool = False) -> str:
    """Rewrite raw-table + file-path-date filters into PARQUET path sources."""
    variables = ("${year}", "${month}", "${day}") if is_sql_notebook else ("{year}", "{month}", "{day}")

    def _replace(match: re.Match) -> str:
        catalog = match.group("catalog")
        schema = match.group("schema")
        table_raw = match.group("table")
        is_da = table_raw.startswith("da_")
        table_name = table_raw.removesuffix("_raw").removeprefix("da_" if is_da else "")
        dwh_db = catalog.removesuffix("-catalog") if catalog else schema.removesuffix("_raw")
        prod_db = MAPPING_DB.get(dwh_db, (dwh_db, dwh_db))[int(is_da)]
        folder = match.group("folder_identifier")
        if is_sql_notebook and folder == "{folder_identifier}":
            folder = "${folder_identifier}"
        source_type = "postgre" if is_da or catalog == "shagbark-catalog" or "shagbark" in schema else "sql_raw"
        path = "/".join((prod_db, table_name, *variables, folder))
        base = f"{match.group('indent')}SELECT `(?!__).*`{match.group('select_suffix').rstrip()} FROM PARQUET.`Files/DWH_Serverless/{source_type}/{path}`"

        extra_filter = match.group("extra_filter")
        if extra_filter:
            # remaining AND-conditions lose their WHERE clause when the ARRAY_JOIN filter is dropped
            extra_filter = re.sub(r"^\s*AND\s*", "WHERE ", extra_filter.strip(), count=1, flags=re.IGNORECASE)
            base += f" {extra_filter}"
        return base

    return _RAW_TABLE_PATTERN.sub(_replace, source_text)


def preprocess_databricks_source(
    source_text: str,
    text_replacements: Optional[dict] = None,
    regex_replacements: Optional[list[dict]] = None,
    magic_command_replacements: Optional[dict] = None,
    line_comment_patterns: Optional[list[str]] = COMMENT_OUT_PATTERNS,
    rewrite_raw_to_parquet: bool = True,
    inject_parameters_cell: bool = False,
) -> str:
    """Apply migration rules to Databricks SOURCE text before conversion."""
    transformed = source_text
    is_sql_notebook = transformed.startswith("-- Databricks notebook source")

    for old_text, new_text in (text_replacements or {}).items():
        transformed = transformed.replace(old_text, new_text)
    for rule in regex_replacements or []:
        transformed = re.sub(rule["pattern"], rule["repl"], transformed, flags=rule.get("flags", 0))

    transformed = re.sub(r"spark\.sql(\s*)\(\s*(\"\"\"|''')", r"spark.sql\1(f\2", transformed)
    has_row_number_alias = re.search(r"\brn\b", transformed, flags=re.IGNORECASE)
    if has_row_number_alias and "details" not in transformed.lower():
        transformed = re.sub(
            r"USING\s*\(\s*SELECT\s+\*\s+FROM\s*\(",
            "USING (\n    SELECT * EXCEPT (rn) FROM\n\t(",
            transformed,
            flags=re.IGNORECASE,
        )

    if magic_command_replacements:
        transformed = "\n".join(
            _replace_magic_command(line, magic_command_replacements)
            for line in transformed.splitlines()
        )
    transformed = _comment_matching_lines(transformed, line_comment_patterns)

    if inject_parameters_cell and PARAMS_MARKER not in transformed:
        injected = _injected_parameter_cells(is_sql_notebook, "details" not in transformed.lower())
        if is_sql_notebook:
            header = "-- Databricks notebook source\n"
            transformed = injected + header + transformed[len(header):]
        else:
            header = "# Databricks notebook source\n"
            transformed = header + injected + transformed[len(header):] if transformed.startswith(header) else injected + transformed

    if rewrite_raw_to_parquet:
        transformed = rewrite_raw_table_filter_to_parquet_path(transformed, is_sql_notebook)

    return re.sub(r"`[^`]+-catalog`\.", "", transformed)


def preprocess_notebook(
    notebook_content: str,
    notebook_name: str,
    **kwargs,
) -> str:
    """Preprocess a single notebook's raw Databricks SOURCE text.

    `notebook_name` is accepted for logging/context (and future per-notebook
    rules); the actual rewrites operate purely on `notebook_content`. Extra
    keyword arguments are forwarded to `preprocess_databricks_source`.
    """
    logger.debug("Preprocessing notebook: %s", notebook_name)
    return preprocess_databricks_source(notebook_content, **kwargs)
