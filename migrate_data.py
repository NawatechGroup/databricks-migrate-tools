"""Insert data from Databricks Delta tables into Fabric Lakehouse tables via OneLake shortcuts.

Assumes a OneLake shortcut has already been created pointing at the Databricks
Unity Catalog storage location (abfss://...), so the underlying Delta files are
reachable from Fabric as `Files/<path>`. This script queries Databricks
information_schema for each table's storage_path, then runs:

    INSERT INTO {schema}.{table} SELECT * FROM DELTA.`Files/{delta_path}`

through the same Fabric Livy session mechanism used by migrate_table.py.

Run this after migrate_table.py has created the target schemas/tables, and
after the OneLake shortcuts pointing at each Databricks table's storage_path
have been created.

Usage:
    python migrate_data.py
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from migrate_table import (
    DatabricksClient,
    FabricLivyClient,
    MigrationConfig,
    load_env_file,
    parse_bool,
    quote_identifier,
)

logger = logging.getLogger("migrate_data")

_ABFSS_PATTERN = re.compile(r"^abfss://[^@]+@[^/]+/(?P<path>.+)$")

_INSERT_VERBS = {"INTO": "INSERT INTO", "OVERWRITE": "INSERT OVERWRITE"}


def storage_path_to_delta_files_path(storage_path: str) -> str:
    """Convert an abfss:// Unity Catalog storage path into a Fabric `Files/...` path."""
    match = _ABFSS_PATTERN.match(storage_path.strip())
    if not match:
        raise ValueError(f"Unsupported storage_path format: {storage_path}")
    return f"Files/{match.group('path')}"


def fetch_table_locations(dbr: DatabricksClient, config: MigrationConfig) -> list[dict[str, Any]]:
    if not config.databricks_catalogs:
        raise ValueError("DATABRICKS_CATALOGS must contain at least one catalog")

    catalogs = ", ".join(f"'{c}'" for c in config.databricks_catalogs)
    schema_exclude = ", ".join(f"'{s}'" for s in config.schema_exclude)

    query = f"""
        SELECT
            table_catalog,
            table_schema,
            table_name,
            storage_path
        FROM system.information_schema.tables
        WHERE
            table_catalog IN ({catalogs})
            AND table_schema NOT IN ({schema_exclude})
            AND table_schema = '{config.schema_include}'
            AND table_name NOT REGEXP '{config.table_exclude_regexp}'
            AND table_type = 'MANAGED'
        ORDER BY table_catalog, table_schema, table_name
""".strip()

    return dbr.run_sql_query(query)


def build_insert_statement(schema: str, table: str, delta_files_path: str, mode: str) -> str:
    verb = _INSERT_VERBS.get(mode.upper(), _INSERT_VERBS["INTO"])
    target = f"{quote_identifier(schema)}.{quote_identifier(table)}"
    return f"{verb} {target} SELECT * FROM DELTA.`{delta_files_path}`"


def _timestamped_path(path: str) -> str:
    """Insert a YYYYmmdd_HHMMSS timestamp before the file extension so reruns don't overwrite it."""
    p = Path(path)
    return str(p.with_name(f"{p.stem}_{datetime.now():%Y%m%d_%H%M%S}{p.suffix}"))


def write_insert_file(path: str, statements: list[str]) -> None:
    Path(path).write_text("\n".join(f"{stmt};" for stmt in statements) + "\n", encoding="utf-8")


def migrate() -> None:
    load_env_file(Path(__file__).with_name(".env"))
    config = MigrationConfig.from_env()

    insert_mode = os.getenv("DATA_INSERT_MODE", "INTO")
    output_path = _timestamped_path(os.getenv("INSERT_OUTPUT_PATH", "migration_inserts.sql"))
    execute_insert = parse_bool(os.getenv("EXECUTE_DATA_INSERT", "true"), default=True)

    logger.info("[1/4] Fetching table storage locations from Databricks...")
    dbr = DatabricksClient(config)
    table_rows = fetch_table_locations(dbr, config)
    logger.info("Found %d table(s)", len(table_rows))

    logger.info("[2/4] Building INSERT statements (mode=%s)...", insert_mode)
    statements: list[str] = []
    skipped: list[tuple[str, str]] = []
    for row in table_rows:
        schema, table, storage_path = row["table_schema"], row["table_name"], row["storage_path"]
        try:
            delta_files_path = storage_path_to_delta_files_path(storage_path)
        except ValueError as exc:
            skipped.append((f"{schema}.{table}", str(exc)))
            logger.warning("Skipping %s.%s: %s", schema, table, exc)
            continue
        statements.append(build_insert_statement(schema, table, delta_files_path, insert_mode))

    logger.info("[3/4] Writing generated statements to: %s", output_path)
    write_insert_file(output_path, statements)

    succeeded: list[str] = []
    failed: list[tuple[str, str]] = []

    if execute_insert:
        logger.info("[4/4] Executing INSERT statements in Fabric...")
        fabric = FabricLivyClient(config)
        fabric.start_session()
        try:
            for index, stmt in enumerate(statements, start=1):
                progress = f"[{index}/{len(statements)}]"
                logger.info("%s Running: %s", progress, stmt)
                failures = fabric.run_sql_statements([stmt])
                if failures:
                    failed.append((stmt, failures[0][1]))
                    logger.error("%s FAILED: %s", progress, failures[0][1])
                else:
                    succeeded.append(stmt)
        finally:
            if config.close_session_on_finish:
                logger.info("Closing Fabric session...")
                fabric.close_session()
    else:
        logger.info("[4/4] EXECUTE_DATA_INSERT=false, skipping execution (statements written to file only)")

    logger.info("=" * 60)
    logger.info("Insert summary")
    logger.info("  Tables discovered : %d", len(table_rows))
    logger.info("  Skipped (bad path): %d", len(skipped))
    logger.info("  Statements built  : %d", len(statements))
    if execute_insert:
        logger.info("  Succeeded         : %d", len(succeeded))
        logger.info("  Failed            : %d", len(failed))
        if failed:
            logger.info("Failed statements:")
            for stmt, reason in failed:
                logger.info("  - %s: %s", stmt, reason)
    logger.info("=" * 60)


if __name__ == "__main__":
    logging.basicConfig(
        level="INFO",
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    migrate()
