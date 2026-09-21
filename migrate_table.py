"""Migrate schemas and table structures from Databricks to Fabric Lakehouse.

This script reads table metadata from Databricks information_schema, builds CREATE
SCHEMA and CREATE TABLE statements, and executes them through Fabric Livy.

Run this first, before migrate_data.py (needs the tables to exist),
migrate_notebook.py, and migrate_job.py.

Usage:
    python migrate_table.py
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("migrate_table")


# ------------------------------
# Config helpers
# ------------------------------


def load_env_file(env_path: Path) -> None:
    """Load key-value pairs from a .env file into environment variables."""
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        if key and key not in os.environ:
            os.environ[key] = value



def parse_csv(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]



def parse_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    return normalized in {"1", "true", "yes", "y", "on"}



def quote_identifier(name: str) -> str:
    """Quote Spark SQL identifiers safely."""
    escaped = name.replace("`", "``")
    return f"`{escaped}`"


@dataclass(frozen=True)
class MigrationConfig:
    databricks_host: str
    databricks_token: str
    databricks_sql_warehouse_id: str

    databricks_catalogs: list[str]
    schema_include: str
    schema_exclude: list[str]
    table_exclude_regexp: str

    fabric_token: str
    fabric_workspace_id: str
    fabric_lakehouse_id: str
    fabric_livy_api_version: str

    poll_interval_seconds: float
    session_poll_interval_seconds: float
    poll_timeout_seconds: float
    max_retries: int
    retry_backoff_seconds: float

    execute_schema_creation: bool
    execute_table_creation: bool
    close_session_on_finish: bool

    ddl_output_path: str

    @property
    def livy_base(self) -> str:
        return (
            "https://api.fabric.microsoft.com/v1/workspaces/"
            f"{self.fabric_workspace_id}/lakehouses/{self.fabric_lakehouse_id}"
            f"/livyapi/versions/{self.fabric_livy_api_version}"
        )

    @staticmethod
    def from_env() -> "MigrationConfig":
        required = {
            "DATABRICKS_HOST": os.getenv("DATABRICKS_HOST"),
            "DATABRICKS_TOKEN": os.getenv("DATABRICKS_TOKEN"),
            "DATABRICKS_SQL_WAREHOUSE_ID": os.getenv("DATABRICKS_SQL_WAREHOUSE_ID"),
            "FABRIC_TOKEN": os.getenv("FABRIC_TOKEN"),
            "FABRIC_WORKSPACE_ID": os.getenv("FABRIC_WORKSPACE_ID"),
            "FABRIC_LAKEHOUSE_ID": os.getenv("FABRIC_LAKEHOUSE_ID"),
        }

        missing = [key for key, value in required.items() if not value]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

        # Narrow to dict[str, str] now that None values are ruled out above.
        required_values: dict[str, str] = {k: v for k, v in required.items() if v is not None}

        return MigrationConfig(
            databricks_host=required_values["DATABRICKS_HOST"].rstrip("/"),
            databricks_token=required_values["DATABRICKS_TOKEN"],
            databricks_sql_warehouse_id=required_values["DATABRICKS_SQL_WAREHOUSE_ID"],
            databricks_catalogs=parse_csv(os.getenv("DATABRICKS_CATALOGS", "")),
            # Regex, not SQL LIKE -- supports alternation, e.g. "raw|view".
            schema_include=os.getenv("DATABRICKS_SCHEMA_INCLUDE", ".*_raw$"),
            schema_exclude=parse_csv(
                os.getenv("DATABRICKS_SCHEMA_EXCLUDE", "default,information_schema")
            ),
            table_exclude_regexp=os.getenv("DATABRICKS_TABLE_EXCLUDE_REGEXP", "raw"),
            fabric_token=required_values["FABRIC_TOKEN"],
            fabric_workspace_id=required_values["FABRIC_WORKSPACE_ID"],
            fabric_lakehouse_id=required_values["FABRIC_LAKEHOUSE_ID"],
            fabric_livy_api_version=os.getenv("FABRIC_LIVY_API_VERSION", "2023-12-01"),
            poll_interval_seconds=float(os.getenv("POLL_INTERVAL_SECONDS", "1")),
            session_poll_interval_seconds=float(
                os.getenv("SESSION_POLL_INTERVAL_SECONDS", "3")
            ),
            poll_timeout_seconds=float(os.getenv("POLL_TIMEOUT_SECONDS", "900")),
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            retry_backoff_seconds=float(os.getenv("RETRY_BACKOFF_SECONDS", "2")),
            execute_schema_creation=parse_bool(
                os.getenv("EXECUTE_SCHEMA_CREATION", "true"), default=True
            ),
            execute_table_creation=parse_bool(
                os.getenv("EXECUTE_TABLE_CREATION", "true"), default=True
            ),
            close_session_on_finish=parse_bool(
                os.getenv("CLOSE_SESSION_ON_FINISH", "true"), default=True
            ),
            ddl_output_path=os.getenv("DDL_OUTPUT_PATH", "migration_ddls.sql"),
        )


# ------------------------------
# HTTP helper
# ------------------------------


def request_with_retry(
    method: str,
    url: str,
    headers: dict[str, str],
    max_retries: int,
    backoff_seconds: float,
    **kwargs: Any,
) -> requests.Response:
    """Perform an HTTP request, retrying on network errors and transient 429/5xx responses."""
    attempt = 0
    while True:
        attempt += 1
        try:
            response = requests.request(method, url, headers=headers, timeout=180, **kwargs)
        except requests.exceptions.RequestException as exc:
            if attempt > max_retries:
                raise
            logger.warning(
                "Request error on %s %s (attempt %d/%d): %s", method, url, attempt, max_retries, exc
            )
            time.sleep(backoff_seconds * attempt)
            continue

        if response.status_code == 429 or response.status_code >= 500:
            if attempt > max_retries:
                response.raise_for_status()
            logger.warning(
                "Transient HTTP %d on %s %s (attempt %d/%d)",
                response.status_code, method, url, attempt, max_retries,
            )
            time.sleep(backoff_seconds * attempt)
            continue

        response.raise_for_status()
        return response


# ------------------------------
# Databricks client
# ------------------------------


class DatabricksClient:
    def __init__(self, config: MigrationConfig) -> None:
        self.config = config
        self.headers = {"Authorization": f"Bearer {config.databricks_token}"}

    def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        url = f"{self.config.databricks_host}{path}"
        response = request_with_retry(
            method, url, self.headers, self.config.max_retries, self.config.retry_backoff_seconds, **kwargs
        )
        return response.json()

    def run_sql_query(self, query: str) -> list[dict[str, Any]]:
        payload = {
            "warehouse_id": self.config.databricks_sql_warehouse_id,
            "statement": query,
            "wait_timeout": "30s",
            "disposition": "INLINE",
        }

        resp = self._request("POST", "/api/2.0/sql/statements", json=payload)
        statement_id = resp["statement_id"]

        deadline = time.monotonic() + self.config.poll_timeout_seconds
        while resp.get("status", {}).get("state") in {"PENDING", "RUNNING"}:
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"Databricks statement {statement_id} did not complete within {self.config.poll_timeout_seconds}s"
                )
            time.sleep(self.config.poll_interval_seconds)
            resp = self._request("GET", f"/api/2.0/sql/statements/{statement_id}")

        state = resp.get("status", {}).get("state")
        if state != "SUCCEEDED":
            error = resp.get("status", {}).get("error", {})
            raise RuntimeError(
                f"Databricks SQL statement failed with state={state}: {error or resp}"
            )

        columns = [c["name"] for c in resp["manifest"]["schema"]["columns"]]
        all_rows: list[list[Any]] = []

        result = resp.get("result", {})
        all_rows.extend(result.get("data_array", []))

        next_chunk = result.get("next_chunk_internal_link")
        while next_chunk:
            chunk = self._request("GET", next_chunk)
            all_rows.extend(chunk.get("data_array", []))
            next_chunk = chunk.get("next_chunk_internal_link")

        return [dict(zip(columns, row)) for row in all_rows]

    def fetch_columns_metadata(self) -> list[dict[str, Any]]:
        if not self.config.databricks_catalogs:
            raise ValueError("DATABRICKS_CATALOGS must contain at least one catalog")

        catalogs = ", ".join(f"'{c}'" for c in self.config.databricks_catalogs)
        schema_exclude = ", ".join(f"'{s}'" for s in self.config.schema_exclude)

        query = f"""
SELECT
    table_catalog,
    table_schema,
    table_name,
    column_name,
    data_type,
    is_nullable,
    ordinal_position,
    comment
FROM system.information_schema.columns
WHERE
    table_catalog IN ({catalogs})
    AND table_schema NOT IN ({schema_exclude})
    AND table_schema = '{self.config.schema_include}'
    AND table_name NOT REGEXP '{self.config.table_exclude_regexp}'
ORDER BY table_catalog, table_schema, table_name, ordinal_position
""".strip()

        return self.run_sql_query(query)


def split_delimited_output(text: str, delimiter: str = "|||") -> set[str]:
    """Split a println-produced, delimiter-joined Scala output line into a set of names."""
    text = text.strip()
    if not text:
        return set()
    return {part for part in text.split(delimiter) if part}


# ------------------------------
# Fabric Livy client
# ------------------------------


class FabricLivyClient:
    def __init__(self, config: MigrationConfig) -> None:
        self.config = config
        self.headers = {
            "Authorization": f"Bearer {config.fabric_token}",
            "Content-Type": "application/json",
        }
        self.session_id: int | None = None

    def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        url = f"{self.config.livy_base}{path}"
        response = request_with_retry(
            method, url, self.headers, self.config.max_retries, self.config.retry_backoff_seconds, **kwargs
        )
        return response.json()

    def start_session(self) -> int:
        resp = self._request(
            "POST",
            "/sessions",
            json={"conf": {"spark.fabric.enableSchema": "true"}},
        )
        session_id = resp["id"]

        deadline = time.monotonic() + self.config.poll_timeout_seconds
        while True:
            status = self._request("GET", f"/sessions/{session_id}")
            state = status.get("state")
            if state == "idle":
                self.session_id = session_id
                return session_id
            if state in {"error", "dead", "killed", "shutting_down"}:
                raise RuntimeError(f"Fabric session failed to start: state={state}")
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"Fabric session {session_id} did not become idle within {self.config.poll_timeout_seconds}s"
                )
            time.sleep(self.config.session_poll_interval_seconds)

    def close_session(self) -> None:
        if self.session_id is None:
            return
        try:
            response = request_with_retry(
                "DELETE",
                f"{self.config.livy_base}/sessions/{self.session_id}",
                self.headers,
                self.config.max_retries,
                self.config.retry_backoff_seconds,
            )
            logger.info("Fabric session %s closed (status=%d)", self.session_id, response.status_code)
        except requests.exceptions.RequestException as exc:
            logger.warning("Failed to close Fabric session %s: %s", self.session_id, exc)

    def run_statement(self, code: str) -> dict[str, Any]:
        if self.session_id is None:
            raise RuntimeError("Fabric session is not started")

        resp = self._request(
            "POST",
            f"/sessions/{self.session_id}/statements",
            json={"code": code, "kind": "spark"},
        )
        statement_id = resp["id"]

        deadline = time.monotonic() + self.config.poll_timeout_seconds
        while True:
            status = self._request(
                "GET", f"/sessions/{self.session_id}/statements/{statement_id}"
            )
            if status.get("state") == "available":
                output = status.get("output", {})
                if output.get("status") == "error":
                    raise RuntimeError(f"Fabric statement failed: {output.get('evalue')}")
                return output
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"Fabric statement {statement_id} did not complete within {self.config.poll_timeout_seconds}s"
                )
            time.sleep(self.config.poll_interval_seconds)

    def run_sql_statements(self, statements: list[str]) -> list[tuple[str, str]]:
        """Execute statements one by one so a single failure doesn't abort the rest; returns failures."""
        failures: list[tuple[str, str]] = []
        for stmt in statements:
            try:
                self.run_statement(f"spark.sql({json.dumps(stmt)})")
            except Exception as exc:  # noqa: BLE001 - collect and continue
                failures.append((stmt, str(exc)))
        return failures

    def list_schemas(self) -> set[str]:
        """Query Fabric for schemas that actually exist, to confirm creation before table DDL runs."""
        code = 'println(spark.sql("SHOW SCHEMAS").collect().map(_.getString(0)).mkString("|||"))'
        output = self.run_statement(code)
        return split_delimited_output(output.get("data", {}).get("text/plain", ""))

    def list_tables(self, schema: str) -> set[str]:
        """Query Fabric for tables that actually exist in a schema."""
        show_tables = json.dumps(f"SHOW TABLES IN {quote_identifier(schema)}")
        code = f'println(spark.sql({show_tables}).collect().map(_.getString(1)).mkString("|||"))'
        output = self.run_statement(code)
        return split_delimited_output(output.get("data", {}).get("text/plain", ""))


# ------------------------------
# Transformation / DDL generation
# ------------------------------


TableDef = dict[str, Any]


def group_columns_to_tables(column_rows: list[dict[str, Any]]) -> list[TableDef]:
    grouped: dict[tuple[str, str], TableDef] = defaultdict(
        lambda: {"schema": None, "table": None, "columns": []}
    )

    for row in column_rows:
        key = (row["table_schema"], row["table_name"])
        grouped[key]["schema"] = row["table_schema"]
        grouped[key]["table"] = row["table_name"]
        grouped[key]["columns"].append(
            {
                "name": row["column_name"],
                "type_text": row["data_type"],
                "nullable": row["is_nullable"] == "YES",
                "position": int(row["ordinal_position"]),
            }
        )

    return list(grouped.values())



def build_create_table_ddl(entry: TableDef) -> str:
    schema = quote_identifier(entry["schema"])
    table = quote_identifier(entry["table"])
    columns = sorted(entry["columns"], key=lambda c: c["position"])

    col_defs: list[str] = []
    for col in columns:
        nullable = "" if col["nullable"] else " NOT NULL"
        col_name = quote_identifier(col["name"])
        col_defs.append(f"    {col_name} {col['type_text']}{nullable}")

    ddl = f"CREATE TABLE IF NOT EXISTS {schema}.{table}\n(\n"
    ddl += ",\n".join(col_defs)
    ddl += "\n)\nUSING DELTA"
    return ddl



def build_schema_create_statements(table_defs: list[TableDef]) -> list[str]:
    schemas = sorted({t["schema"] for t in table_defs})
    return [f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(name)}" for name in schemas]



def build_table_create_statements(table_defs: list[TableDef]) -> list[str]:
    return [build_create_table_ddl(t) for t in table_defs]



def _timestamped_path(path: str) -> str:
    """Insert a YYYYmmdd_HHMMSS timestamp before the file extension so reruns don't overwrite it."""
    p = Path(path)
    return str(p.with_name(f"{p.stem}_{datetime.now():%Y%m%d_%H%M%S}{p.suffix}"))


def write_ddl_file(path: str, schema_stmts: list[str], table_stmts: list[str]) -> None:
    chunks: list[str] = []

    chunks.append("-- Schema DDL")
    chunks.extend(f"{stmt};" for stmt in schema_stmts)
    chunks.append("")
    chunks.append("-- Table DDL")
    chunks.extend(f"{stmt};" for stmt in table_stmts)

    Path(path).write_text("\n".join(chunks) + "\n", encoding="utf-8")


def verify_migration(
    fabric: FabricLivyClient, table_defs: list[TableDef], config: MigrationConfig, check_tables: bool
) -> None:
    """Re-poll Fabric until every expected schema/table is actually visible, tolerating creation delay."""
    tables_by_schema: dict[str, set[str]] = defaultdict(set)
    for entry in table_defs:
        tables_by_schema[entry["schema"]].add(entry["table"])
    required_schemas = sorted(tables_by_schema)

    # range(1, max_retries + 2) always yields at least one attempt, so these are set before use.
    missing_schemas: set[str]
    missing_tables: dict[str, set[str]]

    for attempt in range(1, config.max_retries + 2):
        existing_schemas = {name.rsplit(".", 1)[-1] for name in fabric.list_schemas()}
        missing_schemas = set(required_schemas) - existing_schemas

        missing_tables = {}
        if check_tables:
            for schema in required_schemas:
                if schema in missing_schemas:
                    continue
                still_missing = tables_by_schema[schema] - fabric.list_tables(schema)
                if still_missing:
                    missing_tables[schema] = still_missing

        if not missing_schemas and not missing_tables:
            logger.info(
                "Verification passed: %d schema(s)%s confirmed in Fabric",
                len(required_schemas),
                f" and {sum(len(v) for v in tables_by_schema.values())} table(s)" if check_tables else "",
            )
            return

        logger.warning(
            "Verification attempt %d/%d: %d schema(s) and %d table(s) not yet visible, retrying in %.1fs...",
            attempt,
            config.max_retries + 1,
            len(missing_schemas),
            sum(len(v) for v in missing_tables.values()),
            config.retry_backoff_seconds,
        )
        time.sleep(config.retry_backoff_seconds * attempt)

    if missing_schemas:
        logger.error("Schemas still missing after verification: %s", ", ".join(sorted(missing_schemas)))
    for schema, tables in missing_tables.items():
        logger.error("Tables still missing in schema %s: %s", schema, ", ".join(sorted(tables)))


# ------------------------------
# Main flow
# ------------------------------


def migrate() -> None:
    load_env_file(Path(__file__).with_name(".env"))
    config = MigrationConfig.from_env()

    logger.info("[1/6] Fetching metadata from Databricks...")
    dbr = DatabricksClient(config)
    column_rows = dbr.fetch_columns_metadata()
    logger.info("Fetched %d column records", len(column_rows))

    logger.info("[2/6] Grouping tables and building DDL...")
    table_defs = group_columns_to_tables(column_rows)
    schema_stmts = build_schema_create_statements(table_defs)
    table_stmts = build_table_create_statements(table_defs)
    logger.info("Schemas: %d | Tables: %d", len(schema_stmts), len(table_stmts))

    ddl_output_path = _timestamped_path(config.ddl_output_path)
    logger.info("[3/6] Writing generated DDL to: %s", ddl_output_path)
    write_ddl_file(ddl_output_path, schema_stmts, table_stmts)

    logger.info("[4/6] Starting Fabric session...")
    fabric = FabricLivyClient(config)
    fabric.start_session()

    try:
        if config.execute_schema_creation:
            logger.info("[5/6] Creating schemas in Fabric...")
            failures = fabric.run_sql_statements(schema_stmts)
            for stmt, err in failures:
                logger.error("Schema creation failed: %s -> %s", stmt, err)
            succeeded = len(schema_stmts) - len(failures)
            percentage = (succeeded / len(schema_stmts) * 100) if schema_stmts else 100.0
            logger.info(
                "Schema creation completed: %d/%d success (%.1f%%)",
                succeeded, len(schema_stmts), percentage,
            )

        if config.execute_table_creation:
            logger.info("Confirming schemas exist in Fabric before creating tables...")
            existing_schemas = fabric.list_schemas()
            logger.info("Fabric schemas: %s", ", ".join(sorted(existing_schemas)) or "(none)")

            # Fabric reports fully-qualified names (e.g. workspace.lakehouse.schema); compare by leaf name.
            existing_schema_names = {name.rsplit(".", 1)[-1] for name in existing_schemas}
            required_schemas = {t["schema"] for t in table_defs}
            missing_schemas = required_schemas - existing_schema_names
            if missing_schemas:
                logger.warning(
                    "Skipping tables for schemas not found in Fabric: %s",
                    ", ".join(sorted(missing_schemas)),
                )

            ready_table_defs = [t for t in table_defs if t["schema"] not in missing_schemas]
            ready_table_stmts = build_table_create_statements(ready_table_defs)

            logger.info("[6/6] Creating tables in Fabric...")
            failures = fabric.run_sql_statements(ready_table_stmts)
            for stmt, err in failures:
                logger.error("Table creation failed: %s -> %s", stmt, err)
            succeeded = len(ready_table_stmts) - len(failures)
            percentage = (succeeded / len(table_stmts) * 100) if table_stmts else 100.0
            logger.info(
                "Table creation completed: %d/%d success (%.1f%%), %d skipped due to missing schema",
                succeeded,
                len(table_stmts),
                percentage,
                len(table_stmts) - len(ready_table_stmts),
            )

        if config.execute_schema_creation or config.execute_table_creation:
            logger.info("Verifying all schemas and tables were actually created (allowing for propagation delay)...")
            verify_migration(fabric, table_defs, config, check_tables=config.execute_table_creation)

    finally:
        if config.close_session_on_finish:
            logger.info("Closing Fabric session...")
            fabric.close_session()

    logger.info("Migration completed")


if __name__ == "__main__":
    migrate()
