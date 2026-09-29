"""Migrate Databricks notebooks (SOURCE format) into Microsoft Fabric notebooks.

All secrets/parameters are read from environment variables (see .env.example).
A local .env file (next to this script) is loaded automatically if present,
without overriding variables already set in the real environment.

Preprocessing (parameter-cell injection, PARQUET path rewriting, legacy widget
comment-out, etc.) lives in notebook_preprocessor.py and is used automatically
if that file is present alongside this script; pass preprocess=False (or set
MIGRATE_PREPROCESS=false) to migrate notebooks as-is without it.

Run this after migrate_table.py / migrate_data.py so notebooks can reference
already-migrated tables; run migrate_job.py afterwards to recreate Databricks
Jobs as Fabric pipelines pointing at these notebooks.

Usage:
    python migrate_notebook.py
"""
from __future__ import annotations

import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import logging
import os
import re
import threading
import time
from pathlib import Path
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("migrate_notebook")

try:
    from notebook_preprocessor import COMMENT_OUT_PATTERNS, PARAMS_MARKER, preprocess_notebook
except ImportError:
    PARAMS_MARKER = "# __PARAMETERS_CELL__"
    COMMENT_OUT_PATTERNS: list[str] = []
    preprocess_notebook = None

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


def _get_env(name: str, default: Optional[str] = None, required: bool = False) -> str:
    value = os.environ.get(name, default)
    if required and not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value or ""


def _get_env_list(name: str, default: str = "") -> list[str]:
    raw = _get_env(name, default)
    return [item.strip() for item in raw.split(",") if item.strip()]


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATABRICKS_HOST = _get_env("DATABRICKS_HOST", required=True).rstrip("/")
DATABRICKS_TOKEN = _get_env("DATABRICKS_TOKEN", required=True)
DATABRICKS_NOTEBOOK_PATH = _get_env("DATABRICKS_NOTEBOOK_PATH", "/")
DATABRICKS_EXCLUDE_PATH_CONTAINS = _get_env_list("DATABRICKS_EXCLUDE_PATH_CONTAINS", "Backup")
MIGRATE_PREPROCESS = _get_env("MIGRATE_PREPROCESS", "false").strip().lower() in ("1", "true", "yes")

FABRIC_TOKEN = _get_env("FABRIC_TOKEN", required=True)
FABRIC_WORKSPACE_ID = _get_env("FABRIC_WORKSPACE_ID", required=True)
FABRIC_LAKEHOUSE_ID = _get_env("FABRIC_LAKEHOUSE_ID", required=True)
FABRIC_LAKEHOUSE_NAME = _get_env("FABRIC_LAKEHOUSE_NAME", "SharedLakehouse")
FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"

DATABRICKS_HEADERS = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
FABRIC_HEADERS = {"Authorization": f"Bearer {FABRIC_TOKEN}", "Content-Type": "application/json"}

DEFAULT_LAKEHOUSE = {
    "id": FABRIC_LAKEHOUSE_ID,
    "name": FABRIC_LAKEHOUSE_NAME,
    "workspace_id": FABRIC_WORKSPACE_ID,
}

DEFAULT_TIMEOUT = (
    float(os.getenv("HTTP_CONNECT_TIMEOUT", "15")),
    float(os.getenv("HTTP_READ_TIMEOUT", "60")),
)
HTTP_MAX_RETRIES = int(os.getenv("HTTP_MAX_RETRIES", "3"))
HTTP_RETRY_BACKOFF = float(os.getenv("HTTP_RETRY_BACKOFF", "2.0"))
NOTEBOOK_MIGRATE_CONCURRENCY = int(
    os.getenv("NOTEBOOK_MIGRATE_CONCURRENCY", os.getenv("CONCURRENCY", "4"))
)


# ---------------------------------------------------------------------------
# HTTP Helpers (Resilience, Timeouts, Keep-Alive Connection Pooling)
# ---------------------------------------------------------------------------

def create_session(pool_size: int = 20) -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=pool_size,
        pool_maxsize=pool_size,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    max_retries: int = HTTP_MAX_RETRIES,
    backoff_seconds: float = HTTP_RETRY_BACKOFF,
    timeout: tuple[float, float] = DEFAULT_TIMEOUT,
    **kwargs: Any,
) -> requests.Response:
    attempt = 0
    while True:
        attempt += 1
        try:
            response = session.request(method, url, timeout=timeout, **kwargs)
        except (requests.exceptions.RequestException, ConnectionResetError) as exc:
            if attempt > max_retries:
                raise
            logger.warning(
                "Transient network error on %s %s (attempt %d/%d): %s. Retrying in %.1fs...",
                method, url, attempt, max_retries, exc, backoff_seconds * attempt,
            )
            time.sleep(backoff_seconds * attempt)
            continue

        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            wait_time = float(retry_after) if retry_after and retry_after.isdigit() else backoff_seconds * attempt
            if attempt > max_retries:
                return response
            logger.warning("Rate limited (HTTP 429) on %s %s. Waiting %.1fs...", method, url, wait_time)
            time.sleep(wait_time)
            continue

        if response.status_code >= 500:
            if attempt > max_retries:
                return response
            logger.warning(
                "Transient HTTP %d on %s %s (attempt %d/%d). Retrying in %.1fs...",
                response.status_code, method, url, attempt, max_retries, backoff_seconds * attempt,
            )
            time.sleep(backoff_seconds * attempt)
            continue

        return response


# ---------------------------------------------------------------------------
# Databricks export
# ---------------------------------------------------------------------------

def list_all_notebooks(session: requests.Session, path: str = "/") -> list[dict]:
    resp = request_with_retry(
        session,
        "GET",
        f"{DATABRICKS_HOST}/api/2.0/workspace/list",
        headers=DATABRICKS_HEADERS,
        params={"path": path},
    )
    resp.raise_for_status()
    data = resp.json()
    items: list[dict] = []
    for obj in data.get("objects", []):
        if obj["object_type"] == "DIRECTORY":
            items.extend(list_all_notebooks(session, obj["path"]))
        elif obj["object_type"] == "NOTEBOOK":
            items.append(obj)
    return items


def export_notebook(session: requests.Session, path: str) -> str:
    resp = request_with_retry(
        session,
        "GET",
        f"{DATABRICKS_HOST}/api/2.0/workspace/export",
        headers=DATABRICKS_HEADERS,
        params={"path": path, "format": "SOURCE"},
    )
    resp.raise_for_status()
    data = resp.json()
    if "content" not in data:
        raise RuntimeError(f"Databricks export response missing 'content': {data}")
    return base64.b64decode(data["content"]).decode("utf-8")


# ---------------------------------------------------------------------------
# Databricks SOURCE -> Fabric .ipynb conversion
# ---------------------------------------------------------------------------

def convert_databricks_source_to_ipynb(source_text: str, notebook_name: str, lakehouse: Optional[dict] = None) -> str:
    is_sql_notebook = "-- Databricks notebook source" in source_text
    if is_sql_notebook:
        python_prefix, sql_source = source_text.split("-- Databricks notebook source", 1)
        python_cells = re.split(r"# COMMAND -{2,}\n?", python_prefix)
        sql_cells = re.split(r"-- COMMAND -{2,}\n?", sql_source)
        raw_cells = [
            (cell, False) for cell in python_cells if cell.strip()
        ] + [
            (cell, True) for cell in sql_cells if cell.strip()
        ]
    else:
        source_text = re.sub(r"^# Databricks notebook source\n?", "", source_text)
        raw_cells = [
            (cell, False)
            for cell in re.split(r"# COMMAND -{2,}\n?", source_text)
            if cell.strip()
        ]

    cells = []
    for raw_cell, is_sql_cell in raw_cells:
        lines = raw_cell.strip("\n").split("\n")
        cell_type = "code"
        magic_prefix = "%%sql\n" if is_sql_cell else ""
        is_parameters_cell = False

        if lines and lines[0].strip() == PARAMS_MARKER:
            is_parameters_cell = True
            lines = lines[1:]

        if lines and lines[0].strip().startswith("# MAGIC %"):
            magic = lines[0].strip().replace("# MAGIC ", "").strip()
            lines = lines[1:]
            lines = [
                l.replace("# MAGIC ", "", 1) if l.strip().startswith("# MAGIC") else l
                for l in lines
            ]
            if magic == "%md":
                cell_type = "markdown"
            elif magic == "%sql":
                magic_prefix = "%%sql\n"
            elif magic == "%run":
                magic_prefix = "%run "
            elif magic == "%pip":
                magic_prefix = "%pip "
            elif magic == "%scala":
                magic_prefix = "%%spark\n"
            else:
                magic_prefix = f"{magic}\n"

        cell_source = magic_prefix + "\n".join(lines).strip("\n")

        metadata = {"tags": ["parameters"]} if is_parameters_cell else {}
        cell = {
            "cell_type": cell_type,
            "metadata": metadata,
            "source": cell_source.splitlines(keepends=True),
        }
        if cell_type == "code":
            cell["execution_count"] = None
            cell["outputs"] = []
        cells.append(cell)

    trident = {}
    if lakehouse:
        lh_id = lakehouse["id"]
        trident["lakehouse"] = {
            "default_lakehouse": lh_id,
            "default_lakehouse_name": lakehouse["name"],
            "default_lakehouse_workspace_id": lakehouse["workspace_id"],
            "known_lakehouses": [{"id": lh_id}],
        }

    notebook_json = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "language_info": {"name": "python"},
            "kernelspec": {"name": "synapse_pyspark", "display_name": "Synapse PySpark"},
            **({"trident": trident} if trident else {}),
        },
        "cells": cells,
    }

    return json.dumps(notebook_json, indent=2)


# ---------------------------------------------------------------------------
# Fabric API
# ---------------------------------------------------------------------------

_folder_lock = threading.Lock()


def list_all_folders(session: requests.Session) -> list[dict]:
    resp = request_with_retry(
        session,
        "GET",
        f"{FABRIC_API_BASE}/workspaces/{FABRIC_WORKSPACE_ID}/folders",
        headers=FABRIC_HEADERS,
    )
    resp.raise_for_status()
    return resp.json().get("value", [])


def build_folder_map_from_existing(session: requests.Session) -> dict[str, str]:
    folders = list_all_folders(session)
    id_to_folder = {f["id"]: f for f in folders}

    def resolve_path(folder_id: str) -> str:
        folder = id_to_folder[folder_id]
        parent_id = folder.get("parentFolderId")
        if parent_id and parent_id in id_to_folder:
            return resolve_path(parent_id) + "/" + folder["displayName"]
        return "/" + folder["displayName"]

    return {resolve_path(f["id"]): f["id"] for f in folders}


def get_or_create_folder(
    session: requests.Session, folder_path: str, folder_id_map: dict[str, str]
) -> Optional[str]:
    with _folder_lock:
        if folder_path in folder_id_map:
            return folder_id_map[folder_path]

        parts = folder_path.strip("/").split("/")
        parent_id = None
        current_path = ""
        for part in parts:
            current_path += "/" + part
            if current_path in folder_id_map:
                parent_id = folder_id_map[current_path]
                continue
            body: dict[str, Any] = {"displayName": part}
            if parent_id:
                body["parentFolderId"] = parent_id
            resp = request_with_retry(
                session,
                "POST",
                f"{FABRIC_API_BASE}/workspaces/{FABRIC_WORKSPACE_ID}/folders",
                headers=FABRIC_HEADERS,
                json=body,
            )
            resp.raise_for_status()
            parent_id = resp.json()["id"]
            folder_id_map[current_path] = parent_id
        return parent_id


def create_fabric_notebook(
    session: requests.Session,
    display_name: str,
    folder_id: Optional[str],
    ipynb_content_str: str,
) -> requests.Response:
    content_b64 = base64.b64encode(ipynb_content_str.encode("utf-8")).decode("utf-8")
    body: dict[str, Any] = {
        "displayName": display_name,
        "definition": {
            "format": "ipynb",
            "parts": [
                {
                    "path": "notebook-content.ipynb",
                    "payload": content_b64,
                    "payloadType": "InlineBase64",
                }
            ],
        },
    }
    if folder_id:
        body["folderId"] = folder_id

    return request_with_retry(
        session,
        "POST",
        f"{FABRIC_API_BASE}/workspaces/{FABRIC_WORKSPACE_ID}/notebooks",
        headers=FABRIC_HEADERS,
        json=body,
    )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def _migrate_single_notebook(
    session: requests.Session,
    nb: dict,
    folder_id_map: dict[str, str],
    preprocess: bool,
    progress: str,
) -> tuple[str, bool, str]:
    folder_path = "/".join(nb["path"].split("/")[:-1])
    notebook_name = nb["path"].split("/")[-1]

    try:
        logger.info("%s Migrating: %s", progress, nb["path"])

        folder_id = folder_id_map.get(folder_path) or get_or_create_folder(session, folder_path, folder_id_map)
        raw_source = export_notebook(session, nb["path"])

        if preprocess:
            assert preprocess_notebook is not None  # guaranteed by the guard above
            raw_source = preprocess_notebook(
                raw_source,
                notebook_name,
                line_comment_patterns=COMMENT_OUT_PATTERNS,
                inject_parameters_cell=True,
                rewrite_raw_to_parquet=True,
            )

        ipynb_content = convert_databricks_source_to_ipynb(
            raw_source, notebook_name, lakehouse=DEFAULT_LAKEHOUSE
        )

        result = create_fabric_notebook(session, notebook_name, folder_id, ipynb_content)

        if result.status_code >= 400:
            err_msg = f"HTTP {result.status_code}: {result.text}"
            logger.error("%s FAILED: %s -> %s", progress, notebook_name, err_msg)
            return nb["path"], False, err_msg

        logger.info("%s OK: %s -> %d", progress, notebook_name, result.status_code)
        return nb["path"], True, ""
    except Exception as exc:  # noqa: BLE001 - report and continue with remaining notebooks
        logger.exception("%s FAILED: %s raised an exception: %s", progress, notebook_name, exc)
        return nb["path"], False, str(exc)


def migrate_all_notebooks(notebook_path: str = DATABRICKS_NOTEBOOK_PATH, preprocess: bool = True) -> None:
    """Migrate every Databricks notebook under `notebook_path` into Fabric.

    Set `preprocess=False` to migrate notebooks as-is. If `preprocess=True`
    but preprocess_notebook.py isn't importable, this raises immediately.
    """
    if preprocess and preprocess_notebook is None:
        raise RuntimeError(
            "preprocess=True but notebook_preprocessor.py was not found alongside this script. "
            "Place notebook_preprocessor.py next to migrate_notebook.py, or call with preprocess=False."
        )

    concurrency = max(1, NOTEBOOK_MIGRATE_CONCURRENCY)
    session = create_session(pool_size=max(20, concurrency * 4))

    start_time = time.monotonic()

    logger.info("Listing notebooks under Databricks path: %s", notebook_path)
    notebooks = list_all_notebooks(session, notebook_path)
    logger.info("Found %d notebook(s) total", len(notebooks))

    filtered_notebooks = [
        nb for nb in notebooks
        if not any(exclude in nb["path"] for exclude in DATABRICKS_EXCLUDE_PATH_CONTAINS)
    ]
    skipped_count = len(notebooks) - len(filtered_notebooks)
    logger.info(
        "%d notebook(s) to migrate after exclude filters %s (%d skipped)",
        len(filtered_notebooks), DATABRICKS_EXCLUDE_PATH_CONTAINS, skipped_count,
    )
    logger.info("Preprocessing is %s", "ENABLED" if preprocess else "DISABLED (migrating as-is)")

    logger.info("Fetching existing Fabric folder structure")
    folder_id_map = build_folder_map_from_existing(session)

    # Pre-resolve and pre-create required folders upfront so workers never collide
    distinct_folders = sorted({"/".join(nb["path"].split("/")[:-1]) for nb in filtered_notebooks})
    for fp in distinct_folders:
        if fp:
            get_or_create_folder(session, fp, folder_id_map)

    succeeded: list[str] = []
    failed: list[tuple[str, str]] = []

    logger.info("Starting notebook migration (concurrency=%d)...", concurrency)

    if concurrency > 1:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {}
            for index, nb in enumerate(filtered_notebooks, start=1):
                progress = f"[{index}/{len(filtered_notebooks)}]"
                fut = executor.submit(
                    _migrate_single_notebook,
                    session,
                    nb,
                    folder_id_map,
                    preprocess,
                    progress,
                )
                futures[fut] = nb["path"]

            for fut in as_completed(futures):
                path, is_success, msg = fut.result()
                if is_success:
                    succeeded.append(path)
                else:
                    failed.append((path, msg))
    else:
        for index, nb in enumerate(filtered_notebooks, start=1):
            progress = f"[{index}/{len(filtered_notebooks)}]"
            path, is_success, msg = _migrate_single_notebook(
                session, nb, folder_id_map, preprocess, progress
            )
            if is_success:
                succeeded.append(path)
            else:
                failed.append((path, msg))

    elapsed = time.monotonic() - start_time
    logger.info("=" * 60)
    logger.info("Migration summary")
    logger.info("  Total discovered : %d", len(notebooks))
    logger.info("  Skipped (excluded): %d", skipped_count)
    logger.info("  Attempted        : %d", len(filtered_notebooks))
    logger.info("  Succeeded        : %d", len(succeeded))
    logger.info("  Failed           : %d", len(failed))
    logger.info("  Elapsed          : %.1fs", elapsed)
    if failed:
        logger.info("Failed notebooks:")
        for path, reason in failed:
            logger.info("  - %s: %s", path, reason)
    logger.info("=" * 60)


if __name__ == "__main__":
    migrate_all_notebooks(preprocess=MIGRATE_PREPROCESS)

