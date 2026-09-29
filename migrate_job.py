"""Migrate Databricks Jobs (whose tasks are notebooks) into Fabric Data Pipelines.

Assumes the referenced notebooks have already been migrated into Fabric under the
same folder structure as Databricks (i.e. run migrate_notebook.py first).

Run this last, after migrate_table.py, migrate_data.py, and migrate_notebook.py.

Usage:
    # Migrate a single job:
    python migrate_job.py "Whiteoak_Details_Daily_Job_18"
    python migrate_job.py "Whiteoak_Details_Daily_Job_18" --prefix --folder "Test"

    # Migrate multiple jobs from a text file:
    python migrate_job.py jobs.txt
    python migrate_job.py --file jobs.txt --folder "Production"
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import re
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
logger = logging.getLogger("job_pipeline_migration")


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


DATABRICKS_HOST = _get_env("DATABRICKS_HOST", required=True).rstrip("/")
DATABRICKS_TOKEN = _get_env("DATABRICKS_TOKEN", required=True)

FABRIC_TOKEN = _get_env("FABRIC_TOKEN", required=True)
FABRIC_WORKSPACE_ID = _get_env("FABRIC_WORKSPACE_ID", required=True)
FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
PIPELINE_TARGET_FOLDER = _get_env("PIPELINE_TARGET_FOLDER", "")

DATABRICKS_HEADERS = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
FABRIC_HEADERS = {"Authorization": f"Bearer {FABRIC_TOKEN}", "Content-Type": "application/json"}

DEFAULT_TIMEOUT = (
    float(os.getenv("HTTP_CONNECT_TIMEOUT", "15")),
    float(os.getenv("HTTP_READ_TIMEOUT", "60")),
)
HTTP_MAX_RETRIES = int(os.getenv("HTTP_MAX_RETRIES", "3"))
HTTP_RETRY_BACKOFF = float(os.getenv("HTTP_RETRY_BACKOFF", "2.0"))


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
# Databricks jobs
# ---------------------------------------------------------------------------

def list_all_jobs(session: requests.Session) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    params: dict[str, Any] = {"limit": 100, "expand_tasks": True}
    while True:
        resp = request_with_retry(
            session,
            "GET",
            f"{DATABRICKS_HOST}/api/2.1/jobs/list",
            headers=DATABRICKS_HEADERS,
            params=params,
        )
        resp.raise_for_status()
        data = resp.json()
        jobs.extend(data.get("jobs", []))
        if not data.get("has_more"):
            break
        params["page_token"] = data["next_page_token"]
    return jobs


def get_full_job_detail(
    session: requests.Session,
    job_id: int,
    cache: Optional[dict[int, dict[str, Any]]] = None,
) -> dict[str, Any]:
    if cache is not None and job_id in cache:
        return cache[job_id]

    resp = request_with_retry(
        session,
        "GET",
        f"{DATABRICKS_HOST}/api/2.1/jobs/get",
        headers=DATABRICKS_HEADERS,
        params={"job_id": job_id},
    )
    resp.raise_for_status()
    detail = resp.json()
    if cache is not None:
        cache[job_id] = detail
    return detail


def normalize_notebook_path(path: str) -> str:
    """Jobs API paths are prefixed with /Workspace; the workspace-listing API isn't."""
    if path.startswith("/Workspace/"):
        return path[len("/Workspace"):]
    return path


def collect_notebook_paths_from_jobs(
    session: requests.Session,
    jobs: list[dict[str, Any]],
    cache: Optional[dict[int, dict[str, Any]]] = None,
) -> list[str]:
    """Gather every notebook_path referenced by notebook tasks across the given jobs."""
    paths: set[str] = set()
    for job in jobs:
        detail = get_full_job_detail(session, job["job_id"], cache=cache)
        for task in detail.get("settings", {}).get("tasks", []):
            notebook_task = task.get("notebook_task")
            if notebook_task:
                paths.add(normalize_notebook_path(notebook_task["notebook_path"]))
    return sorted(paths)


# ---------------------------------------------------------------------------
# Fabric folders / notebooks
# ---------------------------------------------------------------------------

def list_all_folders(session: requests.Session) -> list[dict[str, Any]]:
    resp = request_with_retry(
        session,
        "GET",
        f"{FABRIC_API_BASE}/workspaces/{FABRIC_WORKSPACE_ID}/folders",
        headers=FABRIC_HEADERS,
    )
    resp.raise_for_status()
    return resp.json().get("value", [])


def build_folder_maps(session: requests.Session) -> tuple[dict[str, str], dict[str, str]]:
    """Fetch folders once and return (path_to_id, id_to_path)."""
    folders = list_all_folders(session)
    id_to_folder = {f["id"]: f for f in folders}

    def _resolve(folder_id: str) -> str:
        folder = id_to_folder.get(folder_id)
        if not folder:
            return "/"
        parent_id = folder.get("parentFolderId")
        if parent_id and parent_id in id_to_folder:
            return _resolve(parent_id) + "/" + folder["displayName"]
        return "/" + folder["displayName"]

    id_to_path = {fid: _resolve(fid) for fid in id_to_folder}
    path_to_id = {path: fid for fid, path in id_to_path.items()}
    return path_to_id, id_to_path


def get_or_create_folder(
    session: requests.Session,
    folder_path: str,
    folder_id_map: dict[str, str],
) -> Optional[str]:
    cleaned = (folder_path or "").strip("/")
    if not cleaned:
        return None

    if folder_path in folder_id_map:
        return folder_id_map[folder_path]

    parts = cleaned.split("/")
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


def list_all_notebook_items(session: requests.Session) -> list[dict[str, Any]]:
    resp = request_with_retry(
        session,
        "GET",
        f"{FABRIC_API_BASE}/workspaces/{FABRIC_WORKSPACE_ID}/notebooks",
        headers=FABRIC_HEADERS,
    )
    resp.raise_for_status()
    return resp.json().get("value", [])


def reconstruct_notebook_id_map(
    session: requests.Session,
    original_databricks_paths: list[str],
    id_to_path: dict[str, str],
) -> dict[str, str]:
    """Map Databricks notebook paths to Fabric notebook IDs.

    Matches first by exact (folder_path, displayName). If not found at the
    expected path, falls back to searching by notebook name across all folders.
    """
    fabric_notebooks = list_all_notebook_items(session)

    # 1. Exact match lookup: (folder_path, displayName) -> id
    exact_lookup: dict[tuple[str, str], str] = {}
    # 2. Fallback lookup: displayName -> list of (folder_path, id)
    name_lookup: dict[str, list[tuple[str, str]]] = {}

    for nb in fabric_notebooks:
        folder_path = id_to_path.get(nb.get("folderId") or "", "/")
        nb_id = nb["id"]
        nb_name = nb["displayName"]

        exact_lookup[(folder_path, nb_name)] = nb_id
        name_lookup.setdefault(nb_name, []).append((folder_path, nb_id))

    notebook_id_map: dict[str, str] = {}
    for dbx_path in original_databricks_paths:
        clean = dbx_path.strip("/")
        parts = clean.split("/")
        name = parts[-1]
        folder_path = "/" + "/".join(parts[:-1]) if len(parts) > 1 else "/"
        key = (folder_path, name)

        if key in exact_lookup:
            notebook_id_map[dbx_path] = exact_lookup[key]
        elif name in name_lookup:
            candidates = name_lookup[name]
            if len(candidates) == 1:
                matched_folder, matched_id = candidates[0]
                logger.info(
                    "Notebook '%s' not found at exact path '%s', matched by name in folder '%s'",
                    name, folder_path, matched_folder,
                )
                notebook_id_map[dbx_path] = matched_id
            else:
                logger.warning(
                    "Notebook '%s' not found at '%s'. Multiple candidates exist in Fabric: %s",
                    name, folder_path, [c[0] for c in candidates],
                )
        else:
            logger.warning("Could not resolve notebook '%s' (path '%s') anywhere in Fabric", name, folder_path)

    return notebook_id_map


# ---------------------------------------------------------------------------
# Pipeline definition / creation
# ---------------------------------------------------------------------------

def sanitize_activity_name(name: str) -> str:
    """Fabric Data Pipeline activity names only allow letters, numbers, and underscores."""
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    if sanitized and sanitized[0].isdigit():
        sanitized = f"task_{sanitized}"
    return sanitized or "activity"


def build_fabric_pipeline_definition(
    job_detail: dict[str, Any],
    notebook_id_map: dict[str, str],
) -> dict[str, Any]:
    tasks = job_detail.get("settings", {}).get("tasks", [])
    valid_task_keys: dict[str, str] = {}  # task_key -> sanitized_activity_name

    # First pass: identify valid notebook tasks and map sanitized names
    for task in tasks:
        notebook_task = task.get("notebook_task")
        if not notebook_task:
            logger.warning(
                "Skipping non-notebook task '%s' (type: %s)",
                task.get("task_key"), [k for k in task if k.endswith("_task")],
            )
            continue

        nb_path = normalize_notebook_path(notebook_task["notebook_path"])
        if nb_path not in notebook_id_map:
            logger.warning("Skipping task '%s' — no Fabric notebook mapped for %s", task.get("task_key"), nb_path)
            continue

        valid_task_keys[task["task_key"]] = sanitize_activity_name(task["task_key"])

    activities = []
    for task in tasks:
        task_key = task.get("task_key")
        if task_key not in valid_task_keys:
            continue

        notebook_task = task["notebook_task"]
        nb_path = normalize_notebook_path(notebook_task["notebook_path"])
        fabric_notebook_id = notebook_id_map[nb_path]
        activity_name = valid_task_keys[task_key]

        # Only depend on activities that are actually present in the pipeline
        depends_on = []
        for dep in task.get("depends_on", []):
            dep_key = dep.get("task_key")
            if dep_key in valid_task_keys:
                depends_on.append({
                    "activity": valid_task_keys[dep_key],
                    "dependencyConditions": ["Succeeded"],
                })
            else:
                logger.warning(
                    "Task '%s': dropping dependency on '%s' (not present in Fabric pipeline)",
                    task_key, dep_key,
                )

        activity = {
            "name": activity_name,
            "type": "TridentNotebook",
            "dependsOn": depends_on,
            "typeProperties": {
                "notebookId": fabric_notebook_id,
                "workspaceId": FABRIC_WORKSPACE_ID,
                "parameters": {
                    k: {"value": v, "type": "string"}
                    for k, v in notebook_task.get("base_parameters", {}).items()
                },
            },
        }
        activities.append(activity)

    return {"properties": {"activities": activities}}


def create_fabric_pipeline(
    session: requests.Session,
    display_name: str,
    folder_id: Optional[str],
    pipeline_definition: dict[str, Any],
) -> requests.Response:
    payload_str = json.dumps(pipeline_definition)
    body: dict[str, Any] = {
        "displayName": display_name,
        "definition": {
            "parts": [{
                "path": "pipeline-content.json",
                "payload": base64.b64encode(payload_str.encode()).decode(),
                "payloadType": "InlineBase64",
            }],
        },
    }
    if folder_id:
        body["folderId"] = folder_id
    return request_with_retry(
        session,
        "POST",
        f"{FABRIC_API_BASE}/workspaces/{FABRIC_WORKSPACE_ID}/dataPipelines",
        headers=FABRIC_HEADERS,
        json=body,
    )


# ---------------------------------------------------------------------------
# File loading & End-to-end orchestration
# ---------------------------------------------------------------------------

def load_job_names_from_file(file_path: Path | str) -> list[str]:
    """Read a list of job names from a text file (one name per line)."""
    p = Path(file_path)
    if not p.is_file():
        raise FileNotFoundError(f"Jobs file not found: {p.resolve()}")
    lines = p.read_text(encoding="utf-8").splitlines()
    job_names: list[str] = []
    for line in lines:
        cleaned = line.strip()
        if not cleaned or cleaned.startswith("#"):
            continue
        job_names.append(cleaned)
    return job_names


def migrate_databricks_jobs(
    job_targets: list[str],
    folder_path: str = PIPELINE_TARGET_FOLDER,
    exact_match: bool = True,
) -> dict[str, Any]:
    """Migrate one or more Databricks jobs into Fabric Data Pipelines."""
    session = create_session(pool_size=20)
    start_time = time.monotonic()

    logger.info("Fetching Databricks jobs list...")
    all_jobs = list_all_jobs(session)
    logger.info("Discovered %d total job(s) in Databricks", len(all_jobs))

    job_details_cache: dict[int, dict[str, Any]] = {}
    matched_jobs_by_target: dict[str, list[dict[str, Any]]] = {}

    for target in job_targets:
        matched = [
            j for j in all_jobs
            if (j["settings"]["name"] == target if exact_match else j["settings"]["name"].startswith(target))
        ]
        matched_jobs_by_target[target] = matched
        if not matched:
            logger.warning("No job found matching target '%s'", target)

    # Flatten unique matched jobs
    all_matched_jobs: list[dict[str, Any]] = []
    seen_job_ids = set()
    for matched_list in matched_jobs_by_target.values():
        for job in matched_list:
            if job["job_id"] not in seen_job_ids:
                seen_job_ids.add(job["job_id"])
                all_matched_jobs.append(job)

    if not all_matched_jobs:
        logger.warning("No matching Databricks jobs found to migrate.")
        return {"total": len(job_targets), "succeeded": [], "failed": [], "skipped": []}

    logger.info(
        "Total matched unique job(s) to migrate: %d (across %d target pattern(s))",
        len(all_matched_jobs), len(job_targets),
    )

    # Folder resolution (single API call)
    logger.info("Fetching Fabric folder structure...")
    path_to_id, id_to_path = build_folder_maps(session)
    folder_id = get_or_create_folder(session, folder_path, path_to_id) if folder_path else None

    # Collect notebook paths across all matched jobs using cache
    logger.info("Resolving notebook references across all matched jobs...")
    notebook_paths = collect_notebook_paths_from_jobs(session, all_matched_jobs, cache=job_details_cache)
    notebook_id_map = reconstruct_notebook_id_map(session, notebook_paths, id_to_path)
    logger.info(
        "Resolved %d/%d notebook path(s) to Fabric notebook IDs",
        len(notebook_id_map), len(notebook_paths),
    )

    succeeded: list[str] = []
    failed: list[tuple[str, str]] = []
    skipped: list[tuple[str, str]] = []

    for index, job in enumerate(all_matched_jobs, start=1):
        job_name = job["settings"]["name"]
        job_id = job["job_id"]
        progress = f"[{index}/{len(all_matched_jobs)}]"
        logger.info("%s Processing job: '%s' (ID: %s)", progress, job_name, job_id)

        detail = get_full_job_detail(session, job_id, cache=job_details_cache)
        pipeline_def = build_fabric_pipeline_definition(detail, notebook_id_map)
        activity_count = len(pipeline_def["properties"]["activities"])
        task_count = len(detail.get("settings", {}).get("tasks", []))

        if activity_count == 0:
            msg = f"0/{task_count} task(s) mapped to Fabric notebook"
            logger.error("%s %s: %s — skipping pipeline creation", progress, job_name, msg)
            skipped.append((job_name, msg))
            continue

        if activity_count < task_count:
            logger.warning(
                "%s %s: only %d/%d task(s) mapped — pipeline will be created with partial activities",
                progress, job_name, activity_count, task_count,
            )

        resp = create_fabric_pipeline(session, job_name, folder_id, pipeline_def)
        if resp.status_code >= 400:
            err = f"HTTP {resp.status_code}: {resp.text}"
            logger.error("%s FAILED to create pipeline '%s' -> %s", progress, job_name, err)
            failed.append((job_name, err))
        else:
            logger.info("%s SUCCESS: Pipeline '%s' created -> HTTP %d", progress, job_name, resp.status_code)
            succeeded.append(job_name)

    elapsed = time.monotonic() - start_time
    logger.info("=" * 60)
    logger.info("Job to Pipeline Migration Summary")
    logger.info("  Target entries : %d", len(job_targets))
    logger.info("  Matched jobs   : %d", len(all_matched_jobs))
    logger.info("  Succeeded      : %d", len(succeeded))
    logger.info("  Failed         : %d", len(failed))
    logger.info("  Skipped        : %d", len(skipped))
    logger.info("  Elapsed        : %.1fs", elapsed)
    if failed:
        logger.info("Failed pipelines:")
        for name, reason in failed:
            logger.info("  - %s: %s", name, reason)
    if skipped:
        logger.info("Skipped jobs:")
        for name, reason in skipped:
            logger.info("  - %s: %s", name, reason)
    logger.info("=" * 60)

    return {
        "succeeded": succeeded,
        "failed": failed,
        "skipped": skipped,
        "elapsed": elapsed,
    }


def migrate_databricks_job_to_fabric_pipeline(
    job_name: str,
    folder_path: str = PIPELINE_TARGET_FOLDER,
    exact_match: bool = True,
) -> list[tuple[str, requests.Response]]:
    """Backward-compatible entry point for migrating a single job."""
    migrate_databricks_jobs([job_name], folder_path=folder_path, exact_match=exact_match)
    return []


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate Databricks job(s) into Fabric Data Pipeline(s)"
    )
    parser.add_argument(
        "job_name",
        nargs="?",
        default=None,
        help="Databricks job name, or path to a .txt file listing job names",
    )
    parser.add_argument(
        "--file", "-f",
        default=None,
        help="Path to a text file containing job names (one per line)",
    )
    parser.add_argument(
        "--prefix",
        action="store_true",
        help="Match jobs whose name starts with job_name",
    )
    parser.add_argument(
        "--folder",
        default=PIPELINE_TARGET_FOLDER,
        help="Target Fabric folder path",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    targets: list[str] = []
    if args.file:
        targets = load_job_names_from_file(args.file)
    elif args.job_name:
        p = Path(args.job_name)
        if args.job_name.endswith(".txt") or p.is_file():
            targets = load_job_names_from_file(p)
        else:
            targets = [args.job_name]
    else:
        logger.error(
            "Please provide a job name or a .txt file. "
            "Examples: python migrate_job.py my_job  OR  python migrate_job.py jobs.txt"
        )
        raise SystemExit(1)

    migrate_databricks_jobs(
        targets,
        folder_path=args.folder,
        exact_match=not args.prefix,
    )
