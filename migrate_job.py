"""Migrate Databricks Jobs (whose tasks are notebooks) into Fabric Data Pipelines.

Assumes the referenced notebooks have already been migrated into Fabric under the
same folder structure as Databricks (i.e. run migrate_notebook.py first).

Run this last, after migrate_table.py, migrate_data.py, and migrate_notebook.py.

Usage:
    python migrate_job.py "Whiteoak_Details_Daily_Job_18"
    python migrate_job.py "Whiteoak_Details_Daily_Job_18" --prefix --folder "Test"
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
from pathlib import Path
from typing import Any, Optional

import requests

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


# ---------------------------------------------------------------------------
# Databricks jobs
# ---------------------------------------------------------------------------

def list_all_jobs() -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    params: dict[str, Any] = {"limit": 100, "expand_tasks": True}
    while True:
        resp = requests.get(
            f"{DATABRICKS_HOST}/api/2.1/jobs/list",
            headers=DATABRICKS_HEADERS, params=params,
        ).json()
        jobs.extend(resp.get("jobs", []))
        if not resp.get("has_more"):
            break
        params["page_token"] = resp["next_page_token"]
    return jobs


def get_full_job_detail(job_id: int) -> dict[str, Any]:
    return requests.get(
        f"{DATABRICKS_HOST}/api/2.1/jobs/get",
        headers=DATABRICKS_HEADERS, params={"job_id": job_id},
    ).json()


def normalize_notebook_path(path: str) -> str:
    """Jobs API paths are prefixed with /Workspace; the workspace-listing API isn't."""
    if path.startswith("/Workspace/"):
        return path[len("/Workspace"):]
    return path


def collect_notebook_paths_from_jobs(jobs: list[dict[str, Any]]) -> list[str]:
    """Gather every notebook_path referenced by notebook tasks across the given jobs."""
    paths: set[str] = set()
    for job in jobs:
        detail = get_full_job_detail(job["job_id"])
        for task in detail["settings"]["tasks"]:
            notebook_task = task.get("notebook_task")
            if notebook_task:
                paths.add(normalize_notebook_path(notebook_task["notebook_path"]))
    return sorted(paths)


# ---------------------------------------------------------------------------
# Fabric folders / notebooks
# ---------------------------------------------------------------------------

def list_all_folders() -> list[dict[str, Any]]:
    resp = requests.get(
        f"{FABRIC_API_BASE}/workspaces/{FABRIC_WORKSPACE_ID}/folders",
        headers=FABRIC_HEADERS,
    ).json()
    return resp.get("value", [])


def _resolve_folder_path(folder_id: str, id_to_folder: dict[str, dict[str, Any]]) -> str:
    folder = id_to_folder[folder_id]
    parent_id = folder.get("parentFolderId")
    if parent_id and parent_id in id_to_folder:
        return _resolve_folder_path(parent_id, id_to_folder) + "/" + folder["displayName"]
    return "/" + folder["displayName"]


def build_folder_map_from_existing() -> dict[str, str]:
    folders = list_all_folders()
    id_to_folder = {f["id"]: f for f in folders}
    return {_resolve_folder_path(f["id"], id_to_folder): f["id"] for f in folders}


def get_or_create_folder(folder_path: str, folder_id_map: dict[str, str]) -> Optional[str]:
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
        resp = requests.post(
            f"{FABRIC_API_BASE}/workspaces/{FABRIC_WORKSPACE_ID}/folders",
            headers=FABRIC_HEADERS, json=body,
        ).json()
        parent_id = resp["id"]
        folder_id_map[current_path] = parent_id
    return parent_id


def list_all_notebook_items() -> list[dict[str, Any]]:
    resp = requests.get(
        f"{FABRIC_API_BASE}/workspaces/{FABRIC_WORKSPACE_ID}/notebooks",
        headers=FABRIC_HEADERS,
    ).json()
    return resp.get("value", [])


def build_folder_path_lookup() -> dict[str, str]:
    folders = list_all_folders()
    id_to_folder = {f["id"]: f for f in folders}
    return {fid: _resolve_folder_path(fid, id_to_folder) for fid in id_to_folder}


def reconstruct_notebook_id_map(original_databricks_paths: list[str]) -> dict[str, str]:
    fabric_notebooks = list_all_notebook_items()
    folder_paths = build_folder_path_lookup()

    fabric_lookup: dict[tuple[str, str], str] = {}
    for nb in fabric_notebooks:
        folder_path = folder_paths.get(nb.get("folderId") or "", "/")
        fabric_lookup[(folder_path, nb["displayName"])] = nb["id"]

    notebook_id_map: dict[str, str] = {}
    for dbx_path in original_databricks_paths:
        folder_path = "/" + "/".join(dbx_path.strip("/").split("/")[:-1])
        name = dbx_path.strip("/").split("/")[-1]
        key = (folder_path, name)
        if key in fabric_lookup:
            notebook_id_map[dbx_path] = fabric_lookup[key]
        else:
            logger.warning("Could not resolve %s — check for auto-suffixed name", dbx_path)

    return notebook_id_map


# ---------------------------------------------------------------------------
# Pipeline definition / creation
# ---------------------------------------------------------------------------

def build_fabric_pipeline_definition(job_detail: dict[str, Any], notebook_id_map: dict[str, str]) -> dict[str, Any]:
    tasks = job_detail["settings"]["tasks"]
    activities = []

    for task in tasks:
        notebook_task = task.get("notebook_task")
        if not notebook_task:
            logger.warning(
                "Skipping non-notebook task '%s' (type: %s)",
                task["task_key"], [k for k in task if k.endswith("_task")],
            )
            continue

        nb_path = normalize_notebook_path(notebook_task["notebook_path"])
        if nb_path not in notebook_id_map:
            logger.warning("Skipping task '%s' — no Fabric notebook mapped for %s", task["task_key"], nb_path)
            continue
        fabric_notebook_id = notebook_id_map[nb_path]

        activity = {
            "name": task["task_key"],
            "type": "TridentNotebook",
            "dependsOn": [
                {"activity": dep["task_key"], "dependencyConditions": ["Succeeded"]}
                for dep in task.get("depends_on", [])
            ],
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


def create_fabric_pipeline(display_name: str, folder_id: Optional[str], pipeline_definition: dict[str, Any]) -> requests.Response:
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
    return requests.post(
        f"{FABRIC_API_BASE}/workspaces/{FABRIC_WORKSPACE_ID}/dataPipelines",
        headers=FABRIC_HEADERS, json=body,
    )


# ---------------------------------------------------------------------------
# End-to-end orchestration
# ---------------------------------------------------------------------------

def migrate_databricks_job_to_fabric_pipeline(
    job_name: str,
    folder_path: str = PIPELINE_TARGET_FOLDER,
    exact_match: bool = True,
) -> list[tuple[str, requests.Response]]:
    """End-to-end migration of Databricks job(s) into Fabric pipeline(s).

    Set `exact_match=False` to match all jobs whose name starts with `job_name`.
    """
    logger.info("Fetching Databricks jobs...")
    jobs = list_all_jobs()

    matched_jobs = [
        job for job in jobs
        if (job["settings"]["name"] == job_name if exact_match else job["settings"]["name"].startswith(job_name))
    ]
    if not matched_jobs:
        logger.warning("No job found matching '%s'", job_name)
        return []

    logger.info("Found %d job(s) matching '%s'", len(matched_jobs), job_name)

    folder_id_map = build_folder_map_from_existing()
    folder_id = get_or_create_folder(folder_path, folder_id_map)

    notebook_paths = collect_notebook_paths_from_jobs(matched_jobs)
    notebook_id_map = reconstruct_notebook_id_map(notebook_paths)
    logger.info(
        "Resolved %d/%d notebook path(s) to Fabric notebook IDs",
        len(notebook_id_map), len(notebook_paths),
    )

    results: list[tuple[str, requests.Response]] = []
    for job in matched_jobs:
        detail = get_full_job_detail(job["job_id"])
        pipeline_def = build_fabric_pipeline_definition(detail, notebook_id_map)
        activity_count = len(pipeline_def["properties"]["activities"])
        task_count = len(detail["settings"]["tasks"])

        if activity_count == 0:
            logger.error(
                "%s: 0/%d task(s) mapped to a Fabric notebook — skipping pipeline creation "
                "(see 'Skipping task'/'Could not resolve' warnings above for the reason)",
                job["settings"]["name"], task_count,
            )
            continue
        if activity_count < task_count:
            logger.warning(
                "%s: only %d/%d task(s) mapped — pipeline will be created with missing activities",
                job["settings"]["name"], activity_count, task_count,
            )

        result = create_fabric_pipeline(job["settings"]["name"], folder_id, pipeline_def)
        logger.info("%s -> %d %s", job["settings"]["name"], result.status_code, result.text)
        results.append((job["settings"]["name"], result))

    return results


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate a Databricks job into a Fabric pipeline")
    parser.add_argument("job_name", help="Databricks job name (or prefix, with --prefix)")
    parser.add_argument("--prefix", action="store_true", help="Match jobs whose name starts with job_name")
    parser.add_argument("--folder", default=PIPELINE_TARGET_FOLDER, help="Target Fabric folder path")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    migrate_databricks_job_to_fabric_pipeline(args.job_name, folder_path=args.folder, exact_match=not args.prefix)
