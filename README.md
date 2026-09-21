# Databricks → Microsoft Fabric Migration Toolkit

Standalone Python scripts to migrate a Databricks workspace (tables, data, notebooks,
and jobs) into Microsoft Fabric. Each script is independent and can be run on its
own, but together they form an end-to-end migration pipeline that should be run in
the order below.

```
migrate_table.py  →  migrate_data.py  →  migrate_notebook.py  →  migrate_job.py
 (schemas/DDL)        (row data)          (notebooks)             (job pipelines)
```

## Contents

| File | Purpose |
|---|---|
| [migrate_table.py](migrate_table.py) | Creates Fabric Lakehouse schemas/tables matching Databricks Unity Catalog table structures. |
| [migrate_data.py](migrate_data.py) | Loads data into those tables via `INSERT INTO/OVERWRITE ... SELECT * FROM DELTA.\`Files/...\`` using OneLake shortcuts. |
| [migrate_notebook.py](migrate_notebook.py) | Exports Databricks notebooks and recreates them as Fabric notebooks, mirroring the folder structure. |
| [notebook_preprocessor.py](notebook_preprocessor.py) | Optional rewrite rules used by `migrate_notebook.py` (parameter-cell injection, PARQUET path rewriting, legacy widget comment-out, etc.). |
| [migrate_job.py](migrate_job.py) | Recreates Databricks Jobs (whose tasks are notebooks) as Fabric Data Pipelines pointing at the migrated notebooks. |

## Setup

1. Copy `.env.example` to `.env` in this folder and fill in the required values:
   ```
   DATABRICKS_HOST=
   DATABRICKS_TOKEN=
   FABRIC_TOKEN=
   FABRIC_WORKSPACE_ID=
   FABRIC_LAKEHOUSE_ID=
   ```
   Every script auto-loads `.env` from its own directory (without overriding
   real environment variables already set).
2. Install dependencies: `pip install requests`.

## Flow: start to finish

### 1. Migrate table (schemas & structures)

```
python migrate_table.py
```

Reads column metadata from `system.information_schema.columns` for the catalogs
in `DATABRICKS_CATALOGS`, groups columns into tables, and generates/executes:

- `CREATE SCHEMA IF NOT EXISTS ...`
- `CREATE TABLE IF NOT EXISTS ... USING DELTA`

against a Fabric Lakehouse via a Livy Spark session. Generated DDL is also written
to `DDL_OUTPUT_PATH` (default `migration_ddls.sql`) for review. After creation, it
polls Fabric to verify every expected schema/table actually became visible before
exiting.

Key env vars: `DATABRICKS_CATALOGS`, `DATABRICKS_SCHEMA_INCLUDE` (regex),
`DATABRICKS_SCHEMA_EXCLUDE`, `DATABRICKS_TABLE_EXCLUDE_REGEXP`,
`EXECUTE_SCHEMA_CREATION`, `EXECUTE_TABLE_CREATION`.

**Prerequisite for the next step:** manually create a OneLake shortcut in Fabric
pointing at each Databricks Unity Catalog storage location you plan to load data
from (`abfss://<catalog>@<account>.dfs.core.windows.net/...`), so the underlying
Delta files are reachable from Fabric as `Files/<path>`.

### 2. Migrate data (row data)

```
python migrate_data.py
```

Queries `system.information_schema.tables` for each table's `storage_path`,
converts it into a Fabric `Files/<path>` reference, and builds:

```sql
INSERT INTO {schema}.{table} SELECT * FROM DELTA.`Files/{delta_path}`
```

(or `INSERT OVERWRITE` if `DATA_INSERT_MODE=OVERWRITE`). Statements are written to
`INSERT_OUTPUT_PATH` (default `migration_inserts.sql`) and, unless
`EXECUTE_DATA_INSERT=false`, executed one-by-one through Fabric Livy (a failure on
one table doesn't stop the rest).

> `INSERT INTO` is **not idempotent** — re-running appends duplicate rows. Use
> `DATA_INSERT_MODE=OVERWRITE` if you need to safely re-run the migration.

Key env vars: `DATA_INSERT_MODE` (`INTO`/`OVERWRITE`), `INSERT_OUTPUT_PATH`,
`EXECUTE_DATA_INSERT`.

### 3. Migrate notebook (Databricks notebooks → Fabric notebooks)

```
python migrate_notebook.py
```

Lists every notebook under `DATABRICKS_NOTEBOOK_PATH`, skips any whose path
contains a substring in `DATABRICKS_EXCLUDE_PATH_CONTAINS`, exports each one in
`SOURCE` format, optionally preprocesses it (see below), converts it to Fabric
`.ipynb` JSON, and creates it in Fabric under a mirrored folder structure attached
to `FABRIC_LAKEHOUSE_ID`.

**Preprocessing** (enabled by default, `MIGRATE_PREPROCESS=true`) is delegated to
[notebook_preprocessor.py](notebook_preprocessor.py), which:
- Injects a parameters cell (`year`, `month`, `day`, `folder_identifier`, `payload_filter`).
- Rewrites raw-table + file-path-date filter queries into `PARQUET.\`Files/...\`` paths.
- Comments out legacy `dbutils.widgets...` calls and `USE CATALOG`/`USE <db>` statements.
- Normalizes `spark.sql("""...` to an f-string, and `USING (SELECT * FROM (` to
  `USING (SELECT * EXCEPT (rn) FROM (` when a row-number alias is present.

To migrate notebooks completely as-is, set `MIGRATE_PREPROCESS=false` (or call
`migrate_all_notebooks(preprocess=False)` directly) — `notebook_preprocessor.py`
doesn't even need to be present in that case. If `notebook_preprocessor.py` is
missing and preprocessing is left enabled, the script raises immediately with a
clear error instead of silently skipping the rewrites.

You can also use just the preprocessing logic on its own, e.g. to inspect what a
specific notebook will look like after the rewrite rules, without touching the
network:
```python
from notebook_preprocessor import preprocess_notebook
print(preprocess_notebook(raw_source_text, "my_notebook", inject_parameters_cell=True))
```

Key env vars: `DATABRICKS_NOTEBOOK_PATH`, `DATABRICKS_EXCLUDE_PATH_CONTAINS`,
`MIGRATE_PREPROCESS`, `FABRIC_LAKEHOUSE_ID`, `FABRIC_LAKEHOUSE_NAME`,
`MAPPING_DB_DWH`/`MAPPING_DB_CE`/`MAPPING_DB_DA` (used by the PARQUET path rewrite).

### 4. Migrate job (Databricks Jobs → Fabric Data Pipelines)

```
python migrate_job.py "My_Job_Name"
python migrate_job.py "My_Job_Prefix" --prefix --folder "Test"
```

Requires the notebooks referenced by the job's tasks to already exist in Fabric
under the same folder structure (step 3). For a matched job, it:
1. Resolves/creates the target Fabric folder (`PIPELINE_TARGET_FOLDER`, default empty/workspace root).
2. Maps each task's Databricks `notebook_path` to its Fabric notebook ID.
3. Builds a pipeline definition with one `TridentNotebook` activity per notebook
   task (respecting `dependsOn` from the original job), skipping non-notebook
   tasks with a warning.
4. Creates the pipeline in Fabric — but only if at least one task resolved to an
   activity; if none did, it logs an error and skips creation instead of creating
   an empty pipeline.

Can also be used as a library:
```python
from migrate_job import migrate_databricks_job_to_fabric_pipeline
migrate_databricks_job_to_fabric_pipeline("My_Job_Name", folder_path="")
```

Key env vars: `PIPELINE_TARGET_FOLDER`.

## Troubleshooting

- **"Resolved X/Y notebook path(s)"** logged lower than expected in `migrate_job.py`
  means some Databricks notebook paths didn't find a matching Fabric notebook —
  check for `Could not resolve ...` warnings (folder path or display name mismatch).
- **Pipeline created but empty in the Fabric UI** — check the logs right above pipeline
  creation for `Skipping task '...'` warnings; it means 0 tasks mapped to a Fabric
  notebook, so the pipeline is skipped entirely (not silently created empty).
- All scripts log a summary at the end (counts of succeeded/failed/skipped items).
  Set `LOG_LEVEL=DEBUG` for step-by-step detail.
