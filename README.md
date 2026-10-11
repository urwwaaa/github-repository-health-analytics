# GitHub Repository Health Analytics

Data engineering project for analyzing the health and development activity of open-source GitHub repositories using Apache Spark, Medallion Architecture, and Power BI.

**GitHub repository:** <https://github.com/urwwaaa/github-repository-health-analytics>

## Project Concept

This project analyzes the development activity and maintenance patterns of three major open-source GitHub repositories.

The repositories selected are:

- `apache/spark`
- `pandas-dev/pandas`
- `scikit-learn/scikit-learn`

The analysis focuses on:

- Commits
- Issues
- Pull Requests
- Contributor activity
- Repository metadata (stars, forks, subscribers)

## Data Source

The project uses the GitHub REST API to acquire public repository data.

## Ingestion Pattern

### Full Load

For the initial load, the project collects approximately one year of historical GitHub activity for the selected repositories.

Planned historical period: October 2025 to September 2026.

The full load includes:

- Commits
- Issues
- Pull Requests
- Contributor activity
- Repository metadata

### Incremental Load

After the initial full load, the pipeline runs periodically and fetches only new or updated records from the GitHub REST API.

A timestamp watermark is maintained so the pipeline can continue from the last successful ingestion time instead of reloading the entire history.

---

## Phase 2 — Bronze & Silver Data Dictionary

### 1. Project Overview

This project analyzes the health and activity of three open-source GitHub repositories:

- `apache/spark`
- `pandas-dev/pandas`
- `scikit-learn/scikit-learn`

**Data source:** GitHub REST API  
**Processing framework:** Apache Spark (PySpark) on Databricks  
**Storage:** Delta Lake using the Medallion Architecture  
**Historical load date:** October 8, 2026

The pipeline processes five entities: commits, issues, pull requests, repository metadata, and contributors.

| Layer | Tables | Current Records |
|---|---:|---:|
| Bronze | 5 | 30,270 |
| Silver | 5 | 30,270 |

Bronze preserves parsed source data and ingestion lineage. Silver provides cleaned, typed data for analytics.

### 2. Data Types and Primary Keys

- `STRING`: Text values.
- `LONG`: 64-bit integer values, equivalent to BIGINT.
- `BOOLEAN`: True or false.
- `TIMESTAMP`: Date and time value.

All tables use `record_key` as their **logical primary key**.

Uniqueness is maintained through validation, deduplication, and Delta MERGE operations. Unity Catalog reports the columns as nullable, so uniqueness and non-null business requirements are enforced by the pipeline rather than database primary-key constraints.

### 3. Bronze Layer

**Database schema:** `github_health.bronze`

The following nine columns are shared by all five Bronze tables.

| Column | Data Type | Description |
|---|---|---|
| record_key | STRING | Logical primary key identifying the source record |
| repository | STRING | Repository name in owner/repository format |
| batch_id | STRING | Identifier of the ingestion batch |
| source_file | STRING | Path of the original JSONL file |
| source_updated_at | TIMESTAMP | Source timestamp used for update tracking |
| raw_payload | STRING | JSON payload retained for downstream processing |
| payload_hash | STRING | Hash used to detect data changes |
| load_timestamp | TIMESTAMP | Time the stored Bronze record was last inserted or changed |
| ingested_at | TIMESTAMP | Time the source record was collected |

#### 3.1 bronze.commits

**Table:** `github_health.bronze.commits`

**Grain:** One record per unique repository and commit SHA.

**Logical primary key:** `record_key`

**Purpose:** Stores commit information from GitHub for subsequent transformation into Silver. Its nine columns follow the shared Bronze schema above.

**Verified record count:** 8,610.

#### 3.2 bronze.issues

**Table:** `github_health.bronze.issues`

**Grain:** One record per unique GitHub issue.

**Logical primary key:** `record_key`

**Purpose:** Stores issue information, including source states and timestamps, for Silver processing. Its nine columns follow the shared Bronze schema above.

**Verified record count:** 4,351.

#### 3.3 bronze.pulls

**Table:** `github_health.bronze.pulls`

**Grain:** One record per unique GitHub pull request.

**Logical primary key:** `record_key`

**Purpose:** Stores pull-request source information for Silver processing. Its nine columns follow the shared Bronze schema above.

**Verified record count:** 16,155.

#### 3.4 bronze.repo_metadata

**Table:** `github_health.bronze.repo_metadata`

**Grain:** One current metadata record per GitHub repository.

**Logical primary key:** `record_key`, derived from the repository's unique GitHub ID.

**Purpose:** Stores repository metadata, including popularity metrics and activity information, along with source lineage and snapshot collection time. Its nine columns follow the shared Bronze schema above.

**Verified record count:** 3.

#### 3.5 bronze.contributors

**Table:** `github_health.bronze.contributors`

**Grain:** One current contributor record per repository and GitHub contributor ID.

**Logical primary key:** `record_key`, constructed as `repository | contributor_id`.

**Purpose:** Stores contributor snapshots from the GitHub REST API. Its nine columns follow the shared Bronze schema above.

**Verified record count:** 1,151.

**Business fields used for change detection:** `id`, `type`, `site_admin`, and `contributions`.

### 4. Silver Layer

**Database schema:** `github_health.silver`

Silver tables contain clean, analytical fields with explicit data types.

#### 4.1 silver.commits

**Table:** `github_health.silver.commits`

**Grain:** One unique commit per repository.

**Logical primary key:** `record_key`

| Column | Data Type | Description |
|---|---|---|
| record_key | STRING | Unique logical commit key |
| repository | STRING | Repository name |
| commit_sha | STRING | Unique Git commit hash |
| author_user_id | LONG | GitHub user ID of the commit author, where available |
| commit_timestamp | TIMESTAMP | Time of the commit |
| message | STRING | Commit message |
| batch_id | STRING | Processing batch ID |
| source_updated_at | TIMESTAMP | Source timestamp for update tracking |
| payload_hash | STRING | Hash used to identify changed commit data |
| load_timestamp | TIMESTAMP | Time the Silver record was inserted or updated |

**Purpose:** Enables analysis of commit activity, contribution patterns, and development trends.

**Verified record count:** 8,610.

#### 4.2 silver.issues

**Table:** `github_health.silver.issues`

**Grain:** One unique issue per repository.

**Logical primary key:** `record_key`

| Column | Data Type | Description |
|---|---|---|
| record_key | STRING | Unique logical issue key |
| repository | STRING | Repository name |
| issue_id | LONG | GitHub issue ID |
| issue_number | LONG | Issue number within the repository |
| author_user_id | LONG | GitHub user ID of the issue creator |
| title | STRING | Issue title |
| state | STRING | Current issue state |
| created_at | TIMESTAMP | Issue creation time |
| updated_at | TIMESTAMP | Last update reported by GitHub |
| closed_at | TIMESTAMP | Issue closing time, if applicable |
| batch_id | STRING | Processing batch ID |
| source_updated_at | TIMESTAMP | Source timestamp for update tracking |
| payload_hash | STRING | Hash used to identify issue changes |
| load_timestamp | TIMESTAMP | Time the Silver record was inserted or updated |

**Purpose:** Supports analysis of issue volume, open versus closed issues, and resolution activity.

**Verified record count:** 4,351.

#### 4.3 silver.pulls

**Table:** `github_health.silver.pulls`

**Grain:** One unique pull request per repository.

**Logical primary key:** `record_key`

| Column | Data Type | Description |
|---|---|---|
| record_key | STRING | Unique logical pull-request key |
| repository | STRING | Repository name |
| pr_id | LONG | GitHub pull-request ID |
| pr_number | LONG | Pull-request number within the repository |
| author_user_id | LONG | GitHub user ID of the PR creator |
| title | STRING | Pull-request title |
| state | STRING | Pull-request state |
| draft | BOOLEAN | Whether the pull request is a draft |
| created_at | TIMESTAMP | Pull-request creation time |
| updated_at | TIMESTAMP | Last update reported by GitHub |
| closed_at | TIMESTAMP | Closing time, if applicable |
| merged_at | TIMESTAMP | Merge time, if applicable |
| batch_id | STRING | Processing batch ID |
| source_updated_at | TIMESTAMP | Source timestamp for update tracking |
| payload_hash | STRING | Hash used to identify pull-request changes |
| load_timestamp | TIMESTAMP | Time the Silver record was inserted or updated |

**Purpose:** Supports analysis of pull-request activity, review outcomes, and development workflow.

**Verified record count:** 16,155.

#### 4.4 silver.repo_metadata

**Table:** `github_health.silver.repo_metadata`

**Grain:** One current metadata snapshot per repository.

**Logical primary key:** `record_key`

| Column | Data Type | Description |
|---|---|---|
| record_key | STRING | Unique logical repository key |
| repository | STRING | Full repository name |
| repo_id | LONG | Unique GitHub repository ID |
| description | STRING | Repository description |
| html_url | STRING | GitHub repository URL |
| language | STRING | Primary programming language |
| default_branch | STRING | Default Git branch |
| stars_count | LONG | Number of GitHub stars |
| forks_count | LONG | Number of repository forks |
| open_issues_count | LONG | Number of open issues reported by GitHub |
| subscribers_count | LONG | Number of repository subscribers |
| archived | BOOLEAN | Whether the repository is archived |
| created_at | TIMESTAMP | Repository creation time |
| updated_at | TIMESTAMP | GitHub repository update timestamp |
| pushed_at | TIMESTAMP | Last push time reported by GitHub |
| snapshot_at | TIMESTAMP | Time the metadata snapshot was collected |
| batch_id | STRING | Processing batch ID |
| source_updated_at | TIMESTAMP | Source update timestamp |
| payload_hash | STRING | Hash for business-data change detection |
| load_timestamp | TIMESTAMP | Time the Silver record was inserted or updated |

**Purpose:** Provides repository statistics for comparisons involving stars, forks, open issues, subscribers, and repository activity.

**Verified record count:** 3.

#### 4.5 silver.contributors

**Table:** `github_health.silver.contributors`

**Grain:** One current contributor record per repository and GitHub contributor ID.

**Logical primary key:** `record_key`

| Column | Data Type | Description |
|---|---|---|
| record_key | STRING | Logical primary key |
| repository | STRING | GitHub repository name |
| contributor_id | LONG | Numeric GitHub contributor ID |
| contributions | LONG | Contributions reported for the repository |
| account_type | STRING | GitHub account category, such as User or Bot |
| snapshot_at | TIMESTAMP | Collection time of the snapshot |
| batch_id | STRING | Source batch identifier |
| payload_hash | STRING | Hash used to detect business-data changes |
| load_timestamp | TIMESTAMP | Time the stored Silver record was last written |

**Privacy:** GitHub usernames and profile URLs are not exposed as separate Silver columns. Numeric contributor IDs are retained for joining and analysis and remain publicly linkable identifiers.

**Verified record count:** 1,151.

### 5. Data Quality and Validation

The pipeline applies the following controls:

**Explicit schema enforcement:** Raw JSON is read using declared PySpark `StructType` and `StructField` definitions, rather than automatic schema inference.

**Business-key validation:** Required identifiers are checked, and records are deduplicated using their logical `record_key`.

**Timestamp validation:** Required timestamps are checked before records are accepted.

**Quarantine:** Invalid records are separated from valid records and recorded in the appropriate quarantine tables.

**Idempotency:** Reprocessing an identical batch does not create duplicate records or unnecessarily modify unchanged business data.

**Change detection:** `payload_hash` identifies changed business values.

**Historical replay protection:** The pipeline guards against older source records replacing newer accepted records.

**Repository metadata snapshots:** Metadata MERGE uses the collection timestamp (`ingested_at` in Bronze and `snapshot_at` in Silver), because repository popularity metrics can change without a corresponding change to GitHub's `updated_at`.

**Auditing:** Processing status, row counts, upserts, quarantined records, and failures are recorded in execution logs.

### 6. Verification Results

| Entity | Bronze Records | Silver Records |
|---|---:|---:|
| Commits | 8,610 | 8,610 |
| Issues | 4,351 | 4,351 |
| Pull Requests | 16,155 | 16,155 |
| Repository Metadata | 3 | 3 |
| Contributors | 1,151 | 1,151 |
| **Total** | **30,270** | **30,270** |

Verified checks include:

- Unique `record_key` values in the tested tables.
- No null `load_timestamp` values.
- Successful historical and incremental processing of the core datasets.
- Successful metadata full-load processing with zero quarantined records.
- Metadata identical-batch replay with zero inserts and zero updates.
- Metadata Delta-version comparison confirming unchanged keys, hashes, and load timestamps.
- Historical backfill tests confirming preservation of the existing core data and RAW checkpoints.
- Contributors load with zero quarantined records and zero duplicate keys.
- Contributors idempotency: reprocessing the same batch produced zero inserts and zero updates.

### 7. Current Scope and Limitations

The Bronze and Silver layers support five entities: commits, issues, pull requests, repository metadata, and contributors.

The metadata tables keep the latest accepted snapshot for each repository. They do not yet store a complete historical time series of popularity metrics.

The Gold analytical layer and final dashboard belong to the subsequent project phase.

---

## How to Run the Pipeline

### Prerequisites

1. Run the **`00_setup`** notebook once to create the `github_health` catalog, schemas (`raw`, `ops`, `bronze`, `silver`), and operations tables (`pipeline_execution_logs`, `pipeline_watermarks`, `bronze_quarantine`, `silver_quarantine`).
2. Store your GitHub token as a Databricks secret: scope `github`, key `token`. Alternatively, set the `GITHUB_TOKEN` environment variable.

### Running a Full Historical Load

1. Open the **`full_load`** notebook and click **Run All**. This fetches one year of history from the GitHub API for all repositories and writes JSONL files to the raw volume. The notebook prints a `batch_id` at the top of its output — copy it.
2. Open the **`Initialize_Raw_Watermarks`** notebook and run it to set the initial RAW watermark timestamps.
3. Open the **`driver_session`** notebook and set the following widgets:
   - `batch_id`: the UUID printed by `full_load`
   - `run_date`: the load date (e.g., `2026-10-08`)
   - `load_type`: `FULL`
   - `manifest_path`: leave blank (the driver finds the manifest automatically)
   - `run_scope`: `CORE_ONLY` for commits/issues/pulls, `METADATA_ONLY` for repository metadata, `CONTRIBUTORS_ONLY` for contributors, or `ALL` for everything.
4. Click **Run All** on `driver_session`. Bronze processes first, then Silver. Execution results appear at the bottom.

### Running a Normal Incremental Load

1. Open the **`incremental_api_ingestion`** notebook and set the widgets:
   - `batch_id`: a unique identifier for this batch (e.g., `incremental_20261009_001`)
   - `run_date`: the date of the run (e.g., `2026-10-09`)
   - `window_end_utc`: leave blank to use the current time, or set an explicit end timestamp.
2. Click **Run All**. The notebook reads RAW watermarks, fetches only new/updated records from the GitHub API, writes JSONL files, publishes a manifest, and advances the watermarks.
3. Open the **`driver_session`** notebook and set the widgets:
   - `batch_id`: the same `batch_id` used in step 1
   - `run_date`: the same `run_date`
   - `load_type`: `INCREMENTAL`
   - `manifest_path`: leave blank
   - `run_scope`: `CORE_ONLY`
4. Click **Run All** on `driver_session` to load the new records through Bronze and Silver.

### Running a Backfill for an Older Period

A backfill re-processes a past time window without affecting newer data.

1. Open the **`incremental_api_ingestion`** notebook and set the widgets:
   - `batch_id`: a unique identifier for the backfill (e.g., `backfill_20260915_001`)
   - `run_date`: the date of the backfill run
   - `window_end_utc`: set the end of the past period you want to reprocess (e.g., `2026-09-15T23:59:59Z`). The notebook will fetch records from the last watermark up to this timestamp.
2. Click **Run All**. The notebook writes a new manifest for the backfill batch. It does **not** advance watermarks beyond `window_end_utc`, so subsequent normal incremental runs continue from where they left off.
3. Open the **`driver_session`** notebook and set the widgets:
   - `batch_id`: the same backfill `batch_id`
   - `run_date`: the same `run_date`
   - `load_type`: `INCREMENTAL`
   - `manifest_path`: leave blank (the driver finds `incremental_<batch_id>.json` automatically)
   - `run_scope`: `CORE_ONLY`
4. Click **Run All** on `driver_session`. The MERGE logic uses `payload_hash` and `source_updated_at` to ensure only genuinely changed records are updated. Older records will not overwrite newer ones already in Silver.

### Execution Logs


Every execution of `driver_session` records operational information in
`github_health.ops.pipeline_execution_logs`.

The log captures the processing layer, batch ID, execution start and end
times, status, rows processed, rows upserted, rows quarantined, and failure
details where applicable.

Query the execution history using:

```sql
SELECT execution_id, batch_id, notebook_name, layer, run_date, load_type,
       start_time, end_time, status, rows_processed,
       upserted_rows, quarantined_rows
FROM github_health.ops.pipeline_execution_logs
ORDER BY start_time DESC;


---

**Document status:** Phase 2 — Bronze and Silver implementation, October 2026.
