# Redshift-DBT → Snowflake: Migration Execution Plan

> **Status:** WS1–WS6 code conversion complete — ready for WS0 (environment setup) and WS7–WS9 (data migration, testing, cutover)  
> **Source:** [Snowflake Migration Guide](https://docs.snowflake.com/en/migrations/guides/redshift#phase-3-database-code-conversion)  
> **Repo:** `parkerduff/Redshift-DBT`  
> **Approach:** Playbook-first iteration. Test one slice, refine the prompt, then batch.

---

## Current State Inventory

```
  parkerduff/Redshift-DBT
  ========================

  models/
  ├── staging/              13 models (views) — simple SELECT pass-throughs
  │   ├── _sources.yml      Source definitions (12 tables)
  │   ├── stg_companies.sql
  │   ├── stg_users.sql
  │   └── ... (11 more)
  ├── intermediate/          9 models (ephemeral) — business logic CTEs
  │   ├── int_vendor_mappings.sql        ← has = ANY(ARRAY[...])
  │   ├── int_user_product_categories.sql ← has unnest(), '{}' check
  │   ├── int_vendor_product_categories.sql ← has ::text
  │   ├── int_preferred_vendor_mappings.sql ← has ::text
  │   ├── int_vendor_tags.sql            ← has LISTAGG (compatible)
  │   ├── int_vendor_companies.sql       ← clean
  │   ├── int_vendor_all_pocs.sql        ← clean
  │   ├── int_vendor_scores.sql          ← clean
  │   └── int_invited_by_users.sql       ← clean
  └── marts/
      ├── _schema.yml
      └── fact_vendor.sql   1 model (incremental MERGE) ← has EXTRACT(EPOCH), ::text

  macros/
  └── incremental_merge.sql  1 macro — standard MERGE (compatible)

  tests/
  └── test_fact_vendor.sql   1 custom test (compatible)

  Python CDC:
  ├── fixed_intelligent_cdc_processor.py   psycopg2 + Redshift SQL
  ├── surgical_cdc_processor.py            psycopg2 + Redshift SQL
  └── expanded_dictionary.py               column mapping registry (no changes)

  Config:
  ├── profiles.yml          Redshift connection (type: redshift)
  ├── dbt_project.yml       redshift_* vars
  ├── requirements.txt      dbt-redshift, psycopg2-binary
  ├── packages.yml          dbt_utils, dbt_expectations (compatible)
  └── run_pipeline.sh       bash runner (compatible)
```

---

## Architecture Comparison

```
  REDSHIFT (current)                       SNOWFLAKE (target)
  ==================                       ==================

  Tightly coupled compute+storage          Decoupled compute + storage
  Fixed cluster (always-on)                Elastic warehouses (per-second)
  DISTSTYLE / DISTKEY / SORTKEY            Automatic micro-partitioning
  VACUUM / ANALYZE required                Automatic — no maintenance
  PL/pgSQL stored procs                    Snowflake Scripting / Python / JS
  psycopg2 driver                          snowflake-connector-python
```

---

## How This Plan Is Structured

The work is broken into **work streams** (vertical slices of the system) and **tasks** (horizontal units a single developer/session can own). Repeated patterns are identified and turned into a **playbook**: we test the playbook on one item, iterate until reliable, then fan out via batch sessions.

```
  EXECUTION MODEL
  ===============

  1. Identify a repeated pattern (e.g., "convert a dbt model")
  2. Write a playbook for that pattern
  3. Run the playbook on ONE item manually
  4. Review the output — did it work? Fix the playbook.
  5. Run it on a SECOND item to confirm generalization
  6. Once the playbook is solid → batch session all remaining items
  7. Collect results, run integration tests

  ┌─────────────┐     ┌─────────────┐     ┌──────────────────────┐
  │  Playbook    │────▶│  Test 1x     │────▶│  Iterate if needed   │
  │  (draft)     │     │  (manual)    │     │  (fix playbook)      │
  └─────────────┘     └─────────────┘     └──────────┬───────────┘
                                                      │
                                                      ▼
                                           ┌──────────────────────┐
                                           │  Test 2nd item       │
                                           │  (confirm it works)  │
                                           └──────────┬───────────┘
                                                      │
                                                      ▼
                                           ┌──────────────────────┐
                                           │  Batch session all   │
                                           │  remaining items     │
                                           └──────────────────────┘
```

---

## Work Stream 0: Snowflake Environment Setup

> **Owner:** Infra / DBA  
> **Parallelizable:** No — must complete before all other streams  
> **Playbook needed:** No (one-time tasks)

| Task ID | Task | Details | Depends On |
|:---|:---|:---|:---|
| **0.1** | Provision Snowflake account | AWS `ap-south-1`, Standard edition trial | — |
| **0.2** | Create database and schemas | `CREATE DATABASE DEV; CREATE SCHEMA DEV.STAGING; CREATE SCHEMA DEV.PROD_POC; CREATE SCHEMA DEV.PUBLIC;` | 0.1 |
| **0.3** | Create roles and users | `ETL_ADMIN` role, `BI_READ_ONLY` role, `DBT_SERVICE_ACCT` user with key-pair auth | 0.1 |
| **0.4** | Create virtual warehouses | `WH_LOADING` (X-Small), `WH_TRANSFORM` (Small), both auto-suspend 60s | 0.1 |
| **0.5** | Set up S3 storage integration | For data migration (UNLOAD → S3 → COPY INTO). Create external stage. | 0.1 |
| **0.6** | Network policies | Restrict to trusted IPs | 0.1 |

**Exit criteria:** `dbt debug` succeeds against the empty Snowflake database.

---

## Work Stream 1: dbt Config Layer

> **Owner:** 1 developer  
> **Parallelizable:** No (one PR, all config changes together)  
> **Playbook needed:** No (4 small unique files)

| Task ID | Task | File(s) | What Changes |
|:---|:---|:---|:---|
| **1.1** | Swap dbt adapter in profiles | `profiles.yml` | `type: redshift` → `type: snowflake`, replace host/port/dbname with account/warehouse/database/role. Use `{{ env_var('SNOWFLAKE_PASSWORD') }}` instead of hardcoded password. |
| **1.2** | Update Python dependencies | `requirements.txt` | `dbt-redshift` → `dbt-snowflake`, `psycopg2-binary` → `snowflake-connector-python` |
| **1.3** | Update project vars | `dbt_project.yml` | Rename `redshift_database`/`redshift_schema_*` to generic names. Add `quoting:` block. |
| **1.4** | Update source definitions | `models/staging/_sources.yml` | Verify `database: "dev"` / `schema: "staging"` work with quoting config, or uppercase them. |
| **1.5** | Update pipeline runner | `run_pipeline.sh` | No SQL changes, but verify `dbt deps` + `dbt debug` + `dbt run` still work after adapter swap. |

**Validation:** `dbt debug` connects successfully. `dbt compile` generates valid Snowflake SQL (no runtime needed yet).

**PR:** Single PR: "chore: swap dbt adapter from Redshift to Snowflake"

---

## Work Stream 2: Staging Model Conversion (BATCH CANDIDATE)

> **Owner:** Batch sessions  
> **Parallelizable:** YES — all 13 models are independent  
> **Playbook needed:** YES

### Why this is batchable

All 13 staging models follow the exact same pattern:
```sql
{{ config(materialized='view') }}
SELECT col1, col2, ... FROM {{ source('staging', 'table_name') }}
```

Most need **zero SQL changes**. The playbook's job is to:
1. Audit the model for Redshift-specific syntax
2. Apply the conversion rules (primarily `::text` → `::VARCHAR`)
3. Validate with `dbt compile --select <model>`

### Conversion rules for staging models

```
  RULE                          APPLIES TO
  ====                          ==========
  ::text → ::VARCHAR            Any model casting to text
  No other changes expected     These are simple pass-throughs
```

### Playbook iteration plan

| Step | Action |
|:---|:---|
| **2.1** | Write Playbook v1: "Convert one staging model from Redshift to Snowflake" |
| **2.2** | Test on `stg_companies.sql` (largest model, 54 lines, no breaking changes — should be a no-op) |
| **2.3** | Review output. Did the playbook correctly identify "no changes needed"? |
| **2.4** | Test on a second model: `stg_users.sql` (also clean). Confirm playbook handles it. |
| **2.5** | If both pass → batch session the remaining 11 models. Each session gets ONE model. |
| **2.6** | Collect results. Run `dbt compile --select staging` to validate all 13. |

**PR per batch item:** No — collect all into one PR: "feat: convert staging models to Snowflake"

### Playbook draft: `convert-staging-model`

```
INPUTS:
  - model_file: path to staging .sql file (e.g., models/staging/stg_companies.sql)

STEPS:
  1. Read the model file
  2. Scan for Redshift-specific syntax:
     - ::text  →  replace with ::VARCHAR
     - GETDATE()  →  replace with CURRENT_TIMESTAMP()
     - SYSDATE  →  replace with CURRENT_TIMESTAMP()
     - Any DISTSTYLE/DISTKEY/SORTKEY in config  →  remove
  3. If no changes needed, report "no changes" and exit
  4. Apply changes
  5. Run: dbt compile --select <model_name>
  6. Verify compiled SQL is valid Snowflake syntax
  7. Commit the change
```

---

## Work Stream 3: Intermediate Model Conversion (BATCH CANDIDATE)

> **Owner:** Batch sessions  
> **Parallelizable:** YES — all 9 models are independent at the SQL level  
> **Playbook needed:** YES (more complex than staging — each model may have different Redshift constructs)

### File-by-file audit

| Model | Breaking Changes | Complexity |
|:---|:---|:---|
| `int_vendor_mappings.sql` | `= ANY(ARRAY[1, 2, 7])` → `IN (1, 2, 7)` | Low |
| `int_user_product_categories.sql` | `unnest()` → `LATERAL FLATTEN()`, `!= '{}'` → `ARRAY_SIZE() > 0` | **High** |
| `int_vendor_product_categories.sql` | `::text` → `::VARCHAR` (2 occurrences) | Low |
| `int_preferred_vendor_mappings.sql` | `::text` → `::VARCHAR` | Low |
| `int_vendor_tags.sql` | Add `WITHIN GROUP (ORDER BY ...)` to LISTAGG | Low |
| `int_vendor_companies.sql` | None | None |
| `int_vendor_all_pocs.sql` | None | None |
| `int_vendor_scores.sql` | None | None |
| `int_invited_by_users.sql` | None | None |

### Conversion rules for intermediate models

```
  RULE                                    EXAMPLE
  ====                                    =======

  ::text → ::VARCHAR                      item_id::text  →  item_id::VARCHAR

  = ANY(ARRAY[...]) → IN (...)            = ANY(ARRAY[1,2,7])  →  IN (1,2,7)

  unnest(array_col) →                     SELECT unnest(t.product_category_ids)
    LATERAL FLATTEN(INPUT => array_col)     →
                                           SELECT f.value::INTEGER AS category_id
                                           FROM table t,
                                           LATERAL FLATTEN(INPUT => t.product_category_ids) f

  array_col != '{}' →                     t.product_category_ids != '{}'
    ARRAY_SIZE(array_col) > 0               →  ARRAY_SIZE(t.product_category_ids) > 0

  LISTAGG without ORDER →                 LISTAGG(name, ', ')
    Add WITHIN GROUP                        →  LISTAGG(name, ', ') WITHIN GROUP (ORDER BY name)
```

### Playbook iteration plan

| Step | Action |
|:---|:---|
| **3.1** | Write Playbook v1: "Convert one intermediate dbt model from Redshift to Snowflake" |
| **3.2** | Test on `int_vendor_mappings.sql` — has `= ANY(ARRAY[...])`, good first test of a real breaking change |
| **3.3** | Review output. Was the `ANY(ARRAY)` → `IN` conversion correct? Did it preserve surrounding logic? |
| **3.4** | Iterate playbook if needed |
| **3.5** | Test on `int_user_product_categories.sql` — the **hardest** model (unnest + array check). This is the real stress test. |
| **3.6** | Review output. Was `unnest` → `LATERAL FLATTEN` correct? Was `!= '{}'` → `ARRAY_SIZE` correct? |
| **3.7** | Iterate playbook if needed |
| **3.8** | Test on `int_vendor_companies.sql` — a "no changes needed" model. Confirm playbook handles clean files correctly. |
| **3.9** | Once all 3 test cases pass → batch session the remaining 6 models. |
| **3.10** | Collect results. Run `dbt compile --select intermediate` to validate all 9. |

**PR:** Single PR: "feat: convert intermediate models to Snowflake"

### Playbook draft: `convert-intermediate-model`

```
INPUTS:
  - model_file: path to intermediate .sql file

STEPS:
  1. Read the model file
  2. Scan for Redshift-specific syntax (apply ALL rules from the table above)
  3. For each match, apply the Snowflake equivalent
  4. Special handling for unnest → LATERAL FLATTEN:
     a. Identify the unnested column and its alias
     b. Rewrite using LATERAL FLATTEN(INPUT => col) f
     c. Replace column references with f.value::<type>
  5. If no changes needed, report "no changes" and exit
  6. Run: dbt compile --select <model_name>
  7. Verify compiled SQL is valid Snowflake syntax
  8. Commit the change
```

---

## Work Stream 4: Mart Model Conversion

> **Owner:** 1 developer  
> **Parallelizable:** No (only 1 model, but it's the most critical)  
> **Playbook needed:** No (unique, high-stakes — do manually)

| Task ID | Task | File | What Changes |
|:---|:---|:---|:---|
| **4.1** | Convert `fact_vendor.sql` | `models/marts/fact_vendor.sql` | 3 changes: `EXTRACT(EPOCH FROM x)::bigint` → `DATE_PART(EPOCH_SECOND, x)::INTEGER` (2 occurrences, lines 101 and 118), `poc_id::text` → `poc_id::VARCHAR` (line 125) |
| **4.2** | Verify incremental config | same file | Confirm `incremental_strategy='merge'` and `merge_update_columns` work with dbt-snowflake adapter |
| **4.3** | Compile and validate | — | `dbt compile --select fact_vendor` — inspect generated SQL |
| **4.4** | Update `_schema.yml` | `models/marts/_schema.yml` | Check if `epoch timestamp as bigint` description needs updating (now INTEGER) |

**PR:** Include in same PR as intermediate models, or standalone: "feat: convert fact_vendor to Snowflake"

---

## Work Stream 5: Macro Conversion

> **Owner:** 1 developer  
> **Parallelizable:** No (only 1 macro)  
> **Playbook needed:** No

| Task ID | Task | File | What Changes |
|:---|:---|:---|:---|
| **5.1** | Audit `incremental_merge.sql` | `macros/incremental_merge.sql` | Uses standard `MERGE INTO ... USING ...` — **no changes needed**. Snowflake supports this syntax natively. |
| **5.2** | Verify with compile | — | `dbt compile --select fact_vendor` (which uses this macro) |

**Exit criteria:** Compiled SQL uses valid Snowflake MERGE syntax.

---

## Work Stream 6: Python CDC Conversion (BATCH CANDIDATE)

> **Owner:** Batch sessions (2 items)  
> **Parallelizable:** YES — the 2 scripts are independent  
> **Playbook needed:** YES

### What needs to change in each Python script

```
  CHANGE                               BEFORE (Redshift)              AFTER (Snowflake)
  ======                               =================              =================

  1. Import                            import psycopg2                import snowflake.connector

  2. Connection config                 host, port, dbname,            account, warehouse,
                                       user, password                 database, schema,
                                                                      user, password

  3. Connection call                   psycopg2.connect(**cfg)        snowflake.connector.connect(**cfg)

  4. Schema references in SQL          staging.{table}                DEV.STAGING.{table}
                                       public.{table}                 DEV.PUBLIC.{table}

  5. information_schema queries        table_schema = 'staging'       table_schema = 'STAGING'
                                                                      (+ table_catalog = 'DEV')

  6. Hardcoded credentials             password in source code        os.environ['SNOWFLAKE_PASSWORD']
```

### Playbook iteration plan

| Step | Action |
|:---|:---|
| **6.1** | Write Playbook v1: "Convert a Python CDC script from psycopg2/Redshift to snowflake-connector/Snowflake" |
| **6.2** | Test on `fixed_intelligent_cdc_processor.py` (simpler of the two, 384 lines) |
| **6.3** | Review: Are all 6 change categories applied correctly? Does the script import correctly? |
| **6.4** | Iterate playbook if needed |
| **6.5** | Apply to `surgical_cdc_processor.py` (527 lines, same patterns) |
| **6.6** | Validate both scripts parse without import errors: `python -c "import fixed_intelligent_cdc_processor"` |

**PR:** Single PR: "feat: convert CDC processors to Snowflake connector"

### Playbook draft: `convert-python-cdc-script`

```
INPUTS:
  - script_file: path to Python CDC script

STEPS:
  1. Read the script
  2. Replace: import psycopg2  →  import snowflake.connector
  3. Replace connection config dict:
     - Remove: host, port, dbname
     - Add: account, warehouse, database, schema
     - Replace password value with os.environ['SNOWFLAKE_PASSWORD']
  4. Replace: psycopg2.connect(**config)  →  snowflake.connector.connect(**config)
  5. Find all SQL strings containing schema-qualified table names:
     - staging.{table}  →  DEV.STAGING.{table}
     - public.{table}   →  DEV.PUBLIC.{table}
  6. Find all information_schema queries:
     - table_schema = 'staging'  →  table_schema = 'STAGING'
     - Add table_catalog = 'DEV' if not present
  7. Run: python -c "import <module_name>" to verify no import errors
  8. Commit the change
```

---

## Work Stream 7: Data Migration (BATCH CANDIDATE)

> **Owner:** Batch sessions  
> **Parallelizable:** YES — all 12 tables are independent  
> **Playbook needed:** YES  
> **Depends on:** Work Stream 0 (Snowflake environment exists)

### Pipeline per table

```
  REDSHIFT                     S3                          SNOWFLAKE
  ========                     ==                          =========

  UNLOAD staging.{table}  ──▶  s3://bucket/{table}/  ──▶  COPY INTO DEV.STAGING.{TABLE}
  (Parquet, PARALLEL ON)       (Parquet files)             (MATCH_BY_COLUMN_NAME)
```

### Playbook iteration plan

| Step | Action |
|:---|:---|
| **7.1** | Write Playbook v1: "Migrate one Redshift table to Snowflake via S3" |
| **7.2** | Test on `companies` table (largest, most columns — 40+ columns, best stress test) |
| **7.3** | Validate: row count matches, spot-check 5 rows, verify data types |
| **7.4** | Iterate playbook if needed (type mapping issues, encoding, etc.) |
| **7.5** | Test on `users` table (second-largest, has NULLs and timestamps) |
| **7.6** | Once both pass → batch session the remaining 10 tables |
| **7.7** | After all 12 tables loaded, run `dbt run --full-refresh` to build fact_vendor |

### Playbook draft: `migrate-table-redshift-to-snowflake`

```
INPUTS:
  - table_name: source table name in staging schema
  - s3_bucket: target S3 bucket
  - snowflake_stage: Snowflake external stage name

STEPS:
  1. UNLOAD from Redshift:
     UNLOAD ('SELECT * FROM staging.{table_name}')
     TO 's3://{s3_bucket}/{table_name}/'
     IAM_ROLE '...'
     FORMAT AS PARQUET
     PARALLEL ON;

  2. CREATE TABLE in Snowflake (if not exists) matching source schema

  3. COPY INTO Snowflake:
     COPY INTO DEV.STAGING.{TABLE_NAME}
     FROM @{snowflake_stage}/{table_name}/
     FILE_FORMAT = (TYPE = 'PARQUET')
     MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

  4. Validate:
     - Row count: SELECT COUNT(*) from both systems
     - Spot check: SELECT * LIMIT 5 from both, compare
     - Null check: COUNT of NULLs on key columns matches

  5. Report results
```

---

## Work Stream 8: Integration Testing & Validation

> **Owner:** 1 developer  
> **Parallelizable:** No (must run after all conversions)  
> **Depends on:** Work Streams 1-7 all complete

| Task ID | Task | Command | What to Check |
|:---|:---|:---|:---|
| **8.1** | Full dbt compile | `dbt compile` | All models generate valid Snowflake SQL, zero errors |
| **8.2** | Full dbt run | `dbt run --full-refresh` | All models build successfully on Snowflake |
| **8.3** | Full dbt test | `dbt test` | All schema tests pass (not_null, unique) + custom test_fact_vendor |
| **8.4** | Incremental run test | `dbt run` (without --full-refresh) | Incremental MERGE works, 30-min window filter works |
| **8.5** | CDC processor test | `python fixed_intelligent_cdc_processor.py` | Connects, detects changes, maps to fact tables |
| **8.6** | Data validation | Row counts + checksums | Redshift vs Snowflake data matches |

---

## Work Stream 9: Cutover & Optimization

> **Owner:** Infra / DBA + 1 developer  
> **Parallelizable:** Partially  
> **Depends on:** Work Stream 8 passes

| Task ID | Task | Details |
|:---|:---|:---|
| **9.1** | Stop Redshift cron jobs | Pause `dbt run` and CDC processors |
| **9.2** | Final incremental data sync | UNLOAD → S3 → COPY INTO for any changes since last migration |
| **9.3** | Switch pipeline to Snowflake | Update cron / Airflow / scheduler to target Snowflake |
| **9.4** | Keep Redshift read-only | 1–2 week fallback window |
| **9.5** | Decommission Redshift | After fallback period |
| **9.6** | (Optional) Replace CDC with Snowflake Streams | Native change tracking, eliminates Python scripts entirely |
| **9.7** | (Optional) Replace cron with Snowflake Tasks | Managed scheduling with retry logic |

---

## Execution Order & Dependency Graph

```
  WS0: Environment Setup
   │
   ├──▶ WS1: Config Layer ──────────────────────┐
   │                                              │
   ├──▶ WS7: Data Migration (batch, 12 tables)   │
   │         │                                    │
   │         ▼                                    ▼
   │    [data in Snowflake]              [code converted]
   │                                              │
   │                          ┌───────────────────┼───────────────────┐
   │                          │                   │                   │
   │                    WS2: Staging         WS3: Intermediate   WS4: Mart
   │                    (batch, 13)          (batch, 9)          (manual, 1)
   │                          │                   │                   │
   │                          │              WS5: Macros              │
   │                          │              (manual, 1)              │
   │                          │                   │                   │
   │                          └───────────────────┼───────────────────┘
   │                                              │
   │                                    WS6: Python CDC
   │                                    (batch, 2)
   │                                              │
   │                                              ▼
   └─────────────────────────────────▶  WS8: Integration Testing
                                                  │
                                                  ▼
                                        WS9: Cutover & Optimization
```

### What can run in parallel

```
  PARALLEL GROUP A (after WS0):          PARALLEL GROUP B (after WS1):
  ==============================          ==============================
  WS1: Config Layer                       WS2: Staging models (batch)
  WS7: Data Migration (batch)             WS3: Intermediate models (batch)
                                          WS4: Mart model
                                          WS5: Macros
                                          WS6: Python CDC (batch)
```

---

## Playbook Summary

| Playbook Name | Used In | Items | Test First | Then Batch |
|:---|:---|:---|:---|:---|
| `convert-staging-model` | WS2 | 13 models | `stg_companies.sql` + `stg_users.sql` | Remaining 11 |
| `convert-intermediate-model` | WS3 | 9 models | `int_vendor_mappings.sql` + `int_user_product_categories.sql` + `int_vendor_companies.sql` | Remaining 6 |
| `convert-python-cdc-script` | WS6 | 2 scripts | `fixed_intelligent_cdc_processor.py` | `surgical_cdc_processor.py` |
| `migrate-table-redshift-to-snowflake` | WS7 | 12 tables | `companies` + `users` | Remaining 10 |

### Playbook lifecycle (same for each)

```
  DRAFT ──▶ TEST ON ITEM 1 ──▶ FIX ──▶ TEST ON ITEM 2 ──▶ FIX ──▶ BATCH ALL
    │              │                          │                        │
    │         Did it work?              Does it generalize?      Fan out to N
    │         If no → fix                If no → fix              sessions
    │                                                                  │
    └──────────────────────────────────────────────────────────────────┘
                              iterate until solid
```

---

## Effort Estimation (revised)

| Work Stream | Tasks | Playbook Iterations | Batch Sessions | Calendar Time |
|:---|:---|:---|:---|:---|
| WS0: Environment | 6 tasks | — | — | 0.5 day |
| WS1: Config | 5 tasks | — | — | 0.5 day |
| WS2: Staging (batch) | 2 test + 11 batch | 1–2 iterations | 11 parallel | 0.5 day |
| WS3: Intermediate (batch) | 3 test + 6 batch | 2–3 iterations | 6 parallel | 1 day |
| WS4: Mart | 4 tasks | — | — | 0.5 day |
| WS5: Macros | 2 tasks | — | — | 0.25 day |
| WS6: Python CDC (batch) | 1 test + 1 batch | 1–2 iterations | 1 parallel | 0.5 day |
| WS7: Data Migration (batch) | 2 test + 10 batch | 1–2 iterations | 10 parallel | 1 day |
| WS8: Integration Testing | 6 tasks | — | — | 1 day |
| WS9: Cutover | 7 tasks | — | — | 0.5 day |
| **Total** | | | | **~6 days** |

With parallelization, the critical path is:

```
  WS0 (0.5d) → WS1 (0.5d) → WS3 (1d) → WS4 (0.5d) → WS8 (1d) → WS9 (0.5d) = ~4 days
```

---

## Completed Work

The following work streams have been executed in a single PR:

- **WS1: Config Layer** — `profiles.yml` swapped to `type: snowflake` with env-var-based credentials, `requirements.txt` updated (`dbt-snowflake`, `snowflake-connector-python`), `dbt_project.yml` vars renamed from `redshift_*` to generic names with `quoting:` block added, `_sources.yml` updated to `DEV.STAGING`, `run_pipeline.sh` updated.
- **WS2: Staging Models** — All 13 models audited. No Redshift-specific syntax found. Zero changes needed.
- **WS3: Intermediate Models** — All 9 models converted:
  - `int_vendor_mappings.sql`: `= ANY(ARRAY[1,2,7])` → `IN (1,2,7)`
  - `int_user_product_categories.sql`: `unnest()` → `LATERAL FLATTEN()`, `!= '{}'` → `ARRAY_SIZE() > 0`
  - `int_vendor_product_categories.sql`: `::text` → `::VARCHAR` (2 occurrences), `LISTAGG` → added `WITHIN GROUP`
  - `int_preferred_vendor_mappings.sql`: `::text` → `::VARCHAR`, `LISTAGG` → added `WITHIN GROUP`
  - `int_vendor_tags.sql`: `LISTAGG` → added `WITHIN GROUP (ORDER BY tags.name)`
  - 4 clean models: no changes needed
- **WS4: Mart Model** — `fact_vendor.sql` converted: `EXTRACT(EPOCH FROM x)::bigint` → `DATE_PART(EPOCH_SECOND, x)::INTEGER` (2 occurrences), `poc_id::text` → `poc_id::VARCHAR`
- **WS5: Macros** — `incremental_merge.sql` audited. Uses standard `MERGE INTO ... USING ...` — no changes needed.
- **WS6: Python CDC** — Both scripts converted: `psycopg2` → `snowflake.connector`, connection config updated to env-var-based Snowflake params, schema references updated to fully-qualified `DEV.STAGING.*` / `DEV.PUBLIC.*`, `information_schema` queries updated with `table_catalog = 'DEV'` and uppercased identifiers, hardcoded credentials removed.

### WS0: Snowflake Environment Setup — COMPLETE

Provisioned on account `DZNHIUR-VG87224` (2026-04-09):

| Task | Status | Details |
|:---|:---|:---|
| **0.1** Provision account | Already existed | Account `DZNHIUR-VG87224`, database `DEV` pre-existed |
| **0.2** Database & schemas | Already existed | `DEV.STAGING`, `DEV.PUBLIC`, `DEV.PROD_POC`, `DEV.STAGING_PUBLIC` all present |
| **0.3** Roles & users | **Created** | `ETL_ADMIN` (full DEV privileges), `BI_READ_ONLY` (SELECT-only), `DBT_SERVICE_ACCT` user with default role `ETL_ADMIN` and warehouse `WH_TRANSFORM` |
| **0.4** Warehouses | **Created** | `WH_LOADING` (X-Small, auto-suspend 60s), `WH_TRANSFORM` (X-Small, auto-suspend 60s) — both granted to `ETL_ADMIN`; `WH_TRANSFORM` also granted to `BI_READ_ONLY` |
| **0.5** S3 integration | **Deferred** | Requires S3 bucket and IAM role — will be configured when WS7 (data migration) begins |
| **0.6** Network policies | **Deferred** | Requires trusted IP ranges — should be configured before production cutover (WS9) |

**Exit criteria met:** `dbt debug` connects successfully against Snowflake (`All checks passed!`).

### WS7: Data Migration — COMPLETE (synthetic test data)

12 batch sessions created (one per staging table). Each session:
1. Connected to Snowflake using org secrets
2. Created the target table in `DEV.STAGING` with appropriate Snowflake DDL
3. Inserted 20 rows of synthetic test data
4. Validated row count and NULL checks

| Table | Rows | Status |
|:---|:---|:---|
| `BUYER_SELLER_COMPANY_MAPPINGS` | 20 | SUCCESS |
| `TEAMS` | 20 | SUCCESS |
| `TEAM_MEMBERS` | 20 | SUCCESS |
| `COMPANIES` | 20 | SUCCESS |
| `CITIES` | 20 | SUCCESS |
| `COUNTRIES` | 20 | SUCCESS |
| `PRODUCT_CATEGORIES` | 20 | SUCCESS |
| `USER_COMPANY_MAPPINGS` | 20 | SUCCESS |
| `USERS` | 20 | SUCCESS |
| `TAGGINGS` | 20 | SUCCESS |
| `TAGS` | 20 | SUCCESS |
| `PREFERRED_VENDOR_ITEM_MAPPINGS` | 20 | SUCCESS |

**Note:** Tables contain synthetic data for validation. For production cutover (WS9), replace with real data via UNLOAD → S3 → COPY INTO pipeline once Redshift access and S3 bucket are provisioned.

### WS8: Integration Testing — COMPLETE

| Task | Command | Result |
|:---|:---|:---|
| **8.1** dbt compile | `dbt compile` | **PASS** — 22 models, 29 tests, 12 sources, 856 macros, 0 errors |
| **8.2** Full dbt run | `dbt run --full-refresh` | **PASS** — 13 models (12 views + 1 incremental), 0 errors |
| **8.3** Full dbt test | `dbt test` | **PASS** — 29/29 tests passed (not_null, unique, custom test_fact_vendor) |
| **8.4** Incremental run | `dbt run` | **PASS** — 13 models, incremental MERGE on fact_vendor succeeded |
| **8.5** CDC processor test | — | Deferred (requires live data flow to test change detection) |
| **8.6** Data validation | — | N/A with synthetic data; to be validated after real data migration |

## Next Steps (remaining)

1. **Production data migration** — provision S3 bucket + IAM role, then UNLOAD from Redshift → S3 → COPY INTO Snowflake for all 12 tables with real data
2. **WS8.5–8.6** — CDC processor test + data validation with real data
3. **Execute WS9** — cutover (stop Redshift cron, final sync, switch pipeline, decommission Redshift)
