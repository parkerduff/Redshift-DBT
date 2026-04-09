# Testing the Redshift-DBT Pipeline

## Devin Secrets Needed
- `REDSHIFT_PASSWORD` — Redshift database password (required for all dbt commands)

## Setup
```bash
source .venv/bin/activate
export REDSHIFT_PASSWORD="$REDSHIFT_PASSWORD"
dbt deps --profiles-dir .
```

## Verify Connection
```bash
dbt debug --profiles-dir .
```
Expected: `Connection test: [OK connection ok]` and `schema: akshat_staging`

## Run Full Build
```bash
dbt build --profiles-dir .
```
Expected: `PASS=36 WARN=6 ERROR=0 SKIP=0 TOTAL=42`

The 6 warnings are expected — they are uniqueness test warnings on source tables with duplicate IDs in `akshat_raw` (companies, product_categories, team_members, teams, user_company_mappings, users).

## Run Individual Steps
```bash
# Staging views only
dbt run --select staging --profiles-dir .

# fact_vendor incremental model only
dbt run --select fact_vendor --profiles-dir .

# Tests only
dbt test --profiles-dir .
```

## Schema Layout
- **Source data**: `akshat_raw` schema (12 tables)
- **Dev target**: `akshat_staging` schema (staging views go here)
- **Prod target**: `akshat_prod` schema
- **fact_vendor**: lands in `akshat_staging_public` (due to `schema='public'` config in the model which appends `_public` to the target schema)
- **Database**: `dev` on Redshift Serverless (ap-south-1)

## Known Issues
- `buyer_seller_company_mappings` in `akshat_raw` may have 0 rows, causing `fact_vendor` to be empty. This is a source data availability issue, not a pipeline bug.
- 6 source tables have duplicate IDs — these are raw data quality issues. Uniqueness tests are set to `warn` severity so they don't block the pipeline.
- The `calogica/dbt_expectations` package is deprecated; consider migrating to `metaplane/dbt_expectations`.

## Querying Results
Use psycopg2 or any Redshift client to query output tables:
```python
import psycopg2
conn = psycopg2.connect(
    host='default-workgroup.885373794985.ap-south-1.redshift-serverless.amazonaws.com',
    port=5439, dbname='dev', user='admin', password=REDSHIFT_PASSWORD
)
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM akshat_staging_public.fact_vendor')
print(cur.fetchone()[0])
```

## Tips
- All dbt commands need `--profiles-dir .` since profiles.yml is in the repo root
- The `REDSHIFT_HOST` and `REDSHIFT_USER` env vars have sensible defaults and usually don't need to be set
- Intermediate models (int_*) are ephemeral views, not persisted tables — you can't query them directly
- For a full refresh of fact_vendor (ignoring incremental logic): `dbt run --select fact_vendor --full-refresh --profiles-dir .`
