#!/usr/bin/env python3
"""
WS7: Data Migration Script — Create staging tables in Snowflake and load data.

This script handles two tasks:
  1. CREATE OR REPLACE the 12 source tables in DEV.STAGING with proper DDL
  2. Load data from CSV files (local or S3) into those tables

Usage:
  # Step 1: Create empty tables in Snowflake
  python migrate_to_snowflake.py --create-tables

  # Step 2: Load data from CSV files in a local directory
  python migrate_to_snowflake.py --load-csv /path/to/csv/dir

  # Step 3: Load data from a Snowflake stage (S3/GCS/Azure)
  python migrate_to_snowflake.py --load-stage @my_s3_stage

  # Step 4: Validate row counts
  python migrate_to_snowflake.py --validate

  # All-in-one: create tables + load from CSV
  python migrate_to_snowflake.py --create-tables --load-csv /path/to/csv/dir

Environment variables required:
  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
  SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE
"""

import argparse
import os
import sys
from pathlib import Path

try:
    import snowflake.connector
except ImportError:
    print("ERROR: snowflake-connector-python not installed.")
    print("  pip install snowflake-connector-python")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Table definitions: table_name -> list of (column_name, snowflake_type)
#
# Types are inferred from the Redshift source (Rails-backed Postgres).
# Integer PKs/FKs use INTEGER, text fields use VARCHAR, timestamps use
# TIMESTAMP_NTZ, booleans use BOOLEAN, and JSON/array fields use VARIANT.
# ---------------------------------------------------------------------------

TABLE_DEFINITIONS = {
    "buyer_seller_company_mappings": [
        ("id", "INTEGER"),
        ("client_company_id", "INTEGER"),
        ("dealing_with_company_id", "INTEGER"),
        ("vendor_code", "VARCHAR"),
        ("status", "VARCHAR"),
        ("created_at", "TIMESTAMP_NTZ"),
        ("updated_at", "TIMESTAMP_NTZ"),
        ("invited_by", "INTEGER"),
        ("source", "VARCHAR"),
        ("auto_discount", "VARCHAR"),
        ("vrp_code", "VARCHAR"),
        ("integration_status", "VARCHAR"),
        ("meta_data", "VARIANT"),
        ("dms_upload_status", "VARCHAR"),
    ],
    "teams": [
        ("id", "INTEGER"),
        ("company_id", "INTEGER"),
        ("name", "VARCHAR"),
        ("status", "VARCHAR"),
        ("created_at", "TIMESTAMP_NTZ"),
        ("updated_at", "TIMESTAMP_NTZ"),
        ("team_type", "VARCHAR"),
        ("zone_id", "INTEGER"),
        ("other_details", "VARIANT"),
        ("product_category_ids", "ARRAY"),
        ("buyer_hub_ids", "ARRAY"),
        ("purchase_group_ids", "ARRAY"),
    ],
    "team_members": [
        ("id", "INTEGER"),
        ("team_id", "INTEGER"),
        ("user_id", "INTEGER"),
        ("role", "VARCHAR"),
        ("status", "VARCHAR"),
        ("created_at", "TIMESTAMP_NTZ"),
        ("updated_at", "TIMESTAMP_NTZ"),
        ("designation_id", "INTEGER"),
    ],
    "companies": [
        ("id", "INTEGER"),
        ("name", "VARCHAR"),
        ("image_url", "VARCHAR"),
        ("category", "VARCHAR"),
        ("address", "VARCHAR"),
        ("city_id", "INTEGER"),
        ("coordinates", "VARCHAR"),
        ("phone", "VARCHAR"),
        ("email", "VARCHAR"),
        ("status", "VARCHAR"),
        ("rating", "FLOAT"),
        ("email_extension", "VARCHAR"),
        ("created_at", "TIMESTAMP_NTZ"),
        ("updated_at", "TIMESTAMP_NTZ"),
        ("pan_no", "VARCHAR"),
        ("fssai_code", "VARCHAR"),
        ("gst_no", "VARCHAR"),
        ("created_by", "INTEGER"),
        ("is_verified", "BOOLEAN"),
        ("broker_for", "VARCHAR"),
        ("owner_name", "VARCHAR"),
        ("year_of_establishment", "INTEGER"),
        ("tan_number", "VARCHAR"),
        ("number_of_employees", "INTEGER"),
        ("document_images", "VARIANT"),
        ("update_remarks", "VARCHAR"),
        ("invited_by", "INTEGER"),
        ("domain", "VARCHAR"),
        ("trade_mode", "VARCHAR"),
        ("app_configuration_id", "INTEGER"),
        ("category_type", "VARCHAR"),
        ("allowed_modules", "VARIANT"),
        ("supplier_type", "VARCHAR"),
        ("website", "VARCHAR"),
        ("company_initials", "VARCHAR"),
        ("company_status", "VARCHAR"),
        ("created_by_user", "INTEGER"),
        ("created_for_workspace", "VARCHAR"),
        ("gst_verification", "VARCHAR"),
        ("misc", "VARIANT"),
        ("sso_enabled", "BOOLEAN"),
        ("trade_requests_count", "INTEGER"),
        ("intake_requests_count", "INTEGER"),
        ("tax_type_country_mapping_id", "INTEGER"),
        ("recommendation_identifier", "VARCHAR"),
    ],
    "cities": [
        ("id", "INTEGER"),
        ("name", "VARCHAR"),
        ("coordinates", "VARCHAR"),
        ("status", "VARCHAR"),
        ("created_at", "TIMESTAMP_NTZ"),
        ("updated_at", "TIMESTAMP_NTZ"),
        ("apmc_configuration_id", "INTEGER"),
        ("state_id", "INTEGER"),
        ("country_id", "INTEGER"),
    ],
    "countries": [
        ("id", "INTEGER"),
        ("name", "VARCHAR"),
        ("country_code", "VARCHAR"),
        ("phone_prefix", "VARCHAR"),
        ("phone_number_length", "INTEGER"),
        ("created_at", "TIMESTAMP_NTZ"),
        ("updated_at", "TIMESTAMP_NTZ"),
        ("flag_url", "VARCHAR"),
        ("config", "VARIANT"),
        ("send_otp_allowed", "BOOLEAN"),
    ],
    "product_categories": [
        ("id", "INTEGER"),
        ("name", "VARCHAR"),
        ("ancestry", "VARCHAR"),
        ("status", "VARCHAR"),
        ("created_at", "TIMESTAMP_NTZ"),
        ("updated_at", "TIMESTAMP_NTZ"),
        ("hierarchy_type", "VARCHAR"),
        ("alias", "VARCHAR"),
        ("origins", "VARCHAR"),
        ("gst", "FLOAT"),
        ("image_url", "VARCHAR"),
        ("category_type", "VARCHAR"),
        ("quality_params", "VARIANT"),
        ("company_id", "INTEGER"),
        ("is_default_category", "BOOLEAN"),
        ("category_code", "VARCHAR"),
    ],
    "user_company_mappings": [
        ("id", "INTEGER"),
        ("user_id", "INTEGER"),
        ("company_id", "INTEGER"),
        ("status", "VARCHAR"),
        ("created_at", "TIMESTAMP_NTZ"),
        ("updated_at", "TIMESTAMP_NTZ"),
        ("trade_identifier", "VARCHAR"),
        ("designation_id", "INTEGER"),
        ("erp_id", "VARCHAR"),
    ],
    "users": [
        ("id", "INTEGER"),
        ("first_name", "VARCHAR"),
        ("last_name", "VARCHAR"),
        ("gender", "VARCHAR"),
        ("address", "VARCHAR"),
        ("category", "VARCHAR"),
        ("email", "VARCHAR"),
        ("phone", "VARCHAR"),
        ("is_phone_verified", "BOOLEAN"),
        ("city_id", "INTEGER"),
        ("company_id", "INTEGER"),
        ("profile_pic_url", "VARCHAR"),
        ("status", "VARCHAR"),
        ("created_at", "TIMESTAMP_NTZ"),
        ("updated_at", "TIMESTAMP_NTZ"),
        ("role", "VARCHAR"),
        ("firm_name", "VARCHAR"),
        ("is_email_verified", "BOOLEAN"),
        ("should_receive_sms", "BOOLEAN"),
        ("email_verified", "BOOLEAN"),
        ("update_remarks", "VARCHAR"),
        ("user_access_right_ids", "VARIANT"),
        ("should_receive_email", "BOOLEAN"),
        ("referral_code", "VARCHAR"),
        ("deleted_at", "TIMESTAMP_NTZ"),
        ("adderss", "VARCHAR"),
        ("should_receive_sims", "BOOLEAN"),
        ("user_access_right_id", "INTEGER"),
        ("should_recieve_email", "BOOLEAN"),
        ("referal_code", "VARCHAR"),
        ("other_details", "VARIANT"),
        ("send_whatsapp_msg", "BOOLEAN"),
        ("team_identifiers", "VARIANT"),
        ("mfa_secret", "VARCHAR"),
        ("is_mfa_enabled", "BOOLEAN"),
        ("is_subscribed_to_zones", "BOOLEAN"),
        ("created_by_user", "INTEGER"),
        ("ip_info", "VARIANT"),
        ("session_id", "VARCHAR"),
        ("access_mode", "VARCHAR"),
        ("terms_and_policy_accepted", "VARIANT"),
        ("tnc_and_policy_log", "VARIANT"),
    ],
    "taggings": [
        ("id", "INTEGER"),
        ("tag_id", "INTEGER"),
        ("taggable_type", "VARCHAR"),
        ("taggable_id", "INTEGER"),
        ("status", "VARCHAR"),
        ("tagged_by_id", "INTEGER"),
        ("created_at", "TIMESTAMP_NTZ"),
        ("updated_at", "TIMESTAMP_NTZ"),
    ],
    "tags": [
        ("id", "INTEGER"),
        ("name", "VARCHAR"),
        ("creator_company_mapping_id", "INTEGER"),
        ("status", "VARCHAR"),
        ("created_at", "TIMESTAMP_NTZ"),
        ("updated_at", "TIMESTAMP_NTZ"),
        ("visibility_level_type", "VARCHAR"),
        ("visibility_level_id", "INTEGER"),
        ("user_company_mapping_id", "INTEGER"),
    ],
    "preferred_vendor_item_mappings": [
        ("id", "INTEGER"),
        ("item_type", "VARCHAR"),
        ("item_id", "INTEGER"),
        ("company_id", "INTEGER"),
        ("status", "VARCHAR"),
        ("created_at", "TIMESTAMP_NTZ"),
        ("updated_at", "TIMESTAMP_NTZ"),
    ],
}


def get_connection():
    """Create a Snowflake connection using environment variables."""
    required_vars = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
    ]
    missing = [v for v in required_vars if not os.environ.get(v)]
    if missing:
        print(f"ERROR: Missing environment variables: {', '.join(missing)}")
        print("Set them or source your .env.secrets file first.")
        sys.exit(1)

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE", "ETL_ADMIN"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "WH_TRANSFORM"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "DEV"),
        schema="STAGING",
    )


def create_tables(conn):
    """Create all 12 staging tables in DEV.STAGING."""
    cursor = conn.cursor()
    cursor.execute("USE DATABASE DEV")
    cursor.execute("USE SCHEMA STAGING")

    for table_name, columns in TABLE_DEFINITIONS.items():
        col_defs = ",\n    ".join(
            f"{col_name} {col_type}" for col_name, col_type in columns
        )
        ddl = f"CREATE TABLE IF NOT EXISTS {table_name.upper()} (\n    {col_defs}\n)"
        print(f"  Creating {table_name.upper()}... ", end="")
        try:
            cursor.execute(ddl)
            print("OK")
        except Exception as e:
            print(f"FAILED: {e}")

    cursor.close()
    print(f"\nCreated {len(TABLE_DEFINITIONS)} tables in DEV.STAGING.")


def load_from_csv(conn, csv_dir):
    """Load data from CSV files into staging tables.

    Expected file naming: {table_name}.csv (e.g., companies.csv, users.csv).
    Files must have a header row matching the column names.
    """
    csv_path = Path(csv_dir)
    if not csv_path.is_dir():
        print(f"ERROR: Directory not found: {csv_dir}")
        sys.exit(1)

    cursor = conn.cursor()
    cursor.execute("USE DATABASE DEV")
    cursor.execute("USE SCHEMA STAGING")

    loaded = 0
    for table_name in TABLE_DEFINITIONS:
        csv_file = csv_path / f"{table_name}.csv"
        if not csv_file.exists():
            print(f"  Skipping {table_name} (no CSV file found)")
            continue

        print(f"  Loading {table_name} from {csv_file}... ", end="")
        try:
            # Use Snowflake PUT + COPY INTO for efficient loading
            stage_name = f"@%{table_name.upper()}"

            # Truncate existing data
            cursor.execute(f"TRUNCATE TABLE IF EXISTS {table_name.upper()}")

            # PUT file to table stage
            cursor.execute(
                f"PUT 'file://{csv_file.resolve()}' {stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
            )

            # COPY INTO from stage
            cursor.execute(f"""
                COPY INTO {table_name.upper()}
                FROM {stage_name}
                FILE_FORMAT = (
                    TYPE = 'CSV'
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                    SKIP_HEADER = 1
                    NULL_IF = ('', 'NULL', 'null', '\\\\N')
                    EMPTY_FIELD_AS_NULL = TRUE
                    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
                )
                ON_ERROR = 'CONTINUE'
                PURGE = TRUE
            """)
            result = cursor.fetchone()
            rows_loaded = result[3] if result else "?"
            print(f"OK ({rows_loaded} rows)")
            loaded += 1
        except Exception as e:
            print(f"FAILED: {e}")

    cursor.close()
    print(f"\nLoaded {loaded}/{len(TABLE_DEFINITIONS)} tables from CSV.")


def load_from_stage(conn, stage_name):
    """Load data from a Snowflake external stage (S3/GCS/Azure).

    Expected structure: @stage_name/{table_name}/ containing Parquet files.
    Uses MATCH_BY_COLUMN_NAME for flexible column mapping.
    """
    cursor = conn.cursor()
    cursor.execute("USE DATABASE DEV")
    cursor.execute("USE SCHEMA STAGING")

    loaded = 0
    for table_name in TABLE_DEFINITIONS:
        print(f"  Loading {table_name} from {stage_name}/{table_name}/... ", end="")
        try:
            cursor.execute(f"TRUNCATE TABLE IF EXISTS {table_name.upper()}")
            cursor.execute(f"""
                COPY INTO {table_name.upper()}
                FROM {stage_name}/{table_name}/
                FILE_FORMAT = (TYPE = 'PARQUET')
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = 'CONTINUE'
            """)
            result = cursor.fetchone()
            rows_loaded = result[3] if result else "?"
            print(f"OK ({rows_loaded} rows)")
            loaded += 1
        except Exception as e:
            print(f"FAILED: {e}")

    cursor.close()
    print(f"\nLoaded {loaded}/{len(TABLE_DEFINITIONS)} tables from stage.")


def validate(conn):
    """Validate row counts for all staging tables."""
    cursor = conn.cursor()
    cursor.execute("USE DATABASE DEV")
    cursor.execute("USE SCHEMA STAGING")

    print(f"\n{'Table':<45} {'Rows':>10}")
    print("-" * 57)

    total_rows = 0
    empty_tables = []
    for table_name in TABLE_DEFINITIONS:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name.upper()}")
            count = cursor.fetchone()[0]
            total_rows += count
            status = "" if count > 0 else "  <-- EMPTY"
            print(f"  {table_name:<43} {count:>10}{status}")
            if count == 0:
                empty_tables.append(table_name)
        except Exception as e:
            print(f"  {table_name:<43} {'ERROR':>10}  {e}")
            empty_tables.append(table_name)

    print("-" * 57)
    print(f"  {'TOTAL':<43} {total_rows:>10}")

    if empty_tables:
        print(f"\nWARNING: {len(empty_tables)} empty tables: {', '.join(empty_tables)}")
        print("Load data before running dbt run --full-refresh.")
    else:
        print("\nAll tables have data. Ready for: dbt run --full-refresh --profiles-dir .")

    cursor.close()


def main():
    parser = argparse.ArgumentParser(
        description="WS7: Migrate source data to Snowflake DEV.STAGING",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create empty tables
  python migrate_to_snowflake.py --create-tables

  # Load from CSV files
  python migrate_to_snowflake.py --load-csv ./data/

  # Load from S3 via Snowflake stage
  python migrate_to_snowflake.py --load-stage @my_s3_stage

  # Validate row counts
  python migrate_to_snowflake.py --validate

  # Full pipeline: create + load + validate
  python migrate_to_snowflake.py --create-tables --load-csv ./data/ --validate
        """,
    )
    parser.add_argument(
        "--create-tables",
        action="store_true",
        help="Create the 12 staging tables in DEV.STAGING (idempotent)",
    )
    parser.add_argument(
        "--load-csv",
        metavar="DIR",
        help="Load data from CSV files in DIR (one file per table: {table}.csv)",
    )
    parser.add_argument(
        "--load-stage",
        metavar="STAGE",
        help="Load data from a Snowflake external stage (@stage/{table}/ with Parquet)",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Print row counts for all staging tables",
    )

    args = parser.parse_args()

    if not any([args.create_tables, args.load_csv, args.load_stage, args.validate]):
        parser.print_help()
        sys.exit(0)

    print("Connecting to Snowflake...")
    conn = get_connection()
    print(f"  Connected: {os.environ.get('SNOWFLAKE_ACCOUNT')}\n")

    try:
        if args.create_tables:
            print("=== Creating staging tables ===")
            create_tables(conn)
            print()

        if args.load_csv:
            print("=== Loading data from CSV ===")
            load_from_csv(conn, args.load_csv)
            print()

        if args.load_stage:
            print(f"=== Loading data from stage {args.load_stage} ===")
            load_from_stage(conn, args.load_stage)
            print()

        if args.validate:
            print("=== Validating row counts ===")
            validate(conn)
            print()

    finally:
        conn.close()
        print("Done.")


if __name__ == "__main__":
    main()
