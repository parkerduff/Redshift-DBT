#!/usr/bin/env python3
"""
Fixed Intelligent CDC Processor - Handles All Data Types
Compares staging tables with public schema with proper type casting
"""

import os
import sys
import subprocess
import json
import snowflake.connector
from datetime import datetime
from typing import Dict, List, Set, Optional

# Import the dictionary
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from expanded_dictionary import fact_table_mapping

class FixedIntelligentCDCProcessor:
    def __init__(self, db_config: Dict = None):
        """Initialize intelligent CDC processor with robust type handling"""
        self.mapping = fact_table_mapping
        self.staging_tables = [
            'companies', 'buyer_seller_company_mappings', 'users', 'teams',
            'team_members', 'cities', 'countries', 'product_categories',
            'user_company_mappings', 'taggings', 'tags', 'preferred_vendor_item_mappings'
        ]
        
        # Database connection config
        self.db_config = db_config or {
            'account': os.environ.get('SNOWFLAKE_ACCOUNT', ''),
            'warehouse': os.environ.get('SNOWFLAKE_WAREHOUSE', 'WH_TRANSFORM'),
            'database': 'DEV',
            'schema': 'STAGING',
            'user': os.environ.get('SNOWFLAKE_USER', 'DBT_SERVICE_ACCT'),
            'password': os.environ.get('SNOWFLAKE_PASSWORD', '')
        }
        
        print("🚀 Fixed Intelligent CDC Processor initialized")
    
    def get_db_connection(self):
        """Get database connection"""
        try:
            conn = snowflake.connector.connect(**self.db_config)
            return conn
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return None
    
    def get_table_columns(self, table_name: str) -> List[str]:
        """Get all columns for a table (excluding timestamps)"""
        conn = self.get_db_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_catalog = 'DEV'
                AND table_schema = 'STAGING'
                AND table_name = '{table_name.upper()}'
                AND column_name NOT IN ('CREATED_AT', 'UPDATED_AT')
                ORDER BY ordinal_position
            """)
            
            columns = [row[0].lower() for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return columns
            
        except Exception as e:
            print(f"❌ Error getting columns for {table_name}: {e}")
            if conn:
                conn.close()
            return []
    
    def detect_column_changes_simple(self, table_name: str) -> List[Dict]:
        """
        Simplified approach: Compare staging with public and detect any differences
        Uses row-level comparison instead of column-by-column to avoid type issues
        """
        conn = self.get_db_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            
            # Simple approach: Get all changed records and determine change type
            # For now, we'll assume all staging records are changes since they're in 30-min window
            comparison_sql = f"""
            SELECT 
                staging.id,
                CASE 
                    WHEN staging.created_at = staging.updated_at THEN 'INSERT'
                    ELSE 'UPDATE'
                END as change_type,
                staging.created_at,
                staging.updated_at,
                CASE 
                    WHEN public.id IS NULL THEN true  -- New record
                    ELSE false  -- Existing record
                END as is_new_record
            FROM DEV.STAGING.{table_name} staging
            LEFT JOIN DEV.PUBLIC.{table_name} public ON staging.id = public.id
            ORDER BY staging.updated_at DESC
            """
            
            cursor.execute(comparison_sql)
            results = cursor.fetchall()
            
            changes = []
            for row in results:
                record_id, change_type, created_at, updated_at, is_new = row
                
                if change_type == 'INSERT' or is_new:
                    # For new records, assume all columns are "changed"
                    changes.append({
                        'id': record_id,
                        'change_type': 'INSERT',
                        'changed_columns': ['ALL_COLUMNS'],
                        'created_at': created_at,
                        'updated_at': updated_at
                    })
                else:
                    # For existing records, we'll get the columns that could have changed
                    # Since we can't easily compare all columns due to type issues,
                    # we'll use the dictionary to determine relevant columns
                    relevant_columns = self.get_relevant_columns_for_table(table_name)
                    changes.append({
                        'id': record_id,
                        'change_type': 'UPDATE',
                        'changed_columns': relevant_columns,  # Assume all mapped columns changed
                        'created_at': created_at,
                        'updated_at': updated_at
                    })
            
            cursor.close()
            conn.close()
            return changes
            
        except Exception as e:
            print(f"❌ Error detecting changes for {table_name}: {e}")
            if conn:
                conn.close()
            return []
    
    def get_relevant_columns_for_table(self, table_name: str) -> List[str]:
        """
        Get columns that are relevant based on our dictionary mapping
        """
        if table_name not in self.mapping:
            return []
        
        # Get all columns that are mapped in our dictionary
        return list(self.mapping[table_name].keys())
    
    def detect_specific_column_changes(self, table_name: str, record_id: int) -> List[str]:
        """
        For a specific record, detect which columns actually changed
        Uses safe string comparison for all fields
        """
        conn = self.get_db_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            
            # Get the record from both schemas
            staging_query = f"SELECT * FROM DEV.STAGING.{table_name} WHERE id = %s"
            public_query = f"SELECT * FROM DEV.PUBLIC.{table_name} WHERE id = %s"
            
            cursor.execute(staging_query, (record_id,))
            staging_record = cursor.fetchone()
            
            if not staging_record:
                cursor.close()
                conn.close()
                return []
            
            cursor.execute(public_query, (record_id,))
            public_record = cursor.fetchone()
            
            if not public_record:
                # New record - all columns are "changed"
                cursor.close()
                conn.close()
                return ['ALL_COLUMNS']
            
            # Get column names
            cursor.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_catalog = 'DEV' AND table_schema = 'STAGING' AND table_name = '{table_name.upper()}'
                ORDER BY ordinal_position
            """)
            column_names = [row[0].lower() for row in cursor.fetchall()]
            
            # Compare values
            changed_columns = []
            for i, col_name in enumerate(column_names):
                if col_name in ['created_at', 'updated_at']:
                    continue
                
                staging_val = str(staging_record[i]) if staging_record[i] is not None else ''
                public_val = str(public_record[i]) if public_record[i] is not None else ''
                
                if staging_val != public_val:
                    changed_columns.append(col_name)
            
            cursor.close()
            conn.close()
            return changed_columns
            
        except Exception as e:
            print(f"❌ Error comparing record {record_id} in {table_name}: {e}")
            if conn:
                conn.close()
            return []
    
    def get_targeted_fact_updates(self, table_name: str, changed_columns: List[str]) -> Dict[str, List[str]]:
        """Use expanded_dictionary to get exactly which fact table columns need updates"""
        if table_name not in self.mapping:
            return {}
        
        table_mapping = self.mapping[table_name]
        targeted_updates = {}
        
        # Handle INSERT case (all columns)
        if 'ALL_COLUMNS' in changed_columns:
            for column_mappings in table_mapping.values():
                for fact_table, fact_columns in column_mappings.items():
                    if fact_table not in targeted_updates:
                        targeted_updates[fact_table] = []
                    targeted_updates[fact_table].append('FULL_MODEL_RUN')
            return targeted_updates
        
        # Handle UPDATE case (specific columns)
        for column in changed_columns:
            if column in table_mapping:
                fact_mappings = table_mapping[column]
                
                for fact_table, fact_columns in fact_mappings.items():
                    if fact_table not in targeted_updates:
                        targeted_updates[fact_table] = []
                    
                    if isinstance(fact_columns, str):
                        targeted_updates[fact_table].extend(fact_columns.split(','))
                    else:
                        targeted_updates[fact_table].append(fact_columns)
        
        # Remove duplicates
        for fact_table in targeted_updates:
            targeted_updates[fact_table] = list(set(targeted_updates[fact_table]))
        
        return targeted_updates
    
    def execute_fact_table_update(self, fact_table: str) -> bool:
        """Execute fact table update using dbt"""
        try:
            print(f"🔄 Updating {fact_table}...")
            cmd = f"dbt run --select {fact_table} --target dev"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"✅ Successfully updated {fact_table}")
                return True
            else:
                print(f"❌ Failed to update {fact_table}")
                return False
                
        except Exception as e:
            print(f"❌ Exception updating {fact_table}: {e}")
            return False
    
    def process_intelligent_changes(self) -> Dict:
        """Main intelligent CDC processing"""
        start_time = datetime.now()
        print(f"🚀 Starting Intelligent CDC Processing at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("🎯 Comparing staging vs public schemas for precise change detection")
        
        all_changes = {}
        total_records_changed = 0
        all_targeted_updates = {}
        
        print("\n🔍 Detecting changes in staging tables...")
        
        for table in self.staging_tables:
            print(f"\n📋 Processing {table}...")
            
            # Get basic changes
            changes = self.detect_column_changes_simple(table)
            
            if not changes:
                print(f"   ✅ No changes detected")
                continue
            
            all_changes[table] = changes
            total_records_changed += len(changes)
            
            print(f"   📊 Found {len(changes)} changed records")
            
            # Process each changed record
            for change in changes:
                record_id = change['id']
                change_type = change['change_type']
                
                # For UPDATE records, get specific changed columns
                if change_type == 'UPDATE':
                    specific_changes = self.detect_specific_column_changes(table, record_id)
                    if specific_changes:
                        change['changed_columns'] = specific_changes
                        print(f"      🔄 Record {record_id}: {', '.join(specific_changes[:3])}{'...' if len(specific_changes) > 3 else ''}")
                    else:
                        print(f"      🔄 Record {record_id}: Could not detect specific changes")
                else:
                    print(f"      ➕ Record {record_id}: New record (INSERT)")
                
                # Get targeted fact table updates
                targeted = self.get_targeted_fact_updates(table, change['changed_columns'])
                
                for fact_table, columns in targeted.items():
                    if fact_table not in all_targeted_updates:
                        all_targeted_updates[fact_table] = set()
                    all_targeted_updates[fact_table].update(columns)
        
        if not all_targeted_updates:
            print("\n🎉 No fact table updates needed")
            return {
                'status': 'success',
                'duration': (datetime.now() - start_time).total_seconds(),
                'total_records_changed': total_records_changed,
                'updated_fact_tables': []
            }
        
        # Execute fact table updates
        print(f"\n🎯 Updating {len(all_targeted_updates)} affected fact tables...")
        
        successful_updates = []
        failed_updates = []
        
        for fact_table, columns in all_targeted_updates.items():
            print(f"   📝 {fact_table} (affects: {', '.join(list(columns)[:3])}{'...' if len(columns) > 3 else ''})")
            
            if self.execute_fact_table_update(fact_table):
                successful_updates.append(fact_table)
            else:
                failed_updates.append(fact_table)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\n🎉 Intelligent CDC Processing Complete!")
        print(f"⏱️  Duration: {duration.total_seconds():.2f} seconds")
        print(f"📊 Processed: {total_records_changed} record changes")
        print(f"✅ Updated: {len(successful_updates)} fact tables")
        
        if failed_updates:
            print(f"❌ Failed: {len(failed_updates)} fact tables")
        
        return {
            'status': 'success' if not failed_updates else 'partial_success',
            'duration': duration.total_seconds(),
            'total_records_changed': total_records_changed,
            'tables_with_changes': list(all_changes.keys()),
            'updated_fact_tables': successful_updates,
            'failed_fact_tables': failed_updates,
            'targeted_updates': {k: list(v) for k, v in all_targeted_updates.items()}
        }

def main():
    """Main execution"""
    processor = FixedIntelligentCDCProcessor()
    result = processor.process_intelligent_changes()
    
    print("\n" + "="*70)
    print("INTELLIGENT CDC PROCESSING SUMMARY")
    print("="*70)
    print(json.dumps(result, indent=2, default=str))

if __name__ == "__main__":
    main()
