#!/usr/bin/env python3
"""
Surgical CDC Processor - Full Column-by-Column Precision
Compares every single column between staging and public schemas
Provides surgical precision for targeted fact table updates
"""

import os
import sys
import subprocess
import json
import snowflake.connector
from datetime import datetime
from typing import Dict, List, Set, Optional, Tuple, Any

# Import the dictionary
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from expanded_dictionary import fact_table_mapping

class SurgicalCDCProcessor:
    def __init__(self, db_config: Dict = None):
        """Initialize surgical CDC processor with full column precision"""
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
        
        print("🔬 Surgical CDC Processor - Full Column Precision Mode")
        print("🎯 Will compare every single column for surgical accuracy")
    
    def get_db_connection(self):
        """Get database connection"""
        try:
            conn = snowflake.connector.connect(**self.db_config)
            return conn
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return None
    
    def get_table_structure(self, table_name: str) -> List[Dict]:
        """
        Get complete table structure with column names, types, and positions
        """
        conn = self.get_db_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT 
                    column_name,
                    data_type,
                    ordinal_position,
                    is_nullable
                FROM information_schema.columns 
                WHERE table_catalog = 'DEV'
                AND table_schema = 'STAGING'
                AND table_name = '{table_name.upper()}'
                ORDER BY ordinal_position
            """)
            
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    'name': row[0],
                    'type': row[1],
                    'position': row[2],
                    'nullable': row[3] == 'YES'
                })
            
            cursor.close()
            conn.close()
            return columns
            
        except Exception as e:
            print(f"❌ Error getting table structure for {table_name}: {e}")
            if conn:
                conn.close()
            return []
    
    def get_changed_records(self, table_name: str) -> List[Dict]:
        """
        Get all records from staging table with basic change type detection
        """
        conn = self.get_db_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            
            # Get all records from staging with change type
            query = f"""
            SELECT 
                staging.*,
                CASE 
                    WHEN staging.created_at = staging.updated_at THEN 'INSERT'
                    ELSE 'UPDATE'
                END as change_type,
                CASE 
                    WHEN public.id IS NULL THEN true
                    ELSE false
                END as is_new_record
            FROM DEV.STAGING.{table_name} staging
            LEFT JOIN DEV.PUBLIC.{table_name} public ON staging.id = public.id
            ORDER BY staging.updated_at DESC
            """
            
            cursor.execute(query)
            
            # Get column names
            column_names = [desc[0] for desc in cursor.description]
            
            # Convert results to list of dictionaries
            records = []
            for row in cursor.fetchall():
                record = dict(zip(column_names, row))
                records.append(record)
            
            cursor.close()
            conn.close()
            return records
            
        except Exception as e:
            print(f"❌ Error getting changed records for {table_name}: {e}")
            if conn:
                conn.close()
            return []
    
    def compare_record_columns(self, table_name: str, staging_record: Dict, columns: List[Dict]) -> List[str]:
        """
        Compare each column of a staging record with its public schema counterpart
        Returns list of changed column names
        """
        if staging_record.get('is_new_record', False):
            # New record - all columns are "changed"
            return [col['name'] for col in columns if col['name'] not in ['created_at', 'updated_at', 'change_type', 'is_new_record']]
        
        # Get the corresponding record from public schema
        public_record = self.get_public_record(table_name, staging_record['id'])
        if not public_record:
            # Record doesn't exist in public - treat as new
            return [col['name'] for col in columns if col['name'] not in ['created_at', 'updated_at', 'change_type', 'is_new_record']]
        
        changed_columns = []
        
        for col in columns:
            col_name = col['name']
            
            # Skip metadata columns
            if col_name in ['created_at', 'updated_at', 'change_type', 'is_new_record']:
                continue
            
            staging_value = staging_record.get(col_name)
            public_value = public_record.get(col_name)
            
            # Compare values with proper null handling
            if self.values_different(staging_value, public_value, col['type']):
                changed_columns.append(col_name)
                
        return changed_columns
    
    def get_public_record(self, table_name: str, record_id: Any) -> Optional[Dict]:
        """
        Get a specific record from public schema
        """
        conn = self.get_db_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM DEV.PUBLIC.{table_name} WHERE id = %s", (record_id,))
            
            # Get column names
            column_names = [desc[0] for desc in cursor.description]
            
            row = cursor.fetchone()
            if row:
                record = dict(zip(column_names, row))
                cursor.close()
                conn.close()
                return record
            
            cursor.close()
            conn.close()
            return None
            
        except Exception as e:
            print(f"❌ Error getting public record {record_id} from {table_name}: {e}")
            if conn:
                conn.close()
            return None
    
    def values_different(self, staging_val: Any, public_val: Any, data_type: str) -> bool:
        """
        Compare two values considering data type and null handling
        """
        # Handle None/NULL values
        if staging_val is None and public_val is None:
            return False
        if staging_val is None or public_val is None:
            return True
        
        # Convert to strings for comparison (handles all data types)
        staging_str = str(staging_val).strip()
        public_str = str(public_val).strip()
        
        # Special handling for different data types
        if data_type in ['boolean']:
            # Boolean comparison
            staging_bool = str(staging_val).lower() in ['true', '1', 't', 'yes']
            public_bool = str(public_val).lower() in ['true', '1', 't', 'yes']
            return staging_bool != public_bool
        
        elif data_type in ['numeric', 'integer', 'bigint', 'smallint']:
            # Numeric comparison
            try:
                return float(staging_str) != float(public_str)
            except (ValueError, TypeError):
                return staging_str != public_str
        
        else:
            # String comparison (default)
            return staging_str != public_str
    
    def get_targeted_fact_updates(self, table_name: str, changed_columns: List[str]) -> Dict[str, List[str]]:
        """
        Use expanded_dictionary to get exactly which fact table columns need updates
        """
        if table_name not in self.mapping:
            print(f"⚠️  No mapping found for table: {table_name}")
            return {}
        
        table_mapping = self.mapping[table_name]
        targeted_updates = {}
        
        print(f"   🎯 Analyzing {len(changed_columns)} changed columns: {', '.join(changed_columns[:5])}{'...' if len(changed_columns) > 5 else ''}")
        
        for column in changed_columns:
            if column in table_mapping:
                fact_mappings = table_mapping[column]
                print(f"      📝 {column} → affects {len(fact_mappings)} fact tables")
                
                for fact_table, fact_columns in fact_mappings.items():
                    if fact_table not in targeted_updates:
                        targeted_updates[fact_table] = []
                    
                    # Handle comma-separated columns (e.g., "poc_name,primary_contact_phone")
                    if isinstance(fact_columns, str):
                        targeted_updates[fact_table].extend(fact_columns.split(','))
                    else:
                        targeted_updates[fact_table].append(fact_columns)
            else:
                print(f"      ⚠️  {column} not in dictionary - no fact table mapping")
        
        # Remove duplicates and clean up
        for fact_table in targeted_updates:
            targeted_updates[fact_table] = [col.strip() for col in set(targeted_updates[fact_table])]
        
        return targeted_updates
    
    def execute_fact_table_update(self, fact_table: str, affected_columns: List[str]) -> bool:
        """
        Execute targeted update for specific fact table
        """
        print(f"🔄 Updating {fact_table}")
        print(f"   📝 Affected columns: {', '.join(affected_columns[:5])}{'...' if len(affected_columns) > 5 else ''}")
        
        try:
            cmd = f"dbt run --select {fact_table} --target dev"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"✅ Successfully updated {fact_table}")
                return True
            else:
                print(f"❌ Failed to update {fact_table}")
                # Show relevant error lines only
                error_lines = [line for line in result.stderr.split('\n') 
                              if any(keyword in line.lower() for keyword in ['error', 'fail', 'exception'])]
                for line in error_lines[-2:]:
                    if line.strip():
                        print(f"   {line.strip()}")
                return False
                
        except Exception as e:
            print(f"❌ Exception updating {fact_table}: {e}")
            return False
    
    def process_surgical_changes(self) -> Dict:
        """
        Main surgical CDC processing with full column-by-column precision
        """
        start_time = datetime.now()
        print(f"🚀 Starting Surgical CDC Processing at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("🔬 Full column-by-column precision mode activated")
        
        total_records_analyzed = 0
        total_columns_compared = 0
        total_changes_detected = 0
        all_targeted_updates = {}
        detailed_analysis = {}
        
        print("\n🔍 Surgical analysis of all staging tables...")
        
        for table in self.staging_tables:
            print(f"\n📋 Analyzing {table} with surgical precision...")
            
            # Get table structure
            columns = self.get_table_structure(table)
            if not columns:
                print(f"   ⚠️  Could not get table structure - skipping")
                continue
            
            # Get changed records
            records = self.get_changed_records(table)
            if not records:
                print(f"   ✅ No records in staging")
                continue
            
            print(f"   📊 Found {len(records)} records to analyze")
            print(f"   🔍 Will compare {len(columns)} columns per record")
            
            table_analysis = {
                'records_analyzed': len(records),
                'columns_per_record': len(columns),
                'changes': []
            }
            
            for record in records:
                record_id = record.get('id', 'Unknown')
                change_type = record.get('change_type', 'Unknown')
                
                # Perform surgical column comparison
                changed_columns = self.compare_record_columns(table, record, columns)
                
                if changed_columns:
                    total_changes_detected += len(changed_columns)
                    
                    print(f"      🔬 Record {record_id} ({change_type}):")
                    print(f"         📝 {len(changed_columns)} columns changed: {', '.join(changed_columns[:5])}{'...' if len(changed_columns) > 5 else ''}")
                    
                    # Get targeted fact table updates
                    targeted = self.get_targeted_fact_updates(table, changed_columns)
                    
                    # Accumulate all targeted updates
                    for fact_table, fact_columns in targeted.items():
                        if fact_table not in all_targeted_updates:
                            all_targeted_updates[fact_table] = {
                                'columns': set(),
                                'changes': []
                            }
                        
                        all_targeted_updates[fact_table]['columns'].update(fact_columns)
                        all_targeted_updates[fact_table]['changes'].append({
                            'source_table': table,
                            'record_id': record_id,
                            'change_type': change_type,
                            'changed_columns': changed_columns
                        })
                    
                    table_analysis['changes'].append({
                        'record_id': record_id,
                        'change_type': change_type,
                        'changed_columns': changed_columns,
                        'targeted_fact_tables': list(targeted.keys())
                    })
                else:
                    print(f"      ✅ Record {record_id}: No changes detected")
                
                total_records_analyzed += 1
                total_columns_compared += len(columns)
            
            detailed_analysis[table] = table_analysis
        
        if not all_targeted_updates:
            print("\n🎉 Surgical analysis complete - no fact table updates needed")
            return {
                'status': 'success',
                'duration': (datetime.now() - start_time).total_seconds(),
                'records_analyzed': total_records_analyzed,
                'columns_compared': total_columns_compared,
                'changes_detected': 0,
                'fact_tables_updated': []
            }
        
        # Execute surgical updates
        print(f"\n🎯 Executing surgical updates for {len(all_targeted_updates)} fact tables...")
        print(f"🔬 Based on {total_changes_detected} column-level changes detected")
        
        successful_updates = []
        failed_updates = []
        
        for fact_table, update_info in all_targeted_updates.items():
            affected_columns = list(update_info['columns'])
            changes_count = len(update_info['changes'])
            
            print(f"\n🔄 {fact_table}:")
            print(f"   📊 Triggered by {changes_count} record changes")
            print(f"   🎯 Columns affected: {', '.join(affected_columns[:5])}{'...' if len(affected_columns) > 5 else ''}")
            
            if self.execute_fact_table_update(fact_table, affected_columns):
                successful_updates.append(fact_table)
            else:
                failed_updates.append(fact_table)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\n🎉 Surgical CDC Processing Complete!")
        print(f"⏱️  Duration: {duration.total_seconds():.2f} seconds")
        print(f"🔬 Records analyzed: {total_records_analyzed}")
        print(f"📊 Columns compared: {total_columns_compared:,}")
        print(f"🎯 Changes detected: {total_changes_detected}")
        print(f"✅ Fact tables updated: {len(successful_updates)}")
        
        if failed_updates:
            print(f"❌ Failed updates: {len(failed_updates)}")
        
        return {
            'status': 'success' if not failed_updates else 'partial_success',
            'duration': duration.total_seconds(),
            'records_analyzed': total_records_analyzed,
            'columns_compared': total_columns_compared,
            'changes_detected': total_changes_detected,
            'fact_tables_updated': successful_updates,
            'failed_fact_tables': failed_updates,
            'detailed_analysis': detailed_analysis,
            'targeted_updates': {
                k: {
                    'affected_columns': list(v['columns']),
                    'change_count': len(v['changes'])
                } for k, v in all_targeted_updates.items()
            }
        }
    
    def analyze_single_table(self, table_name: str):
        """
        Perform detailed surgical analysis of a single table for debugging
        """
        print(f"🔬 Surgical Analysis of {table_name}")
        print("=" * 50)
        
        # Get table structure
        columns = self.get_table_structure(table_name)
        print(f"📋 Table has {len(columns)} columns")
        
        # Get records
        records = self.get_changed_records(table_name)
        print(f"📊 Found {len(records)} records to analyze")
        
        if not records:
            print("✅ No records to analyze")
            return
        
        for record in records:
            record_id = record.get('id', 'Unknown')
            change_type = record.get('change_type', 'Unknown')
            
            print(f"\n🔍 Analyzing Record {record_id} ({change_type}):")
            
            changed_columns = self.compare_record_columns(table_name, record, columns)
            
            if changed_columns:
                print(f"   📝 {len(changed_columns)} columns changed:")
                for col in changed_columns:
                    staging_val = record.get(col, 'N/A')
                    print(f"      • {col}: {str(staging_val)[:50]}{'...' if len(str(staging_val)) > 50 else ''}")
                
                # Show targeted updates
                targeted = self.get_targeted_fact_updates(table_name, changed_columns)
                if targeted:
                    print(f"   🎯 Will update {len(targeted)} fact tables:")
                    for fact_table, fact_cols in targeted.items():
                        print(f"      → {fact_table}: {', '.join(fact_cols)}")
            else:
                print("   ✅ No changes detected")

def main():
    """Main execution"""
    processor = SurgicalCDCProcessor()
    
    # Check for single table analysis
    if len(sys.argv) > 1:
        table_name = sys.argv[1]
        processor.analyze_single_table(table_name)
    else:
        # Run full surgical CDC processing
        result = processor.process_surgical_changes()
        
        print("\n" + "="*70)
        print("SURGICAL CDC PROCESSING SUMMARY")
        print("="*70)
        
        # Print comprehensive summary
        summary = {
            'status': result['status'],
            'duration_seconds': result['duration'],
            'precision_metrics': {
                'records_analyzed': result['records_analyzed'],
                'columns_compared': result['columns_compared'],
                'changes_detected': result['changes_detected']
            },
            'fact_table_updates': {
                'successful': result['fact_tables_updated'],
                'failed': result.get('failed_fact_tables', [])
            },
            'surgical_precision': result.get('targeted_updates', {})
        }
        
        print(json.dumps(summary, indent=2, default=str))

if __name__ == "__main__":
    main()
