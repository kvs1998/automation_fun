# ml_table_checker.py (REVERTED/MODIFIED check_and_ingest_ml_source_tables)

import os
import json
from datetime import datetime
import hashlib

from config import SnowflakeConfig, FilePaths, load_fqdn_map
from database_manager import DatabaseManager
from snowflake_utils import SnowflakeManager
from confluence_utils import clean_special_characters_iterative


# REMOVED: is_likely_fqdn helper

def check_and_ingest_ml_source_tables():
    print("\n--- Starting Snowflake ML Source Table Existence Check & DDL Ingestion ---")

    db_manager = DatabaseManager()
    snowflake_manager = SnowflakeManager()
    
    try:
        fqdn_map = load_fqdn_map() # Load the map (keys are uppercase)
    except Exception as e:
        print(f"ERROR: Failed to load FQDN map: {e}")
        db_manager.disconnect()
        snowflake_manager.disconnect()
        return

    # Collect unique source_table entries from 'table_1' only
    unique_source_tables_from_db_table1 = set()

    try:
        cursor = db_manager.conn.cursor()
        cursor.execute("SELECT page_id, parsed_json FROM confluence_parsed_content")
        
        for row in cursor.fetchall():
            # page_id = row['page_id'] # No longer directly needed for ML metadata table
            parsed_content_json_str = row['parsed_json']
            if parsed_content_json_str:
                parsed_content = json.loads(parsed_content_json_str)
                cleaned_parsed_content = clean_special_characters_iterative(parsed_content)

                # NEW: Iterate only tables with id 'table_1'
                for table_data in cleaned_parsed_content.get('tables', []):
                    if table_data.get('id') == 'table_1': # Only process 'table_1'
                        for column in table_data.get('columns', []):
                            source_table_raw = column.get('source_table')
                            if source_table_raw and source_table_raw.strip():
                                source_table_cleaned_upper = source_table_raw.strip().upper()
                                # All source_table entries are expected to be keys in fqdn_map
                                if source_table_cleaned_upper in fqdn_map:
                                    unique_source_tables_from_db_table1.add(source_table_cleaned_upper)
                                else:
                                    print(f"WARNING: Source '{source_table_raw}' (from some page) not found in FQDN map. Skipping check for this source.")

    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in confluence_parsed_content for a page: {e}")
        db_manager.disconnect()
        snowflake_manager.disconnect()
        return
    except Exception as e:
        print(f"ERROR: Failed to retrieve source tables from DB: {e}")
        db_manager.disconnect()
        snowflake_manager.disconnect()
        return

    if not unique_source_tables_from_db_table1:
        print("No source_table entries found in 'table_1's in the parsed Confluence content to check.")
        db_manager.disconnect()
        snowflake_manager.disconnect()
        return

    # NEW: Map these sources to their FQDNs for checking
    fqdns_to_check = {source_key_upper: fqdn_map[source_key_upper] for source_key_upper in unique_source_tables_from_db_table1}

    print(f"Found {len(fqdns_to_check)} unique ML source FQDNs derived from 'table_1's for checking in Snowflake.")

    non_existent_ml_tables = []

    for source_key_upper, fqdn_value in fqdns_to_check.items():
        print(f"\nChecking FQDN: {fqdn_value} (mapped from source: {source_key_upper})...")
        
        db_name, schema_name, table_name = fqdn_value.split('.', 2)
        
        ml_db_entry = {
            "fqdn": fqdn_value,
            "db_name": db_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "notes": ""
        }
        
        try:
            check_result = snowflake_manager.check_table_existence_and_get_ddl(fqdn_value)
            
            ml_db_entry["exists_in_snowflake"] = 1 if check_result["exists"] else 0
            ml_db_entry["current_extracted_ddl"] = check_result["ddl"]
            
            if check_result["error"]:
                ml_db_entry["notes"] += f"Error during check: {check_result['error']}"

            if not check_result["exists"]:
                non_existent_ml_tables.append(fqdn_value)
                ml_db_entry["notes"] += " | Table does not exist in Snowflake."
            
        except Exception as e:
            ml_db_entry["exists_in_snowflake"] = 0
            ml_db_entry["current_extracted_ddl"] = None
            ml_db_entry["notes"] += f" | Critical error during Snowflake check: {e}"
            print(f"  ERROR: Critical error checking {fqdn_value}: {e}")

        try:
            db_manager.insert_or_update_snowflake_ml_metadata(ml_db_entry)
            print(f"  ML source table metadata for {fqdn_value} stored/updated in DB (DDL history managed).")
        except Exception as e:
            print(f"  CRITICAL ERROR: Could not store/update DB for {fqdn_value}: {e}")

    db_manager.disconnect()
    snowflake_manager.disconnect()

    print("\n--- Snowflake ML Source Table Existence Check & DDL Ingestion Complete ---")
    if non_existent_ml_tables:
        print("\nACTION REQUIRED: The following ML source tables were NOT found in Snowflake:")
        for fqdn in non_existent_ml_tables:
            print(f"  - {fqdn}")
    else:
        print("\nAll referenced ML source tables exist in Snowflake!")


if __name__ == "__main__":
    check_and_ingest_ml_source_tables()
