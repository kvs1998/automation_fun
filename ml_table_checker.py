# ml_table_checker.py (MODIFIED check_and_ingest_ml_source_tables)

import os
import json
from datetime import datetime
import hashlib # For DDL hash comparison

from config import SnowflakeConfig, FilePaths, load_fqdn_map
from database_manager import DatabaseManager
from snowflake_utils import SnowflakeManager


def check_and_ingest_ml_source_tables():
    print("\n--- Starting Snowflake ML Source Table Existence Check & DDL Ingestion ---")

    db_manager = DatabaseManager()
    snowflake_manager = SnowflakeManager()
    
    try:
        fqdn_map = load_fqdn_map()
    except Exception as e:
        print(f"ERROR: Failed to load FQDN map: {e}")
        db_manager.disconnect()
        snowflake_manager.disconnect()
        return

    unique_source_tables_from_db = set()
    try:
        cursor = db_manager.conn.cursor()
        cursor.execute("SELECT parsed_json FROM confluence_parsed_content")
        
        for row in cursor.fetchall():
            parsed_content_json_str = row['parsed_json']
            if parsed_content_json_str:
                parsed_content = json.loads(parsed_content_json_str)
                for table_data in parsed_content.get('tables', []):
                    if table_data.get('table_type') == 'primary_definitions':
                        for column in table_data.get('columns', []):
                            source_table = column.get('source_table')
                            if source_table and source_table.strip():
                                unique_source_tables_from_db.add(source_table.strip().upper())
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in confluence_parsed_content: {e}")
        db_manager.disconnect()
        snowflake_manager.disconnect()
        return
    except Exception as e:
        print(f"ERROR: Failed to retrieve source tables from DB: {e}")
        db_manager.disconnect()
        snowflake_manager.disconnect()
        return

    referenced_fqdns_to_check = {}
    for source_table_key_upper in unique_source_tables_from_db:
        if source_table_key_upper in fqdn_map:
            referenced_fqdns_to_check[source_table_key_upper] = fqdn_map[source_table_key_upper]
        else:
            print(f"WARNING: Source '{source_table_key_upper}' found in parsed content but not in FQDN map. Skipping check.")


    if not referenced_fqdns_to_check:
        print("No referenced ML source tables found in the FQDN map to check.")
        db_manager.disconnect()
        snowflake_manager.disconnect()
        return

    print(f"Found {len(referenced_fqdns_to_check)} unique referenced ML source tables to check in Snowflake.")

    non_existent_ml_tables = []

    for source_key_upper, fqdn_value in referenced_fqdns_to_check.items():
        print(f"\nChecking FQDN: {fqdn_value} (from Confluence source: {source_key_upper})...")
        
        db_name, schema_name, table_name = fqdn_value.split('.', 2)
        
        ml_db_entry_new = { # This dict will be passed to insert_or_update
            "fqdn": fqdn_value,
            "db_name": db_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "notes": ""
        }
        # Retrieve existing record to compare hashes and set previous DDL
        existing_ml_db_record = db_manager.get_snowflake_ml_metadata(fqdn_value)

        try:
            check_result = snowflake_manager.check_table_existence_and_get_ddl(fqdn_value)
            
            ml_db_entry_new["exists_in_snowflake"] = 1 if check_result["exists"] else 0
            ml_db_entry_new["current_extracted_ddl"] = check_result["ddl"] # Store the new DDL
            
            if check_result["error"]:
                ml_db_entry_new["notes"] += f"Error during check: {check_result['error']}"

            if not check_result["exists"]:
                non_existent_ml_tables.append(fqdn_value)
                ml_db_entry_new["notes"] += " | Table does not exist in Snowflake."
            
            # The insert_or_update_snowflake_ml_metadata will handle DDL hash comparison and previous DDL logic
            
        except Exception as e:
            ml_db_entry_new["exists_in_snowflake"] = 0
            ml_db_entry_new["current_extracted_ddl"] = None # If error fetching, no DDL extracted
            ml_db_entry_new["notes"] += f" | Critical error during Snowflake check: {e}"
            print(f"  ERROR: Critical error checking {fqdn_value}: {e}")

        # Store/Update this in the database. The DB manager now handles the DDL change logic.
        try:
            db_manager.insert_or_update_snowflake_ml_metadata(ml_db_entry_new)
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
