# ml_table_checker.py (MODIFIED check_and_ingest_ml_source_tables)

import os
import json
from datetime import datetime
import hashlib

from config import SnowflakeConfig, FilePaths, load_fqdn_map
from database_manager import DatabaseManager
from snowflake_utils import SnowflakeManager
from confluence_utils import clean_special_characters_iterative


# REMOVED: is_likely_fqdn helper - no longer needed with alias map
# REMOVED: is_likely_fqdn helper from this file. It was never actually used here,
#          only in ddl_utils and now the map handles resolution.


def check_and_ingest_ml_source_tables():
    print("\n--- Starting Snowflake ML Source Table Existence Check & DDL Ingestion ---")

    db_manager = DatabaseManager()
    snowflake_manager = SnowflakeManager()
    
    try:
        # Load the unified map: SOURCE_NAME_UPPER -> FQDN_UPPER
        fqdn_lookup_map = load_fqdn_map() 
    except Exception as e:
        print(f"ERROR: Failed to load FQDN map: {e}")
        db_manager.disconnect()
        snowflake_manager.disconnect()
        return

    # Collect unique source_table entries from 'table_1' only
    unique_source_names_from_content = set() # All source_tables from 'table_1' in content

    try:
        cursor = db_manager.conn.cursor()
        cursor.execute("SELECT parsed_json FROM confluence_parsed_content")
        
        for row in cursor.fetchall():
            parsed_content_json_str = row['parsed_json']
            if parsed_content_json_str:
                parsed_content = json.loads(parsed_content_json_str)
                cleaned_parsed_content = clean_special_characters_iterative(parsed_content)

                for table_data in cleaned_parsed_content.get('tables', []):
                    if table_data.get('id') == 'table_1': # Only process 'table_1'
                        for column in table_data.get('columns', []):
                            source_table_raw = column.get('source_table')
                            if source_table_raw and source_table_raw.strip():
                                source_table_cleaned_upper = source_table_raw.strip().upper()
                                unique_source_names_from_content.add(source_table_cleaned_upper)
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

    # Resolve collected source names to their FQDNs
    # And track any that couldn't be resolved
    resolved_fqdns_for_checking = set() # The final unique FQDNs to check in Snowflake
    unresolved_source_names = set()      # Source names from content that didn't map to an FQDN

    for source_name_upper in unique_source_names_from_content:
        if source_name_upper in fqdn_lookup_map:
            resolved_fqdns_for_checking.add(fqdn_lookup_map[source_name_upper])
        else:
            unresolved_source_names.add(source_name_upper)

    # --- Print warnings for all unique unresolved sources once ---
    if unresolved_source_names:
        print("\nWARNING: The following source_table entries from Confluence content (table_1) were NOT resolved to an FQDN:")
        print("ACTION REQUIRED: Please add these entries (as canonical or alias) to source_to_fqdn_map.json.")
        for src in sorted(list(unresolved_source_names)):
            print(f"  - '{src}'")
    
    if not resolved_fqdns_for_checking:
        print("No valid ML source FQDNs derived from 'table_1's in parsed Confluence content for checking in Snowflake.")
        db_manager.disconnect()
        snowflake_manager.disconnect()
        return

    print(f"\nFound {len(resolved_fqdns_for_checking)} unique ML source FQDNs to check in Snowflake.")

    non_existent_ml_tables = []

    for fqdn_value in sorted(list(resolved_fqdns_for_checking)):
        print(f"\nChecking FQDN: {fqdn_value}...")
        
        parts = fqdn_value.split('.')
        if len(parts) != 3:
            print(f"  ERROR: Resolved FQDN '{fqdn_value}' is not in the expected DATABASE.SCHEMA.TABLE format. Skipping.")
            continue
            
        db_name, schema_name, table_name = parts[0], parts[1], parts[2]
        
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
                ml_db_entry["notes"] += f"Error during DDL check: {check_result['error']}"

            if not check_result["exists"]:
                non_existent_ml_tables.append(fqdn_value)
                ml_db_entry["notes"] += " | Table does not exist in Snowflake."
            
            ml_db_entry["last_checked_on"] = datetime.now().isoformat()

        except Exception as e:
            ml_db_entry["exists_in_snowflake"] = 0
            ml_db_entry["current_extracted_ddl"] = None
            ml_db_entry["notes"] += f" | Critical error during Snowflake check: {e}"
            ml_db_entry["last_checked_on"] = datetime.now().isoformat()
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
