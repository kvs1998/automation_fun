# ml_table_checker.py (MODIFIED check_and_ingest_ml_source_tables)

import os
import json
from datetime import datetime
import hashlib

from config import SnowflakeConfig, FilePaths, load_fqdn_map
from database_manager import DatabaseManager
from snowflake_utils import SnowflakeManager
from confluence_utils import clean_special_characters_iterative # Assuming this is available


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

    # A dictionary to store unique FQDNs and the page_ids that reference them
    fqdns_from_parsed_content = {} # Key: Resolved FQDN, Value: set of page_ids

    try:
        cursor = db_manager.conn.cursor()
        cursor.execute("SELECT page_id, parsed_json FROM confluence_parsed_content")
        
        for row in cursor.fetchall():
            page_id = row['page_id']
            parsed_content_json_str = row['parsed_json']
            if parsed_content_json_str:
                parsed_content = json.loads(parsed_content_json_str)
                # Apply deep cleaning to the loaded JSON from DB
                cleaned_parsed_content = clean_special_characters_iterative(parsed_content) 

                for table_data in cleaned_parsed_content.get('tables', []):
                    # NEW CONDITION: Only process if table_id is 'table_1'
                    if table_data.get('id') == 'table_1': 
                        for column in table_data.get('columns', []):
                            source_table_raw = column.get('source_table')
                            if source_table_raw and source_table_raw.strip():
                                source_table_cleaned_upper = source_table_raw.strip().upper() # Standardize to uppercase
                                
                                resolved_fqdn = None
                                # NEW: ALWAYS lookup in fqdn_map
                                if source_table_cleaned_upper in fqdn_map:
                                    resolved_fqdn = fqdn_map[source_table_cleaned_upper]
                                
                                if resolved_fqdn:
                                    if resolved_fqdn not in fqdns_from_parsed_content:
                                        fqdns_from_parsed_content[resolved_fqdn] = set()
                                    fqdns_from_parsed_content[resolved_fqdn].add(page_id)
                                else:
                                    print(f"WARNING: Source '{source_table_raw}' (from page {page_id}, table_1) not found in FQDN map. Skipping check.")
                                    
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in confluence_parsed_content for page ID: {page_id}: {e}")
        db_manager.disconnect()
        snowflake_manager.disconnect()
        return
    except Exception as e:
        print(f"ERROR: Failed to retrieve source tables from DB: {e}")
        db_manager.disconnect()
        snowflake_manager.disconnect()
        return

    if not fqdns_from_parsed_content:
        print("No referenced ML source tables found in the parsed Confluence content (from table_1) to check.")
        db_manager.disconnect()
        snowflake_manager.disconnect()
        return

    print(f"Found {len(fqdns_from_parsed_content)} unique ML source FQDNs (from table_1) referenced across pages.")

    non_existent_ml_tables = []

    for fqdn_value, page_ids_set in fqdns_from_parsed_content.items():
        print(f"\nChecking FQDN: {fqdn_value} (referenced by pages: {sorted(list(page_ids_set))})...")
        
        db_name, schema_name, table_name = fqdn_value.split('.', 2)
        
        ml_db_entry = { # This dict will be passed to insert_or_update
            "fqdn": fqdn_value,
            "db_name": db_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "referencing_page_ids": json.dumps(sorted(list(page_ids_set))), # Convert set to sorted list then JSON string
            "notes": ""
        }
        
        try:
            check_result = snowflake_manager.check_table_existence_and_get_ddl(fqdn_value)
            
            ml_db_entry["exists_in_snowflake"] = 1 if check_result["exists"] else 0
            ml_db_entry["current_extracted_ddl"] = check_result["ddl"] # Store the new DDL
            
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
