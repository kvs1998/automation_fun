# ml_table_checker.py (MODIFIED to use resolver map)

import os
import json
from datetime import datetime
import hashlib

from config import CHECK_ENVIRONMENTS, DEPLOYMENT_ENVIRONMENT, load_fqdn_resolver # NEW: Use load_fqdn_resolver
from database_manager import DatabaseManager
from snowflake_utils import SnowflakeManager
from confluence_utils import clean_special_characters_iterative


def check_and_ingest_ml_source_tables():
    print("\n--- Starting Snowflake ML Source Table Existence Check & DDL Ingestion (Cross-Environment) ---")

    db_manager = DatabaseManager()
    
    try:
        # Load the unified resolver map: SOURCE_NAME_UPPER -> {ENV_UPPER -> {"fqdn": FQDN_UPPER, "object_type": OBJECT_TYPE_UPPER}}
        fqdn_resolver_map = load_fqdn_resolver() 
    except Exception as e:
        print(f"ERROR: Failed to load FQDN resolver map: {e}")
        db_manager.disconnect()
        return

    unique_source_names_from_content = set()
    try:
        cursor = db_manager.conn.cursor()
        cursor.execute("SELECT parsed_json FROM confluence_parsed_content")
        
        for row in cursor.fetchall():
            parsed_content_json_str = row['parsed_json']
            if parsed_content_json_str:
                parsed_content = json.loads(parsed_content_json_str)
                cleaned_parsed_content = clean_special_characters_iterative(parsed_content)

                for table_data in cleaned_parsed_content.get('tables', []):
                    if table_data.get('id') == 'table_1':
                        for column in table_data.get('columns', []):
                            source_table_raw = column.get('source_table')
                            if source_table_raw and source_table_raw.strip():
                                source_table_cleaned_upper = source_table_raw.strip().upper()
                                unique_source_names_from_content.add(source_table_cleaned_upper)
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in confluence_parsed_content for a page: {e}")
        db_manager.disconnect()
        return
    except Exception as e:
        print(f"ERROR: Failed to retrieve source tables from DB: {e}")
        db_manager.disconnect()
        return

    # --- Resolve source names for each environment ---
    # resolved_fqdns_per_env: { canonical_source_name_upper: {env_name_upper: {"fqdn": FQDN, "object_type": TYPE}}}
    resolved_cross_env_fqdns_by_source = {} 
    unresolved_source_names_from_content = set()      

    for source_name_upper in unique_source_names_from_content:
        if source_name_upper in fqdn_resolver_map:
            resolved_cross_env_fqdns_by_source[source_name_upper] = fqdn_resolver_map[source_name_upper]
        else:
            unresolved_source_names_from_content.add(source_name_upper)

    # --- Print warnings for unmapped sources ---
    if unresolved_source_names_from_content:
        print("\nWARNING: The following source_table entries from Confluence content (table_1) were NOT resolved to any FQDN:")
        print(f"ACTION REQUIRED: Please add these entries (as canonical or alias) to source_to_fqdn_resolver.json.")
        for src in sorted(list(unresolved_source_names_from_content)):
            print(f"  - '{src}'")
    
    if not resolved_cross_env_fqdns_by_source:
        print("No valid logical source names derived from 'table_1's in parsed Confluence content for checking across environments.")
        db_manager.disconnect()
        return

    print(f"\nFound {len(resolved_cross_env_fqdns_by_source)} unique logical source names to check across environments.")

    all_non_existent_ml_objects = [] 
    
    # --- Main Loop: Iterate through each environment to check ---
    for env_name in CHECK_ENVIRONMENTS:
        print(f"\n--- Checking Snowflake Environment: {env_name.upper()} ---")
        sf_manager_env = None
        
        try:
            sf_manager_env = SnowflakeManager(environment_name=env_name)
        except ValueError as e:
            print(f"SKIPPING environment '{env_name}': Configuration error: {e}")
            continue
        except Exception as e:
            print(f"SKIPPING environment '{env_name}': Could not connect to Snowflake: {e}")
            continue

        non_existent_objects_in_this_env = []

        # Iterate through *all* resolved logical source names
        for source_name_upper, env_details_map in resolved_cross_env_fqdns_by_source.items():
            # Get the FQDN and object_type specific to *this* environment
            env_specific_details = env_details_map.get(env_name.upper())

            if not env_specific_details:
                print(f"  INFO: Logical source '{source_name_upper}' has no mapping for environment '{env_name}'. Skipping check for this env.")
                # Insert a non-existent placeholder for this env if not present?
                # For now, just skip. We only record if we check.
                continue

            fqdn_value = env_specific_details["fqdn"]
            object_type = env_specific_details["object_type"] 

            print(f"  Checking {object_type} FQDN: {fqdn_value} (from source: {source_name_upper}) in {env_name}...")
            
            parts = fqdn_value.split('.')
            if len(parts) != 3:
                print(f"    ERROR: Resolved FQDN '{fqdn_value}' is not in the expected DATABASE.SCHEMA.TABLE format. Skipping.")
                continue
                
            db_name, schema_name, table_name = parts[0], parts[1], parts[2]
            
            ml_db_entry = { 
                "fqdn": fqdn_value,
                "environment": env_name.upper(),
                "object_type": object_type,
                "db_name": db_name,
                "schema_name": schema_name,
                "table_name": table_name,
                "notes": ""
            }
            
            try:
                check_result = sf_manager_env.check_table_existence_and_get_ddl(fqdn_value, object_type=object_type) 
                
                ml_db_entry["exists_in_snowflake"] = 1 if check_result["exists"] else 0
                ml_db_entry["current_extracted_ddl"] = check_result["ddl"]
                
                if check_result["error"]:
                    ml_db_entry["notes"] += f"Error during DDL check: {check_result['error']}"

                if not check_result["exists"]:
                    non_existent_objects_in_this_env.append(f"{fqdn_value} ({env_name.upper()}, {object_type})")
                    ml_db_entry["notes"] += f" | {object_type} does not exist in Snowflake."
                
                ml_db_entry["last_checked_on"] = datetime.now().isoformat()

            except Exception as e:
                ml_db_entry["exists_in_snowflake"] = 0
                ml_db_entry["current_extracted_ddl"] = None
                ml_db_entry["notes"] += f" | Critical error during Snowflake check: {e}"
                ml_db_entry["last_checked_on"] = datetime.now().isoformat()
                print(f"    ERROR: Critical error checking {fqdn_value} in {env_name}: {e}")

            try:
                db_manager.insert_or_update_snowflake_ml_metadata(ml_db_entry)
                print(f"  ML source {object_type} metadata for {fqdn_value} in {env_name} stored/updated in DB (DDL history managed).")
            except Exception as e:
                print(f"    CRITICAL ERROR: Could not store/update DB for {fqdn_value} in {env_name}: {e}")
        
        sf_manager_env.disconnect()
        all_non_existent_ml_objects.extend(non_existent_objects_in_this_env)


    db_manager.disconnect()
    print("\n--- Snowflake ML Source Table Existence Check & DDL Ingestion Complete (Cross-Environment) ---")
    if all_non_existent_ml_objects:
        print("\nACTION REQUIRED: The following ML source objects were NOT found across checked Snowflake environments:")
        for entry in all_non_existent_ml_objects:
            print(f"  - {entry}")
    else:
        print("\nAll referenced ML source objects exist in ALL checked Snowflake environments!")


if __name__ == "__main__":
    check_and_ingest_ml_source_tables()
