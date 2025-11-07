# column_mapper.py
import os
import json
from datetime import datetime
import argparse
# from fuzzywuzzy import fuzz, process # REMOVED: fuzzywuzzy
from rapidfuzz import fuzz, process # NEW: rapidfuzz
import re # Needed for cleaning column names

from config import CHECK_ENVIRONMENTS, FilePaths, load_column_mapper_config, load_fqdn_resolver
from database_manager import DatabaseManager
from confluence_utils import clean_special_characters_iterative
from ddl_utils import extract_columns_from_ddl


def map_confluence_columns_to_ml_ddl():
    """
    Performs fuzzy matching to map Confluence-defined columns to actual
    Snowflake ML table DDL columns. Stores results in confluence_ml_column_map.
    Manages user overrides and tracks mapping status.
    """
    print("\n--- Starting Column Mapper ---")

    db_manager = DatabaseManager()
    
    try:
        column_mapper_config = load_column_mapper_config()
    except Exception as e:
        print(f"ERROR: Failed to load column mapper config: {e}")
        db_manager.disconnect()
        return

    match_threshold = column_mapper_config.get("match_threshold", 80)
    match_strategy_str = column_mapper_config.get("match_strategy", "TOKEN_SET_RATIO").upper()
    exact_match_only = column_mapper_config.get("exact_match_only", False)
    
    # Map strategy string to rapidfuzz function
    if match_strategy_str == "RATIO":
        match_strategy = fuzz.ratio
    elif match_strategy_str == "PARTIAL_RATIO":
        match_strategy = fuzz.partial_ratio
    elif match_strategy_str == "TOKEN_SORT_RATIO":
        match_strategy = fuzz.token_sort_ratio
    elif match_strategy_str == "TOKEN_SET_RATIO":
        match_strategy = fuzz.token_set_ratio
    else:
        print(f"ERROR: Invalid match_strategy '{match_strategy_str}'. Exiting.")
        db_manager.disconnect()
        return

    print(f"Column Mapper Config: Threshold={match_threshold}, Strategy='{match_strategy_str}', Exact Only={exact_match_only}")

    # --- 1. Identify pages needing mapping ---
    # Pages that are PARSED_OK (from data_parser.py)
    # And whose ML source DDL hash has changed, or never mapped, or previous mapping failed/is_active=0
    cursor = db_manager.conn.cursor()
    cursor.execute("""
        SELECT 
            cpm.page_id,
            cpm.api_title,
            cpc.parsed_json,
            cpm.last_parsed_content_hash as confluence_metadata_hash_at_parse_time -- Hash of content from metadata_ingestor when parsed
        FROM confluence_page_metadata cpm
        JOIN confluence_parsed_content cpc ON cpm.page_id = cpc.page_id
        WHERE cpm.extraction_status = 'PARSED_OK' AND cpm.user_verified = 1
    """)
    pages_to_map = cursor.fetchall()

    if not pages_to_map:
        print("No PARSED_OK and user-verified Confluence pages found for column mapping.")
        db_manager.disconnect()
        return

    print(f"Found {len(pages_to_map)} approved pages ready for column mapping.")

    # --- 2. Load FQDN Resolver and prepare ML DDL cache ---
    try:
        fqdn_resolver_map = load_fqdn_resolver()
    except Exception as e:
        print(f"ERROR: Failed to load FQDN resolver map: {e}")
        db_manager.disconnect()
        return

    # Cache ML DDLs to avoid repeated DB queries
    ml_ddl_cache = {} # { (fqdn, env, obj_type): { 'current_ddl_hash', 'current_extracted_ddl' } }
    
    cursor.execute(f"SELECT fqdn, environment, object_type, current_ddl_hash, current_extracted_ddl FROM {FilePaths.SNOWFLAKE_ML_SOURCE_TABLE}")
    for row in cursor.fetchall():
        key = (row['fqdn'], row['environment'], row['object_type'])
        ml_ddl_cache[key] = {'current_ddl_hash': row['current_ddl_hash'], 'current_extracted_ddl': row['current_extracted_ddl']}


    # --- Column Mapping Loop ---
    for page_row in pages_to_map:
        confluence_page_id = page_row['page_id']
        confluence_api_title = page_row['api_title']
        confluence_parsed_json_str = page_row['parsed_json']
        confluence_metadata_hash_at_parse_time = page_row['confluence_metadata_hash_at_parse_time'] # Hash of metadata when content was parsed

        print(f"\n--- Mapping columns for page: '{confluence_api_title}' (ID: {confluence_page_id}) ---")

        try:
            parsed_content = json.loads(confluence_parsed_json_str)
            
            confluence_target_columns_to_map = [] # List of { 'source_table', 'target_field_name', 'data_type', 'is_primary_key', 'add_source_to_target' }
            all_current_confluence_target_names = set() # Track all columns that are currently in Confluence for orphan detection

            for table_data in parsed_content.get('tables', []):
                if table_data.get('id') == 'table_1': # Only process 'table_1'
                    for column_detail in table_data.get('columns', []):
                        # Corrected key for filtering and tracking
                        all_current_confluence_target_names.add(column_detail.get('target_field_name')) # Track for orphans
                        if column_detail.get('add_source_to_target') == True: # Only map columns marked for inclusion
                            confluence_target_columns_to_map.append(column_detail)
            
            if not confluence_target_columns_to_map:
                print(f"  No columns marked 'add_source_to_target: True' found in 'table_1' for page {confluence_page_id}. Skipping mapping for this page.")
                # Proceed to orphan cleanup even if no columns to map
            
            # Find the first source_table in the Confluence page's columns (from ALL columns, not just target_columns_to_map)
            # This is to resolve the ML source FQDN, which applies to the whole page/table.
            first_source_table_from_conf = next((col['source_table'] for table_d in parsed_content.get('tables',[]) if table_d.get('id')=='table_1' for col in table_d.get('columns',[]) if col.get('source_table')), None)

            if not first_source_table_from_conf:
                print(f"  WARNING: No 'source_table' found in Confluence columns for page {confluence_page_id}. Cannot resolve ML source. Skipping mapping for this page.")
                continue # Cannot map if no source_table
            
            # Resolve this source_table across all CHECK_ENVIRONMENTS
            resolved_env_fqdns_map = fqdn_resolver_map.get(first_source_table_from_conf.upper())

            if not resolved_env_fqdns_map:
                print(f"  WARNING: Confluence source_table '{first_source_table_from_conf}' not found in FQDN resolver map. Skipping mapping for page {confluence_page_id}.")
                continue # Cannot map if no resolver entry
            
            # --- Iterate through each environment for mapping and orphan cleanup ---
            for ml_env_upper in CHECK_ENVIRONMENTS:
                env_fqdn_details = resolved_env_fqdns_map.get(ml_env_upper)
                if not env_fqdn_details:
                    print(f"  INFO: No FQDN mapping for source '{first_source_table_from_conf}' in environment '{ml_env_upper}'. Skipping mapping for this environment.")
                    continue
                
                ml_source_fqdn = env_fqdn_details['fqdn']
                ml_object_type = env_fqdn_details['object_type']

                # Get the DDL details from cache
                ml_ddl_info = ml_ddl_cache.get((ml_source_fqdn, ml_env_upper, ml_object_type))

                if not ml_ddl_info or not ml_ddl_info['current_extracted_ddl'] or not ml_ddl_info['current_ddl_hash']:
                    print(f"  WARNING: No current DDL or hash found for ML source '{ml_source_fqdn}' in '{ml_env_upper}' ({ml_object_type}). Skipping mapping for this environment.")
                    continue
                
                ml_actual_columns_from_ddl = extract_columns_from_ddl(ml_ddl_info['current_extracted_ddl'])
                ml_actual_column_names_upper = [col['name'] for col in ml_actual_columns_from_ddl]

                if not ml_actual_column_names_upper:
                    print(f"  WARNING: No columns extracted from DDL for '{ml_source_fqdn}' in '{ml_env_upper}'. Skipping mapping for this environment.")
                    continue

                print(f"  Mapping for {ml_source_fqdn} in {ml_env_upper} ({ml_object_type})...")

                # --- Process Confluence columns for mapping (ONLY those marked add_source_to_target: True) ---
                for conf_col_detail in confluence_target_columns_to_map:
                    confluence_target_field_name = conf_col_detail['target_field_name']
                    confluence_source_field_name = conf_col_detail['source_field_name']
                    
                    existing_map_record = db_manager.get_confluence_ml_column_map_entry(
                        confluence_page_id, confluence_target_field_name, ml_source_fqdn, ml_env_upper, ml_object_type
                    )

                    if existing_map_record and existing_map_record['user_override'] == 1:
                        # Case 1: User has overridden. Respect it.
                        print(f"    Skipping '{confluence_target_field_name}': User has manually overridden mapping. Ensuring active status.")
                        db_manager.insert_or_update_confluence_ml_column_map({
                            'confluence_page_id': confluence_page_id,
                            'confluence_target_field_name': confluence_target_field_name,
                            'ml_source_fqdn': ml_source_fqdn,
                            'ml_env': ml_env_upper,
                            'ml_object_type': ml_object_type,
                            'last_mapped_on': datetime.now().isoformat(),
                            'ml_source_ddl_hash_at_mapping': ml_ddl_info['current_ddl_hash'],
                            'is_active': 1, # Ensure active
                        })
                        continue

                    # Case 2: No existing record, or existing record is automated (user_override=0)
                    perform_fuzzy_match = True
                    if existing_map_record:
                        # If existing mapping's DDL hash matches current ML DDL hash, and it's PARSED_OK status, skip
                        if existing_map_record['ml_source_ddl_hash_at_mapping'] == ml_ddl_info['current_ddl_hash'] and \
                           existing_map_record['mapping_status'] not in ['UNMAPPED_LOW_SCORE', 'UNMAPPED_NOT_EXACT', 'INACTIVE_ORPHANED', 'MAPPED_USER_OVERRIDE']:
                            print(f"    Skipping '{confluence_target_field_name}': ML DDL hash unchanged and previously mapped. (Automated)")
                            # Ensure active status and last mapped on is updated for this check
                            db_manager.insert_or_update_confluence_ml_column_map({
                                'confluence_page_id': confluence_page_id,
                                'confluence_target_field_name': confluence_target_field_name,
                                'ml_source_fqdn': ml_source_fqdn,
                                'ml_env': ml_env_upper,
                                'ml_object_type': ml_object_type,
                                'last_mapped_on': datetime.now().isoformat(),
                                'ml_source_ddl_hash_at_mapping': ml_ddl_info['current_ddl_hash'],
                                'is_active': 1, # Ensure active
                                'mapping_status': existing_map_record['mapping_status'] # Keep previous good status
                            })
                            perform_fuzzy_match = False
                    
                    if perform_fuzzy_match:
                        best_match_result = process.extractOne(
                            confluence_target_field_name.upper(),
                            ml_actual_column_names_upper,
                            scorer=match_strategy,
                            score_cutoff=match_threshold
                        )

                        new_mapping_data = {
                            'confluence_page_id': confluence_page_id,
                            'confluence_target_field_name': confluence_target_field_name,
                            'ml_source_fqdn': ml_source_fqdn,
                            'ml_env': ml_env_upper,
                            'ml_object_type': ml_object_type,
                            'last_mapped_on': datetime.now().isoformat(),
                            'ml_source_ddl_hash_at_mapping': ml_ddl_info['current_ddl_hash'],
                            'user_override': 0, # Auto-generated mapping
                            'is_active': 1,
                            'notes': ''
                        }

                        if best_match_result:
                            matched_ml_col_name = best_match_result[0]
                            score = best_match_result[1]
                            
                            new_mapping_data.update({
                                'matched_ml_column_name': matched_ml_col_name,
                                'match_percentage': score,
                                'match_strategy': match_strategy_str
                            })

                            if exact_match_only and score < 100:
                                new_mapping_data['mapping_status'] = 'UNMAPPED_NOT_EXACT'
                                new_mapping_data['notes'] = f"Auto-mapped: Fuzzy match ({score}%) below 100% exact_match_only threshold. Confluence source: {confluence_source_field_name}, Confluence Type: {conf_col_detail.get('data_type')}, Confluence Definition: {conf_col_detail.get('definition')}"
                                print(f"    '{confluence_target_field_name}' -> No exact match found ({score}%). Status: {new_mapping_data['mapping_status']}")
                            elif score == 100:
                                new_mapping_data['mapping_status'] = 'MAPPED_EXACT'
                                new_mapping_data['notes'] = f"Auto-mapped: Exact match found for '{confluence_target_field_name}'. Confluence source: {confluence_source_field_name}, Confluence Type: {conf_col_detail.get('data_type')}, Confluence Definition: {conf_col_detail.get('definition')}"
                                print(f"    '{confluence_target_field_name}' -> '{matched_ml_col_name}' (Exact Match). Status: {new_mapping_data['mapping_status']}")
                            else:
                                new_mapping_data['mapping_status'] = 'MAPPED_FUZZY'
                                new_mapping_data['notes'] = f"Auto-mapped: Fuzzy match ({score}%) for '{confluence_target_field_name}' to '{matched_ml_col_name}'. Confluence source: {confluence_source_field_name}, Confluence Type: {conf_col_detail.get('data_type')}, Confluence Definition: {conf_col_detail.get('definition')}"
                                print(f"    '{confluence_target_field_name}' -> '{matched_ml_col_name}' ({score}%). Status: {new_mapping_data['mapping_status']}")
                        else:
                            new_mapping_data['mapping_status'] = 'UNMAPPED_LOW_SCORE'
                            new_mapping_data['notes'] = f"Auto-mapped: No match found above threshold ({match_threshold}%). Confluence source: {confluence_source_field_name}, Confluence Type: {conf_col_detail.get('data_type')}, Confluence Definition: {conf_col_detail.get('definition')}"
                            print(f"    '{confluence_target_field_name}' -> No match found. Status: {new_mapping_data['mapping_status']}")

                        db_manager.insert_or_update_confluence_ml_column_map(new_mapping_data)
            
            # --- Orphan Mapping Handling for THIS Page and Environment ---
            current_target_field_names_in_conf = all_current_confluence_target_names
            
            cursor.execute("""
                SELECT confluence_target_field_name, matched_ml_column_name, user_override
                FROM confluence_ml_column_map 
                WHERE confluence_page_id = ? AND ml_source_fqdn = ? AND ml_env = ? AND ml_object_type = ? AND is_active = 1
            """, (confluence_page_id, ml_source_fqdn, ml_env_upper, ml_object_type))
            
            for orphan_row in cursor.fetchall():
                orphan_target_field_name = orphan_row['confluence_target_field_name']
                orphan_user_override = orphan_row['user_override']

                if orphan_target_field_name not in current_target_field_names_in_conf:
                    # It's an orphan!
                    if orphan_user_override == 1:
                        print(f"  INFO: Orphan detected for '{orphan_target_field_name}', but skipping deactivation due to user_override.")
                        # Still update last_mapped_on to show it was checked
                        db_manager.insert_or_update_confluence_ml_column_map({
                            'confluence_page_id': confluence_page_id,
                            'confluence_target_field_name': orphan_target_field_name,
                            'ml_source_fqdn': ml_source_fqdn,
                            'ml_env': ml_env_upper,
                            'ml_object_type': ml_object_type,
                            'last_mapped_on': datetime.now().isoformat(), # Update check timestamp
                            'is_active': 1, # Keep active
                        })
                    else:
                        print(f"  WARNING: Orphan mapping detected: '{orphan_target_field_name}' from page {confluence_page_id} is no longer in Confluence content. Marking as inactive.")
                        db_manager.insert_or_update_confluence_ml_column_map({
                            'confluence_page_id': confluence_page_id,
                            'confluence_target_field_name': orphan_target_field_name,
                            'ml_source_fqdn': ml_source_fqdn,
                            'ml_env': ml_env_upper,
                            'ml_object_type': ml_object_type,
                            'last_mapped_on': datetime.now().isoformat(),
                            'is_active': 0, # Mark as inactive
                            'user_override': 0, 
                            'mapping_status': 'INACTIVE_ORPHANED',
                            'notes': 'Automatically marked as inactive: column removed from Confluence page.'
                        })
        except Exception as e:
            print(f"  ERROR: Could not map columns for page {confluence_page_id} ({confluence_api_title}): {e}. Skipping this page/env pair.")
            # Consider updating page_metadata to reflect this column mapping error (Optional)
            # page_metadata_update = {
            #     'page_id': confluence_page_id,
            #     'extraction_status': 'COLUMN_MAP_FAILED',
            #     'notes': page_entry.get('notes', '') + f" | Column mapping failed: {e}"
            # }
            # db_manager.insert_or_update_page_metadata(clean_special_characters_iterative(page_metadata_update))
        
    db_manager.disconnect()
    print("\n--- Column Mapper Complete ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Performs fuzzy matching to map Confluence-defined columns to Snowflake ML DDL columns."
    )
    args = parser.parse_args()
    
    map_confluence_columns_to_ml_ddl()
