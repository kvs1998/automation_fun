# column_mapper.py
import os
import json
from datetime import datetime
import argparse
from fuzzywuzzy import fuzz, process # NEW: Fuzzy matching library

from config import CHECK_ENVIRONMENTS, FilePaths, load_column_mapper_config, load_fqdn_resolver
from database_manager import DatabaseManager
from confluence_utils import clean_special_characters_iterative
from ddl_utils import extract_columns_from_ddl # NEW: Helper to extract columns from DDL


def map_confluence_columns_to_ml_ddl():
    """
    Performs fuzzy matching to map Confluence-defined columns to actual
    Snowflake ML table DDL columns. Stores results in confluence_ml_column_map.
    Manages user overrides and orphan mappings.
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
    
    # Map strategy string to fuzzywuzzy function
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
            cpm.last_parsed_content_hash as confluence_ddl_hash -- This is metadata hash from metadata_ingestor
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
    
    # Fetch all relevant ML DDLs from snowflake_ml_source_metadata in one go
    # This is more efficient than fetching per-page inside the loop
    cursor.execute(f"SELECT fqdn, environment, object_type, current_ddl_hash, current_extracted_ddl FROM {FilePaths.SNOWFLAKE_ML_SOURCE_TABLE}")
    for row in cursor.fetchall():
        key = (row['fqdn'], row['environment'], row['object_type'])
        ml_ddl_cache[key] = {'current_ddl_hash': row['current_ddl_hash'], 'current_extracted_ddl': row['current_extracted_ddl']}


    for page_row in pages_to_map:
        page_entry = dict(page_row)
        confluence_page_id = page_entry['page_id']
        confluence_api_title = page_entry['api_title']
        confluence_parsed_json_str = page_entry['parsed_json']
        confluence_metadata_hash_at_parse_time = page_entry['confluence_ddl_hash'] # Hash of metadata when content was parsed

        print(f"\n--- Mapping columns for page: '{confluence_api_title}' (ID: {confluence_page_id}) ---")

        try:
            # 2.1 Parse Confluence content to get columns
            parsed_content = json.loads(confluence_parsed_json_str)
            # We assume clean_special_characters_iterative already ran in data_parser.py
            
            confluence_target_columns = [] # List of { 'source_table', 'target_field_name', 'data_type', 'is_primary_key', 'add_to_target' }
            
            for table_data in parsed_content.get('tables', []):
                if table_data.get('id') == 'table_1': # Only process 'table_1'
                    for column_detail in table_data.get('columns', []):
                        if column_detail.get('add_to_target') == True: # Only map columns marked for inclusion
                            confluence_target_columns.append(column_detail)
            
            if not confluence_target_columns:
                print(f"  No columns marked 'add_to_target: True' found in 'table_1' for page {confluence_page_id}. Skipping.")
                continue

            # 2.2 Determine the ML source FQDNs for this Confluence page
            # Assuming all columns on a Confluence page map to the *same* ML source table (per environment)
            # Find the first source_table in the Confluence page's columns
            first_source_table_from_conf = next((col['source_table'] for col in confluence_target_columns if col.get('source_table')), None)

            if not first_source_table_from_conf:
                print(f"  WARNING: No 'source_table' found in Confluence columns for page {confluence_page_id}. Cannot resolve ML source. Skipping.")
                continue
            
            # Resolve this source_table across all CHECK_ENVIRONMENTS
            resolved_env_fqdns = fqdn_resolver_map.get(first_source_table_from_conf.upper())

            if not resolved_env_fqdns:
                print(f"  WARNING: Confluence source_table '{first_source_table_from_conf}' not found in FQDN resolver map. Skipping mapping for page {confluence_page_id}.")
                continue
            
            # List to track all successfully mapped Confluence target field names in this run for this page
            processed_confluence_targets = set()
            
            # --- 3. Iterate through each environment for mapping ---
            for ml_env_upper in CHECK_ENVIRONMENTS:
                env_fqdn_details = resolved_env_fqdns.get(ml_env_upper)
                if not env_fqdn_details:
                    print(f"  INFO: No FQDN mapping for source '{first_source_table_from_conf}' in environment '{ml_env_upper}'. Skipping.")
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

                for conf_col_detail in confluence_target_columns:
                    confluence_target_field_name = conf_col_detail['target_field_name']
                    confluence_source_field_name = conf_col_detail['source_field_name'] # For reporting
                    
                    # Store as processed for this page
                    processed_confluence_targets.add(confluence_target_field_name)

                    # --- Get existing mapping from DB to check user_override ---
                    existing_mapping = db_manager.insert_or_update_confluence_ml_column_map({
                        'confluence_page_id': confluence_page_id,
                        'confluence_target_field_name': confluence_target_field_name,
                        'ml_source_fqdn': ml_source_fqdn,
                        'ml_env': ml_env_upper,
                        'ml_object_type': ml_object_type,
                        'last_mapped_on': datetime.now().isoformat(), # Just to get existing record
                        'mapping_status': 'CHECKING', # Temporary status
                        'is_active': 1 # Assume active for now
                    }) # This gets the row if it exists without changing much.
                    # Or a separate get_column_map_entry method in DBManager for cleaner retrieval.
                    
                    # For a clean retrieve, let's add `get_confluence_ml_column_map_entry` to DBManager
                    existing_map_record = db_manager.get_confluence_ml_column_map_entry(
                        confluence_page_id, confluence_target_field_name, ml_source_fqdn, ml_env_upper, ml_object_type
                    )

                    if existing_map_record and existing_map_record['user_override'] == 1:
                        # DO NOT OVERWRITE if user has manually set this
                        print(f"    Skipping '{confluence_target_field_name}': User has manually overridden mapping to '{existing_map_record['matched_ml_column_name']}'.")
                        # Ensure its status is active and DDL hash is current if user overridden
                        db_manager.insert_or_update_confluence_ml_column_map({
                            'confluence_page_id': confluence_page_id,
                            'confluence_target_field_name': confluence_target_field_name,
                            'ml_source_fqdn': ml_source_fqdn,
                            'ml_env': ml_env_upper,
                            'ml_object_type': ml_object_type,
                            'last_mapped_on': datetime.now().isoformat(),
                            'ml_source_ddl_hash_at_mapping': ml_ddl_info['current_ddl_hash'],
                            'mapping_status': 'MAPPED_USER_OVERRIDE',
                            'is_active': 1,
                            'user_override': 1,
                            'matched_ml_column_name': existing_map_record['matched_ml_column_name'], # Preserve user's choice
                            'match_percentage': existing_map_record['match_percentage'],
                            'match_strategy': existing_map_record['match_strategy'],
                            'notes': existing_map_record['notes'] # Preserve user notes
                        })
                        continue # Move to next column

                    # --- Perform Fuzzy Matching ---
                    best_match = process.extractOne(
                        confluence_target_field_name.upper(), # Match Confluence target column (uppercase)
                        ml_actual_column_names_upper,         # Against actual ML columns (all uppercase)
                        scorer=match_strategy,                # Use configured strategy
                        score_cutoff=match_threshold          # Only consider matches above threshold
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

                    if best_match:
                        matched_ml_col_name = best_match[0]
                        score = best_match[1]
                        
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

                    # Store the new or updated auto-mapping
                    db_manager.insert_or_update_confluence_ml_column_map(new_mapping_data)
            
            # --- Orphan Mapping Handling ---
            # Mark existing mappings as inactive if they are no longer in the current Confluence parsed content
            current_target_field_names_in_conf = {col['target_field_name'] for col in confluence_target_columns}
            
            # Get all existing active mappings for this page, FQDN, env, obj_type
            cursor.execute("""
                SELECT confluence_target_field_name 
                FROM confluence_ml_column_map 
                WHERE confluence_page_id = ? AND ml_source_fqdn = ? AND ml_env = ? AND ml_object_type = ? AND is_active = 1
            """, (confluence_page_id, ml_source_fqdn, ml_env_upper, ml_object_type))
            existing_active_mapped_targets = {row[0] for row in cursor.fetchall()}

            orphaned_targets = existing_active_mapped_targets - current_target_field_names_in_conf

            for orphan_target in orphaned_targets:
                print(f"  WARNING: Orphan mapping detected: '{orphan_target}' from page {confluence_page_id} is no longer in Confluence content. Marking as inactive.")
                db_manager.insert_or_update_confluence_ml_column_map({
                    'confluence_page_id': confluence_page_id,
                    'confluence_target_field_name': orphan_target,
                    'ml_source_fqdn': ml_source_fqdn,
                    'ml_env': ml_env_upper,
                    'ml_object_type': ml_object_type,
                    'last_mapped_on': datetime.now().isoformat(),
                    'is_active': 0, # Mark as inactive
                    'user_override': 0, # Assume it was automated if orphaned
                    'mapping_status': 'INACTIVE_ORPHANED',
                    'notes': 'Automatically marked as inactive: column removed from Confluence page.'
                })
        except Exception as e:
            print(f"  ERROR: Could not map columns for page {confluence_page_id} ({confluence_api_title}): {e}. Skipping this page.")
        
    db_manager.disconnect()
    print("\n--- Column Mapper Complete ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Performs fuzzy matching to map Confluence-defined columns to Snowflake ML DDL columns."
    )
    # No config_file arg here, as it's assumed to be loaded by load_column_mapper_config()
    
    args = parser.parse_args()
    
    map_confluence_columns_to_ml_ddl()
