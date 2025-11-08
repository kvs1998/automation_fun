# column_mapper.py
import os
import json
from datetime import datetime
import argparse
from rapidfuzz import fuzz, process # NEW: rapidfuzz
import re

# NEW: Import resolve_snowflake_data_type from data_type_mapper.py
from data_type_mapper import resolve_snowflake_data_type # This function is moved here
from config import CHECK_ENVIRONMENTS, FilePaths, load_column_mapper_config, load_fqdn_resolver, load_data_type_map
from database_manager import DatabaseManager
from confluence_utils import clean_special_characters_iterative
from ddl_utils import extract_columns_from_ddl


# Helper function to interpret raw string values for boolean-like fields from Confluence
def _interpret_confluence_boolean_string(value):
    """
    Interprets Confluence boolean-like strings ('yes', 'no', '', None, etc.).
    Returns True only if the value is explicitly 'yes' (case-insensitive and trimmed).
    """
    if value and isinstance(value, str):
        return value.strip().lower() == 'yes'
    return False


def map_confluence_columns_to_ml_ddl():
    """
    Performs fuzzy matching to map Confluence-defined columns (source_field_name)
    to actual Snowflake ML table DDL columns. Stores results in confluence_ml_column_map.
    Manages user overrides and tracks mapping status.
    """
    print("\n--- Starting Column Mapper ---")

    db_manager = DatabaseManager()
    
    try:
        column_mapper_config = load_column_mapper_config()
        data_type_map = load_data_type_map() # Load data type map here
    except Exception as e:
        print(f"ERROR: Failed to load configuration files: {e}")
        db_manager.disconnect()
        return

    match_threshold = column_mapper_config.get("match_threshold", 80)
    match_strategy_str = column_mapper_config.get("match_strategy", "TOKEN_SET_RATIO").upper()
    exact_match_only = column_mapper_config.get("exact_match_only", False)
    output_report_filename = column_mapper_config.get("output_report_filename", "column_mapping_report.md")
    
    # Map strategy string to rapidfuzz function
    if match_strategy_str == "RATIO":
        match_strategy = fuzz.ratio
    elif match_strategy_str == "PARTIAL_RATIO":
        match_strategy = fuzz.partial_ratio
    elif match_strategy_str == "TOKEN_SORT_RATIO":
        match_strategy = fuzz.token_sort_ratio
    elif match_strategy_str == "TOKEN_SET_RATIO":
        match_strategy = fuzz.token_set_ratio
    elif match_strategy_str == "WRATIO": # More robust weighting for rapidfuzz
        match_strategy = fuzz.WRatio
    elif match_strategy_str == "QRATIO": # Quick ratio for rapidfuzz
        match_strategy = fuzz.QRatio
    else:
        print(f"ERROR: Invalid match_strategy '{match_strategy_str}'. Exiting.")
        db_manager.disconnect()
        return

    print(f"Column Mapper Config: Threshold={match_threshold}, Strategy='{match_strategy_str}', Exact Only={exact_match_only}")

    # --- 1. Identify pages needing mapping ---
    cursor = db_manager.conn.cursor()
    cursor.execute("""
        SELECT 
            cpm.page_id,
            cpm.api_title,
            cpc.parsed_json,
            cpm.last_parsed_content_hash as confluence_metadata_hash_at_parse_time,
            cpm.page_title as confluence_page_actual_title -- Get actual page title for mapping table
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

    ml_ddl_cache = {} # { (fqdn, env, obj_type): { 'current_ddl_hash', 'current_extracted_ddl' } }
    
    cursor.execute(f"SELECT fqdn, environment, object_type, current_ddl_hash, current_extracted_ddl FROM {FilePaths.SNOWFLAKE_ML_SOURCE_TABLE}")
    for row in cursor.fetchall():
        key = (row['fqdn'], row['environment'], row['object_type'])
        ml_ddl_cache[key] = {'current_ddl_hash': row['current_ddl_hash'], 'current_extracted_ddl': row['current_extracted_ddl']}


    # --- Column Mapping Loop ---
    report_lines = []
    report_lines.append(f"# Column Mapping Report")
    report_lines.append(f"Generated On: {datetime.now().isoformat()}")
    report_lines.append(f"Config: Threshold={match_threshold}, Strategy='{match_strategy_str}', Exact Only={exact_match_only}\n")

    for page_row in pages_to_map:
        confluence_page_id = page_row['page_id']
        confluence_api_title = page_row['api_title']
        confluence_page_actual_title = page_row['confluence_page_actual_title']
        confluence_parsed_json_str = page_row['parsed_json']
        confluence_metadata_hash_at_parse_time = page_row['confluence_metadata_hash_at_parse_time']

        report_lines.append(f"\n## Page: {confluence_api_title} (ID: {confluence_page_id})")

        try:
            parsed_content = json.loads(confluence_parsed_json_str)
            
            confluence_columns_for_mapping_context = [] # All columns from Confluence's 'table_1' (for type/def etc.)
            confluence_columns_to_map = [] # Subset where 'add_source_to_target' is True
            all_current_confluence_target_names = set() # For orphan detection (target_field_name)

            for table_data in parsed_content.get('tables', []):
                if table_data.get('id') == 'table_1':
                    for column_detail in table_data.get('columns', []):
                        # Add to mapping context (for data type, definition lookup)
                        confluence_columns_for_mapping_context.append(column_detail)
                        
                        # Add target name for orphan detection
                        all_current_confluence_target_names.add(column_detail.get('target_field_name')) 
                        
                        # Final, correct interpretation of 'add_source_to_target'
                        if _interpret_confluence_boolean_string(column_detail.get('add_source_to_target')):
                            confluence_columns_to_map.append(column_detail)
            
            if not confluence_columns_to_map:
                report_lines.append(f"  *No columns marked 'add_source_to_target: yes' found in 'table_1' for this page. Skipping column mapping.*")
                # Still proceed to orphan cleanup below even if no columns to map
            
            first_source_table_from_conf = next((col['source_table'] for table_d in parsed_content.get('tables',[]) if table_d.get('id')=='table_1' for col in table_d.get('columns',[]) if col.get('source_table')), None)

            if not first_source_table_from_conf:
                report_lines.append(f"  WARNING: No 'source_table' found in Confluence columns for page {confluence_page_id}. Cannot resolve ML source. Skipping mapping for this page.")
                continue
            
            resolved_env_fqdns_map = fqdn_resolver_map.get(first_source_table_from_conf.upper())

            if not resolved_env_fqdns_map:
                report_lines.append(f"  WARNING: Confluence source_table '{first_source_table_from_conf}' not found in FQDN resolver map. Skipping mapping for page {confluence_page_id}.")
                continue
            
            # --- Iterate through each environment for mapping and orphan cleanup ---
            for ml_env_upper in CHECK_ENVIRONMENTS:
                env_fqdn_details = resolved_env_fqdns_map.get(ml_env_upper)
                if not env_fqdn_details:
                    report_lines.append(f"  INFO: No FQDN mapping for source '{first_source_table_from_conf}' in environment '{ml_env_upper}'. Skipping mapping for this environment.")
                    continue
                
                ml_source_fqdn = env_fqdn_details['fqdn']
                ml_object_type = env_fqdn_details['object_type']

                ml_ddl_info = ml_ddl_cache.get((ml_source_fqdn, ml_env_upper, ml_object_type))

                if not ml_ddl_info or not ml_ddl_info['current_extracted_ddl'] or not ml_ddl_info['current_ddl_hash']:
                    report_lines.append(f"  WARNING: No current DDL or hash found for ML source '{ml_source_fqdn}' in '{ml_env_upper}' ({ml_object_type}). Skipping mapping for this environment.")
                    continue
                
                ml_actual_columns_from_ddl = extract_columns_from_ddl(ml_ddl_info['current_extracted_ddl'])
                ml_actual_column_names_upper = [col['name'] for col in ml_actual_columns_from_ddl]

                if not ml_actual_column_names_upper:
                    report_lines.append(f"  WARNING: No columns extracted from DDL for '{ml_source_fqdn}' in '{ml_env_upper}'. Skipping mapping for this environment.")
                    continue

                report_lines.append(f"\n  ### ML Source: {ml_source_fqdn} ({ml_env_upper}, {ml_object_type})")

                # --- Process Confluence columns for mapping (ONLY those marked add_source_to_target: True) ---
                for conf_col_detail in confluence_columns_to_map:
                    confluence_target_field_name = conf_col_detail['target_field_name']
                    confluence_source_field_name = conf_col_detail.get('source_field_name', '')
                    confluence_data_type = conf_col_detail.get('data_type', '')
                    confluence_is_pk = _interpret_confluence_boolean_string(conf_col_detail.get('is_primary_key'))
                    confluence_definition = conf_col_detail.get('definition', '')
                    confluence_comments = conf_col_detail.get('comments', '')

                    # Resolve Snowflake data type for this Confluence column
                    resolved_sf_type, dtype_warnings = resolve_snowflake_data_type(confluence_data_type, data_type_map)
                    if dtype_warnings:
                        print(f"    WARNING (Type Resolution): '{confluence_data_type}' from '{confluence_api_title}': {'; '.join(dtype_warnings)}")


                    existing_map_record = db_manager.get_confluence_ml_column_map_entry(
                        confluence_page_id, confluence_target_field_name, ml_source_fqdn, ml_env_upper, ml_object_type
                    )

                    # Prepare base data for new/updated mapping
                    current_mapping_data = {
                        'confluence_page_id': confluence_page_id,
                        'confluence_page_title': confluence_page_actual_title, # NEW
                        'confluence_source_field_name': confluence_source_field_name, # NEW
                        'confluence_target_field_name': confluence_target_field_name,
                        'confluence_data_type': confluence_data_type, # NEW
                        'confluence_ddl_sf_type': resolved_sf_type, # NEW
                        'confluence_is_pk': 1 if confluence_is_pk else 0, # NEW
                        'confluence_definition': confluence_definition, # NEW
                        'confluence_comments': confluence_comments, # NEW
                        'ml_source_fqdn': ml_source_fqdn,
                        'ml_env': ml_env_upper,
                        'ml_object_type': ml_object_type,
                        'last_mapped_on': datetime.now().isoformat(),
                        'ml_source_ddl_hash_at_mapping': ml_ddl_info['current_ddl_hash'],
                        'is_active': 1,
                        'user_override': 0, # Default to auto
                        'notes': ''
                    }

                    if existing_map_record and existing_map_record['user_override'] == 1:
                        # Case 1: User has overridden. Respect it.
                        report_lines.append(f"    - '{confluence_source_field_name}' -> '{confluence_target_field_name}' (Target). USER OVERRIDDEN. STATUS: {existing_map_record['mapping_status']}. (Active: {bool(existing_map_record['is_active'])})")
                        # Only update audit fields (last_mapped_on, ddl_hash_at_mapping)
                        db_manager.insert_or_update_confluence_ml_column_map(current_mapping_data)
                        continue # Move to next column

                    # No user override or first run: perform/re-perform fuzzy match
                    perform_fuzzy_match = True
                    if existing_map_record:
                        # If ML DDL has not changed AND previous automated mapping was good, skip re-matching
                        if existing_map_record['ml_source_ddl_hash_at_mapping'] == ml_ddl_info['current_ddl_hash'] and \
                           existing_map_record['mapping_status'] in ['MAPPED_EXACT', 'MAPPED_FUZZY']:
                            report_lines.append(f"    - '{confluence_source_field_name}' -> '{confluence_target_field_name}' (Target) -> '{existing_map_record['matched_ml_column_name']}' ({existing_map_record['match_percentage']}%). STATUS: {existing_map_record['mapping_status']}. (Automated, DDL Unchanged)")
                            db_manager.insert_or_update_confluence_ml_column_map(current_mapping_data) # Update audit fields only
                            perform_fuzzy_match = False
                    
                    if perform_fuzzy_match:
                        # Perform fuzzy match on confluence_source_field_name (as requested)
                        best_match_result = process.extractOne(
                            confluence_source_field_name.upper(), # Search using source_field_name
                            ml_actual_column_names_upper,         # Against actual ML columns (uppercase)
                            scorer=match_strategy,                # Use configured strategy
                            score_cutoff=match_threshold          # Only consider matches above threshold
                        )

                        if best_match_result:
                            matched_ml_col_name = best_match_result[0]
                            score = best_match_result[1]
                            
                            current_mapping_data.update({
                                'matched_ml_column_name': matched_ml_col_name,
                                'match_percentage': score,
                                'match_strategy': match_strategy_str
                            })

                            if exact_match_only and score < 100:
                                current_mapping_data['mapping_status'] = 'UNMAPPED_NOT_EXACT'
                                current_mapping_data['notes'] = f"Fuzzy match ({score}%) below 100% exact_match_only threshold."
                                report_lines.append(f"    - '{confluence_source_field_name}' -> '{confluence_target_field_name}' (Target). No exact match ({score}%). STATUS: {current_mapping_data['mapping_status']}")
                            elif score == 100:
                                current_mapping_data['mapping_status'] = 'MAPPED_EXACT'
                                current_mapping_data['notes'] = f"Exact match found for '{confluence_source_field_name}' to '{matched_ml_col_name}'."
                                report_lines.append(f"    - '{confluence_source_field_name}' -> '{confluence_target_field_name}' (Target) -> '{matched_ml_col_name}' (Exact Match). STATUS: {current_mapping_data['mapping_status']}")
                            else:
                                current_mapping_data['mapping_status'] = 'MAPPED_FUZZY'
                                current_mapping_data['notes'] = f"Fuzzy match ({score}%) for '{confluence_source_field_name}' to '{matched_ml_col_name}'."
                                report_lines.append(f"    - '{confluence_source_field_name}' -> '{confluence_target_field_name}' (Target) -> '{matched_ml_col_name}' ({score}%). STATUS: {current_mapping_data['mapping_status']}")
                        else:
                            current_mapping_data['mapping_status'] = 'UNMAPPED_LOW_SCORE'
                            current_mapping_data['notes'] = f"No match found above threshold ({match_threshold}%)."
                            report_lines.append(f"    - '{confluence_source_field_name}' -> '{confluence_target_field_name}' (Target). No match found. STATUS: {current_mapping_data['mapping_status']}")

                        db_manager.insert_or_update_confluence_ml_column_map(current_mapping_data)
            
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
                        report_lines.append(f"  INFO: Orphan detected for '{orphan_target_field_name}' (Page {confluence_page_id}), but skipping deactivation due to user_override.")
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
                        report_lines.append(f"  WARNING: Orphan mapping detected: '{orphan_target_field_name}' (Page {confluence_page_id}) is no longer in Confluence content. Marking as inactive.")
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
            report_lines.append(f"  ERROR: Could not map columns for page {confluence_page_id} ({confluence_api_title}): {e}. Skipping this page/env pair.")
        
    db_manager.disconnect()
    
    # Save the Column Mapping Report
    report_filepath = os.path.join(FilePaths.REPORT_OUTPUT_DIR, output_report_filename)
    os.makedirs(FilePaths.REPORT_OUTPUT_DIR, exist_ok=True)
    with open(report_filepath, 'w', encoding='utf-8') as f:
        f.write("\n".join(report_lines))
    
    print(f"\n--- Column Mapper Report saved to: {report_filepath} ---")
    print("ACTION REQUIRED: Review the generated report for column mapping results.")

    print("\n--- Column Mapper Complete ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Performs fuzzy matching to map Confluence-defined columns to Snowflake ML DDL columns."
    )
    args = parser.parse_args()
    
    map_confluence_columns_to_ml_ddl()
