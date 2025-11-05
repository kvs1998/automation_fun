# ddl_utils.py (REVERTED/MODIFIED validate_source_to_fqdn_map)

import json
import os
from config import FilePaths, load_fqdn_map
from database_manager import DatabaseManager
from confluence_utils import clean_special_characters_iterative


# REMOVED: is_likely_fqdn helper

def validate_source_to_fqdn_map(db_file=None):
    """
    Compares source_table entries from parsed Confluence content in the DB
    against the source_to_fqdn_map.json file.
    Performs case-insensitive matching by standardizing to uppercase.
    Only checks source_table entries from 'table_1' in parsed content.
    
    Returns:
        dict: A dictionary containing:
            'mapped_sources': List of source_table entries found in the map.
            'unmapped_sources': List of source_table entries NOT found in the map.
            'unused_fqdns_in_map': List of FQDNs in the map not referenced (optional, for cleanup).
    """
    print("\n--- Starting Source to FQDN Map Validation ---")

    db_manager = DatabaseManager(db_file)
    fqdn_map = load_fqdn_map() # Loads map with keys already in uppercase

    unique_source_tables_from_db_table1 = set() # NEW: Filter for 'table_1'
    try:
        cursor = db_manager.conn.cursor()
        cursor.execute("SELECT parsed_json FROM confluence_parsed_content")
        
        for row in cursor.fetchall():
            parsed_content_json_str = row['parsed_json']
            if parsed_content_json_str:
                parsed_content = json.loads(parsed_content_json_str)
                cleaned_parsed_content = clean_special_characters_iterative(parsed_content)

                # NEW: Iterate only tables with id 'table_1'
                for table_data in cleaned_parsed_content.get('tables', []):
                    if table_data.get('id') == 'table_1': # Only process 'table_1'
                        for column in table_data.get('columns', []):
                            source_table = column.get('source_table')
                            if source_table and source_table.strip():
                                # Convert extracted source_table to uppercase for lookup
                                unique_source_tables_from_db_table1.add(source_table.strip().upper())
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in confluence_parsed_content for page ID: {row['page_id']}: {e}")
        db_manager.disconnect()
        return None
    except Exception as e:
        print(f"ERROR: Failed to retrieve source tables from DB: {e}")
        db_manager.disconnect()
        return None
    finally:
        db_manager.disconnect()

    print(f"Found {len(unique_source_tables_from_db_table1)} unique source_table entries in 'table_1's in the database.")

    mapped_sources = []
    unmapped_sources = []
    
    # Compare with fqdn_map (keys are already uppercase)
    for source_table_upper in unique_source_tables_from_db_table1:
        if source_table_upper in fqdn_map:
            mapped_sources.append(source_table_upper)
        else:
            unmapped_sources.append(source_table_upper)
    
    # Optional: Find FQDNs in map that are not referenced in the DB (for cleanup)
    unused_fqdns_in_map = []
    # Collect all resolved FQDNs that are actually being used by 'table_1' sources
    resolved_fqdns_being_used = {fqdn_map[s] for s in mapped_sources}
    
    for source_key_upper, fqdn_value in fqdn_map.items():
        # Check if the FQDN value from the map is NOT among the FQDNs we just resolved as being used.
        # This will identify entries in the map that aren't pointed to by any source_table from 'table_1'.
        if fqdn_value not in resolved_fqdns_being_used:
            unused_fqdns_in_map.append(f"{source_key_upper} -> {fqdn_value}")
    
    validation_results = {
        'mapped_sources': sorted(mapped_sources),
        'unmapped_sources': sorted(unmapped_sources),
        'unused_fqdns_in_map': sorted(unused_fqdns_in_map)
    }

    print("\n--- FQDN Map Validation Results ---")
    print(f"Mapped Source Tables (from 'table_1') ({len(validation_results['mapped_sources'])}):")
    for s_upper in validation_results['mapped_sources']:
        print(f"  - {s_upper} -> {fqdn_map[s_upper]}")

    if validation_results['unmapped_sources']:
        print(f"\nUNMAPPED Source Tables (from 'table_1') ({len(validation_results['unmapped_sources'])}):")
        print("ACTION REQUIRED: Please add these entries (in uppercase) to source_to_fqdn_map.json.")
        for s_upper in validation_results['unmapped_sources']:
            print(f"  - {s_upper}")
    else:
        print("\nAll source_table entries from 'table_1's in the database content are successfully mapped!")
    
    if validation_results['unused_fqdns_in_map']:
        print(f"\nUnused FQDNs in map ({len(validation_results['unused_fqdns_in_map'])}):")
        print("INFO: These FQDNs are in your map but not currently referenced by any 'table_1' content. Consider cleanup.")
        for s in validation_results['unused_fqdns_in_map']:
            print(f"  - {s}")

    print("\n--- FQDN Map Validation Complete ---")
    return validation_results

# Example usage (for testing this utility independently)
if __name__ == "__main__":
    try:
        results = validate_source_to_fqdn_map()
    except Exception as e:
        print(f"Error during map validation: {e}")
