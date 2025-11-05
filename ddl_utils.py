# ddl_utils.py (MODIFIED validate_source_to_fqdn_map)

import json
import os
from config import FilePaths, load_fqdn_map
from database_manager import DatabaseManager
from confluence_utils import clean_special_characters_iterative


# REMOVED: is_likely_fqdn helper - no longer needed here

def validate_source_to_fqdn_map(db_file=None):
    """
    Compares source_table entries from 'table_1' in parsed Confluence content in the DB
    against the source_to_fqdn_map.json for resolution.
    Performs case-insensitive matching.
    
    Returns:
        dict: A dictionary containing:
            'resolved_fqdns': List of unique FQDNs resolved from Confluence content.
            'unresolved_source_tables': List of original source_table names NOT resolved.
            'unused_canonical_entries': List of canonical keys in the map not referenced by any resolved source.
    """
    print("\n--- Starting Source to FQDN Map Validation ---")

    db_manager = DatabaseManager(db_file)
    fqdn_lookup_map = load_fqdn_map() # Loads unified map: SOURCE_NAME_UPPER -> FQDN_UPPER

    # Collect unique source_table names from 'table_1's in parsed content
    unique_source_names_from_content = set()

    try:
        cursor = db_manager.conn.cursor()
        cursor.execute("SELECT page_id, parsed_json FROM confluence_parsed_content")
        
        for row in cursor.fetchall():
            # page_id = row['page_id'] # Not needed for this validation
            parsed_content_json_str = row['parsed_json']
            if parsed_content_json_str:
                parsed_content = json.loads(parsed_content_json_str)
                cleaned_parsed_content = clean_special_characters_iterative(parsed_content)

                for table_data in cleaned_parsed_content.get('tables', []):
                    if table_data.get('id') == 'table_1': # Only process 'table_1'
                        for column in table_data.get('columns', []):
                            source_table_raw = column.get('source_table')
                            if source_table_raw and source_table_raw.strip():
                                # Store as uppercase for consistent lookup
                                unique_source_names_from_content.add(source_table_raw.strip().upper())
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in confluence_parsed_content for a page: {e}")
        db_manager.disconnect()
        return None
    except Exception as e:
        print(f"ERROR: Failed to retrieve source tables from DB: {e}")
        db_manager.disconnect()
        return None
    finally:
        db_manager.disconnect()

    print(f"Found {len(unique_source_names_from_content)} unique source_table entries in 'table_1's in the database content.")

    resolved_fqdns = set()        # Unique FQDNs that were successfully mapped
    unresolved_source_tables = set() # Source names that could NOT be resolved to an FQDN

    # Attempt to resolve each unique source name
    for source_name_upper in unique_source_names_from_content:
        if source_name_upper in fqdn_lookup_map:
            resolved_fqdns.add(fqdn_lookup_map[source_name_upper])
        else:
            unresolved_source_tables.add(source_name_upper) # Add to unresolved list

    # Identify unused canonical entries in the map
    # This identifies canonical FQDNs in your map that aren't pointed to by any resolved alias or canonical key
    # (i.e., their FQDN value is not among the 'resolved_fqdns' we actually used).
    unused_canonical_entries = set()
    for canonical_key_upper, details in load_fqdn_map(os.path.join(os.path.dirname(os.path.abspath(__file__)), FilePaths.SOURCE_FQDN_MAP_FILE)).items(): # Reload full map for canonical keys
         # The 'fqdn' key in the loaded details is the actual FQDN string
        if isinstance(details, dict) and 'fqdn' in details:
            if details['fqdn'].upper() not in resolved_fqdns:
                unused_canonical_entries.add(canonical_key_upper) # Add canonical key if its FQDN is unused
    

    validation_results = {
        'resolved_fqdns': sorted(list(resolved_fqdns)),
        'unresolved_source_tables': sorted(list(unresolved_source_tables)),
        'unused_canonical_entries': sorted(list(unused_canonical_entries))
    }

    print("\n--- FQDN Map Validation Results ---")
    print(f"Resolved FQDNs from Pages ({len(validation_results['resolved_fqdns'])}):")
    for fqdn in validation_results['resolved_fqdns']:
        print(f"  - {fqdn}")

    if validation_results['unresolved_source_tables']:
        print(f"\nUNRESOLVED Source Tables (from 'table_1') ({len(validation_results['unresolved_source_tables'])}):")
        print("ACTION REQUIRED: These source_table names from pages could not be resolved.")
        print("Please add them (as canonical keys or aliases) to source_to_fqdn_map.json.")
        for s in validation_results['unresolved_source_tables']:
            print(f"  - {s}")
    else:
        print("\nAll source_table entries from 'table_1's in the database content are successfully resolved!")
    
    if validation_results['unused_canonical_entries']:
        print(f"\nUnused Canonical Entries in map ({len(validation_results['unused_canonical_entries'])}):")
        print("INFO: These canonical entries (and their FQDNs) are in your map but not currently referenced by any 'table_1' content. Consider cleanup.")
        for s in validation_results['unused_canonical_entries']:
            print(f"  - {s}")

    print("\n--- FQDN Map Validation Complete ---")
    return validation_results
