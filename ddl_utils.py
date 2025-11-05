# ddl_utils.py
import json
import os
from config import FilePaths, load_fqdn_map # Import load_fqdn_map
from database_manager import DatabaseManager # To query confluence_parsed_content
from confluence_utils import clean_special_characters_iterative # For cleaning fetched data


def validate_source_to_fqdn_map(db_file=None):
    """
    Compares source_table entries from parsed Confluence content in the DB
    against the source_to_fqdn_map.json file.
    
    Returns:
        dict: A dictionary containing:
            'mapped_sources': List of source_table entries found in the map.
            'unmapped_sources': List of source_table entries NOT found in the map.
            'unused_fqdns_in_map': List of FQDNs in the map not referenced (optional, for cleanup).
    """
    print("\n--- Starting Source to FQDN Map Validation ---")

    db_manager = DatabaseManager(db_file)
    fqdn_map = load_fqdn_map() # Load your FQDN mapping

    # 1. Get all unique source_table entries from confluence_parsed_content
    unique_source_tables_from_db = set()
    try:
        cursor = db_manager.conn.cursor()
        # Query confluence_parsed_content
        cursor.execute("SELECT parsed_json FROM confluence_parsed_content")
        
        for row in cursor.fetchall():
            parsed_content_json_str = row['parsed_json']
            if parsed_content_json_str:
                parsed_content = json.loads(parsed_content_json_str)
                # Apply cleaning to the loaded JSON just in case (though should be clean from data_parser)
                cleaned_parsed_content = clean_special_characters_iterative(parsed_content)

                for table_data in cleaned_parsed_content.get('tables', []):
                    for column in table_data.get('columns', []):
                        source_table = column.get('source_table')
                        if source_table and source_table.strip():
                            unique_source_tables_from_db.add(source_table.strip())
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in confluence_parsed_content: {e}")
        db_manager.disconnect()
        return None
    except Exception as e:
        print(f"ERROR: Failed to retrieve source tables from DB: {e}")
        db_manager.disconnect()
        return None
    finally:
        db_manager.disconnect()

    print(f"Found {len(unique_source_tables_from_db)} unique source_table entries in the database.")

    mapped_sources = []
    unmapped_sources = []
    
    # Compare with fqdn_map
    for source_table in unique_source_tables_from_db:
        if source_table in fqdn_map:
            mapped_sources.append(source_table)
        else:
            unmapped_sources.append(source_table)
    
    # Optional: Find FQDNs in map that are not referenced in the DB (for cleanup)
    referenced_fqdns = {fqdn_map[s] for s in mapped_sources}
    unused_fqdns_in_map = []
    for source_key, fqdn_value in fqdn_map.items():
        # Check if the map key itself is present in the source tables from DB
        # AND if its FQDN value is not in the set of actually used FQDNs
        if source_key not in unique_source_tables_from_db and fqdn_value not in referenced_fqdns:
            unused_fqdns_in_map.append(f"{source_key} -> {fqdn_value}")
    
    validation_results = {
        'mapped_sources': sorted(mapped_sources),
        'unmapped_sources': sorted(unmapped_sources),
        'unused_fqdns_in_map': sorted(unused_fqdns_in_map)
    }

    print("\n--- FQDN Map Validation Results ---")
    print(f"Mapped Source Tables ({len(validation_results['mapped_sources'])}):")
    for s in validation_results['mapped_sources']:
        print(f"  - {s} -> {fqdn_map[s]}")

    if validation_results['unmapped_sources']:
        print(f"\nUNMAPPED Source Tables ({len(validation_results['unmapped_sources'])}):")
        print("ACTION REQUIRED: Please add these entries to source_to_fqdn_map.json.")
        for s in validation_results['unmapped_sources']:
            print(f"  - {s}")
    else:
        print("\nAll source_table entries found in the database are successfully mapped!")
    
    if validation_results['unused_fqdns_in_map']:
        print(f"\nUnused FQDNs in map ({len(validation_results['unused_fqdns_in_map'])}):")
        print("INFO: These FQDNs are in your map but not currently referenced by any page. Consider cleanup.")
        for s in validation_results['unused_fqdns_in_map']:
            print(f"  - {s}")

    print("\n--- FQDN Map Validation Complete ---")
    return validation_results

# Example usage (for testing this utility independently)
if __name__ == "__main__":
    # Ensure you have a populated confluence_metadata.db and source_to_fqdn_map.json
    # before running this example.
    try:
        results = validate_source_to_fqdn_map()
        # You can now act on results['unmapped_sources'] in your DDL generation script
        # if results['unmapped_sources']:
        #     print("DDL generation aborted due to unmapped sources.")
        # else:
        #     print("Proceeding with DDL generation.")
    except Exception as e:
        print(f"Error during map validation: {e}")
