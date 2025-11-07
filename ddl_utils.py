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





# Helper to parse column names and types from a Snowflake CREATE TABLE DDL string
def extract_columns_from_ddl(ddl_string):
    """
    Parses a Snowflake CREATE TABLE DDL string to extract column names.
    Ignores constraints and comments for column names.
    
    Args:
        ddl_string (str): The CREATE TABLE DDL statement.
        
    Returns:
        list: A list of dicts, each with 'name' and 'type' for a column.
              Returns an empty list if DDL cannot be parsed.
    """
    columns = []
    if not ddl_string or not isinstance(ddl_string, str):
        return []

    # Regex to find CREATE TABLE ... (...)
    table_pattern = re.compile(r"CREATE (?:OR REPLACE )?TABLE (?:[^.( ]+\.)?[^.( ]+\.[^.( ]+\s*\((.*)\);", re.DOTALL | re.IGNORECASE)
    match = table_pattern.search(ddl_string)

    if not match:
        print(f"WARNING: Could not find CREATE TABLE structure in DDL: {ddl_string[:100]}...")
        return []

    columns_part = match.group(1)
    
    # Split by comma, but handle commas within parentheses (e.g., in NUMBER(18,2))
    # This regex is for splitting by comma outside of parentheses.
    # It finds a comma (,) that is NOT followed by (any_chars), then another comma.
    # This is often tricky. A simpler approach for DDL is often splitting lines and processing.
    
    # More robust DDL parsing: split into individual lines and process each
    column_lines = [line.strip() for line in columns_part.splitlines() if line.strip() and not line.strip().startswith(')') and not line.strip().startswith('CONSTRAINT')]
    
    for line in column_lines:
        # Regex to capture column name, type, and ignore everything after (comments, constraints, etc.)
        # Examples: "  ID VARCHAR(16) NOT NULL,", "  NAME VARCHAR,", "  LAST_UPDATED_TS TIMESTAMP_LTZ COMMENT 'Last updated timestamp'"
        column_match = re.match(r'^\s*([A-Z0-9_]+)\s+([A-Z0-9_]+\s*(?:\(\s*\d+(?:\s*,\s*\d+)?\s*\))?)', line, re.IGNORECASE)
        if column_match:
            col_name = column_match.group(1).upper()
            col_type = column_match.group(2).upper()
            columns.append({"name": col_name, "type": col_type})
        else:
            # print(f"WARNING: Could not parse column from DDL line: '{line}'")
            pass # Ignore lines that are not simple column definitions (e.g., table constraints)

    return columns


# Test block for ddl_utils.py (NEW)
if __name__ == "__main__":
    print("--- Testing ddl_utils.py functions ---")

    # Test extract_columns_from_ddl
    sample_ddl = """
    CREATE TABLE MY_DB.MY_SCHEMA.MY_TABLE (
        ID NUMBER(38,0) NOT NULL COMMENT 'Primary key for the table',
        NAME VARCHAR(255) COMMENT 'Name of the item',
        AMOUNT DECIMAL(18,2),
        IS_ACTIVE BOOLEAN DEFAULT TRUE,
        CREATED_DATE TIMESTAMP_LTZ(9),
        CONSTRAINT PK_MY_TABLE PRIMARY KEY (ID)
    );
    """
    columns = extract_columns_from_ddl(sample_ddl)
    print("\nExtracted columns from sample DDL:")
    for col in columns:
        print(f"  - Name: {col['name']}, Type: {col['type']}")
    
    expected_columns = [
        {"name": "ID", "type": "NUMBER(38,0)"},
        {"name": "NAME", "type": "VARCHAR(255)"},
        {"name": "AMOUNT", "type": "DECIMAL(18,2)"},
        {"name": "IS_ACTIVE", "type": "BOOLEAN"},
        {"name": "CREATED_DATE", "type": "TIMESTAMP_LTZ(9)"},
    ]
    if columns == expected_columns:
        print("SUCCESS: DDL column extraction matches expected.")
    else:
        print("FAILURE: DDL column extraction DOES NOT match expected.")


    # Test validate_source_to_fqdn_map (will use actual DB if run)
    try:
        print("\n--- Testing validate_source_to_fqdn_map ---")
        # Ensure your DB and resolver JSON are set up for this test
        # results = validate_source_to_fqdn_map() 
        # print(results)
    except Exception as e:
        print(f"Error during validate_source_to_fqdn_map test: {e}")
    print("\n--- Testing ddl_utils.py complete ---")


