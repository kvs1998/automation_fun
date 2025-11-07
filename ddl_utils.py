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
# Helper to parse column names and types from a Snowflake CREATE TABLE DDL string
def extract_columns_from_ddl(ddl_string):
    """
    Parses a Snowflake CREATE TABLE DDL string to extract column names and their types.
    It robustly handles constraints, comments, nullability, and default values.
    
    Args:
        ddl_string (str): The CREATE TABLE DDL statement.
        
    Returns:
        list: A list of dicts, each with 'name' and 'type' for a column.
              Returns an empty list if DDL cannot be parsed or if no columns found.
    """
    columns = []
    if not ddl_string or not isinstance(ddl_string, str):
        return []

    # Regex to find CREATE TABLE ... (...) and capture the content inside the parentheses
    # Uses re.DOTALL to match newlines inside the parentheses
    table_pattern = re.compile(r"CREATE (?:OR REPLACE )?TABLE (?:[^.( ]+\.)?[^.( ]+\.[^.( ]+\s*\((.*)\);", re.DOTALL | re.IGNORECASE)
    match = table_pattern.search(ddl_string)

    if not match:
        print(f"WARNING: Could not find CREATE TABLE structure in DDL: {ddl_string[:100]}...")
        return []

    columns_part = match.group(1) # This contains all column definitions, constraints, etc.

    # NEW ROBUST PARSING STRATEGY:
    # Use a regex that specifically targets column definitions, ignoring everything else.
    # This regex is designed to be more flexible, capturing the column name and its type,
    # and then ignoring constraints, comments, etc., on the same line.
    
    # Components:
    # ^\s*                       - Start of line, optional leading whitespace
    # ([A-Z0-9_]+)               - Group 1: Column Name (alphanumeric, underscore)
    # \s+                        - At least one space
    # ([A-Z0-9_]+\s*(?:\(\s*\d+(?:\s*,\s*\d+)?\s*\))?) - Group 2: Data Type (e.g., VARCHAR, NUMBER(38,0))
    #                                                    (Base type + optional params)
    # (?:                        - Non-capturing group for the rest of the line (optional constraints, comments, commas)
    #   \s+                      - At least one space
    #   (?:NOT NULL|DEFAULT|COMMENT|PRIMARY KEY|FOREIGN KEY|UNIQUE|CHECK|AUTOINCREMENT|\w+)+ - Keywords to ignore
    #   .*?                      - Any characters non-greedily
    # )?                         - The entire non-capturing group is optional
    # \s*                        - Optional trailing whitespace
    # ,?                         - Optional trailing comma
    # $                          - End of line
    
    # We will refine this to specifically look for columns and their types.
    # The crucial part is to correctly handle the type definition which can have parameters.
    
    # This regex attempts to find (NAME TYPE) pattern and ignore anything after that.
    # It accounts for spaces in type names (e.g., TIMESTAMP LTZ) if that's how Snowflake returns them,
    # but based on GET_DDL, it's usually `TIMESTAMP_LTZ`.
    column_def_pattern = re.compile(
        r'^\s*([A-Z0-9_]+)\s+'  # Group 1: Column Name (e.g., ID)
        r'([A-Z0-9_]+\s*(?:\(\s*\d+(?:\s*,\s*\d+)?\s*\))?)' # Group 2: Data Type (e.g., NUMBER(38,0), VARCHAR(255))
        r'(?:(?!\s+(?:CONSTRAINT|PRIMARY KEY|FOREIGN KEY)).)*?' # Non-greedy match anything until a constraint starts or end of line.
                                                                # This is the key to ignore "NOT NULL", "DEFAULT", "COMMENT" etc.
        r'(?:,|$)', # Match trailing comma or end of line (but don't capture)
        re.IGNORECASE | re.MULTILINE
    )
    
    for match in column_def_pattern.finditer(columns_part):
        col_name = match.group(1).strip().upper()
        col_type = match.group(2).strip().upper()
        columns.append({"name": col_name, "type": col_type})

    return columns


# Test block for ddl_utils.py (NEW)
# Test block for ddl_utils.py (MODIFIED to reflect robust parsing)
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
        print(f"  Actual: {columns}")
        print(f"  Expected: {expected_columns}")


    # Test with a DDL that has different formatting (e.g., no NOT NULL, different order)
    sample_ddl_2 = """
    CREATE OR REPLACE TABLE ANOTHER_DB.ANOTHER_SCHEMA.ANOTHER_TABLE (
        COL1 VARCHAR,
        COL2 INTEGER COMMENT 'Some integer value',
        COL3 TIMESTAMP_NTZ NOT NULL,
        COL4 TEXT
    );
    """
    columns_2 = extract_columns_from_ddl(sample_ddl_2)
    print("\nExtracted columns from sample DDL 2:")
    for col in columns_2:
        print(f"  - Name: {col['name']}, Type: {col['type']}")
    
    expected_columns_2 = [
        {"name": "COL1", "type": "VARCHAR"},
        {"name": "COL2", "type": "INTEGER"},
        {"name": "COL3", "type": "TIMESTAMP_NTZ"},
        {"name": "COL4", "type": "TEXT"},
    ]
    if columns_2 == expected_columns_2:
        print("SUCCESS: DDL column extraction 2 matches expected.")
    else:
        print("FAILURE: DDL column extraction 2 DOES NOT match expected.")
        print(f"  Actual: {columns_2}")
        print(f"  Expected: {expected_columns_2}")

    # Test with a DDL that has complex parameters
    sample_ddl_3 = """
    CREATE TABLE MY_DB.COMPLEX_SCHEMA.COMPLEX_TABLE (
        AMOUNT_NUM NUMBER(38, 9),
        LONG_TEXT VARCHAR(16777216),
        FLAG_VAL BOOLEAN
    );
    """
    columns_3 = extract_columns_from_ddl(sample_ddl_3)
    print("\nExtracted columns from sample DDL 3:")
    for col in columns_3:
        print(f"  - Name: {col['name']}, Type: {col['type']}")
    
    expected_columns_3 = [
        {"name": "AMOUNT_NUM", "type": "NUMBER(38, 9)"},
        {"name": "LONG_TEXT", "type": "VARCHAR(16777216)"},
        {"name": "FLAG_VAL", "type": "BOOLEAN"},
    ]
    if columns_3 == expected_columns_3:
        print("SUCCESS: DDL column extraction 3 matches expected.")
    else:
        print("FAILURE: DDL column extraction 3 DOES NOT match expected.")
        print(f"  Actual: {columns_3}")
        print(f"  Expected: {expected_columns_3}")


    print("\n--- Testing ddl_utils.py complete ---")

