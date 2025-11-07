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
    Parses a Snowflake CREATE TABLE DDL string to extract column names and their types.
    It is designed to be highly robust to various formatting, complex DEFAULT clauses,
    and table-level constraints.
    
    Args:
        ddl_string (str): The CREATE TABLE DDL statement from Snowflake.
        
    Returns:
        list: A list of dicts, each with 'name' and 'type' for a column.
              Returns an empty list if DDL cannot be parsed or if no columns found.
    """
    columns = []
    if not ddl_string or not isinstance(ddl_string, str):
        return []

    # NEW HIGHLY ROBUST TABLE PATTERN:
    # Captures the content inside the FIRST outermost parentheses of a CREATE TABLE statement.
    # This accounts for 'OR REPLACE', multi-part table names, and allows any content within parentheses.
    table_pattern = re.compile(
        r"CREATE (?:OR REPLACE )?TABLE \S+(?:\.\S+){1,2}\s*\("  # Matches CREATE TABLE DB.SCHEMA.TABLE (
        r"(.*?)"                                            # Group 1: Captures everything non-greedily until the next part
        r"\)[^;]*;",                                         # Matches the closing parenthesis, optional non-semicolons, then semicolon
        re.DOTALL | re.IGNORECASE                           # DOTALL to match newlines, IGNORECASE for flexibility
    )
    match = table_pattern.search(ddl_string)

    if not match:
        print(f"WARNING: Could not find CREATE TABLE structure in DDL: {ddl_string[:100].replace('\n', ' ')}...")
        return []

    columns_and_constraints_block = match.group(1) # This is the entire content inside the main ( )
    
    # NEW ROBUST COLUMN DEFINITION PATTERN:
    # This pattern is applied line-by-line or on segments of the block.
    # It specifically targets COLUMN NAME and DATA TYPE, and then ignores everything else on that line.
    
    column_def_pattern = re.compile(
        r'^\s*([A-Z0-9_]+)\s+'  # Group 1: Column Name (alphanumeric, underscore)
        r'([A-Z0-9_]+\s*(?:\(\s*\d+(?:\s*,\s*\d+)?\s*\))?)' # Group 2: Data Type (e.g., VARCHAR(6), NUMBER(38,0))
        r'(?:'                                              # Start non-capturing group for modifiers to ignore
        r'\s+'                                              # One or more spaces
        r'(?:NOT\s+NULL|DEFAULT(?:\s+CAST\(.*?\)\s*)?|COMMENT\s+\'.*?\'|[A-Z0-9_]+)' # Common modifiers (DEFAULT CAST handled)
        r')*?'                                              # Match zero or more modifiers non-greedily
        r',?'                                               # Optional trailing comma
        r'$',                                               # End of line
        re.IGNORECASE | re.MULTILINE                        # IGNORECASE and MULTILINE for line-by-line processing
    )

    # Patterns to explicitly identify and IGNORE table-level constraints
    table_constraint_patterns = [
        re.compile(r'^\s*CONSTRAINT\s+', re.IGNORECASE),
        re.compile(r'^\s*PRIMARY\s+KEY\s*\(', re.IGNORECASE),
        re.compile(r'^\s*FOREIGN\s+KEY\s*\(', re.IGNORECASE),
        re.compile(r'^\s*UNIQUE\s*\(', re.IGNORECASE),
        re.compile(r'^\s*CHECK\s*\(', re.IGNORECASE),
    ]

    # Process each line from the DDL block
    for line in columns_and_constraints_block.splitlines():
        stripped_line = line.strip()
        if not stripped_line:
            continue # Skip empty lines

        # First, check if this line is clearly a table-level constraint
        is_constraint_line = False
        for pattern in table_constraint_patterns:
            if pattern.match(stripped_line):
                is_constraint_line = True
                break
        
        if is_constraint_line:
            continue # Skip this line, it's a table-level constraint

        # If not a constraint, try to parse it as a column definition
        column_match = column_def_pattern.match(stripped_line)
        if column_match:
            col_name = column_match.group(1).upper()
            col_type = column_match.group(2).upper()
            columns.append({"name": col_name, "type": col_type})
        # else:
            # print(f"DEBUG: Could not parse column from line: '{stripped_line}'")


    return columns


# validate_source_to_fqdn_map function (remains unchanged)
def validate_source_to_fqdn_map(db_file=None):
    # ... (unchanged)

# Test block for ddl_utils.py (MODIFIED with your ML DDL and expanded tests)
if __name__ == "__main__":
    print("--- Testing ddl_utils.py functions ---")

    # Your provided ML DDL example
    ml_ddl_example = """
    create or replace TABLE ISSUER_TICKER (
        ISSUER VARCHAR(6) NOT NULL DEFAULT '',
        TICKER VARCHAR(32) NOT NULL DEFAULT '',
        EXCHANGE VARCHAR(16) NOT NULL DEFAULT '',
        MODIFY_TIME TIMESTAMP_NTZ(3),
        CREATED_BY_USER_ID VARCHAR(255) NOT NULL DEFAULT '',
        CREATED_DTS TIMESTAMP_LTZ(3) NOT NULL DEFAULT CAST('0001-01-01 00:00:00' AS TIMESTAMP_LTZ(3)),
        DATA_DOMAIN VARCHAR(10) NOT NULL DEFAULT '',
        HVR_CAPTURE_LOCATION VARCHAR(255) NOT NULL DEFAULT '',
        HVR_CHANGE_OP NUMBER(38,0) NOT NULL DEFAULT 0,
        HVR_CHANGE_TIME TIMESTAMP_LTZ(3) NOT NULL DEFAULT CAST('0001-01-01 00:00:00' AS TIMESTAMP_LTZ(3)),
        HVR_DUPLICATE VARCHAR(1) NOT NULL DEFAULT '',
        HVR_TX_COUNT_DOWN NUMBER(38,0) NOT NULL DEFAULT 0,
        HVR_TX_SEQUENCE VARCHAR(45) NOT NULL DEFAULT '',
        PROVIDER_NM VARCHAR(10) NOT NULL DEFAULT '',
        HVR_DC VARCHAR(10) NOT NULL DEFAULT '',
        primary key (ISSUER, EXCHANGE, HVR_TX_SEQUENCE)
    );
    """
    ml_columns = extract_columns_from_ddl(ml_ddl_example)
    print("\nExtracted columns from ML DDL example:")
    for col in ml_columns:
        print(f"  - Name: {col['name']}, Type: {col['type']}")
    
    expected_ml_columns = [
        {"name": "ISSUER", "type": "VARCHAR(6)"},
        {"name": "TICKER", "type": "VARCHAR(32)"},
        {"name": "EXCHANGE", "type": "VARCHAR(16)"},
        {"name": "MODIFY_TIME", "type": "TIMESTAMP_NTZ(3)"},
        {"name": "CREATED_BY_USER_ID", "type": "VARCHAR(255)"},
        {"name": "CREATED_DTS", "type": "TIMESTAMP_LTZ(3)"},
        {"name": "DATA_DOMAIN", "type": "VARCHAR(10)"},
        {"name": "HVR_CAPTURE_LOCATION", "type": "VARCHAR(255)"},
        {"name": "HVR_CHANGE_OP", "type": "NUMBER(38,0)"},
        {"name": "HVR_CHANGE_TIME", "type": "TIMESTAMP_LTZ(3)"},
        {"name": "HVR_DUPLICATE", "type": "VARCHAR(1)"},
        {"name": "HVR_TX_COUNT_DOWN", "type": "NUMBER(38,0)"},
        {"name": "HVR_TX_SEQUENCE", "type": "VARCHAR(45)"},
        {"name": "PROVIDER_NM", "type": "VARCHAR(10)"},
        {"name": "HVR_DC", "type": "VARCHAR(10)"},
    ]
    if ml_columns == expected_ml_columns:
        print("SUCCESS: ML DDL column extraction matches expected.")
    else:
        print("FAILURE: ML DDL column extraction DOES NOT match expected.")
        print(f"  Actual: {ml_columns}")
        print(f"  Expected: {expected_ml_columns}")


    # Previous test cases (keep for robustness)
    sample_ddl = """
    CREATE TABLE MY_DB.MY_SCHEMA.MY_TABLE (
        ID NUMBER(38,0) NOT NULL COMMENT 'Primary key for the table',
        NAME VARCHAR(255) COMMENT 'Name of the item',
        AMOUNT DECIMAL(18,2),
        IS_ACTIVE BOOLEAN DEFAULT TRUE,
        CREATED_DATE TIMESTAMP_LTZ(9),
        CONSTRAINT PK_MY_TABLE PRIMARY KEY (ID),
        FOREIGN KEY (ID) REFERENCES OTHER_TABLE(OTHER_ID)
    );
    """
    columns = extract_columns_from_ddl(sample_ddl)
    print("\nExtracted columns from sample DDL:")
    # ... (print and check logic as before)
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
    # ... (print and check logic as before)
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

    sample_ddl_3 = """
    CREATE TABLE MY_DB.COMPLEX_SCHEMA.COMPLEX_TABLE (
        AMOUNT_NUM NUMBER(38, 9),
        LONG_TEXT VARCHAR(16777216),
        FLAG_VAL BOOLEAN
    );
    """
    columns_3 = extract_columns_from_ddl(sample_ddl_3)
    print("\nExtracted columns from sample DDL 3:")
    # ... (print and check logic as before)
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

    sample_ddl_view = """
    CREATE VIEW MY_DB.MY_SCHEMA.MY_VIEW AS
    SELECT ID, NAME FROM OTHER_TABLE;
    """
    columns_view = extract_columns_from_ddl(sample_ddl_view)
    print("\nExtracted columns from sample VIEW DDL:")
    if not columns_view:
        print("  (No columns extracted, as expected for a VIEW)")
    else:
        print(f"  {columns_view}")
    if not columns_view:
        print("SUCCESS: DDL column extraction for VIEW DDL returns empty list as expected.")
    else:
        print("FAILURE: DDL column extraction for VIEW DDL returned non-empty list.")


    print("\n--- Testing ddl_utils.py complete ---")
