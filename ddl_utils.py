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
from sqlglot import parse_one, exp
from sqlglot.errors import ParseError


def extract_columns_from_ddl(ddl_string):
    """
    Parses a Snowflake CREATE TABLE DDL string using sqlglot to extract column names and their types.
    It robustly handles various formatting, constraints, and complex default values.
    
    Args:
        ddl_string (str): The CREATE TABLE DDL statement from Snowflake.
        
    Returns:
        list: A list of dicts, each with 'name' and 'type' for a column.
              Returns an empty list if DDL cannot be parsed or if no columns found.
    """
    columns = []
    if not ddl_string or not isinstance(ddl_string, str):
        return []

    try:
        # Parse the DDL string using sqlglot, specifying the Snowflake dialect
        # parse_one returns an Expression object (the AST)
        # We need to ensure it's a CREATE TABLE statement
        expression = parse_one(ddl_string, dialect="snowflake")
        
        if not isinstance(expression, exp.Create):
            print(f"WARNING: DDL is not a CREATE statement. Skipping: {ddl_string[:100].replace('\n', ' ')}...")
            return []

        # Find the TABLE expression within the CREATE statement
        table_expression = expression.this # The main object being created

        # Iterate through the expressions (definitions) within the table creation
        # These are usually arguments to the CREATE TABLE, like column definitions and constraints
        for element in expression.expressions: # Accessing arguments
            if isinstance(element, exp.ColumnDef):
                # If it's a ColumnDef expression, extract name and data type
                col_name = element.this.name.upper() # Column name
                col_type = element.args.get('kind').this.name.upper() # Data type name
                
                # Extract parameters like (38,0) for NUMBER or (255) for VARCHAR
                # The 'this' of the 'kind' expression is the base type (e.g., NUMBER),
                # its expressions are the parameters
                type_params = []
                for param in element.args.get('kind').expressions:
                    if isinstance(param, exp.DataTypeParam): # For NUMBER(P,S) or VARCHAR(L)
                        type_params.append(param.this.name)
                    # More complex parameters might need custom handling
                
                if type_params:
                    # Reconstruct type string, e.g., "NUMBER(38,0)"
                    full_col_type = f"{col_type}({', '.join(type_params)})"
                else:
                    full_col_type = col_type

                columns.append({"name": col_name, "type": full_col_type})
            # We explicitly ignore other elements like exp.Constraint (PRIMARY KEY, FOREIGN KEY)
            # or exp.Comment, etc., as we are only interested in column definitions here.

    except ParseError as e:
        print(f"ERROR: SQLGlot Parse Error for DDL: {ddl_string[:100].replace('\n', ' ')}... Error: {e}")
        return []
    except Exception as e:
        print(f"ERROR: An unexpected error occurred during DDL parsing with SQLGlot: {e}. DDL: {ddl_string[:100].replace('\n', ' ')}...")
        return []

    return columns


# validate_source_to_fqdn_map function (remains unchanged)
def validate_source_to_fqdn_map(db_file=None):
    # ... (unchanged)

# Test block for ddl_utils.py (UPDATED with sqlglot for DDL parsing)
if __name__ == "__main__":
    print("--- Testing ddl_utils.py functions (with SQLGlot) ---")

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
