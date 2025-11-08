# data_type_mapper.py (MODIFIED resolve_snowflake_data_type for correct parameter handling)

import os
import json
from datetime import datetime
import argparse
import re
from tabulate import tabulate

from config import FilePaths, load_data_type_map, SNOWFLAKE_VALID_BASE_TYPES, TYPE_SYNONYMS
from database_manager import DatabaseManager
from confluence_utils import clean_special_characters_iterative

from sqlglot import parse_one, exp
from sqlglot.errors import ParseError


# Helper function to clean SQLGlot error messages (UNMODIFIED)
def _clean_sqlglot_error_message(error_message):
    """
    Cleans SQLGlot error messages, removing the full SQL statement context
    and ANSI escape codes, to leave only the concise error message.
    """
    cleaned_message = re.sub(r'\x1b\[[0-9;]*[a-zA-Z]', '', error_message)
    
    match = re.search(r'(Expecting.*?|Incorrect syntax.*?).*(?:SELECT CAST\(1 AS.*)', cleaned_message, re.DOTALL)
    if match:
        return match.group(1).strip()
    
    return cleaned_message.strip()


def resolve_snowflake_data_type(confluence_data_type, data_type_map):
    """
    Resolves a Confluence data type string to its corresponding Snowflake data type
    using sqlglot for robust parsing and validation against a type map and Snowflake base types.
    Correctly re-applies parameters (length/precision) if they exist.
    
    Args:
        confluence_data_type (str): The raw data type string from Confluence.
        data_type_map (dict): The loaded data type mapping (keys are uppercase Confluence base types).
        
    Returns:
        tuple: (resolved_snowflake_type: str, warnings: list[str])
    """
    warnings = []
    resolved_type_internal = "VARCHAR(16777216)" 
    
    if not confluence_data_type or not isinstance(confluence_data_type, str):
        warnings.append(f"Missing or invalid Confluence data type input: '{confluence_data_type}'")
        return resolved_type_internal, warnings

    cleaned_conf_type = confluence_data_type.upper().strip()
    
    if "FLOAT OR NUMBER" in cleaned_conf_type:
        cleaned_conf_type = cleaned_conf_type.replace("FLOAT OR NUMBER", "NUMBER")
    
    parsed_base_type_raw = None
    parsed_base_type_canonical = None
    parsed_params = [] # NEW: This will store the string parameters, e.g., ["128"] or ["38", "0"]

    is_fundamentally_malformed = False

    try:
        type_statement = parse_one(f"SELECT CAST(1 AS {cleaned_conf_type})", read="snowflake") 
        data_type_node = next(type_statement.find_all(exp.DataType), None)

        if data_type_node:
            parsed_base_type_raw = data_type_node.this.name.upper()
            parsed_base_type_canonical = TYPE_SYNONYMS.get(parsed_base_type_raw, parsed_base_type_raw)

            for param in data_type_node.expressions:
                if isinstance(param, exp.DataTypeParam):
                    parsed_params.append(param.this.name) # Store the parameter strings
        else:
            warnings.append(f"SQLGlot could not identify a valid DataType node for '{confluence_data_type}'. Defaulting to VARCHAR.")
            is_fundamentally_malformed = True
            
    except ParseError as e:
        clean_error = _clean_sqlglot_error_message(str(e))
        warnings.append(f"Malformed or unrecognized data type format: '{confluence_data_type}' (SQLGlot parse error: {clean_error}). Defaulting to VARCHAR.")
        is_fundamentally_malformed = True
    except Exception as e:
        warnings.append(f"Unexpected error during SQLGlot parsing for '{confluence_data_type}': {e}. Defaulting to VARCHAR.")
        is_fundamentally_malformed = True
    
    
    if cleaned_conf_type.count('(') != cleaned_conf_type.count(')'):
        warnings.append(f"Mismatched parentheses in type '{confluence_data_type}'. Parameters will be discarded.")
        parsed_params = [] # Discard parameters if parentheses are mismatched
        is_fundamentally_malformed = True


    if is_fundamentally_malformed:
        return resolved_type_internal, warnings 


    if not parsed_base_type_canonical:
        warnings.append(f"Could not determine base type for '{confluence_data_type}'. Defaulting to VARCHAR.")
        return resolved_type_internal, warnings

    if parsed_base_type_canonical not in SNOWFLAKE_VALID_BASE_TYPES:
        warnings.append(f"Parsed base type '{parsed_base_type_canonical}' (from '{confluence_data_type}') is not a known Snowflake base type. Defaulting to VARCHAR.")
        return resolved_type_internal, warnings 


    snowflake_base_type_from_map = data_type_map.get(parsed_base_type_canonical)

    if snowflake_base_type_from_map:
        # If the map explicitly gives a full type with parameters (e.g., "INTEGER" -> "NUMBER(38,0)"),
        # then we prioritize the map's full type.
        if re.match(r'^[A-Z_]+\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\)$', snowflake_base_type_from_map.upper().strip()):
             return snowflake_base_type_from_map, warnings # Map provides a full type, use it directly
        
        # If the map gives a base type (e.g., "VARCHAR" -> "VARCHAR"), then re-apply original parameters.
        # But handle specific cases like NUMBER/INTEGER defaults if parameters were NOT parsed from Confluence.
        if not parsed_params: # No parameters were found/valid in Confluence type
            if snowflake_base_type_from_map.upper() == 'NUMBER' and parsed_base_type_canonical in ['NUMBER', 'INTEGER', 'INT', 'DECIMAL', 'NUMERIC']:
                return "NUMBER(38,0)", warnings # Default precision/scale for INTEGER/NUMBER
            else:
                return snowflake_base_type_from_map, warnings # Just the base type
        else: # Parameters *were* parsed from Confluence (e.g., "(128)" for VARCHAR)
            # Re-assemble type with parameters
            # This is where VARCHAR(128) -> VARCHAR(128) is handled
            return f"{snowflake_base_type_from_map}({', '.join(parsed_params)})", warnings

    else:
        # If the base type is not found in the map, default to VARCHAR
        warnings.append(f"Confluence data type '{confluence_data_type}' (base: '{parsed_base_type_canonical}') not found in map. Defaulting to VARCHAR.")
        return resolved_type_internal, warnings


def generate_data_type_report(config_file=None):
    # ... (rest of the function remains unchanged)
    """
    Generates a report on Confluence data types and their resolved Snowflake equivalents.
    Identifies unmapped/malformed Confluence data types and reports them in separate sections.
    """
    print("\n--- Starting Data Type Mapping and Report Generation ---")

    db_manager = DatabaseManager()
    
    try:
        data_type_map = load_data_type_map()
    except Exception as e:
        print(f"ERROR: Failed to load data type map: {e}")
        db_manager.disconnect()
        return

    confluence_data_types_with_sources = {} 

    try:
        cursor = db_manager.conn.cursor()
        page_metadata_map = {p['page_id']: p['api_title'] for p in cursor.execute("SELECT page_id, api_title FROM confluence_page_metadata").fetchall()}

        cursor.execute("SELECT page_id, parsed_json FROM confluence_parsed_content")
        
        for row in cursor.fetchall():
            page_id = row['page_id']
            page_title = page_metadata_map.get(page_id, f"Page ID:{page_id}")
            parsed_content_json_str = row['parsed_json']
            if parsed_content_json_str:
                parsed_content = json.loads(parsed_content_json_str)
                cleaned_parsed_content = parsed_content 
                
                for table_data in cleaned_parsed_content.get('tables', []):
                    if table_data.get('id') == 'table_1':
                        for column in table_data.get('columns', []):
                            conf_data_type = column.get('data_type')
                            if conf_data_type and conf_data_type.strip():
                                conf_type_key = conf_data_type.strip()
                                if conf_type_key not in confluence_data_types_with_sources:
                                    confluence_data_types_with_sources[conf_type_key] = set()
                                confluence_data_types_with_sources[conf_type_key].add(page_title)

    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in confluence_parsed_content for a page: {e}")
        db_manager.disconnect()
        return
    except Exception as e:
        print(f"ERROR: Failed to retrieve Confluence data types from DB: {e}")
        db_manager.disconnect()
        return
    finally:
        db_manager.disconnect()

    if not confluence_data_types_with_sources:
        print("No Confluence data types found in parsed content (table_1) to map.")
        return

    # --- Generate Report Content ---
    report_lines = []
    report_lines.append(f"# Confluence Data Type Mapping Report")
    report_lines.append(f"Generated On: {datetime.now().isoformat()}\n")

    report_data_rows = []
    
    syntax_or_malformed_warnings = {} 
    unmapped_types_for_action = {}    

    for conf_type in sorted(confluence_data_types_with_sources.keys()):
        resolved_sf_type, warnings_list = resolve_snowflake_data_type(conf_type, data_type_map)
        
        notes = "; ".join(warnings_list) 

        # Categorize for separate report sections based on warning content
        is_malformed_syntax = any(
            "Malformed" in w or "Unrecognized" in w or "SQLGlot parse" in w or 
            "Unexpected error during SQLGlot parsing" in w or "Mismatched" in w or
            "not a known Snowflake base type" in w 
            for w in warnings_list
        )
        is_unmapped_in_json = any("not found in map" in w for w in warnings_list)

        if is_malformed_syntax:
            syntax_or_malformed_warnings[conf_type] = warnings_list
        elif is_unmapped_in_json:
            unmapped_types_for_action[conf_type] = ", ".join(sorted(list(confluence_data_types_with_sources[conf_type])))
        
        if not notes:
             notes = "Mapped via data_type_map.json"
        
        report_data_rows.append([
            conf_type,
            resolved_sf_type,
            notes
        ])
    
    headers = ["Confluence Type", "Resolved Snowflake Type", "Notes"]
    report_lines.append("## 1. Confluence Data Type Resolution\n")
    report_lines.append(tabulate(report_data_rows, headers=headers, tablefmt="pipe"))
    report_lines.append("\n")

    if syntax_or_malformed_warnings:
        report_lines.append("## 2. Data Type Syntax / Malformation Warnings")
        report_lines.append(f"**ACTION REQUIRED:** The following Confluence data types have syntax or format issues or use non-standard base types. These have been strictly defaulted to VARCHAR(16777216) in the generated outputs.")
        for conf_type, warnings_list in sorted(syntax_or_malformed_warnings.items()):
            pages_str = ", ".join(sorted(list(confluence_data_types_with_sources[conf_type])))
            report_lines.append(f"  - Type: '{conf_type}' (Found in pages: {pages_str})")
            for warning in warnings_list:
                report_lines.append(f"    - WARNING: {warning}")
        report_lines.append(f"Please correct the data types in Confluence or update '{FilePaths.DATA_TYPE_MAP_FILE}' if this is a known variant.\n")

    if unmapped_types_for_action:
        report_lines.append("## 3. Unmapped Confluence Data Types")
        report_lines.append(f"**ACTION REQUIRED:** The following Confluence data types were not explicitly mapped (though syntactically valid) and have been defaulted to VARCHAR(16777216).")
        report_lines.append(f"Please review and update '{FilePaths.DATA_TYPE_MAP_FILE}'.")
        for conf_type, pages_str in sorted(unmapped_types_for_action.items()):
            report_lines.append(f"  - Type: '{conf_type}' (Found in pages: {pages_str})")
    else:
        if not syntax_or_malformed_warnings:
            report_lines.append("All Confluence data types found were either explicitly mapped or known to default to VARCHAR.")

    report_filename = f"confluence_data_type_report.md"
    report_filepath = os.path.join(FilePaths.REPORT_OUTPUT_DIR, report_filename)
    os.makedirs(FilePaths.REPORT_OUTPUT_DIR, exist_ok=True)

    with open(report_filepath, 'w', encoding='utf-8') as f:
        f.write("\n".join(report_lines))
    
    print(f"\n--- Confluence Data Type Mapping Report saved to: {report_filepath} ---")
    print("ACTION REQUIRED: Review the generated report for data type mappings.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generates a report on Confluence data types and their resolved Snowflake equivalents."
    )
    args = parser.parse_args() 
    
    generate_data_type_report()
