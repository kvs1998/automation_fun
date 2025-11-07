# data_type_mapper.py
import os
import json
from datetime import datetime
import argparse
import re
from tabulate import tabulate # For pretty printing tables in Markdown

from config import FilePaths, load_data_type_map
from database_manager import DatabaseManager
from confluence_utils import clean_special_characters_iterative # Assumed to be available


def resolve_snowflake_data_type(confluence_data_type, data_type_map):
    """
    Resolves a Confluence data type string to its corresponding Snowflake data type.
    If the type string is malformed or unrecognized, it's strictly defaulted to VARCHAR(16777216).
    Returns the resolved type and a list of any warnings/errors encountered.
    
    Args:
        confluence_data_type (str): The raw data type string from Confluence (e.g., "VARCHAR(6)", "NUMBER", "Integer").
        data_type_map (dict): The loaded data type mapping (keys are uppercase Confluence base types).
        
    Returns:
        tuple: (resolved_snowflake_type: str, warnings: list[str])
    """
    warnings = []
    resolved_type_internal = "VARCHAR(16777216)" # Initialize with the ultimate default
    
    if not confluence_data_type or not isinstance(confluence_data_type, str):
        warnings.append(f"Missing or invalid Confluence data type input: '{confluence_data_type}'")
        return resolved_type_internal, warnings

    cleaned_conf_type = confluence_data_type.upper().strip()
    
    # Handle composite types like "FLOAT or NUMBER" before main regex, converting to canonical.
    if "FLOAT OR NUMBER" in cleaned_conf_type:
        cleaned_conf_type = cleaned_conf_type.replace("FLOAT OR NUMBER", "NUMBER")
    
    base_type_confluence = None
    params_confluence = "" 
    
    is_fundamentally_malformed = False # Flag to decide if we default immediately

    # Regex to capture base type and a *potentially valid* parameter part
    # It allows for: (digits) or (digits,digits)
    # e.g., "VARCHAR(6)", "NUMBER(18,2)", "INTEGER"
    match = re.match(r'([A-Z_]+)\s*(\(\s*\d+(?:\s*,\s*\d+)?\s*\))?', cleaned_conf_type)
    
    if match:
        base_type_confluence = match.group(1)
        if match.group(2):
            params_confluence = match.group(2)
        
        # Additional check for malformed parameters specifically
        if params_confluence and not re.fullmatch(r'\(\s*\d+(?:\s*,\s*\d+)?\s*\)', params_confluence):
            warnings.append(f"Malformed parameters '{params_confluence}' for type '{confluence_data_type}'. Parameters will be discarded.")
            params_confluence = "" # Discard malformed params
            # This alone doesn't make it *fundamentally* malformed; we still try to map the base.
    else:
        # If no regex match, it means the structure itself is unrecognized
        warnings.append(f"Unrecognized or malformed data type format: '{confluence_data_type}'. Defaulting to VARCHAR.")
        is_fundamentally_malformed = True # This type is fundamentally broken

    # Check for mismatched parentheses (independent of regex match success, as it could be `VARCHAR(232 NUMBER`)
    if cleaned_conf_type.count('(') != cleaned_conf_type.count(')'):
        warnings.append(f"Mismatched parentheses in type '{confluence_data_type}'. Parameters will be discarded.")
        params_confluence = "" # Discard parameters
        is_fundamentally_malformed = True # This type is also fundamentally broken due to mismatch


    # --- DECISION POINT: If fundamentally malformed, skip mapping and use ultimate default ---
    if is_fundamentally_malformed:
        # If no base type could be extracted, use the cleaned_conf_type as base for lookup, 
        # but if it was fundamentally malformed, we already know we default.
        # This branch ensures the output type is the default VARCHAR.
        return resolved_type_internal, warnings 


    # --- If not fundamentally malformed, proceed with mapping attempt ---
    # Ensure base_type_confluence is set (it should be if not fundamentally_malformed after regex match)
    if not base_type_confluence: # Final check before map lookup
        warnings.append(f"Could not determine base type for '{confluence_data_type}'. Defaulting to VARCHAR.")
        return resolved_type_internal, warnings

    # Lookup in the provided data_type_map (keys are already uppercase)
    snowflake_base_type = data_type_map.get(base_type_confluence)

    if snowflake_base_type:
        # Special handling for NUMBER/INTEGER default precision if SME doesn't specify
        if snowflake_base_type.upper() == 'NUMBER' and base_type_confluence in ['NUMBER', 'INTEGER', 'INT', 'DECIMAL', 'NUMERIC']:
            if not params_confluence:
                return "NUMBER(38,0)", warnings
        
        # General case: If Confluence gave parameters, and Snowflake type is compatible, keep parameters
        if params_confluence and (snowflake_base_type.upper() in ["VARCHAR", "NUMBER", "DECIMAL", "CHAR", "STRING", "TEXT"]):
            # Parameters should already be clean/discarded if malformed.
            return f"{snowflake_base_type}{params_confluence}", warnings
        else:
            return snowflake_base_type, warnings

    else:
        # If the base type is not found in the map, default to VARCHAR
        warnings.append(f"Confluence data type '{confluence_data_type}' (base: '{base_type_confluence}') not found in map. Defaulting to VARCHAR.")
        return resolved_type_internal, warnings


def generate_data_type_report(config_file=None):
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
    
    syntax_or_malformed_warnings = {} # For types with syntax issues that defaulted to VARCHAR
    unmapped_types_for_action = {}    # For types not in map, but syntactically fine, defaulted to VARCHAR

    for conf_type in sorted(confluence_data_types_with_sources.keys()):
        resolved_sf_type, warnings_list = resolve_snowflake_data_type(conf_type, data_type_map)
        
        notes = "; ".join(warnings_list) # Combine all warnings into notes for the main table row

        # Categorize for separate report sections based on warning content
        is_malformed = any("Malformed" in w or "Unrecognized" in w or "Mismatched" in w or "Invalid content" in w for w in warnings_list)
        is_unmapped = any("not found in map" in w for w in warnings_list)

        if is_malformed:
            syntax_or_malformed_warnings[conf_type] = warnings_list
        elif is_unmapped: # Only categorize as unmapped if it's not also malformed
            unmapped_types_for_action[conf_type] = ", ".join(sorted(list(confluence_data_types_with_sources[conf_type])))
        
        # If notes are empty, means it was perfectly mapped
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
        report_lines.append(f"**ACTION REQUIRED:** The following Confluence data types have syntax or format issues. These have been strictly defaulted to VARCHAR(16777216) in the generated outputs.")
        for conf_type, warnings_list in sorted(syntax_or_malformed_warnings.items()):
            pages_str = ", ".join(sorted(list(confluence_data_types_with_sources[conf_type])))
            report_lines.append(f"  - Type: '{conf_type}' (Found in pages: {pages_str})")
            for warning in warnings_list:
                report_lines.append(f"    - WARNING: {warning}")
        report_lines.append(f"Please correct the data types in Confluence or update '{FilePaths.DATA_TYPE_MAP_FILE}' if this is a known variant.\n")

    if unmapped_types_for_action:
        report_lines.append("## 3. Unmapped Confluence Data Types")
        report_lines.append(f"**ACTION REQUIRED:** The following Confluence data types were not explicitly mapped and have been defaulted to VARCHAR(16777216).")
        report_lines.append(f"Please review and update '{FilePaths.DATA_TYPE_MAP_FILE}'.")
        for conf_type, pages_str in sorted(unmapped_types_for_action.items()):
            report_lines.append(f"  - Type: '{conf_type}' (Found in pages: {pages_str})")
    else:
        # This else branch ensures a message is printed if there are *no* issues at all
        if not syntax_or_malformed_warnings:
            report_lines.append("All Confluence data types found were either explicitly mapped or known to default to VARCHAR.")

    # --- Save Report to File ---
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
