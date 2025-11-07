# data_type_mapper.py
import os
import json
from datetime import datetime
import argparse # For custom config file

from config import FilePaths, load_data_type_map
from database_manager import DatabaseManager
from confluence_utils import clean_special_characters_iterative # For cleaning parsed content


def resolve_snowflake_data_type(confluence_data_type, data_type_map):
    """
    Resolves a Confluence data type string to its corresponding Snowflake data type.
    Handles length/precision and defaults to VARCHAR if base type is unmapped or empty.
    
    Args:
        confluence_data_type (str): The raw data type string from Confluence (e.g., "VARCHAR(6)", "NUMBER", "Integer").
        data_type_map (dict): The loaded data type mapping (keys are uppercase Confluence base types).
        
    Returns:
        str: The resolved Snowflake data type, or a formatted error string if unmappable.
    """
    if not confluence_data_type or not isinstance(confluence_data_type, str):
        # NEW: Default empty or non-string types to VARCHAR
        return "VARCHAR(16777216)" # Snowflake's max VARCHAR length as a safe default

    # Extract base type and any precision/scale/length
    # e.g., "VARCHAR(6)" -> base="VARCHAR", params="(6)"
    #       "NUMBER(18,2)" -> base="NUMBER", params="(18,2)"
    #       "INTEGER" -> base="INTEGER", params=""
    match = re.match(r'([A-Z_]+)\s*(\(.*\))?', confluence_data_type.upper().strip())
    
    base_type_confluence = None
    params_confluence = "" # Will include parentheses if present

    if match:
        base_type_confluence = match.group(1)
        if match.group(2): # If (something) exists
            params_confluence = match.group(2)
    else:
        # If no regex match, just take the whole thing as base type
        base_type_confluence = confluence_data_type.upper().strip()

    if not base_type_confluence:
        # Should not happen if confluence_data_type is not empty, but for safety
        return "VARCHAR(16777216)" # Fallback default

    # Lookup in the provided data_type_map (keys are already uppercase)
    snowflake_base_type = data_type_map.get(base_type_confluence)

    if snowflake_base_type:
        # If Confluence type explicitly has parameters, try to retain them for Snowflake
        # e.g., VARCHAR(6) -> VARCHAR(6)
        # If Confluence type is just INTEGER, map to NUMBER (no params needed)
        
        # Special handling for NUMBER/INTEGER default precision if SME doesn't specify
        if snowflake_base_type.upper() == 'NUMBER' and 'NUMBER' in base_type_confluence: # Check if original base was number-like
            # If SME provided 'NUMBER(18,2)', use that. Otherwise, map "NUMBER" to "NUMBER(38,0)"
            if not params_confluence: # If Confluence didn't specify precision/scale
                return "NUMBER(38,0)" # Default precision/scale for INTEGER/NUMBER in Snowflake
        elif snowflake_base_type.upper() == 'INTEGER' and 'INTEGER' in base_type_confluence:
            if not params_confluence:
                return "NUMBER(38,0)" # Default for INTEGER if SME didn't specify
        
        # General case: If Confluence gave params, and Snowflake type is compatible, keep params
        if params_confluence and (snowflake_base_type.upper() in ["VARCHAR", "NUMBER", "DECIMAL", "CHAR", "STRING"]):
            return f"{snowflake_base_type}{params_confluence}"
        else:
            return snowflake_base_type # Otherwise, use base Snowflake type

    else:
        # If the base type is not found in the map, default to VARCHAR
        print(f"  WARNING: Confluence data type '{confluence_data_type}' (base: '{base_type_confluence}') not found in map. Defaulting to VARCHAR.")
        return "VARCHAR(16777216)" # Default for unmapped types


def generate_data_type_report(config_file=None):
    """
    Generates a report on Confluence data types and their resolved Snowflake equivalents.
    Identifies unmapped Confluence data types.
    """
    print("\n--- Starting Data Type Mapping and Report Generation ---")

    db_manager = DatabaseManager()
    
    # Load data type map
    try:
        data_type_map = load_data_type_map()
    except Exception as e:
        print(f"ERROR: Failed to load data type map: {e}")
        db_manager.disconnect()
        return

    # Collect all unique Confluence data types from parsed content
    unique_confluence_data_types = set()
    try:
        cursor = db_manager.conn.cursor()
        cursor.execute("SELECT parsed_json FROM confluence_parsed_content")
        
        for row in cursor.fetchall():
            parsed_content_json_str = row['parsed_json']
            if parsed_content_json_str:
                parsed_content = json.loads(parsed_content_json_str)
                # No need for deep clean here, data_parser should have already cleaned it
                cleaned_parsed_content = parsed_content # Assume already clean
                
                for table_data in cleaned_parsed_content.get('tables', []):
                    if table_data.get('id') == 'table_1': # Only process 'table_1'
                        for column in table_data.get('columns', []):
                            conf_data_type = column.get('data_type')
                            if conf_data_type and conf_data_type.strip():
                                unique_confluence_data_types.add(conf_data_type.strip())
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in confluence_parsed_content for a page: {e}")
        db_manager.disconnect()
        return
    except Exception as e:
        print(f"ERROR: Failed to retrieve Confluence data types from DB: {e}")
        db_manager.disconnect()
        return

    if not unique_confluence_data_types:
        print("No Confluence data types found in parsed content (table_1) to map.")
        db_manager.disconnect()
        return

    # --- Generate Report Content ---
    report_lines = []
    report_lines.append(f"# Confluence Data Type Mapping Report")
    report_lines.append(f"Generated On: {datetime.now().isoformat()}\n")

    report_lines.append("## 1. Confluence Data Type Resolution")
    report_lines.append("| Confluence Type | Resolved Snowflake Type | Notes |")
    report_lines.append("|-----------------|-------------------------|-------|")

    unresolved_types = []
    for conf_type in sorted(list(unique_confluence_data_types)):
        resolved_sf_type = resolve_snowflake_data_type(conf_type, data_type_map)
        
        if resolved_sf_type == "VARCHAR(16777216)" and conf_type.upper() != "VARCHAR" and conf_type.upper() != "STRING":
            notes = "Defaulted to VARCHAR due to unknown or empty Confluence type."
            unresolved_types.append(f"'{conf_type}'")
        else:
            notes = "Mapped via data_type_map.json"
        
        report_lines.append(f"| {conf_type} | {resolved_sf_type} | {notes} |")
    
    if unresolved_types:
        report_lines.append(f"\n**ACTION REQUIRED:** The following Confluence data types were not explicitly mapped and defaulted to VARCHAR: {', '.join(unresolved_types)}.")
        report_lines.append(f"Please review and update '{FilePaths.DATA_TYPE_MAP_FILE}'.")
    else:
        report_lines.append("\nAll Confluence data types found were either explicitly mapped or known to default to VARCHAR.")

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
    # For now, no config_file needed, as data_type_map is loaded directly.
    # Future versions could allow overriding data_type_map file.
    args = parser.parse_args() # Parse no args for now.
    
    generate_data_type_report()
