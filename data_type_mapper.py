# data_type_mapper.py (MODIFIED generate_data_type_report for page titles in WARNING)

import os
import json
from datetime import datetime
import argparse
import re
from tabulate import tabulate

from config import FilePaths, load_data_type_map
from database_manager import DatabaseManager
from confluence_utils import clean_special_characters_iterative


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
        return "VARCHAR(16777216)"

    match = re.match(r'([A-Z_]+)\s*(\(.*\))?', confluence_data_type.upper().strip())
    
    base_type_confluence = None
    params_confluence = ""

    if match:
        base_type_confluence = match.group(1)
        if match.group(2):
            params_confluence = match.group(2)
    else:
        base_type_confluence = confluence_data_type.upper().strip()

    if not base_type_confluence:
        return "VARCHAR(16777216)"

    snowflake_base_type = data_type_map.get(base_type_confluence)

    if snowflake_base_type:
        if snowflake_base_type.upper() == 'NUMBER' and 'NUMBER' in base_type_confluence:
            if not params_confluence:
                return "NUMBER(38,0)"
        elif snowflake_base_type.upper() == 'INTEGER' and 'INTEGER' in base_type_confluence:
            if not params_confluence:
                return "NUMBER(38,0)"
        
        if params_confluence and (snowflake_base_type.upper() in ["VARCHAR", "NUMBER", "DECIMAL", "CHAR", "STRING"]):
            return f"{snowflake_base_type}{params_confluence}"
        else:
            return snowflake_base_type

    else:
        # Default to VARCHAR for unmapped base types
        return "VARCHAR(16777216)"


def generate_data_type_report(config_file=None):
    """
    Generates a report on Confluence data types and their resolved Snowflake equivalents.
    Identifies unmapped Confluence data types and lists the pages where they were found
    in the ACTION REQUIRED section.
    """
    print("\n--- Starting Data Type Mapping and Report Generation ---")

    db_manager = DatabaseManager()
    
    try:
        data_type_map = load_data_type_map()
    except Exception as e:
        print(f"ERROR: Failed to load data type map: {e}")
        db_manager.disconnect()
        return

    # NEW: Dictionary to store unique Confluence data types and the *unique page titles* they originated from
    # { "CONFLUENCE_TYPE_STRING": ["Page Title A", "Page Title B"], ... }
    confluence_data_types_with_sources = {} 

    try:
        cursor = db_manager.conn.cursor()
        # Fetch page metadata to get titles for the report
        page_metadata_map = {p['page_id']: p['api_title'] for p in cursor.execute("SELECT page_id, api_title FROM confluence_page_metadata").fetchall()}

        cursor.execute("SELECT page_id, parsed_json FROM confluence_parsed_content")
        
        for row in cursor.fetchall():
            page_id = row['page_id']
            page_title = page_metadata_map.get(page_id, f"Page ID:{page_id}") # Get title, fallback if not found
            parsed_content_json_str = row['parsed_json']
            if parsed_content_json_str:
                parsed_content = json.loads(parsed_content_json_str)
                cleaned_parsed_content = parsed_content # Assume already clean from data_parser
                
                for table_data in cleaned_parsed_content.get('tables', []):
                    if table_data.get('id') == 'table_1': # Only process 'table_1'
                        for column in table_data.get('columns', []):
                            conf_data_type = column.get('data_type')
                            if conf_data_type and conf_data_type.strip():
                                conf_type_key = conf_data_type.strip()
                                if conf_type_key not in confluence_data_types_with_sources:
                                    confluence_data_types_with_sources[conf_type_key] = set() # Use a set to store unique titles
                                confluence_data_types_with_sources[conf_type_key].add(page_title) # Add page title (unique by set)

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
    unresolved_types_for_action_detail = {} # NEW: { "TYPE_STR": "Page1, Page2", ... }

    for conf_type in sorted(confluence_data_types_with_sources.keys()):
        resolved_sf_type = resolve_snowflake_data_type(conf_type, data_type_map)
        
        notes = ""
        if resolved_sf_type == "VARCHAR(16777216)":
            notes = "Defaulted to VARCHAR due to unknown or empty Confluence type."
            # NEW: Collect page titles for the warning section
            unresolved_types_for_action_detail[conf_type] = ", ".join(sorted(list(confluence_data_types_with_sources[conf_type])))
        else:
            notes = "Mapped via data_type_map.json"
        
        # OLD: Removed Source Pages column from here.
        report_data_rows.append([
            conf_type,
            resolved_sf_type,
            notes
        ])
    
    # NEW: Headers for the simplified table
    headers = ["Confluence Type", "Resolved Snowflake Type", "Notes"]
    report_lines.append("## 1. Confluence Data Type Resolution\n")
    report_lines.append(tabulate(report_data_rows, headers=headers, tablefmt="pipe"))
    report_lines.append("\n") # Add a newline after the table

    if unresolved_types_for_action_detail:
        report_lines.append(f"**ACTION REQUIRED:** The following Confluence data types were not explicitly mapped and defaulted to VARCHAR.")
        report_lines.append(f"Please review and update '{FilePaths.DATA_TYPE_MAP_FILE}'.")
        for conf_type, pages_str in unresolved_types_for_action_detail.items():
            report_lines.append(f"  - Type: '{conf_type}' (Found in pages: {pages_str})")
    else:
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
