# ml_ddl_change_reporter.py (MODIFIED for JSON arguments)

import os
import json
import argparse
from datetime import datetime
import difflib

from config import CHECK_ENVIRONMENTS, DEPLOYMENT_ENVIRONMENT, FilePaths
from database_manager import DatabaseManager


def generate_ml_ddl_change_report(report_args): # NEW: Now takes a dict of args
    """
    Generates a report comparing ML DDLs across environments and
    flags internal DDL changes, based on a dictionary of report arguments.
    
    Args:
        report_args (dict): Dictionary containing:
                            'source_env' (str), 'target_env' (str),
                            'objects' (list or None), 'output_filename' (str or None).
    """
    source_env = report_args.get("source_env", "DEV").upper()
    target_env = report_args.get("target_env", "DEV").upper()
    objects_to_compare = [obj.upper() for obj in report_args["objects"]] if report_args.get("objects") else None
    custom_output_filename = report_args.get("output_filename")

    print("\n--- Starting ML DDL Change Report Generation ---")
    print(f"Comparison Parameters: Source Env='{source_env}', Target Env='{target_env}'")
    if objects_to_compare:
        print(f"  Filtering for specific FQDNs: {', '.join(objects_to_compare)}\n")
    else:
        print("  Including all available FQDNs.\n")

    # Validate provided environments
    if source_env not in CHECK_ENVIRONMENTS:
        print(f"ERROR: Source environment '{source_env}' is not defined in CHECK_ENVIRONMENTS in config.py.")
        return
    if target_env not in CHECK_ENVIRONMENTS:
        print(f"ERROR: Target environment '{target_env}' is not defined in CHECK_ENVIRONMENTS in config.py.")
        return
    
    db_manager = DatabaseManager()
    
    # --- 1. Retrieve all ML DDL metadata ---
    cursor = db_manager.conn.cursor()
    query_params = []
    where_clause_parts = ["exists_in_snowflake = 1"] # Always filter by existing objects
    
    if objects_to_compare:
        placeholders = ','.join(['?' for _ in objects_to_compare])
        where_clause_parts.append(f"fqdn IN ({placeholders})")
        query_params.extend(objects_to_compare) # Objects are already uppercased

    final_where_clause = " WHERE " + " AND ".join(where_clause_parts) if where_clause_parts else ""
    cursor.execute(f"SELECT * FROM {FilePaths.SNOWFLAKE_ML_SOURCE_TABLE}{final_where_clause}", tuple(query_params))
    
    all_ml_ddl_records = cursor.fetchall()
    db_manager.disconnect()

    if not all_ml_ddl_records:
        print("No existing ML source DDL records found in the database matching criteria. Run ml_table_checker.py first, or check your --config_file filter.")
        return

    # Organize data by FQDN -> Environment -> Object Type for easy lookup
    ddl_data_by_fqdn = {} # {FQDN: {ENV: {OBJ_TYPE: {...ddl_record_dict...}}}}
    for record_row in all_ml_ddl_records:
        record = dict(record_row)
        fqdn = record['fqdn']
        env = record['environment'].upper()
        obj_type = record['object_type'].upper()

        if fqdn not in ddl_data_by_fqdn:
            ddl_data_by_fqdn[fqdn] = {}
        if env not in ddl_data_by_fqdn[fqdn]:
            ddl_data_by_fqdn[fqdn][env] = {}
        
        ddl_data_by_fqdn[fqdn][env][obj_type] = record
    
    # --- 2. Generate Report Content ---
    report_lines = []
    report_lines.append(f"# Snowflake ML DDL Change Report (Comparison: {source_env} vs {target_env})")
    report_lines.append(f"Generated On: {datetime.now().isoformat()}")
    report_lines.append(f"Deployment Environment: {DEPLOYMENT_ENVIRONMENT}\n")
    if objects_to_compare:
        report_lines.append(f"Objects Filtered: {', '.join(objects_to_compare)}\n")


    # Section 1: Internal DDL Changes (within each environment)
    report_lines.append("## 1. Internal DDL Changes Detected (Current vs Previous)")
    internal_changes_found = False
    for fqdn, env_data in sorted(ddl_data_by_fqdn.items()):
        for env, obj_type_data in sorted(env_data.items()):
            for obj_type, record in sorted(obj_type_data.items()):
                if record['current_ddl_hash'] and record['previous_ddl_hash'] and \
                   record['current_ddl_hash'] != record['previous_ddl_hash']:
                    internal_changes_found = True
                    report_lines.append(f"\n### DDL Changed: {fqdn} in {env} ({obj_type})")
                    report_lines.append(f"  Detected On: {record['ddl_changed_on']}")
                    report_lines.append(f"  Old Hash: {record['previous_ddl_hash']}")
                    report_lines.append(f"  New Hash: {record['current_ddl_hash']}")
                    report_lines.append("\n```diff")
                    if record['previous_extracted_ddl'] and record['current_extracted_ddl']:
                        diff = difflib.unified_diff(
                            record['previous_extracted_ddl'].splitlines(keepends=True),
                            record['current_extracted_ddl'].splitlines(keepends=True),
                            fromfile=f"Previous_{fqdn}_{env}_{obj_type}.sql",
                            tofile=f"Current_{fqdn}_{env}_{obj_type}.sql",
                            lineterm=''
                        )
                        report_lines.extend(list(diff))
                    else:
                        report_lines.append("  (Previous DDL content not available for diff)")
                    report_lines.append("```")
    if not internal_changes_found:
        report_lines.append("\n*No internal DDL changes detected in any environment.*")

    # Section 2: Cross-Environment Comparison (Source vs Target)
    report_lines.append(f"\n## 2. Cross-Environment Comparison: {source_env} vs {target_env} (by FQDN)")
    
    comparison_found = False
    
    for fqdn, env_objects_data in sorted(ddl_data_by_fqdn.items()):
        
        all_object_types_for_fqdn = sorted(list(set(ot for env_rec in env_objects_data.values() for ot in env_rec.keys())))

        for obj_type in all_object_types_for_fqdn:
            source_obj_detail = env_objects_data.get(source_env, {}).get(obj_type) # Use source_env directly
            target_obj_detail = env_objects_data.get(target_env, {}).get(obj_type) # Use target_env directly

            source_fqdn_for_report = source_obj_detail['fqdn'] if source_obj_detail else f"N/A ({source_env})"
            target_fqdn_for_report = target_obj_detail['fqdn'] if target_obj_detail else f"N/A ({target_env})"


            if source_obj_detail and target_obj_detail:
                comparison_found = True
                report_lines.append(f"\n### {obj_type} {fqdn} Parity ({source_env} vs {target_env})")
                report_lines.append(f"  Source Env ({source_env}): Exists, Hash: {source_obj_detail['current_ddl_hash']}")
                report_lines.append(f"  Target Env ({target_env}): Exists, Hash: {target_obj_detail['current_ddl_hash']}")

                if source_obj_detail['current_ddl_hash'] != target_obj_detail['current_ddl_hash']:
                    report_lines.append("  **DDL HASH MISMATCH!**")
                    report_lines.append("\n```diff")
                    diff = difflib.unified_diff(
                        source_obj_detail['current_extracted_ddl'].splitlines(keepends=True),
                        target_obj_detail['current_extracted_ddl'].splitlines(keepends=True),
                        fromfile=f"{source_env}_{source_fqdn_for_report}_{obj_type}.sql",
                        tofile=f"{target_env}_{target_fqdn_for_report}_{obj_type}.sql",
                        lineterm=''
                    )
                    report_lines.extend(list(diff))
                    report_lines.append("```")
                else:
                    report_lines.append("  DDL Hashes MATCH.")
                
                if source_obj_detail['object_type'] != target_obj_detail['object_type']:
                     report_lines.append(f"  **OBJECT TYPE MISMATCH!** Source: {source_obj_detail['object_type']}, Target: {target_obj_detail['object_type']}")

            elif source_obj_detail and not target_obj_detail:
                comparison_found = True
                report_lines.append(f"\n### {obj_type} {fqdn} in {source_env} (Missing in {target_env})")
                report_lines.append(f"  Exists in {source_env} ({source_fqdn_for_report}), but NOT in {target_env}.")
            elif not source_obj_detail and target_obj_detail:
                comparison_found = True
                report_lines.append(f"\n### {obj_type} {fqdn} in {target_env} (Missing in {source_env})")
                report_lines.append(f"  Exists in {target_env} ({target_fqdn_for_report}), but NOT in {source_env}.")
    
    if not comparison_found:
        report_lines.append(f"\n*No DDL objects found for comparison between {source_env} and {target_env} by FQDN.*")
    
    # --- 3. Save Report to File ---
    if custom_output_filename:
        report_filename = custom_output_filename
    else:
        report_filename = f"ml_ddl_change_report_{source_env}_vs_{target_env}"
        if objects_to_compare:
            report_filename += "_filtered"
        report_filename += ".md"

    report_filepath = os.path.join(FilePaths.REPORT_OUTPUT_DIR, report_filename)
    os.makedirs(FilePaths.REPORT_OUTPUT_DIR, exist_ok=True)

    with open(report_filepath, 'w', encoding='utf-8') as f:
        f.write("\n".join(report_lines))
    
    print(f"\n--- ML DDL Change Report saved to: {report_filepath} ---")
    print("ACTION REQUIRED: Review the generated report for DDL changes and parity issues.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generates a report comparing Snowflake ML DDLs across environments. "
                    "Arguments can be provided via a JSON config file or directly."
    )
    parser.add_argument(
        "--config_file",
        type=str,
        default=FilePaths.DEFAULT_REPORT_ARGS_FILE, # Default to loading from this file
        help=f"Path to a JSON file containing report arguments (e.g., source_env, target_env, objects). "
             f"Default: '{FilePaths.DEFAULT_REPORT_ARGS_FILE}'."
    )
    # The individual arguments are removed from argparse here, as they'll come from the JSON.
    # We still need to parse the --config_file argument itself.
    
    args = parser.parse_args()
    
    report_args = {}
    try:
        if not os.path.exists(args.config_file):
            print(f"WARNING: Config file '{args.config_file}' not found. Using empty arguments.")
            # If the default isn't found, fall back to hardcoded defaults
            report_args = {
                "source_env": "DEV",
                "target_env": "DEV",
                "objects": None,
                "output_filename": None
            }
        else:
            with open(args.config_file, 'r', encoding='utf-8') as f:
                report_args = json.load(f)
            print(f"Loaded report arguments from '{args.config_file}'.")
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in config file '{args.config_file}': {e}. Using empty arguments.")
        report_args = {}
    except Exception as e:
        print(f"ERROR: Could not load config file '{args.config_file}': {e}. Using empty arguments.")
        report_args = {}

    # Ensure environment names are consistent casing
    report_args["source_env"] = report_args.get("source_env", "DEV").upper()
    report_args["target_env"] = report_args.get("target_env", "DEV").upper()
    
    # Ensure objects are uppercased lists if present
    if report_args.get("objects") is not None:
        if isinstance(report_args["objects"], list):
            report_args["objects"] = [obj.upper() for obj in report_args["objects"]]
        else:
            print(f"WARNING: 'objects' in config file is not a list. Ignoring. Value: {report_args['objects']}")
            report_args["objects"] = None

    generate_ml_ddl_change_report(report_args)
