# ml_ddl_change_reporter.py
import os
import json
import argparse # NEW: For command-line arguments
from datetime import datetime
import difflib # NEW: For generating DDL diffs

from config import CHECK_ENVIRONMENTS, DEPLOYMENT_ENVIRONMENT, FilePaths, load_fqdn_resolver
from database_manager import DatabaseManager


def generate_ml_ddl_change_report(source_env="DEV", target_env="DEV"):
    """
    Generates a report comparing ML DDLs across environments and
    flags internal DDL changes.
    
    Args:
        source_env (str): The environment to use as the primary comparison source.
        target_env (str): The environment to compare against the source.
    """
    print("\n--- Starting ML DDL Change Report Generation ---")

    # Validate provided environments
    if source_env.upper() not in CHECK_ENVIRONMENTS:
        print(f"ERROR: Source environment '{source_env}' is not defined in CHECK_ENVIRONMENTS in config.py.")
        return
    if target_env.upper() not in CHECK_ENVIRONMENTS:
        print(f"ERROR: Target environment '{target_env}' is not defined in CHECK_ENVIRONMENTS in config.py.")
        return

    db_manager = DatabaseManager()
    
    # --- 1. Retrieve all ML DDL metadata ---
    cursor = db_manager.conn.cursor()
    cursor.execute(f"SELECT * FROM {FilePaths.SNOWFLAKE_ML_SOURCE_TABLE} WHERE exists_in_snowflake = 1") # Only consider existing objects
    all_ml_ddl_records = cursor.fetchall()
    db_manager.disconnect()

    if not all_ml_ddl_records:
        print("No existing ML source DDL records found in the database. Run ml_table_checker.py first.")
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
    report_lines.append(f"# Snowflake ML DDL Change Report (Comparison: {source_env.upper()} vs {target_env.upper()})")
    report_lines.append(f"Generated On: {datetime.now().isoformat()}")
    report_lines.append(f"Deployment Environment: {DEPLOYMENT_ENVIRONMENT}\n")

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
                    # Generate a diff if both old and new DDLs are available
                    if record['previous_extracted_ddl'] and record['current_extracted_ddl']:
                        diff = difflib.unified_diff(
                            record['previous_extracted_ddl'].splitlines(keepends=True),
                            record['current_extracted_ddl'].splitlines(keepends=True),
                            fromfile=f"Previous_{fqdn}_{env}_{obj_type}.sql",
                            tofile=f"Current_{fqdn}_{env}_{obj_type}.sql",
                            lineterm='' # Prevent adding extra newlines if DDL already has them
                        )
                        report_lines.extend(list(diff))
                    else:
                        report_lines.append("  (Previous DDL content not available for diff)")
                    report_lines.append("```")
    if not internal_changes_found:
        report_lines.append("\n*No internal DDL changes detected in any environment.*")

    # Section 2: Cross-Environment Comparison (Source vs Target)
    report_lines.append(f"\n## 2. Cross-Environment Comparison: {source_env.upper()} vs {target_env.upper()}")
    
    comparison_found = False
    for fqdn, env_data in sorted(ddl_data_by_fqdn.items()):
        source_obj = env_data.get(source_env.upper(), {}).get('TABLE') # Assuming TABLE for primary check for now
        target_obj = env_data.get(target_env.upper(), {}).get('TABLE') # Assuming TABLE for primary check for now

        # Add more logic here to iterate over other object_types (VIEW, etc.) if needed
        # For simplicity, focus on TABLE for now, or find all object types for an FQDN

        all_object_types_for_fqdn = sorted(list(set(ot for env_rec in env_data.values() for ot in env_rec.keys())))
        
        for obj_type in all_object_types_for_fqdn:
            source_obj_detail = env_data.get(source_env.upper(), {}).get(obj_type)
            target_obj_detail = env_data.get(target_env.upper(), {}).get(obj_type)

            if source_obj_detail and target_obj_detail:
                # Both exist, compare DDL hash and object type
                comparison_found = True
                report_lines.append(f"\n### {obj_type} {fqdn} Parity")
                report_lines.append(f"  Source Env ({source_env.upper()}): Exists, Hash: {source_obj_detail['current_ddl_hash']}")
                report_lines.append(f"  Target Env ({target_env.upper()}): Exists, Hash: {target_obj_detail['current_ddl_hash']}")

                if source_obj_detail['current_ddl_hash'] != target_obj_detail['current_ddl_hash']:
                    report_lines.append("  **DDL HASH MISMATCH!**")
                    report_lines.append("\n```diff")
                    # Generate a diff
                    diff = difflib.unified_diff(
                        source_obj_detail['current_extracted_ddl'].splitlines(keepends=True),
                        target_obj_detail['current_extracted_ddl'].splitlines(keepends=True),
                        fromfile=f"{source_env.upper()}_{fqdn}_{obj_type}.sql",
                        tofile=f"{target_env.upper()}_{fqdn}_{obj_type}.sql",
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
                report_lines.append(f"\n### {obj_type} {fqdn} in {source_env.upper()} (Missing in {target_env.upper()})")
                report_lines.append(f"  Exists in {source_env.upper()}, but NOT in {target_env.upper()}.")
            elif not source_obj_detail and target_obj_detail:
                comparison_found = True
                report_lines.append(f"\n### {obj_type} {fqdn} in {target_env.upper()} (Missing in {source_env.upper()})")
                report_lines.append(f"  Exists in {target_env.upper()}, but NOT in {source_env.upper()}.")
    
    if not comparison_found:
        report_lines.append(f"\n*No common DDL objects found for comparison between {source_env.upper()} and {target_env.upper()}.*")
    
    # --- 3. Save Report to File ---
    report_filename = f"ml_ddl_change_report_{source_env.upper()}_vs_{target_env.upper()}.md"
    report_filepath = os.path.join(FilePaths.REPORT_OUTPUT_DIR, report_filename)
    os.makedirs(FilePaths.REPORT_OUTPUT_DIR, exist_ok=True) # Ensure dir exists

    with open(report_filepath, 'w', encoding='utf-8') as f:
        f.write("\n".join(report_lines))
    
    print(f"\n--- ML DDL Change Report saved to: {report_filepath} ---")
    print("ACTION REQUIRED: Review the generated report for DDL changes and parity issues.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generates a report comparing Snowflake ML DDLs across environments."
    )
    parser.add_argument(
        "--source_env",
        type=str,
        default="DEV", # Default to DEV
        help="The source environment for comparison (e.g., DEV, SPC). Default: DEV"
    )
    parser.add_argument(
        "--target_env",
        type=str,
        default="DEV", # Default to DEV
        help="The target environment for comparison (e.g., DEV, PROD). Default: DEV"
    )
    args = parser.parse_args()

    generate_ml_ddl_change_report(source_env=args.source_env.upper(), target_env=args.target_env.upper())
