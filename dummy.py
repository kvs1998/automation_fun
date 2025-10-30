import re
import os

def process_sql_script_with_prefix(sql_content, num_files_to_create=2):
    """
    Processes SQL content to create YAML files for view definitions,
    adding a specific prefix and modifying author/id.

    Args:
        sql_content (str): The entire SQL script content as a string.
        num_files_to_create (int): The maximum number of YAML files to create.
    """
    lines = sql_content.strip().split('\n')
    output_dir = "view_yamls_modified"
    os.makedirs(output_dir, exist_ok=True)

    files_created = 0

    for line in lines:
        if files_created >= num_files_to_create:
            break # Stop after creating the desired number of files

        line = line.strip()
        if line.startswith("create or replace secure view"):
            # Regex to capture view name and source table name
            match = re.match(
                r"create or replace secure view "
                r"ALADDINDB\.INVESTMENTS\.SV_REF_(\w+)\s+AS\s+SELECT\s+\*\s+FROM\s+"
                r"EDP_ADC_PUB_DB\.ADC_PUB_LL\.SV_REF_(\w+);",
                line
            )

            if match:
                view_name_suffix = match.group(1)
                source_table_suffix = match.group(2)

                full_view_name = (
                    f"ALADDINDB.INVESTMENTS.SV_REF_{view_name_suffix}"
                )
                full_source_table_name = (
                    f"EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_{source_table_suffix}"
                )

                yaml_filename = f"{view_name_suffix}.yaml"
                yaml_filepath = os.path.join(output_dir, yaml_filename)

                # --- NEW: Define the prefix YAML content ---
                prefix_yaml_content = f"""
databaseChangeLog:
  - changeSet:
      id: SV_{view_name_suffix}-view-1 # Modified ID
      author: kvishwaj # Modified Author
      runOnChange: true
      changes:
        - createView:
            fullDefinition: true
            remarks: ''
            viewName: {view_name_suffix}
            schemaName: INVESTMENTS
            selectQuery: >
"""
                # --- END NEW ---

                # Indent the selectQuery SQL to match the YAML structure
                # The 'line' variable already contains the SQL string
                indented_sql = f"              {line}\n"


                yaml_content = prefix_yaml_content + indented_sql

                try:
                    with open(yaml_filepath, 'w') as f:
                        f.write(yaml_content.strip()) # .strip() to remove leading/trailing newlines from prefix
                    print(f"Created {yaml_filepath}")
                    files_created += 1 # Increment counter only if file is created
                except IOError as e:
                    print(f"Error writing file {yaml_filepath}: {e}")
            else:
                print(f"Warning: Could not parse line (regex mismatch): {line}")

# Example Usage:
# Replace this multiline string with the actual content of your SQL file
sql_script_content = """
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_SECURITY_RELATIONSHIP_HIST AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_SECURITY_RELATIONSHIP_HIST;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_SECURITY_RELATIONSHIP AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_SECURITY_RELATIONSHIP;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_SECURITY_RATE_CALCULATION_DETAIL_HIST AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_SECURITY_RATE_CALCULATION_DETAIL_HIST;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_SECURITY_RATE_CALCULATION_DETAIL AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_SECURITY_RATE_CALCULATION_DETAIL;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_SECURITY_AMOUNT_OUTSTANDING_HIST AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_SECURITY_AMOUNT_OUTSTANDING_HIST;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_SECURITY_AMOUNT_OUTSTANDING AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_SECURITY_AMOUNT_OUTSTANDING;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_PORTFOLIOS_HIST AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_PORTFOLIOS_HIST;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_PORTFOLIOS AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_PORTFOLIOS;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_PORTFOLIO_VALUATION_HIST AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_PORTFOLIO_VALUATION_HIST;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_PORTFOLIO_VALUATION AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_PORTFOLIO_VALUATION;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_PORTFOLIO_OPS_HIST AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_PORTFOLIO_OPS_HIST;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_PORTFOLIO_OPS AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_PORTFOLIO_OPS;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_PORTFOLIO_ASSIGNMENTS_HIST AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_PORTFOLIO_ASSIGNMENTS_HIST;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_PORTFOLIO_ASSIGNMENTS AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_PORTFOLIO_ASSIGNMENTS;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_PORTFOLIO_ASSIGNMENT_TEAM_HIST AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_PORTFOLIO_ASSIGNMENT_TEAM_HIST;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_PORTFOLIO_ASSIGNMENT_TEAM AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_PORTFOLIO_ASSIGNMENT_TEAM;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_PORT_GROUP_EXPAND_HIST AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_PORT_GROUP_EXPAND_HIST;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_PORT_GROUP_EXPAND AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_PORT_GROUP_EXPAND;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_LOOKUP_DECODE_HIST AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_LOOKUP_DECODE_HIST;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_LOOKUP_DECODE AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_LOOKUP_DECODE;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_ISSUER_SECTOR_HIST AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_ISSUER_SECTOR_HIST;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_ISSUER_SECTOR AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_ISSUER_SECTOR;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_ISSUER_PROGRAM_STEM_HIST AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_ISSUER_PROGRAM_STEM_HIST;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_ISSUER_PROGRAM_STEM AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_ISSUER_PROGRAM_STEM;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_ISSUER_NOTE_HIST AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_ISSUER_NOTE_HIST;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_ISSUER_NOTE AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_ISSUER_NOTE;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_ISSUER_IDENTIFIER_HIST AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_ISSUER_IDENTIFIER_HIST;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_ISSUER_IDENTIFIER AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_ISSUER_IDENTIFIER;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_ISSUER_HIST AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_ISSUER_HIST;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_ISSUER_HIERARCHY_HIST AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_ISSUER_HIERARCHY_HIST;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_ISSUER_HIERARCHY AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_ISSUER_HIERARCHY;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_ISSUER_FEATURE_HIST AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_ISSUER_FEATURE_HIST;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_ISSUER_FEATURE AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_ISSUER_FEATURE;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_ISSUER_CREDIT_EVENT_HIST AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_ISSUER_CREDIT_EVENT_HIST;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_ISSUER_CREDIT_EVENT AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_ISSUER_CREDIT_EVENT;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_ISSUER_CREDIT_ENHANCEMENT_HIST AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_ISSUER_CREDIT_ENHANCEMENT_HIST;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_ISSUER_CREDIT_ENHANCEMENT AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_ISSUER_CREDIT_ENHANCEMENT;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_ISSUER_COUNTRY_HIST AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_ISSUER_COUNTRY_HIST;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_ISSUER_COUNTRY AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_ISSUER_COUNTRY;
create or replace secure view ALADDINDB.INVESTMENTS.SV_REF_ALADDIN_ISSUER_CLIENT_DEFINED_FIELD_HIST AS SELECT * FROM EDP_ADC_PUB_DB.ADC_PUB_LL.SV_REF_ALADDIN_ISSUER_CLIENT_DEFINED_FIELD_HIST;
"""

# Call the function with num_files_to_create set to 2
process_sql_script_with_prefix(sql_script_content, num_files_to_create=2)
