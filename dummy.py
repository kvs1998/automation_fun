The extraction_status column (and potentially user_verified) provides granular control over your data pipeline:


- PENDING_METADATA_INGESTION: Waiting for metadata to be fetched.

- METADATA_INGESTED: Metadata is in the DB, ready for content parsing.

- PENDING_PARSE_APPROVAL: (Optional) User intervention needed before content parsing.

- PARSED_OK: Content parsed and saved.

- PARSE_FAILED: Something went wrong during content parsing.

- DB_FAILED: Database operation failed.


table_name_from_parsed_content = cleaned_structured_data.get("metadata", {}).get("table_name", api_title)
        sanitized_table_name_for_ref = re.sub(r'[^a-z0-9_.-]', '', table_name_from_parsed_content.lower().replace(" ", "_"))

        page_entry["structured_data_file"] = f"{sanitized_table_name_for_ref}.json" # Store sanitized name as a reference
        page_entry["extraction_status"] = "PARSED_OK"
        page_entry["last_parsed_content_hash"] = current_metadata_hash
        db_manager.insert_or_update_parsed_content(page_id, parsed_json_str) # Store parsed JSON in DB
        print(f"  Structured content for '{api_title}' (ID: {page_id}) parsed and stored in DB.")


def clean_special_characters_iterative(data):
    """
    Iteratively cleans special (non-ASCII printable) characters from strings
    in a nested structure. Preserves standard ASCII.
    """
    if isinstance(data, (str, int, float, bool, type(None))):
        return data # Base case: return non-string/non-collection data directly
    
    queue = deque([data])

    while queue:
        current = queue.popleft()

        if isinstance(current, dict):
            for key, value in list(current.items()): # Iterate on a copy for safe modification
                if isinstance(value, (dict, list)):
                    queue.append(value)
                elif isinstance(value, str):
                    # NEW & CORRECTED:
                    # Replace characters that are NOT in the printable ASCII range (32-126)
                    # or common whitespace (tab, newline, carriage return) with a space.
                    # This keeps standard letters, numbers, punctuation, etc.
                    cleaned_value = re.sub(r'[^\x20-\x7E\t\n\r]+', ' ', value)
                    # Normalize whitespace (multiple spaces to single, strip)
                    current[key] = re.sub(r'\s+', ' ', cleaned_value).strip()
        
        elif isinstance(current, list):
            for i in range(len(current)):
                item = current[i]
                if isinstance(item, (dict, list)):
                    queue.append(item)
                elif isinstance(item, str):
                    # NEW & CORRECTED: Same cleaning logic for list items
                    cleaned_item = re.sub(r'[^\x20-\x7E\t\n\r]+', ' ', item)
                    current[i] = re.sub(r'\s+', ' ', cleaned_item).strip()
    return data

# ... (rest of confluence_utils.py, config.py, report_generator.py remain as last full code)



# .env
DEPLOYMENT_ENVIRONMENT=DEV # This script is running in DEV

# Confluence config (unchanged)
CONFLUENCE_BASE_URL=https://your-company.atlassian.net/wiki
CONFLUENCE_API_TOKEN=your_personal_access_token_from_confluence
CONFLUENCE_SPACE_KEY=YOUR_CONFLUENCE_SPACE_KEY

# Snowflake DEV environment credentials
SNOWFLAKE_DEV_USER=your_dev_snowflake_user
SNOWFLAKE_DEV_PASSWORD=your_dev_snowflake_password
SNOWFLAKE_DEV_ACCOUNT=your_dev_snowflake_account.region
SNOWFLAKE_DEV_WAREHOUSE=your_dev_snowflake_warehouse
SNOWFLAKE_DEV_DATABASE=your_dev_snowflake_database
SNOWFLAKE_DEV_SCHEMA=your_dev_snowflake_schema
SNOWFLAKE_DEV_ROLE=your_dev_snowflake_role

# Snowflake SPC environment credentials (example)
SNOWFLAKE_SPC_USER=your_spc_snowflake_user
SNOWFLAKE_SPC_PASSWORD=your_spc_snowflake_password
SNOWFLAKE_SPC_ACCOUNT=your_spc_snowflake_account.region
SNOWFLAKE_SPC_WAREHOUSE=your_spc_snowflake_warehouse
SNOWFLAKE_SPC_DATABASE=your_spc_snowflake_database
SNOWFLAKE_SPC_SCHEMA=your_spc_snowflake_schema
SNOWFLAKE_SPC_ROLE=your_spc_snowflake_role

# ... and so on for BFM, PRU, ELD, QA, UAT, DR ...
# Ensure all environment names listed in config.py CHECK_ENVIRONMENTS have corresponding credentials here.
