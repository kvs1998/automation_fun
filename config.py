# config.py (MODIFIED)

import os
from dotenv import load_dotenv
import json

load_dotenv()


DEPLOYMENT_ENVIRONMENT = os.getenv("DEPLOYMENT_ENVIRONMENT", "DEV").upper()

# Ensure these are your specific environment names (SPC, BFM, PRU, ELD)
# Make sure the list is complete for all environments you might compare against
CHECK_ENVIRONMENTS = [
    "DEV", "QA", "PREPOD", "PROD", "UAT", "DR", "SPC", "BFM", "PRU", "ELD" 
]

class ConfluenceConfig:
    BASE_URL = os.getenv("CONFLUENCE_BASE_URL")
    API_TOKEN = os.getenv("CONFLUENCE_API_TOKEN")
    SPACE_KEY = os.getenv("CONFLUENCE_SPACE_KEY")


def load_snowflake_env_credentials(env_name):
    env_prefix = f"SNOWFLAKE_{env_name.upper()}_"
    
    user = os.getenv(f"{env_prefix}USER")
    password = os.getenv(f"{env_prefix}PASSWORD")
    account = os.getenv(f"{env_prefix}ACCOUNT")
    warehouse = os.getenv(f"{env_prefix}WAREHOUSE")
    database = os.getenv(f"{env_prefix}DATABASE")
    schema = os.getenv(f"{env_prefix}SCHEMA")
    role = os.getenv(f"{env_prefix}ROLE")

    if not all([user, password, account, warehouse, database, schema, role]):
        missing_vars = [f"{env_prefix}{v}" for v in ["USER", "PASSWORD", "ACCOUNT", "WAREHOUSE", "DATABASE", "SCHEMA", "ROLE"] if not os.getenv(f"{env_prefix}{v}")]
        raise ValueError(
            f"Missing Snowflake credentials for environment '{env_name}'. "
            f"Ensure all of {', '.join(missing_vars)} are set in .env"
        )
    
    return {
        "user": user,
        "password": password,
        "account": account,
        "warehouse": warehouse,
        "database": database,
        "schema": schema,
        "role": role
    }


class FilePaths:
    TITLES_JSON_FILE = "titles.json"
    REPORT_JSON_FILE = "confluence_ingest_report.json"
    TABLES_DIR = "tables"
    DB_FILE = "confluence_metadata.db"
    SOURCE_FQDN_RESOLVER_FILE = "source_to_fqdn_resolver.json"
    SNOWFLAKE_ML_SOURCE_TABLE = "snowflake_ml_source_metadata"


def get_confluence_page_titles(json_file_path=FilePaths.TITLES_JSON_FILE):
    """
    Reads a list of Confluence page titles from a JSON file.
    """
    if not os.path.exists(json_file_path):
        raise FileNotFoundError(f"Titles JSON file not found at: {json_file_path}")
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            titles = json.load(f)
            if not isinstance(titles, list):
                raise ValueError("Titles JSON file must contain a list of strings.")
            return titles
    except json.JSONDecodeError as e:
        raise ValueError(f"Error decoding titles JSON file: {e}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred reading titles file: {e}")

# MODIFIED: Utility function to load the FQDN map with both case-sensitive and case-insensitive duplicate checks
# MODIFIED: Utility function to load the FQDN map with alias resolution
# MODIFIED: load_fqdn_resolver to parse the new group/fallback structure
def load_fqdn_resolver(json_file_path=None):
    """
    Loads the environment-aware FQDN resolver map from a JSON file.
    The map now supports 'defaults' for environment groups and 'specific_environments' overrides.
    Returns a resolved map:
        {
            source_name_upper: {
                env_name_upper: {
                    "fqdn": FQDN_UPPER,
                    "object_type": OBJECT_TYPE_UPPER
                }
            }
        }
    Raises ValueError on duplicates, missing keys, or malformed FQDNs.
    """
    if json_file_path is None:
        json_file_path = FilePaths.SOURCE_FQDN_RESOLVER_FILE

    if not os.path.exists(json_file_path):
        raise FileNotFoundError(f"Source FQDN resolver file not found at: {json_file_path}. "
                                f"Ensure '{json_file_path}' exists.")
    
    try:
        def _raise_on_duplicate_keys(ordered_pairs):
            d = {}
            for k, v in ordered_pairs:
                if k in d:
                    raise ValueError(f"Duplicate key '{k}' found in '{json_file_path}' (case-sensitive). Please ensure all keys within the JSON file itself are unique.")
                d[k] = v
            return d

        with open(json_file_path, 'r', encoding='utf-8') as f:
            raw_resolver_map = json.load(f, object_pairs_hook=_raise_on_duplicate_keys)
            
            if not isinstance(raw_resolver_map, dict):
                raise ValueError("Source FQDN resolver file must contain a dictionary of canonical entries.")
            
            resolved_fqdn_map = {} 
            
            for canonical_key_raw, details in raw_resolver_map.items():
                if not isinstance(details, dict):
                    raise ValueError(f"Entry for '{canonical_key_raw}' in {json_file_path} is malformed. Expected a dictionary value.")

                canonical_key_upper = canonical_key_raw.upper()

                # Build the environment-specific FQDNs for this canonical key
                current_canonical_env_fqdns = {} # {ENV_UPPER: {"fqdn": FQDN_UPPER, "object_type": OBJECT_TYPE_UPPER}}

                # --- Process defaults first ---
                defaults_detail = details.get('defaults')
                if defaults_detail:
                    if not isinstance(defaults_detail, dict) or 'environments' not in defaults_detail or 'fqdn' not in defaults_detail:
                        raise ValueError(f"Malformed 'defaults' entry for '{canonical_key_raw}'. Expected 'environments' (list) and 'fqdn' (string).")
                    
                    default_envs = defaults_detail['environments']
                    if not isinstance(default_envs, list):
                        raise ValueError(f"'defaults.environments' for '{canonical_key_raw}' must be a list.")

                    default_fqdn_raw = defaults_detail['fqdn']
                    default_fqdn_upper = default_fqdn_raw.upper()
                    default_object_type = defaults_detail.get('object_type', 'TABLE').upper()

                    if len(default_fqdn_upper.split('.')) != 3:
                        raise ValueError(f"Default FQDN '{default_fqdn_raw}' for '{canonical_key_raw}' is not in DATABASE.SCHEMA.TABLE format.")
                    
                    for env_name_raw in default_envs:
                        current_canonical_env_fqdns[env_name_raw.upper()] = {
                            "fqdn": default_fqdn_upper, 
                            "object_type": default_object_type
                        }
                
                # --- Process specific_environments (overrides defaults) ---
                specific_environments_detail = details.get('specific_environments')
                if specific_environments_detail:
                    if not isinstance(specific_environments_detail, dict):
                         raise ValueError(f"Malformed 'specific_environments' for '{canonical_key_raw}'. Expected a dictionary.")

                    for env_raw, env_details in specific_environments_detail.items():
                        if not isinstance(env_details, dict) or 'fqdn' not in env_details:
                             raise ValueError(f"Entry for specific environment '{env_raw}' under '{canonical_key_raw}' is malformed. Expected 'fqdn' key.")
                        
                        env_fqdn_raw = env_details['fqdn']
                        env_fqdn_upper = env_fqdn_raw.upper()
                        env_object_type = env_details.get('object_type', 'TABLE').upper()

                        if len(env_fqdn_upper.split('.')) != 3:
                             raise ValueError(f"FQDN '{env_fqdn_raw}' for specific environment '{env_raw}' under '{canonical_key_raw}' is not in DATABASE.SCHEMA.TABLE format.")
                        
                        current_canonical_env_fqdns[env_raw.upper()] = { # This overwrites defaults
                            "fqdn": env_fqdn_upper, 
                            "object_type": env_object_type
                        }

                # Validation: Ensure at least one environment is mapped for the canonical key
                if not current_canonical_env_fqdns:
                    raise ValueError(f"Canonical key '{canonical_key_raw}' has no FQDN mappings defined across any environments in {json_file_path}. Please define 'defaults' or 'specific_environments'.")


                # Add canonical key itself to the final lookup map
                if canonical_key_upper in resolved_fqdn_map:
                    raise ValueError(f"Alias conflict: Canonical key '{canonical_key_raw}' (resolves to '{canonical_key_upper}') is defined multiple times in '{json_file_path}' (after case conversion).")
                resolved_fqdn_map[canonical_key_upper] = current_canonical_env_fqdns

                # Add all aliases to the final lookup map
                aliases = details.get('aliases', [])
                if not isinstance(aliases, list):
                    raise ValueError(f"Aliases for '{canonical_key_raw}' in {json_file_path} must be a list.")
                
                for alias_raw in aliases:
                    if not isinstance(alias_raw, str):
                         raise ValueError(f"Alias '{alias_raw}' for '{canonical_key_raw}' in {json_file_path} is not a string.")
                    alias_upper = alias_raw.upper()
                    
                    if alias_upper in resolved_fqdn_map:
                        # Check if the alias points to the same canonical entry/FQDN set
                        if resolved_fqdn_map[alias_upper] != current_canonical_env_fqdns:
                             raise ValueError(
                                f"Alias conflict: '{alias_raw}' (resolves to '{alias_upper}') "
                                f"is defined as an alias for multiple distinct canonical entries in '{json_file_path}'. "
                                f"Existing maps to '{resolved_fqdn_map[alias_upper]}', new maps to '{current_canonical_env_fqdns}'."
                            )
                    resolved_fqdn_map[alias_upper] = current_canonical_env_fqdns # Map alias to the full environment-specific FQDN map

            return resolved_fqdn_map
    except json.JSONDecodeError as e:
        raise ValueError(f"Error decoding Source FQDN resolver file: {e}")
    except ValueError as e:
        raise e
    except Exception as e:
        raise Exception(f"An unexpected error occurred reading Source FQDN resolver file: {e}")
        

# config.py (Add this to the very end of the file)

# ... (all existing code for classes and functions)

if __name__ == "__main__":
    print("--- Testing load_fqdn_map function ---")

    # Test Case 1: Valid map with aliases
    print("\n=== Test Case 1: Valid map with aliases ===")
    # Create a temporary test JSON file for this case
    test_valid_json_path = "test_valid_fqdn_map.json"
    valid_map_content = {
      "PORTDB.PORTFOLIO_OPS_CANONICAL": {
        "fqdn": "RAW_DB.CORE.PORTFOLIO_OPS_TABLE",
        "aliases": ["PORTDB.PORTFOLIO_OPS", "PORTFOLIO_OPS_ALT"]
      },
      "ISSUER_TICKER_CANONICAL": {
        "fqdn": "RAW_DB.CORE.ISSUER_TICKER_TABLE",
        "aliases": ["ML_ASE.T_ASE_ISSUER_TICKER", "ISSUER_TICKER_VIEW"]
      }
    }
    with open(test_valid_json_path, 'w', encoding='utf-8') as f:
        json.dump(valid_map_content, f, indent=2)
    
    try:
        # Temporarily override FilePaths.SOURCE_FQDN_MAP_FILE for the test
        original_map_file = FilePaths.SOURCE_FQDN_MAP_FILE
        FilePaths.SOURCE_FQDN_MAP_FILE = test_valid_json_path

        test_map = load_fqdn_map()
        print("Successfully loaded valid map:")
        for k, v in test_map.items():
            print(f"  '{k}' -> '{v}'")
        print("All keys are correctly uppercased.")
    except Exception as e:
        print(f"ERROR in Test Case 1 (Valid map): {e}")
    finally:
        # Clean up temporary file and restore original path
        if os.path.exists(test_valid_json_path):
            os.remove(test_valid_json_path)
        FilePaths.SOURCE_FQDN_MAP_FILE = original_map_file


    # Test Case 2: Duplicate key (different case)
    print("\n=== Test Case 2: Duplicate key (different case) ===")
    test_duplicate_case_json_path = "test_duplicate_case_fqdn_map.json"
    duplicate_case_content = {
      "PORTDB.PORTFOLIO_OPS_CANONICAL": {
        "fqdn": "RAW_DB.CORE.TABLE_A",
        "aliases": []
      },
      "portdb.portfolio_ops_canonical": { # Duplicate key, different case
        "fqdn": "RAW_DB.CORE.TABLE_B",
        "aliases": []
      }
    }
    with open(test_duplicate_case_json_path, 'w', encoding='utf-8') as f:
        json.dump(duplicate_case_content, f, indent=2)
    
    try:
        original_map_file = FilePaths.SOURCE_FQDN_MAP_FILE
        FilePaths.SOURCE_FQDN_MAP_FILE = test_duplicate_case_json_path
        load_fqdn_map()
        print("ERROR: Duplicate key (different case) was NOT detected.")
    except ValueError as e:
        print(f"SUCCESS: Caught expected error for duplicate key (different case): {e}")
    except Exception as e:
        print(f"ERROR in Test Case 2 (Duplicate case): Unexpected error: {e}")
    finally:
        if os.path.exists(test_duplicate_case_json_path):
            os.remove(test_duplicate_case_json_path)
        FilePaths.SOURCE_FQDN_MAP_FILE = original_map_file


    # Test Case 3: Duplicate alias (alias defined twice)
    print("\n=== Test Case 3: Duplicate alias ===")
    test_duplicate_alias_json_path = "test_duplicate_alias_fqdn_map.json"
    duplicate_alias_content = {
      "CANONICAL_A": {
        "fqdn": "RAW_DB.A.TABLE",
        "aliases": ["ALIAS1", "COMMON_ALIAS"]
      },
      "CANONICAL_B": {
        "fqdn": "RAW_DB.B.TABLE",
        "aliases": ["ALIAS2", "COMMON_ALIAS"] # COMMON_ALIAS points to two FQDNs
      }
    }
    with open(test_duplicate_alias_json_path, 'w', encoding='utf-8') as f:
        json.dump(duplicate_alias_content, f, indent=2)
    
    try:
        original_map_file = FilePaths.SOURCE_FQDN_MAP_FILE
        FilePaths.SOURCE_FQDN_MAP_FILE = test_duplicate_alias_json_path
        load_fqdn_map()
        print("ERROR: Duplicate alias was NOT detected.")
    except ValueError as e:
        print(f"SUCCESS: Caught expected error for duplicate alias: {e}")
    except Exception as e:
        print(f"ERROR in Test Case 3 (Duplicate alias): Unexpected error: {e}")
    finally:
        if os.path.exists(test_duplicate_alias_json_path):
            os.remove(test_duplicate_alias_json_path)
        FilePaths.SOURCE_FQDN_MAP_FILE = original_map_file


    # Test Case 4: Missing FQDN in detail
    print("\n=== Test Case 4: Missing 'fqdn' key ===")
    test_missing_fqdn_json_path = "test_missing_fqdn_map.json"
    missing_fqdn_content = {
      "CANONICAL_X": {
        "aliases": ["ALIASX"] # Missing 'fqdn' key
      }
    }
    with open(test_missing_fqdn_json_path, 'w', encoding='utf-8') as f:
        json.dump(missing_fqdn_content, f, indent=2)
    
    try:
        original_map_file = FilePaths.SOURCE_FQDN_MAP_FILE
        FilePaths.SOURCE_FQDN_MAP_FILE = test_missing_fqdn_json_path
        load_fqdn_map()
        print("ERROR: Missing 'fqdn' key was NOT detected.")
    except ValueError as e:
        print(f"SUCCESS: Caught expected error for missing 'fqdn' key: {e}")
    except Exception as e:
        print(f"ERROR in Test Case 4 (Missing 'fqdn'): Unexpected error: {e}")
    finally:
        if os.path.exists(test_missing_fqdn_json_path):
            os.remove(test_missing_fqdn_json_path)
        FilePaths.SOURCE_FQDN_MAP_FILE = original_map_file


    print("\n--- Testing load_fqdn_map function complete ---")
