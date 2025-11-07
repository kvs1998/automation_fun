# config.py
import os
from dotenv import load_dotenv
import json

load_dotenv()

# We need the ENVIRONMENT where *this script is running/deploying* (DEV)
# This is usually set in the .env or by the CI/CD pipeline
DEPLOYMENT_ENVIRONMENT = os.getenv("DEPLOYMENT_ENVIRONMENT", "DEV").upper()

# NEW: Environments to perform parity checks against (the target environments for comparison)
# These should be YOUR SPECIFIC ENVIRONMENT NAMES (e.g., SPC, BFM, PRU, ELD)
# Make sure the list is complete for all environments you might compare against
CHECK_ENVIRONMENTS = [
    "DEV", # Your deployment target
    "SPC", # Example of another environment
    "BFM", # Another example
    "PRU"  # Another example
    # Add other environments like "QA", "UAT", "PROD", "DR", "ELD" as needed
]

class ConfluenceConfig:
    BASE_URL = os.getenv("CONFLUENCE_BASE_URL")
    API_TOKEN = os.getenv("CONFLUENCE_API_TOKEN")
    SPACE_KEY = os.getenv("CONFLUENCE_SPACE_KEY")

class SnowflakeConfig:
    # This class will now primarily hold the structure for dynamic loading
    # Credentials are loaded dynamically via load_snowflake_env_credentials
    pass


# NEW: Function to dynamically load Snowflake credentials for a specific environment
def load_snowflake_env_credentials(env_name):
    env_prefix = f"SNOWFLAKE_{env_name.upper()}_" # e.g., SNOWFLAKE_PREPOD_USER
    
    user = os.getenv(f"{env_prefix}USER")
    password = os.getenv(f"{env_prefix}PASSWORD")
    account = os.getenv(f"{env_prefix}ACCOUNT")
    warehouse = os.getenv(f"{env_prefix}WAREHOUSE")
    database = os.getenv(f"{env_prefix}DATABASE")
    schema = os.getenv(f"{env_prefix}SCHEMA")
    role = os.getenv(f"{env_prefix}ROLE")

    if not all([user, password, account, warehouse, database, schema, role]):
        # Raise error but provide useful info
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
    # MODIFIED: Single resolver file, not environment-specific in its name
    SOURCE_FQDN_RESOLVER_FILE = "source_to_fqdn_resolver.json"
    SNOWFLAKE_ML_SOURCE_TABLE = "snowflake_ml_source_metadata"
    
    # NEW: Report output directory, same as TABLES_DIR for simplicity
    REPORT_OUTPUT_DIR = "tables"

    DEFAULT_REPORT_ARGS_FILE = "default_ml_ddl_report_args.json"
    # NEW: Data Type Mapping file
    DATA_TYPE_MAP_FILE = "data_type_map.json"

    # NEW: Column Mapper Configuration File
    COLUMN_MAPPER_CONFIG_FILE = "column_mapper_config.json"
    
def get_confluence_page_titles(json_file_path="titles.json"):
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
                        if resolved_fqdn_map[alias_upper] != current_canonical_env_fqdns:
                             raise ValueError(
                                f"Alias conflict: '{alias_raw}' (resolves to '{alias_upper}') "
                                f"is defined as an alias for multiple distinct canonical entries in '{json_file_path}'. "
                                f"Existing maps to '{resolved_fqdn_map[alias_upper]}', new maps to '{current_canonical_env_fqdns}'."
                            )
                    resolved_fqdn_map[alias_upper] = current_canonical_env_fqdns

            return resolved_fqdn_map
    except json.JSONDecodeError as e:
        raise ValueError(f"Error decoding Source FQDN resolver file: {e}")
    except ValueError as e:
        raise e
    except Exception as e:
        raise Exception(f"An unexpected error occurred reading Source FQDN resolver file: {e}")

# NEW: Function to load column mapper configuration
def load_column_mapper_config(json_file_path=FilePaths.COLUMN_MAPPER_CONFIG_FILE):
    """
    Loads column mapper configuration from a JSON file.
    """
    if not os.path.exists(json_file_path):
        raise FileNotFoundError(f"Column mapper config file not found at: {json_file_path}")
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            if not isinstance(config, dict):
                raise ValueError("Column mapper config file must contain a dictionary.")
            
            # Basic validation of expected keys
            if 'match_threshold' not in config or not isinstance(config['match_threshold'], (int, float)):
                raise ValueError("Column mapper config must contain 'match_threshold' (int/float).")
            if 'match_strategy' not in config or not isinstance(config['match_strategy'], str):
                raise ValueError("Column mapper config must contain 'match_strategy' (str, e.g., 'ratio', 'token_set_ratio').")
            if config['match_strategy'].upper() not in ["RATIO", "PARTIAL_RATIO", "TOKEN_SORT_RATIO", "TOKEN_SET_RATIO"]:
                 raise ValueError(f"Invalid match_strategy: '{config['match_strategy']}'. Must be one of RATIO, PARTIAL_RATIO, TOKEN_SORT_RATIO, TOKEN_SET_RATIO.")

            # Ensure match_threshold is within 0-100
            config['match_threshold'] = max(0, min(100, config['match_threshold']))
            
            return config
    except json.JSONDecodeError as e:
        raise ValueError(f"Error decoding Column Mapper config file: {e}")
    except ValueError as e:
        raise e
    except Exception as e:
        raise Exception(f"An unexpected error occurred reading Column Mapper config file: {e}")
        
# NEW: Function to load the data type mapping
def load_data_type_map(json_file_path=FilePaths.DATA_TYPE_MAP_FILE):
    """
    Loads the Confluence data type to Snowflake data type mapping from a JSON file.
    All keys (Confluence types) are converted to uppercase for case-insensitive matching.
    """
    if not os.path.exists(json_file_path):
        raise FileNotFoundError(f"Data type map file not found at: {json_file_path}")
    try:
        def _raise_on_duplicate_keys(ordered_pairs):
            d = {}
            for k, v in ordered_pairs:
                if k in d:
                    raise ValueError(f"Duplicate key '{k}' found in '{json_file_path}' (case-sensitive). Please ensure all keys within the JSON file itself are unique.")
                d[k] = v
            return d

        with open(json_file_path, 'r', encoding='utf-8') as f:
            raw_type_map = json.load(f, object_pairs_hook=_raise_on_duplicate_keys)
            
            if not isinstance(raw_type_map, dict):
                raise ValueError("Data type map file must contain a dictionary of key-value pairs.")
            
            # Convert all keys to uppercase for consistent, case-insensitive lookup
            data_type_map = {k.upper(): v for k, v in raw_type_map.items()}
            
            # Basic validation of map values
            for conf_type, sf_type in data_type_map.items():
                if not isinstance(sf_type, str) or not sf_type.strip():
                    raise ValueError(f"Snowflake type for Confluence type '{conf_type}' is invalid: '{sf_type}'. Must be a non-empty string.")
            return data_type_map
    except json.JSONDecodeError as e:
        raise ValueError(f"Error decoding Data Type map file: {e}")
    except ValueError as e:
        raise e
    except Exception as e:
        raise Exception(f"An unexpected error occurred reading Data Type map file: {e}")
        
        
        
# --- Test Block for load_fqdn_resolver ---
# --- Test Block for load_fqdn_resolver and load_data_type_map ---

# --- Test Block for load_fqdn_resolver and load_data_type_map ---
if __name__ == "__main__":
    print("--- Testing load_fqdn_resolver and load_data_type_map functions ---")

    # Test Case 1: Valid resolver map with defaults and specific overrides
    print("\n=== Test Case 1: Valid map with defaults and specific overrides ===")
    test_valid_json_path = "test_valid_fqdn_resolver.json"
    valid_map_content = {
      "PORTDB.PORTFOLIO_OPS_CANONICAL": {
        "aliases": ["PORTDB.PORTFOLIO_OPS", "PORTFOLIO_OPS_ALT"],
        "defaults": {
          "environments": ["DEV", "QA"],
          "fqdn": "RAW_DB.CORE.PORTFOLIO_OPS_COMMON",
          "object_type": "TABLE"
        },
        "specific_environments": {
          "PROD": {
            "fqdn": "PROD_RAW_DB.PROD_CORE.PORTFOLIO_OPS_PROD",
            "object_type": "TABLE"
          }
        }
      },
      "ISSUER_TICKER_CANONICAL": {
        "aliases": ["ML_ASE.T_ASE_ISSUER_TICKER"],
        "defaults": {
          "environments": ["PREPOD", "DR"],
          "fqdn": "RAW_DB.CORE.ISSUER_TICKER_PREPOD",
          "object_type": "VIEW"
        }
      }
    }
    with open(test_valid_json_path, 'w', encoding='utf-8') as f:
        json.dump(valid_map_content, f, indent=2)
    
    try:
        original_resolver_file = FilePaths.SOURCE_FQDN_RESOLVER_FILE
        FilePaths.SOURCE_FQDN_RESOLVER_FILE = test_valid_json_path

        test_map = load_fqdn_resolver()
        print("Successfully loaded valid resolver map:")
        for k, v in test_map.items():
            print(f"  '{k}' -> '{v}'")
        if test_map.get("PORTDB.PORTFOLIO_OPS_CANONICAL", {}).get("DEV") == {"fqdn": "RAW_DB.CORE.PORTFOLIO_OPS_COMMON", "object_type": "TABLE"} and \
           test_map.get("PORTDB.PORTFOLIO_OPS_CANONICAL", {}).get("PROD") == {"fqdn": "PROD_RAW_DB.PROD_CORE.PORTFOLIO_OPS_PROD", "object_type": "TABLE"}:
            print("  Specific environment lookups work as expected.")
        else:
            print("  WARNING: Specific environment lookups may not be working as expected.")
    except Exception as e:
        print(f"ERROR in Test Case 1 (Valid map): {e}")
    finally:
        if os.path.exists(test_valid_json_path):
            os.remove(test_valid_json_path)
        FilePaths.SOURCE_FQDN_RESOLVER_FILE = original_resolver_file


    # Test Case 2: Duplicate canonical key (different case)
    print("\n=== Test Case 2: Duplicate canonical key (different case) ===")
    test_duplicate_case_json_path = "test_duplicate_case_fqdn_resolver.json"
    duplicate_case_content = {
      "CANONICAL_A": {
        "defaults": {"environments": ["DEV"], "fqdn": "DB.A.TABLE", "object_type": "TABLE"}
      },
      "canonical_a": { # Duplicate key, different case
        "defaults": {"environments": ["PROD"], "fqdn": "DB.B.TABLE", "object_type": "TABLE"}
      }
    }
    with open(test_duplicate_case_json_path, 'w', encoding='utf-8') as f:
        json.dump(duplicate_case_content, f, indent=2)
    
    try:
        original_resolver_file = FilePaths.SOURCE_FQDN_RESOLVER_FILE
        FilePaths.SOURCE_FQDN_RESOLVER_FILE = test_duplicate_case_json_path
        load_fqdn_resolver()
        print("ERROR: Duplicate canonical key (different case) was NOT detected.")
    except ValueError as e:
        print(f"SUCCESS: Caught expected error for duplicate canonical key (different case): {e}")
    except Exception as e:
        print(f"ERROR in Test Case 2 (Duplicate case): Unexpected error: {e}")
    finally:
        if os.path.exists(test_duplicate_case_json_path):
            os.remove(test_duplicate_case_json_path)
        FilePaths.SOURCE_FQDN_RESOLVER_FILE = original_resolver_file


    # Test Case 3: Alias conflict (same alias points to different canonicals/FQDNs)
    print("\n=== Test Case 3: Alias conflict ===")
    test_duplicate_alias_json_path = "test_duplicate_alias_fqdn_resolver.json"
    duplicate_alias_content = {
      "CANONICAL_X": {
        "defaults": {"environments": ["DEV"], "fqdn": "DB.X.TABLE", "object_type": "TABLE"},
        "aliases": ["COMMON_ALIAS"]
      },
      "CANONICAL_Y": {
        "defaults": {"environments": ["DEV"], "fqdn": "DB.Y.TABLE", "object_type": "TABLE"}, # Different FQDN
        "aliases": ["COMMON_ALIAS"] # Same alias, different FQDN
      }
    }
    with open(test_duplicate_alias_json_path, 'w', encoding='utf-8') as f:
        json.dump(duplicate_alias_content, f, indent=2)
    
    try:
        original_resolver_file = FilePaths.SOURCE_FQDN_RESOLVER_FILE
        FilePaths.SOURCE_FQDN_RESOLVER_FILE = test_duplicate_alias_json_path
        load_fqdn_resolver()
        print("ERROR: Alias conflict was NOT detected.")
    except ValueError as e:
        print(f"SUCCESS: Caught expected error for alias conflict: {e}")
    except Exception as e:
        print(f"ERROR in Test Case 3 (Alias conflict): Unexpected error: {e}")
    finally:
        if os.path.exists(test_duplicate_alias_json_path):
            os.remove(test_duplicate_alias_json_path)
        FilePaths.SOURCE_FQDN_RESOLVER_FILE = original_resolver_file


    # Test Case 4: Missing 'fqdn' key in default entry
    print("\n=== Test Case 4: Missing 'fqdn' in 'defaults' ===")
    test_missing_fqdn_default_json_path = "test_missing_fqdn_default_resolver.json"
    missing_fqdn_default_content = {
      "CANONICAL_Z": {
        "defaults": {"environments": ["DEV"]}, # Missing 'fqdn' key
        "aliases": []
      }
    }
    with open(test_missing_fqdn_default_json_path, 'w', encoding='utf-8') as f:
        json.dump(missing_fqdn_default_content, f, indent=2)
    
    try:
        original_resolver_file = FilePaths.SOURCE_FQDN_RESOLVER_FILE
        FilePaths.SOURCE_FQDN_RESOLVER_FILE = test_missing_fqdn_default_json_path
        load_fqdn_resolver()
        print("ERROR: Missing 'fqdn' in 'defaults' was NOT detected.")
    except ValueError as e:
        print(f"SUCCESS: Caught expected error for missing 'fqdn' in 'defaults': {e}")
    except Exception as e:
        print(f"ERROR in Test Case 4 (Missing 'fqdn' default): Unexpected error: {e}")
    finally:
        if os.path.exists(test_missing_fqdn_default_json_path):
            os.remove(test_missing_fqdn_default_json_path)
        FilePaths.SOURCE_FQDN_RESOLVER_FILE = original_resolver_file


    # Test Case 5: Malformed FQDN format
    print("\n=== Test Case 5: Malformed FQDN format ===")
    test_malformed_fqdn_json_path = "test_malformed_fqdn_resolver.json"
    malformed_fqdn_content = {
      "CANONICAL_M": {
        "defaults": {"environments": ["DEV"], "fqdn": "DB.SCHEMA_ONLY", "object_type": "TABLE"}, # Malformed FQDN
        "aliases": []
      }
    }
    with open(test_malformed_fqdn_json_path, 'w', encoding='utf-8') as f:
        json.dump(malformed_fqdn_content, f, indent=2)
    
    try:
        original_resolver_file = FilePaths.SOURCE_FQDN_RESOLVER_FILE
        FilePaths.SOURCE_FQDN_RESOLVER_FILE = test_malformed_fqdn_json_path
        load_fqdn_resolver()
        print("ERROR: Malformed FQDN was NOT detected.")
    except ValueError as e:
        print(f"SUCCESS: Caught expected error for malformed FQDN: {e}")
    except Exception as e:
        print(f"ERROR in Test Case 5 (Malformed FQDN): Unexpected error: {e}")
    finally:
        if os.path.exists(test_malformed_fqdn_json_path):
            os.remove(test_malformed_fqdn_json_path)
        FilePaths.SOURCE_FQDN_RESOLVER_FILE = original_resolver_file


    # Test Case 6: Missing 'environments' key in defaults
    print("\n=== Test Case 6: Missing 'environments' in 'defaults' ===")
    test_missing_envs_json_path = "test_missing_envs_resolver.json"
    missing_envs_content = {
      "CANONICAL_E": {
        "defaults": {"fqdn": "DB.E.TABLE", "object_type": "TABLE"}, # Missing 'environments'
        "aliases": []
      }
    }
    with open(test_missing_envs_json_path, 'w', encoding='utf-8') as f:
        json.dump(missing_envs_content, f, indent=2)
    
    try:
        original_resolver_file = FilePaths.SOURCE_FQDN_RESOLVER_FILE
        FilePaths.SOURCE_FQDN_RESOLVER_FILE = test_missing_envs_json_path
        load_fqdn_resolver()
        print("ERROR: Missing 'environments' in 'defaults' was NOT detected.")
    except ValueError as e:
        print(f"SUCCESS: Caught expected error for missing 'environments' in 'defaults': {e}")
    except Exception as e:
        print(f"ERROR in Test Case 6 (Missing environments): Unexpected error: {e}")
    finally:
        if os.path.exists(test_missing_envs_json_path):
            os.remove(test_missing_envs_json_path)
        FilePaths.SOURCE_FQDN_RESOLVER_FILE = original_resolver_file
        
    # Test Case 7: Canonical key with no environment mapping at all
    print("\n=== Test Case 7: Canonical key with no environment mapping ===")
    test_no_env_map_json_path = "test_no_env_map_resolver.json"
    no_env_map_content = {
      "CANONICAL_N": {
        "aliases": ["ALIAS_N"]
        # No 'defaults' or 'specific_environments'
      }
    }
    with open(test_no_env_map_json_path, 'w', encoding='utf-8') as f:
        json.dump(no_env_map_content, f, indent=2)
    
    try:
        original_resolver_file = FilePaths.SOURCE_FQDN_RESOLVER_FILE
        FilePaths.SOURCE_FQDN_RESOLVER_FILE = test_no_env_map_json_path
        load_fqdn_resolver()
        print("ERROR: No environment mapping for canonical key was NOT detected.")
    except ValueError as e:
        print(f"SUCCESS: Caught expected error for no environment mapping: {e}")
    except Exception as e:
        print(f"ERROR in Test Case 7 (No env mapping): Unexpected error: {e}")
    finally:
        if os.path.exists(test_no_env_map_json_path):
            os.remove(test_no_env_map_json_path)
        FilePaths.SOURCE_FQDN_RESOLVER_FILE = original_resolver_file

    print("\n--- Testing load_fqdn_resolver function complete ---")
    
