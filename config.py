# config.py (MODIFIED)

import os
from dotenv import load_dotenv
import json

load_dotenv()

class ConfluenceConfig:
    BASE_URL = os.getenv("CONFLUENCE_BASE_URL")
    API_TOKEN = os.getenv("CONFLUENCE_API_TOKEN")
    SPACE_KEY = os.getenv("CONFLUENCE_SPACE_KEY")

class SnowflakeConfig:
    USER = os.getenv("SNOWFLAKE_USER")
    PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
    ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
    WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
    DATABASE = os.getenv("SNOWFLAKE_DATABASE")
    SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
    ROLE = os.getenv("SNOWFLAKE_ROLE")

class FilePaths:
    TITLES_JSON_FILE = "titles.json" # Input: list of page titles
    REPORT_JSON_FILE = "confluence_ingest_report.json" # Output: hit-or-miss report
    TABLES_DIR = "tables" # Output directory for structured table data & DB
    DB_FILE = "confluence_metadata.db" # SQLite database file name
    SOURCE_FQDN_MAP_FILE = "source_to_fqdn_map.json" # NEW: Mirror source for FQDN mapping

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
def load_fqdn_map(json_file_path=FilePaths.SOURCE_FQDN_MAP_FILE):
    """
    Loads the source_table to FQDN mapping from a JSON file.
    Performs checks for duplicate keys, both case-sensitive within the JSON file
    and case-insensitive after standardization.
    All keys in the final map will be converted to uppercase for case-insensitive matching.
    """
    if not os.path.exists(json_file_path):
        raise FileNotFoundError(f"Source FQDN map file not found at: {json_file_path}")
    try:
        # Custom hook to check for duplicate keys (case-sensitive) during JSON loading
        def _check_for_duplicate_keys_hook(ordered_pairs):
            d = {}
            for k, v in ordered_pairs:
                if k in d:
                    raise ValueError(
                        f"Duplicate key '{k}' found in '{json_file_path}' "
                        f"(case-sensitive). Please ensure all keys within the JSON "
                        f"file itself are unique."
                    )
                d[k] = v
            return d

        with open(json_file_path, 'r', encoding='utf-8') as f:
            # Load raw map, checking for case-sensitive duplicates first
            raw_fqdn_map = json.load(f, object_pairs_hook=_check_for_duplicate_keys_hook)
            
            if not isinstance(raw_fqdn_map, dict):
                raise ValueError("Source FQDN map file must contain a dictionary of key-value pairs.")
            
            # Now, convert keys to uppercase and check for case-insensitive duplicates
            fqdn_map = {}
            for k_raw, v in raw_fqdn_map.items():
                k_upper = k_raw.upper()
                if k_upper in fqdn_map:
                    # This catches duplicates like "Key" and "key" after conversion
                    # (though _check_for_duplicate_keys_hook should already handle "Key" and "Key")
                    raise ValueError(
                        f"Duplicate key '{k_raw}' (after case conversion to '{k_upper}') "
                        f"found in '{json_file_path}'. Please ensure all keys are unique "
                        f"when compared case-insensitively."
                    )
                fqdn_map[k_upper] = v
            return fqdn_map
    except json.JSONDecodeError as e:
        # json.decoder.JSONDecodeError itself handles basic syntax errors
        # If _check_for_duplicate_keys_hook raises ValueError, it will be caught below
        raise ValueError(f"Error decoding Source FQDN map file: {e}")
    except ValueError as e: # Catch our custom ValueError from the hook or post-processing
        raise e
    except Exception as e:
        raise Exception(f"An unexpected error occurred reading Source FQDN map file: {e}")
