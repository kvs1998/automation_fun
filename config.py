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
# MODIFIED: Utility function to load the FQDN map with alias resolution
def load_fqdn_map(json_file_path=FilePaths.SOURCE_FQDN_MAP_FILE):
    """
    Loads the source_table to FQDN mapping from a JSON file.
    The map can now define canonical FQDNs with multiple aliases.
    It returns a resolved map where keys are all possible uppercase source_table names (canonical or alias)
    and values are their corresponding FQDNs.
    Raises ValueError if duplicate keys are found after case conversion or if an alias points to multiple FQDNs.
    """
    if not os.path.exists(json_file_path):
        raise FileNotFoundError(f"Source FQDN map file not found at: {json_file_path}")
    try:
        # Custom hook for case-sensitive duplicate keys in the raw JSON file
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
            raw_canonical_map = json.load(f, object_pairs_hook=_check_for_duplicate_keys_hook)
            
            if not isinstance(raw_canonical_map, dict):
                raise ValueError("Source FQDN map file must contain a dictionary of canonical entries.")
            
            resolved_fqdn_lookup_map = {} # This will be the final map: ALL_POSSIBLE_SOURCE_NAMES_UPPER -> FQDN
            
            for canonical_key_raw, details in raw_canonical_map.items():
                if not isinstance(details, dict) or 'fqdn' not in details:
                    raise ValueError(f"Entry for '{canonical_key_raw}' in {json_file_path} is malformed. Expected 'fqdn' key.")
                
                canonical_fqdn = details['fqdn'].upper() # Store FQDN in uppercase for consistency in Snowflake
                canonical_key_upper = canonical_key_raw.upper()

                # Check if canonical_fqdn has parts (DB.SCHEMA.TABLE)
                if len(canonical_fqdn.split('.')) != 3:
                     raise ValueError(f"FQDN '{canonical_fqdn}' for canonical key '{canonical_key_raw}' in {json_file_path} is not in DATABASE.SCHEMA.TABLE format.")

                # 1. Add canonical key itself to the lookup map
                if canonical_key_upper in resolved_fqdn_lookup_map and resolved_fqdn_lookup_map[canonical_key_upper] != canonical_fqdn:
                    raise ValueError(
                        f"Alias conflict: Canonical key '{canonical_key_raw}' (resolves to '{canonical_key_upper}') "
                        f"is attempting to map to multiple FQDNs in '{json_file_path}'."
                    )
                resolved_fqdn_lookup_map[canonical_key_upper] = canonical_fqdn

                # 2. Add all aliases to the lookup map
                aliases = details.get('aliases', [])
                if not isinstance(aliases, list):
                    raise ValueError(f"Aliases for '{canonical_key_raw}' in {json_file_path} must be a list.")
                
                for alias_raw in aliases:
                    alias_upper = alias_raw.upper()
                    if alias_upper in resolved_fqdn_lookup_map and resolved_fqdn_lookup_map[alias_upper] != canonical_fqdn:
                        raise ValueError(
                            f"Alias conflict: '{alias_raw}' (resolves to '{alias_upper}') "
                            f"is defined as an alias for multiple FQDNs in '{json_file_path}'. "
                            f"Existing maps to '{resolved_fqdn_lookup_map[alias_upper]}', new maps to '{canonical_fqdn}'."
                        )
                    resolved_fqdn_lookup_map[alias_upper] = canonical_fqdn

            return resolved_fqdn_lookup_map
    except json.JSONDecodeError as e:
        raise ValueError(f"Error decoding Source FQDN map file: {e}")
    except ValueError as e:
        raise e
    except Exception as e:
        raise Exception(f"An unexpected error occurred reading Source FQDN map file: {e}")
