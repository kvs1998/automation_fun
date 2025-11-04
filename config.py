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

# NEW: Utility function to load the FQDN map
def load_fqdn_map(json_file_path=FilePaths.SOURCE_FQDN_MAP_FILE):
    """
    Loads the source_table to FQDN mapping from a JSON file.
    """
    if not os.path.exists(json_file_path):
        raise FileNotFoundError(f"Source FQDN map file not found at: {json_file_path}")
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            fqdn_map = json.load(f)
            if not isinstance(fqdn_map, dict):
                raise ValueError("Source FQDN map file must contain a dictionary of key-value pairs.")
            return fqdn_map
    except json.JSONDecodeError as e:
        raise ValueError(f"Error decoding Source FQDN map file: {e}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred reading Source FQDN map file: {e}")
