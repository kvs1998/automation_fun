# Confluence-to-Snowflake Data Pipeline Automation

## Project Overview

This project automates the extraction of structured data (table definitions) from Confluence wiki pages, performs validation and metadata enrichment, and tracks these data assets in a local SQLite database. The ultimate goal is to facilitate the generation of Snowflake DDL (Data Definition Language) and enable cross-environment schema parity checks for data assets, particularly for ML source tables.

The pipeline is designed with a modular, multi-stage approach, allowing for user verification and robust error handling at critical junctures.

## Key Features

*   **Modular Design:** Separates concerns into distinct Python scripts and utility modules.
*   **Confluence Data Extraction:** Connects to Confluence REST API to retrieve page content.
*   **Resilient Page Matching:** Uses a fuzzy-matching retry mechanism to find Confluence pages despite minor title variations.
*   **Dynamic HTML Parsing:** Dynamically extracts table structures and column definitions from varying Confluence HTML layouts using BeautifulSoup.
*   **Centralized FQDN Resolution:** Utilizes a `source_to_fqdn_resolver.json` file to map logical `source_table` names (from Confluence) to their Fully Qualified Domain Names (FQDNs) in Snowflake, supporting environment-specific FQDNs and aliases.
*   **Robust Data Cleaning:** Iteratively cleans special characters and normalizes whitespace in all extracted string data.
*   **Database-Driven State Management:** Uses SQLite (`confluence_metadata.db`) to persist:
    *   Confluence page metadata (author, dates, etc.).
    *   Hashes (`hash_id`, `last_parsed_content_hash`) for intelligent change detection.
    *   Parsed Confluence table definitions (stored as JSON strings).
    *   Snowflake ML source table metadata (existence, DDL, DDL history) across multiple environments.
*   **Cross-Environment Checks:** Capable of checking Snowflake object existence and extracting DDL from multiple defined environments (e.g., DEV, SPC, PROD).
*   **User Verification Workflow:** Incorporates a manual review step for Confluence page discovery before proceeding to deeper data processing.

## Project Structure


your_project_folder/

├── .env                          # Environment variables (credentials, environment names) - DO NOT COMMIT!

├── config.py                     # Global configuration (paths, env settings, FQDN map loader)

├── confluence_utils.py           # Confluence API interaction and HTML parsing utilities

├── database_manager.py           # SQLite database connection, table creation, and CRUD operations

├── report_generator.py           # Stage 1: Generates initial hit-or-miss report

├── metadata_ingestor.py          # Stage 2: Ingests Confluence page metadata into DB

├── data_parser.py                # Stage 3: Parses Confluence HTML content into DB

├── ml_table_checker.py           # Stage 3.5: Checks Snowflake ML source tables, extracts DDL, stores in DB

├── titles.json                   # Input: List of Confluence page titles to process

└── source_to_fqdn_resolver.json  # Input: Maps Confluence source_table names to Snowflake FQDNs across environments



## Setup and Installation

1.  **Clone the Repository:**
    ```bash
    git clone <your-repo-url>
    cd your_project_folder
    ```

2.  **Create a Python Virtual Environment (Recommended):**
    ```bash
    python -m venv venv
    # On Windows:
    .\venv\Scripts\activate
    # On macOS/Linux:
    source venv/bin/activate
    ```

3.  **Install Dependencies:**
    ```bash
    pip install requests beautifulsoup4 python-dotenv snowflake-connector-python
    ```
    (Note: `sqlite3` and `hashlib` are built-in to Python.)

4.  **Configure `.env` File:**
    Create a file named `.env` in your project root (`your_project_folder/`).
    **DO NOT commit this file to version control!**
    Populate it with your Confluence API token and Snowflake credentials for all environments you wish to check.

    ```ini
    # .env
    # Define the environment where *this script is currently running/deploying*
    DEPLOYMENT_ENVIRONMENT=DEV # Change to QA, PROD, etc. based on where you deploy this script

    # Confluence API Configuration
    CONFLUENCE_BASE_URL=https://your-company.atlassian.net/wiki # e.g., https://wiki.example.com
    CONFLUENCE_API_TOKEN=your_confluence_personal_access_token # Generate from Confluence profile settings
    CONFLUENCE_SPACE_KEY=YOURSPACEKEY # e.g., DEPT, PROJ

    # Snowflake Credentials for various environments to be CHECKED
    # Naming convention: SNOWFLAKE_{ENVIRONMENT_NAME}_PROPERTY
    
    # Example: DEV Environment Credentials
    SNOWFLAKE_DEV_USER=your_dev_snowflake_user
    SNOWFLAKE_DEV_PASSWORD=your_dev_snowflake_password
    SNOWFLAKE_DEV_ACCOUNT=your_dev_snowflake_account.region # e.g., abc12345.east-us-2.azure
    SNOWFLAKE_DEV_WAREHOUSE=your_dev_snowflake_warehouse
    SNOWFLAKE_DEV_DATABASE=your_dev_snowflake_database
    SNOWFLAKE_DEV_SCHEMA=your_dev_snowflake_schema
    SNOWFLAKE_DEV_ROLE=your_dev_snowflake_role

    # Example: SPC Environment Credentials
    SNOWFLAKE_SPC_USER=your_spc_snowflake_user
    SNOWFLAKE_SPC_PASSWORD=your_spc_snowflake_password
    SNOWFLAKE_SPC_ACCOUNT=your_spc_snowflake_account.region
    SNOWFLAKE_SPC_WAREHOUSE=your_spc_snowflake_warehouse
    SNOWFLAKE_SPC_DATABASE=your_spc_snowflake_database
    SNOWFLAKE_SPC_SCHEMA=your_spc_snowflake_schema
    SNOWFLAKE_SPC_ROLE=your_spc_snowflake_role

    # Add similar blocks for other environments listed in config.py's CHECK_ENVIRONMENTS (e.g., BFM, PRU, PROD)
    ```

5.  **Configure `titles.json`:**
    Create/update `titles.json` in your project root. This is a list of the exact Confluence page titles you want to process.

    ```json
    [
      "Table: portfolio_ops",
      "Table: Issuer Identifier",
      "Table : Sample Page"
    ]
    ```

6.  **Configure `source_to_fqdn_resolver.json`:**
    Create/update `source_to_fqdn_resolver.json` in your project root. This file defines how logical `source_table` names (from Confluence) map to their environment-specific FQDNs in Snowflake. Use the `defaults` and `specific_environments` structure to minimize duplication. All keys and FQDNs should be in **uppercase** for consistency.

    ```json
    {
      "PORTDB.PORTFOLIO_OPS_CANONICAL": {
        "aliases": ["PORTDB.PORTFOLIO_OPS", "PORTFOLIO_OPS_ALT"],
        "defaults": { # Environments using this default FQDN/type
          "environments": ["DEV", "QA", "PREPOD", "PRU"], 
          "fqdn": "COMMON_RAW_DB.COMMON_CORE.PORTFOLIO_OPS",
          "object_type": "TABLE"
        },
        "specific_environments": { # Overrides defaults for specific environments
          "PROD": {
            "fqdn": "PROD_RAW_DB.PROD_CORE.PORTFOLIO_OPS_PROD",
            "object_type": "TABLE"
          },
          "SPC": { 
            "fqdn": "SPC_ANALYTICS.CUSTOM.PORTFOLIO_SPC",
            "object_type": "VIEW"
          }
        }
      },
      "ISSUER_TICKER_CANONICAL": {
        "aliases": ["ML_ASE.T_ASE_ISSUER_TICKER", "ISSUER_MASTER_V"],
        "defaults": {
          "environments": ["DEV", "QA"],
          "fqdn": "ML_DB.MASTER.ISSUER_TICKER_COMMON",
          "object_type": "TABLE"
        },
        "specific_environments": {
          "PROD": {
            "fqdn": "PROD_ML_DB.ML_REF.ISSUER_PROD",
            "object_type": "TABLE"
          }
        }
      }
      # ... other canonical mappings
    }
    ```

## How to Run the Pipeline (Stage by Stage)

**Important:** Each stage creates or updates data in `tables/confluence_metadata.db`. If you need to **restart from a clean slate (e.g., after schema changes or for fresh testing)**, simply **delete the `tables/confluence_metadata.db` file** in your `your_project_folder/tables/` directory.

### Stage 1: Confluence Report Generation

This stage discovers Confluence pages and generates an initial hit-or-miss report for user review.

1.  **Run:**
    ```bash
    python report_generator.py
    ```
2.  **Output:** `your_project_folder/tables/confluence_ingest_report.json`

### Stage 1.5: User Review and Verification (Manual Action)

This is a **critical manual step** to ensure data quality and integrity before proceeding.

1.  **Open `your_project_folder/tables/confluence_ingest_report.json`** in a text editor.
2.  **Review the entries:**
    *   For each entry where `"status": "HIT"`, and you confirm that the `"found_title"` and `page_id` are correct, and you want to process this page further: **change `"user_verified": false` to `"user_verified": true`**.
    *   If a page is `"MISS"`, or `HIT` but you don't want to process its content, leave `"user_verified": false`.
3.  **Save the `confluence_ingest_report.json` file.**

### Stage 2: Confluence Metadata Ingestion

This fetches comprehensive metadata for *user-verified* pages and stores it in the SQLite database, including a `hash_id` for change detection.

1.  **Run:**
    ```bash
    python metadata_ingestor.py
    ```
2.  **Output:** Updates the `confluence_page_metadata` table in `your_project_folder/tables/confluence_metadata.db`.

### Stage 3: Confluence Content Parsing & Storage

This fetches the `body.storage` (HTML content) for pages with new or changed metadata, parses the embedded tables, and stores the structured JSON data in the `confluence_parsed_content` database table.

1.  **Run:**
    ```bash
    python data_parser.py
    ```
2.  **Output:** Updates the `confluence_parsed_content` table in `your_project_folder/tables/confluence_metadata.db`.

### Stage 3.5: Snowflake ML Source Table Existence Check & DDL Ingestion (Cross-Environment)

This stage identifies all `source_table` entries from your Confluence content, resolves them to environment-specific FQDNs, checks their existence in multiple Snowflake environments, extracts their DDL, and stores this information (including DDL history) in the `snowflake_ml_source_metadata` table.

1.  **Run:**
    ```bash
    python ml_table_checker.py
    ```
2.  **Output:** Updates the `snowflake_ml_source_metadata` table in `your_project_folder/tables/confluence_metadata.db`. The console will report any non-existent objects across environments.

## Module Details

### `config.py`
*   **Purpose:** Centralized configuration for the entire pipeline.
*   **Key Classes/Functions:**
    *   `DEPLOYMENT_ENVIRONMENT`: Global variable indicating the script's current deployment context.
    *   `CHECK_ENVIRONMENTS`: List of Snowflake environments to include in cross-environment checks.
    *   `ConfluenceConfig`: Holds Confluence API base URL, token, and space key.
    *   `SnowflakeConfig`: (Placeholder) Credentials for Snowflake, dynamically loaded per environment.
    *   `FilePaths`: Defines paths for all input/output files and database.
    *   `load_snowflake_env_credentials(env_name)`: Dynamically loads Snowflake credentials from `.env` based on a naming convention (e.g., `SNOWFLAKE_DEV_USER`).
    *   `get_confluence_page_titles()`: Loads the list of titles from `titles.json`.
    *   `load_fqdn_resolver()`: Loads and validates `source_to_fqdn_resolver.json`. It resolves logical source names/aliases to environment-specific FQDNs and object types, handling duplicates and structural validation.

### `confluence_utils.py`
*   **Purpose:** Encapsulates all interactions with the Confluence API and HTML parsing logic.
*   **Key Classes/Functions:**
    *   `clean_special_characters_iterative(data)`: Your provided function for deep-cleaning strings in nested data structures.
    *   `clean_text_from_html_basic(element)`: A simpler helper for initial HTML text extraction and basic non-breaking space handling.
    *   `ConfluencePageParser`:
        *   `__init__(base_url, api_token, space_key)`: Initializes the parser.
        *   `find_page_by_title(title)`: Uses fuzzy matching with retries to find a Confluence page by title, returning its ID and official title.
        *   `get_expanded_page_metadata(page_id)`: Fetches comprehensive page metadata (author, dates, labels, parent page) directly from the Confluence REST API (without `body.storage`).
        *   `get_structured_data_from_html(page_id, page_title_for_struct, page_content_html)`: Parses the raw HTML content (from `body.storage`) to extract structured table definitions and columns. It dynamically extracts headers and values.

### `database_manager.py`
*   **Purpose:** Manages the SQLite database connection, table creation, and all CRUD (Create, Read, Update, Delete) operations for the project's metadata.
*   **Key Classes/Functions:**
    *   `DatabaseManager`:
        *   `__init__(db_file)`: Connects to the specified SQLite database file.
        *   `connect()`: Establishes connection.
        *   `disconnect()`: Closes connection.
        *   `create_tables()`: Creates `confluence_page_metadata`, `confluence_parsed_content`, and `snowflake_ml_source_metadata` tables if they don't exist, with their respective schemas.
        *   `insert_or_update_page_metadata(metadata_dict)`: Inserts/updates records in `confluence_page_metadata`.
        *   `insert_or_update_parsed_content(page_id, parsed_json_str)`: Inserts/updates records in `confluence_parsed_content`.
        *   `insert_or_update_snowflake_ml_metadata(ml_metadata_dict)`: Inserts/updates records in `snowflake_ml_source_metadata`, handling DDL hash comparisons and DDL history.
        *   `get_page_metadata(page_id)`: Retrieves a single page's metadata.
        *   `get_parsed_content(page_id)`: Retrieves a page's parsed content.
        *   `get_snowflake_ml_metadata(fqdn, environment, object_type)`: Retrieves ML source metadata for a specific object in a specific environment.

### `report_generator.py`
*   **Purpose:** Stage 1 of the pipeline. Creates a basic report on Confluence page existence.
*   **Key Logic:**
    *   Reads `titles.json`.
    *   Uses `ConfluencePageParser.find_page_by_title()` for each title.
    *   Generates `confluence_ingest_report.json` with `HIT`/`MISS` status, `page_id`, `attempts_made`, and `user_verified` flag.

### `metadata_ingestor.py`
*   **Purpose:** Stage 2 of the pipeline. Fetches detailed page metadata and calculates hashes.
*   **Key Logic:**
    *   Reads the *user-verified* `confluence_ingest_report.json`.
    *   For approved pages, uses `ConfluencePageParser.get_expanded_page_metadata()` to get rich metadata.
    *   `calculate_metadata_hash(metadata_dict)`: Computes a hash of key metadata fields for change detection.
    *   Uses `DatabaseManager` to store/update this metadata in `confluence_page_metadata`, intelligently handling `extraction_status` to avoid overwriting downstream progress.

### `data_parser.py`
*   **Purpose:** Stage 3 of the pipeline. Fetches HTML content, parses it, and stores the structured JSON.
*   **Key Logic:**
    *   Queries `confluence_page_metadata` for pages needing parsing (hash changed, never parsed, or previous failure).
    *   Fetches `body.storage` content for these pages.
    *   Uses `ConfluencePageParser.get_structured_data_from_html()` for dynamic HTML table parsing.
    *   Stores the resulting structured JSON content as a string in `confluence_parsed_content` table.
    *   Updates `confluence_page_metadata` with `PARSED_OK` status and `last_parsed_content_hash`.

### `ml_table_checker.py`
*   **Purpose:** Stage 3.5 of the pipeline. Performs cross-environment existence checks and DDL extraction for source tables.
*   **Key Logic:**
    *   Loads `source_to_fqdn_resolver.json` to resolve logical `source_table` names to environment-specific FQDNs.
    *   Iterates through `CHECK_ENVIRONMENTS`.
    *   For each environment, initializes `SnowflakeManager` and uses `check_table_existence_and_get_ddl()` to verify existence and extract DDL for each resolved FQDN.
    *   Stores/updates this information in `snowflake_ml_source_metadata` table, including DDL history.
    *   Reports on non-existent objects across all checked environments.

## Future Stages

### Stage 4: SQL DDL Generation (`ddl_generator.py`)
*   Generates `CREATE TABLE` / `ALTER TABLE` SQL for Confluence-defined tables for a target (e.g., `DEPLOYMENT_ENVIRONMENT`).

### Stage 5: Snowflake DDL Execution & `SELECT` Statement Generation (`snowflake_deployer.py`)
*   Executes generated DDL in Snowflake and generates/executes `SELECT` statements for validation.

### Stage 6: Cross-Environment Schema Parity Reporting (`schema_parity_reporter.py`)
*   Compares DDLs/schemas of logical objects across different environments based on data in `snowflake_ml_source_metadata`.

---

This `README.md` provides a complete overview. Please replace your existing `README.md` with this content. Let me know if you'd like any section clarified or expanded!
