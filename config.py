# .env - This file should NOT be committed to version control!

# Confluence Credentials
CONFLUENCE_BASE_URL=https://your-company.atlassian.net/wiki
CONFLUENCE_USERNAME=your_confluence_username
CONFLUENCE_API_TOKEN=your_confluence_personal_access_token
CONFLUENCE_SPACE_KEY=YOURSPACE

# Snowflake Credentials
SNOWFLAKE_USER=your_snowflake_user
SNOWFLAKE_PASSWORD=your_snowflake_password
SNOWFLAKE_ACCOUNT=your_snowflake_account_identifier
SNOWFLAKE_WAREHOUSE=your_snowflake_warehouse
SNOWFLAKE_DATABASE=your_snowflake_database
SNOWFLAKE_SCHEMA=your_snowflake_schema
SNOWFLAKE_ROLE=your_snowflake_role

# Git Credentials (Example)
GIT_USERNAME=your_git_username
GIT_API_TOKEN=your_git_pat





# config.py

import os
from dotenv import load_dotenv

# Load environment variables from a .env file if it exists
# This is useful for local development to avoid setting environment variables
# directly in your shell for every session.
load_dotenv()

class ConfluenceConfig:
    BASE_URL = os.getenv("CONFLUENCE_BASE_URL")
    USERNAME = os.getenv("CONFLUENCE_USERNAME") # Often for basic auth or token owner
    API_TOKEN = os.getenv("CONFLUENCE_API_TOKEN") # Personal Access Token
    SPACE_KEY = os.getenv("CONFLUENCE_SPACE_KEY") # e.g., 'DEPT' or 'MYSPACE'

class SnowflakeConfig:
    USER = os.getenv("SNOWFLAKE_USER")
    PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
    ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
    WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
    DATABASE = os.getenv("SNOWFLAKE_DATABASE")
    SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
    ROLE = os.getenv("SNOWFLAKE_ROLE")

class GitConfig:
    # Example for Git, could be GitHub/GitLab PAT
    USERNAME = os.getenv("GIT_USERNAME")
    API_TOKEN = os.getenv("GIT_API_TOKEN")

# Add other service configs as needed (Azure Repos, dbt, etc.)

# Example: Centralized function to get a specific page title
# We might enhance this to use page IDs or search later.
def get_confluence_page_title():
    # This could be stored in a more general config if many pages
    # For now, let's keep it simple.
    return "Table: portfolio_ops"
