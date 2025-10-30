
pip install requests beautifulsoup4 python-dotenv


# confluence_client.py
import requests
from bs4 import BeautifulSoup
from config import ConfluenceConfig, get_confluence_page_title
import os # For checking if .env loaded

class ConfluencePageParser:
    def __init__(self):
        self.base_url = ConfluenceConfig.BASE_URL
        self.username = ConfluenceConfig.USERNAME
        self.api_token = ConfluenceConfig.API_TOKEN
        self.space_key = ConfluenceConfig.SPACE_KEY
        self.page_title = get_confluence_page_title()

        if not all([self.base_url, self.username, self.api_token, self.space_key]):
            raise ValueError(
                "Confluence configuration is incomplete. "
                "Please ensure CONFLUENCE_BASE_URL, CONFLUENCE_USERNAME, "
                "CONFLUENCE_API_TOKEN, and CONFLUENCE_SPACE_KEY "
                "are set in your environment variables or .env file."
            )

    def _get_page_id_by_title(self, title):
        """
        Retrieves the page ID for a given page title in a specific space.
        """
        search_url = f"{self.base_url}/rest/api/content"
        headers = {
            "Accept": "application/json"
        }
        auth = (self.username, self.api_token)
        params = {
            "title": title,
            "spaceKey": self.space_key,
            "expand": "body.storage", # Request page content in storage format
            "limit": 1
        }

        print(f"Searching for page '{title}' in space '{self.space_key}'...")
        response = requests.get(search_url, headers=headers, auth=auth, params=params)
        response.raise_for_status() # Raise an exception for HTTP errors

        data = response.json()
        if data and data["results"]:
            page = data["results"][0]
            print(f"Found page '{page['title']}' with ID: {page['id']}")
            return page['id'], page['body']['storage']['value']
        else:
            print(f"Page '{title}' not found in space '{self.space_key}'.")
            return None, None

    def get_table_data_from_page(self):
        """
        Fetches the Confluence page content and attempts to parse
        the first table for our specific column definitions.
        """
        page_id, page_content_html = self._get_page_id_by_title(self.page_title)

        if not page_id or not page_content_html:
            print("Could not retrieve page content. Exiting.")
            return None, None

        soup = BeautifulSoup(page_content_html, 'html.parser')
        
        # --- Extract Header Information ---
        table_name = None
        source_table_full_name = None
        # You'll need to inspect the HTML structure of your page's "Destination" section
        # The provided image shows it's likely paragraphs or strong tags.
        # This is a heuristic and might need adjustment.
        
        # Example: looking for "Table name: portfolio_ops"
        table_name_tag = soup.find(lambda tag: tag.name == "p" and "Table name:" in tag.text)
        if table_name_tag:
            table_name = table_name_tag.text.split("Table name:")[1].strip()
        
        # Example: looking for "Source table" in the table content itself,
        # or from a different section if it's explicitly mentioned there.
        # For now, let's assume it's "portdb_portfolio_ops" as observed in the image
        # and we can make this more dynamic later if needed.
        source_table_full_name = "portdb_portfolio_ops" # Hardcoding for prototype, can be dynamic

        # --- Extract Table Data ---
        tables = soup.find_all('table')
        if not tables:
            print("No tables found on the Confluence page.")
            return None, None

        # Assuming the first table is the one with column definitions
        target_table_html = tables[0]

        headers = [th.text.strip() for th in target_table_html.find('thead').find_all('th')]
        
        # We need specific headers: 'Source field name', 'Add Source To Target?', 'Target Field name'
        required_headers = [
            'Source table', # Although it seems consistent, we should still extract it
            'Source field name', 
            'Add Source To Target?', 
            'Target Field name'
        ]
        
        # Check if all required headers are present
        if not all(rh in headers for rh in required_headers):
            print(f"Missing one or more required headers in the table. Found: {headers}")
            print(f"Expected: {required_headers}")
            return None, None

        # Get the indices of the required headers
        header_indices = {h: headers.index(h) for h in required_headers}

        extracted_rows = []
        for row in target_table_html.find('tbody').find_all('tr'):
            cols = row.find_all('td')
            if len(cols) > 0: # Ensure it's not an empty row
                row_data = {}
                for rh, idx in header_indices.items():
                    # Handle potential missing columns if table structure is inconsistent
                    row_data[rh] = cols[idx].text.strip() if idx < len(cols) else ""
                extracted_rows.append(row_data)

        table_metadata = {
            "table_name": table_name if table_name else "portfolio_ops", # Default if not found
            "database_name": "YOUR_SNOWFLAKE_DB", # Still needs clarification
            "schema_name": "YOUR_SNOWFLAKE_SCHEMA", # Still needs clarification
            "primary_keys": ["portfolio_id", "portfolio_tax_id"], # From observation, can be parsed
            "source_table_full_name": source_table_full_name
        }

        return extracted_rows, table_metadata

# Example usage (run this in your main script or a separate test)
if __name__ == "__main__":
    # Just a check to see if .env was loaded
    if os.getenv("CONFLUENCE_API_TOKEN"):
        print(".env file loaded successfully (Confluence API token found).")
    else:
        print("Warning: .env file might not be loaded or CONFLUENCE_API_TOKEN not set.")
        print("Please ensure your .env file is in the same directory and contains the necessary credentials.")

    try:
        parser = ConfluencePageParser()
        columns, metadata = parser.get_table_data_from_page()

        if columns and metadata:
            print("\n--- Successfully Extracted Data ---")
            print("Table Metadata:", metadata)
            print("\nColumns:")
            for col in columns:
                print(col)
        else:
            print("\n--- Failed to extract data from Confluence ---")

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error during Confluence API call: {e}")
        print(f"Response content: {e.response.text}")
    except ValueError as e:
        print(f"Configuration Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
