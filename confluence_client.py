# confluence_client.py
import requests
from bs4 import BeautifulSoup
from config import ConfluenceConfig, get_confluence_page_title
import os # For checking if .env loaded

class ConfluencePageParser:
    def __init__(self):
        self.base_url = ConfluenceConfig.BASE_URL
        # self.username = ConfluenceConfig.USERNAME # No longer directly used for Bearer auth
        self.api_token = ConfluenceConfig.API_TOKEN # This is now the Bearer token
        self.space_key = ConfluenceConfig.SPACE_KEY
        self.page_title = get_confluence_page_title()

        if not all([self.base_url, self.api_token, self.space_key]): # Removed username from check
            raise ValueError(
                "Confluence configuration is incomplete. "
                "Please ensure CONFLUENCE_BASE_URL, CONFLUENCE_API_TOKEN, "
                "and CONFLUENCE_SPACE_KEY are set in your environment variables or .env file."
            )

    def _get_page_id_by_title(self, title):
        """
        Retrieves the page ID and content for a given page title in a specific space.
        Uses Bearer token authentication.
        """
        search_url = f"{self.base_url}/rest/api/content"
        
        # --- IMPORTANT CHANGE HERE ---
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.api_token}" # Use Bearer token
        }
        # We no longer pass an 'auth' tuple with username/password

        params = {
            "title": title,
            "spaceKey": self.space_key,
            "expand": "body.storage",
            "limit": 1
        }

        print(f"Searching for page '{title}' in space '{self.space_key}'...")
        response = requests.get(search_url, headers=headers, params=params) # Removed auth=auth
        response.raise_for_status()

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
        
        # Confluence Storage Format can be tricky, it's not always clean HTML.
        # It's an XML-like format. BeautifulSoup can often parse it.
        soup = BeautifulSoup(page_content_html, 'html.parser')
        
        # --- Extract Header Information ---
        table_name = None
        
        # Inspect the HTML to find the "Table name: portfolio_ops" part.
        # It was originally in a <p> tag, but it might be different in storage format.
        # Let's try to be flexible.
        table_name_tag = soup.find(lambda tag: tag.name in ['p', 'strong', 'h1', 'h2', 'h3'] and "Table name:" in tag.text)
        if table_name_tag:
            table_name = table_name_tag.text.split("Table name:")[1].strip()
        
        # For prototype, use observed source table name.
        # We can make this dynamic later if it varies per page.
        source_table_full_name = "portdb_portfolio_ops" 
        
        # Extract Primary Keys and Foreign Keys as well from the text
        primary_keys = []
        foreign_keys = []
        
        # This part requires careful inspection of the storage format HTML for these specific lines.
        # Assuming they are in paragraph tags or similar, we can search.
        pk_tag = soup.find(lambda tag: tag.name in ['p', 'strong'] and "Primary Keys:" in tag.text)
        if pk_tag:
            pk_text = pk_tag.text.split("Primary Keys:")[1].strip()
            primary_keys = [k.strip() for k in pk_text.split(',')]
            
        fk_tag = soup.find(lambda tag: tag.name in ['p', 'strong'] and "Foreign Keys:" in tag.text)
        if fk_tag:
            fk_text = fk_tag.text.split("Foreign Keys:")[1].strip()
            foreign_keys = [k.strip() for k in fk_text.split(',')]


        # --- Extract Table Data ---
        tables = soup.find_all('table')
        if not tables:
            print("No tables found on the Confluence page.")
            return None, None

        target_table_html = tables[0]

        # Extract headers from the table head
        headers = [th.text.strip() for th in target_table_html.find('thead').find_all('th')]
        
        # Define the headers we specifically need for SQL generation
        required_headers = [
            'Source table',
            'Source field name', 
            'Add Source To Target?', 
            'Target Field name'
        ]
        
        if not all(rh in headers for rh in required_headers):
            print(f"Missing one or more required headers in the table. Found: {headers}")
            print(f"Expected: {required_headers}")
            return None, None

        header_indices = {h: headers.index(h) for h in required_headers}

        extracted_rows = []
        for row in target_table_html.find('tbody').find_all('tr'):
            cols = row.find_all('td')
            if len(cols) > 0:
                row_data = {}
                for rh, idx in header_indices.items():
                    row_data[rh] = cols[idx].text.strip() if idx < len(cols) else ""
                extracted_rows.append(row_data)

        # Still need clarification on actual DB/Schema names for Snowflake
        table_metadata = {
            "table_name": table_name if table_name else "portfolio_ops",
            "database_name": "YOUR_SNOWFLAKE_DB", 
            "schema_name": "YOUR_SNOWFLAKE_SCHEMA", 
            "primary_keys": primary_keys if primary_keys else ["portfolio_id", "portfolio_tax_id"], # Fallback
            "foreign_keys": foreign_keys,
            "source_table_full_name": source_table_full_name
        }

        return extracted_rows, table_metadata

# Example usage (run this in your main script or a separate test)
if __name__ == "__main__":
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
