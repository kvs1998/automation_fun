# confluence_client.py
import requests
from bs4 import BeautifulSoup
from config import ConfluenceConfig, get_confluence_page_title
import os
import json # To output structured JSON

class ConfluencePageParser:
    def __init__(self):
        self.base_url = ConfluenceConfig.BASE_URL
        self.api_token = ConfluenceConfig.API_TOKEN
        self.space_key = ConfluenceConfig.SPACE_KEY
        self.page_title = get_confluence_page_title()

        if not all([self.base_url, self.api_token, self.space_key]):
            raise ValueError(
                "Confluence configuration is incomplete. "
                "Please ensure CONFLUENCE_BASE_URL, CONFLUENCE_API_TOKEN, "
                "and CONFLUENCE_SPACE_KEY are set in your environment variables or .env file."
            )

    def _get_page_id_by_title(self, title):
        search_url = f"{self.base_url}/rest/api/content"
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.api_token}"
        }
        params = {
            "title": title,
            "spaceKey": self.space_key,
            "expand": "body.storage",
            "limit": 1
        }

        print(f"Searching for page '{title}' in space '{self.space_key}'...")
        response = requests.get(search_url, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()
        if data and data["results"]:
            page = data["results"][0]
            print(f"Found page '{page['title']}' with ID: {page['id']}")
            return page['id'], page['body']['storage']['value']
        else:
            print(f"Page '{title}' not found in space '{self.space_key}'.")
            return None, None

    def get_structured_data_from_page(self): # Renamed for broader output
        """
        Fetches the Confluence page content and attempts to parse
        all tables and page metadata.
        Returns a dictionary containing page metadata and a list of parsed tables.
        """
        page_id, page_content_html = self._get_page_id_by_title(self.page_title)

        if not page_id or not page_content_html:
            print("Could not retrieve page content. Exiting.")
            return None

        soup = BeautifulSoup(page_content_html, 'html.parser')
        
        # Initialize dictionary for all structured data
        structured_page_data = {
            "page_title": self.page_title,
            "page_id": page_id,
            "metadata": {},
            "tables": []
        }

        # --- Extract Page-Level Metadata (Table Name, DB, Schema, Keys) ---
        # This part is highly dependent on how your SME formats the text *outside* the table
        # Based on your initial screenshot, these were typically in <p> tags.
        
        # Helper to extract key-value pairs from text
        def extract_text_metadata(soup_obj, label):
            tag = soup_obj.find(lambda t: t.name in ['p', 'strong', 'h1', 'h2', 'h3', 'div'] and label in t.text)
            if tag:
                # Remove common tags and get pure text, then split
                clean_text = ' '.join(tag.stripped_strings) # Better for mixed content
                parts = clean_text.split(label)
                if len(parts) > 1:
                    value = parts[1].strip()
                    # Remove "Historization: SCD-2" if present in Database name
                    if label == "Database name:" and "Historization: SCD-2" in value:
                        value = value.replace("Historization: SCD-2", "").strip()
                    return value
            return None

        structured_page_data["metadata"]["table_name"] = extract_text_metadata(soup, "Table name:")
        structured_page_data["metadata"]["schema_name"] = extract_text_metadata(soup, "Schema name:")
        structured_page_data["metadata"]["database_name"] = extract_text_metadata(soup, "Database name:")

        # For primary/foreign keys, they might be comma-separated
        pk_text = extract_text_metadata(soup, "Primary Keys:")
        if pk_text:
            structured_page_data["metadata"]["primary_keys"] = [k.strip() for k in pk_text.split(',') if k.strip()]
        else:
            structured_page_data["metadata"]["primary_keys"] = [] # Default if not found

        fk_text = extract_text_metadata(soup, "Foreign Keys:")
        if fk_text:
            structured_page_data["metadata"]["foreign_keys"] = [k.strip() for k in fk_text.split(',') if k.strip()]
        else:
            structured_page_data["metadata"]["foreign_keys"] = [] # Default if not found

        # Fallback if primary page metadata extraction fails
        if not structured_page_data["metadata"].get("table_name"):
             structured_page_data["metadata"]["table_name"] = "portfolio_ops" # Based on the page title
        
        # Source table name from the columns, or a default
        structured_page_data["metadata"]["source_table_full_name"] = "portdb_portfolio_ops" # Still assuming this, can make dynamic if needed


        # --- Extract Table Data (Iterate through all tables) ---
        all_html_tables = soup.find_all('table')
        if not all_html_tables:
            print("No tables found on the Confluence page.")
            return structured_page_data # Return metadata even if no tables

        for i, html_table in enumerate(all_html_tables):
            table_id = f"table_{i+1}"
            parsed_table_data = {
                "id": table_id,
                "columns": []
            }
            
            rows = html_table.find_all('tr')
            if not rows:
                print(f"Table {table_id} has no rows. Skipping.")
                continue

            # Extract headers from the first row (assuming first row is always headers)
            # Use 'th' if available, otherwise 'td' for the first row
            header_cells = rows[0].find_all(['th', 'td'])
            headers = [cell.get_text(strip=True) for cell in header_cells]

            # Define headers we specifically need for SQL generation.
            # We'll use these to filter and rename.
            required_output_headers = {
                'Source table': 'source_table',
                'Source field name': 'source_field_name', 
                'Add Source To Target?': 'add_to_target', 
                'Target Field name': 'target_field_name'
            }
            
            # Find indices for the headers we care about in the *actual* table headers
            header_indices = {}
            for req_header_orig, req_header_standardized in required_output_headers.items():
                try:
                    header_indices[req_header_standardized] = headers.index(req_header_orig)
                except ValueError:
                    print(f"Warning: Required header '{req_header_orig}' not found in table {table_id}. It might be skipped.")
                    # If a critical header is missing, we might decide to skip the whole table
                    # For now, we'll allow it and the corresponding field will be missing.
                    header_indices[req_header_standardized] = -1 # Sentinel value

            # Process data rows
            for row in rows[1:]: # Skip the first row (headers)
                cols = row.find_all('td')
                if len(cols) > 0:
                    column_data = {}
                    for standardized_name, idx in header_indices.items():
                        if idx != -1 and idx < len(cols):
                            # Use .get_text(strip=True) to get clean text, ignoring HTML tags
                            column_data[standardized_name] = cols[idx].get_text(strip=True)
                        else:
                            column_data[standardized_name] = "" # Assign empty string if column is missing or header not found
                    parsed_table_data["columns"].append(column_data)
            
            structured_page_data["tables"].append(parsed_table_data)
            
        return structured_page_data

# Example usage
if __name__ == "__main__":
    if os.getenv("CONFLUENCE_API_TOKEN"):
        print(".env file loaded successfully (Confluence API token found).")
    else:
        print("Warning: .env file might not be loaded or CONFLUENCE_API_TOKEN not set.")
        print("Please ensure your .env file is in the same directory and contains the necessary credentials.")

    try:
        parser = ConfluencePageParser()
        structured_data = parser.get_structured_data_from_page()

        if structured_data:
            print("\n--- Successfully Extracted Structured Data ---")
            # Pretty print the JSON output
            print(json.dumps(structured_data, indent=2))

            # You can now access metadata and individual tables
            # For example, to get columns from the first table:
            if structured_data["tables"]:
                main_table_columns = structured_data["tables"][0]["columns"]
                print("\nExample: Columns from the first table:")
                for col in main_table_columns:
                    print(col)
        else:
            print("\n--- Failed to extract data from Confluence ---")

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error during Confluence API call: {e}")
        print(f"Response content: {e.response.text}")
    except ValueError as e:
        print(f"Configuration Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}. Trace: {e.__traceback__.tb_frame.f_code.co_filename}:{e.__traceback__.tb_lineno}")
