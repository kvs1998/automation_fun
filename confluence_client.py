# confluence_client.py
import requests
from bs4 import BeautifulSoup
from config import ConfluenceConfig, get_confluence_page_title
import os
import json

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

    def get_structured_data_from_page(self):
        page_id, page_content_html = self._get_page_id_by_title(self.page_title)

        if not page_id or not page_content_html:
            print("Could not retrieve page content. Exiting.")
            return None

        soup = BeautifulSoup(page_content_html, 'html.parser')
        
        structured_page_data = {
            "page_title": self.page_title,
            "page_id": page_id,
            "metadata": {},
            "tables": []
        }

        # --- Extract Page-Level Metadata ---
        def extract_text_metadata(soup_obj, label):
            tag = soup_obj.find(lambda t: t.name in ['p', 'div', 'h1', 'h2', 'h3'] and label in t.get_text(strip=True))
            if tag:
                clean_text = ' '.join(tag.stripped_strings)
                parts = clean_text.split(label, 1)
                if len(parts) > 1:
                    value = parts[1].strip()
                    if label == "Database name:" and "Historization: SCD-2" in value:
                        value = value.replace("Historization: SCD-2", "").strip()
                    return value
            return None

        structured_page_data["metadata"]["table_name"] = extract_text_metadata(soup, "Table name:")
        structured_page_data["metadata"]["schema_name"] = extract_text_metadata(soup, "Schema name:")
        structured_page_data["metadata"]["database_name"] = extract_text_metadata(soup, "Database name:")

        pk_text = extract_text_metadata(soup, "Primary Keys:")
        structured_page_data["metadata"]["primary_keys"] = [k.strip() for k in pk_text.split(',') if k.strip()] if pk_text else []

        fk_text = extract_text_metadata(soup, "Foreign Keys:")
        structured_page_data["metadata"]["foreign_keys"] = [k.strip() for k in fk_text.split(',') if k.strip()] if fk_text else []

        if not structured_page_data["metadata"].get("table_name"):
             structured_page_data["metadata"]["table_name"] = self.page_title.replace("Table: ", "").strip()
        
        # --- Extract Table Data (Iterate through all tables) ---
        all_html_tables = soup.find_all('table')
        if not all_html_tables:
            print("No tables found on the Confluence page.")
            return structured_page_data

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

            # Robust header extraction: Check for 'th' first, then default to 'td'
            header_cells = rows[0].find_all(['th', 'td'])
            
            # Use a clean version of headers for dictionary keys (e.g., replace spaces, lowercase)
            headers_raw = [cell.get_text(strip=True) for cell in header_cells]
            headers_cleaned = [
                h.replace(' ', '_').replace('?', '').replace('-', '_').lower() 
                for h in headers_raw
            ]

            # Process data rows (skipping the header row)
            for row in rows[1:]:
                cols = row.find_all('td')
                if not cols:
                    continue
                
                column_data = {}
                for col_idx, cell in enumerate(cols):
                    if col_idx < len(headers_cleaned): # Ensure we have a corresponding header
                        header_key = headers_cleaned[col_idx]
                        value = cell.get_text(strip=True)
                        
                        # Specific handling for boolean-like fields if they match expected names
                        if header_key in ['add_source_to_target', 'primary_key', 'deprecated']:
                            column_data[header_key] = (value.lower() == 'yes')
                        else:
                            column_data[header_key] = value
                    # Else: ignore columns that don't have a corresponding header (shouldn't happen with well-formed tables)
                
                # Only add column data if it has at least a source or target field name to be meaningful
                if column_data.get('source_field_name') or column_data.get('target_field_name'):
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
            print(json.dumps(structured_data, indent=2))

            if structured_data["tables"]:
                # Assuming the first table is the main one for SQL generation
                main_table_data = structured_data["tables"][0] 
                table_metadata = structured_data["metadata"]
                
                # Try to dynamically set source_table_full_name if it wasn't caught in page metadata
                first_column_source_table = next((c.get('source_table') for c in main_table_data['columns'] if c.get('source_table')), None)
                if first_column_source_table:
                    table_metadata['source_table_full_name'] = first_column_source_table
                else:
                    table_metadata['source_table_full_name'] = "UNKNOWN_SOURCE_TABLE" # Fallback if no source_table in columns

                print("\n--- Columns for SQL Generation (from first table with 'add_source_to_target=True') ---")
                
                columns_to_select = [
                    col for col in main_table_data["columns"] 
                    if col.get("add_source_to_target")
                ]

                if columns_to_select:
                    print("Table Name (from page metadata):", table_metadata.get("table_name"))
                    print("Source Table (derived):", table_metadata.get("source_table_full_name"))
                    print("Selected Columns:")
                    for col in columns_to_select:
                        print(f"  - Source: {col.get('source_field_name')}, "
                              f"Target: {col.get('target_field_name')}, "
                              f"Is PK in table: {col.get('primary_key')}, "
                              f"Data Type: {col.get('data_type')}")
                else:
                    print("No columns marked 'True' for 'add_source_to_target' in the first table.")
            else:
                print("No tables found in the structured data.")
        else:
            print("\n--- Failed to extract data from Confluence ---")

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error during Confluence API call: {e}")
        print(f"Response content: {e.response.text}")
    except ValueError as e:
        print(f"Configuration Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}. Trace: {e.__traceback__.tb_frame.f_code.co_filename}:{e.__traceback__.tb_lineno}")
