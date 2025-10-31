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
        # Helper to extract key-value pairs from text, robust to various tags
        def extract_text_metadata(soup_obj, label):
            # Look for strong/b tags within p/div, or direct p/div tags
            tag = soup_obj.find(lambda t: t.name in ['p', 'div', 'h1', 'h2', 'h3'] and label in t.get_text(strip=True))
            if tag:
                clean_text = ' '.join(tag.stripped_strings)
                parts = clean_text.split(label, 1) # Split only once
                if len(parts) > 1:
                    value = parts[1].strip()
                    # Remove "Historization: SCD-2" if present in Database name
                    if label == "Database name:" and "Historization: SCD-2" in value:
                        value = value.replace("Historization: SCD-2", "").strip()
                    return value
            return None

        # Populate metadata directly from page text
        structured_page_data["metadata"]["table_name"] = extract_text_metadata(soup, "Table name:")
        structured_page_data["metadata"]["schema_name"] = extract_text_metadata(soup, "Schema name:")
        structured_page_data["metadata"]["database_name"] = extract_text_metadata(soup, "Database name:")

        pk_text = extract_text_metadata(soup, "Primary Keys:")
        structured_page_data["metadata"]["primary_keys"] = [k.strip() for k in pk_text.split(',') if k.strip()] if pk_text else []

        fk_text = extract_text_metadata(soup, "Foreign Keys:")
        structured_page_data["metadata"]["foreign_keys"] = [k.strip() for k in fk_text.split(',') if k.strip()] if fk_text else []

        # Fallback for table_name
        if not structured_page_data["metadata"].get("table_name"):
             structured_page_data["metadata"]["table_name"] = self.page_title.replace("Table: ", "").strip()
        
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

            # Robust header extraction: Check for 'th' first, then default to 'td'
            header_cells = rows[0].find_all(['th', 'td'])
            headers = [cell.get_text(strip=True) for cell in header_cells]

            # Define headers we specifically need for SQL generation and internal processing.
            # Including 'Primary Key' now.
            required_output_headers_map = {
                'Source table': 'source_table',
                'Source field name': 'source_field_name', 
                'Add Source To Target?': 'add_to_target', 
                'Target Field name': 'target_field_name',
                'Data type': 'data_type', # Adding this for completeness
                'Primary Key': 'is_primary_key' # Adding this specific column
            }
            
            # Find indices for the headers we care about
            header_indices = {}
            found_headers_for_table = []
            for req_header_orig, req_header_standardized in required_output_headers_map.items():
                try:
                    idx = headers.index(req_header_orig)
                    header_indices[req_header_standardized] = idx
                    found_headers_for_table.append(req_header_orig)
                except ValueError:
                    # If a required header isn't found, store -1 or handle as needed
                    header_indices[req_header_standardized] = -1
            
            # Warn if critical headers are missing
            critical_headers = ['Source field name', 'Target Field name']
            if not all(h in found_headers_for_table for h in critical_headers):
                 print(f"Warning: Table {table_id} is missing critical headers: {critical_headers}. Extracted headers: {headers}. This table might not be suitable for SQL generation.")
                 # Decide if you want to skip this table entirely if critical headers are missing
                 # For now, we'll continue but the column data will be incomplete.


            # Process data rows (skipping the header row)
            for row in rows[1:]:
                cols = row.find_all('td')
                if not cols: # Skip empty rows
                    continue
                
                column_data = {}
                for standardized_name, idx in header_indices.items():
                    if idx != -1 and idx < len(cols):
                        value = cols[idx].get_text(strip=True)
                        # Specific handling for boolean-like fields
                        if standardized_name == 'add_to_target' or standardized_name == 'is_primary_key':
                            column_data[standardized_name] = (value.lower() == 'yes')
                        else:
                            column_data[standardized_name] = value
                    else:
                        # Assign a default empty/false value if column is missing
                        if standardized_name == 'add_to_target' or standardized_name == 'is_primary_key':
                            column_data[standardized_name] = False
                        else:
                            column_data[standardized_name] = ""
                
                # Only add column data if it has a source and target field name
                if column_data.get('source_field_name') and column_data.get('target_field_name'):
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

            # You can now proceed to SQL generation using this structured_data
            if structured_data["tables"]:
                print("\n--- Columns for SQL Generation (from first table with 'add_to_target=True') ---")
                
                # Assuming the first table is the one of interest for SQL generation
                main_table_data = structured_data["tables"][0] 
                
                # Extracting table-level metadata for SQL generation
                table_metadata = structured_data["metadata"]
                
                # Let's refine the source_table_full_name dynamically if possible
                # If there's a source_table in the first column, use that.
                first_column_source_table = next((c['source_table'] for c in main_table_data['columns'] if c.get('source_table')), None)
                if first_column_source_table:
                    table_metadata['source_table_full_name'] = first_column_source_table
                
                columns_to_select = [
                    col for col in main_table_data["columns"] 
                    if col.get("add_to_target")
                ]

                if columns_to_select:
                    # Now we have all the pieces for SQL generation!
                    # Next step would be to pass `table_metadata` and `columns_to_select`
                    # to a SQL generation function.

                    print("Table Name (from page metadata):", table_metadata.get("table_name"))
                    print("Source Table (from first column):", table_metadata.get("source_table_full_name"))
                    print("Selected Columns:")
                    for col in columns_to_select:
                        print(f"  - Source: {col['source_field_name']}, Target: {col['target_field_name']}, Is PK: {col['is_primary_key']}")
                else:
                    print("No columns marked 'Yes' for 'Add Source To Target?' in the first table.")
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
