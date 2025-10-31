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
        
        # Define ALL expected headers and their standardized keys for the *primary* table structure
        all_expected_primary_table_headers_map = {
            'Source table': 'source_table',
            'Source field name': 'source_field_name', 
            'Add Source To Target?': 'add_to_target', 
            'Target Field name': 'target_field_name',
            'Data type': 'data_type',
            'Decode': 'decode',
            'ADC Transformation': 'adc_transformation',
            'Deprecated': 'deprecated',
            'Primary Key': 'is_primary_key', 
            'Definition': 'definition',
            'proto file': 'proto_file',
            'proto column name': 'proto_column_name',
            'Comments': 'comments'
        }

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

            header_cells = rows[0].find_all(['th', 'td'])
            actual_headers_raw = [cell.get_text(strip=True) for cell in header_cells]

            current_table_headers_map = {}
            table_type = ""

            if i == 0: # This is the first table, use the predefined map
                table_type = "primary_definitions"
                current_table_headers_map = all_expected_primary_table_headers_map
                print(f"Parsing table {table_id} with predefined structure (primary_definitions).")
            else: # Subsequent tables, parse headers dynamically
                table_type = "dynamic_auxiliary"
                print(f"Parsing table {table_id} with dynamic structure (dynamic_auxiliary).")
                for h_raw in actual_headers_raw:
                    # Clean headers for dynamic tables: lowercase, replace spaces/special with underscore
                    h_cleaned = h_raw.replace(' ', '_').replace('?', '').replace('-', '_').lower()
                    current_table_headers_map[h_raw] = h_cleaned # Map raw header to cleaned key

            parsed_table_data["table_type"] = table_type

            # Build header_indices for the current table's parsing logic
            header_indices = {}
            for original_header, standardized_key in current_table_headers_map.items():
                try:
                    # For dynamic tables, original_header is the actual_headers_raw entry
                    # For primary tables, original_header is the key from all_expected_primary_table_headers_map
                    idx = actual_headers_raw.index(original_header if i == 0 else h_raw) # Re-find index
                    if i > 0: # For dynamic tables, we rely on the cleaned name as the key in header_indices
                        header_indices[standardized_key] = idx
                    else: # For primary table, use the standardized key as defined in map
                        header_indices[standardized_key] = idx
                except ValueError:
                    header_indices[standardized_key] = -1

            # Process data rows (skipping the header row)
            for row in rows[1:]:
                cols = row.find_all('td')
                if not cols:
                    continue
                
                column_data = {}
                # Determine which map to iterate for keys: predefined or dynamically generated
                keys_to_process = all_expected_primary_table_headers_map.values() if i == 0 else current_table_headers_map.values()
                
                for standardized_key in keys_to_process:
                    idx = header_indices.get(standardized_key, -1) # Get index for this standardized key
                    if idx != -1 and idx < len(cols):
                        value = cols[idx].get_text(strip=True)
                        
                        # Type conversion for boolean-like fields
                        if standardized_key in ['add_to_target', 'is_primary_key', 'deprecated']:
                            column_data[standardized_key] = (value.lower() == 'yes')
                        else:
                            column_data[standardized_key] = value
                    else:
                        # Assign appropriate default values for missing columns
                        if standardized_key in ['add_to_target', 'is_primary_key', 'deprecated']:
                            column_data[standardized_key] = False
                        else:
                            column_data[standardized_key] = ""
                
                # Only add column data if it has at least a source or target field name for the primary table,
                # or if it's not empty for dynamic tables.
                if i == 0: # Primary table filter
                    if column_data.get('source_field_name') or column_data.get('target_field_name'):
                        parsed_table_data["columns"].append(column_data)
                else: # Dynamic tables, append if it has any meaningful data (not all empty strings/falses)
                    if any(v for k, v in column_data.items() if v not in ["", False, None]):
                         parsed_table_data["columns"].append(column_data)

            structured_page_data["tables"].append(parsed_table_data)
            
        return structured_page_data

def save_structured_data_to_files(structured_data, output_dir="tables"):
    """
    Saves the full structured_data and individual table data to JSON files.
    """
    if not structured_data:
        print("No structured data to save.")
        return

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    page_title_sanitized = structured_data["page_title"].replace(" ", "_").replace(":", "").lower()
    
    # Save the full page data
    full_page_filename = os.path.join(output_dir, f"{page_title_sanitized}_full_page_data.json")
    with open(full_page_filename, 'w', encoding='utf-8') as f:
        json.dump(structured_data, f, indent=2, ensure_ascii=False)
    print(f"Saved full page data to: {full_page_filename}")

    # Save each table's columns data separately
    for table_data in structured_data["tables"]:
        table_filename = os.path.join(output_dir, f"{page_title_sanitized}_{table_data['id']}_columns.json")
        with open(table_filename, 'w', encoding='utf-8') as f:
            # We save the entire table_data dictionary, which includes 'id' and 'table_type'
            json.dump(table_data, f, indent=2, ensure_ascii=False)
        print(f"Saved {table_data['id']} columns to: {table_filename}")


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
            print(json.dumps(structured_data, indent=2)) # Print to console

            save_structured_data_to_files(structured_data) # Save to files

            # --- Example: How you would use this data for SQL generation ---
            if structured_data["tables"]:
                print("\n--- Columns for SQL Generation (from PRIMARY table with 'add_to_target=True') ---")
                
                # Find the primary table
                primary_table_data = next((t for t in structured_data["tables"] if t.get("table_type") == "primary_definitions"), None)

                if primary_table_data:
                    table_metadata = structured_data["metadata"]
                    
                    first_column_source_table = next((c.get('source_table') for c in primary_table_data['columns'] if c.get('source_table')), None)
                    if first_column_source_table:
                        table_metadata['source_table_full_name'] = first_column_source_table
                    else:
                        table_metadata['source_table_full_name'] = "UNKNOWN_SOURCE_TABLE"

                    columns_to_select = [
                        col for col in primary_table_data["columns"] 
                        if col.get("add_to_target")
                    ]

                    if columns_to_select:
                        print("Table Name (from page metadata):", table_metadata.get("table_name"))
                        print("Source Table (derived):", table_metadata.get("source_table_full_name"))
                        print("Selected Columns:")
                        for col in columns_to_select:
                            print(f"  - Source: {col.get('source_field_name')}, "
                                  f"Target: {col.get('target_field_name')}, "
                                  f"Is PK: {col.get('is_primary_key')}, "
                                  f"Data Type: {col.get('data_type')}")
                    else:
                        print("No columns marked 'True' for 'add_to_target' in the primary table.")
                else:
                    print("No primary_definitions table found for SQL generation.")
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
