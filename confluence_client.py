# confluence_client.py
import requests
from bs4 import BeautifulSoup
from config import ConfluenceConfig, get_confluence_page_title
import os
import json
from datetime import datetime

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


    def _get_page_id_and_content_html(self, title):
        """
        Retrieves the page ID and content (in storage format) for a given page title.
        """
        search_url = f"{self.base_url}/rest/api/content"
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.api_token}"
        }
        params = {
            "title": title,
            "spaceKey": self.space_key,
            "expand": "body.storage", # Request storage format content
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
        page_id, page_content_html = self._get_page_id_and_content_html(self.page_title)

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

        # --- Helper to extract key-value pairs from content HTML ---
        def extract_text_metadata_from_content(soup_obj, label_start_str):
            # Confluence Storage Format often wraps text like "Author: Chris Lee" in <p> or <div>.
            # We look for the raw text content to contain the label_start_str.
            target_element = soup_obj.find(lambda t: t.name in ['p', 'div', 'strong', 'em', 'span'] and label_start_str in t.get_text(strip=True))
            if target_element:
                # Use .stripped_strings to get clean text, handling potential mixed tags
                full_text = ' '.join(target_element.stripped_strings).strip()
                if label_start_str in full_text:
                    value = full_text.split(label_start_str, 1)[1].strip()
                    # Clean up date if possible
                    if label_start_str == "Last Changed by:": # Expecting "Name Date"
                        parts = value.rsplit(' ', 2) # Split from right, max 2 times to get possible date
                        if len(parts) >= 2:
                            name = ' '.join(parts[:-1]) # Name part
                            date_str = parts[-1]        # Date part
                            
                            try: # Try specific Confluence date formats
                                # Example: Sep 11, 2023 or Oct 28, 2025
                                parsed_date = datetime.strptime(date_str, '%b %d, %Y')
                                return name, parsed_date.isoformat()
                            except ValueError:
                                return name, date_str # Fallback if date parsing fails
                        return value, None # No specific date part found
                    return value
            return None

        # --- Extract Page-Level Metadata from the content HTML ---
        # Assuming these are typically in <p> tags at the top of the content
        
        # Table Name, Schema, Database, Primary/Foreign Keys (from previous logic)
        def extract_text_metadata_for_labels(soup_obj, label): # Renamed to avoid clash
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

        structured_page_data["metadata"]["table_name"] = extract_text_metadata_for_labels(soup, "Table name:")
        structured_page_data["metadata"]["schema_name"] = extract_text_metadata_for_labels(soup, "Schema name:")
        structured_page_data["metadata"]["database_name"] = extract_text_metadata_for_labels(soup, "Database name:")

        pk_text = extract_text_metadata_for_labels(soup, "Primary Keys:")
        structured_page_data["metadata"]["primary_keys"] = [k.strip() for k in pk_text.split(',') if k.strip()] if pk_text else []

        fk_text = extract_text_metadata_for_labels(soup, "Foreign Keys:")
        structured_page_data["metadata"]["foreign_keys"] = [k.strip() for k in fk_text.split(',') if k.strip()] if fk_text else []

        if not structured_page_data["metadata"].get("table_name"):
             structured_page_data["metadata"]["table_name"] = self.page_title.replace("Table: ", "").strip()

        # Extract Author and Last Changed Info from 'Created by' and 'last modified on' lines
        # These are usually at the very top of the Confluence Storage Format
        created_by_tag = soup.find('p', class_='smalltext') # Often "Created by X, last modified on Y"
        if created_by_tag:
            text = created_by_tag.get_text(strip=True)
            # Example: "Created by Chris Lee, last modified on Oct 28, 2025"
            if "Created by" in text:
                author_part = text.split("Created by", 1)[1].strip()
                if "last modified on" in author_part:
                    author_name = author_part.split(", last modified on", 1)[0].strip()
                    modified_date_str = author_part.split(", last modified on", 1)[1].strip()
                    
                    structured_page_data["metadata"]["page_author"] = author_name
                    structured_page_data["metadata"]["page_last_changed_by"] = author_name # Assuming same person
                    try:
                        structured_page_data["metadata"]["page_last_changed_date"] = datetime.strptime(modified_date_str, '%b %d, %Y').isoformat()
                    except ValueError:
                        structured_page_data["metadata"]["page_last_changed_date"] = modified_date_str # Fallback
                else:
                    structured_page_data["metadata"]["page_author"] = author_part.strip()
                    structured_page_data["metadata"]["page_last_changed_by"] = author_part.strip()
                    structured_page_data["metadata"]["page_last_changed_date"] = None # No date found


        # Define ALL expected headers and their standardized keys for the *first* table structure
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
            print("No tables found on the Confluence page content.")
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

            current_table_headers_mapping_strategy = {}
            table_type = ""

            if i == 0: # First table: Use the predefined map
                table_type = "primary_definitions"
                current_table_headers_mapping_strategy = {
                    h_orig: h_std for h_orig, h_std in all_expected_primary_table_headers_map.items()
                }
                print(f"Parsing table {table_id} with predefined structure (primary_definitions).")
            else: # Subsequent tables: Dynamically generate map
                table_type = "dynamic_auxiliary"
                print(f"Parsing table {table_id} with dynamic structure (dynamic_auxiliary).")
                for h_raw in actual_headers_raw:
                    h_cleaned = h_raw.replace(' ', '_').replace('?', '').replace('-', '_').lower()
                    current_table_headers_mapping_strategy[h_raw] = h_cleaned

            parsed_table_data["table_type"] = table_type

            header_indices = {}
            if i == 0:
                for original_header, standardized_key in all_expected_primary_table_headers_map.items():
                    try:
                        header_indices[standardized_key] = actual_headers_raw.index(original_header)
                    except ValueError:
                        header_indices[standardized_key] = -1
            else:
                for col_idx, h_raw in enumerate(actual_headers_raw):
                    h_cleaned = h_raw.replace(' ', '_').replace('?', '').replace('-', '_').lower()
                    header_indices[h_cleaned] = col_idx


            for row in rows[1:]:
                cols = row.find_all('td')
                if not cols:
                    continue
                
                column_data = {}
                keys_to_process = list(current_table_headers_mapping_strategy.values())
                
                for standardized_key in keys_to_process:
                    idx = header_indices.get(standardized_key, -1) 
                    if idx != -1 and idx < len(cols):
                        value = cols[idx].get_text(strip=True)
                        
                        if standardized_key in ['add_to_target', 'is_primary_key', 'deprecated']:
                            column_data[standardized_key] = (value.lower() == 'yes')
                        else:
                            column_data[standardized_key] = value
                    else:
                        if standardized_key in ['add_to_target', 'is_primary_key', 'deprecated']:
                            column_data[standardized_key] = False
                        else:
                            column_data[standardized_key] = ""
                
                if i == 0:
                    if column_data.get('source_field_name') or column_data.get('target_field_name'):
                        parsed_table_data["columns"].append(column_data)
                else:
                    if any(v for k, v in column_data.items() if v not in ["", False, None]):
                         parsed_table_data["columns"].append(column_data)

            structured_page_data["tables"].append(parsed_table_data)
            
        return structured_page_data

def save_structured_data_to_single_file(structured_data, output_dir="tables"):
    if not structured_data:
        print("No structured data to save.")
        return

    os.makedirs(output_dir, exist_ok=True)

    table_name_for_file = structured_data["metadata"].get("table_name", "untitled_table").replace(" ", "_").lower()
    
    filename = os.path.join(output_dir, f"{table_name_for_file}.json")
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(structured_data, f, indent=2, ensure_ascii=False)
    print(f"Saved full page data to: {filename}")


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

            save_structured_data_to_single_file(structured_data)

            if structured_data["tables"]:
                print("\n--- Columns for SQL Generation (from PRIMARY table with 'add_to_target=True') ---")
                
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
