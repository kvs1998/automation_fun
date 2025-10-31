# confluence_client.py
import requests
from bs4 import BeautifulSoup
from config import ConfluenceConfig, get_confluence_page_title
import os
import json
from urllib.parse import quote
# Removed unicodedata import as its core function is now in clean_special_characters_iterative
import re # Already needed for filename sanitization, also used in your cleaner
from collections import deque # NEW: For your iterative cleaner

# NEW: Your iterative cleaning function
def clean_special_characters_iterative(data):
    """
    Iteratively cleans special characters from strings in a nested structure.
    Handles dicts and lists without recursion.
    """
    if isinstance(data, (str, int, float, bool, type(None))): # Handle simple types directly
        return data
    
    # Use deque for iterative processing
    queue = deque([data])

    while queue:
        current = queue.popleft()

        if isinstance(current, dict):
            for key, value in current.items():
                if isinstance(value, (dict, list)):
                    queue.append(value)
                elif isinstance(value, str):
                    # Use unicode_escape to convert non-ASCII chars to their escape sequences
                    # then remove anything that is not standard ASCII after decoding back
                    decoded = value.encode('unicode_escape').decode('latin-1') # Use latin-1 for single byte
                    # The pattern matches any character outside the basic ASCII range (0x00-0x7F)
                    cleaned = re.sub(r'[^\\x00-\\x7F]+', ' ', decoded) 
                    # Re-decode to ensure it's proper string, then strip and replace multi-spaces
                    current[key] = re.sub(r'\s+', ' ', cleaned).strip() 
        
        elif isinstance(current, list):
            for i in range(len(current)):
                item = current[i]
                if isinstance(item, (dict, list)):
                    queue.append(item)
                elif isinstance(item, str):
                    decoded = item.encode('unicode_escape').decode('latin-1')
                    cleaned = re.sub(r'[^\\x00-\\x7F]+', ' ', decoded)
                    current[i] = re.sub(r'\s+', ' ', cleaned).strip()
    return data


# MODIFIED: Simplified clean_text_from_html.
# This now focuses on basic get_text and initial space handling,
# with the deep cleaning delegated to clean_special_characters_iterative.
def clean_text_from_html(element):
    """
    Extracts text from a BeautifulSoup element, replaces non-breaking spaces
    with regular spaces, and strips whitespace.
    The deep cleaning for non-ASCII chars is handled later by
    clean_special_characters_iterative.
    """
    if element is None:
        return ""
    
    text = element.get_text(separator=" ", strip=True) 
    
    # Replace common non-breaking space characters with regular spaces
    text = text.replace(u'\xa0', u' ')
    text = text.replace('&nbsp;', ' ')

    return text.strip()


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
        
        encoded_title = quote(title, safe='') 
        
        params = {
            "title": encoded_title, 
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
            print(f"Found page '{page.get('title', 'N/A')}' with ID: {page['id']}") 
            return page['id'], page['body']['storage']['value']
        else:
            print(f"Page with title '{title}' not found in space '{self.space_key}'. "
                  f"Please ensure the page title in config.py is exact and case-sensitive.")
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
            # Use clean_text_from_html for text within the element
            tag = soup_obj.find(lambda t: t.name in ['p', 'div', 'h1', 'h2', 'h3'] and label in clean_text_from_html(t))
            if tag:
                clean_full_text = clean_text_from_html(tag)
                parts = clean_full_text.split(label, 1)
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
            # Apply basic cleaning to raw headers after extraction
            actual_headers_raw_cleaned = [clean_text_from_html(cell) for cell in header_cells]

            # Determine the header mapping strategy
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
                for h_raw_cleaned in actual_headers_raw_cleaned: # Already basic cleaned
                    h_cleaned_for_map = h_raw_cleaned.replace(' ', '_').replace('?', '').replace('-', '_').lower()
                    current_table_headers_mapping_strategy[h_raw_cleaned] = h_cleaned_for_map

            parsed_table_data["table_type"] = table_type

            # Build header_indices for the current table's parsing logic
            header_indices = {}
            if i == 0:
                for original_header, standardized_key in all_expected_primary_table_headers_map.items():
                    try:
                        header_indices[standardized_key] = actual_headers_raw_cleaned.index(original_header)
                    except ValueError:
                        header_indices[standardized_key] = -1
            else:
                for col_idx, h_raw_cleaned in enumerate(actual_headers_raw_cleaned):
                    h_cleaned_for_map = h_raw_cleaned.replace(' ', '_').replace('?', '').replace('-', '_').lower()
                    header_indices[h_cleaned_for_map] = col_idx


            for row in rows[1:]:
                cols = row.find_all('td')
                if not cols:
                    continue
                
                column_data = {}
                keys_to_process = list(current_table_headers_mapping_strategy.values())
                
                for standardized_key in keys_to_process:
                    idx = header_indices.get(standardized_key, -1) 
                    if idx != -1 and idx < len(cols):
                        value = clean_text_from_html(cols[idx]) # Apply basic cleaning to cell content
                        
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
    """
    Saves the full structured_data to a single JSON file.
    The filename is derived from the extracted table_name metadata.
    """
    if not structured_data:
        print("No structured data to save.")
        return

    os.makedirs(output_dir, exist_ok=True)

    table_name_raw = structured_data["metadata"].get("table_name", "untitled_table")
    table_name_for_file = table_name_raw.lower().replace(" ", "_")
    table_name_for_file = re.sub(r'[^a-z0-9_.-]', '', table_name_for_file)
    
    filename = os.path.join(output_dir, f"{table_name_for_file}.json")
    with open(filename, 'w', encoding='utf-8') as f:
        # NEW: Apply the deep cleaning here before saving
        cleaned_structured_data = clean_special_characters_iterative(structured_data)
        json.dump(cleaned_structured_data, f, indent=2, ensure_ascii=False)
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
            # print(json.dumps(structured_data, indent=2)) # No longer printing raw data

            save_structured_data_to_single_file(structured_data) # Save to a single file

            # For console output, let's print the *cleaned* version
            # (assuming primary_table_data will be clean after save_structured_data_to_single_file)
            print("\n--- Displaying Cleaned & Filtered Data for SQL Generation ---")
            
            # Reload the cleaned data or get a copy if we want to print it
            # For simplicity, we'll work with the 'structured_data' object
            # and just keep in mind it's being deep cleaned before save.
            # To *display* cleaned data, we'd need to clean it *before* this print block.
            # Let's call the cleaner on a copy for display.
            display_data = clean_special_characters_iterative(json.loads(json.dumps(structured_data))) # Deep copy and clean for display

            if display_data["tables"]:
                print("\n--- Columns for SQL Generation (from PRIMARY table with 'add_to_target=True') ---")
                
                primary_table_data = next((t for t in display_data["tables"] if t.get("table_type") == "primary_definitions"), None)

                if primary_table_data:
                    table_metadata = display_data["metadata"]
                    
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
