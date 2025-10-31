# confluence_client.py
import requests
from bs4 import BeautifulSoup
from config import ConfluenceConfig, get_confluence_page_title
import os
import json
from urllib.parse import quote
import re 
from collections import deque
import itertools # NEW: For generating title permutations

# Your provided iterative cleaning function
def clean_special_characters_iterative(data):
    """
    Iteratively cleans special characters from strings in a nested structure.
    Handles dicts and lists without recursion.
    """
    if isinstance(data, (str, int, float, bool, type(None))):
        return data
    
    queue = deque([data])

    while queue:
        current = queue.popleft()

        if isinstance(current, dict):
            for key, value in list(current.items()):
                if isinstance(value, (dict, list)):
                    queue.append(value)
                elif isinstance(value, str):
                    try:
                        decoded = value.encode('unicode_escape').decode('latin-1')
                    except UnicodeEncodeError:
                        decoded = value
                    
                    cleaned = re.sub(r'[^\\x00-\\x7F]+', ' ', decoded) 
                    current[key] = re.sub(r'\s+', ' ', cleaned).strip()
        
        elif isinstance(current, list):
            for i in range(len(current)):
                item = current[i]
                if isinstance(item, (dict, list)):
                    queue.append(item)
                elif isinstance(item, str):
                    try:
                        decoded = item.encode('unicode_escape').decode('latin-1')
                    except UnicodeEncodeError:
                        decoded = item
                    cleaned = re.sub(r'[^\\x00-\\x7F]+', ' ', decoded)
                    current[i] = re.sub(r'\s+', ' ', cleaned).strip()
    return data

# Basic HTML text cleaner (still useful for initial extraction from BS4)
def clean_text_from_html_basic(element):
    """
    Extracts text from a BeautifulSoup element, replaces non-breaking spaces
    with regular spaces, and strips whitespace.
    """
    if element is None:
        return ""
    
    text = element.get_text(separator=" ", strip=True) 
    text = text.replace(u'\xa0', u' ')
    text = text.replace('&nbsp;', ' ')
    return text.strip()


class ConfluencePageParser:
    # NEW: Max retries for fuzzy title matching
    MAX_TITLE_SEARCH_RETRIES = 5 # You can adjust this number

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

    # MODIFIED: _get_page_id_by_title now includes retry logic
    def _get_page_id_by_title(self, title):
        search_url = f"{self.base_url}/rest/api/content"
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.api_token}"
        }
        
        # Generator for title variations
        def generate_title_variations(original_title):
            # 1. Original title
            yield original_title

            # 2. Normalize multiple spaces to single spaces
            normalized_spaces_title = " ".join(original_title.split()).strip()
            if normalized_spaces_title != original_title:
                yield normalized_spaces_title
            
            # 3. Normalize colon spacing (e.g., "Table : Title" -> "Table: Title")
            #    Apply this to the already single-spaced title
            normalized_colon_title = normalized_spaces_title.replace(" : ", ": ").replace(":  ", ": ")
            if normalized_colon_title != normalized_spaces_title:
                yield normalized_colon_title
            
            # 4. Try removing all spaces (if page titles can sometimes be found this way)
            no_space_title = original_title.replace(" ", "")
            if no_space_title != original_title:
                yield no_space_title

            # 5. Try adding spaces around colons (e.g., "Table:Title" -> "Table : Title")
            #    This might be useful if the original has no space, but Confluence expects one.
            tokens = re.split(r'(:)', normalized_spaces_title) # Split by colon, keep colon
            spaced_colon_title = ""
            for i, token in enumerate(tokens):
                if token == ':':
                    if i > 0 and not spaced_colon_title.endswith(' '): # Ensure space before if not there
                        spaced_colon_title += ' '
                    spaced_colon_title += token
                    if i < len(tokens) - 1 and not tokens[i+1].startswith(' '): # Ensure space after if not there
                        spaced_colon_title += ' '
                else:
                    spaced_colon_title += token
            spaced_colon_title = " ".join(spaced_colon_title.split()).strip() # Re-normalize general spaces
            if spaced_colon_title != original_title and spaced_colon_title != normalized_spaces_title:
                 yield spaced_colon_title

            # 6. Generate more permutations with varying spaces if necessary (e.g., up to MAX_RETRIES)
            #    For deeper fuzzy matching. This could be complex.
            #    For now, these specific patterns cover common variations.
            #    If MAX_RETRIES is higher, we could generate more complex patterns.

        tried_titles = set()
        
        for attempt_num, current_title_variant in enumerate(generate_title_variations(title)):
            if attempt_num >= self.MAX_TITLE_SEARCH_RETRIES:
                print(f"Reached MAX_TITLE_SEARCH_RETRIES ({self.MAX_TITLE_SEARCH_RETRIES}). Aborting fuzzy search.")
                break # Exit if we hit max retries, even if generator has more

            if current_title_variant in tried_titles:
                continue # Skip if already tried
            tried_titles.add(current_title_variant)

            encoded_title = quote(current_title_variant, safe='') 
            
            params = {
                "title": encoded_title, 
                "spaceKey": self.space_key,
                "expand": "body.storage",
                "limit": 1
            }

            print(f"Attempt {attempt_num + 1}/{self.MAX_TITLE_SEARCH_RETRIES}: Searching for page '{current_title_variant}' in space '{self.space_key}'...")
            try:
                response = requests.get(search_url, headers=headers, params=params)
                response.raise_for_status() # This will raise HTTPError for 4xx/5xx responses
                
                data = response.json()
                if data and data["results"]:
                    page = data["results"][0]
                    print(f"SUCCESS: Found page '{page.get('title', 'N/A')}' with ID: {page['id']} (attempt {attempt_num + 1}).") 
                    return page['id'], page['body']['storage']['value']
            except requests.exceptions.HTTPError as e:
                # 404 Not Found, 401 Unauthorized, etc.
                if e.response.status_code == 404:
                    print(f"INFO: Page '{current_title_variant}' not found (HTTP 404). Trying next variation.")
                else:
                    print(f"WARNING: HTTP error {e.response.status_code} for title '{current_title_variant}'. "
                          f"Content: {e.response.text.strip()} Trying next variation.")
            except Exception as e:
                print(f"ERROR: An unexpected error occurred during API call for '{current_title_variant}': {e}. Trying next variation.")

        print(f"FINAL FAILURE: Page with title '{title}' not found in space '{self.space_key}' after {attempt_num + 1} attempts. "
              f"Please ensure the page title in config.py is exact, the space key is correct, and permissions allow access.")
        return None, None

    # ... (rest of your ConfluencePageParser class and functions, including get_structured_data_from_page)

    def get_structured_data_from_page(self):
        page_id, page_content_html = self._get_page_id_by_title(self.page_title)

        if not page_id or not page_content_html:
            print("Could not retrieve page content. Exiting.")
            return None

        soup = BeautifulSoup(page_content_html, 'html.parser')
        
        structured_page_data = {
            "page_title": self.page_title, # This will be cleaned by the iterative cleaner later
            "page_id": page_id,
            "metadata": {},
            "tables": []
        }

        # --- Extract Page-Level Metadata ---
        def extract_text_metadata(soup_obj, label):
            # Using basic cleaner here, as deep cleaner will run on entire structure
            tag = soup_obj.find(lambda t: t.name in ['p', 'div', 'h1', 'h2', 'h3'] and label in clean_text_from_html_basic(t))
            if tag:
                clean_full_text = clean_text_from_html_basic(tag)
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
            actual_headers_raw_cleaned = [clean_text_from_html_basic(cell) for cell in header_cells]

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
                for h_raw_cleaned in actual_headers_raw_cleaned:
                    h_cleaned_for_map = h_raw_cleaned.replace(' ', '_').replace('?', '').replace('-', '_').lower()
                    current_table_headers_mapping_strategy[h_raw_cleaned] = h_cleaned_for_map 

            parsed_table_data["table_type"] = table_type

            header_indices = {}
            if i == 0:
                for original_header, standardized_key in all_expected_primary_table_headers_map.items():
                    try:
                        header_indices[standardized_key] = actual_headers_raw_cleaned.index(original_header)
                    except ValueError:
                        header_indices[standardized_key] = -1
            else:
                for col_idx, h_raw_cleaned in enumerate(actual_headers_raw_cleaned):
                    h_standardized_key = h_raw_cleaned.replace(' ', '_').replace('?', '').replace('-', '_').lower()
                    header_indices[h_standardized_key] = col_idx


            for row in rows[1:]:
                cols = row.find_all('td')
                if not cols:
                    continue
                
                column_data = {}
                keys_to_process = list(current_table_headers_mapping_strategy.values())
                
                for standardized_key in keys_to_process:
                    idx = header_indices.get(standardized_key, -1) 
                    if idx != -1 and idx < len(cols):
                        value = clean_text_from_html_basic(cols[idx])
                        
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
            display_data = clean_special_characters_iterative(json.loads(json.dumps(structured_data))) 
            print(json.dumps(display_data, indent=2))

            save_structured_data_to_single_file(structured_data)

            print("\n--- Columns for SQL Generation (from PRIMARY table with 'add_to_target=True') ---")
            
            if display_data["tables"]:
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
