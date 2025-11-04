# confluence_utils.py (Full code for this file)
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote
import re 
from collections import deque
from datetime import datetime
import json # For handling labels as JSON string


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

# Basic HTML text cleaner
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
    MAX_TITLE_SEARCH_RETRIES = 5
    # Define common expand parameters for detailed metadata (NO body.storage here)
    EXPAND_METADATA_PARAMS = "history.createdBy,history.createdDate,history.lastUpdated.by,history.lastUpdated.when,metadata.labels,ancestors"

    def __init__(self, base_url, api_token, space_key):
        self.base_url = base_url
        self.api_token = api_token
        self.space_key = space_key
        
        if not all([self.base_url, self.api_token, self.space_key]):
            raise ValueError(
                "Confluence configuration is incomplete. "
                "Please ensure base_url, api_token, and space_key are provided."
            )

    def find_page_by_title(self, title): 
        search_url = f"{self.base_url}/rest/api/content"
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.api_token}"
        }
        
        def generate_title_variations(original_title):
            yield original_title
            normalized_spaces_title = " ".join(original_title.split()).strip()
            if normalized_spaces_title != original_title: yield normalized_spaces_title
            
            normalized_colon_title = normalized_spaces_title.replace(" : ", ": ").replace(":  ", ": ")
            if normalized_colon_title != normalized_spaces_title: yield normalized_colon_title
            
            no_space_title = original_title.replace(" ", "")
            if no_space_title != original_title: yield no_space_title

            tokens = re.split(r'(:)', normalized_spaces_title)
            spaced_colon_title = ""
            for i, token in enumerate(tokens):
                if token == ':':
                    if i > 0 and not spaced_colon_title.endswith(' '): spaced_colon_title += ' '
                    spaced_colon_title += token
                    if i < len(tokens) - 1 and not tokens[i+1].startswith(' '): spaced_colon_title += ' '
                else: spaced_colon_title += token
            spaced_colon_title = " ".join(spaced_colon_title.split()).strip()
            if spaced_colon_title != original_title and spaced_colon_title != normalized_spaces_title and spaced_colon_title != normalized_colon_title:
                 yield spaced_colon_title

        tried_titles = set()
        
        attempts_made = 0 

        for attempt_num, current_title_variant in enumerate(generate_title_variations(title)):
            attempts_made = attempt_num + 1

            if attempts_made > self.MAX_TITLE_SEARCH_RETRIES:
                return {
                    "status": "MISS", 
                    "found_title": None, 
                    "page_id": None, 
                    "notes": f"Reached max retries ({self.MAX_TITLE_SEARCH_RETRIES}) for title variations.",
                    "attempts_made": attempts_made - 1
                }

            if current_title_variant in tried_titles:
                continue
            tried_titles.add(current_title_variant)
            
            params = {
                "title": current_title_variant,
                "spaceKey": self.space_key,
                "limit": 1
            }

            print(f"Attempt {attempts_made}/{self.MAX_TITLE_SEARCH_RETRIES}: Searching for page '{current_title_variant}'...")
            try:
                response = requests.get(search_url, headers=headers, params=params)
                response.raise_for_status()
                
                data = response.json()
                if data and data["results"]:
                    page = data["results"][0]
                    found_title = page.get('title', current_title_variant)
                    print(f"SUCCESS: Found page '{found_title}' with ID: {page['id']} (attempt {attempts_made}).") 
                    return {
                        "status": "HIT",
                        "found_title": found_title,
                        "page_id": page['id'],
                        "notes": f"Matched using variation: '{current_title_variant}'",
                        "attempts_made": attempts_made
                    }
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    print(f"INFO: Page '{current_title_variant}' not found (HTTP 404). Trying next variation.")
                else:
                    print(f"WARNING: HTTP error {e.response.status_code} for title '{current_title_variant}'. "
                          f"Content: {e.response.text.strip()} Trying next variation.")
            except Exception as e:
                print(f"ERROR: An unexpected error occurred during API call for '{current_title_variant}': {e}. Trying next variation.")

        return {
            "status": "MISS", 
            "found_title": None, 
            "page_id": None, 
            "notes": f"Page not found after all {attempts_made} variations.",
            "attempts_made": attempts_made
        }

    # NEW: Method to fetch ONLY expanded page metadata (no body.storage)
    def get_expanded_page_metadata(self, page_id):
        """
        Fetches ONLY expanded metadata for a given page ID.
        """
        content_url = f"{self.base_url}/rest/api/content/{page_id}"
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.api_token}"
        }
        params = {
            "expand": self.EXPAND_METADATA_PARAMS
        }

        print(f"Fetching expanded metadata for page ID: {page_id}...")
        response = requests.get(content_url, headers=headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        metadata = {
            "api_title": data.get('title'),
            "api_type": data.get('type'),
            "api_status": data.get('status'),
            # REMOVED: "author_display_name": None, (No longer relevant/populated)
            # REMOVED: "author_username": None,
        }

        history = data.get('history', {})
        created_by = history.get('createdBy', {})
        last_updated_by = history.get('lastUpdated', {}).get('by', {})
        last_updated_when = history.get('lastUpdated', {}).get('when')

        metadata.update({
            "created_by_display_name": created_by.get('displayName'),
            "created_by_username": created_by.get('username'),
            "created_date": history.get('createdDate'),
            "last_modified_by_display_name": last_updated_by.get('displayName'),
            "last_modified_by_username": last_updated_by.get('username'),
            "last_modified_date": last_updated_when,
            "parent_page_title": None,
            "parent_page_id": None,
            "labels": []
        })
        
        labels_data = data.get('metadata', {}).get('labels', {}).get('results', [])
        metadata['labels'] = [label.get('name') for label in labels_data if label.get('name')]

        ancestors = data.get('ancestors', [])
        if ancestors:
            parent = ancestors[-1] 
            metadata['parent_page_title'] = parent.get('title')
            metadata['parent_page_id'] = parent.get('id')

        return metadata


    # This method remains unchanged for now, will be used in Stage 3
    def get_structured_data_from_html(self, page_id, page_title_for_struct, page_content_html):
        """
        Parses the Confluence page content HTML into a structured dictionary of tables and columns.
        This function expects raw HTML content and does not make API calls.
        """
        soup = BeautifulSoup(page_content_html, 'html.parser')
        
        structured_page_data = {
            "page_title": page_title_for_struct,
            "page_id": page_id,
            "metadata": {}, 
            "tables": []
        }

    # MODIFIED: get_structured_data_from_html for full dynamic parsing
    def get_structured_data_from_html(self, page_id, page_title_for_struct, page_content_html):
        """
        Parses the Confluence page content HTML into a structured dictionary of tables and columns.
        This function now dynamically parses all tables found on the page based on their headers.
        """
        soup = BeautifulSoup(page_content_html, 'html.parser')
        
        structured_page_data = {
            "page_title": page_title_for_struct,
            "page_id": page_id,
            "metadata": {}, # This will hold table-specific metadata extracted from HTML
            "tables": []
        }

        # --- Extract Page-Level (table-specific) Metadata from content HTML ---
        # This part still extracts specific labels IF they exist in the HTML content
        def extract_text_metadata(soup_obj, label):
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

        # Fallback if no table_name found in content
        if not structured_page_data["metadata"].get("table_name"):
             structured_page_data["metadata"]["table_name"] = page_title_for_struct.replace("Table: ", "").strip()
        
        # --- NEW: NO predefined header map ---
        # all_expected_primary_table_headers_map is removed.

        # --- Extract Table Data (Iterate through all tables, ALL dynamically) ---
        all_html_tables = soup.find_all('table')
        if not all_html_tables:
            print("No tables found on the Confluence page content.")
            return structured_page_data

        for i, html_table in enumerate(all_html_tables):
            table_id = f"table_{i+1}"
            parsed_table_data = {
                "id": table_id,
                "table_type": "dynamically_parsed", # All tables are now dynamic
                "columns": []
            }
            
            rows = html_table.find_all('tr')
            if not rows:
                print(f"Table {table_id} has no rows. Skipping.")
                continue

            header_cells = rows[0].find_all(['th', 'td'])
            actual_headers_raw_cleaned = [clean_text_from_html_basic(cell) for cell in header_cells]

            # --- NEW: Dynamic Header Mapping Strategy for ALL tables ---
            # Maps raw (but cleaned) header text to standardized (cleaned, lower, underscored) keys
            current_table_headers_mapping = {}
            for h_raw_cleaned in actual_headers_raw_cleaned:
                h_standardized_key = h_raw_cleaned.replace(' ', '_').replace('?', '').replace('-', '_').lower()
                current_table_headers_mapping[h_raw_cleaned] = h_standardized_key 
            
            # Build header_indices: map standardized key to its column index
            header_indices = {}
            for col_idx, h_raw_cleaned in enumerate(actual_headers_raw_cleaned):
                h_standardized_key = current_table_headers_mapping[h_raw_cleaned]
                header_indices[h_standardized_key] = col_idx


            for row in rows[1:]: # Skip header row
                cols = row.find_all('td')
                if not cols:
                    continue
                
                column_data = {}
                keys_to_process = list(current_table_headers_mapping.values()) # Standardized keys
                
                for standardized_key in keys_to_process:
                    idx = header_indices.get(standardized_key, -1) 
                    if idx != -1 and idx < len(cols):
                        value = clean_text_from_html_basic(cols[idx])
                        
                        # Apply boolean conversion for specific known keywords, if they exist
                        if standardized_key in ['add_to_target', 'is_primary_key', 'deprecated']:
                            column_data[standardized_key] = (value.lower() == 'yes')
                        else:
                            column_data[standardized_key] = value
                    else:
                        # Assign default values for missing columns based on type
                        if standardized_key in ['add_to_target', 'is_primary_key', 'deprecated']:
                            column_data[standardized_key] = False
                        else:
                            column_data[standardized_key] = ""
                
                # Append if it has any meaningful data (not all empty strings/falses)
                if any(v for k, v in column_data.items() if v not in ["", False, None]):
                     parsed_table_data["columns"].append(column_data)

            structured_page_data["tables"].append(parsed_table_data)
            
        return structured_page_data
