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
        
        # Confluence viewinfo page URL structure
        # We need the page_id which we get from the content API
        self.page_info_base_url = f"{self.base_url}/pages/viewinfo.action"


    def _get_page_id_by_title(self, title):
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

    def _get_page_information_html(self, page_id):
        """
        Fetches the HTML content from the /pages/viewinfo.action page.
        """
        page_info_url = f"{self.page_info_base_url}?pageId={page_id}"
        headers = {
            "Accept": "text/html",
            "Authorization": f"Bearer {self.api_token}" # Use Bearer token even for HTML pages if needed
        }
        print(f"Fetching page information from: {page_info_url}...")
        response = requests.get(page_info_url, headers=headers)
        response.raise_for_status()
        return response.text

    def _parse_page_information_html(self, html_content):
        """
        Parses the HTML from the page information page to extract metadata.
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        info = {}

        # Look for the .table-section or similar wrapper that contains the info
        # This part is highly dependent on the Confluence UI structure.
        # Based on screenshot, it's typically a definition list (dl) or a table structure.
        # Let's target the dl.
        
        # Find the main information panel, typically a div with class 'wiki-content' or similar
        # or a direct table if the info is laid out that way.
        # From screenshot, it looks like div with dl elements.
        main_info_div = soup.find('div', class_='wiki-content') # or other more specific class
        if not main_info_div:
            # Fallback for simpler structure or if wiki-content isn't primary wrapper
            main_info_div = soup 
            
        # Extract Title (can also be from content API, but good to cross-check)
        title_tag = main_info_div.find('th', string='Title:')
        if title_tag and title_tag.find_next_sibling('td'):
            info['page_title_from_info_page'] = title_tag.find_next_sibling('td').get_text(strip=True)

        # Extract Author
        author_tag = main_info_div.find('th', string='Author:')
        if author_tag and author_tag.find_next_sibling('td'):
            info['page_author'] = author_tag.find_next_sibling('td').get_text(strip=True)

        # Extract Last Changed By and Last Changed Date
        last_changed_by_tag = main_info_div.find('th', string='Last Changed by:')
        if last_changed_by_tag and last_changed_by_tag.find_next_sibling('td'):
            td_content = last_changed_by_tag.find_next_sibling('td')
            # The structure is 'Chris Lee Oct 28, 2025' or similar, separated by a line break often
            parts = [s.strip() for s in td_content.stripped_strings if s.strip()]
            if len(parts) >= 2:
                info['page_last_changed_by'] = parts[0]
                # Attempt to parse the date string
                date_str = parts[1]
                try:
                    # Confluence dates can be 'Month Day, Year' (e.g., 'Oct 28, 2025')
                    info['page_last_changed_date'] = datetime.strptime(date_str, '%b %d, %Y').isoformat() # ISO 8601 format
                except ValueError:
                    info['page_last_changed_date'] = date_str # Fallback to raw string

        # Extract Parent Page from Hierarchy
        parent_page_tag = main_info_div.find('span', class_='parent-page-item')
        if parent_page_tag:
            info['page_parent_page'] = parent_page_tag.get_text(strip=True)

        return info


    def get_structured_data_from_page(self):
        # First, get page ID and content (including page ID)
        page_id, page_content_html = self._get_page_id_by_title(self.page_title)

        if not page_id or not page_content_html:
            print("Could not retrieve page content. Exiting.")
            return None

        # Now, fetch and parse the page information HTML
        extra_page_info = {}
        try:
            page_info_html = self._get_page_information_html(page_id)
            extra_page_info = self._parse_page_information_html(page_info_html)
            print("Successfully extracted additional page information.")
        except requests.exceptions.HTTPError as e:
            print(f"Warning: Could not fetch page information for pageId {page_id}. HTTP Error: {e}")
            print(f"Response content (if available): {e.response.text}")
        except Exception as e:
            print(f"Warning: Failed to parse page information HTML: {e}")
        

        soup = BeautifulSoup(page_content_html, 'html.parser')
        
        structured_page_data = {
            "page_title": self.page_title, # Main title from content API query
            "page_id": page_id,
            "metadata": {},
            "tables": []
        }
        # Merge extra page info into metadata
        structured_page_data["metadata"].update(extra_page_info)


        # --- Extract Page-Level Metadata from content HTML ---
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

        # These are specific to the 'Table: portfolio_ops' content page layout
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
                for h_raw in actual_headers_raw:
                    h_cleaned = h_raw.replace(' ', '_').replace('?', '').replace('-', '_').lower()
                    current_table_headers_mapping_strategy[h_raw] = h_cleaned

            parsed_table_data["table_type"] = table_type

            # Build header_indices for the current table's parsing logic
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


            # Process data rows (skipping the header row)
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
    """
    Saves the full structured_data to a single JSON file.
    The filename is derived from the extracted table_name metadata.
    """
    if not structured_data:
        print("No structured data to save.")
        return

    os.makedirs(output_dir, exist_ok=True)

    # Use table_name from metadata for the filename
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
