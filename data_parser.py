# data_parser.py
import os
import json
from datetime import datetime
import hashlib # For hash calculation
import re # For filename sanitization

from config import ConfluenceConfig, FilePaths
from confluence_utils import ConfluencePageParser, clean_special_characters_iterative
from database_manager import DatabaseManager


def parse_and_store_confluence_content():
    """
    Reads metadata from the DB, checks hash for changes, fetches body.storage content,
    parses it, and stores the structured content JSON directly into the DB.
    """
    print("\n--- Starting Confluence Content Parsing and Storage ---")

    db_manager = DatabaseManager()
    confluence_parser = ConfluencePageParser(
        base_url=ConfluenceConfig.BASE_URL,
        api_token=ConfluenceConfig.API_TOKEN,
        space_key=ConfluenceConfig.SPACE_KEY
    )

    cursor = db_manager.conn.cursor()
    cursor.execute("""
        SELECT * FROM confluence_page_metadata 
        WHERE user_verified = 1
    """)
    pages_from_db = cursor.fetchall()

    if not pages_from_db:
        print("No approved pages found in the database for content parsing.")
        db_manager.disconnect()
        return

    pages_to_parse = []
    for page_row in pages_from_db:
        page_entry = dict(page_row)
        current_metadata_hash = page_entry.get("hash_id")
        last_parsed_hash = page_entry.get("last_parsed_content_hash")
        extraction_status = page_entry.get("extraction_status")

        if (current_metadata_hash != last_parsed_hash or 
            last_parsed_hash is None or 
            extraction_status in ['PENDING_PARSE', 'PARSE_FAILED', 'API_FAILED_CONTENT', 'DB_FAILED']): # Include DB_FAILED for re-attempt
            pages_to_parse.append(page_entry)
        else:
            print(f"Skipping '{page_entry.get('api_title', 'N/A')}' (ID: {page_entry.get('page_id')}): "
                  f"Metadata hash unchanged and previously PARSED_OK.")

    if not pages_to_parse:
        print("No approved pages with updated metadata or pending parsing found in the database.")
        db_manager.disconnect()
        return

    print(f"Found {len(pages_to_parse)} approved pages requiring content parsing.")

    for page_entry in pages_to_parse:
        page_id = page_entry.get("page_id")
        api_title = page_entry.get("api_title") or page_entry.get("found_title")
        current_metadata_hash = page_entry.get("hash_id")

        print(f"\nProcessing content for page: '{api_title}' (ID: {page_id})...")
        
        page_entry["extraction_status"] = "PENDING_PARSE"
        db_manager.insert_or_update_page_metadata(clean_special_characters_iterative(page_entry))

        try:
            content_url = f"{confluence_parser.base_url}/rest/api/content/{page_id}"
            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {confluence_parser.api_token}"
            }
            params = {
                "expand": "body.storage"
            }

            print(f"  Fetching body.storage content for page ID: {page_id}...")
            response = requests.get(content_url, headers=headers, params=params)
            response.raise_for_status()
            
            data_with_body = response.json()
            content_html = data_with_body.get('body', {}).get('storage', {}).get('value')

            if not content_html:
                raise ValueError("No body.storage content found for parsing.")

            # Stage 3.2: Parse structured table data from content_html
            structured_data_from_html = confluence_parser.get_structured_data_from_html(
                page_id=page_id,
                page_title_for_struct=api_title,
                page_content_html=content_html
            )
            
            # FIX: Check if structured_data_from_html is valid before proceeding
            if structured_data_from_html is None:
                raise ValueError("HTML parser returned None, likely no tables found on page.")

            # Apply deep cleaning to the structured HTML data
            cleaned_structured_data = clean_special_characters_iterative(structured_data_from_html)

            # Stage 3.3: Store structured data as JSON string in DB
            parsed_json_str = json.dumps(cleaned_structured_data, ensure_ascii=False)
            db_manager.insert_or_update_parsed_content(page_id, parsed_json_str)
            
            # Update metadata table with successful parsing status and hash
            # FIX: Correctly access the table name from cleaned_structured_data
            table_name_from_parsed_content = cleaned_structured_data.get("metadata", {}).get("table_name", api_title)
            sanitized_table_name_for_ref = re.sub(r'[^a-z0-9_.-]', '', table_name_from_parsed_content.lower().replace(" ", "_"))

            page_entry["structured_data_file"] = f"{sanitized_table_name_for_ref}.json"
            page_entry["extraction_status"] = "PARSED_OK"
            page_entry["last_parsed_content_hash"] = current_metadata_hash
            print(f"  Structured content for '{api_title}' (ID: {page_id}) parsed and stored in DB.")
            
        except requests.exceptions.HTTPError as e:
            page_entry["extraction_status"] = "API_FAILED_CONTENT"
            page_entry["notes"] += f" | API error fetching content: {e.response.status_code} - {e.response.text.strip()}"
            print(f"  ERROR: API error fetching content for {api_title} (ID: {page_id}): {e.response.status_code}")
        except ValueError as e: # Catch ValueErrors from parsing failures or missing content
            page_entry["extraction_status"] = "PARSE_FAILED"
            page_entry["notes"] += f" | Content parsing/access error: {e}"
            print(f"  ERROR: Content parsing/access error for {api_title} (ID: {page_id}): {e}")
        except Exception as e:
            page_entry["extraction_status"] = "PARSE_FAILED"
            page_entry["notes"] += f" | Error during content parsing: {e}. Trace: {e.__traceback__.tb_frame.f_code.co_filename}:{e.__traceback__.tb_lineno}"
            print(f"  ERROR: Unexpected parsing error for {api_title} (ID: {page_id}): {e}")
        
        # Always update metadata table with latest status and hash (including potential error states)
        try:
            cleaned_page_entry_for_db = clean_special_characters_iterative(page_entry)
            db_manager.insert_or_update_page_metadata(cleaned_page_entry_for_db)
            print(f"  Metadata table updated for '{api_title}' (ID: {page_id}).")
        except Exception as e:
            print(f"  CRITICAL ERROR: Could not update DB metadata after content parse for '{api_title}' (ID: {page_id}): {e}")
            page_entry["notes"] += f" | CRITICAL DB STORE ERROR: {e}"
            # Attempt to update it again with the error status (minimal fields to prevent new errors)
            try:
                db_manager.insert_or_update_page_metadata({
                    "page_id": page_id,
                    "extraction_status": "DB_FAILED",
                    "notes": page_entry["notes"]
                })
            except Exception as e_inner:
                print(f"  FINAL DB WRITE FAILED for {api_title} (ID: {page_id}) with error: {e_inner}")

    db_manager.disconnect()
    print("\n--- Confluence Content Parsing and Storage Complete ---")
    


if __name__ == "__main__":
    parse_and_store_confluence_content()
