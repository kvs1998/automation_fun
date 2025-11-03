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

    # Query DB for pages that are HIT, user_verified, and need parsing
    # This implies a state like METADATA_INGESTED and hash needs recheck OR first parse
    cursor = db_manager.conn.cursor()
    cursor.execute("""
        SELECT * FROM confluence_page_metadata 
        WHERE user_verified = 1 AND (
            extraction_status = 'METADATA_INGESTED' OR 
            extraction_status = 'PARSE_FAILED' OR
            hash_id != last_parsed_content_hash OR
            last_parsed_content_hash IS NULL
        )
    """)
    pages_to_parse = cursor.fetchall()

    if not pages_to_parse:
        print("No approved pages with updated metadata or pending parsing found in the database.")
        db_manager.disconnect()
        return

    print(f"Found {len(pages_to_parse)} approved pages requiring content parsing.")

    for page_row in pages_to_parse:
        page_entry = dict(page_row) # Convert sqlite3.Row to dict for easier access
        
        page_id = page_entry.get("page_id")
        api_title = page_entry.get("api_title") or page_entry.get("found_title")
        current_metadata_hash = page_entry.get("hash_id")
        last_parsed_hash = page_entry.get("last_parsed_content_hash")

        print(f"\nProcessing content for page: '{api_title}' (ID: {page_id})...")
        
        # Determine if re-parsing is strictly necessary based on hash comparison
        if current_metadata_hash == last_parsed_hash and page_entry.get("extraction_status") == "PARSED_OK":
            print(f"  Page '{api_title}' (ID: {page_id}) metadata hash unchanged. Skipping content re-parse.")
            # Update last_checked_on if not already done by metadata_ingestor, but skip content work
            continue # Skip to next page

        # Update status to reflect start of parsing attempt
        page_entry["extraction_status"] = "PENDING_PARSE"
        db_manager.insert_or_update_page_metadata(page_entry) # Update status in DB

        try:
            # Stage 3.1: Fetch full page content (body.storage) from Confluence API
            # This requires a *separate* API call, or an additional expand for body.storage
            # Let's add a dedicated method to confluence_parser for this
            # or extend get_expanded_page_metadata if it could optionally return content.
            # For modularity, a new dedicated fetch_page_body_content is clearer.

            # Need to define a new method in ConfluencePageParser for just body.storage
            content_url = f"{confluence_parser.base_url}/rest/api/content/{page_id}"
            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {confluence_parser.api_token}"
            }
            params = {
                "expand": "body.storage" # Only fetch body.storage
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
            
            # Apply deep cleaning to the structured HTML data
            cleaned_structured_data = clean_special_characters_iterative(structured_data_from_html)

            # Stage 3.3: Store structured data as JSON string in DB
            parsed_json_str = json.dumps(cleaned_structured_data, ensure_ascii=False)
            db_manager.insert_or_update_parsed_content(page_id, parsed_json_str)
            
            page_entry["structured_data_file"] = f"{table_html_metadata.get('table_name', 'parsed_content')}.json" # For reference, though not a file
            page_entry["extraction_status"] = "PARSED_OK"
            page_entry["last_parsed_content_hash"] = current_metadata_hash # Update with current hash
            print(f"  Structured content for '{api_title}' (ID: {page_id}) parsed and stored in DB.")
            
        except requests.exceptions.HTTPError as e:
            page_entry["extraction_status"] = "API_FAILED_CONTENT"
            page_entry["notes"] += f" | API error fetching content: {e.response.status_code} - {e.response.text.strip()}"
            print(f"  ERROR: API error fetching content for {api_title} (ID: {page_id}): {e.response.status_code}")
        except Exception as e:
            page_entry["extraction_status"] = "PARSE_FAILED"
            page_entry["notes"] += f" | Error during content parsing: {e}"
            print(f"  ERROR: Parsing error for {api_title} (ID: {page_id}): {e}")
        
        # Always update metadata table with latest status and hash
        try:
            cleaned_page_entry_for_db = clean_special_characters_iterative(page_entry)
            db_manager.insert_or_update_page_metadata(cleaned_page_entry_for_db)
            print(f"  Metadata table updated for '{api_title}' (ID: {page_id}).")
        except Exception as e:
            print(f"  CRITICAL ERROR: Could not update DB metadata after content parse for '{api_title}' (ID: {page_id}): {e}")


    db_manager.disconnect()
    print("\n--- Confluence Content Parsing and Storage Complete ---")


if __name__ == "__main__":
    parse_and_store_confluence_content()
