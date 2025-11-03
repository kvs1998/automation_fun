# metadata_ingestor.py
import os
import json
from datetime import datetime
import hashlib # NEW: For hash_id calculation

from config import ConfluenceConfig, FilePaths, get_confluence_page_titles
from confluence_utils import ConfluencePageParser, clean_special_characters_iterative
from database_manager import DatabaseManager


def calculate_metadata_hash(metadata_dict):
    """
    Calculates an SHA256 hash for key metadata fields to track changes.
    Uses 'author_username', 'created_date', 'last_modified_by_username',
    'last_modified_date', 'parent_page_id', 'labels'.
    """
    # Ensure all relevant fields are strings for hashing
    data_points = []
    data_points.append(str(metadata_dict.get('author_username', '')))
    data_points.append(str(metadata_dict.get('created_date', '')))
    data_points.append(str(metadata_dict.get('last_modified_by_username', '')))
    data_points.append(str(metadata_dict.get('last_modified_date', '')))
    data_points.append(str(metadata_dict.get('parent_page_id', '')))
    
    # Labels should be sorted for consistent hashing
    labels = metadata_dict.get('labels', [])
    if isinstance(labels, list):
        data_points.append(json.dumps(sorted(labels))) # Sort list, then dump to string
    elif isinstance(labels, str): # If already a JSON string from DB
        try:
            parsed_labels = json.loads(labels)
            if isinstance(parsed_labels, list):
                data_points.append(json.dumps(sorted(parsed_labels)))
            else:
                data_points.append(labels) # If not a list, use as is
        except json.JSONDecodeError:
            data_points.append(labels) # Use as is if invalid JSON string
    else:
        data_points.append('') # Fallback for non-string, non-list labels

    # Concatenate all data points into a single string
    hash_input = "|".join(data_points)
    
    # Calculate SHA256 hash
    return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()


def ingest_confluence_metadata():
    """
    Reads the hit-or-miss report, fetches approved page metadata from Confluence API,
    calculates a hash_id, and stores it in the SQLite database.
    """
    print("\n--- Starting Confluence Metadata Ingestion ---")

    db_manager = DatabaseManager()
    
    report_file_path = os.path.join(FilePaths.TABLES_DIR, FilePaths.REPORT_JSON_FILE)
    if not os.path.exists(report_file_path):
        print(f"ERROR: Report file not found at {report_file_path}. Please run report_generator.py first.")
        db_manager.disconnect()
        return

    try:
        with open(report_file_path, 'r', encoding='utf-8') as f:
            full_report = json.load(f)
    except Exception as e:
        print(f"ERROR: Could not read report file: {e}")
        db_manager.disconnect()
        return

    approved_pages = [
        entry for entry in full_report 
        if entry.get("status") == "HIT" and entry.get("user_verified") == True
    ]

    if not approved_pages:
        print("No approved (HIT and user_verified) pages found in the report. Please review the report.")
        db_manager.disconnect()
        return

    confluence_parser = ConfluencePageParser(
        base_url=ConfluenceConfig.BASE_URL,
        api_token=ConfluenceConfig.API_TOKEN,
        space_key=ConfluenceConfig.SPACE_KEY
    )

    for page_entry in approved_pages:
        page_id = page_entry.get("page_id")
        given_title = page_entry.get("given_title")
        found_title = page_entry.get("found_title")

        if not page_id:
            print(f"Skipping '{given_title}': No page_id found in report entry.")
            continue

        print(f"\nProcessing metadata for approved page: '{found_title}' (ID: {page_id})...")
        
        db_metadata = {
            "page_id": page_id,
            "given_title": given_title,
            "found_title": found_title,
            "page_status": page_entry.get("status"),
            "user_verified": page_entry.get("user_verified"),
            "attempts_made": page_entry.get("attempts_made", 0),
            "first_checked_on": page_entry.get("first_checked_on"),
            "last_checked_on": page_entry.get("last_checked_on"),
            "extraction_status": "PENDING_METADATA_INGESTION", # Initial status for this stage
            "notes": page_entry.get("notes", "")
        }

        try:
            # Stage 2.1: Fetch ONLY expanded page metadata from Confluence API
            expanded_api_metadata = confluence_parser.get_expanded_page_metadata(page_id)
            db_metadata.update(expanded_api_metadata) # Merge expanded API metadata

            # Ensure labels are stored as JSON string in DB
            if 'labels' in db_metadata and isinstance(db_metadata['labels'], list):
                db_metadata['labels'] = json.dumps(db_metadata['labels'])
            else:
                db_metadata['labels'] = json.dumps([])

            # Stage 2.2: Calculate hash_id
            db_metadata["hash_id"] = calculate_metadata_hash(db_metadata)
            db_metadata["extraction_status"] = "METADATA_INGESTED"
            print(f"  Metadata hash_id calculated: {db_metadata['hash_id']}")
            
        except requests.exceptions.HTTPError as e:
            db_metadata["extraction_status"] = "API_FAILED_METADATA"
            db_metadata["notes"] += f" | API error fetching metadata: {e.response.status_code} - {e.response.text.strip()}"
            print(f"  ERROR: API error fetching metadata for {found_title} (ID: {page_id}): {e.response.status_code}")
        except Exception as e:
            db_metadata["extraction_status"] = "METADATA_INGEST_FAILED"
            db_metadata["notes"] += f" | Error during metadata ingestion: {e}"
            print(f"  ERROR: Metadata ingestion error for {found_title} (ID: {page_id}): {e}")
        
        # Stage 2.3: Store/Update metadata in SQLite
        try:
            cleaned_db_metadata = clean_special_characters_iterative(db_metadata)
            db_manager.insert_or_update_page_metadata(cleaned_db_metadata)
            print(f"  Metadata for '{found_title}' (ID: {page_id}) stored/updated in DB.")
        except Exception as e:
            print(f"  CRITICAL ERROR: Could not store/update DB metadata for '{found_title}' (ID: {page_id}): {e}")
            db_metadata["extraction_status"] = "DB_FAILED"
            db_metadata["notes"] += f" | CRITICAL DB STORE ERROR: {e}"
            try:
                db_manager.insert_or_update_page_metadata(db_metadata)
            except:
                pass


    db_manager.disconnect()
    print("\n--- Confluence Metadata Ingestion Complete ---")


if __name__ == "__main__":
    ingest_confluence_metadata()
