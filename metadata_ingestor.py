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
    Uses 'created_by_username', 'created_date', 'last_modified_by_username',
    'last_modified_date', 'parent_page_id', 'labels'.
    """
    data_points = []
    # FIX: Use created_by_username as the primary "author" identifier for the hash
    data_points.append(str(metadata_dict.get('created_by_username', '')))
    data_points.append(str(metadata_dict.get('created_date', '')))
    data_points.append(str(metadata_dict.get('last_modified_by_username', '')))
    data_points.append(str(metadata_dict.get('last_modified_date', '')))
    data_points.append(str(metadata_dict.get('parent_page_id', '')))
    
    labels = metadata_dict.get('labels', [])
    if isinstance(labels, list):
        data_points.append(json.dumps(sorted(labels)))
    elif isinstance(labels, str):
        try:
            parsed_labels = json.loads(labels)
            if isinstance(parsed_labels, list):
                data_points.append(json.dumps(sorted(parsed_labels)))
            else:
                data_points.append(labels)
        except json.JSONDecodeError:
            data_points.append(labels)
    else:
        data_points.append('')

    hash_input = "|".join(data_points)
    
    return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()

def ingest_confluence_metadata():
    """
    Reads the hit-or-miss report, fetches approved page metadata from Confluence API,
    calculates a hash_id, and stores it in the SQLite database.
    Updates extraction_status intelligently based on existing state.
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

    for page_entry_from_report in approved_pages: # Renamed for clarity
        page_id = page_entry_from_report.get("page_id")
        given_title = page_entry_from_report.get("given_title")
        found_title = page_entry_from_report.get("found_title")

        if not page_id:
            print(f"Skipping '{given_title}': No page_id found in report entry.")
            continue

        print(f"\nProcessing metadata for approved page: '{found_title}' (ID: {page_id})...")
        
        # NEW: Fetch existing record from DB to preserve downstream status
        existing_db_record = db_manager.get_page_metadata(page_id)
        
        db_metadata = {
            "page_id": page_id,
            "given_title": given_title,
            "found_title": found_title,
            "page_status": page_entry_from_report.get("status"),
            "user_verified": page_entry_from_report.get("user_verified"),
            "attempts_made": page_entry_from_report.get("attempts_made", 0),
            "first_checked_on": page_entry_from_report.get("first_checked_on"),
            "last_checked_on": datetime.now().isoformat(), # Always update last_checked_on
            # Initialize hash_id and extraction_status (will be overwritten if API fetch is successful)
            "hash_id": None,
            "last_parsed_content_hash": existing_db_record.get('last_parsed_content_hash') if existing_db_record else None,
            "structured_data_file": existing_db_record.get('structured_data_file') if existing_db_record else None,
            "notes": page_entry_from_report.get("notes", "")
        }

        # Preserve existing extraction_status if it indicates a further-downstream success
        # E.g., if it was PARSED_OK, we don't want to reset it unless hash changes
        if existing_db_record and existing_db_record.get('extraction_status') == 'PARSED_OK':
            db_metadata["extraction_status"] = 'PARSED_OK'
        else:
            db_metadata["extraction_status"] = 'PENDING_METADATA_INGESTION' # Default if no previous successful state

        try:
            expanded_api_metadata = confluence_parser.get_expanded_page_metadata(page_id)
            db_metadata.update(expanded_api_metadata)

            if 'labels' in db_metadata and isinstance(db_metadata['labels'], list):
                db_metadata['labels'] = json.dumps(db_metadata['labels'])
            else:
                db_metadata['labels'] = json.dumps([])

            new_hash_id = calculate_metadata_hash(db_metadata)
            old_hash_id = existing_db_record.get('hash_id') if existing_db_record else None

            db_metadata["hash_id"] = new_hash_id
            
            if old_hash_id is None:
                print(f"  First time metadata ingested for '{found_title}'. Hash: {new_hash_id}")
                db_metadata["extraction_status"] = 'METADATA_INGESTED'
            elif new_hash_id != old_hash_id:
                print(f"  Metadata changed for '{found_title}'. Old Hash: {old_hash_id}, New Hash: {new_hash_id}")
                db_metadata["extraction_status"] = 'METADATA_INGESTED' # Signal downstream stage for reprocessing
            else:
                print(f"  Metadata unchanged for '{found_title}'. Hash: {new_hash_id}")
                # Only update status to METADATA_INGESTED if it was previously an error/pending
                # Otherwise, preserve downstream status like PARSED_OK
                if db_metadata["extraction_status"] not in ['PARSED_OK', 'DB_FAILED']: # Don't overwrite PARSED_OK or DB_FAILED
                     db_metadata["extraction_status"] = 'METADATA_INGESTED'

            
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
            db_metadata["notes"] += f" | CRITICAL DB STORE ERROR: {e}"
            # Attempt to update it again with the error status (minimal fields to prevent new errors)
            try:
                db_manager.insert_or_update_page_metadata({
                    "page_id": page_id,
                    "extraction_status": "DB_FAILED",
                    "notes": db_metadata["notes"] # Use the combined notes
                })
            except:
                pass


    db_manager.disconnect()
    print("\n--- Confluence Metadata Ingestion Complete ---")


