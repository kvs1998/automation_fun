# report_generator.py
import os
import json
from datetime import datetime
from config import ConfluenceConfig, FilePaths, get_confluence_page_titles
from confluence_utils import ConfluencePageParser, clean_special_characters_iterative # Only ConfluencePageParser used now


def generate_hit_or_miss_report():
    """
    Reads titles, checks them on Confluence, and generates a hit-or-miss report.
    This report is for user review and does not parse page content.
    """
    print("--- Starting Confluence Hit-or-Miss Report Generation ---")

    # Ensure output directory for reports exists (using TABLES_DIR for consistency, though no tables are saved yet)
    os.makedirs(FilePaths.TABLES_DIR, exist_ok=True)
    report_file_path = os.path.join(FilePaths.TABLES_DIR, FilePaths.REPORT_JSON_FILE)

    # Load existing report if it exists, to preserve 'first_checked_on' and allow updates
    existing_report_map = {}
    if os.path.exists(report_file_path):
        try:
            with open(report_file_path, 'r', encoding='utf-8') as f:
                # Ensure the loaded report is a list, or initialize empty
                loaded_report = json.load(f)
                if isinstance(loaded_report, list):
                    existing_report_map = {item['given_title']: item for item in loaded_report}
                else:
                    print("WARNING: Existing report file is not a list. Starting fresh.")
            print(f"Loaded existing report with {len(existing_report_map)} entries.")
        except json.JSONDecodeError:
            print("WARNING: Existing report file is corrupted. Starting fresh.")
        except Exception as e:
            print(f"WARNING: Could not load existing report: {e}. Starting fresh.")

    titles_to_process = get_confluence_page_titles()
    current_report_entries = []

    if not titles_to_process:
        print("No page titles found in titles.json to process.")
        return

    # Initialize parser once
    parser = ConfluencePageParser(
        base_url=ConfluenceConfig.BASE_URL,
        api_token=ConfluenceConfig.API_TOKEN,
        space_key=ConfluenceConfig.SPACE_KEY
    )

    for title in titles_to_process:
        report_entry = existing_report_map.get(title, {
            "given_title": title,
            "status": "PENDING", # Initial status
            "found_title": None,
            "page_id": None,
            "notes": "",
            "first_checked_on": None, # Will set this on first check
            "last_checked_on": None,
            "user_verified": False # NEW: Field for user review
        })
        
        current_timestamp = datetime.now().isoformat()

        # Update first_checked_on if this is a new entry or it was previously None
        if report_entry["first_checked_on"] is None:
            report_entry["first_checked_on"] = current_timestamp
        report_entry["last_checked_on"] = current_timestamp

        # Skip processing if already verified as a HIT
        if report_entry["status"] == "HIT" and report_entry["user_verified"]:
            print(f"Skipping '{title}' as it was previously a HIT and user_verified.")
            current_report_entries.append(report_entry)
            continue
        
        print(f"\n--- Checking page: '{title}' ---")
        try:
            # Call find_page_by_title (it does not return content_html now)
            search_result = parser.find_page_by_title(title)

            report_entry["status"] = search_result["status"]
            report_entry["found_title"] = search_result.get("found_title")
            report_entry["page_id"] = search_result.get("page_id")
            report_entry["notes"] = search_result.get("notes", "")
            
            if search_result["status"] == "HIT":
                print(f"Page '{search_result['found_title']}' found. Page ID: {search_result['page_id']}.")
            else:
                print(f"Page '{title}' NOT found. Status: {search_result['status']}.")

        except Exception as e:
            report_entry["status"] = "ERROR"
            report_entry["notes"] = f"Error during Confluence check: {e}"
            print(f"ERROR checking '{title}': {e}")
        
        current_report_entries.append(report_entry)

    # Apply deep cleaning to the report itself to ensure all string fields are clean
    cleaned_report = clean_special_characters_iterative(current_report_entries)

    # Save the consolidated hit-or-miss report
    with open(report_file_path, 'w', encoding='utf-8') as f:
        json.dump(cleaned_report, f, indent=2, ensure_ascii=False)
    print(f"\n--- Confluence Hit-or-Miss Report saved to: {report_file_path} ---")
    print("ACTION REQUIRED: Please review this report before proceeding to content parsing.")
    print("You can set 'user_verified': true for HIT entries in the report to approve them.")


if __name__ == "__main__":
    generate_hit_or_miss_report()
