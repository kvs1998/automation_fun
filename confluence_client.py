# confluence_client.py (changes only in save_structured_data_to_single_file)

# ... (all existing code as you provided it, no other changes)

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
    table_name_raw = structured_data["metadata"].get("table_name", "untitled_table")
    
    # --- NEW IMPROVEMENT: Aggressively sanitize the filename ---
    # Convert to lowercase, replace spaces with underscores.
    # Then, remove any character that is not alphanumeric or an underscore.
    table_name_for_file = table_name_raw.lower().replace(" ", "_")
    
    # Remove any characters that are NOT alphanumeric, underscore, or hyphen
    # This specifically targets problem characters like ':' from "Table: Identifier"
    # and other symbols that might be illegal in filenames.
    import re # Add this import at the top of the file if not already present
    table_name_for_file = re.sub(r'[^a-z0-9_.-]', '', table_name_for_file)
    
    filename = os.path.join(output_dir, f"{table_name_for_file}.json")
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(structured_data, f, indent=2, ensure_ascii=False)
    print(f"Saved full page data to: {filename}")


# ... (rest of the example usage)
