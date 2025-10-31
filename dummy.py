# confluence_utils.py (MODIFIED clean_special_characters_iterative)

# ... (rest of imports)

def clean_special_characters_iterative(data):
    """
    Iteratively cleans special (non-ASCII printable) characters from strings
    in a nested structure. Preserves standard ASCII.
    """
    if isinstance(data, (str, int, float, bool, type(None))):
        return data # Base case: return non-string/non-collection data directly
    
    queue = deque([data])

    while queue:
        current = queue.popleft()

        if isinstance(current, dict):
            for key, value in list(current.items()): # Iterate on a copy for safe modification
                if isinstance(value, (dict, list)):
                    queue.append(value)
                elif isinstance(value, str):
                    # NEW & CORRECTED:
                    # Replace characters that are NOT in the printable ASCII range (32-126)
                    # or common whitespace (tab, newline, carriage return) with a space.
                    # This keeps standard letters, numbers, punctuation, etc.
                    cleaned_value = re.sub(r'[^\x20-\x7E\t\n\r]+', ' ', value)
                    # Normalize whitespace (multiple spaces to single, strip)
                    current[key] = re.sub(r'\s+', ' ', cleaned_value).strip()
        
        elif isinstance(current, list):
            for i in range(len(current)):
                item = current[i]
                if isinstance(item, (dict, list)):
                    queue.append(item)
                elif isinstance(item, str):
                    # NEW & CORRECTED: Same cleaning logic for list items
                    cleaned_item = re.sub(r'[^\x20-\x7E\t\n\r]+', ' ', item)
                    current[i] = re.sub(r'\s+', ' ', cleaned_item).strip()
    return data

# ... (rest of confluence_utils.py, config.py, report_generator.py remain as last full code)
