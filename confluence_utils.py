# confluence_utils.py
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote
import re 
from collections import deque
from datetime import datetime


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

# Basic HTML text cleaner (still useful if any parsing needed here, but kept minimal)
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

    def __init__(self, base_url, api_token, space_key): # page_title removed from __init__
        self.base_url = base_url
        self.api_token = api_token
        self.space_key = space_key
        # self.page_title is no longer stored here as it's passed to find_page_by_title
        
        if not all([self.base_url, self.api_token, self.space_key]):
            raise ValueError(
                "Confluence configuration is incomplete. "
                "Please ensure base_url, api_token, and space_key are provided."
            )

    # find_page_by_title modified to NOT expand body.storage and NOT return content_html
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
        
        for attempt_num, current_title_variant in enumerate(generate_title_variations(title)):
            if attempt_num >= self.MAX_TITLE_SEARCH_RETRIES:
                return {"status": "MISS", "found_title": None, "page_id": None, "notes": f"Reached max retries ({self.MAX_TITLE_SEARCH_RETRIES}) for title variations."}

            if current_title_variant in tried_titles:
                continue
            tried_titles.add(current_title_variant)
            
            params = {
                "title": current_title_variant,
                "spaceKey": self.space_key,
                # REMOVED: "expand": "body.storage", # No longer fetching content at this stage
                "limit": 1
            }

            print(f"Attempt {attempt_num + 1}/{self.MAX_TITLE_SEARCH_RETRIES}: Searching for page '{current_title_variant}'...")
            try:
                response = requests.get(search_url, headers=headers, params=params)
                response.raise_for_status()
                
                data = response.json()
                if data and data["results"]:
                    page = data["results"][0]
                    found_title = page.get('title', current_title_variant)
                    print(f"SUCCESS: Found page '{found_title}' with ID: {page['id']} (attempt {attempt_num + 1}).") 
                    return {
                        "status": "HIT",
                        "found_title": found_title,
                        "page_id": page['id'],
                        # REMOVED: "content_html": page['body']['storage']['value'], 
                        "notes": f"Matched using variation: '{current_title_variant}'"
                    }
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    print(f"INFO: Page '{current_title_variant}' not found (HTTP 404). Trying next variation.")
                else:
                    print(f"WARNING: HTTP error {e.response.status_code} for title '{current_title_variant}'. "
                          f"Content: {e.response.text.strip()} Trying next variation.")
            except Exception as e:
                print(f"ERROR: An unexpected error occurred during API call for '{current_title_variant}': {e}. Trying next variation.")

        return {"status": "MISS", "found_title": None, "page_id": None, "notes": f"Page not found after all {attempt_num + 1} variations."}

    # REMOVED: get_structured_data_from_html for now, it will be in a later module
