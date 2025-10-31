import re
import unicodedata
from bs4 import BeautifulSoup

def clean_text_from_html(element):
    """
    Accepts either BeautifulSoup element or plain string.
    Extracts text, removes HTML entities, normalizes Unicode, and strips whitespace.
    """
    if element is None:
        return ""

    # If it's a string, parse it as HTML; else assume it's a BS element
    if isinstance(element, str):
        soup = BeautifulSoup(element, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
    else:
        text = element.get_text(separator=" ", strip=True)

    # Map of replacements
    replacements = {
        u'\xa0': ' ',
        '&nbsp;': ' ',
        '\u2013': '-',
        '\u2014': '-',
        '\u2018': "'", '\u2019': "'",
        '\u201c': '"', '\u201d': '"',
        '\u2026': '...'
    }

    # Replace all in one go
    pattern = re.compile("|".join(map(re.escape, replacements.keys())))
    text = pattern.sub(lambda m: replacements[m.group(0)], text)

    # Unicode normalize
    text = unicodedata.normalize('NFKD', text).strip()

    # Remove extra spaces
    return " ".join(text.split())
