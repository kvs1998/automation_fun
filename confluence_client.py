import re
import unicodedata

def clean_text_from_html(element):
    """
    Extracts text from a BeautifulSoup element, removes HTML entities,
    normalizes Unicode, and strips whitespace.
    """
    if element is None:
        return ""

    text = element.get_text(separator=" ", strip=True)

    # Patterns to replace (unicode → ascii)
    replacements = {
        u'\xa0': ' ',   # non-breaking space
        '&nbsp;': ' ',
        '\u2013': '-',  # en dash
        '\u2014': '-',  # em dash
        '\u2018': "'", '\u2019': "'",  # curly single quotes
        '\u201c': '"', '\u201d': '"',  # curly double quotes
        '\u2026': '...' # ellipsis
    }

    # Replace all patterns in one pass
    pattern = re.compile("|".join(map(re.escape, replacements.keys())))
    text = pattern.sub(lambda m: replacements[m.group(0)], text)

    # Unicode normalize
    text = unicodedata.normalize('NFKD', text).strip()

    return text
