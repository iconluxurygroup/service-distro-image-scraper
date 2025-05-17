import re
import logging
from charset_normalizer import detect
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
from icon_image_lib.LR import LR

def clean_source_url(s: str) -> str:
    """Clean and decode URL string by replacing encoded characters."""
    simplified_str = s.replace('\\\\', '')
    replacements = {
        'u0026': '&', 'u003d': '=', 'u003f': '?', 'u0020': ' ', 'u0025': '%', 'u002b': '+', 'u003c': '<',
        'u003e': '>', 'u0023': '#', 'u0024': '$', 'u002f': '/', 'u005c': '\\', 'u007c': '|', 'u002d': '-',
        'u003a': ':', 'u003b': ';', 'u002c': ',', 'u002e': '.', 'u0021': '!', 'u0040': '@', 'u005e': '^',
        'u0060': '`', 'u007b': '{', 'u007d': '}', 'u005b': '[', 'u005d': ']', 'u002a': '*', 'u0028': '(',
        'u0029': ')'
    }
    for encoded, decoded in replacements.items():
        simplified_str = simplified_str.replace(encoded, decoded)
    return simplified_str

def clean_image_url(url: str) -> str:
    """Extract the base image URL without query parameters."""
    pattern = re.compile(r'(.*\.(?:png|jpg|jpeg|gif))(?:\?.*)?', re.IGNORECASE)
    match = pattern.match(url)
    return match.group(1) if match else url

def decode_html_bytes(html_bytes: bytes, logger: logging.Logger) -> str:
    """Decode HTML bytes to string with fallback encoding using charset-normalizer."""
    try:
        detected = detect(html_bytes)
        encoding = detected.get('encoding', 'utf-8') or 'utf-8'
        logger.debug(f"Detected encoding: {encoding}")
        return html_bytes.decode(encoding, errors='replace')
    except Exception as e:
        logger.error(f"Decoding error: {e}", exc_info=True)
        return html_bytes.decode('utf-8', errors='replace')

def get_original_images(
    html_bytes: bytes,
    logger: Optional[logging.Logger] = None,
    max_results: int = 50
) -> List[Dict]:
    """Extract image data from Google image search HTML."""
    logger = logger or logging.getLogger(__name__)
    
    if not isinstance(html_bytes, bytes):
        logger.error("html_bytes must be bytes")
        return []

    html_content = decode_html_bytes(html_bytes, logger)

    # Updated tags for modern Google Image Search (2025)
    start_tag = 'data:image/jpeg;base64,'  # Common prefix for inline images
    end_tag = '</script>'  # End of relevant script tag
    matched_google_image_data = LR().get(html_content, start_tag, end_tag, logger)
    
    if not matched_google_image_data:
        logger.warning(f"No data extracted between '{start_tag}' and '{end_tag}'. HTML structure may have changed.")
        return []

    # Parse with BeautifulSoup for robustness
    soup = BeautifulSoup(html_content, 'lxml')
    results = []
    image_divs = soup.find_all('div', class_='isv-r')  # Common class for image results
    trusted_domains = ['scotchandsoda.com', 'scotch-soda.com.au', 'shopninenorth.com']

    for div in image_divs[:max_results]:
        img_tag = div.find('img')
        if not img_tag or 'src' not in img_tag.attrs or 'data:image' in img_tag['src']:
            continue

        image_url = clean_image_url(img_tag['src'])
        desc_tag = div.find('div', class_='ZGwO7')  # Description class
        description = desc_tag.get_text(strip=True) if desc_tag else 'No description'
        source_tag = div.find('a', class_='fH9F7')  # Source link
        source_url = clean_source_url(source_tag['href']) if source_tag and 'href' in source_tag.attrs else 'No source'
        thumb_url = image_url  # Use main image as thumbnail if no separate thumbnail

        result = {
            'ImageUrl': image_url,
            'ImageDesc': description,
            'ImageSource': source_url,
            'ImageUrlThumbnail': thumb_url,
            'Priority': 1 if any(domain in source_url.lower() for domain in trusted_domains) else 2
        }
        results.append(result)

    logger.debug(f"Extracted {len(results)} main images for EntryID")
    return sorted(results, key=lambda x: x['Priority'])[:max_results]

def get_results_page_results(
    html_bytes: bytes,
    existing_results: List[Dict],
    logger: Optional[logging.Logger] = None,
    max_total: int = 100
) -> List[Dict]:
    """Extract additional image data from results page HTML."""
    logger = logger or logging.getLogger(__name__)
    
    if not isinstance(html_bytes, bytes):
        logger.error("html_bytes must be bytes")
        return existing_results

    html_content = decode_html_bytes(html_bytes, logger)
    try:
        soup = BeautifulSoup(html_content, 'lxml')
    except Exception as e:
        logger.error(f"BeautifulSoup parsing error: {e}", exc_info=True)
        return existing_results

    result_divs = soup.find_all('div', class_='H8Rx8c')
    trusted_domains = ['scotchandsoda.com', 'scotch-soda.com.au', 'shopninenorth.com']

    for div in result_divs:
        if len(existing_results) >= max_total:
            logger.debug(f"Reached maximum total results: {max_total}")
            break

        img_tag = div.find('img', class_='gdOPf uhHOwf ez24Df')
        if not img_tag or 'src' not in img_tag.attrs or 'data:image' in img_tag['src']:
            continue

        thumb_url = img_tag['src']
        desc_div = div.find_next('div', class_='VwiC3b')
        description = desc_div.get_text(strip=True) if desc_div else 'No description'
        source_tag = div.find_next('cite', class_='qLRx3b')
        source_url = clean_source_url(source_tag.get_text(strip=True)) if source_tag else 'No source'
        link_tag = div.find_next('a', class_='zReHs')
        image_url = clean_image_url(link_tag['href']) if link_tag and 'href' in link_tag.attrs else thumb_url

        result = {
            'ImageUrl': image_url,
            'ImageDesc': description,
            'ImageSource': source_url,
            'ImageUrlThumbnail': thumb_url,
            'Priority': 1 if any(domain in source_url.lower() for domain in trusted_domains) else 2
        }
        existing_results.append(result)

    logger.debug(f"Appended {len(existing_results)} total results")
    return sorted(existing_results, key=lambda x: x['Priority'])[:max_total]

def process_search_result(
    image_html_bytes: bytes,
    results_html_bytes: Optional[bytes],
    entry_id: int,
    logger: Optional[logging.Logger] = None,
    max_results: int = 100
) -> List[Dict]:
    """Process search result HTML bytes and return a list of image data dictionaries."""
    logger = logger or logging.getLogger(__name__)
    
    if not isinstance(image_html_bytes, bytes):
        logger.error("image_html_bytes must be bytes")
        return []
    if results_html_bytes and not isinstance(results_html_bytes, bytes):
        logger.error("results_html_bytes must be bytes")
        return []
    if not isinstance(entry_id, int):
        logger.error("entry_id must be an integer")
        return []

    results = get_original_images(image_html_bytes, logger, max_results=max_results // 2)
    
    if results_html_bytes:
        results = get_results_page_results(results_html_bytes, results, logger, max_total=max_results)

    # Add EntryID and validate
    for res in results:
        res['EntryID'] = entry_id
        required_fields = ['EntryID', 'ImageUrl', 'ImageDesc', 'ImageSource', 'ImageUrlThumbnail']
        for field in required_fields:
            if field not in res:
                logger.warning(f"Missing field {field} in result: {res}")
                res[field] = ''

    if not results:
        logger.warning(f"No valid data for EntryID {entry_id}")
        return []

    logger.info(f"Processed EntryID {entry_id} with {len(results)} images")
    return results