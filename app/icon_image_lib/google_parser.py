import re
import logging
from charset_normalizer import detect
from bs4 import BeautifulSoup
import pandas as pd
from typing import Tuple, List, Optional

# Assuming LR class is in icon_image_lib.LR; otherwise, include it directly here
from icon_image_lib.LR import LR

# Configure logging with detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s - [File: %(filename)s, Line: %(lineno)d]'
)

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
) -> Tuple[List[str], List[str], List[str], List[str]]:
    """Extract image data from Google image search HTML."""
    logger = logger or logging.getLogger(__name__)
    
    # Validate input
    if not isinstance(html_bytes, bytes):
        logger.error("html_bytes must be bytes")
        return [], [], [], []

    # Decode HTML
    html_content = decode_html_bytes(html_bytes, logger)

    # Extract data between specific tags using LR
    start_tag = 'FINANCE",[22,1]]]]]'
    end_tag = ':[null,null,null,"glbl'
    try:
        matched_google_image_data = LR().get(html_content, start_tag, end_tag)
        if not matched_google_image_data or 'Error' in matched_google_image_data:
            logger.warning(f"No data extracted between '{start_tag}' and '{end_tag}'. HTML structure may have changed.")
            return [], [], [], []
    except Exception as e:
        logger.error(f"Error in LR.get: {e}", exc_info=True)
        return [], [], [], []

    # Process extracted data
    thumbnails = str(matched_google_image_data).replace('\u003d', '=').replace('\u0026', '&')
    if '"2003":' not in thumbnails:
        logger.warning("No '2003' tag found in thumbnails. Check HTML structure or regex patterns.")
        return [], [], [], []

    # Extract main thumbnails, descriptions, and source URLs
    main_thumbs = [clean_source_url(url) for url in re.findall(
        r'\[\"(https:\/\/encrypted-tbn0\.gstatic\.com\/images\?.*?)\",\d+,\d+\]', thumbnails)]
    main_descriptions = re.findall(r'"2003":\[null,"[^"]*","[^"]*","(.*?)"', thumbnails)
    main_source_urls = [clean_source_url(url) for url in re.findall(r'"2003":\[null,"[^"]*","(.*?)"', thumbnails)]
    
    # Extract main image URLs
    removed_thumbs = re.sub(r'\[\"(https:\/\/encrypted-tbn0\.gstatic\.com\/images\?.*?)\",\d+,\d+\]', "", thumbnails)
    main_image_urls = [clean_image_url(url) for url in re.findall(
        r"(?:|,),\[\"(https:|http.*?)\",\d+,\d+\]", removed_thumbs)]
    
    # Fallback to thumbnails if no main image URLs
    if not main_image_urls:
        main_image_urls = main_thumbs
        logger.debug("No main image URLs found; using thumbnails as fallback.")

    # Truncate to minimum length and apply max_results cap
    min_length = min(len(main_image_urls), len(main_descriptions), len(main_source_urls), len(main_thumbs))
    if min_length < max(len(main_image_urls), len(main_descriptions), len(main_source_urls), len(main_thumbs)):
        logger.warning(f"Data mismatch in main results: URLs={len(main_image_urls)}, "
                      f"Descriptions={len(main_descriptions)}, Sources={len(main_source_urls)}, "
                      f"Thumbs={len(main_thumbs)}. Truncating to {min_length}")
        for i in range(min_length, max(len(main_image_urls), len(main_descriptions), len(main_source_urls), len(main_thumbs))):
            logger.debug(f"Mismatched item at index {i}: "
                        f"URL={main_image_urls[i] if i < len(main_image_urls) else None}, "
                        f"Desc={main_descriptions[i] if i < len(main_descriptions) else None}, "
                        f"Source={main_source_urls[i] if i < len(main_source_urls) else None}, "
                        f"Thumb={main_thumbs[i] if i < len(main_thumbs) else None}")

    main_image_urls = main_image_urls[:min_length][:max_results]
    main_descriptions = main_descriptions[:min_length][:max_results]
    main_source_urls = main_source_urls[:min_length][:max_results]
    main_thumbs = main_thumbs[:min_length][:max_results]

    logger.debug(f"Main results extracted: URLs={len(main_image_urls)}, Descriptions={len(main_descriptions)}, "
                 f"Sources={len(main_source_urls)}, Thumbs={len(main_thumbs)}")
    return main_image_urls, main_descriptions, main_source_urls, main_thumbs

def get_results_page_results(
    html_bytes: bytes,
    final_urls: List[str],
    final_descriptions: List[str],
    final_sources: List[str],
    final_thumbs: List[str],
    logger: Optional[logging.Logger] = None,
    max_total: int = 100
) -> Tuple[List[str], List[str], List[str], List[str]]:
    """Extract additional image data from results page HTML without base64."""
    logger = logger or logging.getLogger(__name__)
    
    # Validate input
    if not isinstance(html_bytes, bytes):
        logger.error("html_bytes must be bytes")
        return final_urls, final_descriptions, final_sources, final_thumbs

    # Decode HTML
    html_content = decode_html_bytes(html_bytes, logger)

    # Parse HTML with lxml for efficiency
    try:
        soup = BeautifulSoup(html_content, 'lxml')
    except Exception as e:
        logger.error(f"BeautifulSoup parsing error: {e}", exc_info=True)
        return final_urls, final_descriptions, final_sources, final_thumbs

    # Find result divs
    result_divs = soup.find_all('div', class_='H8Rx8c')
    if not result_divs:
        logger.warning("No result items found in additional results page. Check class name 'H8Rx8c'.")
        return final_urls, final_descriptions, final_sources, final_thumbs

    # Process each result div
    for div in result_divs:
        if len(final_urls) >= max_total:
            logger.debug(f"Reached maximum total results: {max_total}")
            break

        # Extract thumbnail
        img_tag = div.find('img', class_='gdOPf uhHOwf ez24Df')
        thumb = img_tag.get('src') if img_tag and 'src' in img_tag.attrs and 'data:image' not in img_tag['src'] else None
        if not thumb:
            logger.debug("No valid thumbnail URL in result item")
            continue
        final_thumbs.append(thumb)

        # Extract description
        desc_div = div.find_next('div', class_='VwiC3b')
        description = desc_div.get_text(strip=True) if desc_div else 'No description'
        final_descriptions.append(description)

        # Extract source URL
        source_tag = div.find_next('cite', class_='qLRx3b')
        source = clean_source_url(source_tag.get_text(strip=True)) if source_tag else 'No source'
        final_sources.append(source)

        # Extract image URL
        link_tag = div.find_next('a', class_='zReHs')
        url = clean_image_url(link_tag['href']) if link_tag and 'href' in link_tag.attrs else 'No image URL'
        final_urls.append(url)

    # Cap total results
    if len(final_urls) > max_total:
        final_urls = final_urls[:max_total]
        final_descriptions = final_descriptions[:max_total]
        final_sources = final_sources[:max_total]
        final_thumbs = final_thumbs[:max_total]
        logger.debug(f"Capped total results at {max_total}")

    logger.debug(f"Appended results from additional page. Total now: URLs={len(final_urls)}, "
                 f"Descriptions={len(final_descriptions)}, Sources={len(final_sources)}, "
                 f"Thumbs={len(final_thumbs)}")
    return final_urls, final_descriptions, final_sources, final_thumbs

def process_search_result(
    image_html_bytes: bytes,
    results_html_bytes: Optional[bytes],
    entry_id: int,
    logger: Optional[logging.Logger] = None,
    max_results: int = 100
) -> pd.DataFrame:
    """Process search result HTML bytes and return a DataFrame with image data."""
    logger = logger or logging.getLogger(__name__)
    
    # Input validation
    if not isinstance(image_html_bytes, bytes):
        logger.error("image_html_bytes must be bytes")
        return pd.DataFrame(columns=['EntryID', 'ImageUrl', 'ImageDesc', 'ImageSource', 'ImageUrlThumbnail'])
    if results_html_bytes and not isinstance(results_html_bytes, bytes):
        logger.error("results_html_bytes must be bytes")
        return pd.DataFrame(columns=['EntryID', 'ImageUrl', 'ImageDesc', 'ImageSource', 'ImageUrlThumbnail'])
    if not isinstance(entry_id, int):
        logger.error("entry_id must be an integer")
        return pd.DataFrame(columns=['EntryID', 'ImageUrl', 'ImageDesc', 'ImageSource', 'ImageUrlThumbnail'])

    # Extract main images
    final_urls, final_descriptions, final_sources, final_thumbs = get_original_images(
        image_html_bytes, logger, max_results=max_results // 2
    )
    
    # Extract additional results if provided
    if results_html_bytes:
        final_urls, final_descriptions, final_sources, final_thumbs = get_results_page_results(
            results_html_bytes, final_urls, final_descriptions, final_sources, final_thumbs,
            logger, max_total=max_results
        )
    
    # Handle empty results
    min_length = min(len(final_urls), len(final_descriptions), len(final_sources), len(final_thumbs))
    if min_length == 0:
        logger.warning(f"No valid data for EntryID {entry_id}")
        return pd.DataFrame(columns=['EntryID', 'ImageUrl', 'ImageDesc', 'ImageSource', 'ImageUrlThumbnail'])

    # Handle length mismatches
    if min_length < max(len(final_urls), len(final_descriptions), len(final_sources), len(final_thumbs)):
        logger.warning(f"Data mismatch: URLs={len(final_urls)}, Descriptions={len(final_descriptions)}, "
                      f"Sources={len(final_sources)}, Thumbs={len(final_thumbs)}. Truncating to {min_length}")
        for i in range(min_length, max(len(final_urls), len(final_descriptions), len(final_sources), len(final_thumbs))):
            logger.debug(f"Mismatched item at index {i}: "
                        f"URL={final_urls[i] if i < len(final_urls) else None}, "
                        f"Desc={final_descriptions[i] if i < len(final_descriptions) else None}, "
                        f"Source={final_sources[i] if i < len(final_sources) else None}, "
                        f"Thumb={final_thumbs[i] if i < len(final_thumbs) else None}")

    # Truncate to minimum length
    final_urls = final_urls[:min_length]
    final_descriptions = final_descriptions[:min_length]
    final_sources = final_sources[:min_length]
    final_thumbs = final_thumbs[:min_length]

    # Create DataFrame
    df = pd.DataFrame({
        'EntryID': [entry_id] * min_length,
        'ImageUrl': final_urls,
        'ImageDesc': final_descriptions,
        'ImageSource': final_sources,
        'ImageUrlThumbnail': final_thumbs
    })
    
    logger.info(f"Processed EntryID {entry_id} with {len(df)} images")
    return df