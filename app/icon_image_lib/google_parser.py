import re
import chardet
from bs4 import BeautifulSoup
import logging

from icon_image_lib.LR import LR

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_source_url(s):
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

def clean_image_url(url):
    """Extract the base image URL without query parameters."""
    pattern = re.compile(r'(.*\.(?:png|jpg|jpeg|gif))(?:\?.*)?', re.IGNORECASE)
    match = pattern.match(url)
    return match.group(1) if match else url

def decode_html_bytes(html_bytes, logger):
    """Decode HTML bytes to string with fallback encoding."""
    try:
        detected_encoding = chardet.detect(html_bytes)['encoding'] or 'utf-8'
        logger.debug(f"Detected encoding: {detected_encoding}")
        return html_bytes.decode(detected_encoding, errors='replace')
    except Exception as e:
        logger.error(f"🔴 Decoding error: {e}", exc_info=True)
        return html_bytes.decode('utf-8', errors='replace')

def get_original_images(html_bytes, logger=None):
    """Extract image data from Google image search HTML."""
    logger = logger or logging.getLogger(__name__)
    html_content = decode_html_bytes(html_bytes, logger)
    logger.debug(f"Raw HTML (first 500 chars): {html_content[:500]}")

    start_tag = 'FINANCE",[22,1]]]]]'
    end_tag = ':[null,null,null,"glbl'
    matched_google_image_data = LR().get(html_content, start_tag, end_tag)
    logger.debug(f"Matched Google image data (first 200 chars): {str(matched_google_image_data)[:200]}")
    
    if 'Error' in matched_google_image_data or not matched_google_image_data:
        logger.warning('Main results tags not found or no data extracted')
        return [], [], [], []
    
    thumbnails = str(matched_google_image_data).replace('\u003d', '=').replace('\u0026', '&')
    if '"2003":' not in thumbnails:
        logger.warning('No 2003 tag found in main thumbnails')
        return [], [], [], []
    
    main_thumbs = [clean_source_url(url) for url in re.findall(r'\[\"(https:\/\/encrypted-tbn0\.gstatic\.com\/images\?.*?)\",\d+,\d+\]', thumbnails)]
    main_descriptions = re.findall(r'"2003":\[null,"[^"]*","[^"]*","(.*?)"', thumbnails)
    main_source_urls = [clean_source_url(url) for url in re.findall(r'"2003":\[null,"[^"]*","(.*?)"', thumbnails)]
    removed_thumbs = re.sub(r'\[\"(https:\/\/encrypted-tbn0\.gstatic\.com\/images\?.*?)\",\d+,\d+\]', "", thumbnails)
    main_image_urls = [clean_image_url(url) for url in re.findall(r"(?:|,),\[\"(https:|http.*?)\",\d+,\d+\]", removed_thumbs)]
    
    if not main_image_urls:
        main_image_urls = main_thumbs
        logger.debug("No main image URLs found, falling back to thumbnails")

    min_length = min(len(main_image_urls), len(main_descriptions), len(main_source_urls), len(main_thumbs))
    main_image_urls = main_image_urls[:min_length][:50]
    main_descriptions = main_descriptions[:min_length][:50]
    main_source_urls = main_source_urls[:min_length][:50]
    main_thumbs = main_thumbs[:min_length][:50]

    logger.debug(f"Main results: URLs={len(main_image_urls)}, Descriptions={len(main_descriptions)}, "
                 f"Sources={len(main_source_urls)}, Thumbs={len(main_thumbs)}")
    logger.debug(f"Sample URLs: {main_image_urls[:3]}")
    logger.debug(f"Sample Descriptions: {main_descriptions[:3]}")
    logger.debug(f"Sample Sources: {main_source_urls[:3]}")
    return main_image_urls, main_descriptions, main_source_urls, main_thumbs

def get_results_page_results(html_bytes, final_urls, final_descriptions, final_sources, final_thumbs, logger=None):
    """Extract additional image data from results page HTML without base64."""
    logger = logger or logging.getLogger(__name__)
    html_content = decode_html_bytes(html_bytes, logger)
    logger.debug(f"Results page HTML (first 500 chars): {html_content[:500]}")

    soup = BeautifulSoup(html_content, 'html.parser')
    result_divs = soup.find_all('div', class_='H8Rx8c')
    logger.debug(f"Found {len(result_divs)} result divs with class 'H8Rx8c'")

    if not result_divs:
        logger.warning("No result items found in additional results page")
        return final_urls, final_descriptions, final_sources, final_thumbs

    for div in result_divs:
        if len(final_urls) >= 100:
            break

        img_tag = div.find('img', class_='gdOPf uhHOwf ez24Df')
        thumb = img_tag.get('src') if img_tag and 'src' in img_tag.attrs and 'data:image' not in img_tag['src'] else None
        if not thumb:
            logger.debug("No valid thumbnail URL in result item")
            continue
        final_thumbs.append(thumb)

        desc_div = div.find_next('div', class_='VwiC3b')
        description = desc_div.get_text(strip=True) if desc_div else 'No description'
        final_descriptions.append(description)

        source_tag = div.find_next('cite', class_='qLRx3b')
        source = clean_source_url(source_tag.get_text(strip=True)) if source_tag else 'No source'
        final_sources.append(source)

        link_tag = div.find_next('a', class_='zReHs')
        url = clean_image_url(link_tag['href']) if link_tag and 'href' in link_tag.attrs else 'No image URL'
        final_urls.append(url)
        logger.debug(f"Appended result: URL={url[:100]}, Desc={description[:50]}, Source={source[:50]}, Thumb={thumb[:100]}")

    if len(final_urls) > 100:
        final_urls = final_urls[:100]
        final_descriptions = final_descriptions[:100]
        final_sources = final_sources[:100]
        final_thumbs = final_thumbs[:100]
        logger.debug("Capped total results at 100")

    logger.debug(f"Appended results from additional page. Total now: {len(final_urls)}")
    return final_urls, final_descriptions, final_sources, final_thumbs

def process_search_result(image_html_bytes, results_html_bytes, entry_id: int, logger=None) -> list:
    """Process search result HTML bytes and return a list of dictionaries with image data."""
    logger = logger or logging.getLogger(__name__)
    
    final_urls, final_descriptions, final_sources, final_thumbs = get_original_images(image_html_bytes, logger)
    logger.debug(f"After get_original_images for EntryID {entry_id}: URLs={len(final_urls)}, "
                 f"Sample URLs={final_urls[:3]}")
    
    if results_html_bytes:
        final_urls, final_descriptions, final_sources, final_thumbs = get_results_page_results(
            results_html_bytes, final_urls, final_descriptions, final_sources, final_thumbs, logger
        )
        logger.debug(f"After get_results_page_results for EntryID {entry_id}: URLs={len(final_urls)}, "
                     f"Sample URLs={final_urls[:3]}")
    
    min_length = min(len(final_urls), len(final_descriptions), len(final_sources), len(final_thumbs))
    if min_length < max(len(final_urls), len(final_descriptions), len(final_sources), len(final_thumbs)):
        logger.warning(f"Data lists have different lengths: {[len(lst) for lst in [final_urls, final_descriptions, final_sources, final_thumbs]]}. "
                      f"Using minimum length: {min_length}")
        final_urls = final_urls[:min_length]
        final_descriptions = final_descriptions[:min_length]
        final_sources = final_sources[:min_length]
        final_thumbs = final_thumbs[:min_length]

    if not final_urls:
        logger.warning(f"No valid image URLs extracted for EntryID {entry_id}")
        return []

    results = [
        {
            'entry_id': entry_id,  # Changed to match SearchClient.search expectation
            'image_url': final_urls[i],
            'description': final_descriptions[i],
            'source': final_sources[i],
            'thumbnail_url': final_thumbs[i]
        }
        for i in range(min_length)
    ]
    
    logger.info(f"Processed EntryID {entry_id} with {len(results)} images")
    logger.debug(f"Final results for EntryID {entry_id}: "
                 f"{[(r['image_url'][:100], r['description'][:50]) for r in results]}")
    return results