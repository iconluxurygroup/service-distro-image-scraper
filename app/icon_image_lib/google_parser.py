import re
import chardet
from bs4 import BeautifulSoup
from icon_image_lib.LR import LR  # Assumes LR is a custom class for extracting text between tags
import logging
import base64

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_source_url(s):
    """
    Clean source URLs by replacing encoded characters with their decoded equivalents.
    
    Args:
        s (str): The raw source URL string.
    
    Returns:
        str: The cleaned URL.
    """
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
    """
    Clean image URLs to extract the base image path, removing query parameters.
    
    Args:
        url (str): The raw image URL.
    
    Returns:
        str: The cleaned image URL.
    """
    pattern = re.compile(r'(.*\.(?:png|jpg|jpeg|gif))(?:\?.*)?', re.IGNORECASE)
    match = pattern.match(url)
    return match.group(1) if match else url

def get_original_images(html_bytes, logger=None):
    """
    Parse the main Google image search results from HTML bytes, extracting up to 50 images.
    
    Args:
        html_bytes (bytes): The HTML content of the main search page.
        logger (logging.Logger, optional): Logger instance for debugging.
    
    Returns:
        tuple: (image_urls, descriptions, source_urls, thumbnails) - Lists of extracted data.
    """
    logger = logger or logging.getLogger(__name__)
    
    # Decode HTML with fallback
    try:
        detected_encoding = chardet.detect(html_bytes)['encoding'] or 'utf-8'
        logger.debug(f"Detected encoding: {detected_encoding}")
        html_content = html_bytes.decode(detected_encoding, errors='replace')
    except Exception as e:
        logger.error(f"🔴 Decoding error: {e}", exc_info=True)
        html_content = html_bytes.decode('utf-8', errors='replace')

    # Extract main results section using LR
    start_tag = 'FINANCE",[22,1]]]]]'
    end_tag = ':[null,null,null,"glbl'
    matched_google_image_data = LR().get(html_content, start_tag, end_tag)
    
    if 'Error' in matched_google_image_data or not matched_google_image_data:
        logger.warning('Main results tags not found or no data extracted')
        return [], [], [], []
    
    matched_google_image_data = str(matched_google_image_data).replace('\u003d', '=').replace('\u0026', '&')
    thumbnails = matched_google_image_data
    
    if '"2003":' not in thumbnails:
        logger.warning('No 2003 tag found in main thumbnails')
        return [], [], [], []
    
    # Extract thumbnails
    main_thumbs = re.findall(r'\[\"(https\:\/\/encrypted-tbn0\.gstatic\.com\/images\?.*?)\",\d+,\d+\]', thumbnails)
    main_thumbs = [clean_source_url(url) for url in main_thumbs]
    
    # Extract descriptions
    regex_pattern_desc = r'"2003":\[null,"[^"]*","[^"]*","(.*?)"'
    main_descriptions = re.findall(regex_pattern_desc, thumbnails)
    
    # Extract source URLs
    regex_pattern_src = r'"2003":\[null,"[^"]*","(.*?)"'
    main_source_urls = [clean_source_url(url) for url in re.findall(regex_pattern_src, thumbnails)]
    
    # Extract full-resolution images
    removed_thumbs = re.sub(r'\[\"(https\:\/\/encrypted-tbn0\.gstatic\.com\/images\?.*?)\",\d+,\d+\]', "", thumbnails)
    main_image_urls = [clean_image_url(url) for url in re.findall(r"(?:|,),\[\"(https:|http.*?)\",\d+,\d+\]", removed_thumbs)]
    
    if not main_image_urls:
        main_image_urls = main_thumbs

    # Ensure consistent lengths and limit to 50
    min_length = min(len(main_image_urls), len(main_descriptions), len(main_source_urls), len(main_thumbs))
    main_image_urls = main_image_urls[:min_length][:50]
    main_descriptions = main_descriptions[:min_length][:50]
    main_source_urls = main_source_urls[:min_length][:50]
    main_thumbs = main_thumbs[:min_length][:50]

    logger.debug(f"Main results: URLs={len(main_image_urls)}, Descriptions={len(main_descriptions)}, "
                 f"Sources={len(main_source_urls)}, Thumbs={len(main_thumbs)}")
    return main_image_urls, main_descriptions, main_source_urls, main_thumbs

def get_results_page_results(html_bytes, final_urls, final_descriptions, final_sources, final_thumbs, logger=None):
    """
    Parse the additional Google results page HTML and append image data to the provided lists.
    
    Args:
        html_bytes (bytes): The HTML content of the additional results page.
        final_urls (list): List to append image URLs.
        final_descriptions (list): List to append descriptions.
        final_sources (list): List to append source URLs.
        final_thumbs (list): List to append thumbnails.
        logger (logging.Logger, optional): Logger instance for debugging.
    
    Returns:
        tuple: Updated (final_urls, final_descriptions, final_sources, final_thumbs).
    """
    logger = logger or logging.getLogger(__name__)
    
    # Decode HTML with fallback
    try:
        detected_encoding = chardet.detect(html_bytes)['encoding'] or 'utf-8'
        html_content = html_bytes.decode(detected_encoding, errors='replace')
    except Exception as e:
        logger.error(f"🔴 Decoding error: {e}", exc_info=True)
        html_content = html_bytes.decode('utf-8', errors='replace')

    soup = BeautifulSoup(html_content, 'html.parser')
    result_divs = soup.find_all('div', class_='H8Rx8c')  # Class assumed for result items

    if not result_divs:
        logger.warning("No result items found in additional results page")
        return final_urls, final_descriptions, final_sources, final_thumbs

    for div in result_divs:
        if len(final_urls) >= 100:
            break

        # Extract and validate base64 thumbnail
        img_tag = div.find('img', class_='YQ4gaf')
        thumb = None
        if img_tag and 'src' in img_tag.attrs and 'data:image' in img_tag['src']:
            thumb_base64 = img_tag['src']
            try:
                base64.b64decode(thumb_base64.split(',')[1])
                thumb = thumb_base64
            except Exception as e:
                logger.warning(f"Invalid base64 thumbnail: {e}")
        if not thumb:
            logger.debug("No valid base64 thumbnail in result item")
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

        # Extract full image URL
        link_tag = div.find_next('a', class_='zReHs')
        url = clean_image_url(link_tag['href']) if link_tag and 'href' in link_tag.attrs else 'No image URL'
        final_urls.append(url)

    # Cap at 100 total results
    if len(final_urls) > 100:
        final_urls = final_urls[:100]
        final_descriptions = final_descriptions[:100]
        final_sources = final_sources[:100]
        final_thumbs = final_thumbs[:100]
        logger.debug("Capped total results at 100")

    logger.debug(f"Appended results from additional page. Total now: {len(final_urls)}")
    return final_urls, final_descriptions, final_sources, final_thumbs

def process_search_result(image_html_bytes, results_html_bytes, logger=None):
    """
    Process both main and additional results pages, combining up to 100 results.
    
    Args:
        image_html_bytes (bytes): HTML bytes of the main image search page.
        results_html_bytes (bytes): HTML bytes of the additional results page.
        logger (logging.Logger, optional): Logger instance for debugging.
    
    Returns:
        tuple: (image_urls, descriptions, source_urls, thumbnails) - Combined results.
    """
    logger = logger or logging.getLogger(__name__)
    
    # Parse main results (up to 50)
    final_urls, final_descriptions, final_sources, final_thumbs = get_original_images(image_html_bytes, logger)
    
    # Append additional results (up to 100 total)
    if results_html_bytes:
        final_urls, final_descriptions, final_sources, final_thumbs = get_results_page_results(
            results_html_bytes, final_urls, final_descriptions, final_sources, final_thumbs, logger
        )
    
    logger.info(f"Total combined results: {len(final_urls)}")
    return final_urls, final_descriptions, final_sources, final_thumbs

