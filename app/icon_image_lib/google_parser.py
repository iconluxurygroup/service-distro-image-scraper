import re
import chardet
from bs4 import BeautifulSoup
from icon_image_lib.LR import LR  # Assumes LR is a custom class
import logging
import base64
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_source_url(s):
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
    pattern = re.compile(r'(.*\.(?:png|jpg|jpeg|gif))(?:\?.*)?', re.IGNORECASE)
    match = pattern.match(url)
    return match.group(1) if match else url

def get_original_images(html_bytes, logger=None):
    logger = logger or logging.getLogger(__name__)
    try:
        detected_encoding = chardet.detect(html_bytes)['encoding'] or 'utf-8'
        logger.debug(f"Detected encoding: {detected_encoding}")
        html_content = html_bytes.decode(detected_encoding, errors='replace')
    except Exception as e:
        logger.error(f"🔴 Decoding error: {e}", exc_info=True)
        html_content = html_bytes.decode('utf-8', errors='replace')

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
    
    main_thumbs = re.findall(r'\[\"(https\:\/\/encrypted-tbn0\.gstatic\.com\/images\?.*?)\",\d+,\d+\]', thumbnails)
    main_thumbs = [clean_source_url(url) for url in main_thumbs]
    
    regex_pattern_desc = r'"2003":\[null,"[^"]*","[^"]*","(.*?)"'
    main_descriptions = re.findall(regex_pattern_desc, thumbnails)
    
    regex_pattern_src = r'"2003":\[null,"[^"]*","(.*?)"'
    main_source_urls = [clean_source_url(url) for url in re.findall(regex_pattern_src, thumbnails)]
    
    removed_thumbs = re.sub(r'\[\"(https\:\/\/encrypted-tbn0\.gstatic\.com\/images\?.*?)\",\d+,\d+\]', "", thumbnails)
    main_image_urls = [clean_image_url(url) for url in re.findall(r"(?:|,),\[\"(https:|http.*?)\",\d+,\d+\]", removed_thumbs)]
    
    if not main_image_urls:
        main_image_urls = main_thumbs

    min_length = min(len(main_image_urls), len(main_descriptions), len(main_source_urls), len(main_thumbs))
    main_image_urls = main_image_urls[:min_length][:50]
    main_descriptions = main_descriptions[:min_length][:50]
    main_source_urls = main_source_urls[:min_length][:50]
    main_thumbs = main_thumbs[:min_length][:50]

    logger.debug(f"Main results: URLs={len(main_image_urls)}, Descriptions={len(main_descriptions)}, "
                 f"Sources={len(main_source_urls)}, Thumbs={len(main_thumbs)}")
    return main_image_urls, main_descriptions, main_source_urls, main_thumbs

def get_results_page_results(html_bytes, final_urls, final_descriptions, final_sources, final_thumbs, logger=None):
    logger = logger or logging.getLogger(__name__)
    try:
        detected_encoding = chardet.detect(html_bytes)['encoding'] or 'utf-8'
        html_content = html_bytes.decode(detected_encoding, errors='replace')
    except Exception as e:
        logger.error(f"🔴 Decoding error: {e}", exc_info=True)
        html_content = html_bytes.decode('utf-8', errors='replace')

    soup = BeautifulSoup(html_content, 'html.parser')
    result_divs = soup.find_all('div', class_='H8Rx8c')

    if not result_divs:
        logger.warning("No result items found in additional results page")
        return final_urls, final_descriptions, final_sources, final_thumbs

    for div in result_divs:
        if len(final_urls) >= 100:
            break

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

        desc_div = div.find_next('div', class_='VwiC3b')
        description = desc_div.get_text(strip=True) if desc_div else 'No description'
        final_descriptions.append(description)

        source_tag = div.find_next('cite', class_='qLRx3b')
        source = clean_source_url(source_tag.get_text(strip=True)) if source_tag else 'No source'
        final_sources.append(source)

        link_tag = div.find_next('a', class_='zReHs')
        url = clean_image_url(link_tag['href']) if link_tag and 'href' in link_tag.attrs else 'No image URL'
        final_urls.append(url)

    if len(final_urls) > 100:
        final_urls = final_urls[:100]
        final_descriptions = final_descriptions[:100]
        final_sources = final_sources[:100]
        final_thumbs = final_thumbs[:100]
        logger.debug("Capped total results at 100")

    logger.debug(f"Appended results from additional page. Total now: {len(final_urls)}")
    return final_urls, final_descriptions, final_sources, final_thumbs

def process_search_result(image_html_bytes, results_html_bytes, entry_id: int, logger=None) -> pd.DataFrame:
    logger = logger or logging.getLogger(__name__)
    
    final_urls, final_descriptions, final_sources, final_thumbs = get_original_images(image_html_bytes, logger)
    
    if results_html_bytes:
        final_urls, final_descriptions, final_sources, final_thumbs = get_results_page_results(
            results_html_bytes, final_urls, final_descriptions, final_sources, final_thumbs, logger
        )
    
    # Check if all lists have the same length
    lists = [final_urls, final_descriptions, final_sources, final_thumbs]
    lengths = [len(lst) for lst in lists]
    if len(set(lengths)) > 1:  # If lengths differ
        min_length = min(lengths)
        logger.warning(f"Data lists have different lengths: {lengths}. Using minimum length: {min_length}")
        final_urls = final_urls[:min_length]
        final_descriptions = final_descriptions[:min_length]
        final_sources = final_sources[:min_length]
        final_thumbs = final_thumbs[:min_length]
    else:
        min_length = lengths[0]
    
    df = pd.DataFrame({
        'EntryID': [entry_id] * min_length,
        'ImageURL': final_urls,
        'ImageDesc': final_descriptions,
        'ImageSource': final_sources,
        'ImageURLThumbnail': final_thumbs
    })
    
    logger.info(f"Processed EntryID {entry_id} with {len(df)} images")
    return df