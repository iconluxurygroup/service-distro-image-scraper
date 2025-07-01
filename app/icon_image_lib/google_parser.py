import urllib.parse
import logging # Assuming logging is already configured

# Make sure this logger is accessible or passed appropriately
# For simplicity, defining a logger here, but ideally use the one passed down
module_logger = logging.getLogger(__name__) # Or use the logger passed as param

def extract_true_url_from_wrapper(url_string: str, logger=None) -> str:
    """
    Extracts the true target URL from known wrapper URL patterns
    (e.g., Google redirects, Next.js image optimizer).
    Assumes uXXXX sequences have already been handled by clean_source_url.
    The returned URL will be standard percent-decoded.
    """
    logger = logger or module_logger # Use passed logger or module's default
    if not url_string or not isinstance(url_string, str): # Basic check
        return url_string

    try:
        # Parse the URL. For relative URLs like "/url?q=...", netloc will be empty.
        parsed = urllib.parse.urlparse(url_string)
        # parse_qs automatically unquotes parameter values (handles %20 etc.)
        query_params = urllib.parse.parse_qs(parsed.query)

        # 1. Check for Next.js _next/image wrapper
        #    Example: https://thedropdate.com/_next/image?url=ACTUAL_URL&w=...&q=...
        if parsed.path.endswith("/_next/image") and 'url' in query_params:
            inner_url = query_params['url'][0]
            logger.debug(f"Extracted from _next/image wrapper: {inner_url} (from {url_string})")
            # The inner_url from parse_qs is already unquoted.
            # We can return it, or unquote it again just in case of double encoding (rare).
            return urllib.parse.unquote_plus(inner_url)

        # 2. Check for Google image redirect/wrapper
        #    Examples:
        #    https://www.google.com/url?sa=i&url=ENCODED_TARGET_URL&psig=...
        #    /url?q=ENCODED_TARGET_URL&sa=... (relative path)
        #    https://images.google.com/imgres?imgurl=ACTUAL_URL&imgrefurl=...
        is_google_domain = parsed.netloc.startswith(('www.google.', 'images.google.'))
        is_relative_google_path = parsed.netloc == '' and parsed.path in ('/url', '/imgres')

        if is_google_domain or is_relative_google_path:
            if parsed.path in ('/url', '/imgres') or is_google_domain: # Check path for domain cases too
                target_param_key = None
                if 'url' in query_params: # Common for image results or sa=i links
                    target_param_key = 'url'
                elif 'q' in query_params: # Common for /url?q=
                    target_param_key = 'q'
                elif 'imgurl' in query_params and parsed.path == '/imgres': # Specific to /imgres
                    target_param_key = 'imgurl'

                if target_param_key and query_params[target_param_key]:
                    inner_url = query_params[target_param_key][0]
                    logger.debug(f"Extracted from Google wrapper (param '{target_param_key}'): {inner_url} (from {url_string})")
                    return urllib.parse.unquote_plus(inner_url) # parse_qs already unquotes, this is for safety

        # Add other known wrapper patterns here if necessary (e.g., Bing, Yahoo)
        # Example Bing (hypothetical, check actual Bing patterns):
        # if parsed.netloc.endswith('bing.com') and parsed.path.startswith('/images/async') and 'url' in query_params:
        #    inner_url = query_params['url'][0]
        #    logger.debug(f"Extracted from Bing wrapper: {inner_url} (from {url_string})")
        #    return urllib.parse.unquote_plus(inner_url)

        # If not a known wrapper, return the original URL, but ensure it's standard URL-decoded.
        # This handles cases like "https://example.com/my%20image.jpg"
        final_url = urllib.parse.unquote_plus(url_string)
        if final_url != url_string and not (is_google_domain or is_relative_google_path): # Avoid logging already handled Google URLs as simple unquotes
             logger.debug(f"Unquoted non-wrapper URL: {final_url} (from {url_string})")
        return final_url

    except Exception as e:
        logger.error(f"🔴 Error in extract_true_url_from_wrapper for '{url_string}': {e}", exc_info=False) # exc_info=True for full traceback
        # Fallback: try to unquote the original string, or return as is if unquoting fails
        try:
            return urllib.parse.unquote_plus(url_string)
        except Exception:
            return url_string # Absolute fallback
import re
import chardet
from bs4 import BeautifulSoup
import logging
import pandas as pd
import urllib.parse # Added for the new function

# Assuming LR class is in icon_image_lib.LR; otherwise, include it directly here
from icon_image_lib.LR import LR # Make sure this import works

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# It's good practice to get a logger instance per module
# logger = logging.getLogger(__name__) # This would be used if not passing logger around

def clean_source_url(s): # Your existing function
    """Clean and decode URL string by replacing encoded characters."""
    if not isinstance(s, str): # robustness
        return s
    simplified_str = s.replace('\\\\', '') # Consider if this is always correct or if `codecs.decode(s, 'unicode_escape')` might be better for some inputs
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

def clean_image_url(url): # Your existing function
    """Extract the base image URL without query parameters if it matches image extensions."""
    if not isinstance(url, str): # robustness
        return url
    # This pattern specifically targets URLs ending with common image extensions
    # and strips query parameters from *them*.
    pattern = re.compile(r'(.*\.(?:png|jpg|jpeg|gif|webp|avif|svg))(?:\?.*)?', re.IGNORECASE) # Added more extensions
    match = pattern.match(url)
    return match.group(1) if match else url

def decode_html_bytes(html_bytes, logger): # Your existing function
    """Decode HTML bytes to string with fallback encoding."""
    try:
        detected_encoding = chardet.detect(html_bytes)['encoding'] or 'utf-8'
        logger.debug(f"Detected encoding: {detected_encoding}")
        return html_bytes.decode(detected_encoding, errors='replace')
    except Exception as e:
        logger.error(f"🔴 Decoding error: {e}", exc_info=True)
        return html_bytes.decode('utf-8', errors='replace')

# The extract_true_url_from_wrapper function defined above should be here

def get_original_images(html_bytes, logger=None):
    """Extract image data from Google image search HTML."""
    logger = logger or logging.getLogger(__name__) # Ensure logger is available
    html_content = decode_html_bytes(html_bytes, logger)

    start_tag = 'FINANCE",[22,1]]]]]'
    end_tag = ':[null,null,null,"glbl'
    matched_google_image_data = LR().get(html_content, start_tag, end_tag)
    
    if 'Error' in matched_google_image_data or not matched_google_image_data:
        logger.warning('Main results tags not found or no data extracted (LR step)')
        return [], [], [], []
    
    # This part handles \uXXXX escapes if they are Python string literals
    thumbnails_data_str = str(matched_google_image_data).replace('\u003d', '=').replace('\u0026', '&')
    if '"2003":' not in thumbnails_data_str:
        logger.warning('No "2003" tag found in main thumbnails data')
        return [], [], [], []
    
    # Thumbnails (usually direct, like encrypted-tbn0.gstatic.com)
    raw_thumbs = re.findall(r'\[\"(https:\/\/encrypted-tbn0\.gstatic\.com\/images\?.*?)\",\d+,\d+\]', thumbnails_data_str)
    main_thumbs = [clean_source_url(url) for url in raw_thumbs] # Apply uXXXX decoding

    main_descriptions = re.findall(r'"2003":\[null,"[^"]*","[^"]*","(.*?)"', thumbnails_data_str)
    
    # Source URLs (page where image is found)
    raw_source_urls = re.findall(r'"2003":\[null,"[^"]*","(.*?)"', thumbnails_data_str)
    main_source_urls = []
    for url in raw_source_urls:
        s1 = clean_source_url(url) # Handles uXXXX literals
        s2 = extract_true_url_from_wrapper(s1, logger) # Handles wrappers & %-decode
        main_source_urls.append(s2)
        
    # Main Image URLs (should be direct image links)
    # Regex to find image URLs; removed_thumbs is thumbnails_data_str after removing thumbnail patterns
    removed_thumbs_data = re.sub(r'\[\"(https:\/\/encrypted-tbn0\.gstatic\.com\/images\?.*?)\",\d+,\d+\]', "", thumbnails_data_str)
    raw_main_image_urls = re.findall(r"(?:|,),\[\"(https:|http.*?)\",\d+,\d+\]", removed_thumbs_data)
    main_image_urls = []
    for url in raw_main_image_urls:
        img1 = clean_source_url(url) # Handles uXXXX literals
        img2 = extract_true_url_from_wrapper(img1, logger) # Handles wrappers & %-decode
        img3 = clean_image_url(img2) # Strips query params from actual image URL
        main_image_urls.append(img3)
    
    if not main_image_urls and main_thumbs: # Fallback if direct images not found
        logger.info("No main_image_urls found, using main_thumbs as fallback for image URLs.")
        # Thumbs are already processed by clean_source_url. They are less likely to be wrappers.
        # If they need to be treated as full image URLs, they might need clean_image_url too.
        main_image_urls = [clean_image_url(thumb_url) for thumb_url in main_thumbs]


    min_length = min(len(main_image_urls), len(main_descriptions), len(main_source_urls), len(main_thumbs))
    # Truncate lists to the minimum common length to ensure DataFrame integrity
    main_image_urls = main_image_urls[:min_length][:50]
    main_descriptions = main_descriptions[:min_length][:50]
    main_source_urls = main_source_urls[:min_length][:50]
    main_thumbs = main_thumbs[:min_length][:50]

    logger.debug(f"GetOriginal: URLs={len(main_image_urls)}, Desc={len(main_descriptions)}, Sources={len(main_source_urls)}, Thumbs={len(main_thumbs)}")
    return main_image_urls, main_descriptions, main_source_urls, main_thumbs

def get_results_page_results(html_bytes, final_urls, final_descriptions, final_sources, final_thumbs, logger=None):
    """Extract additional image data from results page HTML without base64."""
    logger = logger or logging.getLogger(__name__) # Ensure logger
    html_content = decode_html_bytes(html_bytes, logger)

    soup = BeautifulSoup(html_content, 'html.parser')
    # Class names for Google search results can change, verify these if issues arise
    result_divs = soup.find_all('div', class_='H8Rx8c') # This class seems to be for "Related images" or similar blocks

    if not result_divs:
        # Try another common class for image result items if H8Rx8c fails
        result_divs = soup.find_all('div', class_='isv motiva_DRHBO') # Example: for individual image results
        if not result_divs:
            result_divs = soup.find_all('a', class_='isv-r') # Another common pattern for image items
            if not result_divs:
                 logger.warning("No primary result item divs (H8Rx8c, isv motiva_DRHBO, isv-r) found in additional results page")
                 return final_urls, final_descriptions, final_sources, final_thumbs

    logger.info(f"Found {len(result_divs)} potential items in additional results page.")

    for item_container in result_divs:
        if len(final_urls) >= 100: # Overall cap
            break

        # Thumbnail
        # Common img tags: 'rg_i Q4LuWd', 'n3VNCb KAlRDb', 'YQ4gaf'
        img_tag = item_container.find('img', class_=lambda c: c and any(cls in c for cls in ['rg_i', 'n3VNCb', 'YQ4gaf', 'gdOPf', 'uhHOwf', 'ez24Df']))
        raw_thumb_url = None
        if img_tag:
            raw_thumb_url = img_tag.get('src') or img_tag.get('data-src')
        
        if not raw_thumb_url or 'data:image' in raw_thumb_url: # Skip base64
            logger.debug("No valid thumbnail URL or base64 src in result item, skipping.")
            continue
        
        thumb = clean_source_url(raw_thumb_url) # uXXXX decode for thumb
        # Thumbnails are usually direct, no need for extract_true_url_from_wrapper or clean_image_url
        final_thumbs.append(thumb)

        # Description
        # Common description/title holders: 'bytUYc', 'VFACy kGQAp S2WUTe', 'mVDVAe'
        desc_element = item_container.find(['div', 'span', 'a'], class_=lambda c: c and any(cls in c for cls in ['bytUYc', 'VFACy', 'mVDVAe', 'VwiC3b']))
        description = desc_element.get_text(strip=True) if desc_element else 'No description'
        final_descriptions.append(description)

        # Source (often the domain name or page title linking to the source page)
        # Common source holders: 'VuuXrf', 'SW5pqf', 'cite' with class 'qLRx3b'
        source_element = item_container.find(['cite', 'div', 'span'], class_=lambda c: c and any(cls in c for cls in ['VuuXrf', 'SW5pqf', 'qLRx3b']))
        raw_source_text = source_element.get_text(strip=True) if source_element else 'No source'
        
        # Process source text: clean uXXXX, then extract if it's a wrapper URL
        # Check if it looks like a URL before trying to extract from wrapper
        cleaned_source_text = clean_source_url(raw_source_text)
        if cleaned_source_text.startswith(('http', '/', 'www.')):
            source = extract_true_url_from_wrapper(cleaned_source_text, logger)
        else:
            source = cleaned_source_text # It's just text, not a URL
        final_sources.append(source)

        # Main Image URL (often found in a link wrapping the image or a data attribute)
        # Common link tags: 'ایش zReHs', 'VFACy kGQAp S2WUTe', 'isv-r' (if item_container is this)
        # Sometimes the link is the item_container itself if it's an <a> tag
        link_tag = None
        if item_container.name == 'a':
            link_tag = item_container
        else:
            link_tag = item_container.find('a', class_=lambda c: c and any(cls in c for cls in ['zReHs', 'VFACy', 'isv-r']))

        raw_href = link_tag.get('href') if link_tag and link_tag.get('href') else None
        
        # Alternative: Look for data-actualn3r (or similar) on img_tag for direct full image URL
        if not raw_href and img_tag:
             raw_href = img_tag.get('data-actualn3r') # This sometimes holds the direct image link

        image_url_final = 'No image URL'
        if raw_href:
            url1 = clean_source_url(raw_href)
            url2 = extract_true_url_from_wrapper(url1, logger)
            image_url_final = clean_image_url(url2)
        final_urls.append(image_url_final)

    if len(final_urls) > 100: # Cap after processing
        final_urls = final_urls[:100]
        final_descriptions = final_descriptions[:100]
        final_sources = final_sources[:100]
        final_thumbs = final_thumbs[:100]
        logger.debug("Capped total results from additional page processing at 100")

    logger.debug(f"GetResultsPage: Total after append: URLs={len(final_urls)}")
    return final_urls, final_descriptions, final_sources, final_thumbs


def process_search_result(image_html_bytes, entry_id: int, logger=None) -> pd.DataFrame:
    """Process search result HTML bytes and return a DataFrame with image data."""
    logger = logger or logging.getLogger(__name__) # Ensure logger
    # It seems image_html_bytes is used for both original and results_page_results
    # This implies it contains all necessary data.
    
    # First, try to get images from the script/data block
    final_urls, final_descriptions, final_sources, final_thumbs = get_original_images(image_html_bytes, logger)
    
    # Then, try to get images from structured HTML elements (if any, or as supplement)
    # Pass the *current* lists to be appended to.
    final_urls, final_descriptions, final_sources, final_thumbs = get_results_page_results(
        image_html_bytes, final_urls, final_descriptions, final_sources, final_thumbs, logger
    )
    
    # Ensure all lists have the same length before creating DataFrame
    all_lists = [final_urls, final_descriptions, final_sources, final_thumbs]
    if not all_lists: # Should not happen if functions return empty lists
        logger.warning(f"EntryID {entry_id}: No data extracted. Returning empty DataFrame.")
        return pd.DataFrame()
        
    min_len = min(len(lst) for lst in all_lists)
    
    # Check if any list was longer and got truncated, indicating a potential mismatch
    if any(len(lst) > min_len for lst in all_lists):
        logger.warning(
            f"EntryID {entry_id}: Data lists had different lengths "
            f"({[len(lst) for lst in all_lists]}). Truncated to min length: {min_len}."
        )

    df = pd.DataFrame({
        'EntryID': [entry_id] * min_len,
        'ImageUrl': final_urls[:min_len],
        'ImageDesc': final_descriptions[:min_len],
        'ImageSource': final_sources[:min_len],
        'ImageUrlThumbnail': final_thumbs[:min_len]
    })
    
    logger.info(f"Processed EntryID {entry_id} with {len(df)} images (after potential truncation).")
    return df
def process_web_search_result(html_content: bytes, logger: logging.Logger) -> pd.DataFrame:
    """
    Parses the HTML content from a standard Google web search results page.

    Args:
        html_content: The byte string of the HTML page.
        logger: A logging instance for output.

    Returns:
        A pandas DataFrame containing the search results with columns
        ['title', 'link', 'snippet'], or an empty DataFrame if parsing fails.
    """
    results = []
    if not html_content:
        logger.warning("HTML content provided to web parser was empty.")
        return pd.DataFrame()

    try:
        soup = BeautifulSoup(html_content, 'html.parser')

        # Google's main container for organic search results. This selector is relatively stable.
        # It avoids ads, "People also ask", and other widgets.
        for result_container in soup.select('div.g'):
            try:
                # Find the title and link
                link_tag = result_container.select_one('a')
                if not link_tag or not link_tag.has_attr('href'):
                    continue # Skip if there's no link

                link = link_tag['href']
                title_tag = result_container.select_one('h3')
                title = title_tag.get_text() if title_tag else "No Title Found"

                # Find the snippet/description
                # Google uses different structures, so we try a few common ones.
                snippet_tag = result_container.select_one('div[data-sncf="1"]')
                snippet = snippet_tag.get_text() if snippet_tag else "No Snippet Found"

                # Basic validation to filter out irrelevant links
                if link.startswith("http") and not "google.com/search" in link:
                    results.append({
                        'title': title,
                        'link': link,
                        'snippet': snippet
                    })

            except Exception as e_parse_item:
                logger.warning(f"Could not parse a specific search result item: {e_parse_item}", exc_info=False)
                continue # Move to the next result container

        if not results:
            logger.warning("BeautifulSoup parsing finished but found no valid web result items.")

    except Exception as e_parse_main:
        logger.error(f"An error occurred during BeautifulSoup parsing of the web page: {e_parse_main}", exc_info=True)
        return pd.DataFrame()

    return pd.DataFrame(results)

