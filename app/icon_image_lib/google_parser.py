# icon_image_lib/google_parser.py
import re
import chardet
from icon_image_lib.LR import LR
import logging

def get_original_images(html_bytes, logger=None):
    logger = logger or logging.getLogger(__name__)
    # ... rest as provided previously

def get_original_images(html_bytes, logger=None):
    """Parse Google image search HTML and return image data."""
    logger = logger or logging.getLogger(__name__)
    try:
        detected_encoding = chardet.detect(html_bytes)['encoding']
        if not detected_encoding:
            logger.warning("No encoding detected, falling back to UTF-8")
            detected_encoding = 'utf-8'
        try:
            soup = html_bytes.decode(detected_encoding)
        except UnicodeDecodeError:
            logger.warning(f"Failed to decode with detected encoding '{detected_encoding}', using UTF-8 with replacement")
            soup = html_bytes.decode('utf-8', errors='replace')  # Replace invalid chars
    except Exception as e:
        logger.error(f"🔴 Decoding error: {e}", exc_info=True)
        soup = html_bytes.decode('utf-8', errors='replace')  # Fallback with replacement
    
    start_tag = 'FINANCE",[22,1]]]]]'
    end_tag = ':[null,null,null,"glbl'
    matched_google_image_data = LR().get(soup, start_tag, end_tag)
    if 'Error' in matched_google_image_data:
        logger.warning('Tags not found in HTML')
        return None
    if not matched_google_image_data:
        logger.warning('No matched_google_image_data found')
        return (['No start_tag or end_tag'], ['No start_tag or end_tag'], ['No start_tag or end_tag'], ['No start_tag or end_tag'])
    
    matched_google_image_data = str(matched_google_image_data).replace('\u003d', '=').replace('\u0026', '&')
    thumbnails = matched_google_image_data
    
    if '"2003":' not in thumbnails:
        logger.warning('No 2003 tag found in thumbnails')
        return (['No google image results found'], ['No google image results found'], ['No google image results found'], ['No google image results found'])
    
    matched_google_images_thumbnails = ", ".join(
        re.findall(r'\[\"(https\:\/\/encrypted-tbn0\.gstatic\.com\/images\?.*?)\",\d+,\d+\]', str(thumbnails))
    ).split(", ")
    
    regex_pattern_desc = r'"2003":\[null,"[^"]*","[^"]*","(.*?)"'
    matched_description = re.findall(regex_pattern_desc, str(thumbnails))
    
    regex_pattern_src = r'"2003":\[null,"[^"]*","(.*?)"'
    matched_source = re.findall(regex_pattern_src, str(thumbnails))
    
    removed_matched_google_images_thumbnails = re.sub(
        r'\[\"(https\:\/\/encrypted-tbn0\.gstatic\.com\/images\?.*?)\",\d+,\d+\]', "", str(thumbnails)
    )
    
    matched_google_full_resolution_images = re.findall(
        r"(?:|,),\[\"(https:|http.*?)\",\d+,\d+\]", removed_matched_google_images_thumbnails
    )
    
    full_res_images = [
        bytes(bytes(img, "utf-8").decode("unicode-escape"), "utf-8").decode("unicode-escape")
        for img in matched_google_full_resolution_images
    ]
    cleaned_urls = [clean_image_url(url) for url in full_res_images]
    cleaned_source = [clean_source_url(url) for url in matched_source]
    cleaned_thumbs = [clean_source_url(url) for url in matched_google_images_thumbnails]
    
    if not cleaned_urls:
        cleaned_urls = cleaned_thumbs
    
    final_thumbnails = []
    final_full_res_images = []
    final_descriptions = []
    
    if len(cleaned_urls) >= 8:
        logger.debug('Found 8 or more image results')
        if len(matched_description) < 8:
            matched_description = matched_description + ["No descriptions found"] * (8 - len(matched_description))
        if len(cleaned_source) < 8:
            cleaned_source = cleaned_source + ["No sources found"] * (8 - len(cleaned_source))
        if len(cleaned_thumbs) < 8:
            cleaned_thumbs = cleaned_thumbs + ["No thumbnails found"] * (8 - len(cleaned_thumbs))
        
        final_image_urls = cleaned_urls[:8]
        final_descriptions = matched_description[:8]
        final_source_url = cleaned_source[:8]
        final_thumbs = cleaned_thumbs[:8]
        
        logger.debug(f"Returning 8 images: URLs={len(final_image_urls)}, Descriptions={len(final_descriptions)}, Sources={len(final_source_url)}, Thumbs={len(final_thumbs)}")
        max_length = max(len(final_image_urls), len(final_descriptions), len(final_source_url), len(final_thumbs))
        min_length = min(len(final_image_urls), len(final_descriptions), len(final_source_url), len(final_thumbs))
        if max_length != min_length:
            logger.error(f"Length mismatch: URLs={len(final_image_urls)}, Descriptions={len(final_descriptions)}, Sources={len(final_source_url)}, Thumbs={len(final_thumbs)}")
            raise ValueError("Mismatch in lengths of image data arrays")
        
        return final_image_urls, final_descriptions, final_source_url, final_thumbs
    else:
        logger.debug('Found fewer than 8 image results')
        min_length = len(cleaned_urls)
        logger.debug(f"Processing {min_length} images: URLs={len(cleaned_urls)}, Descriptions={len(matched_description)}, Sources={len(cleaned_source)}, Thumbs={len(cleaned_thumbs)}")
        
        if len(matched_description) < min_length:
            matched_description = matched_description + ["No descriptions found"] * (min_length - len(matched_description))
        if len(cleaned_source) < min_length:
            cleaned_source = cleaned_source + ["No sources found"] * (min_length - len(cleaned_source))
        if len(cleaned_thumbs) < min_length:
            cleaned_thumbs = cleaned_thumbs + ["No thumbnails found"] * (min_length - len(cleaned_thumbs))
        
        final_image_urls = cleaned_urls[:min_length]
        final_descriptions = matched_description[:min_length]
        final_source_url = cleaned_source[:min_length]
        final_thumbs = cleaned_thumbs[:min_length]
        
        logger.debug(f"Returning {min_length} images: URLs={len(final_image_urls)}, Descriptions={len(final_descriptions)}, Sources={len(final_source_url)}, Thumbs={len(final_thumbs)}")
        max_length = max(len(final_image_urls), len(final_descriptions), len(final_source_url), len(final_thumbs))
        min_length = min(len(final_image_urls), len(final_descriptions), len(final_source_url), len(final_thumbs))
        if max_length != min_length:
            logger.error(f"Length mismatch: URLs={len(final_image_urls)}, Descriptions={len(final_descriptions)}, Sources={len(final_source_url)}, Thumbs={len(final_thumbs)}")
            raise ValueError("Mismatch in lengths of image data arrays")
        
        return final_image_urls, final_descriptions, final_source_url, final_thumbs

# ... clean_image_url and clean_source_url remain unchanged
def clean_source_url(s):
    # First, remove '\\\\' to simplify handling
    simplified_str = s.replace('\\\\', '')

    # Mapping of encoded sequences to their decoded characters
    replacements = {
        'u0026': '&',
        'u003d': '=',
        'u003f': '?',
        'u0020': ' ',
        'u0025': '%',
        'u002b': '+',
        'u003c': '<',
        'u003e': '>',
        'u0023': '#',
        'u0024': '$',
        'u002f': '/',
        'u005c': '\\',
        'u007c': '|',
        'u002d': '-',
        'u003a': ':',
        'u003b': ';',
        'u002c': ',',
        'u002e': '.',
        'u0021': '!',
        'u0040': '@',
        'u005e': '^',
        'u0060': '`',
        'u007b': '{',
        'u007d': '}',
        'u005b': '[',
        'u005d': ']',
        'u002a': '*',
        'u0028': '(',
        'u0029': ')'
    }

    # Apply the replacements
    for encoded, decoded in replacements.items():
        simplified_str = simplified_str.replace(encoded, decoded)

    return simplified_str
def clean_image_url(url):
    # Pattern matches common image file extensions followed by a question mark and any characters after it
    pattern = re.compile(r'(.*\.(?:png|jpg|jpeg|gif))(?:\?.*)?', re.IGNORECASE)

    # Search for matches in the input URL
    match = pattern.match(url)

    # If a match is found, return the part of the URL before the query parameters (group 1)
    if match:
        return match.group(1)

    # If no match is found, return the original URL
    return url

# with open("text.html", "r", encoding='utf-8') as f:
#     html_content = f.read()
#     results = get_original_images(html_content)
#     print(results)