import urllib.parse
import logging
import re
from typing import Optional

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

def double_encode_plus(url: str, logger: Optional[logging.Logger] = None) -> str:
    logger = logger or default_logger
    logger.debug(f"Encoding URL: {url}")
    try:
        parsed = urllib.parse.urlparse(url)
        path = urllib.parse.quote(parsed.path, safe='/')
        query_dict = urllib.parse.parse_qs(parsed.query)
        query = urllib.parse.urlencode(query_dict, doseq=True) if query_dict else ''
        fragment = urllib.parse.quote(parsed.fragment, safe='') if parsed.fragment else ''
        encoded = f"{parsed.scheme}://{parsed.netloc}{path}"
        if query:
            encoded += f"?{query}"
        if fragment:
            encoded += f"#{fragment}"
        logger.debug(f"Double-encoded URL: {encoded}")
        return encoded
    except Exception as e:
        logger.error(f"Error encoding URL {url}: {e}", exc_info=True)
        return url

def decode_url(url: str, logger: Optional[logging.Logger] = None) -> str:
    logger = logger or default_logger
    logger.debug(f"Decoding URL: {url}")
    try:
        # Check for Google thumbnail URL
        if 'tbn' in url.lower() or 'tbn%3A' in url:
            logger.debug("Detected Google thumbnail URL")
            # Remove backslashes and encoded backslashes
            cleaned = re.sub(r'\\+', '', url)
            cleaned = re.sub(r'%25{0,2}5[Cc]', '', cleaned)
            # Extract thumbnail ID (between 'tbn:' or 'tbn' and '&s' or end)
            tbn_match = re.search(r'tbn[:%3A]([A-Za-z0-9_-]+)(?:\\&s|&s|$)', cleaned)
            if tbn_match:
                thumbnail_id = tbn_match.group(1)
                # Rebuild using template
                decoded = f"https://encrypted-tbn0.gstatic.com/images?q=tbn:{thumbnail_id}&s"
                logger.debug(f"Rebuilt Google thumbnail URL: {decoded}")
            else:
                logger.warning("Failed to extract thumbnail ID, falling back to generic decoding")
                decoded = urllib.parse.unquote(cleaned)
        else:
            # Generic URL handling
            cleaned = re.sub(r'\\+', '', url)
            cleaned = re.sub(r'%25{0,2}5[Cc]', '', cleaned)
            decoded = urllib.parse.unquote(cleaned)
        # Validate URL
        parsed = urllib.parse.urlparse(decoded)
        if not parsed.scheme or not parsed.netloc:
            logger.warning(f"Invalid URL structure after decoding: {decoded}")
            return url
        logger.debug(f"Decoded URL: {decoded}")
        return decoded
    except Exception as e:
        logger.error(f"Error decoding URL {url}: {e}", exc_info=True)
        return url