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
        # Replace '+' with '%2B' to preserve it
        first_pass = url.replace('+', '%2B')
        # URL-encode, preserving safe characters for HTTP requests
        encoded = urllib.parse.quote(first_pass, safe=':/?=&')
        logger.debug(f"Double-encoded URL: {encoded}")
        return encoded
    except Exception as e:
        logger.error(f"Error encoding URL {url}: {e}", exc_info=True)
        return url

def decode_url(url: str, logger: Optional[logging.Logger] = None) -> str:
    logger = logger or default_logger
    logger.debug(f"Decoding URL: {url}")
    try:
        # Remove literal backslashes and common escape sequences
        cleaned = re.sub(r'\\([=&#])', r'\1', url)
        # Replace escaped backslash (%5C) with nothing
        cleaned = cleaned.replace('%5C', '').replace('%5c', '')
        # Repeatedly unquote to handle double-encoding
        decoded = cleaned
        for _ in range(3):  # Max 3 iterations to avoid infinite loops
            new_decoded = urllib.parse.unquote(decoded)
            if new_decoded == decoded:
                break
            decoded = new_decoded
        # Validate URL structure
        parsed = urllib.parse.urlparse(decoded)
        if not parsed.scheme or not parsed.netloc:
            logger.warning(f"Invalid URL structure after decoding: {decoded}")
            return url
        logger.debug(f"Decoded URL: {decoded}")
        return decoded
    except Exception as e:
        logger.error(f"Error decoding URL {url}: {e}", exc_info=True)
        return url