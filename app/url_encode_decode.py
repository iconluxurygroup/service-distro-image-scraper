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
        # Parse URL components
        parsed = urllib.parse.urlparse(url)
        # Encode path, preserving slashes
        path = urllib.parse.quote(parsed.path, safe='/')
        # Encode query parameters, handling multi-valued queries
        query = urllib.parse.urlencode(urllib.parse.parse_qs(parsed.query), doseq=True) if parsed.query else ''
        # Encode fragment
        fragment = urllib.parse.quote(parsed.fragment, safe='') if parsed.fragment else ''
        # Reconstruct URL
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
        # Remove all backslashes aggressively
        cleaned = re.sub(r'\\+', '', url)
        # Remove encoded backslashes (%5C, %255C, etc.)
        cleaned = re.sub(r'%25{0,2}5[Cc]', '', cleaned)
        # Fix Google-specific patterns
        cleaned = cleaned.replace('q=tbn', 'q=tbn').replace('q=tbn:', 'q=tbn:').replace('tbn\\:', 'tbn:')
        # Handle other common escape mistakes
        cleaned = cleaned.replace('&=s', '&s').replace('&=t', '&t')
        # Repeatedly unquote to resolve double-encoding
        decoded = cleaned
        for _ in range(10):  # Increased for stubborn cases
            new_decoded = urllib.parse.unquote(decoded)
            if new_decoded == decoded:
                break
            decoded = new_decoded
            logger.debug(f"Unquote iteration: {decoded}")
        # Parse URL to normalize components
        parsed = urllib.parse.urlparse(decoded)
        # Reconstruct query parameters
        query = urllib.parse.urlencode(urllib.parse.parse_qs(parsed.query), doseq=True) if parsed.query else ''
        # Reconstruct URL
        final_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if query:
            final_url += f"?{query}"
        if parsed.fragment:
            final_url += f"#{urllib.parse.quote(parsed.fragment, safe='')}"
        # Validate URL structure
        if not parsed.scheme or not parsed.netloc:
            logger.warning(f"Invalid URL structure after decoding: {final_url}")
            return url
        logger.debug(f"Decoded URL: {final_url}")
        return final_url
    except Exception as e:
        logger.error(f"Error decoding URL {url}: {e}", exc_info=True)
        return url