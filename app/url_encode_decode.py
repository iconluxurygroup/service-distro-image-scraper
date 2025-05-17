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
        # Parse URL to handle components separately
        parsed = urllib.parse.urlparse(url)
        # Encode path, preserving slashes
        path = urllib.parse.quote(parsed.path, safe='/')
        # Encode query parameters
        query = urllib.parse.urlencode(urllib.parse.parse_qs(parsed.query), doseq=True) if parsed.query else ''
        # Encode fragment
        fragment = urllib.parse.quote(parsed.fragment) if parsed.fragment else ''
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
        # Remove all backslashes except those in valid escape sequences
        cleaned = re.sub(r'\\([^\\])', r'\1', url)
        # Remove encoded backslashes (%5C, %255C)
        cleaned = re.sub(r'%25?5[Cc]', '', cleaned)
        # Replace common escape mistakes
        cleaned = cleaned.replace('q\\=tbn', 'q=tbn').replace('q=tbn', 'q=tbn')
        # Repeatedly unquote to handle double-encoding
        decoded = cleaned
        for _ in range(5):  # Increased iterations for complex cases
            new_decoded = urllib.parse.unquote(decoded)
            if new_decoded == decoded:
                break
            decoded = new_decoded
        # Parse and normalize query parameters
        parsed = urllib.parse.urlparse(decoded)
        if parsed.query:
            query_dict = urllib.parse.parse_qs(parsed.query)
            normalized_query = urllib.parse.urlencode(query_dict, doseq=True)
            decoded = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            if normalized_query:
                decoded += f"?{normalized_query}"
            if parsed.fragment:
                decoded += f"#{parsed.fragment}"
        # Validate URL
        if not parsed.scheme or not parsed.netloc:
            logger.warning(f"Invalid URL structure after decoding: {decoded}")
            return url
        logger.debug(f"Decoded URL: {decoded}")
        return decoded
    except Exception as e:
        logger.error(f"Error decoding URL {url}: {e}", exc_info=True)
        return url