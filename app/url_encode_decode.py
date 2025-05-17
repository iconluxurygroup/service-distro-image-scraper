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
        # Remove backslashes and fix specific Google thumbnail patterns
        cleaned = re.sub(r'\\([=:&])', r'\1', url)  # Remove \ before =, :, &
        cleaned = cleaned.replace('q\=tbn', 'q=tbn').replace('q\\=tbn', 'q=tbn')
        cleaned = cleaned.replace('tbn\\:', 'tbn:').replace('tbn%3A', 'tbn:')
        # Remove encoded backslashes
        cleaned = re.sub(r'%25{0,2}5[Cc]', '', cleaned)
        # Unquote to handle percent-encoding
        decoded = cleaned
        for _ in range(3):  # Reduced iterations for simplicity
            new_decoded = urllib.parse.unquote(decoded)
            if new_decoded == decoded:
                break
            decoded = new_decoded
            logger.debug(f"Unquote iteration: {decoded}")
        # Parse and normalize query parameters
        parsed = urllib.parse.urlparse(decoded)
        query_dict = urllib.parse.parse_qs(parsed.query)
        # Ensure Google thumbnail 'q' parameter is correct
        if 'q' in query_dict and any('tbn:' in q for q in query_dict['q']):
            query_dict['q'] = [q.replace('tbn\\:', 'tbn:').replace('tbn%3A', 'tbn:') for q in query_dict['q']]
        query = urllib.parse.urlencode(query_dict, doseq=True) if query_dict else ''
        # Reconstruct URL
        final_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if query:
            final_url += f"?{query}"
        if parsed.fragment:
            final_url += f"#{urllib.parse.quote(parsed.fragment, safe='')}"
        # Validate URL
        if not parsed.scheme or not parsed.netloc:
            logger.warning(f"Invalid URL structure after decoding: {final_url}")
            return url
        logger.debug(f"Decoded URL: {final_url}")
        return final_url
    except Exception as e:
        logger.error(f"Error decoding URL {url}: {e}", exc_info=True)
        return url