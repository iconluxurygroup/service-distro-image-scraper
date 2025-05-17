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
        # Remove all backslashes
        cleaned = re.sub(r'\\+', '', url)
        # Remove encoded backslashes (%5C, %255C)
        cleaned = re.sub(r'%25{0,2}5[Cc]', '', cleaned)
        # Handle Google image thumbnail URLs
        is_google_thumb = 'tbn:' in cleaned or 'tbn%3A' in cleaned
        if is_google_thumb:
            logger.debug("Detected Google thumbnail URL")
            # Extract metadata
            parsed = urllib.parse.urlparse(cleaned)
            query_dict = urllib.parse.parse_qs(parsed.query)
            # Fix 'q' parameter for tbn
            if 'q' in query_dict:
                q_values = query_dict['q']
                fixed_q = []
                for q in q_values:
                    # Remove backslashes and fix tbn
                    q = re.sub(r'\\+', '', q)
                    if 'tbn' in q:
                        # Ensure tbn: is intact
                        q = q.replace('tbn\\:', 'tbn:').replace('tbn%3A', 'tbn:')
                        # Extract tbn ID
                        tbn_match = re.search(r'tbn:([A-Za-z0-9_-]+)', q)
                        if tbn_match:
                            tbn_id = tbn_match.group(1)
                            fixed_q.append(f"tbn:{tbn_id}")
                        else:
                            fixed_q.append(q)
                    else:
                        fixed_q.append(q)
                query_dict['q'] = fixed_q
            # Normalize other query parameters
            for key, values in query_dict.items():
                query_dict[key] = [re.sub(r'\\+', '', v) for v in values]
            # Reconstruct query
            query = urllib.parse.urlencode(query_dict, doseq=True) if query_dict else ''
            # Reconstruct URL
            decoded = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            if query:
                decoded += f"?{query}"
            if parsed.fragment:
                decoded += f"#{urllib.parse.quote(parsed.fragment, safe='')}"
        else:
            # Generic URL handling
            decoded = cleaned
            for _ in range(5):
                new_decoded = urllib.parse.unquote(decoded)
                if new_decoded == decoded:
                    break
                decoded = new_decoded
                logger.debug(f"Unquote iteration: {decoded}")
            parsed = urllib.parse.urlparse(decoded)
            query_dict = urllib.parse.parse_qs(parsed.query)
            query = urllib.parse.urlencode(query_dict, doseq=True) if query_dict else ''
            decoded = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            if query:
                decoded += f"?{query}"
            if parsed.fragment:
                decoded += f"#{urllib.parse.quote(parsed.fragment, safe='')}"
        # Validate URL
        if not parsed.scheme or not parsed.netloc:
            logger.warning(f"Invalid URL structure after decoding: {decoded}")
            return url
        logger.debug(f"Decoded URL: {decoded}")
        return decoded
    except Exception as e:
        logger.error(f"Error decoding URL {url}: {e}", exc_info=True)
        return url