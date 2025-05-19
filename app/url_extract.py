import logging
import re
from typing import Optional

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


async def extract_thumbnail_url(url: str, logger: Optional[logging.Logger] = None) -> str:
    logger = logger or default_logger
    logger.debug(f"Processing URL: {url}")
    try:
        if 'tbn' in url.lower():
            logger.debug("Detected Google thumbnail URL")
            tbn_match = re.search(r'(?:q[\\=]+tbn|tbn[:%3A]?)([A-Za-z0-9_-]{43})(?:[\\&]?s|$)', url, re.IGNORECASE)
            if tbn_match:
                thumbnail_id = tbn_match.group(1)
                logger.debug(f"Extracted thumbnail ID: {thumbnail_id} (length: {len(thumbnail_id)})")
                rebuilt_url = f"https://encrypted-tbn0.gstatic.com/images?q=tbn:{thumbnail_id}&s"
                logger.debug(f"Rebuilt thumbnail URL: {rebuilt_url}")
                return rebuilt_url
            else:
                logger.warning(f"Failed to extract thumbnail ID from {url}")
                return url
        logger.debug("Non-Google URL, returning unchanged")
        return url
    except Exception as e:
        logger.error(f"Error processing URL {url}: {e}", exc_info=True)
        return url