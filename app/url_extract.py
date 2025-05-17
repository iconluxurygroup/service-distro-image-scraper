import logging
import re
from typing import Optional

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

def extract_thumbnail_url(url: str, logger: Optional[logging.Logger] = None) -> str:
    logger = logger or default_logger
    logger.debug(f"Processing URL: {url}")
    try:
        # Detect Google thumbnail URL
        if 'tbn' in url.lower():
            logger.debug("Detected Google thumbnail URL")
            # Extract thumbnail ID (between 'tbn' or 'tbn:' and '&s', '\&s', or end)
            tbn_match = re.search(r'tbn[:%3A]?([A-Za-z0-9_-]+)(?:[\\&]?s|$)', url, re.IGNORECASE)
            if tbn_match:
                thumbnail_id = tbn_match.group(1)
                # Rebuild using template
                rebuilt_url = f"https://encrypted-tbn0.gstatic.com/images?q=tbn:{thumbnail_id}"
                logger.debug(f"Rebuilt thumbnail URL: {rebuilt_url}")
                return rebuilt_url
            else:
                logger.warning("Failed to extract thumbnail ID")
        # Return unchanged for non-Google URLs
        logger.debug("Non-Google URL, returning unchanged")
        return url
    except Exception as e:
        logger.error(f"Error processing URL {url}: {e}", exc_info=True)
        return url