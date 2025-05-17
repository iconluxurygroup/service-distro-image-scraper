import os
import asyncio
import logging
import aiohttp
import aiofiles
from typing import List, Dict, Tuple, Optional
from urllib.parse import quote, urlparse, parse_qs, urlencode
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import re

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

def clean_url(url: str) -> str:
    """Clean URLs by fixing common issues like backslashes or double-encoding."""
    # Replace backslashes with forward slashes or remove them
    url = re.sub(r'\\+', '/', url)
    # Remove erroneous %5C or literal backslashes in query strings
    url = url.replace('%5C', '')
    # Decode any double-encoded % signs (e.g., %25 -> %)
    try:
        while '%25' in url:
            url = url.replace('%25', '%')
    except Exception as e:
        default_logger.warning(f"Error decoding URL {url}: {e}")
    return url

def encode_url(url: str) -> str:
    """Encode a URL, preserving scheme, netloc, and reserved characters where appropriate."""
    parsed = urlparse(url)
    # Clean the URL first
    cleaned_url = clean_url(url)
    parsed = urlparse(cleaned_url)
    
    # Encode path and query separately
    path = quote(parsed.path, safe='/')
    if parsed.query:
        # Parse query string into key-value pairs
        query_dict = parse_qs(parsed.query)
        # Encode each value
        encoded_query = urlencode(
            {k: [quote(v, safe='') for v in vs] for k, vs in query_dict.items()},
            doseq=True
        )
    else:
        encoded_query = ''
    
    # Reconstruct URL
    encoded_url = f"{parsed.scheme}://{parsed.netloc}{path}"
    if encoded_query:
        encoded_url += f"?{encoded_query}"
    if parsed.fragment:
        encoded_url += f"#{quote(parsed.fragment)}"
    
    return encoded_url

async def validate_url(url: str, session: aiohttp.ClientSession, logger: logging.Logger) -> bool:
    """Validate if a URL is accessible with a HEAD request."""
    try:
        async with session.head(url, timeout=5) as response:
            if response.status == 200:
                logger.debug(f"URL {url} is accessible")
                return True
            logger.warning(f"URL {url} returned status {response.status}")
            return False
    except aiohttp.ClientError as e:
        logger.warning(f"URL {url} is not accessible: {e}")
        return False

async def download_image(
    url: str,
    filename: str,
    session: aiohttp.ClientSession,
    logger: logging.Logger,
    timeout: int = 30
) -> bool:
    try:
        # Clean and encode the URL
        cleaned_url = clean_url(url)
        encoded_url = encode_url(cleaned_url)
        logger.debug(f"Raw URL: {url}")
        logger.debug(f"Cleaned URL: {cleaned_url}")
        logger.debug(f"Encoded URL: {encoded_url}")

        # Validate URL accessibility
        if not await validate_url(encoded_url, session, logger):
            logger.warning(f"Skipping download for inaccessible URL: {encoded_url}")
            return False

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        async with session.get(encoded_url, timeout=timeout, headers=headers) as response:
            if response.status != 200:
                logger.warning(f"⚠️ HTTP error for image {encoded_url}: {response.status} {response.reason}")
                return False
            async with aiofiles.open(filename, 'wb') as f:
                await f.write(await response.read())
            logger.debug(f"Successfully downloaded {encoded_url} to {filename}")
            return True
    except aiohttp.ClientError as e:
        logger.error(f"❌ HTTP error for image {encoded_url}: {str(e)}")
        return False
    except asyncio.TimeoutError:
        logger.error(f"❌ Timeout downloading image {encoded_url}")
        return False
    except Exception as e:
        logger.error(f"❌ Error downloading image {encoded_url}: {str(e)}", exc_info=True)
        return False

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(Exception),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying download_all_images for {len(retry_state.kwargs['image_list'])} images "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def download_all_images(
    image_list: List[Dict],
    temp_dir: str,
    logger: Optional[logging.Logger] = None,
    batch_size: int = 10
) -> List[Tuple[str, int]]:
    logger = logger or default_logger
    failed_downloads = []
    logger.info(f"📥 Starting download of {len(image_list)} images to {temp_dir}")

    os.makedirs(temp_dir, exist_ok=True)
    logger.info(f"📁 Ensured directory exists: {temp_dir}")

    async def process_image(image: Dict) -> None:
        excel_row_id = image['ExcelRowID']
        main_url = image['ImageUrl']
        thumb_url = image.get('ImageUrlThumbnail', '')
        filename = os.path.join(temp_dir, f"image_{excel_row_id}.jpg")

        async with aiohttp.ClientSession() as session:
            # Try main image
            success = await download_image(main_url, filename, session, logger)
            if not success and thumb_url:
                # Fallback to thumbnail
                logger.debug(f"Falling back to thumbnail {thumb_url} for ExcelRowID {excel_row_id}")
                success = await download_image(thumb_url, filename, session, logger)
            
            if not success:
                logger.error(f"Failed to download both main and thumbnail for ExcelRowID {excel_row_id}")
                failed_downloads.append((main_url, excel_row_id))

    batches = [image_list[i:i + batch_size] for i in range(0, len(image_list), batch_size)]
    for batch_idx, batch in enumerate(batches, 1):
        logger.info(f"Processing batch {batch_idx} with {len(batch)} images")
        await asyncio.gather(*(process_image(image) for image in batch))
        logger.info(f"Batch {batch_idx} completed, failures: {len(failed_downloads)}")

    logger.info(f"📸 Completed image downloads. Total failed: {len(failed_downloads)}/{len(image_list)}")
    return failed_downloads