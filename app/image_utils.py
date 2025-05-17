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

def clean_url(url: str, attempt: int = 1) -> str:
    try:
        if attempt == 1:
            url = re.sub(r'\\+|%5[Cc]', '', url)
            parsed = urlparse(url)
            path = parsed.path.replace('%2F', '/').replace('%2f', '/')
            cleaned_url = f"{parsed.scheme}://{parsed.netloc}{path}"
            if parsed.query:
                query = parsed.query.replace('%5C', '').replace('%5c', '')
                cleaned_url += f"?{query}"
            if parsed.fragment:
                cleaned_url += f"#{parsed.fragment}"
            return cleaned_url
        elif attempt == 2:
            url = re.sub(r'\\+|%5[Cc]|%2[Ff]', '', url)
            while '%25' in url:
                url = url.replace('%25', '%')
            parsed = urlparse(url)
            path = parsed.path
            cleaned_url = f"{parsed.scheme}://{parsed.netloc}{path}"
            if parsed.query:
                cleaned_url += f"?{parsed.query}"
            if parsed.fragment:
                cleaned_url += f"#{parsed.fragment}"
            return cleaned_url
        elif attempt == 3:
            url = re.sub(r'[\x00-\x1F\x7F]', '', url)
            parsed = urlparse(url)
            return f"{parsed.scheme}://{parsed.netloc}{parsed.path}" + \
                   (f"?{parsed.query}" if parsed.query else "") + \
                   (f"#{parsed.fragment}" if parsed.fragment else "")
        return url
    except Exception as e:
        default_logger.warning(f"Error cleaning URL {url} on attempt {attempt}: {e}")
        return url

def encode_url(url: str) -> str:
    try:
        parsed = urlparse(url)
        path = parsed.path
        if parsed.query:
            query_dict = parse_qs(parsed.query)
            encoded_query = urlencode(
                {k: [quote(v, safe=':') for v in vs] for k, vs in query_dict.items()},
                doseq=True
            )
        else:
            encoded_query = ''
        encoded_url = f"{parsed.scheme}://{parsed.netloc}{path}"
        if encoded_query:
            encoded_url += f"?{encoded_query}"
        if parsed.fragment:
            encoded_url += f"#{quote(parsed.fragment)}"
        return encoded_url
    except Exception as e:
        default_logger.warning(f"Error encoding URL {url}: {e}")
        return url

async def validate_url(url: str, session: aiohttp.ClientSession, logger: logging.Logger) -> bool:
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124',
            'Accept': 'image/*,*/*;q=0.8',
            'Referer': 'https://www.google.com/'
        }
        async with session.head(url, timeout=5, headers=headers) as response:
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
    timeout: int = 30,
    max_clean_attempts: int = 3
) -> bool:
    for attempt in range(1, max_clean_attempts + 1):
        try:
            cleaned_url = clean_url(url, attempt)
            encoded_url = encode_url(cleaned_url)
            logger.debug(f"Attempt {attempt} - Raw URL: {url}")
            logger.debug(f"Attempt {attempt} - Cleaned URL: {cleaned_url}")
            logger.debug(f"Attempt {attempt} - Encoded URL: {encoded_url}")

            if not await validate_url(encoded_url, session, logger):
                logger.warning(f"Attempt {attempt} - Skipping inaccessible URL: {encoded_url}")
                continue

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124',
                'Accept': 'image/*,*/*;q=0.8',
                'Referer': 'https://www.google.com/'
            }
            async with session.get(encoded_url, timeout=timeout, headers=headers) as response:
                if response.status != 200:
                    logger.warning(f"Attempt {attempt} - HTTP error for image {encoded_url}: {response.status} {response.reason}")
                    continue
                async with aiofiles.open(filename, 'wb') as f:
                    await f.write(await response.read())
                logger.debug(f"Attempt {attempt} - Successfully downloaded {encoded_url} to {filename}")
                return True
        except aiohttp.ClientError as e:
            logger.error(f"Attempt {attempt} - HTTP error for image {encoded_url}: {str(e)}")
            continue
        except asyncio.TimeoutError:
            logger.error(f"Attempt {attempt} - Timeout downloading image {encoded_url}")
            continue
        except Exception as e:
            logger.error(f"Attempt {attempt} - Error downloading image {encoded_url}: {str(e)}", exc_info=True)
            continue
    logger.error(f"All {max_clean_attempts} attempts failed for URL {url}")
    return False

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(Exception),
    before_sleep=lambda retry_state: retry_state.kwargs.get('logger', default_logger).info(
        f"Retrying download_all_images for {len(retry_state.args[0])} images "
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
    logger.debug(f"Image list: {image_list}")

    os.makedirs(temp_dir, exist_ok=True)
    logger.info(f"📁 Ensured directory exists: {temp_dir}")

    async def process_image(image: Dict) -> None:
        excel_row_id = image['ExcelRowID']
        main_url = image['ImageUrl']
        thumb_url = image.get('ImageUrlThumbnail', '')
        filename = os.path.join(temp_dir, f"image_{excel_row_id}.jpg")

        logger.debug(f"Processing ExcelRowID {excel_row_id}: Main URL = {main_url}, Thumbnail URL = {thumb_url}")

        async with aiohttp.ClientSession() as session:
            success = await download_image(main_url, filename, session, logger)
            if not success:
                if thumb_url:
                    logger.debug(f"Falling back to thumbnail {thumb_url} for ExcelRowID {excel_row_id}")
                    success = await download_image(thumb_url, filename, session, logger)
                else:
                    logger.warning(f"No thumbnail URL available for ExcelRowID {excel_row_id}")

            if not success:
                logger.error(f"Failed to download both main and thumbnail for ExcelRowID {excel_row_id}")
                failed_downloads.append((main_url, excel_row_id))
                if not main_url and not thumb_url:
                    logger.critical(f"No valid URLs for ExcelRowID {excel_row_id}. Check database for FileID 315.")

    batches = [image_list[i:i + batch_size] for i in range(0, len(image_list), batch_size)]
    for batch_idx, batch in enumerate(batches, 1):
        logger.info(f"Processing batch {batch_idx} with {len(batch)} images")
        await asyncio.gather(*(process_image(image) for image in batch))
        logger.info(f"Batch {batch_idx} completed, failures: {len(failed_downloads)}")

    logger.info(f"📸 Completed image downloads. Total failed: {len(failed_downloads)}/{len(image_list)}")
    return failed_downloads