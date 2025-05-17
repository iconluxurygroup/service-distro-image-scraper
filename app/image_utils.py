import os
import asyncio
import logging
import aiohttp
import aiofiles
from typing import List, Dict, Tuple, Optional
from urllib.parse import urlparse
from url_extract import extract_thumbnail_url   
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import re
from operator import itemgetter

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

async def validate_url(url: str, session: aiohttp.ClientSession, logger: logging.Logger) -> bool:
    try:
        if not re.match(r'^https?://', url):
            logger.warning(f"Invalid URL format: {url}")
            return False
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124',
            'Accept': 'image/*,*/*;q=0.8',
            'Referer': 'https://www.google.com/'
        }
        async with session.head(url, timeout=5, headers=headers) as response:
            if response.status == 200:
                logger.debug(f"URL {url} is accessible")
                return True
            elif response.status == 404:
                logger.warning(f"URL {url} is permanently unavailable (404)")
                return False
            logger.warning(f"URL {url} returned status {response.status}")
            return False
    except aiohttp.ClientError as e:
        logger.warning(f"URL {url} is not accessible: {e}")
        return False

@retry(
    stop=stop_after_attempt(lambda attempt, kwargs: 4 if kwargs.get('entry_index', 0) >= 2 else 3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((aiohttp.ClientResponseError, asyncio.TimeoutError)),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying download for URL {retry_state.kwargs['url']} (attempt {retry_state.attempt_number}/{4 if retry_state.kwargs.get('entry_index', 0) >= 2 else 3}) after {retry_state.next_action.sleep}s"
    )
)
async def download_image(
    url: str,
    filename: str,
    session: aiohttp.ClientSession,
    logger: logging.Logger,
    entry_index: int = 0,
    timeout: int = 30,
    max_clean_attempts: int = 3
) -> bool:
    extracted_url = extract_thumbnail_url(url, logger)
    logger.debug(f"Extracted URL: {extracted_url}")
    for attempt in range(1, max_clean_attempts + 1):
        try:
            logger.debug(f"Attempt {attempt} - Raw URL: {url}")
            logger.debug(f"Attempt {attempt} - Extracted URL: {extracted_url}")
            if not await validate_url(extracted_url, session, logger):
                logger.warning(f"Attempt {attempt} - Skipping inaccessible URL: {extracted_url}")
                continue
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124',
                'Accept': 'image/*,*/*;q=0.8',
                'Referer': 'https://www.google.com/'
            }
            async with session.get(extracted_url, timeout=timeout, headers=headers) as response:
                if response.status != 200:
                    logger.warning(f"Attempt {attempt} - HTTP error for image {extracted_url}: {response.status} {response.reason}")
                    if response.status == 404:
                        return False
                    continue
                async with aiofiles.open(filename, 'wb') as f:
                    await f.write(await response.read())
                logger.debug(f"Attempt {attempt} - Successfully downloaded {extracted_url} to {filename}")
                return True
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                logger.error(f"Attempt {attempt} - Permanent failure for {url}: 404 Not Found")
                return False
            logger.error(f"Attempt {attempt} - HTTP error for image {url}: {str(e)}")
            raise
        except asyncio.TimeoutError:
            logger.error(f"Attempt {attempt} - Timeout downloading image {url}")
            raise
        except Exception as e:
            logger.error(f"Attempt {attempt} - Error downloading image {url}: {str(e)}", exc_info=True)
            continue
    logger.error(f"All {max_clean_attempts} attempts failed for URL {url}")
    return False

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

    # Define sorting strategies for entries starting from index 2
    sort_strategies = [
        lambda lst: sorted(lst, key=lambda x: 1 if x.get('ImageUrlThumbnail') else 0, reverse=True),
        lambda lst: sorted(lst, key=lambda x: len(x.get('ImageUrl', '')), reverse=False),
        lambda lst: sorted(lst, key=lambda x: urlparse(x.get('ImageUrl', '')).netloc or ''),
        lambda lst: sorted(lst, key=lambda x: x.get('ExcelRowID', 0)),
    ]

    async def process_image(image: Dict, index: int) -> None:
        excel_row_id = image['ExcelRowID']
        main_url = image['ImageUrl']
        thumb_url = image.get('ImageUrlThumbnail', '')
        filename = os.path.join(temp_dir, f"image_{excel_row_id}.jpg")

        logger.debug(f"Processing ExcelRowID {excel_row_id} at index {index}: Main URL = {main_url}, Thumbnail URL = {thumb_url}")

        async with aiohttp.ClientSession() as session:
            success = await download_image(main_url, filename, session, logger, entry_index=index)
            if not success and thumb_url:
                logger.debug(f"Falling back to thumbnail {thumb_url} for ExcelRowID {excel_row_id}")
                success = await download_image(thumb_url, filename, session, logger, entry_index=index)
            
            if not success:
                logger.error(f"Failed to download both main and thumbnail for ExcelRowID {excel_row_id}")
                failed_downloads.append((main_url or thumb_url, excel_row_id))
                if not main_url and not thumb_url:
                    logger.critical(f"No valid URLs for ExcelRowID {excel_row_id}. Check database for FileID {image.get('FileID', 'unknown')}.")

    # Try each sort strategy for entries starting from index 2
    best_failed_downloads = []
    min_failures = float('inf')
    original_tail = image_list[2:] if len(image_list) > 2 else []
    fixed_prefix = image_list[:2] if len(image_list) >= 2 else image_list

    for strategy_idx, sort_func in enumerate(sort_strategies, 1):
        failed_downloads.clear()
        logger.info(f"Trying sort strategy {strategy_idx}")
        sorted_tail = sort_func(original_tail)
        current_list = fixed_prefix + sorted_tail
        logger.debug(f"Current list order: {[item['ExcelRowID'] for item in current_list]}")

        batches = [current_list[i:i + batch_size] for i in range(0, len(current_list), batch_size)]
        for batch_idx, batch in enumerate(batches, 1):
            logger.info(f"Processing batch {batch_idx} with {len(batch)} images")
            await asyncio.gather(*(process_image(image, idx) for idx, image in enumerate(current_list)))
            logger.info(f"Batch {batch_idx} completed, failures: {len(failed_downloads)}")

        logger.info(f"Sort strategy {strategy_idx} completed with {len(failed_downloads)} failures")
        if len(failed_downloads) < min_failures:
            min_failures = len(failed_downloads)
            best_failed_downloads = failed_downloads.copy()
        if min_failures == 0:
            break

    # Retry failed downloads with fallback strategy
    if best_failed_downloads:
        logger.info(f"Retrying {len(best_failed_downloads)} failed downloads with fallback strategy")
        retry_failed = []
        for url, excel_row_id in best_failed_downloads:
            image = next((img for img in image_list if img['ExcelRowID'] == excel_row_id), None)
            if not image:
                continue
            filename = os.path.join(temp_dir, f"image_{excel_row_id}.jpg")
            async with aiohttp.ClientSession() as session:
                main_url = clean_url(image['ImageUrl'], attempt=3)
                success = await download_image(main_url, filename, session, logger, entry_index=image_list.index(image))
                if not success and image.get('ImageUrlThumbnail'):
                    thumb_url = clean_url(image['ImageUrlThumbnail'], attempt=3)
                    success = await download_image(thumb_url, filename, session, logger, entry_index=image_list.index(image))
                if not success:
                    retry_failed.append((url, excel_row_id))
        best_failed_downloads = retry_failed

    logger.info(f"📸 Completed image downloads. Total failed: {len(best_failed_downloads)}/{len(image_list)}")
    return best_failed_downloads