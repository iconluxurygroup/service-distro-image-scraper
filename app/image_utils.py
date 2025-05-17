import os
import asyncio
import logging
import aiohttp
import aiofiles
from typing import List, Dict, Tuple, Optional
from urllib.parse import quote
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

async def download_image(
    url: str,
    filename: str,
    session: aiohttp.ClientSession,
    logger: logging.Logger,
    timeout: int = 30
) -> bool:
    try:
        encoded_url = quote(url, safe=':/?=&')
        logger.debug(f"Downloading image from {encoded_url} to {filename}")
        async with session.get(encoded_url, timeout=timeout) as response:
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