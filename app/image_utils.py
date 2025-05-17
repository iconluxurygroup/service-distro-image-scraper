import logging
import asyncio
import httpx
import aiofiles
import os
import re
from typing import List, Dict, Optional, Tuple

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

async def download_image(
    client: httpx.AsyncClient,
    item: Dict,
    temp_dir: str,
    logger: Optional[logging.Logger] = None
) -> Optional[Tuple[str, int]]:
    """Download an image from a URL or thumbnail URL."""
    logger = logger or default_logger
    try:
        row_id = int(item.get('ExcelRowID'))  # Ensure integer
    except (ValueError, TypeError) as e:
        logger.error(f"Invalid ExcelRowID for item {item}: {e}")
        return (item.get('ImageUrl') or item.get('ImageUrlThumbnail'), item.get('ExcelRowID'))

    main_url = item.get('ImageUrl')
    thumb_url = item.get('ImageUrlThumbnail')

    # Sanitize filename
    def sanitize_filename(filename: str) -> str:
        return re.sub(r'[^\w\-_\.]', '_', filename)

    if main_url:
        original_filename = sanitize_filename(main_url.split('/')[-1].split('?')[0])
        image_path = os.path.join(temp_dir, f"{row_id}_{original_filename}")
    else:
        original_filename = sanitize_filename(thumb_url.split('/')[-1].split('?')[0] if thumb_url else f"{row_id}_no_image")
        image_path = os.path.join(temp_dir, f"{row_id}_{original_filename}")

    timeout = httpx.Timeout(30, connect=10)

    try:
        if not main_url and not thumb_url:
            logger.warning(f"⚠️ No URLs provided for row {row_id}")
            return (None, row_id)

        if main_url:
            try:
                response = await client.get(main_url, timeout=timeout)
                response.raise_for_status()
                async with aiofiles.open(image_path, 'wb') as f:
                    await f.write(response.content)
                if await aiofiles.os.path.exists(image_path):
                    logger.info(f"✅ Downloaded main image for row {row_id}: {image_path}")
                    return None
                else:
                    logger.error(f"❌ File not found after download: {image_path}")
                    return (main_url, row_id)
            except httpx.RequestError as e:
                logger.warning(f"⚠️ Network error for main image {main_url}: {e}")
            except httpx.HTTPStatusError as e:
                logger.warning(f"⚠️ HTTP error for main image {main_url}: {e}")

        if thumb_url:
            try:
                original_filename = sanitize_filename(thumb_url.split('/')[-1].split('?')[0])
                image_path = os.path.join(temp_dir, f"{row_id}_{original_filename}")
                response = await client.get(thumb_url, timeout=timeout)
                response.raise_for_status()
                async with aiofiles.open(image_path, 'wb') as f:
                    await f.write(response.content)
                if await aiofiles.os.path.exists(image_path):
                    logger.info(f"✅ Downloaded thumbnail for row {row_id}: {image_path}")
                    return None
                else:
                    logger.error(f"❌ File not found after download: {image_path}")
                    return (thumb_url, row_id)
            except httpx.RequestError as e:
                logger.error(f"❌ Network error for thumbnail {thumb_url}: {e}")
                return (thumb_url, row_id)
            except httpx.HTTPStatusError as e:
                logger.error(f"❌ HTTP error for thumbnail {thumb_url}: {e}")
                return (thumb_url, row_id)
        return (main_url or thumb_url, row_id)
    except Exception as e:
        logger.error(f"🔴 Unexpected error for row {row_id}: {e}", exc_info=True)
        return (main_url or thumb_url, row_id)

async def download_all_images(
    image_list: List[Dict],
    temp_dir: str,
    logger: Optional[logging.Logger] = None
) -> List[Tuple[str, int]]:
    """Download all images in a list with batch processing."""
    logger = logger or default_logger
    failed_img_urls: List[Tuple[str, int]] = []
    if not image_list:
        logger.warning("⚠️ No images to download")
        return failed_img_urls

    logger.info(f"📥 Starting download of {len(image_list)} images to {temp_dir}")
    batch_size = 200
    semaphore = asyncio.Semaphore(50)

    # Ensure temp_dir exists
    await aiofiles.os.makedirs(temp_dir, exist_ok=True)
    logger.info(f"📁 Ensured directory exists: {temp_dir}")

    async with httpx.AsyncClient() as client:
        for i in range(0, len(image_list), batch_size):
            batch = image_list[i:i + batch_size]
            logger.info(f"Processing batch {i // batch_size + 1} with {len(batch)} images")
            tasks = [
                download_image(client, item, temp_dir, logger)
                for item in batch
                if item.get('ExcelRowID') and (item.get('ImageUrl') or item.get('ImageUrlThumbnail'))
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            batch_failures = [
                result for result in results
                if result is not None and not isinstance(result, Exception)
            ]
            failed_img_urls.extend(batch_failures)
            try:
                dir_contents = await aiofiles.os.listdir(temp_dir)
                logger.debug(f"📁 Temp dir contents after batch {i // batch_size + 1}: {dir_contents}")
            except Exception as e:
                logger.error(f"❌ Failed to list temp_dir {temp_dir}: {e}")
            logger.info(f"Batch {i // batch_size + 1} completed, failures: {len(batch_failures)}")

    logger.info(f"📸 Completed image downloads. Total failed: {len(failed_img_urls)}/{len(image_list)}")
    return failed_img_urls