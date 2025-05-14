import logging
import asyncio
import httpx
import aiofiles
import os
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
) -> Optional[Tuple[str, str]]:
    """Download an image from a URL or thumbnail URL."""
    logger = logger or default_logger
    row_id = item.get('ExcelRowID')
    main_url = item.get('ImageUrl')
    thumb_url = item.get('ImageUrlThumbnail')
    
    # Validate temp_dir
    try:
        if not await aiofiles.os.path.exists(temp_dir):
            await aiofiles.os.makedirs(temp_dir)
            logger.info(f"📁 Created directory: {temp_dir}")
    except Exception as e:
        logger.error(f"❌ Failed to create/validate temp_dir {temp_dir}: {e}")
        return (main_url or thumb_url, row_id)

    if main_url:
        original_filename = main_url.split('/')[-1].split('?')[0]
        image_path = os.path.join(temp_dir, f"{row_id}_{original_filename}")
    else:
        original_filename = thumb_url.split('/')[-1].split('?')[0] if thumb_url else f"{row_id}_no_image"
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
            except (httpx.RequestError, httpx.HTTPStatusError) as e:
                logger.warning(f"⚠️ Failed main image for row {row_id}: {e}")

        if thumb_url:
            try:
                original_filename = thumb_url.split('/')[-1].split('?')[0]
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
            except (httpx.RequestError, httpx.HTTPStatusError) as e:
                logger.error(f"❌ Failed thumbnail for row {row_id}: {e}")
                return (thumb_url, row_id)
        return (main_url or thumb_url, row_id)
    except Exception as e:
        logger.error(f"🔴 Unexpected error for row {row_id}: {e}", exc_info=True)
        return (main_url or thumb_url, row_id)
async def download_all_images(
    image_list: List[Dict],
    temp_dir: str,
    logger: Optional[logging.Logger] = None
) -> List[Tuple[str, str]]:
    """Download all images in a list with batch processing."""
    logger = logger or default_logger
    failed_img_urls: List[Tuple[str, str]] = []
    if not image_list:
        logger.warning("⚠️ No images to download")
        return failed_img_urls

    logger.info(f"📥 Starting download of {len(image_list)} images to {temp_dir}")
    batch_size = 200
    semaphore = asyncio.Semaphore(50)

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
            # Log directory contents for debugging
            try:
                dir_contents = await aiofiles.os.listdir(temp_dir)
                logger.debug(f"📁 Temp dir contents after batch {i // batch_size + 1}: {dir_contents}")
            except Exception as e:
                logger.error(f"❌ Failed to list temp_dir {temp_dir}: {e}")
            logger.info(f"Batch {i // batch_size + 1} completed, failures: {len(batch_failures)}")

    logger.info(f"📸 Completed image downloads. Total failed: {len(failed_img_urls)}/{len(image_list)}")
    return failed_img_urls