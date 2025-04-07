import asyncio
import logging
import os
import shutil
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional, Tuple
import pandas as pd
import aiofiles
import httpx
import pyodbc
import ray
from ray_workers import fetch_brand_rules, process_batch, BRAND_RULES_URL
from openpyxl import load_workbook
from aws_s3 import upload_file_to_space
from config import conn_str
from database import (
    fetch_missing_images,
    get_images_excel_db,
    get_records_to_search,
    get_send_to_email,
    update_file_generate_complete,
    insert_search_results,
    update_file_location_complete,
    update_initial_sort_order,
    update_log_url_in_db,
    update_search_sort_order,
)
from email_utils import send_email, send_message_email
from excel_utils import (
    write_excel_image,
    write_failed_downloads_to_excel,
)

# Configure default logger
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)

async def create_temp_dirs(unique_id: str, logger: Optional[logging.Logger] = None) -> Tuple[str, str]:
    logger = logger or default_logger
    loop = asyncio.get_running_loop()
    base_dir = os.path.join(os.getcwd(), 'temp_files')
    temp_images_dir = os.path.join(base_dir, 'images', str(unique_id))
    temp_excel_dir = os.path.join(base_dir, 'excel', str(unique_id))

    try:
        await loop.run_in_executor(None, lambda: os.makedirs(temp_images_dir, exist_ok=True))
        await loop.run_in_executor(None, lambda: os.makedirs(temp_excel_dir, exist_ok=True))
        logger.info(f"Created temporary directories for ID: {unique_id}")
        return temp_images_dir, temp_excel_dir
    except Exception as e:
        logger.error(f"🔴 Failed to create temp directories for ID {unique_id}: {e}", exc_info=True)
        raise

async def cleanup_temp_dirs(directories: List[str], logger: Optional[logging.Logger] = None) -> None:
    logger = logger or default_logger
    loop = asyncio.get_running_loop()
    
    for dir_path in directories:
        try:
            if os.path.exists(dir_path):
                await loop.run_in_executor(None, lambda dp=dir_path: shutil.rmtree(dp, ignore_errors=True))
                logger.info(f"Cleaned up directory: {dir_path}")
        except Exception as e:
            logger.error(f"🔴 Failed to clean up directory {dir_path}: {e}", exc_info=True)

async def download_image_with_sem(
    client: httpx.AsyncClient,
    item: Dict,
    temp_dir: str,
    logger: logging.Logger,
    semaphore: asyncio.Semaphore
) -> Optional[Tuple[str, str]]:
    async with semaphore:
        return await download_image(client, item, temp_dir, logger)

async def download_image(
    client: httpx.AsyncClient,
    item: Dict,
    temp_dir: str,
    logger: logging.Logger
) -> Optional[Tuple[str, str]]:
    row_id = item.get('ExcelRowID')
    main_url = item.get('ImageUrl')
    thumb_url = item.get('ImageUrlThumbnail')
    
    if main_url:
        original_filename = main_url.split('/')[-1].split('?')[0]
        image_path = os.path.join(temp_dir, f"{row_id}_{original_filename}")
    else:
        original_filename = thumb_url.split('/')[-1].split('?')[0] if thumb_url else f"{row_id}_no_image"
        image_path = os.path.join(temp_dir, f"{row_id}_{original_filename}")
    
    timeout = httpx.Timeout(30, connect=10)

    try:
        if not main_url and not thumb_url:
            logger.warning(f"No URLs provided for row {row_id}")
            return (None, row_id)

        if main_url:
            try:
                response = await client.get(main_url, timeout=timeout)
                response.raise_for_status()
                async with aiofiles.open(image_path, 'wb') as f:
                    await f.write(response.content)
                logger.info(f"✅ Downloaded main image for row {row_id}: {original_filename}")
                return None
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
                logger.info(f"✅ Downloaded thumbnail for row {row_id}: {original_filename}")
                return None
            except (httpx.RequestError, httpx.HTTPStatusError) as e:
                logger.error(f"❌ Failed thumbnail for row {row_id}: {e}")
                return (main_url or thumb_url, row_id)
        return (main_url or thumb_url, row_id)
    except Exception as e:
        logger.error(f"🔴 Unexpected error for row {row_id}: {e}", exc_info=True)
        return (main_url or thumb_url, row_id)

async def download_all_images(
    image_list: List[Dict],
    temp_dir: str,
    logger: Optional[logging.Logger] = None
) -> List[Tuple[str, str]]:
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
            tasks = [download_image(client, item, temp_dir, logger) for item in batch 
                     if item.get('ExcelRowID') and (item.get('ImageUrl') or item.get('ImageUrlThumbnail'))]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            batch_failures = [result for result in results if result is not None and not isinstance(result, Exception)]
            failed_img_urls.extend(batch_failures)
            logger.info(f"Batch {i // batch_size + 1} completed, failures: {len(batch_failures)}")

    logger.info(f"📸 Completed image downloads. Total failed: {len(failed_img_urls)}/{len(image_list)}")
    return failed_img_urls

async def generate_download_file(
    file_id: int,
    logger: Optional[logging.Logger] = None,
    file_id_param: Optional[int] = None
) -> Dict[str, str]:
    logger = logger or default_logger
    temp_images_dir, temp_excel_dir = None, None

    try:
        start_time = time.time()
        loop = asyncio.get_running_loop()
        file_id = int(file_id)

        with pyodbc.connect(conn_str) as conn:
            query = "SELECT FileName FROM utb_ImageScraperFiles WHERE ID = ?"
            cursor = conn.cursor()
            cursor.execute(query, (file_id,))
            result = cursor.fetchone()
            if not result:
                logger.error(f"❌ No file found for FileID {file_id}")
                return {"error": f"No file found for FileID {file_id}"}
            original_filename = result[0]

        logger.info(f"🕵️ Fetching images for FileID: {file_id}")
        selected_images_df = await loop.run_in_executor(ThreadPoolExecutor(), get_images_excel_db, file_id, logger)
        if selected_images_df.empty:
            logger.warning(f"⚠️ No images found for FileID {file_id}")
            return {"error": f"No images found for FileID {file_id}"}

        selected_image_list = [
            {
                'ExcelRowID': row['ExcelRowID'],
                'ImageUrl': row['ImageUrl'],
                'ImageUrlThumbnail': row['ImageUrlThumbnail'],
                'Brand': row.get('Brand', ''),
                'Style': row.get('Style', ''),
                'Color': row.get('Color', ''),
                'Category': row.get('Category', '')
            }
            for _, row in selected_images_df.iterrows()
        ]
        logger.debug(f"📋 Selected {len(selected_image_list)} images: {selected_image_list[:2]}")

        template_file_path = "https://iconluxurygroup.s3.us-east-2.amazonaws.com/ICON_DISTRO_USD_20250312.xlsx"
        header_index = 5
        
        base_name, extension = os.path.splitext(original_filename)
        unique_id = uuid.uuid4().hex[:8]
        processed_file_name = f"{base_name}_processed_{unique_id}{extension}"
        logger.info(f"🆔 Generated filename: {processed_file_name}")

        temp_images_dir, temp_excel_dir = await create_temp_dirs(file_id, logger=logger)
        local_filename = os.path.join(temp_excel_dir, original_filename)

        failed_img_urls = await download_all_images(selected_image_list, temp_images_dir, logger=logger)
        logger.info(f"📥 Downloading template file from {template_file_path}")
        async with httpx.AsyncClient() as client:
            response = await client.get(template_file_path, timeout=httpx.Timeout(30, connect=10))
            response.raise_for_status()
            async with aiofiles.open(local_filename, 'wb') as f:
                await f.write(response.content)

        row_offset = header_index
        logger.info(f"🖼️ Writing images with row_offset={row_offset}")
        failed_rows = await loop.run_in_executor(
            ThreadPoolExecutor(),
            write_excel_image,
            local_filename,
            temp_images_dir,
            selected_image_list,
            "A",
            row_offset,
            logger
        )

        public_url = await loop.run_in_executor(
            ThreadPoolExecutor(),
            upload_file_to_space,
            local_filename,
            processed_file_name,
            True,
            logger,
            file_id
        )
        if not public_url:
            logger.error(f"❌ Upload failed for FileID {file_id}")
            return {"error": "Failed to upload processed file"}

        await loop.run_in_executor(ThreadPoolExecutor(), update_file_location_complete, file_id, public_url, logger)
        await loop.run_in_executor(ThreadPoolExecutor(), update_file_generate_complete, file_id, logger)

        send_to_email_addr = await loop.run_in_executor(ThreadPoolExecutor(), get_send_to_email, file_id, logger)
        if not send_to_email_addr:
            logger.error(f"❌ No email address for FileID {file_id}")
            return {"error": "Failed to retrieve email address"}
        subject_line = f"{original_filename} Job Notification"
        await loop.run_in_executor(
            ThreadPoolExecutor(),
            lambda: send_email(
                to_emails=send_to_email_addr,
                subject=subject_line,
                download_url=public_url,
                job_id=file_id,
                logger=logger
            )
        )

        logger.info(f"🏁 Completed FileID {file_id} in {(time.time() - start_time):.2f} seconds")
        return {"message": "Processing completed successfully", "public_url": public_url}

    except Exception as e:
        logger.error(f"🔴 Error for FileID {file_id}: {e}", exc_info=True)
        logger.error(f"❌ Job generate_download_file failed for FileID {file_id}")
        return {"error": f"An error occurred: {str(e)}"}
    finally:
        if temp_images_dir and temp_excel_dir:
            await cleanup_temp_dirs([temp_images_dir, temp_excel_dir], logger=logger)

async def process_restart_batch(
    file_id_db: int,
    logger: Optional[logging.Logger] = None,
    file_id: Optional[int] = None
) -> Dict[str, str]:
    logger = logger or default_logger
    log_filename = logger.handlers[0].baseFilename if logger.handlers else os.path.join(os.getcwd(), 'logs', f"file_{file_id_db}.log")

    try:
        logger.info(f"🔁 Restarting processing for FileID: {file_id_db}")
        file_id_db = int(file_id_db)

        brand_rules = await fetch_brand_rules(BRAND_RULES_URL, logger=logger)
        if not brand_rules:
            logger.error("Failed to load brand rules; proceeding without brand-specific logic")
            brand_rules = {"brand_rules": []}

        logger.info(f"🖼️ Fetching missing images for FileID: {file_id_db}")
        missing_urls_df = fetch_missing_images(file_id_db, limit=10000, ai_analysis_only=False, logger=logger)
        logger.debug(f"🟨 Missing URLs DataFrame columns: {missing_urls_df.columns.tolist()}")
        logger.debug(f"🟨 Missing URLs DataFrame sample: {missing_urls_df.head().to_dict()}")

        image_url_col = 'ImageUrl'
        if image_url_col not in missing_urls_df.columns:
            logger.error(f"🔴 'ImageUrl' not found in DataFrame columns: {missing_urls_df.columns.tolist()}")
            raise KeyError(f"'ImageUrl' column not found in DataFrame")

        needs_url_generation = missing_urls_df[missing_urls_df[image_url_col].isnull() | (missing_urls_df[image_url_col] == '')]
        if not needs_url_generation.empty:
            logger.info(f"🔗 Found {len(needs_url_generation)} records needing URL generation for FileID: {file_id_db}")
            search_df = get_records_to_search(file_id_db, logger=logger)
            if not search_df.empty:
                search_list = search_df.to_dict('records')
                logger.info(f"🔬🔍 Preparing {len(search_list)} searches for FileID: {file_id_db}")

                BATCH_SIZE = 10
                batches = [search_list[i:i + BATCH_SIZE] for i in range(0, len(search_list), BATCH_SIZE)]
                logger.info(f"📋 Processing {len(batches)} batches with Ray")
                futures = [process_batch.remote(batch, brand_rules, logger=logger) for batch in batches]
                batch_results = ray.get(futures)
                all_results = [result for batch_result in batch_results for result in batch_result]

                success_count = sum(1 for r in all_results if r['status'] == 'success')
                logger.info(f"🟢 Completed {success_count}/{len(all_results)} searches successfully")

                for result in all_results:
                    if result['status'] == 'success' and result['result'] is not None:
                        insert_success = await asyncio.get_running_loop().run_in_executor(
                            None, insert_search_results, result['result'], logger
                        )
                        if not insert_success:
                            logger.error(f"Failed to insert results for EntryID {result['entry_id']}")
                        else:
                            logger.info(f"Successfully inserted results for EntryID {result['entry_id']}")
            else:
                logger.info(f"🟡 No records to search for FileID: {file_id_db}")

            logger.debug(f"Updating initial sort order for FileID: {file_id_db}")
            initial_sort_result = update_initial_sort_order(file_id_db, logger=logger)
            if initial_sort_result is None:
                logger.error(f"🔴 Initial SortOrder update failed for FileID: {file_id_db}")
                raise Exception("Initial SortOrder update failed")

        logger.info(f"🔍🔀 Updating search sort order for FileID: {file_id_db}")
        sort_result = update_search_sort_order(file_id_db, logger=logger)
        if sort_result is None:
            logger.error(f"🔴 Search sort order update failed for FileID: {file_id_db}")
            raise Exception("Search sort order update failed")

        logger.info(f"💾 Generating download file for FileID: {file_id_db}")
        result = await generate_download_file(file_id_db, logger=logger)
        if "error" in result and result["error"] != f"No images found for FileID {file_id_db}":
            logger.error(f"🔴 Failed to generate download file: {result['error']}")
            raise Exception(result["error"])

        logger.info(f"✅ Restart processing completed successfully for FileID: {file_id_db}")
        return {"message": "Restart processing completed successfully", "file_id": str(file_id_db)}

    except Exception as e:
        logger.error(f"🔴 Error restarting processing for FileID {file_id_db}: {e}", exc_info=True)
        loop = asyncio.get_running_loop()
        if os.path.exists(log_filename):
            upload_url = await loop.run_in_executor(
                ThreadPoolExecutor(),
                upload_file_to_space,
                log_filename,
                f"job_logs/job_{file_id_db}.log",
                True,
                logger,
                file_id_db
            )
            logger.info(f"Log file uploaded to: {upload_url}")
            await update_log_url_in_db(file_id_db, upload_url, logger)
        
        send_to_email_addr = get_send_to_email(file_id_db, logger=logger)
        if send_to_email_addr:
            file_name = f"FileID: {file_id_db}"
            send_message_email(send_to_email_addr, f"Error processing {file_name}", f"An error occurred while reprocessing your file: {str(e)}")
        return {"error": str(e)}
