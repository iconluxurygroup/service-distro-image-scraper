import asyncio
import os
import logging
import json
import pyodbc
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from database import (
    insert_file_db, load_payload_db, get_records_to_search, process_images, 
    batch_process_images, fetch_missing_images, 
    update_file_location_complete, update_file_generate_complete, 
    get_send_to_email, get_file_location, get_images_excel_db, get_lm_products,
    process_search_row, get_endpoint, update_search_sort_order, update_initial_sort_order
)
from config import conn_str
from aws_s3 import upload_file_to_space
from email_utils import send_email, send_message_email
from excel_utils import write_excel_image, write_failed_downloads_to_excel, verify_png_image_single
from image_processing import get_image_data, analyze_image_with_grok_vision, evaluate_with_grok_text
from ray_workers import process_batch
import requests
import urllib.parse
import shutil
import aiohttp
from aiohttp import ClientTimeout
from aiohttp_retry import RetryClient, ExponentialRetry
from PIL import Image as IMG2
from io import BytesIO
import ray
from logging_config import setup_job_logger

# Fallback logger for standalone calls
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)

async def create_temp_dirs(unique_id, logger=None):
    """Create temporary directories for processing."""
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
        logger.error(f"🔴 Failed to create temp directories for ID {unique_id}: {e}")
        raise

async def cleanup_temp_dirs(directories, logger=None):
    """Clean up temporary directories."""
    logger = logger or default_logger
    loop = asyncio.get_running_loop()
    for dir_path in directories:
        try:
            if os.path.exists(dir_path):
                await loop.run_in_executor(None, lambda dp=dir_path: shutil.rmtree(dp, ignore_errors=True))
                logger.info(f"Cleaned up directory: {dir_path}")
        except Exception as e:
            logger.error(f"🔴 Failed to clean up directory {dir_path}: {e}")

async def download_all_images(data, save_path, logger=None):
    """Download images with retry logic and limited concurrency."""
    logger = logger or default_logger
    failed_downloads = []
    valid_data = [item for item in data if item[1] and isinstance(item[1], str) and item[1].strip()]
    if not valid_data:
        logger.info("No valid image URLs to download")
        return failed_downloads
    
    pool_size = min(10, len(valid_data))  # Conservative pool size
    timeout = ClientTimeout(total=60)
    retry_options = ExponentialRetry(attempts=3, start_timeout=3)
    
    async with RetryClient(timeout=timeout, retry_options=retry_options) as session:
        semaphore = asyncio.Semaphore(pool_size)
        tasks = [
            image_download(semaphore, item[1], item[2], str(item[0]), save_path, session, logger=logger)
            for item in valid_data
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for idx, result in enumerate(results):
            if not result or isinstance(result, Exception):
                logger.warning(f"🟨 Failed to download image for row {valid_data[idx][0]}: {valid_data[idx][1]}")
                failed_downloads.append((valid_data[idx][1], valid_data[idx][0]))
    logger.info(f"✅ Completed image downloads. 📒 Failed: {len(failed_downloads)}/{len(valid_data)}")
    return failed_downloads

async def image_download(semaphore, url, thumbnail, image_name, save_path, session, logger=None):
    """Download a single image with retry logic."""
    logger = logger or default_logger
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "image/*",
        "Referer": "https://www.google.com"
    }
    async with semaphore:
        try:
            async with session.get(url, headers=headers, timeout=30) as response:
                if response.status == 200:
                    data = await response.read()
                    with IMG2.open(BytesIO(data)) as img:
                        img_path = os.path.join(save_path, f"{image_name}.png")
                        img.save(img_path)
                        if verify_png_image_single(img_path):
                            logger.debug(f"Successfully downloaded and verified {url}")
                            return True
                        else:
                            os.remove(img_path)
                            logger.warning(f"Image verification failed for {url}")
                elif response.status == 400 and thumbnail and thumbnail != url:
                    logger.warning(f"Bad Request for {url}. Trying thumbnail: {thumbnail}")
                    async with session.get(thumbnail, headers=headers, timeout=30) as thumb_response:
                        if thumb_response.status == 200:
                            data = await thumb_response.read()
                            with IMG2.open(BytesIO(data)) as img:
                                img_path = os.path.join(save_path, f"{image_name}.png")
                                img.save(img_path)
                                if verify_png_image_single(img_path):
                                    logger.debug(f"Successfully downloaded thumbnail {thumbnail}")
                                    return True
                                else:
                                    os.remove(img_path)
                return False
        except Exception as e:
            logger.error(f"🔴 Error downloading {url}: {e}")
            return False

async def generate_download_file(file_id, logger=None, file_id_param=None):
    """Generate and upload a processed Excel file with images."""
    logger = logger or default_logger
    temp_images_dir, temp_excel_dir = None, None
    try:
        preferred_image_method = 'append'
        start_time = time.time()
        loop = asyncio.get_running_loop()
        
        selected_images_df = await loop.run_in_executor(ThreadPoolExecutor(), get_images_excel_db, file_id, logger)
        if selected_images_df.empty:
            logger.warning(f"No images found for FileID {file_id} to generate download file")
            return {"error": f"No images found for FileID {file_id}"}
        
        selected_image_list = await loop.run_in_executor(ThreadPoolExecutor(), prepare_images_for_download_dataframe, selected_images_df, logger)
        
        provided_file_path = await loop.run_in_executor(ThreadPoolExecutor(), get_file_location, file_id, logger)
        if provided_file_path == "No File Found":
            logger.error(f"🔴 No file location found for FileID {file_id}")
            return {"error": "Original file not found"}
        
        file_name = provided_file_path.split('/')[-1]
        base_name, extension = os.path.splitext(file_name)
        processed_file_name = f"{base_name}_processed{extension}"
        
        temp_images_dir, temp_excel_dir = await create_temp_dirs(file_id, logger=logger)
        local_filename = os.path.join(temp_excel_dir, file_name)
        
        failed_img_urls = await download_all_images(selected_image_list, temp_images_dir, logger=logger)
        
        response = await loop.run_in_executor(None, requests.get, provided_file_path, {'allow_redirects': True, 'timeout': 60})
        if response.status_code != 200:
            logger.error(f"🔴 Failed to download file {provided_file_path}: {response.status_code}")
            return {"error": "Failed to download the provided file"}
        
        with open(local_filename, "wb") as file:
            file.write(response.content)
        
        failed_rows = await loop.run_in_executor(ThreadPoolExecutor(), write_excel_image, local_filename, temp_images_dir, preferred_image_method)
        if failed_rows:
            logger.warning(f"Failed to write images for {len(failed_rows)} rows: {failed_rows}")
        
        if failed_img_urls:
            await loop.run_in_executor(ThreadPoolExecutor(), write_failed_downloads_to_excel, failed_img_urls, local_filename)
            logger.info(f"Logged {len(failed_img_urls)} failed downloads to Excel")
        
        public_url = await loop.run_in_executor(ThreadPoolExecutor(), upload_file_to_space, local_filename, processed_file_name, True, logger, file_id)
        if not public_url:
            logger.error(f"🔴 Failed to upload processed file for FileID {file_id}")
            return {"error": "Failed to upload processed file"}
        
        await loop.run_in_executor(ThreadPoolExecutor(), update_file_location_complete, file_id, public_url, logger)
        await loop.run_in_executor(ThreadPoolExecutor(), update_file_generate_complete, file_id, logger)
        
        subject_line = f"{file_name} Job Notification"
        send_to_email = await loop.run_in_executor(ThreadPoolExecutor(), get_send_to_email, file_id, logger)
        await loop.run_in_executor(ThreadPoolExecutor(), send_email, send_to_email, subject_line, public_url, file_id)
        
        execution_time = time.time() - start_time
        logger.info(f"Processing completed for FileID {file_id} in {execution_time:.2f} seconds")
        
        return {"message": "Processing completed successfully", "public_url": public_url}
    except Exception as e:
        logger.error(f"🔴 Error generating download file for FileID {file_id}: {e}")
        return {"error": f"An error occurred: {str(e)}"}
    finally:
        if temp_images_dir and temp_excel_dir:
            await cleanup_temp_dirs([temp_images_dir, temp_excel_dir], logger=logger)
# workflow.py (partial update)
async def process_restart_batch(file_id_db, logger=None, file_id=None):
    """Restart processing for a failed batch."""
    logger = logger or default_logger
    try:
        logger.info(f"🔁 Restarting processing for FileID: {file_id_db}")
        file_id_db = int(file_id_db)
        
        logger.info(f"🖼️ Fetching missing images for FileID: {file_id_db}")
        missing_urls_df = fetch_missing_images(file_id_db, limit=1000, ai_analysis_only=False, logger=logger)
        logger.debug(f"🟨 🕵️‍♂️Missing URLs DataFrame columns: {missing_urls_df.columns.tolist()}")
        logger.debug(f"🟨 🕵️‍♂️Missing URLs DataFrame sample: {missing_urls_df.head().to_dict()}")

        image_url_col = 'ImageUrl'  # Standardized to match process_search_row
        if image_url_col not in missing_urls_df.columns:
            logger.error(f"🔴 'ImageUrl' not found in DataFrame columns: {missing_urls_df.columns.tolist()}")
            raise KeyError(f"'ImageUrl' column not found in DataFrame")

        needs_url_generation = missing_urls_df[missing_urls_df[image_url_col].isnull() | (missing_urls_df[image_url_col] == '')]
        
        if not needs_url_generation.empty:
            logger.info(f"🔗 Found {len(needs_url_generation)} records needing URL generation for FileID: {file_id_db}")
            # ... rest of the function remains unchanged
            search_df = get_records_to_search(file_id_db, logger=logger)
            if not search_df.empty:
                search_list = search_df.to_dict('records')
                logger.info(f"🔬🔍 Preparing {len(search_list)} searches (2 per EntryID) for FileID: {file_id_db}")
                
                BATCH_SIZE = 100
                batches = [search_list[i:i + BATCH_SIZE] for i in range(0, len(search_list), BATCH_SIZE)]
                logger.info(f"📋 Processing {len(batches)} batches with Ray")
                futures = [process_batch.remote(batch, logger=logger) for batch in batches]
                batch_results = ray.get(futures)
                all_results = [result for batch_result in batch_results for result in batch_result]
                
                success_count = sum(1 for r in all_results if r['status'] == 'success')
                logger.info(f"🟢 Completed {success_count}/{len(all_results)} searches successfully")
            else:
                logger.info(f"🟡 No records to search for FileID: {file_id_db}")
            
            logger.debug(f"Updating initial sort order for FileID: {file_id_db}")
            initial_sort_result = update_initial_sort_order(file_id_db, logger=logger)
            if initial_sort_result is None:
                logger.error(f"🔴 Initial SortOrder update failed for FileID: {file_id_db}")
                raise Exception("Initial SortOrder update failed")
        
        # Stage 2: Update search sort order
        logger.info(f"🔍🔀Updating search sort order for FileID: {file_id_db}")
        sort_result = update_search_sort_order(file_id_db, logger=logger)
        if sort_result is None:
            logger.error(f"🔴 Search sort order update failed for FileID: {file_id_db}")
            raise Exception("Search sort order update failed")
        
        # Stage 3: Generate download file
        logger.info(f"💾 Generating download file for FileID: {file_id_db}")
        result = await generate_download_file(file_id_db, logger=logger)
        if "error" in result:
            logger.error(f"🔴 Failed to generate download file: {result['error']}")
            raise Exception(result["error"])
        
        logger.info(f"Restart processing completed successfully for FileID: {file_id_db}")
        return {"message": "Restart processing completed successfully", "file_id": file_id_db}
    except Exception as e:
        logger.error(f"🔴 Error restarting processing for FileID {file_id_db}: {e}", exc_info=True)
        send_to_email = get_send_to_email(file_id_db, logger=logger)
        file_name = f"FileID: {file_id_db}"
        # Remove logger parameter from send_message_email call
        send_message_email(send_to_email, f"Error processing {file_name}", f"An error occurred while reprocessing your file: {str(e)}")
        return {"error": str(e)}
    
async def process_image_batch(payload, logger=None, file_id=None):
    """Process a new image batch from payload."""
    logger = logger or default_logger
    try:
        rows = payload.get('rowData', [])
        if not rows:
            logger.error("No rowData provided in payload")
            raise ValueError("No rowData provided")
        
        provided_file_path = payload.get('filePath')
        if not provided_file_path:
            logger.error("No filePath provided in payload")
            raise ValueError("No filePath provided")
        
        file_name = provided_file_path.split('/')[-1]
        send_to_email = payload.get('sendToEmail', 'nik@iconluxurygroup.com')
        
        file_id_db = insert_file_db(file_name, provided_file_path, send_to_email, logger=logger)
        load_payload_db(rows, file_id_db, logger=logger)
        get_lm_products(file_id_db, logger=logger)
        
        search_df = get_records_to_search(file_id_db, logger=logger)
        if not search_df.empty:
            search_list = search_df.to_dict('records')
            BATCH_SIZE = 100
            batches = [search_list[i:i + BATCH_SIZE] for i in range(0, len(search_list), BATCH_SIZE)]
            logger.info(f"Processing {len(batches)} batches for FileID: {file_id_db}")
            futures = [process_batch.remote(batch, logger=logger) for batch in batches]
            batch_results = ray.get(futures)
            all_results = [result for batch_result in batch_results for result in batch_result]
            success_count = sum(1 for r in all_results if r['status'] == 'success')
            logger.info(f"Completed {success_count}/{len(all_results)} searches successfully")
        else:
            logger.info(f"No records to search for FileID: {file_id_db}")
        
        update_search_sort_order(file_id_db, logger=logger)
        await process_images(file_id_db, logger=logger)
        result = await generate_download_file(file_id_db, logger=logger)
        if "error" in result:
            raise Exception(result["error"])
        
        logger.info(f"Processing completed for FileID: {file_id_db}")
        return {"message": "Processing completed successfully"}
    except Exception as e:
        logger.error(f"🔴 Error processing batch: {e}")
        send_message_email(send_to_email, f"Error processing {file_name}", f"An error occurred while processing your file: {str(e)}", logger=logger)
        return {"error": str(e)}

def prepare_images_for_download_dataframe(df, logger=None):
    """Prepare image data for download from DataFrame."""
    logger = logger or default_logger
    try:
        if df.empty:
            logger.warning("Empty DataFrame provided for image preparation")
            return []
        
        images_to_download = [
            (row.ExcelRowID, row.ImageUrl, row.ImageUrlThumbnail)
            for row in df.itertuples(index=False)
            if row.ImageUrl and row.ImageUrl != 'No google image results found'
        ]
        logger.info(f"Prepared {len(images_to_download)} images for download")
        return images_to_download
    except Exception as e:
        logger.error(f"🔴 Error preparing images for download: {e}")
        return []
