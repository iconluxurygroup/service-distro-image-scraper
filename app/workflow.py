import asyncio
import os
import logging
import json
import pyodbc,httpx
import time,uuid
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from database import (
    insert_file_db, load_payload_db, get_records_to_search, 
    batch_process_images, fetch_missing_images, 
    update_file_location_complete, update_file_generate_complete, 
    get_send_to_email, get_file_location, get_images_excel_db, get_lm_products,
    process_search_row, get_endpoint, update_search_sort_order, update_initial_sort_order,update_log_url_in_db
)
    
import aiohttp
import pandas as pd
from io import BytesIO
import logging
import uuid

import asyncio
from config import conn_str
from aws_s3 import upload_file_to_space
from email_utils import send_email, send_message_email
from excel_utils import write_excel_image, write_failed_downloads_to_excel, verify_png_image_single
from image_processing import get_image_data, analyze_image_with_gemini, evaluate_with_grok_text
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
import aiofiles
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
async def download_image_with_sem(client, item, temp_dir, logger, semaphore):
    """Wrapper for download_image with semaphore."""
    async with semaphore:
        return await download_image(client, item, temp_dir, logger)
async def download_all_images(image_list, temp_dir, logger=None):
    """Download all images in batches of 200 with main-to-thumbnail fallback."""
    logger = logger or default_logger
    failed_img_urls = []
    
    if not image_list:
        logger.warning("⚠️ No images to download")
        return failed_img_urls
    
    logger.info(f"📥 Starting download of {len(image_list)} images for {temp_dir}")
    batch_size = 200
    semaphore = asyncio.Semaphore(50)  # Limit to 50 concurrent downloads per batch
    
    async with httpx.AsyncClient() as client:
        for i in range(0, len(image_list), batch_size):
            batch = image_list[i:i + batch_size]
            logger.info(f"Processing batch {i // batch_size + 1} with {len(batch)} images")
            tasks = [
                download_image_with_sem(client, item, temp_dir, logger, semaphore)
                for item in batch
                if item.get('ExcelRowID') and (item.get('ImageUrl') or item.get('ImageUrlThumbnail'))
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            batch_failures = [result for result in results if result is not None]
            failed_img_urls.extend(batch_failures)
            logger.info(f"Batch {i // batch_size + 1} completed, failures: {len(batch_failures)}")
    
    logger.info(f"📸 Completed image downloads. Total failed: {len(failed_img_urls)}/{len(image_list)}")
    return failed_img_urls
async def download_image(client, item, temp_dir, logger):
    """Download a single image with main-to-thumbnail fallback."""
    row_id = item.get('ExcelRowID')
    main_url = item.get('ImageUrl')
    thumb_url = item.get('ImageUrlThumbnail')
    image_path = os.path.join(temp_dir, f"{row_id}.png")
    timeout = httpx.Timeout(30, connect=10)
    
    # Try main URL
    try:
        response = await client.get(main_url, timeout=timeout)
        response.raise_for_status()
        with open(image_path, 'wb') as f:
            f.write(response.content)
        logger.info(f"✅ Downloaded main image for row {row_id}")
        return None  # Success, no failure
    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        logger.warning(f"⚠️ Failed main image for row {row_id}: {e}")
        
        # Try thumbnail URL
        try:
            response = await client.get(thumb_url, timeout=timeout)
            response.raise_for_status()
            with open(image_path, 'wb') as f:
                f.write(response.content)
            logger.info(f"✅ Downloaded thumbnail for row {row_id}")
            return None  # Success, no failure
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            logger.error(f"❌ Failed thumbnail for row {row_id}: {e}")
            return (main_url, row_id)  # Return failed URL and row ID
async def download_image(client, item, temp_dir, logger):
    """Download a single image with main-to-thumbnail fallback, using async file I/O."""
    row_id = item.get('ExcelRowID')
    main_url = item.get('ImageUrl')
    thumb_url = item.get('ImageUrlThumbnail')
    image_path = os.path.join(temp_dir, f"{row_id}.png")
    timeout = httpx.Timeout(30, connect=10)
    
    # Try main URL
    try:
        response = await client.get(main_url, timeout=timeout)
        response.raise_for_status()
        async with aiofiles.open(image_path, 'wb') as f:
            await f.write(response.content)
        logger.info(f"✅ Downloaded main image for row {row_id}")
        return None
    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        logger.warning(f"⚠️ Failed main image for row {row_id}: {e}")
        
        # Try thumbnail URL
        try:
            response = await client.get(thumb_url, timeout=timeout)
            response.raise_for_status()
            async with aiofiles.open(image_path, 'wb') as f:
                await f.write(response.content)
            logger.info(f"✅ Downloaded thumbnail for row {row_id}")
            return None
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            logger.error(f"❌ Failed thumbnail for row {row_id}: {e}")
            return (main_url, row_id)
    except Exception as e:
        logger.error(f"🔴 Unexpected error for row {row_id}: {e}")
        return (main_url, row_id)  # Catch all other
async def generate_download_file(file_id, logger=None, file_id_param=None):
    """Generate and upload a processed Excel file with images, appending a UUID to the filename."""
    logger = logger or default_logger
    temp_images_dir, temp_excel_dir = None, None
    try:
        preferred_image_method = 'append'
        start_time = time.time()
        loop = asyncio.get_running_loop()
        
        logger.info(f"🕵️ Fetching images for Excel export for FileID: {file_id}")
        selected_images_df = await loop.run_in_executor(ThreadPoolExecutor(), get_images_excel_db, file_id, logger)
        if selected_images_df.empty:
            logger.warning(f"⚠️ No images found for FileID {file_id} to generate download file")
            return {"error": f"No images found for FileID {file_id}"}
        
        logger.info(f"📋 Preparing images for download")
        selected_image_list = [
            {
                'ExcelRowID': row['ExcelRowID'],
                'ImageUrl': row['ImageURL'],
                'ImageUrlThumbnail': row['ImageURLThumbnail']
            } for _, row in selected_images_df.iterrows()
        ]
        logger.debug(f"📋 Selected image list: {len(selected_image_list)} items")
        
        logger.info(f"📍 Retrieving file location for FileID: {file_id}")
        # Fetch both FileLocationUrl and UserId (header_index)
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            query = "SELECT FileLocationUrl, UserId FROM utb_ImageScraperFiles WHERE ID = ?"
            cursor.execute(query, (file_id,))
            result = cursor.fetchone()
            if not result:
                logger.error(f"❌ No file location or header index found for FileID {file_id}")
                return {"error": "Original file not found"}
            provided_file_path, header_index_str = result
            header_index = int(header_index_str) if header_index_str else 0  # Default to 0 if not set
        
        file_name = provided_file_path.split('/')[-1]
        base_name, extension = os.path.splitext(file_name)
        unique_id = uuid.uuid4().hex[:8]
        processed_file_name = f"{base_name}_processed_{unique_id}{extension}"
        logger.info(f"🆔 Generated unique filename with UUID: {processed_file_name}")
        
        logger.info(f"📂 Creating temporary directories for FileID: {file_id}")
        temp_images_dir, temp_excel_dir = await create_temp_dirs(file_id, logger=logger)
        local_filename = os.path.join(temp_excel_dir, file_name)
        
        logger.info(f"📥 Downloading images for FileID: {file_id}")
        failed_img_urls = await download_all_images(selected_image_list, temp_images_dir, logger=logger)
        
        logger.info(f"📥 Downloading original file from {provided_file_path}")
        async with httpx.AsyncClient() as client:
            response = await client.get(provided_file_path, timeout=httpx.Timeout(30, connect=10))
            response.raise_for_status()
            with open(local_filename, "wb") as file:
                file.write(response.content)
        # Adjust row_offset based on header_index
        row_offset = header_index # Start writing images after the header row
        logger.info(f"📏 Set row_offset to {row_offset} based on header_index: {header_index}")
        
        logger.info(f"🖼️ Writing images to Excel for FileID: {file_id}")
        failed_rows = await loop.run_in_executor(
            ThreadPoolExecutor(),
            write_excel_image,
            local_filename,
            temp_images_dir,
            "A",  # Strictly column A
            row_offset,  # No offset
            logger
        )
        if failed_img_urls:
            logger.info(f"📝 Logging {len(failed_img_urls)} failed downloads to Excel")
            await loop.run_in_executor(
                ThreadPoolExecutor(),
                write_failed_downloads_to_excel,
                failed_img_urls,
                local_filename,
                logger
            )
        logger.info(f"📤 Uploading processed file to S3: {processed_file_name}")
        public_url = await loop.run_in_executor(ThreadPoolExecutor(), upload_file_to_space, local_filename, processed_file_name, True, logger, file_id)
        if not public_url:
            logger.error(f"❌ Failed to upload processed file for FileID {file_id}")
            return {"error": "Failed to upload processed file"}
        
        logger.info(f"📝 Updating file location and completion status for FileID: {file_id}")
        await loop.run_in_executor(ThreadPoolExecutor(), update_file_location_complete, file_id, public_url, logger)
        await loop.run_in_executor(ThreadPoolExecutor(), update_file_generate_complete, file_id, logger)
        
        subject_line = f"{file_name} Job Notification"
        send_to_email = await loop.run_in_executor(ThreadPoolExecutor(), get_send_to_email, file_id, logger)
        logger.info(f"📧 Sending email to {send_to_email} with download URL: {public_url}")
        # Fixed call with lambda to use keyword arguments
        await loop.run_in_executor(
            ThreadPoolExecutor(),
            lambda: send_email(
                to_emails=send_to_email,
                subject=subject_line,
                download_url=public_url,
                job_id=file_id,
                logger=logger
            )
        )
        
        logger.info(f"🏁 Processing completed for FileID {file_id} in {(time.time() - start_time):.2f} seconds")
        return {"message": "Processing completed successfully", "public_url": public_url}
    except Exception as e:
        logger.error(f"🔴 Error generating download file for FileID {file_id}: {e}", exc_info=True)
        return {"error": f"An error occurred: {str(e)}"}
    finally:
        if temp_images_dir and temp_excel_dir:
            await cleanup_temp_dirs([temp_images_dir, temp_excel_dir], logger=logger)
async def process_restart_batch(file_id_db, logger=None, file_id=None):
    """Restart processing for a failed batch."""
    logger = logger or default_logger
    log_filename = logger.handlers[0].baseFilename if logger.handlers else os.path.join(os.getcwd(), 'logs', f"file_{file_id_db}.log")
    
    try:
        logger.info(f"🔁 Restarting processing for FileID: {file_id_db}")
        file_id_db = int(file_id_db)
        
        logger.info(f"🖼️ Fetching missing images for FileID: {file_id_db}")
        missing_urls_df = fetch_missing_images(file_id_db, limit=10000, ai_analysis_only=False, logger=logger)
        logger.debug(f"🟨 🕵️‍♂️Missing URLs DataFrame columns: {missing_urls_df.columns.tolist()}")
        logger.debug(f"🟨 🕵️‍♂️Missing URLs DataFrame sample: {missing_urls_df.head().to_dict()}")

        image_url_col = 'ImageURL'
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
        
        logger.info(f"🔍🔀Updating search sort order for FileID: {file_id_db}")
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
        return {"message": "Restart processing completed successfully", "file_id": file_id_db}
    except Exception as e:
        logger.error(f"🔴 Error restarting processing for FileID {file_id_db}: {e}", exc_info=True)
        loop = asyncio.get_running_loop()
        if os.path.exists(log_filename):
            # Corrected argument order for upload_file_to_space
            upload_url = await loop.run_in_executor(
                ThreadPoolExecutor(), 
                upload_file_to_space, 
                log_filename,           # file_src
                f"job_logs/job_{file_id_db}.log",  # save_as
                True,                   # is_public
                logger,                 # logger
                file_id_db              # file_id
            )
            logger.info(f"Log file uploaded to: {upload_url}")
            await update_log_url_in_db(file_id_db, upload_url, logger)
        send_to_email = get_send_to_email(file_id_db, logger=logger)
        file_name = f"FileID: {file_id_db}"
        send_message_email(send_to_email, f"Error processing {file_name}", f"An error occurred while reprocessing your file: {str(e)}")
        return {"error": str(e)}
def column_letter_to_index(letter):
    """Convert Excel column letter (e.g., 'A') to zero-based index."""
    letter = letter.upper()
    index = 0
    for char in letter:
        index = index * 26 + (ord(char) - ord('A') + 1)
    return index - 1

async def process_image_batch(payload, logger=None, file_id=None):
    """Process a new image batch from payload, extracting columns and images from the file."""
    logger = logger or logging.getLogger(__name__)
    try:
        file_path = payload.get('filePath')
        image_column = payload.get('imageColumnImage')
        search_column = payload.get('searchColImage')
        brand_column = payload.get('brandColImage')
        color_column = payload.get('ColorColImage')
        category_column = payload.get('CategoryColImage')
        send_to_email = payload.get('sendToEmail', 'nik@iconluxurygroup.com')

        if not file_path:
            logger.error("No filePath provided in payload")
            raise ValueError("No filePath provided")

        if not all([image_column, search_column, brand_column]):
            logger.error("Missing required column names in payload")
            raise ValueError("Missing required column names")

        file_name = file_path.split('/')[-1]
        file_id = file_id or str(uuid.uuid4())
        logger.info(f"Processing image batch for FileID: {file_id}")

        # Fetch the Excel file from S3 URL
        async with aiohttp.ClientSession() as session:
            async with session.get(file_path) as response:
                if response.status != 200:
                    raise ValueError(f"Failed to fetch file from {file_path}: {response.status}")
                file_content = await response.read()

        # Parse the Excel file
        df = pd.read_excel(BytesIO(file_content))
        logger.info(f"Fetched and parsed Excel file with {len(df)} rows from {file_path}")

        # Normalize column names to be case-insensitive
        df.columns = [col.lower() for col in df.columns]
        col_map = {}
        for col in [image_column, search_column, brand_column, color_column, category_column]:
            if col:
                col_lower = col.lower()
                if col_lower.isalpha() and len(col_lower) <= 2:  # Assume letter if short and alphabetic
                    col_map[col] = df.columns[column_letter_to_index(col_lower)]
                elif col_lower in df.columns:  # Use name directly if present (case-insensitive)
                    col_map[col] = col_lower
                else:
                    raise ValueError(f"Column '{col}' not found in Excel file")

        # Extract data using mapped column names
        all_cols = list(col_map.values())
        extracted_data = df[all_cols].to_dict(orient='records')
        logger.info(f"Extracted {len(extracted_data)} rows with columns: {all_cols}")

        if not extracted_data:
            raise ValueError("No rows found in the provided file")

        # Process images from the image column
        images = []
        for row in extracted_data:
            image_ref = row.get(col_map[image_column])
            if image_ref:
                images.append(image_ref)
                logger.debug(f"Found image reference: {image_ref}")
            else:
                logger.warning(f"No image reference in row: {row}")

        # Insert file metadata into database
        file_id_db = insert_file_db(file_name, file_path, send_to_email, logger=logger)
        
        # Pass column mappings to load_payload_db
        load_payload_db(extracted_data, file_id_db, column_map={
            'image': col_map[image_column],
            'search': col_map[search_column],
            'brand': col_map[brand_column],
            'color': col_map.get(color_column),
            'category': col_map.get(category_column)
        }, logger=logger)
        
        get_lm_products(file_id_db, logger=logger)

        # Process search records with dynamic column names
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
        result = await generate_download_file(file_id_db, logger=logger)
        if "error" in result:
            raise Exception(result["error"])

        logger.info(f"Processing completed for FileID: {file_id_db}")
        return {"message": "Processing completed successfully"}

    except Exception as e:
        logger.error(f"🔴 Error processing batch: {e}")
        if send_to_email:
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
