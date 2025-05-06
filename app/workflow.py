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
    process_search_row_gcloud,
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


import asyncio
import logging
import pyodbc
import pandas as pd
from typing import Optional, Dict
from ray_workers import fetch_brand_rules, process_batch, BRAND_RULES_URL
from config import conn_str
from database import insert_search_results, update_search_sort_order, process_search_row_gcloud
import asyncio
import logging
import pyodbc
import pandas as pd
from typing import Optional, Dict
from ray_workers import fetch_brand_rules, process_batch, BRAND_RULES_URL
from config import conn_str
from database import insert_search_results, update_search_sort_order, process_search_row_gcloud

import asyncio
import logging
import pyodbc
import pandas as pd
from typing import Optional, Dict
from ray_workers import fetch_brand_rules, process_batch, BRAND_RULES_URL
from config import conn_str
from database import insert_search_results, update_search_sort_order, process_search_row_gcloud

import asyncio
import logging
from typing import Optional
import pyodbc
import pandas as pd
import ray
from config import conn_str  # Assumes database connection string is defined here
from database import insert_search_results, update_search_sort_order  # Database helper functions
from ray_workers import process_batch  # Ray-based batch processing function
import asyncio
import logging
import pyodbc
import pandas as pd
import ray
from typing import Optional, Dict
from config import conn_str
from database import insert_search_results, update_search_sort_order, process_search_row_gcloud
from ray_workers import process_batch
import asyncio
import logging
import pyodbc
import pandas as pd
from typing import Optional
from sqlalchemy import create_engine
from config import conn_str
from database import insert_search_results, update_search_sort_order
async def process_single_row(
    entry_id: int,
    search_string: str,
    max_row_retries: int,
    file_id_db: int,
    brand_rules: dict,
    brand: Optional[str] = None,
    model: Optional[str] = None,
    logger: Optional[logging.Logger] = None
) -> bool:
    logger = logger or default_logger
    
    try:
        entry_id = int(entry_id)
        file_id_db = int(file_id_db)
        if not search_string or not isinstance(search_string, str):
            logger.error(f"Invalid search string for EntryID {entry_id}")
            return False
        model = model or search_string
    except (ValueError, TypeError) as e:
        logger.error(f"Invalid input parameters for EntryID {entry_id}: {e}", exc_info=True)
        return False

    search_types = ["default", "brand_name", "retry_with_alternative"]
    loop = asyncio.get_running_loop()
    all_results = []
    result_brand = brand
    result_model = model
    result_category = None

    api_to_db_mapping = {
        'image_url': 'ImageUrl', 'thumbnail_url': 'ImageUrlThumbnail', 'url': 'ImageUrl',
        'thumb': 'ImageUrlThumbnail', 'image': 'ImageUrl', 'thumbnail': 'ImageUrlThumbnail',
        'img_url': 'ImageUrl', 'thumb_url': 'ImageUrlThumbnail', 'imageURL': 'ImageUrl',
        'imageUrl': 'ImageUrl', 'thumbnailURL': 'ImageUrlThumbnail', 'thumbnailUrl': 'ImageUrlThumbnail',
        'brand': 'Brand', 'model': 'Model', 'brand_name': 'Brand', 'product_model': 'Model'
    }
    required_columns = ["EntryID", "ImageUrl", "ImageDesc", "ImageSource", "ImageUrlThumbnail"]

    if not brand or not model or not result_category:
        try:
            with pyodbc.connect(conn_str, autocommit=False) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT ProductBrand, ProductModel, ProductCategory FROM utb_ImageScraperRecords WHERE FileID = ? AND EntryID = ?",
                    (file_id_db, entry_id)
                )
                result = cursor.fetchone()
                if result:
                    result_brand = result_brand or result[0]
                    result_model = result_model or result[1]
                    result_category = result[2]
                    logger.info(f"Fetched attributes for EntryID {entry_id}: Brand={result_brand}, Model={result_model}, Category={result_category}")
                else:
                    logger.warning(f"No attributes found for FileID {file_id_db}, EntryID {entry_id}")
        except pyodbc.Error as e:
            logger.error(f"Failed to fetch attributes for EntryID {entry_id}: {e}", exc_info=True)

    for search_type in search_types:
        logger.info(f"Processing search type '{search_type}' for EntryID {entry_id}")
        batch = [{"EntryID": entry_id, "SearchString": search_string, "SearchTypes": [search_type]}]
        try:
            from ray_workers import process_batch
            results_ref = process_batch.remote(batch, brand_rules, logger=logger)
            results = ray.get(results_ref)

            for res in results:
                if res.get("status") != "success" or res.get("result") is None:
                    logger.warning(f"Search type '{search_type}' failed for EntryID {entry_id}")
                    continue

                result_df = res["result"]
                logger.debug(f"Raw result_df columns for EntryID {entry_id}: {list(result_df.columns)}")

                for api_col, db_col in api_to_db_mapping.items():
                    if api_col in result_df.columns and db_col not in result_df.columns:
                        result_df.rename(columns={api_col: db_col}, inplace=True)

                missing = [col for col in required_columns if col not in result_df.columns]
                if missing:
                    logger.error(f"Missing columns {missing} in result_df for EntryID {entry_id}")
                    continue

                result_df["EntryID"] = entry_id
                result_brand = result_df.get('Brand', [result_brand])[0] if 'Brand' in result_df else result_brand
                result_model = result_df.get('Model', [result_model])[0] if 'Model' in result_df else result_model
                type_results = result_df.head(50)  # Cap at 50 results
                logger.info(f"Stored {len(type_results)} results for EntryID {entry_id} from '{search_type}'")
                all_results.append(type_results)
                break  # Use first successful result
        except Exception as e:
            logger.error(f"Error processing search type '{search_type}' for EntryID {entry_id}: {e}", exc_info=True)

    if all_results:
        try:
            combined_df = pd.concat(all_results, ignore_index=True)
            logger.info(f"Combined {len(combined_df)} results for EntryID {entry_id} for batch insertion")
            deduplicated_df = combined_df.drop_duplicates(subset=['EntryID', 'ImageUrl'], keep='first')
            logger.info(f"Deduplicated to {len(deduplicated_df)} rows")
            insert_success = await loop.run_in_executor(None, insert_search_results, deduplicated_df, logger)
            if not insert_success:
                logger.error(f"Failed to insert deduplicated results for EntryID {entry_id}")
                return False
            logger.info(f"Inserted {len(deduplicated_df)} results for EntryID {entry_id}")
            update_result = await loop.run_in_executor(
                None, update_search_sort_order, str(file_id_db), str(entry_id), result_brand, result_model, None, result_category, logger
            )
            if update_result is None:
                logger.error(f"SortOrder update failed for EntryID {entry_id}")
                return False
            logger.info(f"Updated sort order for EntryID {entry_id} with Brand: {result_brand}, Model: {result_model}, Category: {result_category}")

            try:
                with pyodbc.connect(conn_str, autocommit=False) as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT COUNT(*) FROM utb_ImageScraperResult WHERE EntryID = ? AND SortOrder > 0",
                        (entry_id,)
                    )
                    count = cursor.fetchone()[0]
                    cursor.execute(
                        "SELECT COUNT(*) FROM utb_ImageScraperResult WHERE EntryID = ? AND SortOrder IS NULL",
                        (entry_id,)
                    )
                    null_count = cursor.fetchone()[0]
                    logger.info(f"Verification: Found {count} rows with positive SortOrder, {null_count} rows with NULL SortOrder for EntryID {entry_id}")
                    if null_count > 0:
                        logger.error(f"Found {null_count} rows with NULL SortOrder after update for EntryID {entry_id}")
            except pyodbc.Error as e:
                logger.error(f"Failed to verify SortOrder for EntryID {entry_id}: {e}", exc_info=True)

            return True
        except Exception as e:
            logger.error(f"Error during batch database update for EntryID {entry_id}: {e}", exc_info=True)
            return False

    logger.info(f"No results to insert for EntryID {entry_id}")
    return False
import asyncio
import logging
import os
import pyodbc
from typing import Optional, Dict
from concurrent.futures import ThreadPoolExecutor
from ray_workers import fetch_brand_rules, process_batch, BRAND_RULES_URL
from config import conn_str
from database import (
    insert_search_results,
    process_search_row_gcloud,
    update_search_sort_order,
    get_send_to_email,
    update_log_url_in_db
)
from aws_s3 import upload_file_to_space
from email_utils import send_message_email
async def process_restart_batch(
    file_id_db: int,
    max_retries: int = 15,
    logger: Optional[logging.Logger] = None
) -> Dict[str, str]:
    logger = logger or logging.getLogger(f"job_{file_id_db}")
    log_filename = logger.handlers[0].baseFilename if logger.handlers else os.path.join(os.getcwd(), 'logs', f"file_{file_id_db}.log")

    try:
        logger.info(f"🔁 Starting concurrent search processing for FileID: {file_id_db}")
        file_id_db = int(file_id_db)

        brand_rules = await fetch_brand_rules(BRAND_RULES_URL, logger=logger)
        if not brand_rules:
            logger.warning("⚠️ Failed to load brand rules; using default logic")
            brand_rules = {"brand_rules": []}

        with pyodbc.connect(conn_str, autocommit=False) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT EntryID, ProductModel, ProductBrand FROM utb_ImageScraperRecords WHERE FileID = ?",
                (file_id_db,)
            )
            entries = [(row[0], row[1], row[2]) for row in cursor.fetchall() if row[1] is not None]
            logger.info(f"📋 Found {len(entries)} valid entries for FileID: {file_id_db}")

        if not entries:
            logger.warning(f"⚠️ No entries found for FileID: {file_id_db}")
            return {"error": f"No entries found for FileID: {file_id_db}"}

        tasks = [
            process_single_row(
                entry_id, search_string, max_retries, file_id_db, brand_rules,
                brand=brand, model=search_string, logger=logger
            )
            for entry_id, search_string, brand in entries
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        total_processed = len(tasks)
        successful_entries = sum(1 for result in results if result is True)
        failed_entries = sum(1 for result in results if isinstance(result, Exception))

        with pyodbc.connect(conn_str, autocommit=False) as conn:
            cursor = conn.cursor()
            time.sleep(5)  # Ensure transaction visibility
            cursor.execute(
                """
                SELECT COUNT(DISTINCT t.EntryID)
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                WHERE r.FileID = ? AND t.SortOrder > 0
                """,
                (file_id_db,)
            )
            positive_entries = cursor.fetchone()[0]
            cursor.execute(
                """
                SELECT COUNT(DISTINCT t.EntryID)
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                WHERE r.FileID = ? AND t.SortOrder IS NULL
                """,
                (file_id_db,)
            )
            null_entries = cursor.fetchone()[0]
            logger.info(f"Final verification: Found {positive_entries} entries with positive SortOrder, {null_entries} entries with NULL SortOrder")

            cursor.execute(
                """
                SELECT TOP 5 t.ResultID, t.EntryID, t.SortOrder, t.ImageDesc
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                WHERE r.FileID = ? AND t.SortOrder > 0
                ORDER BY t.SortOrder
                """, (file_id_db,)
            )
            sample_results = cursor.fetchall()
            for result in sample_results:
                logger.info(f"Sample - ResultID: {result[0]}, EntryID: {result[1]}, SortOrder: {result[2]}, ImageDesc: {result[3]}")

        if os.path.exists(log_filename):
            loop = asyncio.get_running_loop()
            upload_url = await loop.run_in_executor(
                None,
                lambda: upload_file_to_space(
                    log_filename, f"job_logs/job_{file_id_db}.log", True, logger, file_id_db
                )
            )
            logger.info(f"📤 Log file uploaded to: {upload_url}")
            await update_log_url_in_db(str(file_id_db), upload_url, logger)

        logger.info(f"✅ Completed processing for FileID: {file_id_db}. {positive_entries}/{total_processed} entries with positive SortOrder. Failed entries: {failed_entries}")
        return {
            "message": "Search processing completed",
            "file_id": str(file_id_db),
            "successful_entries": str(successful_entries),
            "total_entries": str(total_processed),
            "failed_entries": str(failed_entries)
        }

    except Exception as e:
        logger.error(f"🔴 Error processing FileID {file_id_db}: {e}", exc_info=True)
        if os.path.exists(log_filename):
            loop = asyncio.get_running_loop()
            upload_url = await loop.run_in_executor(
                None,
                lambda: upload_file_to_space(
                    log_filename, f"job_logs/job_{file_id_db}.log", True, logger, file_id_db
                )
            )
            logger.info(f"📤 Log file uploaded to: {upload_url}")
            await update_log_url_in_db(str(file_id_db), upload_url, logger)

        send_to_email_addr = get_send_to_email(file_id_db, logger)  # Synchronous call
        if send_to_email_addr:
            await send_message_email(
                send_to_email_addr,
                f"Error processing FileID: {file_id_db}",
                f"An error occurred while processing your file: {str(e)}"
            )
        return {"error": str(e)}