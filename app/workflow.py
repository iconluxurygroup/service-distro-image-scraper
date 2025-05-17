
import threading
from concurrent.futures import ThreadPoolExecutor
import logging
import asyncio
import os
import pandas as pd
import time
import pyodbc
import httpx
import json
import aiofiles
import datetime
from typing import Optional, Dict, List, Tuple
from db_utils import (
    sync_get_endpoint,
    update_search_sort_order,
    get_send_to_email,
    insert_search_results,
    get_images_excel_db,
    fetch_missing_images,
    update_file_location_complete,
    update_file_generate_complete,
    export_dai_json,
    update_log_url_in_db,
    fetch_last_valid_entry,
)
from image_utils import download_all_images
from excel_utils import write_excel_image, write_failed_downloads_to_excel
from common import fetch_brand_rules
from utils import (
    create_temp_dirs,
    cleanup_temp_dirs,
    process_and_tag_results,
    generate_search_variations,
    process_search_row_gcloud
)
from logging_config import setup_job_logger
from aws_s3 import upload_file_to_space
import psutil
from email_utils import send_message_email
from image_reason import process_entry
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import aiohttp
from database_config import conn_str, async_engine, engine
from sqlalchemy.sql import text

BRAND_RULES_URL = os.getenv("BRAND_RULES_URL", "https://raw.githubusercontent.com/iconluxurygroup/legacy-icon-product-api/refs/heads/main/task_settings/brand_settings.json")

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=15),
    retry=retry_if_exception_type((aiohttp.ClientError, TimeoutError, pd.errors.EmptyDataError, ValueError)),
    before_sleep=lambda retry_state: logging.getLogger(f"worker_{retry_state.kwargs['entry_id']}").info(
        f"Worker PID {psutil.Process().pid}: Retrying task for EntryID {retry_state.kwargs['entry_id']} (attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def async_process_entry_search(
    search_string: str,
    brand: str,
    endpoint: str,
    entry_id: int,
    use_all_variations: bool,
    file_id_db: int,
    logger: logging.Logger
) -> List[pd.DataFrame]:
    process = psutil.Process()
    mem_info = process.memory_info()
    logger.debug(f"Worker PID {process.pid}: Memory before task for EntryID {entry_id}: RSS={mem_info.rss / 1024**2:.2f} MB")
    
    result = await process_and_tag_results(
        search_string=search_string,
        brand=brand,
        model=search_string,
        endpoint=endpoint,
        entry_id=entry_id,
        logger=logger,
        use_all_variations=use_all_variations,
        file_id_db=file_id_db
    )
    
    for df in result:
        if 'EntryID' in df.columns and not df['EntryID'].eq(entry_id).all():
            logger.error(f"Worker PID {process.pid}: EntryID mismatch in DataFrame for EntryID {entry_id}: {df['EntryID'].tolist()}")
            raise ValueError(f"EntryID mismatch in DataFrame for EntryID {entry_id}")
    
    mem_info = process.memory_info()
    logger.debug(f"Worker PID {process.pid}: Memory after task for EntryID {entry_id}: RSS={mem_info.rss / 1024**2:.2f} MB")
    return result

    

def process_entry_search(args):
    search_string, brand, endpoint, entry_id, use_all_variations, file_id_db = args
    logger = logging.getLogger(f"worker_{entry_id}")
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(f"job_logs/worker_{entry_id}.log")
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    process = psutil.Process()  # Define process here
    try:
        mem_info = process.memory_info()  # Use process instead of psutil.Process()
        logger.debug(f"Worker PID {process.pid}: Memory: RSS={mem_info.rss / 1024**2:.2f} MB")  # Fixed
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(
                async_process_entry_search(
                    search_string=search_string,
                    brand=brand,
                    endpoint=endpoint,
                    entry_id=entry_id,
                    use_all_variations=use_all_variations,
                    file_id_db=file_id_db,
                    logger=logger
                )
            )
            logger.debug(f"Worker PID {process.pid}: Result for EntryID {entry_id}: {result}")
            return result
        finally:
            loop.close()
    except Exception as e:
        logger.error(f"Worker PID {process.pid}: Task failed for EntryID {entry_id}: {e}", exc_info=True)  # Fixed
        return None
    finally:
        logger.removeHandler(handler)
        handler.close()

async def process_restart_batch(
    file_id_db: int,
    entry_id: Optional[int] = None,
    use_all_variations: bool = False,
    logger: Optional[logging.Logger] = None
) -> Dict[str, str]:
    log_filename = f"job_logs/job_{file_id_db}.log"
    try:
        if logger is None:
            logger, log_filename = setup_job_logger(job_id=str(file_id_db), log_dir="job_logs", console_output=True)
        logger.setLevel(logging.DEBUG)
        process = psutil.Process()
        logger.debug(f"Worker PID {process.pid}: Logger initialized")

        def log_memory_usage():
            mem_info = process.memory_info()
            logger.info(f"Worker PID {process.pid}: Memory: RSS={mem_info.rss / 1024**2:.2f} MB")
            if mem_info.rss / 1024**2 > 1000:
                logger.warning(f"Worker PID {process.pid}: High memory usage")

        logger.info(f"Worker PID {process.pid}: Starting processing for FileID: {file_id_db}")
        log_memory_usage()

        file_id_db_int = file_id_db
        BATCH_SIZE = 1
        CPU_CORES = psutil.cpu_count(logical=False) or 4
        MAX_WORKERS = min(CPU_CORES * 2, 8)

        logger.info(f"Worker PID {process.pid}: Detected {CPU_CORES} physical CPU cores, setting max_workers={MAX_WORKERS}")

        with pyodbc.connect(conn_str, autocommit=False, timeout=30) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM utb_ImageScraperFiles WHERE ID = ?", (file_id_db_int,))
            if cursor.fetchone()[0] == 0:
                logger.error(f"Worker PID {process.pid}: FileID {file_id_db} does not exist")
                cursor.close()
                return {"error": f"FileID {file_id_db} does not exist", "log_filename": log_filename, "log_public_url": "", "last_entry_id": str(entry_id or "")}
            cursor.close()

        if entry_id is None:
            entry_id = await fetch_last_valid_entry(str(file_id_db_int), logger)
            if entry_id is not None:
                with pyodbc.connect(conn_str) as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT MIN(EntryID) FROM utb_ImageScraperRecords WHERE FileID = ? AND EntryID > ?",
                        (file_id_db_int, entry_id)
                    )
                    next_entry = cursor.fetchone()
                    entry_id = next_entry[0] if next_entry and next_entry[0] else None
                    logger.info(f"Worker PID {process.pid}: Resuming from EntryID: {entry_id}")
                    cursor.close()

        brand_rules = await fetch_brand_rules(BRAND_RULES_URL, max_attempts=3, timeout=10, logger=logger)
        if not brand_rules:
            logger.warning(f"Worker PID {process.pid}: No brand rules fetched")
            return {"message": "Failed to fetch brand rules", "file_id": str(file_id_db), "log_filename": log_filename, "log_public_url": "", "last_entry_id": str(entry_id or "")}

        endpoint = None
        for attempt in range(5):
            try:
                endpoint = sync_get_endpoint(logger=logger)
                if endpoint:
                    logger.info(f"Worker PID {process.pid}: Selected endpoint: {endpoint}")
                    break
                logger.warning(f"Worker PID {process.pid}: Attempt {attempt + 1} failed")
                time.sleep(2)
            except Exception as e:
                logger.warning(f"Worker PID {process.pid}: Attempt {attempt + 1} failed: {e}")
                time.sleep(2)
        if not endpoint:
            logger.error(f"Worker PID {process.pid}: No healthy endpoint")
            return {"error": "No healthy endpoint", "log_filename": log_filename, "log_public_url": "", "last_entry_id": str(entry_id or "")}

        with pyodbc.connect(conn_str, autocommit=False, timeout=30) as conn:
            cursor = conn.cursor()
            query = """
                SELECT EntryID, ProductModel, ProductBrand, ProductColor, ProductCategory 
                FROM utb_ImageScraperRecords 
                WHERE FileID = ? AND (? IS NULL OR EntryID >= ?) 
                ORDER BY EntryID
            """
            cursor.execute(query, (file_id_db_int, entry_id, entry_id))
            entries = [(row[0], row[1], row[2], row[3], row[4]) for row in cursor.fetchall() if row[1] is not None]
            logger.info(f"Worker PID {process.pid}: Found {len(entries)} entries")
            cursor.close()
        if not entries:
            logger.warning(f"Worker PID {process.pid}: No entries found")
            return {"error": "No entries found", "log_filename": log_filename, "log_public_url": "", "last_entry_id": str(entry_id or "")}

        entry_batches = [entries[i:i + BATCH_SIZE] for i in range(0, len(entries), BATCH_SIZE)]
        logger.info(f"Worker PID {process.pid}: Created {len(entry_batches)} batches")

        successful_entries = 0
        failed_entries = 0
        last_entry_id_processed = entry_id or 0
        api_to_db_mapping = {
            'image_url': 'ImageUrl', 'thumbnail_url': 'ImageUrlThumbnail', 'url': 'ImageUrl',
            'thumb': 'ImageUrlThumbnail', 'image': 'ImageUrl', 'thumbnail': 'ImageUrlThumbnail'
        }
        required_columns = ["EntryID", "ImageUrl", "ImageDesc", "ImageSource", "ImageUrlThumbnail"]

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for batch_idx, batch_entries in enumerate(entry_batches, 1):
                logger.info(f"Worker PID {process.pid}: Processing batch {batch_idx}/{len(entry_batches)}")
                start_time = datetime.datetime.now()

                tasks = [
                    (search_string, brand, endpoint, entry_id, use_all_variations, file_id_db)
                    for entry_id, search_string, brand, color, category in batch_entries
                ]
                results = [executor.submit(process_entry_search, task) for task in tasks]
                results = [future.result() for future in results]

                for (entry_id, search_string, brand, color, category), result in zip(batch_entries, results):
                    try:
                        if result is None or not result:
                            logger.error(f"Worker PID {process.pid}: No results for EntryID {entry_id}")
                            failed_entries += 1
                            continue

                        combined_df = pd.concat(result, ignore_index=True, copy=False)
                        if 'EntryID' not in combined_df.columns or not combined_df['EntryID'].eq(entry_id).all():
                            logger.error(f"Worker PID {process.pid}: EntryID mismatch for EntryID {entry_id}")
                            failed_entries += 1
                            continue

                        for api_col, db_col in api_to_db_mapping.items():
                            if api_col in combined_df.columns and db_col not in combined_df.columns:
                                combined_df.rename(columns={api_col: db_col}, inplace=True)

                        if not all(col in combined_df.columns for col in required_columns):
                            logger.error(f"Worker PID {process.pid}: Missing columns for EntryID {entry_id}")
                            failed_entries += 1
                            continue

                        deduplicated_df = combined_df.drop_duplicates(subset=['EntryID', 'ImageUrl'], keep='first')
                        insert_success = insert_search_results(deduplicated_df, logger=logger)
                        if not insert_success:
                            logger.error(f"Worker PID {process.pid}: Failed to insert results for EntryID {entry_id}")
                            failed_entries += 1
                            continue

                        update_result = await update_search_sort_order(
                            str(file_id_db), str(entry_id), brand, search_string, color, category, logger, brand_rules=brand_rules
                        )
                        if update_result is None:
                            logger.error(f"Worker PID {process.pid}: SortOrder update failed for EntryID {entry_id}")
                            failed_entries += 1
                            continue

                        successful_entries += 1
                        last_entry_id_processed = entry_id

                    except Exception as e:
                        logger.error(f"Worker PID {process.pid}: Error processing EntryID {entry_id}: {e}", exc_info=True)
                        failed_entries += 1

                elapsed_time = (datetime.datetime.now() - start_time).total_seconds()
                logger.info(f"Worker PID {process.pid}: Completed batch {batch_idx} in {elapsed_time:.2f}s")
                log_memory_usage()

        with pyodbc.connect(conn_str, autocommit=False, timeout=30) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT COUNT(DISTINCT t.EntryID), 
                       SUM(CASE WHEN t.SortOrder > 0 THEN 1 ELSE 0 END) AS positive_count,
                       SUM(CASE WHEN t.SortOrder IS NULL THEN 1 ELSE 0 END) AS null_count
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                WHERE r.FileID = ?
                """,
                (file_id_db_int,)
            )
            total_entries, positive_entries, null_entries = cursor.fetchone()
            logger.info(
                f"Worker PID {process.pid}: Verification: {total_entries} total entries, "
                f"{positive_entries} with positive SortOrder, {null_entries} with NULL SortOrder"
            )
            if null_entries > 0:
                logger.warning(f"Worker PID {process.pid}: Found {null_entries} entries with NULL SortOrder")
            cursor.close()

        # Skip log upload here; let generate_download_file handle it
        to_emails = await get_send_to_email(file_id_db, logger=logger)
        if to_emails:
            has_failure = await detect_job_failure(log_filename, logger)
            subject = f"Processing {'Failed' if has_failure else 'Completed'} for FileID: {file_id_db}"
            message = (
                f"Processing for FileID {file_id_db} {'failed' if has_failure else 'completed'}.\n"
                f"Successful entries: {successful_entries}/{len(entries)}\n"
                f"Failed entries: {failed_entries}\n"
                f"Last EntryID: {last_entry_id_processed}\n"
                f"Log file: {log_filename}"
            )
            await send_message_email(to_emails, subject=subject, message=message, logger=logger)

        return {
            "message": "Search processing completed",
            "file_id": str(file_id_db),
            "successful_entries": str(successful_entries),
            "total_entries": str(len(entries)),
            "failed_entries": str(failed_entries),
            "log_filename": log_filename,
            "log_public_url": "",
            "last_entry_id": str(last_entry_id_processed)
        }

    except Exception as e:
        logger.error(f"Worker PID {process.pid}: Error processing FileID {file_id_db}: {e}", exc_info=True)
        return {"error": str(e), "log_filename": log_filename, "log_public_url": "", "last_entry_id": str(entry_id or "")}
    finally:
        await async_engine.dispose()
        engine.dispose()
        logger.info(f"Worker PID {process.pid}: Disposed database engines")

async def generate_download_file(
    file_id: int,
    logger: Optional[logging.Logger] = None,
    file_id_param: Optional[int] = None
) -> Dict[str, str]:
    logger, log_filename = setup_job_logger(job_id=str(file_id), log_dir="job_logs", console_output=True)
    process = psutil.Process()
    temp_images_dir, temp_excel_dir = None, None

    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str, timeout=30) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT FileName FROM utb_ImageScraperFiles WHERE ID = ?", (file_id,))
            result = cursor.fetchone()
            cursor.close()
            if not result:
                logger.error(f"Worker PID {process.pid}: No file found for ID {file_id}")
                return {"error": f"No file found for ID {file_id}", "log_filename": log_filename}
            original_filename = result[0]

        logger.info(f"Worker PID {process.pid}: Fetching images for ID: {file_id}")
        mem_info = process.memory_info()
        logger.debug(f"Worker PID {process.pid}: Memory before fetching images: RSS={mem_info.rss / 1024**2:.2f} MB")
        
        selected_images_df = await get_images_excel_db(str(file_id), logger=logger)
        logger.info(f"Fetched DataFrame for ID {file_id}, shape: {selected_images_df.shape}, columns: {list(selected_images_df.columns)}")
        
        expected_columns = ["ExcelRowID", "ImageUrl", "ImageUrlThumbnail", "Brand", "Style", "Color", "Category"]
        if list(selected_images_df.columns) != expected_columns:
            logger.error(f"Invalid columns in DataFrame for ID {file_id}. Got: {list(selected_images_df.columns)}, Expected: {expected_columns}")
            return {"error": f"Invalid DataFrame columns: {list(selected_images_df.columns)}", "log_filename": log_filename}
        
        if selected_images_df.shape[1] != 7:
            logger.error(f"Invalid DataFrame shape for ID {file_id}: got {selected_images_df.shape}, expected (N, 7)")
            return {"error": f"Invalid DataFrame shape: {selected_images_df.shape}", "log_filename": log_filename}
        
        if selected_images_df.empty:
            logger.warning(f"Worker PID {process.pid}: No images found for ID {file_id}. Creating empty Excel file.")
            with pyodbc.connect(conn_str, timeout=30) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM utb_ImageScraperFiles WHERE ID = ?", (file_id,))
                file_count = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM utb_ImageScraperRecords WHERE FileID = ?", (file_id,))
                record_count = cursor.fetchone()[0]
                cursor.execute(
                    "SELECT COUNT(*) FROM utb_ImageScraperResult r INNER JOIN utb_ImageScraperRecords s ON r.EntryID = s.EntryID WHERE s.FileID = ? AND r.SortOrder >= 0",
                    (file_id,)
                )
                result_count = cursor.fetchone()[0]
                cursor.close()
                logger.info(f"Diagnostics: Files={file_count}, Records={record_count}, Results={result_count}")
            
            template_file_path = "https://iconluxurygroup.s3.us-east-2.amazonaws.com/ICON_DISTRO_USD_20250312.xlsx"
            base_name, extension = os.path.splitext(original_filename)
            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            unique_id = base_name[-8:] if len(base_name) >= 8 else base_name
            processed_file_name = f"super_scraper/jobs/{file_id}/{base_name}_scraper_{timestamp}_{unique_id}{extension}"
            
            temp_images_dir, temp_excel_dir = await create_temp_dirs(file_id, logger=logger)
            local_filename = os.path.join(temp_excel_dir, original_filename)
            
            async with httpx.AsyncClient() as client:
                response = await client.get(template_file_path, timeout=httpx.Timeout(30, connect=10))
                response.raise_for_status()
                async with aiofiles.open(local_filename, 'wb') as f:
                    await f.write(response.content)
            
            if not os.path.exists(local_filename):
                logger.error(f"Worker PID {process.pid}: Template file not found at {local_filename}")
                return {"error": f"Failed to download template file", "log_filename": log_filename}
            logger.debug(f"Worker PID {process.pid}: Template file saved: {local_filename}, size: {os.path.getsize(local_filename)} bytes")
            
            with pd.ExcelWriter(local_filename, engine='openpyxl', mode='a') as writer:
                pd.DataFrame({"Message": [f"No images found for FileID {file_id}. Check utb_ImageScraperResult for valid images."]}).to_excel(writer, sheet_name="NoImages", index=False)
            
            public_url = await upload_file_to_space(
                file_src=local_filename,
                save_as=processed_file_name,
                is_public=True,
                logger=logger,
                file_id=file_id
            )
            
            if not public_url:
                logger.error(f"Worker PID {process.pid}: Upload failed for ID {file_id}")
                return {"error": "Failed to upload processed file", "log_filename": log_filename}
            
            await update_file_location_complete(str(file_id), public_url, logger=logger)
            await update_file_generate_complete(str(file_id), logger=logger)
            
            send_to_email_addr = await get_send_to_email(file_id, logger=logger)
            if not send_to_email_addr:
                logger.error(f"Worker PID {process.pid}: No email address for ID {file_id}")
                return {"error": "Failed to retrieve email address", "log_filename": log_filename}
            subject_line = f"{original_filename} Job Notification - No Images"
            await send_message_email(
                to_emails=send_to_email_addr,
                subject=subject_line,
                message=(
                    f"No images were found for FileID {file_id}. An empty Excel file has been generated.\n"
                    f"Download URL: {public_url}\n"
                    f"Diagnostics: {file_count} files, {record_count} records, {result_count} image results."
                ),
                logger=logger
            )
            
            logger.info(f"Worker PID {process.pid}: Completed ID {file_id} with no images")
            return {"message": "No images found, empty Excel file generated", "public_url": public_url, "log_filename": log_filename}
        
        logger.debug(f"Sample DataFrame rows: {selected_images_df.head(2).to_dict(orient='records')}")
        
        selected_image_list = [
            {
                'ExcelRowID': int(row['ExcelRowID']),
                'ImageUrl': row['ImageUrl'],
                'ImageUrlThumbnail': row['ImageUrlThumbnail'],
                'Brand': row.get('Brand', ''),
                'Style': row.get('Style', ''),
                'Color': row.get('Color', ''),
                'Category': row.get('Category', '')
            }
            for _, row in selected_images_df.iterrows()
            if pd.notna(row['ImageUrl']) and row['ImageUrl']
        ]
        logger.info(f"Worker PID {process.pid}: Selected {len(selected_image_list)} valid images after filtering")
        
        if not selected_image_list:
            logger.warning(f"Worker PID {process.pid}: No valid images after filtering for ID {file_id}")
            template_file_path = "https://iconluxurygroup.s3.us-east-2.amazonaws.com/ICON_DISTRO_USD_20250312.xlsx"
            base_name, extension = os.path.splitext(original_filename)
            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            unique_id = base_name[-8:] if len(base_name) >= 8 else base_name
            processed_file_name = f"super_scraper/jobs/{file_id}/{base_name}_scraper_{timestamp}_{unique_id}{extension}"
            
            temp_images_dir, temp_excel_dir = await create_temp_dirs(file_id, logger=logger)
            local_filename = os.path.join(temp_excel_dir, original_filename)
            
            async with httpx.AsyncClient() as client:
                response = await client.get(template_file_path, timeout=httpx.Timeout(30, connect=10))
                response.raise_for_status()
                async with aiofiles.open(local_filename, 'wb') as f:
                    await f.write(response.content)
            
            if not os.path.exists(local_filename):
                logger.error(f"Worker PID {process.pid}: Template file not found at {local_filename}")
                return {"error": f"Failed to download template file", "log_filename": log_filename}
            
            with pd.ExcelWriter(local_filename, engine='openpyxl', mode='a') as writer:
                pd.DataFrame({"Message": [f"No valid images found for FileID {file_id} after filtering."]}).to_excel(writer, sheet_name="NoImages", index=False)
            
            public_url = await upload_file_to_space(
                file_src=local_filename,
                save_as=processed_file_name,
                is_public=True,
                logger=logger,
                file_id=file_id
            )
            
            if not public_url:
                logger.error(f"Worker PID {process.pid}: Upload failed for ID {file_id}")
                return {"error": "Failed to upload processed file", "log_filename": log_filename}
            
            await update_file_location_complete(str(file_id), public_url, logger=logger)
            await update_file_generate_complete(str(file_id), logger=logger)
            
            send_to_email_addr = await get_send_to_email(file_id, logger=logger)
            if not send_to_email_addr:
                logger.error(f"Worker PID {process.pid}: No email address for ID {file_id}")
                return {"error": "Failed to retrieve email address", "log_filename": log_filename}
            subject_line = f"{original_filename} Job Notification - No Images"
            await send_message_email(
                to_emails=send_to_email_addr,
                subject=subject_line,
                message=(
                    f"No valid images were found for FileID {file_id} after filtering.\n"
                    f"An empty Excel file has been generated.\n"
                    f"Download URL: {public_url}\n"
                    f"Diagnostics: {file_count} files, {record_count} records, {result_count} image results."
                ),
                logger=logger
            )
            
            logger.info(f"Worker PID {process.pid}: Completed ID {file_id} with no images")
            return {"message": "No valid images found, empty Excel file generated", "public_url": public_url, "log_filename": log_filename}
        
        logger.debug(f"Selected image list sample: {selected_image_list[:2]}")
        
        template_file_path = "https://iconluxurygroup.s3.us-east-2.amazonaws.com/ICON_DISTRO_USD_20250312.xlsx"
        header_index = 5
        base_name, extension = os.path.splitext(original_filename)
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = base_name[-8:] if len(base_name) >= 8 else base_name
        processed_file_name = f"super_scraper/jobs/{file_id}/{base_name}_scraper_{timestamp}_{unique_id}{extension}"
        logger.info(f"Worker PID {process.pid}: Generated filename: {processed_file_name}")

        temp_images_dir, temp_excel_dir = await create_temp_dirs(file_id, logger=logger)
        local_filename = os.path.join(temp_excel_dir, original_filename)

        logger.info(f"Worker PID {process.pid}: Downloading template file from {template_file_path}")
        async with httpx.AsyncClient() as client:
            response = await client.get(template_file_path, timeout=httpx.Timeout(30, connect=10))
            response.raise_for_status()
            async with aiofiles.open(local_filename, 'wb') as f:
                await f.write(response.content)
        
        if not os.path.exists(local_filename):
            logger.error(f"Worker PID {process.pid}: Template file not found at {local_filename}")
            return {"error": f"Failed to download template file", "log_filename": log_filename}
        logger.debug(f"Worker PID {process.pid}: Template file saved: {local_filename}, size: {os.path.getsize(local_filename)} bytes")

        logger.debug(f"Worker PID {process.pid}: Using temp_images_dir: {temp_images_dir}")
        failed_downloads = await download_all_images(selected_image_list, temp_images_dir, logger=logger)
        logger.info(f"Worker PID {process.pid}: Downloaded images, {len(failed_downloads)} failed")

        failed_downloads = [(url, int(row_id)) for url, row_id in failed_downloads]
        
        logger.info(f"Worker PID {process.pid}: Writing images with row_offset={header_index}")
        failed_rows = await write_excel_image(
            local_filename, temp_images_dir, selected_image_list, "A", header_index, logger
        )

        if failed_downloads:
            logger.info(f"Worker PID {process.pid}: Writing {len(failed_downloads)} failed downloads to Excel")
            success = await write_failed_downloads_to_excel(failed_downloads, local_filename, logger=logger)
            if not success:
                logger.warning(f"Worker PID {process.pid}: Failed to write some failed downloads to Excel")

        if not os.path.exists(local_filename):
            logger.error(f"Worker PID {process.pid}: Excel file not found at {local_filename}")
            return {"error": f"Excel file not found", "log_filename": log_filename}
        logger.debug(f"Worker PID {process.pid}: Excel file exists: {local_filename}, size: {os.path.getsize(local_filename)} bytes")
        logger.debug(f"Worker PID {process.pid}: Temp excel dir contents: {os.listdir(temp_excel_dir)}")

        mem_info = process.memory_info()
        logger.debug(f"Worker PID {process.pid}: Memory before S3 upload: RSS={mem_info.rss / 1024**2:.2f} MB")
        public_url = await upload_file_to_space(
            file_src=local_filename,
            save_as=processed_file_name,
            is_public=True,
            logger=logger,
            file_id=file_id
        )
        
        if not public_url:
            logger.error(f"Worker PID {process.pid}: Upload failed for ID {file_id}")
            return {"error": "Failed to upload processed file", "log_filename": log_filename}

        await update_file_location_complete(str(file_id), public_url, logger=logger)
        await update_file_generate_complete(str(file_id), logger=logger)

        send_to_email_addr = await get_send_to_email(file_id, logger=logger)
        if not send_to_email_addr:
            logger.error(f"Worker PID {process.pid}: No email address for ID {file_id}")
            return {"error": "Failed to retrieve email address", "log_filename": log_filename}
        subject_line = f"{original_filename} Job Notification"
        await send_message_email(
            to_emails=send_to_email_addr,
            subject=subject_line,
            message=f"Excel file generation for FileID {file_id} completed successfully.\nDownload URL: {public_url}",
            logger=logger
        )

        logger.info(f"Worker PID {process.pid}: Completed ID {file_id}")
        mem_info = process.memory_info()
        logger.debug(f"Worker PID {process.pid}: Memory after completion: RSS={mem_info.rss / 1024**2:.2f} MB")
        return {"message": "Processing completed successfully", "public_url": public_url, "log_filename": log_filename}
    except Exception as e:
        logger.error(f"Worker PID {process.pid}: Error for ID {file_id}: {e}", exc_info=True)
        send_to_email_addr = await get_send_to_email(file_id, logger=logger)
        if send_to_email_addr:
            await send_message_email(
                to_emails=send_to_email_addr,
                subject=f"Error: Job Failed for FileID {file_id}",
                message=f"Excel file generation for FileID {file_id} failed.\nError: {str(e)}\nLog file: {log_filename}",
                logger=logger
            )
        return {"error": f"An error occurred: {str(e)}", "log_filename": log_filename}
    finally:
        if temp_images_dir and temp_excel_dir:
            await cleanup_temp_dirs([temp_images_dir, temp_excel_dir], logger=logger)
        logger.info(f"Worker PID {process.pid}: Cleaned up temporary directories for ID {file_id}")
        await async_engine.dispose()
        engine.dispose()
        logger.info(f"Worker PID {process.pid}: Disposed database engines")

async def batch_vision_reason(
    file_id: str,
    entry_ids: Optional[List[int]] = None,
    step: int = 0,
    limit: int = 5000,
    concurrency: int = 10,
    logger: Optional[logging.Logger] = None
) -> None:
    logger, log_filename = setup_job_logger(job_id=str(file_id), log_dir="job_logs", console_output=True)
    process = psutil.Process()
    try:
        file_id = int(file_id)
        logger.info(f"Worker PID {process.pid}: Starting batch image processing for FileID: {file_id}, Step: {step}, Limit: {limit}")
        mem_info = process.memory_info()
        logger.debug(f"Worker PID {process.pid}: Memory before processing: RSS={mem_info.rss / 1024**2:.2f} MB")

        df = await fetch_missing_images(file_id, limit, True, logger)
        if df.empty:
            logger.warning(f"Worker PID {process.pid}: No missing images found for FileID: {file_id}")
            return

        if entry_ids is not None:
            df = df[df['EntryID'].isin(entry_ids)]
            if df.empty:
                logger.warning(f"Worker PID {process.pid}: No missing images found for specified EntryIDs: {entry_ids}")
                return

        columns_to_drop = ['Step1', 'Step2', 'Step3', 'Step4', 'CreateTime_1', 'CreateTime_2']
        df = df.drop(columns=[col for col in columns_to_drop if col in df.columns], errors='ignore')
        logger.info(f"Worker PID {process.pid}: Retrieved {len(df)} image rows for FileID: {file_id}")
        entry_ids_to_process = list(df.groupby('EntryID').groups.keys())

        valid_updates = []
        semaphore = asyncio.Semaphore(concurrency)
        async def process_with_semaphore(entry_id, df_subset):
            async with semaphore:
                return await process_entry_wrapper(file_id, entry_id, df_subset, logger)

        tasks = [
            process_with_semaphore(entry_id, df[df['EntryID'] == entry_id])
            for entry_id in entry_ids_to_process
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for entry_id, updates in zip(entry_ids_to_process, results):
            if isinstance(updates, Exception):
                logger.error(f"Worker PID {process.pid}: Error processing EntryID {entry_id}: {updates}", exc_info=True)
                continue
            if not updates:
                logger.warning(f"Worker PID {process.pid}: No valid updates for EntryID: {entry_id}")
                continue
            valid_updates.extend(updates)
            logger.info(f"Worker PID {process.pid}: Collected {len(updates)} updates for EntryID: {entry_id}")

        if valid_updates:
            with pyodbc.connect(conn_str, timeout=30) as conn:
                cursor = conn.cursor()
                cursor.executemany(
                    "UPDATE utb_ImageScraperResult SET AiJson = ?, ImageIsFashion = ?, AiCaption = ? WHERE ResultID = ?",
                    valid_updates
                )
                conn.commit()
                cursor.close()
                logger.info(f"Worker PID {process.pid}: Updated {len(valid_updates)} records: {[update[3] for update in valid_updates]}")

        for entry_id in entry_ids_to_process:
            with pyodbc.connect(conn_str, timeout=30) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT ProductBrand, ProductModel, ProductColor, ProductCategory
                    FROM utb_ImageScraperRecords
                    WHERE FileID = ? AND EntryID = ?
                    """,
                    (file_id, entry_id)
                )
                result = cursor.fetchone()
                cursor.close()
                product_brand = product_model = product_color = product_category = ''
                if result:
                    product_brand, product_model, product_color, product_category = result
                else:
                    logger.warning(f"Worker PID {process.pid}: No attributes for FileID: {file_id}, EntryID: {entry_id}")

            await update_search_sort_order(
                file_id=str(file_id),
                entry_id=str(entry_id),
                brand=product_brand,
                model=product_model,
                color=product_color,
                category=product_category,
                logger=logger
            )
            logger.info(f"Worker PID {process.pid}: Updated sort order for FileID: {file_id}, EntryID: {entry_id}")

        mem_info = process.memory_info()
        logger.debug(f"Worker PID {process.pid}: Memory before JSON export: RSS={mem_info.rss / 1024**2:.2f} MB")
        json_url = await export_dai_json(file_id, entry_ids, logger)
        if json_url:
            logger.info(f"Worker PID {process.pid}: DAI JSON exported to {json_url}")
            await update_log_url_in_db(file_id, json_url, logger)
        else:
            logger.warning(f"Worker PID {process.pid}: Failed to export DAI JSON for FileID: {file_id}")

        mem_info = process.memory_info()
        logger.debug(f"Worker PID {process.pid}: Memory after processing: RSS={mem_info.rss / 1024**2:.2f} MB")

    except Exception as e:
        logger.error(f"Worker PID {process.pid}: Error in batch_vision_reason for FileID {file_id}: {e}", exc_info=True)
        raise
    finally:
        await async_engine.dispose()
        engine.dispose()
        logger.info(f"Worker PID {process.pid}: Disposed database engines")

async def process_entry_wrapper(
    file_id: int,
    entry_id: int,
    entry_df: pd.DataFrame,
    logger: logging.Logger,
    max_retries: int = 3
) -> List[Tuple[str, bool, str, int]]:
    process = psutil.Process()
    attempt = 1
    while attempt <= max_retries:
        logger.info(f"Worker PID {process.pid}: Processing EntryID {entry_id}, attempt {attempt}/{max_retries}")
        try:
            mem_info = process.memory_info()
            logger.debug(f"Worker PID {process.pid}: Memory before processing EntryID {entry_id}: RSS={mem_info.rss / 1024**2:.2f} MB")
            
            if not all(pd.notna(entry_df.get('ResultID', pd.Series([])))):
                logger.error(f"Worker PID {process.pid}: Invalid ResultID in entry_df for EntryID {entry_id}")
                return []

            updates = await process_entry(file_id, entry_id, entry_df, logger)
            if not updates:
                logger.warning(f"Worker PID {process.pid}: No updates returned for EntryID {entry_id} on attempt {attempt}")
                attempt += 1
                await asyncio.sleep(2)
                continue

            valid_updates = []
            for update in updates:
                if not isinstance(update, (list, tuple)) or len(update) != 4:
                    logger.error(f"Worker PID {process.pid}: Invalid update tuple for EntryID {entry_id}: {update}")
                    continue
                
                ai_json, image_is_fashion, ai_caption, result_id = update
                if not isinstance(ai_json, str):
                    logger.error(f"Worker PID {process.pid}: Invalid ai_json type for ResultID {result_id}: {type(ai_json).__name__}")
                    ai_json = json.dumps({"error": f"Invalid ai_json type: {type(ai_json).__name__}", "result_id": result_id, "scores": {"sentiment": 0.0, "relevance": 0.0}})
                
                if is_valid_ai_result(ai_json, ai_caption or "", logger):
                    valid_updates.append((ai_json, image_is_fashion, ai_caption, result_id))
                else:
                    logger.warning(f"Worker PID {process.pid}: Invalid AI result for ResultID {result_id} on attempt {attempt}")

            if valid_updates:
                logger.info(f"Worker PID {process.pid}: Valid updates for EntryID {entry_id}: {len(valid_updates)}")
                mem_info = process.memory_info()
                logger.debug(f"Worker PID {process.pid}: Memory after processing EntryID {entry_id}: RSS={mem_info.rss / 1024**2:.2f} MB")
                return valid_updates
            else:
                logger.warning(f"Worker PID {process.pid}: No valid updates for EntryID {entry_id} on attempt {attempt}")
                attempt += 1
                await asyncio.sleep(2)
        
        except Exception as e:
            logger.error(f"Worker PID {process.pid}: Error processing EntryID {entry_id} on attempt {attempt}: {e}", exc_info=True)
            attempt += 1
            await asyncio.sleep(2)
    
    logger.error(f"Worker PID {process.pid}: Failed to process EntryID {entry_id} after {max_retries} attempts")
    return [
        (
            json.dumps({"scores": {"sentiment": 0.0, "relevance": 0.0}, "category": "unknown", "error": "Processing failed"}),
            False,
            "Failed to generate caption",
            int(row.get('ResultID', 0))
        ) for _, row in entry_df.iterrows() if pd.notna(row.get('ResultID'))
    ]

def is_valid_ai_result(ai_json: str, ai_caption: str, logger: logging.Logger) -> bool:
    process = psutil.Process()
    try:
        if not ai_caption or ai_caption.strip() == "":
            logger.warning(f"Worker PID {process.pid}: Invalid AI result: AiCaption is empty")
            return False
        
        parsed_json = json.loads(ai_json)
        if not isinstance(parsed_json, dict):
            logger.warning(f"Worker PID {process.pid}: Invalid AI result: AiJson is not a dictionary")
            return False
        
        if "scores" not in parsed_json or not parsed_json["scores"]:
            logger.warning(f"Worker PID {process.pid}: Invalid AI result: AiJson missing or empty 'scores' field, AiJson: {ai_json}")
            return False
        
        return True
    except json.JSONDecodeError as e:
        logger.warning(f"Worker PID {process.pid}: Invalid AI result: AiJson is not valid JSON: {e}, AiJson: {ai_json}")
        return False
async def detect_job_failure(log_filename: str, logger: logging.Logger) -> bool:
    try:
        async with aiofiles.open(log_filename, 'r') as f:
            content = await f.read()
            error_keywords = ['ERROR', 'Traceback', 'Exception', 'NameError', 'TypeError']
            for keyword in error_keywords:
                if keyword in content:
                    logger.warning(f"Detected failure in log {log_filename}: {keyword} found")
                    return True
        logger.info(f"No failure detected in log {log_filename}")
        return False
    except Exception as e:
        logger.error(f"Error reading log {log_filename}: {e}", exc_info=True)
        return True