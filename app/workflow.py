
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
    insert_search_results,
    update_search_sort_order,
    get_send_to_email,
    sync_update_search_sort_order,
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
    sync_process_and_tag_results,
    generate_search_variations,
    search_variation,
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
    """Async wrapper for process_and_tag_results with retry logic."""
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
    """Wrapper for async_process_entry_search to run in a thread."""
    search_string, brand, endpoint, entry_id, use_all_variations, file_id_db = args
    logger = logging.getLogger(f"worker_{entry_id}")
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(f"job_logs/worker_{entry_id}.log")
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    try:
        mem_info = psutil.Process().memory_info()
        logger.debug(f"Worker PID {process.pid}: Memory: RSS={mem_info.rss / 1024**2:.2f} MB")
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
        logger.error(f"Worker PID {process.pid}: Task failed for EntryID {entry_id}: {e}", exc_info=True)
        return None
    finally:
        logger.removeHandler(handler)
        handler.close()

def insert_search_results(df: pd.DataFrame, logger: logging.Logger) -> bool:
    """Insert search results into database with transaction isolation."""
    process = psutil.Process()
    try:
        with pyodbc.connect(conn_str, autocommit=False, timeout=30) as conn:
            cursor = conn.cursor()
            query = """
                INSERT INTO utb_ImageScraperResult (
                    EntryID, ImageUrl, ImageDesc, ImageSource, ImageUrlThumbnail
                ) VALUES (?, ?, ?, ?, ?)
            """
            for _, row in df.iterrows():
                cursor.execute(query, (
                    row['EntryID'],
                    row['ImageUrl'],
                    row.get('ImageDesc', ''),
                    row.get('ImageSource', ''),
                    row.get('ImageUrlThumbnail', '')
                ))
            conn.commit()
            logger.info(f"Worker PID {process.pid}: Inserted {len(df)} rows into utb_ImageScraperResult")
            cursor.close()
            return True
    except pyodbc.Error as e:
        logger.error(f"Worker PID {process.pid}: Failed to insert search results: {e}", exc_info=True)
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()

async def process_restart_batch(
    file_id_db: int,
    entry_id: Optional[int] = None,
    use_all_variations: bool = False,
    logger: Optional[logging.Logger] = None
) -> Dict[str, str]:
    """Process a batch of entries for a file using threading and upload log file to S3."""
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

        endpoint = sync_get_endpoint(logger=logger)
        if not endpoint:
            logger.error(f"Worker PID {process.pid}: No healthy endpoint")
            return {"error": "No healthy endpoint", "log_filename": log_filename, "log_public_url": "", "last_entry_id": str(entry_id or "")}

        with pyodbc.connect(conn_str, autocommit=False, timeout=30) as conn:
            cursor = conn.cursor()
            query = """
                SELECT EntryID, ProductModel, ProductBrand, ProductColor, ProductCategory 
                FROM utb_ImageScraperRecords 
                WHERE FileID = ? AND (:entry_id IS NULL OR EntryID >= :entry_id) 
                ORDER BY EntryID
            """
            cursor.execute(query, (file_id_db_int, entry_id))
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

        log_public_url = ""
        if os.path.exists(log_filename):
            log_public_url = await upload_file_to_space(
                file_src=log_filename,
                save_as=f"super_scraper/logs/job_{file_id_db}.log",
                is_public=True,
                logger=logger,
                file_id=file_id_db
            )
            if log_public_url:
                await update_log_url_in_db(str(file_id_db), log_public_url, logger)
            else:
                logger.error(f"Worker PID {process.pid}: Failed to upload log file")

        to_emails = await get_send_to_email(file_id_db, logger=logger)
        if to_emails:
            subject = f"Processing Completed for FileID: {file_id_db}"
            message = (
                f"Processing for FileID {file_id_db} completed.\n"
                f"Successful entries: {successful_entries}/{len(entries)}\n"
                f"Failed entries: {failed_entries}\n"
                f"Last EntryID: {last_entry_id_processed}\n"
                f"Log URL: {log_public_url or 'Not available'}"
            )
            await send_message_email(to_emails, subject=subject, message=message, logger=logger)

        return {
            "message": "Search processing completed",
            "file_id": str(file_id_db),
            "successful_entries": str(successful_entries),
            "total_entries": str(len(entries)),
            "failed_entries": str(failed_entries),
            "log_filename": log_filename,
            "log_public_url": log_public_url,
            "last_entry_id": str(last_entry_id_processed)
        }

    except Exception as e:
        logger.error(f"Worker PID {process.pid}: Error processing FileID {file_id_db}: {e}", exc_info=True)
        return {"error": str(e), "log_filename": log_filename, "log_public_url": "", "last_entry_id": str(entry_id or "")}
    finally:
        await async_engine.dispose()
        engine.dispose()
        logger.info(f"Worker PID {process.pid}: Disposed database engines")