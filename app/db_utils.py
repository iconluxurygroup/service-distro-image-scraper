import logging
import pandas as pd
import pyodbc
import asyncio
import json
import datetime
import os
import time
import hashlib
import psutil
import httpx
from fastapi import BackgroundTasks
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import text
from sqlalchemy import engine
from sqlalchemy import func
import aiofiles
from typing import Optional, List, Dict
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from database_config import conn_str, async_engine
from s3_utils import upload_file_to_space

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

BRAND_RULES_URL = os.getenv("BRAND_RULES_URL", "https://raw.githubusercontent.com/iconluxurygroup/legacy-icon-product-api/refs/heads/main/task_settings/brand_settings.json")

async def upload_log_file(file_id: str, log_filename: str, logger: logging.Logger) -> Optional[str]:
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(Exception),
        before_sleep=lambda retry_state: logger.info(
            f"Retrying log upload for FileID {file_id} (attempt {retry_state.attempt_number}/3)"
        )
    )
    async def try_upload():
        if not os.path.exists(log_filename):
            logger.warning(f"Log file {log_filename} does not exist, skipping upload")
            return None

        file_hash = hashlib.md5(open(log_filename, "rb").read()).hexdigest()
        current_time = time.time()
        key = (log_filename, file_id)

        LAST_UPLOAD = {}  # Local cache for deduplication
        if key in LAST_UPLOAD and LAST_UPLOAD[key]["hash"] == file_hash and current_time - LAST_UPLOAD[key]["time"] < 60:
            logger.info(f"Skipping redundant upload for {log_filename}")
            return LAST_UPLOAD[key]["url"]

        try:
            upload_url = await upload_file_to_space(
                file_src=log_filename,
                save_as=f"job_logs/job_{file_id}.log",
                is_public=True,
                logger=logger,
                file_id=file_id
            )
            await update_log_url_in_db(file_id, upload_url, logger)
            LAST_UPLOAD[key] = {"hash": file_hash, "time": current_time, "url": upload_url}
            logger.info(f"Log uploaded to R2: {upload_url}")
            return upload_url
        except Exception as e:
            logger.error(f"Failed to upload log for FileID {file_id}: {e}", exc_info=True)
            raise

    try:
        return await try_upload()
    except Exception as e:
        logger.error(f"Failed to upload log for FileID {file_id} after retries: {e}", exc_info=True)
        return None

async def async_process_entry_search(
    search_string: str,
    brand: str,
    endpoint: str,
    entry_id: int,
    use_all_variations: bool,
    file_id_db: int,
    logger: logging.Logger
) -> List[Dict]:
    process = psutil.Process()
    mem_info = process.memory_info()
    logger.debug(f"Worker PID {process.pid}: Memory before task for EntryID {entry_id}: RSS={mem_info.rss / 1024**2:.2f} MB")
    
    try:
        results = await process_and_tag_results(
            search_string=search_string,
            brand=brand,
            model=search_string,
            endpoint=endpoint,
            entry_id=entry_id,
            logger=logger,
            use_all_variations=use_all_variations,
            file_id_db=file_id_db
        )
        logger.info(f"Worker PID {process.pid}: Processed EntryID {entry_id} with {len(results)} results")
        
        if not results:
            logger.warning(f"Worker PID {process.pid}: No results for EntryID {entry_id}")
            return []
        
        required_columns = ['EntryID', 'ImageUrl', 'ImageDesc', 'ImageSource', 'ImageUrlThumbnail']
        for item in results:
            if not all(col in item for col in required_columns):
                missing_cols = set(required_columns) - set(item.keys())
                logger.error(f"Worker PID {process.pid}: Missing required columns {missing_cols} in result for EntryID {entry_id}")
                return []
        
        mem_info = process.memory_info()
        logger.debug(f"Worker PID {process.pid}: Memory after task for EntryID {entry_id}: RSS={mem_info.rss / 1024**2:.2f} MB")
        return results
    except Exception as e:
        logger.error(f"Worker PID {process.pid}: Failed to process EntryID {entry_id}: {e}", exc_info=True)
        return []

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
        logger.debug(f"Logger initialized")

        def log_memory_usage():
            mem_info = process.memory_info()
            logger.info(f"Memory: RSS={mem_info.rss / 1024**2:.2f} MB")
            if mem_info.rss / 1024**2 > 1000:
                logger.warning(f"High memory usage")

        logger.info(f"Starting processing for FileID: {file_id_db}")
        log_memory_usage()

        file_id_db_int = file_id_db
        BATCH_SIZE = 1
        MAX_CONCURRENCY = 4

        async with async_engine.connect() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) FROM utb_ImageScraperFiles WHERE ID = :file_id"),
                {"file_id": file_id_db_int}
            )
            if result.fetchone()[0] == 0:
                logger.error(f"FileID {file_id_db} does not exist")
                return {"error": f"FileID {file_id_db} does not exist", "log_filename": log_filename, "log_public_url": "", "last_entry_id": str(entry_id or "")}
            result.close()

        if entry_id is None:
            entry_id = await fetch_last_valid_entry(str(file_id_db_int), logger)
            if entry_id is not None:
                async with async_engine.connect() as conn:
                    result = await conn.execute(
                        text("SELECT MIN(EntryID) FROM utb_ImageScraperRecords WHERE FileID = :file_id AND EntryID > :entry_id"),
                        {"file_id": file_id_db_int, "entry_id": entry_id}
                    )
                    next_entry = result.fetchone()
                    entry_id = next_entry[0] if next_entry and next_entry[0] else None
                    logger.info(f"Resuming from EntryID: {entry_id}")
                    result.close()

        brand_rules = await fetch_brand_rules(BRAND_RULES_URL, max_attempts=3, timeout=10, logger=logger)
        if not brand_rules:
            logger.warning(f"No brand rules fetched")
            return {"message": "Failed to fetch brand rules", "file_id": str(file_id_db), "log_filename": log_filename, "log_public_url": "", "last_entry_id": str(entry_id or "")}

        endpoint = None
        for attempt in range(5):
            try:
                endpoint = sync_get_endpoint(logger=logger)
                if endpoint:
                    logger.info(f"Selected endpoint: {endpoint}")
                    break
                logger.warning(f"Attempt {attempt + 1} failed")
                await asyncio.sleep(2)
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                await asyncio.sleep(2)
        if not endpoint:
            logger.error(f"No healthy endpoint")
            return {"error": "No healthy endpoint", "log_filename": log_filename, "log_public_url": "", "last_entry_id": str(entry_id or "")}

        async with async_engine.connect() as conn:
            query = text("""
                SELECT EntryID, ProductModel, ProductBrand, ProductColor, ProductCategory 
                FROM utb_ImageScraperRecords 
                WHERE FileID = :file_id AND (:entry_id IS NULL OR EntryID >= :entry_id) 
                ORDER BY EntryID
            """)
            result = await conn.execute(query, {"file_id": file_id_db_int, "entry_id": entry_id})
            entries = [(row[0], row[1], row[2], row[3], row[4]) for row in result.fetchall() if row[1] is not None]
            logger.info(f"Found {len(entries)} entries")
            result.close()

        if not entries:
            logger.warning(f"No valid EntryIDs found for FileID {file_id_db}")
            return {"error": "No entries found", "log_filename": log_filename, "log_public_url": "", "last_entry_id": str(entry_id or "")}

        entry_batches = [entries[i:i + BATCH_SIZE] for i in range(0, len(entries), BATCH_SIZE)]
        logger.info(f"Created {len(entry_batches)} batches")

        successful_entries = 0
        failed_entries = 0
        last_entry_id_processed = entry_id or 0
        required_columns = ["EntryID", "ImageUrl", "ImageDesc", "ImageSource", "ImageUrlThumbnail"]

        semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
        async def process_entry(entry):
            entry_id, search_string, brand, color, category = entry
            async with semaphore:
                try:
                    logger.info(f"Processing EntryID {entry_id}")
                    results = await async_process_entry_search(
                        search_string=search_string,
                        brand=brand,
                        endpoint=endpoint,
                        entry_id=entry_id,
                        use_all_variations=use_all_variations,
                        file_id_db=file_id_db,
                        logger=logger
                    )
                    if not results:
                        logger.error(f"No results for EntryID {entry_id}")
                        return entry_id, False

                    if not all(all(col in res for col in required_columns) for res in results):
                        logger.error(f"Missing columns for EntryID {entry_id}")
                        return entry_id, False

                    deduplicated_results = []
                    seen = set()
                    for res in results:
                        key = (res['EntryID'], res['ImageUrl'])
                        if key not in seen:
                            seen.add(key)
                            deduplicated_results.append(res)
                    logger.info(f"Deduplicated to {len(deduplicated_results)} rows")

                    insert_success = await insert_search_results(deduplicated_results, logger=logger, file_id=str(file_id_db))
                    if not insert_success:
                        logger.error(f"Failed to insert results for EntryID {entry_id}")
                        return entry_id, False

                    update_result = await update_search_sort_order(
                        str(file_id_db), str(entry_id), brand, search_string, color, category, logger, brand_rules=brand_rules
                    )
                    if update_result is None or not update_result:
                        logger.error(f"SortOrder update failed for EntryID {entry_id}")
                        return entry_id, False

                    return entry_id, True
                except Exception as e:
                    logger.error(f"Error processing EntryID {entry_id}: {e}", exc_info=True)
                    return entry_id, False

        for batch_idx, batch_entries in enumerate(entry_batches, 1):
            logger.info(f"Processing batch {batch_idx}/{len(entry_batches)}")
            start_time = datetime.datetime.now()

            results = await asyncio.gather(
                *(process_entry(entry) for entry in batch_entries),
                return_exceptions=True
            )

            for entry, result in zip(batch_entries, results):
                entry_id = entry[0]
                if isinstance(result, Exception):
                    logger.error(f"Error processing EntryID {entry_id}: {result}", exc_info=True)
                    failed_entries += 1
                    continue
                entry_id_result, success = result
                if success:
                    successful_entries += 1
                    last_entry_id_processed = entry_id
                else:
                    failed_entries += 1

            elapsed_time = (datetime.datetime.now() - start_time).total_seconds()
            logger.info(f"Completed batch {batch_idx} in {elapsed_time:.2f}s")
            log_memory_usage()

        async with async_engine.connect() as conn:
            result = await conn.execute(
                text("""
                    SELECT COUNT(DISTINCT t.EntryID), 
                           SUM(CASE WHEN t.SortOrder > 0 THEN 1 ELSE 0 END) AS positive_count,
                           SUM(CASE WHEN t.SortOrder IS NULL THEN 1 ELSE 0 END) AS null_count
                    FROM utb_ImageScraperResult t
                    INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                    WHERE r.FileID = :file_id
                """),
                {"file_id": file_id_db_int}
            )
            row = result.fetchone()
            total_entries = row[0] if row else 0
            positive_entries = row[1] if row and row[1] is not None else 0
            null_entries = row[2] if row and row[2] is not None else 0
            logger.info(
                f"Verification: {total_entries} total entries, "
                f"{positive_entries} with positive SortOrder, {null_entries} with NULL SortOrder"
            )
            if null_entries > 0:
                logger.warning(f"Found {null_entries} entries with NULL SortOrder")
            result.close()

        to_emails = await get_send_to_email(file_id_db, logger=logger)
        if to_emails:
            subject = f"Processing Completed for FileID: {file_id_db}"
            message = (
                f"Processing for FileID {file_id_db} completed.\n"
                f"Successful entries: {successful_entries}/{len(entries)}\n"
                f"Failed entries: {failed_entries}\n"
                f"Last EntryID: {last_entry_id_processed}\n"
                f"Log file: {log_filename}"
            )
            await send_message_email(to_emails, subject=subject, message=message, logger=logger)

        log_public_url = await upload_log_file(str(file_id_db), log_filename, logger)
        return {
            "message": "Search processing completed",
            "file_id": str(file_id_db),
            "successful_entries": str(successful_entries),
            "total_entries": str(len(entries)),
            "failed_entries": str(failed_entries),
            "log_filename": log_filename,
            "log_public_url": log_public_url or "",
            "last_entry_id": str(last_entry_id_processed)
        }
    except Exception as e:
        logger.error(f"Error processing FileID {file_id_db}: {e}", exc_info=True)
        log_public_url = await upload_log_file(str(file_id_db), log_filename, logger)
        return {"error": str(e), "log_filename": log_filename, "log_public_url": log_public_url or "", "last_entry_id": str(entry_id or "")}
    finally:
        await async_engine.dispose()
        engine.dispose()
        logger.info(f"Disposed database engines")

async def generate_download_file(
    file_id: int,
    background_tasks: BackgroundTasks,
    logger: Optional[logging.Logger] = None,
    file_id_param: Optional[int] = None
) -> Dict[str, str]:
    logger, log_filename = setup_job_logger(job_id=str(file_id), log_dir="job_logs", console_output=True)
    process = psutil.Process()
    temp_images_dir, temp_excel_dir = None, None
    successful_entries = 0
    failed_entries = 0
    entries = []

    try:
        file_id = int(file_id)
        async with async_engine.connect() as conn:
            result = await conn.execute(
                text("SELECT FileName FROM utb_ImageScraperFiles WHERE ID = :file_id"),
                {"file_id": file_id}
            )
            row = result.fetchone()
            result.close()
            if not row:
                logger.error(f"No file found for ID {file_id}")
                return {"error": f"No file found for ID {file_id}", "log_filename": log_filename}
            original_filename = row[0]

        logger.info(f"Fetching images for ID: {file_id}")
        selected_images_df = await get_images_excel_db(str(file_id), logger=logger)
        logger.info(f"Fetched DataFrame for ID {file_id}, shape: {selected_images_df.shape}")

        expected_columns = ["ExcelRowID", "ImageUrl", "ImageUrlThumbnail", "Brand", "Style", "Color", "Category"]
        if list(selected_images_df.columns) != expected_columns:
            logger.error(f"Invalid columns in DataFrame for ID {file_id}. Got: {list(selected_images_df.columns)}")
            return {"error": f"Invalid DataFrame columns", "log_filename": log_filename}

        entries = selected_images_df['ExcelRowID'].unique().tolist()
        logger.info(f"Processing {len(entries)} unique entries for FileID {file_id}")

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
            logger.error(f"Template file not found at {local_filename}")
            return {"error": f"Failed to download template file", "log_filename": log_filename}
        logger.debug(f"Template file saved: {local_filename}, size: {os.path.getsize(local_filename)} bytes")

        has_valid_images = not selected_images_df.empty and any(pd.notna(row['ImageUrl']) and row['ImageUrl'] for _, row in selected_images_df.iterrows())
        
        if not has_valid_images:
            logger.warning(f"No valid images found for ID {file_id}")
            with pd.ExcelWriter(local_filename, engine='openpyxl', mode='a') as writer:
                pd.DataFrame({"Message": [f"No valid images found for FileID {file_id}."]}).to_excel(writer, sheet_name="NoImages", index=False)
            failed_entries = len(entries)
        else:
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
            logger.info(f"Selected {len(selected_image_list)} valid images")

            failed_downloads = await download_all_images(selected_image_list, temp_images_dir, logger=logger)
            failed_downloads = [(url, int(row_id)) for url, row_id in failed_downloads]

            header_index = 5
            await write_excel_image(local_filename, temp_images_dir, selected_image_list, "A", header_index, logger)

            successful_entries = len(entries) - len(failed_downloads)
            failed_entries = len(failed_downloads)
            logger.info(f"Processed {successful_entries} successful entries, {failed_entries} failed entries")

            if failed_downloads:
                logger.info(f"Writing {len(failed_downloads)} failed downloads to Excel")
                await write_failed_downloads_to_excel(failed_downloads, local_filename, logger=logger)

        if not os.path.exists(local_filename):
            logger.error(f"Excel file not found at {local_filename}")
            return {"error": f"Excel file not found", "log_filename": log_filename}
        logger.debug(f"Excel file exists: {local_filename}, size: {os.path.getsize(local_filename)} bytes")

        logger.debug(f"Uploading Excel file to R2: {processed_file_name}")
        public_url = await upload_file_to_space(
            file_src=local_filename,
            save_as=processed_file_name,
            is_public=True,
            logger=logger,
            file_id=file_id
        )

        if not public_url:
            logger.error(f"Upload failed for ID {file_id}")
            return {"error": "Failed to upload processed file", "log_filename": log_filename}
        logger.info(f"File uploaded to R2, public_url: {public_url}")

        await update_file_location_complete(str(file_id), public_url, logger=logger)
        await update_file_generate_complete(str(file_id), logger=logger)

        log_public_url = await upload_log_file(str(file_id), log_filename, logger)

        send_to_email_addr = await get_send_to_email(file_id, logger=logger)
        if not send_to_email_addr:
            logger.error(f"No email address for ID {file_id}")
            return {"error": "Failed to retrieve email address", "log_filename": log_filename}

        subject_line = f"{original_filename} Job Notification{' - No Images' if not has_valid_images else ''}"
        user_message = (
            f"Excel file generation for FileID {file_id} {'completed successfully' if has_valid_images else 'completed with no valid images'}.\n"
            f"Download Excel file: {public_url}"
        )
        logger.debug(f"Scheduling user email to {send_to_email_addr} with public_url: {public_url}, message: {user_message}")
        background_tasks.add_task(
            send_message_email,
            to_emails=send_to_email_addr,
            subject=subject_line,
            message=user_message,
            logger=logger
        )

        admin_email = "nik@luxurymarket.com"
        admin_subject = f"Admin Log: Job Completed for FileID {file_id}"
        admin_message = (
            f"Processing for FileID {file_id} completed.\n"
            f"Successful entries: {successful_entries}/{len(entries)}\n"
            f"Failed entries: {failed_entries}\n"
            f"Last EntryID: {max(entries) if entries else 0}\n"
            f"Log file: {log_filename}"
        )
        logger.debug(f"Scheduling admin email to {admin_email} with message: {admin_message}")
        background_tasks.add_task(
            send_message_email,
            to_emails=[admin_email],
            subject=admin_subject,
            message=admin_message,
            logger=logger
        )

        logger.info(f"Completed ID {file_id}")
        return {
            "message": "Processing completed successfully" if has_valid_images else "No valid images found, empty Excel file generated",
            "public_url": public_url,
            "log_filename": log_filename
        }
    except Exception as e:
        logger.error(f"Error for ID {file_id}: {e}", exc_info=True)
        log_public_url = await upload_log_file(str(file_id), log_filename, logger)
        send_to_email_addr = await get_send_to_email(file_id, logger=logger)
        if send_to_email_addr:
            error_message = f"Excel file generation for FileID {file_id} failed.\nError: {str(e)}\nLog file: {log_filename}"
            logger.debug(f"Scheduling error email to {send_to_email_addr} with message: {error_message}")
            background_tasks.add_task(
                send_message_email,
                to_emails=send_to_email_addr,
                subject=f"Error: Job Failed for FileID {file_id}",
                message=error_message,
                logger=logger
            )
        return {"error": f"An error occurred: {str(e)}", "log_filename": log_filename}
    finally:
        if temp_images_dir and temp_excel_dir:
            await cleanup_temp_dirs([temp_images_dir, temp_excel_dir], logger=logger)
        await async_engine.dispose()
        engine.dispose()
        logger.info(f"Cleaned up resources for ID {file_id}")