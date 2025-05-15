import threading
from concurrent.futures import ThreadPoolExecutor
import logging
import asyncio
import os
import pandas as pd
import time
import pyodbc
import httpx
import datetime
from typing import Optional, Dict, List, Tuple
from config import conn_str
from db_utils import sync_get_endpoint, insert_search_results, update_search_sort_order, get_send_to_email
from common import fetch_brand_rules
from utils import sync_process_and_tag_results
from logging_config import setup_job_logger
import psutil
from email_utils import send_message_email

BRAND_RULES_URL = os.getenv("BRAND_RULES_URL", "https://raw.githubusercontent.com/iconluxurygroup/legacy-icon-product-api/refs/heads/main/task_settings/brand_settings.json")

def process_entry(args):
    """Wrapper for sync_process_and_tag_results to run in a thread."""
    search_string, brand, endpoint, entry_id, use_all_variations, file_id_db = args
    logger = logging.getLogger(f"worker_{entry_id}")
    logger.setLevel(logging.DEBUG)
    # Ensure thread-safe logging
    handler = logging.FileHandler(f"job_logs/worker_{entry_id}.log")
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    try:
        mem_info = psutil.Process().memory_info()
        logger.debug(f"Worker {entry_id} memory: RSS={mem_info.rss / 1024 / 1024:.2f} MB")
        result = sync_process_and_tag_results(
            search_string=search_string,
            brand=brand,
            model=search_string,
            endpoint=endpoint,
            entry_id=entry_id,
            logger=logger,
            use_all_variations=use_all_variations,
            file_id_db=file_id_db
        )
        logger.debug(f"Result for EntryID {entry_id}: {result}")
        return result
    except Exception as e:
        logger.error(f"Task failed for EntryID {entry_id}: {e}", exc_info=True)
        return None
    finally:
        logger.removeHandler(handler)
        handler.close()

async def process_restart_batch(
    file_id_db: int,
    entry_id: Optional[int] = None,
    use_all_variations: bool = False
) -> Dict[str, str]:
    """Process a batch of entries for a file using threading."""
    log_filename = f"job_logs/job_{file_id_db}.log"
    try:
        # Initialize logger
        logger, log_filename = setup_job_logger(job_id=str(file_id_db), log_dir="job_logs", console_output=True)
        logger.setLevel(logging.DEBUG)
        logger.debug("Logger initialized")

        def log_memory_usage():
            try:
                process = psutil.Process()
                mem_info = process.memory_info()
                logger.info(f"Memory usage: RSS={mem_info.rss / 1024 / 1024:.2f} MB")
            except Exception as e:
                logger.error(f"Memory logging failed: {e}")

        logger.debug(f"Input file_id_db: {file_id_db}, entry_id: {entry_id}, use_all_variations: {use_all_variations}")
        logger.info(f"🔁 Starting processing for FileID: {file_id_db}")
        log_memory_usage()

        file_id_db_int = file_id_db
        BATCH_SIZE = 1

        # Validate FileID
        logger.debug("Validating FileID...")
        with pyodbc.connect(conn_str, autocommit=False, timeout=30) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM utb_ImageScraperFiles WHERE ID = ?", (file_id_db_int,))
            if cursor.fetchone()[0] == 0:
                logger.error(f"FileID {file_id_db} does not exist")
                return {"error": f"FileID {file_id_db} does not exist", "log_filename": log_filename}

        # Fetch brand rules
        logger.debug("Fetching brand rules...")
        brand_rules = await fetch_brand_rules(BRAND_RULES_URL, max_attempts=3, timeout=10, logger=logger)
        if not brand_rules:
            logger.warning("No brand rules fetched")
            return {"message": "Failed to fetch brand rules", "file_id": str(file_id_db), "log_filename": log_filename}

        # Fetch endpoint
        logger.debug("Fetching endpoint...")
        endpoint = None
        for attempt in range(5):
            try:
                endpoint = sync_get_endpoint(logger=logger)
                if endpoint:
                    logger.info(f"Selected endpoint: {endpoint}")
                    break
                logger.warning(f"Attempt {attempt + 1} failed")
                time.sleep(2)
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                time.sleep(2)
        if not endpoint:
            logger.error("No healthy endpoint")
            return {"error": "No healthy endpoint", "log_filename": log_filename}

        # Fetch entries
        logger.debug("Fetching entries...")
        with pyodbc.connect(conn_str, autocommit=False, timeout=30) as conn:
            cursor = conn.cursor()
            try:
                if entry_id:
                    cursor.execute(
                        "SELECT EntryID, ProductModel, ProductBrand, ProductColor, ProductCategory FROM utb_ImageScraperRecords WHERE FileID = ? AND EntryID = ?",
                        (file_id_db_int, entry_id)
                    )
                else:
                    cursor.execute(
                        "SELECT EntryID, ProductModel, ProductBrand, ProductColor, ProductCategory FROM utb_ImageScraperRecords WHERE FileID = ?",
                        (file_id_db_int,)
                    )
                entries = [(row[0], row[1], row[2], row[3], row[4]) for row in cursor.fetchall() if row[1] is not None]
                logger.info(f"Found {len(entries)} entries")
            except pyodbc.Error as e:
                logger.error(f"Database query failed: {e}", exc_info=True)
                return {"error": f"Database query failed: {e}", "log_filename": log_filename}

        if not entries:
            logger.warning("No entries found")
            return {"error": "No entries found", "log_filename": log_filename}

        # Create batches
        entry_batches = [entries[i:i + BATCH_SIZE] for i in range(0, len(entries), BATCH_SIZE)]
        logger.info(f"Created {len(entry_batches)} batches")

        successful_entries = 0
        failed_entries = 0
        api_to_db_mapping = {
            'image_url': 'ImageUrl', 'thumbnail_url': 'ImageUrlThumbnail', 'url': 'ImageUrl',
            'thumb': 'ImageUrlThumbnail', 'image': 'ImageUrl', 'thumbnail': 'ImageUrlThumbnail'
        }
        required_columns = ["EntryID", "ImageUrl", "ImageDesc", "ImageSource", "ImageUrlThumbnail"]

        # Use ThreadPoolExecutor instead of multiprocessing.Pool
        with ThreadPoolExecutor(max_workers=4) as executor:
            for batch_idx, batch_entries in enumerate(entry_batches, 1):
                logger.info(f"Processing batch {batch_idx}/{len(entry_batches)}")
                start_time = datetime.datetime.now()

                tasks = [
                    (search_string, brand, endpoint, entry_id, use_all_variations, file_id_db_int)
                    for entry_id, search_string, brand, color, category in batch_entries
                ]
                logger.debug(f"Tasks: {tasks}")

                # Submit tasks to ThreadPoolExecutor
                results = list(executor.map(process_entry, tasks))
                logger.debug(f"Batch {batch_idx} results: {results}")

                for (entry_id, search_string, brand, color, category), result in zip(batch_entries, results):
                    try:
                        if result is None:
                            logger.error(f"No results for EntryID {entry_id}")
                            failed_entries += 1
                            continue

                        dfs = result
                        if not dfs:
                            logger.error(f"Empty results for EntryID {entry_id}")
                            failed_entries += 1
                            continue

                        combined_df = pd.concat(dfs, ignore_index=True)
                        logger.debug(f"Combined DataFrame for EntryID {entry_id}: {combined_df.to_dict()}")

                        for api_col, db_col in api_to_db_mapping.items():
                            if api_col in combined_df.columns and db_col not in combined_df.columns:
                                combined_df.rename(columns={api_col: db_col}, inplace=True)

                        if not all(col in combined_df.columns for col in required_columns):
                            logger.error(f"Missing columns {set(required_columns) - set(combined_df.columns)} for EntryID {entry_id}")
                            failed_entries += 1
                            continue

                        deduplicated_df = combined_df.drop_duplicates(subset=['EntryID', 'ImageUrl'], keep='first')
                        logger.info(f"Deduplicated to {len(deduplicated_df)} rows for EntryID {entry_id}")

                        # Ensure thread-safe database operations
                        insert_success = insert_search_results(deduplicated_df, logger=logger)
                        if not insert_success:
                            logger.error(f"Failed to insert results for EntryID {entry_id}")
                            failed_entries += 1
                            continue

                        logger.info(f"Inserted {len(deduplicated_df)} results for EntryID {entry_id}")

                        update_result = update_search_sort_order(
                            str(file_id_db_int), str(entry_id), brand, search_string, color, category, logger, brand_rules=brand_rules
                        )
                        if update_result is None:
                            logger.error(f"SortOrder update failed for EntryID {entry_id}")
                            failed_entries += 1
                            continue

                        logger.info(f"Updated sort order for EntryID {entry_id}")
                        successful_entries += 1

                    except Exception as e:
                        logger.error(f"Error processing EntryID {entry_id}: {e}", exc_info=True)
                        failed_entries += 1

                elapsed_time = (datetime.datetime.now() - start_time).total_seconds()
                logger.info(f"Completed batch {batch_idx} in {elapsed_time:.2f} seconds")
                log_memory_usage()

        # Final verification
        with pyodbc.connect(conn_str, autocommit=False) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT COUNT(DISTINCT t.EntryID)
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                WHERE r.FileID = ? AND t.SortOrder > 0
                """,
                (file_id_db_int,)
            )
            positive_entries = cursor.fetchone()[0]
            cursor.execute(
                """
                SELECT COUNT(DISTINCT t.EntryID)
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                WHERE r.FileID = ? AND t.SortOrder IS NULL
                """,
                (file_id_db_int,)
            )
            null_entries = cursor.fetchone()[0]
            logger.info(f"Final verification: Found {positive_entries} entries with positive SortOrder, {null_entries} entries with NULL SortOrder")

        logger.info(f"Completed processing. Successful: {successful_entries}, Failed: {failed_entries}")
        log_memory_usage()

        to_emails = get_send_to_email(file_id_db_int, logger=logger)
        if to_emails:
            subject = f"Processing Completed for FileID: {file_id_db}"
            message = (
                f"Processing for FileID {file_id_db} has completed successfully.\n"
                f"Successful entries: {successful_entries}/{len(entries)}\n"
                f"Failed entries: {failed_entries}\n"
                f"Log file: {log_filename}"
            )
            send_message_email(to_emails=to_emails, subject=subject, message=message, logger=logger)

        return {
            "message": "Search processing completed",
            "file_id": str(file_id_db),
            "successful_entries": str(successful_entries),
            "total_entries": str(len(entries)),
            "failed_entries": str(failed_entries),
            "log_filename": log_filename
        }

    except Exception as e:
        logger.error(f"Error processing FileID {file_id_db}: {e}", exc_info=True)
        return {"error": str(e), "log_filename": log_filename}


















async def generate_download_file(
    file_id: int,
    logger: Optional[logging.Logger] = None,
    file_id_param: Optional[int] = None
) -> Dict[str, str]:
    """Generate and upload a processed Excel file with images asynchronously."""
    logger = logger or default_logger
    temp_images_dir, temp_excel_dir = None, None

    try:
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
        selected_images_df = await get_images_excel_db(str(file_id), logger=logger)
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
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = base_name[-8:] if len(base_name) >= 8 else base_name
        processed_file_name = f"super_scraper/jobs/{file_id}/{base_name}_scraper_{timestamp}_{unique_id}{extension}"
        logger.info(f"🆔 Generated filename: {processed_file_name}")

        temp_images_dir, temp_excel_dir = await create_temp_dirs(file_id, logger=logger)
        local_filename = os.path.join(temp_excel_dir, original_filename)

        # Download images with thumbnail fallback
        failed_img_urls = await download_all_images(selected_image_list, temp_images_dir, logger=logger)
        logger.info(f"📥 Downloaded images, {len(failed_img_urls)} failed")

        logger.info(f"📥 Downloading template file from {template_file_path}")
        async with httpx.AsyncClient() as client:
            response = await client.get(template_file_path, timeout=httpx.Timeout(30, connect=10))
            response.raise_for_status()
            async with aiofiles.open(local_filename, 'wb') as f:
                await f.write(response.content)

        row_offset = header_index
        logger.info(f"🖼️ Writing images with row_offset={row_offset}")
        failed_rows = await write_excel_image(
            local_filename, temp_images_dir, selected_image_list, "A", row_offset, logger
        )

        # Write failed downloads to Excel if any
        if failed_img_urls:
            logger.info(f"📝 Writing {len(failed_img_urls)} failed downloads to Excel")
            # failed_img_urls is already a list of (url, row_id) tuples
            success = await write_failed_downloads_to_excel(failed_img_urls, local_filename, logger=logger)
            if not success:
                logger.warning(f"⚠️ Failed to write some failed downloads to Excel")

        public_url = await upload_file_to_space(
            local_filename, processed_file_name, True, logger, file_id
        )
        if not public_url:
            logger.error(f"❌ Upload failed for FileID {file_id}")
            return {"error": "Failed to upload processed file"}

        await update_file_location_complete(str(file_id), public_url, logger=logger)
        await update_file_generate_complete(str(file_id), logger=logger)

        send_to_email_addr = await get_send_to_email(file_id, logger=logger)
        if not send_to_email_addr:
            logger.error(f"❌ No email address for FileID {file_id}")
            return {"error": "Failed to retrieve email address"}
        subject_line = f"{original_filename} Job Notification"
        await send_email(
            to_emails=send_to_email_addr,
            subject=subject_line,
            download_url=public_url,
            job_id=file_id,
            logger=logger
        )

        logger.info(f"🏁 Completed FileID {file_id}")
        return {"message": "Processing completed successfully", "public_url": public_url}

    except Exception as e:
        logger.error(f"🔴 Error for FileID {file_id}: {e}", exc_info=True)
        return {"error": f"An error occurred: {str(e)}"}
    finally:
        if temp_images_dir and temp_excel_dir:
            await cleanup_temp_dirs([temp_images_dir, temp_excel_dir], logger=logger)
        logger.info(f"🧹 Cleaned up temporary directories for FileID {file_id}")

async def batch_vision_reason(
    file_id: str,
    entry_ids: Optional[List[int]] = None,
    step: int = 0,
    limit: int = 5000,
    concurrency: int = 10,
    logger: Optional[logging.Logger] = None
) -> None:
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        logger.info(f"📷 Starting batch image processing for FileID: {file_id}, Step: {step}, Limit: {limit}")

        df = await fetch_missing_images(file_id, limit, True, logger)
        if df.empty:
            logger.warning(f"⚠️ No missing images found for FileID: {file_id}")
            return

        if entry_ids is not None:
            df = df[df['EntryID'].isin(entry_ids)]
            if df.empty:
                logger.warning(f"⚠️ No missing images found for specified EntryIDs: {entry_ids}")
                return

        columns_to_drop = ['Step1', 'Step2', 'Step3', 'Step4', 'CreateTime_1', 'CreateTime_2']
        df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
        logger.info(f"Retrieved {len(df)} image rows for FileID: {file_id}")
        entry_ids_to_process = list(df.groupby('EntryID').groups.keys())

        semaphore = asyncio.Semaphore(concurrency)  # Limit to `concurrency` tasks
        futures = []

        async def submit_task(entry_id, entry_df):
            async with semaphore:
                logger.info(f"Submitting Ray task for EntryID: {entry_id}")
                future = process_entry_remote.remote(file_id, entry_id, entry_df, logger)
                futures.append(future)

        tasks = [submit_task(entry_id, df[df['EntryID'] == entry_id]) for entry_id in entry_ids_to_process]
        await asyncio.gather(*tasks)

        results = ray.get(futures)
        valid_updates = []
        for updates in results:
            valid_updates.extend(updates)

        if valid_updates:
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                query = "UPDATE utb_ImageScraperResult SET AiJson = ?, ImageIsFashion = ?, AiCaption = ? WHERE ResultID = ?"
                cursor.executemany(query, valid_updates)
                conn.commit()
                logger.info(f"Updated {len(valid_updates)} records")

        for entry_id in entry_ids_to_process:
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT ProductBrand, ProductModel, ProductColor, ProductCategory
                    FROM utb_ImageScraperRecords
                    WHERE FileID = ? AND EntryID = ?
                """, (file_id, entry_id))
                result = cursor.fetchone()
                if result:
                    product_brand, product_model, product_color, product_category = result
                else:
                    product_brand = product_model = product_color = product_category = ''
            
            await asyncio.get_event_loop().run_in_executor(
                None, lambda: sync_update_search_sort_order(
                    file_id=str(file_id),
                    entry_id=str(entry_id),
                    brand=product_brand,
                    model=product_model,
                    color=product_color,
                    category=product_category,
                    logger=logger
                )
            )
            logger.info(f"Updated sort order for FileID: {file_id}, EntryID: {entry_id}")

    except Exception as e:
        logger.error(f"🔴 Error in batch_vision_reason for FileID {file_id}: {e}", exc_info=True)
        raise