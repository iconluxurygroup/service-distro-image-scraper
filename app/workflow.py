# workflow.py
# Contains workflow functions for batch processing, file generation, and retries

import logging
import asyncio
import os
import pandas as pd
import time
import pyodbc
import ray
from typing import List, Dict, Optional, Tuple
from config import conn_str
from database import (
    insert_search_results,
    update_search_sort_order,
    get_records_to_search,
    fetch_missing_images,
    update_initial_sort_order,
    update_log_url_in_db,
    get_endpoint,
    sync_update_search_sort_order,
    get_images_excel_db,
    get_send_to_email,
    update_file_generate_complete,
    update_file_location_complete,
    set_sort_order_negative_four_for_zero_match,
)
from utils import (
    fetch_brand_rules,
    sync_process_and_tag_results,
    generate_search_variations,
    process_search_row,
    process_and_tag_results,
    process_search_row_gcloud,
)
from image_reason import process_entry_remote
from image_utils import download_all_images
from excel_utils import write_excel_image
from email_utils import send_email, send_message_email
from file_utils import create_temp_dirs, cleanup_temp_dirs
from aws_s3 import upload_file_to_space
import httpx
import aiofiles
import datetime
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

BRAND_RULES_URL = "https://raw.githubusercontent.com/iconluxurygroup/legacy-icon-product-api/refs/heads/main/task_settings/brand_settings.json"

def run_process_restart_batch(*args, **kwargs):
    """Wrapper for process_restart_batch to match expected API."""
    return process_restart_batch.remote(*args, **kwargs)

@ray.remote(max_retries=3)
def process_restart_batch(
    file_id_db: int,
    max_retries: int = 7,
    logger: Optional[logging.Logger] = None,
    entry_id: Optional[int] = None,
    use_all_variations: bool = False
) -> Dict[str, str]:
    """Process a batch of entries for a file using Ray, handling retries and logging."""
    logger = logger or logging.getLogger(f"job_{file_id_db}")
    log_dir = os.path.join(os.getcwd(), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_filename = logger.handlers[0].baseFilename if logger.handlers else os.path.join(log_dir, f"file_{file_id_db}.log")
    BATCH_SIZE = 10

    try:
        logger.info(f"🔁 Starting concurrent search processing for FileID: {file_id_db}" + (f", EntryID: {entry_id}" if entry_id else "") + f", use_all_variations: {use_all_variations}")
        file_id_db = int(file_id_db)

        # Fetch brand rules synchronously
        brand_rules = asyncio.run(fetch_brand_rules(BRAND_RULES_URL, max_attempts=3, timeout=10, logger=logger))
        if not brand_rules:
            logger.warning(f"No brand rules fetched for FileID: {file_id_db}")
            brand_rules = {"brand_rules": []}
            return {
                "message": "Failed to fetch brand rules",
                "file_id": str(file_id_db),
                "successful_entries": "0",
                "total_entries": "0",
                "failed_entries": "0",
                "log_filename": log_filename
            }

        # Fetch endpoint with retries
        max_endpoint_retries = 3
        endpoint = None
        for attempt in range(max_endpoint_retries):
            try:
                endpoint = asyncio.run(get_endpoint(logger=logger))
                if endpoint:
                    logger.info(f"Selected healthy endpoint: {endpoint}")
                    break
                logger.warning(f"Attempt {attempt + 1} failed to find healthy endpoint")
                time.sleep(2)
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed to get endpoint: {e}")
                time.sleep(2)
        if not endpoint:
            logger.error(f"No healthy endpoint available for FileID: {file_id_db} after {max_endpoint_retries} attempts")
            return {"error": f"No healthy endpoint available for FileID: {file_id_db}", "log_filename": log_filename}

        # Fetch entries
        with pyodbc.connect(conn_str, autocommit=False) as conn:
            cursor = conn.cursor()
            if entry_id:
                cursor.execute(
                    "SELECT EntryID, ProductModel, ProductBrand, ProductColor, ProductCategory FROM utb_ImageScraperRecords WHERE FileID = ? AND EntryID = ?",
                    (file_id_db, entry_id)
                )
            else:
                cursor.execute(
                    "SELECT EntryID, ProductModel, ProductBrand, ProductColor, ProductCategory FROM utb_ImageScraperRecords WHERE FileID = ?",
                    (file_id_db,)
                )
            entries = [(row[0], row[1], row[2], row[3], row[4]) for row in cursor.fetchall() if row[1] is not None]
            logger.info(f"📋 Found {len(entries)} valid entries for FileID: {file_id_db}" + (f", EntryID: {entry_id}" if entry_id else ""))

        if not entries:
            logger.warning(f"⚠️ No entries found for FileID {file_id_db}" + (f", EntryID: {entry_id}" if entry_id else ""))
            return {"error": f"No entries found for FileID: {file_id_db}", "log_filename": log_filename}

        # Process entries in batches
        entry_batches = [entries[i:i + BATCH_SIZE] for i in range(0, len(entries), BATCH_SIZE)]
        logger.info(f"Created {len(entry_batches)} batches for processing")

        successful_entries = 0
        failed_entries = 0
        api_to_db_mapping = {
            'image_url': 'ImageUrl', 'thumbnail_url': 'ImageUrlThumbnail', 'url': 'ImageUrl',
            'thumb': 'ImageUrlThumbnail', 'image': 'ImageUrl', 'thumbnail': 'ImageUrlThumbnail',
            'img_url': 'ImageUrl', 'thumb_url': 'ImageUrlThumbnail', 'imageURL': 'ImageUrl',
            'imageUrl': 'ImageUrl', 'thumbnailURL': 'ImageUrlThumbnail', 'thumbnailUrl': 'ImageUrlThumbnail',
            'brand': 'Brand', 'model': 'Model', 'brand_name': 'Brand', 'product_model': 'Model'
        }
        required_columns = ["EntryID", "ImageUrl", "ImageDesc", "ImageSource", "ImageUrlThumbnail"]

        for batch_idx, batch_entries in enumerate(entry_batches, 1):
            logger.info(f"Processing batch {batch_idx}/{len(entry_batches)} with {len(batch_entries)} entries")
            batch_results = []

            # Create a list of Ray tasks for concurrent execution
            tasks = [
                ray.remote(sync_process_and_tag_results).remote(
                    search_string=search_string,
                    brand=brand,
                    model=search_string,
                    endpoint=endpoint,
                    entry_id=entry_id,
                    logger=logger,
                    use_all_variations=use_all_variations,
                    file_id_db=file_id_db
                )
                for entry_id, search_string, brand, color, category in batch_entries
            ]

            # Wait for up to 10 tasks to complete
            try:
                results = ray.get(tasks)
            except Exception as e:
                logger.error(f"Error in batch {batch_idx}: {e}", exc_info=True)
                failed_entries += len(batch_entries)
                continue

            # Process results
            for (entry_id, search_string, brand, color, category), result in zip(batch_entries, results):
                try:
                    if isinstance(result, Exception):
                        logger.error(f"Error processing EntryID {entry_id}: {result}", exc_info=True)
                        failed_entries += 1
                        batch_results.append(False)
                        continue

                    dfs = result
                    if dfs:
                        # Combine and deduplicate results
                        combined_df = pd.concat(dfs, ignore_index=True)
                        logger.info(f"Combined {len(combined_df)} results for EntryID {entry_id}")

                        # Rename columns
                        for api_col, db_col in api_to_db_mapping.items():
                            if api_col in combined_df.columns and db_col not in combined_df.columns:
                                combined_df.rename(columns={api_col: db_col}, inplace=True)

                        # Verify required columns
                        if not all(col in combined_df.columns for col in required_columns):
                            logger.error(f"Missing columns {set(required_columns) - set(combined_df.columns)} for EntryID {entry_id}")
                            failed_entries += 1
                            batch_results.append(False)
                            continue

                        # Deduplicate
                        deduplicated_df = combined_df.drop_duplicates(subset=['EntryID', 'ImageUrl'], keep='first')
                        logger.info(f"Deduplicated to {len(deduplicated_df)} rows for EntryID {entry_id}")

                        # Insert into database
                        insert_success = insert_search_results(deduplicated_df, logger=logger)
                        if not insert_success:
                            logger.error(f"Failed to insert results for EntryID {entry_id}")
                            failed_entries += 1
                            batch_results.append(False)
                            continue

                        logger.info(f"Inserted {len(deduplicated_df)} results for EntryID {entry_id}")

                        # Update sort order
                        update_result = asyncio.run(update_search_sort_order(
                            str(file_id_db), str(entry_id), brand, search_string, color, category, logger, brand_rules=brand_rules
                        ))
                        if update_result is None:
                            logger.error(f"SortOrder update failed for EntryID {entry_id}")
                            failed_entries += 1
                            batch_results.append(False)
                            continue

                        logger.info(f"Updated sort order for EntryID {entry_id}")
                        successful_entries += 1
                        batch_results.append(True)

                        # Verify database insertion
                        with pyodbc.connect(conn_str, autocommit=False) as conn:
                            cursor = conn.cursor()
                            cursor.execute("SELECT COUNT(*) FROM utb_ImageScraperResult WHERE EntryID = ?", (entry_id,))
                            total_count = cursor.fetchone()[0]
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
                            logger.info(f"Verification: Found {total_count} total rows, {count} with positive SortOrder, {null_count} with NULL SortOrder for EntryID {entry_id}")
                            if null_count > 0:
                                logger.error(f"Found {null_count} rows with NULL SortOrder for EntryID {entry_id}")
                            if total_count == 0:
                                logger.error(f"No rows found for EntryID {entry_id} after insertion")

                    else:
                        logger.error(f"No results returned for EntryID {entry_id}")
                        failed_entries += 1
                        batch_results.append(False)

                except Exception as e:
                    logger.error(f"Error processing EntryID {entry_id}: {e}", exc_info=True)
                    failed_entries += 1
                    batch_results.append(False)

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

        logger.info(f"✅ Completed processing for FileID: {file_id_db}. {positive_entries}/{len(entries)} entries with positive SortOrder. Failed entries: {failed_entries}")
        return {
            "message": "Search processing completed",
            "file_id": str(file_id_db),
            "successful_entries": str(successful_entries),
            "total_entries": str(len(entries)),
            "failed_entries": str(failed_entries),
            "log_filename": log_filename
        }

    except Exception as e:
        logger.error(f"🔴 Error processing FileID {file_id_db}: {e}", exc_info=True)
        to_emails = asyncio.run(get_send_to_email(file_id_db, logger))
        if to_emails:
            send_message_email(
                to_emails=to_emails,
                subject=f"Error processing FileID: {file_id_db}",
                message=f"An error occurred while processing your file: {str(e)}",
                logger=to_emails
            )
        return {
            "error": str(e),
            "log_filename": log_filename
        }

async def generate_download_file(
    file_id: int,
    logger: Optional[logging.Logger] = None,
    file_id_param: Optional[int] = None
) -> Dict[str, str]:
    """Generate and upload a processed Excel file with images."""
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

        failed_img_urls = await download_all_images(selected_image_list, temp_images_dir, logger=logger)
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

        semaphore = asyncio.Semaphore(concurrency)
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
        logger.error(f"🔴 Error in batch_process_images for FileID {file_id}: {e}", exc_info=True)
        raise