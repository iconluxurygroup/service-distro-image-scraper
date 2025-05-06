import logging
import asyncio
import os
import pandas as pd
import pyodbc
import ray
from typing import List, Dict, Optional, Tuple
from config import conn_str
from database import (
    insert_search_results,
    update_search_sort_order,
    get_records_to_search,
    update_initial_sort_order,
    update_log_url_in_db,
    get_endpoint,
    get_images_excel_db,
    get_send_to_email,
    update_file_generate_complete,
    update_file_location_complete,
    set_sort_order_negative_four_for_zero_match,
)
from utils import (
    fetch_brand_rules,
    generate_search_variations,
    process_search_row,
    process_search_row_gcloud,
    get_healthy_endpoint,
)
from image_utils import download_all_images
from excel_utils import write_excel_image
from email_utils import send_email, send_message_email
from file_utils import create_temp_dirs, cleanup_temp_dirs
from aws_s3 import upload_file_to_space
import httpx
import aiofiles


default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

BRAND_RULES_URL = "https://raw.githubusercontent.com/iconluxurygroup/legacy-icon-product-api/refs/heads/main/task_settings/brand_settings.json"

@ray.remote(max_retries=3)
def search_variation(
    variation: str,
    endpoint: str,
    entry_id: int,
    search_type: str,
    brand: Optional[str] = None,
    category: Optional[str] = None,
    logger: Optional[logging.Logger] = None
) -> Dict:
    """Process a single search variation using Ray."""
    logger = logger or default_logger
    try:
        regions = ['northamerica-northeast', 'us-east', 'southamerica', 'us-central', 'us-west', 'europe', 'australia']
        max_attempts = 5
        total_attempts = [0]

        def log_retry_status(attempt_type: str, attempt_num: int) -> bool:
            total_attempts[0] += 1
            if total_attempts[0] > max_attempts:
                logger.error(f"Exceeded max retries ({max_attempts}) for EntryID {entry_id} after {attempt_type} attempt {attempt_num}")
                return False
            logger.info(f"{attempt_type} attempt {attempt_num} (Total attempts: {total_attempts[0]}/{max_attempts}) for EntryID {entry_id}")
            return True

        # Try GCloud first to avoid Google rate limits
        for region in regions:
            if not log_retry_status("GCloud", total_attempts[0] + 1):
                break
            result = process_search_row_gcloud(variation, entry_id, logger, remaining_retries=5, total_attempts=total_attempts)
            if not result.empty:
                logger.info(f"GCloud attempt succeeded for EntryID {entry_id} with {len(result)} images in region {region}")
                return {"variation": variation, "result": result, "status": "success", "result_count": len(result)}
            logger.warning(f"GCloud attempt failed in region {region}")

        # Fall back to primary endpoint
        for attempt in range(3):
            if not log_retry_status("Primary", attempt + 1):
                break
            result = process_search_row(variation, endpoint, entry_id, logger, search_type, max_retries=15, brand=brand, category=category)
            if not result.empty:
                logger.info(f"Primary attempt succeeded for EntryID {entry_id} with {len(result)} images")
                return {"variation": variation, "result": result, "status": "success", "result_count": len(result)}
            logger.warning(f"Primary attempt {attempt + 1} failed")

        logger.error(f"All attempts failed for EntryID {entry_id} after {total_attempts[0]} total attempts")
        return {
            "variation": variation,
            "result": pd.DataFrame([{
                "EntryID": entry_id,
                "ImageUrl": "placeholder://no-results",
                "ImageDesc": f"No results found for {variation}",
                "ImageSource": "N/A",
                "ImageUrlThumbnail": "placeholder://no-results"
            }]),
            "status": "failed",
            "result_count": 1,
            "error": "All search attempts failed"
        }
    except Exception as e:
        logger.error(f"Error searching variation '{variation}' for EntryID {entry_id}: {e}", exc_info=True)
        return {
            "variation": variation,
            "result": pd.DataFrame([{
                "EntryID": entry_id,
                "ImageUrl": "placeholder://no-results",
                "ImageDesc": f"Error for {variation}: {str(e)}",
                "ImageSource": "N/A",
                "ImageUrlThumbnail": "placeholder://no-results"
            }]),
            "status": "failed",
            "result_count": 1,
            "error": str(e)
        }
@ray.remote(max_retries=3)
def process_single_row(
    entry_id: int,
    search_string: str,
    max_row_retries: int,
    file_id_db: int,
    brand_rules: dict,
    endpoint: str,
    brand: Optional[str] = None,
    model: Optional[str] = None,
    color: Optional[str] = None,
    category: Optional[str] = None,
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

    search_types = [
        "default", "delimiter_variations", "brand_delimiter", "color_delimiter",
        "brand_alias", "brand_name", "no_color", "brand_settings", "retry_with_alternative"
    ]
    all_results = []
    result_brand = brand
    result_model = model
    result_color = color
    result_category = category

    api_to_db_mapping = {
        'image_url': 'ImageUrl', 'thumbnail_url': 'ImageUrlThumbnail', 'url': 'ImageUrl',
        'thumb': 'ImageUrlThumbnail', 'image': 'ImageUrl', 'thumbnail': 'ImageUrlThumbnail',
        'img_url': 'ImageUrl', 'thumb_url': 'ImageUrlThumbnail', 'imageURL': 'ImageUrl',
        'imageUrl': 'ImageUrl', 'thumbnailURL': 'ImageUrlThumbnail', 'thumbnailUrl': 'ImageUrlThumbnail',
        'brand': 'Brand', 'model': 'Model', 'brand_name': 'Brand', 'product_model': 'Model'
    }
    required_columns = ["EntryID", "ImageUrl", "ImageDesc", "ImageSource", "ImageUrlThumbnail"]

    if not brand or not model or not color or not category:
        try:
            with pyodbc.connect(conn_str, autocommit=False) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT ProductBrand, ProductModel, ProductColor, ProductCategory FROM utb_ImageScraperRecords WHERE FileID = ? AND EntryID = ?",
                    (file_id_db, entry_id)
                )
                result = cursor.fetchone()
                if result:
                    result_brand = result_brand or result[0]
                    result_model = result_model or result[1]
                    result_color = result_color or result[2]
                    result_category = result_category or result[3]
                    logger.info(f"Fetched attributes for EntryID {entry_id}: Brand={result_brand}, Model={result_model}, Color={result_color}, Category={result_category}")
                else:
                    logger.warning(f"No attributes found for FileID {file_id_db}, EntryID {entry_id}")
        except pyodbc.Error as e:
            logger.error(f"Failed to fetch attributes for EntryID {entry_id}: {e}", exc_info=True)

    variations = generate_search_variations(search_string, result_brand, result_model, brand_rules, logger)
    loop = asyncio.get_event_loop()
    endpoint = loop.run_until_complete(get_endpoint(logger=logger))
    if not endpoint:
        logger.error(f"No healthy endpoint available for EntryID {entry_id}")
        return False
    if not endpoint:
        logger.error(f"No healthy endpoint available for EntryID {entry_id}")
        return False

    for search_type in search_types:
        if search_type not in variations:
            continue
        logger.info(f"Processing search type '{search_type}' for EntryID {entry_id}")
        futures = [search_variation.remote(variation, endpoint, entry_id, search_type, result_brand, result_category, logger)
                   for variation in variations[search_type]]
        results = ray.get(futures)
        successful_results = [res for res in results if res["status"] == "success" and not res["result"].empty]
        if successful_results:
            all_results.extend([res["result"] for res in successful_results])
            break

    loop = asyncio.get_event_loop()
    if all_results:
        try:
            combined_df = pd.concat(all_results, ignore_index=True)
            logger.info(f"Combined {len(combined_df)} results for EntryID {entry_id} for batch insertion")
            combined_df = pd.concat(all_results, ignore_index=True)
            logger.info(f"Combined {len(combined_df)} results for EntryID {entry_id} for batch insertion")
            for api_col, db_col in api_to_db_mapping.items():
                if api_col in combined_df.columns and db_col not in combined_df.columns:
                    combined_df.rename(columns={api_col: db_col}, inplace=True)
            if not all(col in combined_df.columns for col in required_columns):
                logger.error(f"Missing columns {set(required_columns) - set(combined_df.columns)} in result for EntryID {entry_id}")
                return False
            deduplicated_df = combined_df.drop_duplicates(subset=['EntryID', 'ImageUrl'], keep='first')
            logger.info(f"Deduplicated to {len(deduplicated_df)} rows")
            insert_success = insert_search_results(deduplicated_df, logger=logger)
            if not insert_success:
                logger.error(f"Failed to insert deduplicated results for EntryID {entry_id}")
                return False
            logger.info(f"Inserted {len(deduplicated_df)} results for EntryID {entry_id}")
            update_result = loop.run_until_complete(update_search_sort_order(
                str(file_id_db), str(entry_id), result_brand, result_model, result_color, result_category, logger, brand_rules=brand_rules
            ))
            if update_result is None:
                logger.error(f"SortOrder update failed for EntryID {entry_id}")
                return False
            logger.info(f"Updated sort order for EntryID {entry_id} with Brand: {result_brand}, Model: {result_model}, Color: {result_color}, Category: {result_category}")
            try:
                with pyodbc.connect(conn_str, autocommit=False) as conn:
                    cursor = conn.cursor()
                    # Log total rows for debugging
                    cursor.execute("SELECT COUNT(*) FROM utb_ImageScraperResult WHERE EntryID = ?", (entry_id,))
                    total_count = cursor.fetchone()[0]
                    logger.info(f"Verification: Found {total_count} total rows for EntryID {entry_id}")
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
                    if total_count == 0:
                        logger.error(f"No rows found in utb_ImageScraperResult for EntryID {entry_id} after insertion")
            except pyodbc.Error as e:
                logger.error(f"Failed to verify SortOrder for EntryID {entry_id}: {e}", exc_info=True)

            return True
        except Exception as e:
            logger.error(f"Error during batch database update for EntryID {entry_id}: {e}", exc_info=True)
            return False

    logger.info(f"No results to insert for EntryID {entry_id}")
    placeholder_df = pd.DataFrame([{
        "EntryID": entry_id,
        "ImageUrl": "placeholder://no-results",
        "ImageDesc": f"No results found for {search_string}",
        "ImageSource": "N/A",
        "ImageUrlThumbnail": "placeholder://no-results"
    }])
    try:
        insert_success = insert_search_results(placeholder_df, logger=logger)
        if insert_success:
            logger.info(f"Inserted placeholder row for EntryID {entry_id}")
            with pyodbc.connect(conn_str, autocommit=False) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE utb_ImageScraperResult
                    SET SortOrder = 1
                    WHERE EntryID = ? AND ImageUrl = ?
                    """,
                    (entry_id, "placeholder://no-results")
                )
                conn.commit()
                logger.info(f"Fallback applied: Set SortOrder=1 for placeholder row for EntryID {entry_id}")
            return True
        else:
            logger.error(f"Failed to insert placeholder row for EntryID {entry_id}")
            return False
    except pyodbc.Error as e:
        logger.error(f"Failed to insert placeholder row for EntryID {entry_id}: {e}", exc_info=True)
        return False

async def run_process_restart_batch(
    file_id_db: int,
    max_retries: int = 7,
    logger: Optional[logging.Logger] = None,
    entry_id: Optional[int] = None
) -> Dict[str, str]:
    """Async wrapper to run process_restart_batch and handle async operations."""
    logger = logger or logging.getLogger(f"job_{file_id_db}")
    try:
        result = ray.get(process_restart_batch.remote(file_id_db, max_retries, logger, entry_id))
        log_filename = result.get("log_filename")
        if log_filename and os.path.exists(log_filename):
            upload_url = upload_file_to_space(
                log_filename, f"job_logs/job_{file_id_db}.log", True, logger, file_id_db
            )  # Remove await
            logger.info(f"📤 Log file uploaded to: {upload_url}")
            await update_log_url_in_db(str(file_id_db), upload_url, logger)
        return result
    except Exception as e:
        logger.error(f"Error running process_restart_batch for FileID {file_id_db}: {e}", exc_info=True)
        log_filename = logger.handlers[0].baseFilename if logger.handlers else os.path.join(os.getcwd(), 'logs', f"file_{file_id_db}.log")
        if os.path.exists(log_filename):
            upload_url = upload_file_to_space(
                log_filename, f"job_logs/job_{file_id_db}.log", True, logger, file_id_db
            )  # Remove await
            logger.info(f"📤 Log file uploaded to: {upload_url}")
            await update_log_url_in_db(str(file_id_db), upload_url, logger)
        return {"error": str(e)}

@ray.remote(max_retries=3)
def process_restart_batch(
    file_id_db: int,
    max_retries: int = 7,
    logger: Optional[logging.Logger] = None,
    entry_id: Optional[int] = None
) -> Dict[str, str]:
    """Process a batch of entries for a file using Ray, handling retries and logging."""
    logger = logger or logging.getLogger(f"job_{file_id_db}")
    log_dir = os.path.join(os.getcwd(), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_filename = logger.handlers[0].baseFilename if logger.handlers else os.path.join(log_dir, f"file_{file_id_db}.log")
    BATCH_SIZE = 10

    try:
        logger.info(f"🔁 Starting concurrent search processing for FileID: {file_id_db}" + (f", EntryID: {entry_id}" if entry_id else ""))
        file_id_db = int(file_id_db)

        # Fetch brand rules synchronously using the current event loop
        loop = asyncio.get_event_loop()
        brand_rules = loop.run_until_complete(fetch_brand_rules(BRAND_RULES_URL, max_attempts=3, timeout=10, logger=logger))
        logger.debug(f"Fetched brand rules: {brand_rules}")
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

        # Fetch endpoint synchronously
        endpoint = loop.run_until_complete(get_endpoint(logger=logger))
        if not endpoint:
            logger.error(f"No healthy endpoint available for FileID: {file_id_db}")
            return {"error": f"No healthy endpoint available for FileID: {file_id_db}", "log_filename": log_filename}

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

        entry_batches = [entries[i:i + BATCH_SIZE] for i in range(0, len(entries), BATCH_SIZE)]
        logger.info(f"Created {len(entry_batches)} batches for processing")

        successful_entries = 0
        failed_entries = 0
        for batch_idx, batch_entries in enumerate(entry_batches, 1):
            logger.info(f"Processing batch {batch_idx}/{len(entry_batches)} with {len(batch_entries)} entries")
            futures = [
                process_single_row.remote(
                    entry_id, search_string, max_retries, file_id_db, brand_rules, endpoint,
                    brand=brand, model=search_string, color=color, category=category, logger=logger
                )
                for entry_id, search_string, brand, color, category in batch_entries
            ]
            results = ray.get(futures)
            successful_entries += sum(1 for result in results if result is True)
            failed_entries += sum(1 for result in results if result is False)
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
        to_emails = loop.run_until_complete(get_send_to_email(file_id_db, logger))
        if to_emails:
            send_message_email(
                to_emails=to_emails[0],
                subject=f"Error processing FileID: {file_id_db}",
                message=f"An error occurred while processing your file: {str(e)}",
                logger=logger
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
        unique_id = base_name[-8:] if len(base_name) >= 8 else base_name
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

@ray.remote(max_retries=3)
def process_file_with_retries(file_id: int, max_retries: int = 3, logger: Optional[logging.Logger] = None) -> None:
    """Process a file with retries using Ray to ensure all entries have valid results."""
    from image_process import batch_process_images
    logger = logger or default_logger
    try:
        brand_rules = asyncio.run(fetch_brand_rules(BRAND_RULES_URL, logger=logger))
        if not brand_rules:
            logger.warning("Failed to load brand rules; skipping brand-specific search types")
            brand_rules = {"brand_rules": []}

        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT EntryID, ProductModel FROM utb_ImageScraperRecords WHERE FileID = ?", (file_id,))
            entries = [(row[0], row[1]) for row in cursor.fetchall() if row[1]]

        if not entries:
            logger.warning(f"No entries found for FileID {file_id}")
            return

        entries_to_process = set(entry_id for entry_id, _ in entries)
        for retry in range(max_retries):
            if not entries_to_process:
                logger.info("All entries have valid results")
                break
            logger.info(f"Retry {retry+1}/{max_retries}: Processing {len(entries_to_process)} entries")

            batch = [{"EntryID": entry_id, "SearchString": search_string} 
                     for entry_id, search_string in entries if entry_id in entries_to_process]
            results = ray.get(process_restart_batch.remote(file_id, max_retries, logger))

            #batch_process_images(file_id, logger=logger)
            set_sort_order_negative_four_for_zero_match(str(file_id), logger=logger)

            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT DISTINCT r.EntryID
                    FROM utb_ImageScraperRecords r
                    LEFT JOIN utb_ImageScraperResult t ON r.EntryID = t.EntryID
                    WHERE r.FileID = ? 
                    AND (t.SortOrder IS NULL OR t.SortOrder <= 0 OR t.EntryID IS NULL)
                    """,
                    (file_id,)
                )
                remaining_entries = [row[0] for row in cursor.fetchall()]
                entries_to_process = set(remaining_entries)
                logger.info(f"After retry {retry+1}, {len(entries_to_process)} entries still need processing")
                if entries_to_process:
                    logger.warning(f"Remaining entries to process: {entries_to_process}")

        if entries_to_process:
            logger.warning(f"After {max_retries} retries, {len(entries_to_process)} entries still have no valid results")
        else:
            logger.info("All entries have valid results")
    except Exception as e:
        logger.error(f"Error in process_file_with_retries: {e}", exc_info=True)