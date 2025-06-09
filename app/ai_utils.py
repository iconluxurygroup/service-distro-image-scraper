# file: ai_utils.py

import asyncio
import logging
from typing import Any, Dict, List, Optional

import psutil
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text

# --- Project-Specific Imports ---
from database_config import async_engine
from db_utils import enqueue_db_update
from search_utils import update_search_sort_order

# This is the new, consolidated analyzer module from the previous step.
# It contains the `analyze_single_image_record` function.
from image_analyzer import analyze_single_image_record


# --- Constants for Table and Column Names (for clarity and maintenance) ---
IMAGE_SCRAPER_RESULT_TABLE_NAME = "utb_ImageScraperResult"
IMAGE_SCRAPER_RESULT_PK_COLUMN = "ResultID"
IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN = "EntryID"
IMAGE_SCRAPER_RESULT_IMAGE_URL_COLUMN = "ImageUrl"
IMAGE_SCRAPER_RESULT_AI_JSON_COLUMN = "AiJson"
IMAGE_SCRAPER_RESULT_AI_CAPTION_COLUMN = "AiCaption"
IMAGE_SCRAPER_RESULT_FASHION_FLAG_COLUMN = "ImageIsFashion"

SCRAPER_RECORDS_TABLE_NAME = "utb_ImageScraperRecords"
SCRAPER_RECORDS_PK_COLUMN = "EntryID"
SCRAPER_RECORDS_FILE_ID_FK_COLUMN = "FileID"
SCRAPER_RECORDS_PRODUCT_BRAND_COLUMN = "ProductBrand"
SCRAPER_RECORDS_PRODUCT_MODEL_COLUMN = "ProductModel"
SCRAPER_RECORDS_PRODUCT_COLOR_COLUMN = "ProductColor"
SCRAPER_RECORDS_PRODUCT_CATEGORY_COLUMN = "ProductCategory"


async def batch_vision_reason(
    file_id: str,
    entry_ids: Optional[List[int]],
    step: int,  # Note: 'step' is no longer used in this design but kept for API compatibility.
    limit: int,
    concurrency: int,
    background_tasks: Any, # Note: Not used, but kept for API compatibility.
    logger: logging.Logger,
) -> Dict[str, Any]:
    """
    Processes images through a vision AI model in manageable batches to handle large datasets.

    This function iteratively fetches records from the database using pagination, sends them
    to an AI service for analysis, and enqueues the results for database updates. This
    approach keeps memory usage low and stable, regardless of the total number of records.
    """
    job_id = f"ai_processing_{file_id}"
    logger.info(
        f"[{job_id}] Starting BATCHED AI vision processing. "
        f"FileID: {file_id}, Concurrency: {concurrency}, Overall Limit: {limit}"
    )
    process = psutil.Process()

    try:
        file_id_int = int(file_id)
    except ValueError:
        logger.error(f"[{job_id}] Invalid FileID format: '{file_id}'. Must be an integer.")
        return {"error": "Invalid FileID format."}

    # --- Configuration ---
    BATCH_SIZE = 150  # The number of records to process at once. Key to memory safety.
    
    # --- State Tracking ---
    total_processed_successfully = 0
    total_failed = 0
    offset = 0
    processed_entry_ids = set() # To efficiently track which EntryIDs need sorting later.
    
    semaphore = asyncio.Semaphore(concurrency)

    async def _process_and_update_record(record: Dict) -> bool:
        """
        Async helper to process a single record, call the AI, and enqueue the DB update.
        Returns True on success, False on failure.
        """
        async with semaphore:
            result_id = record.get("ResultID")
            entry_id = record.get("EntryID")
            
            try:
                # This single call orchestrates the entire analysis for one image.
                ai_json, image_is_fashion, ai_caption = await analyze_single_image_record(record, logger)

                # A basic check to see if the analysis succeeded.
                if "Error:" in ai_caption:
                    logger.warning(f"[{job_id}] AI processing failed for ResultID {result_id} with caption: {ai_caption}")
                    # Optionally, you could still update the DB with the error JSON.
                    return False

                # Enqueue the database update. This is non-blocking and robust.
                update_sql = (
                    f"UPDATE {IMAGE_SCRAPER_RESULT_TABLE_NAME} "
                    f"SET {IMAGE_SCRAPER_RESULT_AI_JSON_COLUMN} = :ai_json, "
                    f"{IMAGE_SCRAPER_RESULT_FASHION_FLAG_COLUMN} = :is_fashion, "
                    f"{IMAGE_SCRAPER_RESULT_AI_CAPTION_COLUMN} = :ai_caption "
                    f"WHERE {IMAGE_SCRAPER_RESULT_PK_COLUMN} = :result_id"
                )
                update_params = {
                    "ai_json": ai_json,
                    "is_fashion": image_is_fashion,
                    "ai_caption": ai_caption,
                    "result_id": result_id
                }
                
                await enqueue_db_update(
                    file_id=job_id,
                    sql=update_sql,
                    params=update_params,
                    task_type=f"ai_update_{result_id}",
                    logger_param=logger
                )
                
                processed_entry_ids.add(entry_id) # Add to set for the final sorting step.
                return True

            except Exception as e:
                logger.error(f"[{job_id}] Unhandled exception processing ResultID {result_id}: {e}", exc_info=True)
                return False

    # --- Main Batch Processing Loop ---
    while total_processed_successfully < limit:
        current_batch_size = min(BATCH_SIZE, limit - total_processed_successfully)
        if current_batch_size <= 0:
            break

        mem_info = process.memory_info()
        logger.info(
            f"[{job_id}] Fetching next batch of {current_batch_size} records. Offset: {offset}. "
            f"Memory: {mem_info.rss / 1024**2:.2f} MB"
        )
        
        # This paginated SQL query is the core of the memory-safe approach.
        base_query = f"""
            SELECT
                res.{IMAGE_SCRAPER_RESULT_PK_COLUMN}, res.{IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN} AS EntryID,
                res.{IMAGE_SCRAPER_RESULT_IMAGE_URL_COLUMN},
                rec.{SCRAPER_RECORDS_PRODUCT_BRAND_COLUMN}, rec.{SCRAPER_RECORDS_PRODUCT_MODEL_COLUMN},
                rec.{SCRAPER_RECORDS_PRODUCT_COLOR_COLUMN}, rec.{SCRAPER_RECORDS_PRODUCT_CATEGORY_COLUMN}
            FROM {IMAGE_SCRAPER_RESULT_TABLE_NAME} AS res
            JOIN {SCRAPER_RECORDS_TABLE_NAME} AS rec ON res.{IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN} = rec.{SCRAPER_RECORDS_PK_COLUMN}
            WHERE rec.{SCRAPER_RECORDS_FILE_ID_FK_COLUMN} = :fid
              AND res.{IMAGE_SCRAPER_RESULT_IMAGE_URL_COLUMN} IS NOT NULL
              AND (res.{IMAGE_SCRAPER_RESULT_AI_JSON_COLUMN} IS NULL OR res.{IMAGE_SCRAPER_RESULT_AI_JSON_COLUMN} = '')
        """
        
        if entry_ids:
            base_query += f" AND res.{IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN} IN :entry_ids"
        
        paginated_query = base_query + f" ORDER BY res.{IMAGE_SCRAPER_RESULT_PK_COLUMN} OFFSET :offset ROWS FETCH NEXT :batch_size ROWS ONLY;"
        
        sql_params = {"fid": file_id_int, "offset": offset, "batch_size": current_batch_size}
        if entry_ids:
            sql_params["entry_ids"] = tuple(entry_ids)

        records_batch = []
        try:
            async with async_engine.connect() as conn:
                result_cursor = await conn.execute(text(paginated_query), sql_params)
                records_batch = result_cursor.mappings().fetchall()
        except SQLAlchemyError as db_exc:
            logger.critical(f"[{job_id}] Database error during batch fetch, aborting job: {db_exc}", exc_info=True)
            break 

        if not records_batch:
            logger.info(f"[{job_id}] No more records to process. Job finished.")
            break

        tasks = [_process_and_update_record(record) for record in records_batch]
        batch_results = await asyncio.gather(*tasks)

        batch_success_count = sum(1 for success in batch_results if success)
        total_processed_successfully += batch_success_count
        total_failed += len(records_batch) - batch_success_count
        offset += len(records_batch) 
        
        logger.info(
            f"[{job_id}] Batch finished. "
            f"Successful: {batch_success_count}. Failed: {len(records_batch) - batch_success_count}. "
            f"Total successful: {total_processed_successfully}. Total failed: {total_failed}."
        )

    # --- Post-Processing: Update Sort Order for affected entries ---
    if processed_entry_ids:
        logger.info(f"[{job_id}] AI processing complete. Now updating sort order for {len(processed_entry_ids)} affected entries.")
        try:
            # We can run these in parallel. update_search_sort_order should be safe for this.
            sort_tasks = [
                update_search_sort_order(
                    file_id=str(file_id_int),
                    entry_id=str(entry_id_to_sort),
                    logger=logger
                ) for entry_id_to_sort in processed_entry_ids
            ]
            await asyncio.gather(*sort_tasks)
            logger.info(f"[{job_id}] Finished enqueuing/running sort order updates.")
        except Exception as e_sort:
            logger.error(f"[{job_id}] An error occurred during the post-processing sort step: {e_sort}", exc_info=True)


    final_message = f"Batch AI processing job finished for FileID {file_id}."
    logger.info(f"[{job_id}] {final_message} Final counts - Success: {total_processed_successfully}, Failed: {total_failed}")

    return {
        "message": final_message,
        "file_id": file_id,
        "processed_successfully": total_processed_successfully,
        "failed_to_process": total_failed,
    }