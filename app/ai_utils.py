import asyncio
import json
import logging
from typing import Any, Dict, List, Optional

import psutil
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text

# --- Project-Specific Imports ---
from database_config import async_engine
from db_utils import enqueue_db_update
# from logging_config import setup_job_logger # Assuming this is handled at a higher level
from search_utils import update_search_sort_order

# This is the new, consolidated analyzer module.
# The `process_batch_for_relevance` function is the new entry point.
from image_analyzer import process_batch_for_relevance

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
    step: int,  # Note: 'step' is no longer used, kept for API compatibility.
    limit: int,
    concurrency: int, # Note: Concurrency is now managed inside image_analyzer, but we can keep param.
    background_tasks: Any, # Note: Not used, kept for API compatibility.
    logger: logging.Logger,
) -> Dict[str, Any]:
    """
    Processes images in batches using a group-based AI analysis for efficiency and deduplication.

    This function iteratively fetches records from the database, sends an entire batch to
    the `process_batch_for_relevance` function, which analyzes them as a group,
    and then enqueues the individual results for database updates. This approach is highly
    efficient for large, potentially redundant image sets.
    """
    job_id = f"ai_processing_{file_id}"
    logger.info(
        f"[{job_id}] Starting BATCHED AI vision processing with GROUP ANALYSIS. "
        f"FileID: {file_id}, Overall Limit: {limit}"
    )
    process = psutil.Process()

    try:
        file_id_int = int(file_id)
    except ValueError:
        logger.error(f"[{job_id}] Invalid FileID format: '{file_id}'. Must be an integer.")
        return {"error": "Invalid FileID format."}

    # --- Configuration ---
    # This BATCH_SIZE determines how many records are passed to the intelligent batch analyzer at once.
    # It should be large enough to benefit from deduplication but small enough to manage memory for image downloads.
    # 50-100 is a good starting point.
    BATCH_SIZE = 75

    # --- State Tracking ---
    total_processed_count = 0 # This will count all records attempted in a fetched batch.
    total_successfully_updated = 0
    total_failed = 0
    offset = 0
    processed_entry_ids = set()  # To efficiently track which EntryIDs need sorting later.

    # --- Main Batch Processing Loop ---
    # The loop condition is based on the number of records we've attempted to process.
    while total_processed_count < limit:
        # Determine the size of the next batch to fetch, respecting the overall limit.
        current_batch_size = min(BATCH_SIZE, limit - total_processed_count)
        if current_batch_size <= 0:
            break

        mem_info = process.memory_info()
        logger.info(
            f"[{job_id}] Fetching next DB batch. Size: {current_batch_size}, Offset: {offset}. "
            f"Memory: {mem_info.rss / 1024**2:.2f} MB"
        )

        # This paginated SQL query is the core of the memory-safe approach.
        base_query = f"""
            SELECT
                res.{IMAGE_SCRAPER_RESULT_PK_COLUMN}, res.{IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN} AS EntryID,
                res.{IMAGE_SCRAPER_RESULT_IMAGE_URL_COLUMN}, rec.{SCRAPER_RECORDS_PRODUCT_BRAND_COLUMN},
                rec.{SCRAPER_RECORDS_PRODUCT_MODEL_COLUMN}, rec.{SCRAPER_RECORDS_PRODUCT_COLOR_COLUMN},
                rec.{SCRAPER_RECORDS_PRODUCT_CATEGORY_COLUMN}
            FROM {IMAGE_SCRAPER_RESULT_TABLE_NAME} AS res
            JOIN {SCRAPER_RECORDS_TABLE_NAME} AS rec ON res.{IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN} = rec.{SCRAPER_RECORDS_PK_COLUMN}
            WHERE rec.{SCRAPER_RECORDS_FILE_ID_FK_COLUMN} = :fid
              AND res.{IMAGE_SCRAPER_RESULT_IMAGE_URL_COLUMN} IS NOT NULL
              AND res.{IMAGE_SCRAPER_RESULT_IMAGE_URL_COLUMN} != ''
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

        # Update the offset for the next iteration *before* processing.
        # This ensures we don't re-fetch the same batch if an error occurs.
        offset += len(records_batch)
        total_processed_count += len(records_batch)

        # --- Call the new batch-oriented analyzer ---
        try:
            logger.info(f"[{job_id}] Sending batch of {len(records_batch)} records to AI group analyzer...")
            # This one call does all the heavy lifting for the batch.
            analysis_results = await process_batch_for_relevance(records_batch, logger)

            # --- Process results and enqueue DB updates ---
            batch_update_tasks = []
            
            # Create a map to get EntryID from ResultID for the final sort-order update
            record_map = {rec['ResultID']: rec for rec in records_batch}

            for result_tuple in analysis_results:
                ai_json_str, is_fashion, caption = result_tuple
                try:
                    # The ResultID is embedded in the JSON, which is essential for mapping.
                    analysis_data = json.loads(ai_json_str)
                    result_id = analysis_data.get("result_id")

                    if not result_id:
                        logger.warning(f"[{job_id}] AI analysis result missing 'result_id'. Cannot update. JSON: {ai_json_str[:200]}")
                        total_failed += 1
                        continue
                    
                    original_record = record_map.get(result_id)
                    if not original_record:
                        logger.error(f"[{job_id}] Could not map result_id {result_id} back to a fetched record. Skipping update.")
                        total_failed += 1
                        continue

                    # Enqueue the database update for this specific record.
                    update_sql = (
                        f"UPDATE {IMAGE_SCRAPER_RESULT_TABLE_NAME} "
                        f"SET {IMAGE_SCRAPER_RESULT_AI_JSON_COLUMN} = :ai_json, "
                        f"{IMAGE_SCRAPER_RESULT_FASHION_FLAG_COLUMN} = :is_fashion, "
                        f"{IMAGE_SCRAPER_RESULT_AI_CAPTION_COLUMN} = :ai_caption "
                        f"WHERE {IMAGE_SCRAPER_RESULT_PK_COLUMN} = :result_id"
                    )
                    update_params = {
                        "ai_json": ai_json_str, "is_fashion": is_fashion,
                        "ai_caption": caption, "result_id": result_id
                    }
                    
                    update_task = enqueue_db_update(
                        file_id=job_id, sql=update_sql, params=update_params,
                        task_type=f"ai_update_{result_id}", logger_param=logger
                    )
                    batch_update_tasks.append(update_task)
                    processed_entry_ids.add(original_record["EntryID"])
                    total_successfully_updated += 1

                except (json.JSONDecodeError, KeyError) as e:
                    logger.error(f"[{job_id}] Failed to process a single AI result from batch: {e}. Result: {result_tuple}", exc_info=True)
                    total_failed += 1

            # Await all the DB enqueueing tasks for this batch to complete.
            if batch_update_tasks:
                await asyncio.gather(*batch_update_tasks)

        except Exception as e:
            logger.critical(f"[{job_id}] Unhandled exception during 'process_batch_for_relevance' for an entire batch. Error: {e}", exc_info=True)
            # Since the whole batch failed, count all its records as failed.
            total_failed += len(records_batch)
        
        logger.info(
            f"[{job_id}] Batch finished. "
            f"Successfully updated: {total_successfully_updated}. Failed: {total_failed}. "
            f"Total attempted so far: {total_processed_count}."
        )


    # --- Post-Processing: Update Sort Order for affected entries ---
    if processed_entry_ids:
        logger.info(f"[{job_id}] AI processing complete. Updating sort order for {len(processed_entry_ids)} affected entries.")
        sort_tasks = [
            update_search_sort_order(
                file_id=str(file_id_int),
                entry_id=str(entry_id_to_sort),
                logger=logger
            ) for entry_id_to_sort in processed_entry_ids
        ]
        await asyncio.gather(*sort_tasks)
        logger.info(f"[{job_id}] Finished updating sort orders.")

    final_message = f"Batch AI processing job finished for FileID {file_id}."
    logger.info(f"[{job_id}] {final_message} Final counts - Success: {total_successfully_updated}, Failed: {total_failed}")

    return {
        "message": final_message,
        "file_id": file_id,
        "processed_successfully": total_successfully_updated,
        "failed_to_process": total_failed,
    }