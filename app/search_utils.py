import logging
import re
import urllib.parse
import asyncio
from typing import Optional, List, Dict, Any
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from fastapi import BackgroundTasks
import psutil
from database_config import async_engine
from rabbitmq_producer import RabbitMQProducer
from db_utils import enqueue_db_update
import uuid
import aio_pika
from common import clean_string, normalize_model, generate_aliases,validate_thumbnail_url,clean_url_string

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

MAX_CONCURRENCY = 10

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type((SQLAlchemyError, aio_pika.exceptions.AMQPError,
    RuntimeError)),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying update_search_sort_order for FileID {retry_state.kwargs.get('file_id', 'unknown')} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def update_search_sort_order(
    file_id: str,
    entry_id: str,
    brand: Optional[str] = None,
    model: Optional[str] = None,
    color: Optional[str] = None,
    category: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
    brand_rules: Optional[Dict] = None,
    background_tasks: Optional[BackgroundTasks] = None
) -> List[Dict]:
    """
    Updates SortOrder for search results based on brand and model matches.
    """
    logger = logger or default_logger
    process = psutil.Process()
    logger.info(f"Worker PID {process.pid}: Starting update_search_sort_order for FileID {file_id}, EntryID {entry_id}")

    try:
        # Fetch results
        async with async_engine.connect() as conn:
            query = text("""
                SELECT r.ResultID, r.ImageUrl, r.ImageDesc, r.ImageSource, r.ImageUrlThumbnail
                FROM utb_ImageScraperResult r
                INNER JOIN utb_ImageScraperRecords rec ON r.EntryID = rec.EntryID
                WHERE r.EntryID = :entry_id AND rec.FileID = :file_id
            """)
            result = await conn.execute(query, {"entry_id": entry_id, "file_id": file_id})
            rows = result.fetchall()
            columns = result.keys()
            result.close()

        if not rows:
            logger.warning(f"Worker PID {process.pid}: No results found for FileID {file_id}, EntryID {entry_id}")
            return []

        results = [dict(zip(columns, row)) for row in rows]
        logger.info(f"Worker PID {process.pid}: Fetched {len(results)} rows for EntryID {entry_id}")

        # Preprocess inputs
        brand_clean = clean_string(brand).lower() if brand else ""
        model_clean = normalize_model(model) if model else ""
        logger.debug(f"Worker PID {process.pid}: Cleaned brand: {brand_clean}, Cleaned model: {model_clean}")

        # Generate aliases
        brand_aliases = []
        if brand and brand_rules and isinstance(brand_rules, dict) and "brand_rules" in brand_rules:
            for rule in brand_rules.get("brand_rules", []):
                if any(brand.lower() in name.lower() for name in rule.get("names", [])):
                    brand_aliases = rule.get("names", [])
                    break
        if not brand_aliases and brand_clean:
            brand_aliases = [brand_clean, brand_clean.replace(" & ", " and "), brand_clean.replace(" ", "")]
        brand_aliases = [clean_string(alias).lower() for alias in brand_aliases if alias]
        model_aliases = generate_aliases(model_clean) if model_clean else []
        if model_clean and not model_aliases:
            model_aliases = [model_clean, model_clean.replace("-", ""), model_clean.replace(" ", "")]
        logger.debug(f"Worker PID {process.pid}: Brand aliases: {brand_aliases}, Model aliases: {model_aliases}")

        if not brand_aliases and not model_aliases:
            logger.warning(f"Worker PID {process.pid}: No valid aliases generated for EntryID {entry_id}")

        # Assign priorities
        for res in results:
            image_desc = clean_string(res.get("ImageDesc", ""), preserve_url=False).lower()
            image_source = clean_string(res.get("ImageSource", ""), preserve_url=True).lower()
            image_url = clean_string(res.get("ImageUrl", ""), preserve_url=True).lower()
            logger.debug(f"Worker PID {process.pid}: ImageDesc: {image_desc[:100]}, ImageSource: {image_source[:100]}, ImageUrl: {image_url[:100]}")

            model_matched = any(alias in image_desc or alias in image_source or alias in image_url for alias in model_aliases)
            brand_matched = any(alias in image_desc or alias in image_source or alias in image_url for alias in brand_aliases)
            logger.debug(f"Worker PID {process.pid}: Model matched: {model_matched}, Brand matched: {brand_matched}")

            if model_matched and brand_matched:
                res["priority"] = 1
            elif model_matched:
                res["priority"] = 2
            elif brand_matched:
                res["priority"] = 3
            else:
                res["priority"] = 4
            logger.debug(f"Worker PID {process.pid}: Assigned priority {res['priority']} to ResultID {res['ResultID']}")

        # Sort results
        sorted_results = sorted(results, key=lambda x: x["priority"])
        logger.debug(f"Worker PID {process.pid}: Sorted {len(sorted_results)} results for EntryID {entry_id}")

        # Enqueue updates
        producer = await RabbitMQProducer.get_producer()
        try:
            update_data = []
            correlation_id = str(uuid.uuid4())
            for index, res in enumerate(sorted_results):
                sort_order = -2 if res["priority"] == 4 else (index + 1)
                params = {
                    "sort_order": sort_order,
                    "entry_id": entry_id,
                    "result_id": res["ResultID"]
                }
                update_query = """
                    UPDATE utb_ImageScraperResult
                    SET SortOrder = :sort_order
                    WHERE EntryID = :entry_id AND ResultID = :result_id
                """
                await enqueue_db_update(
                    file_id=file_id,
                    sql=update_query,
                    params=params,
                    task_type="update_sort_order",
                    producer_instance=producer,
                    correlation_id=correlation_id,
                    logger=logger
                )
                update_data.append({
                    "ResultID": res["ResultID"],
                    "EntryID": entry_id,
                    "SortOrder": sort_order
                })
                logger.debug(
                    f"Worker PID {process.pid}: Enqueued UPDATE for ResultID {res['ResultID']} with SortOrder {sort_order}"
                )

            logger.info(
                f"Worker PID {process.pid}: Enqueued SortOrder updates for {len(update_data)} rows for EntryID {entry_id}"
            )
            return update_data
        finally:
            await producer.close()
            logger.info(f"Worker PID {process.pid}: Closed RabbitMQ producer for EntryID {entry_id}")
    except Exception as e:
        logger.error(f"Worker PID {process.pid}: Error in update_search_sort_order for EntryID {entry_id}: {e}", exc_info=True)
        raise

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type(SQLAlchemyError),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying update_sort_order for FileID {retry_state.kwargs['file_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def update_sort_order(
    file_id: str,
    logger: Optional[logging.Logger] = None,
    background_tasks: Optional[BackgroundTasks] = None
) -> List[Dict]:
    """
    Updates SortOrder for all entries in a FileID by applying update_search_sort_order.
    """
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        logger.info(f"Starting batch SortOrder update for FileID: {file_id}")

        # Fetch entries
        async with async_engine.connect() as conn:
            query = text("""
                SELECT EntryID, ProductBrand, ProductModel, ProductColor, ProductCategory 
                FROM utb_ImageScraperRecords 
                WHERE FileID = :file_id
            """)
            logger.debug(f"Executing query: {query} with FileID: {file_id}")
            result = await conn.execute(query, {"file_id": file_id})
            entries = result.fetchall()
            result.close()

        if not entries:
            logger.warning(f"No entries found for FileID: {file_id}")
            return []

        results = []
        success_count = 0
        failure_count = 0
        semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

        async def process_entry(entry):
            async with semaphore:
                entry_id, brand, model, color, category = entry
                logger.debug(f"Worker PID {psutil.Process().pid}: Processing EntryID {entry_id}, Brand: {brand}, Model: {model}")
                try:
                    entry_results = await update_search_sort_order(
                        file_id=str(file_id),
                        entry_id=str(entry_id),
                        brand=brand,
                        model=model,
                        color=color,
                        category=category,
                        logger=logger,
                        background_tasks=background_tasks
                    )
                    return {"EntryID": entry_id, "Success": bool(entry_results), "Results": entry_results}
                except Exception as e:
                    logger.error(f"Error processing EntryID {entry_id}: {e}", exc_info=True)
                    return {"EntryID": entry_id, "Success": False, "Error": str(e)}

        # Process entries in parallel
        tasks = [process_entry(entry) for entry in entries]
        processed = await asyncio.gather(*tasks, return_exceptions=True)

        for result in processed:
            if isinstance(result, asyncio.CancelledError):
                logger.warning(f"A task was cancelled during batch processing: {result}")
                failure_count += 1 # Or handle as appropriate for cancelled tasks
                # Potentially log which EntryID was being processed if you can associate it
                continue
            elif isinstance(result, Exception): # Handle other exceptions
                logger.error(f"Unexpected error in batch processing for an entry: {result}", exc_info=True)
                failure_count += 1
                continue

            # If it's not an exception, it should be the dictionary
            results.append(result)
            if result.get("Success"): # Use .get() for safety, though it should be a dict here
                success_count += 1
            else:
                failure_count += 1
                logger.warning(f"No results for EntryID {result.get('EntryID', 'Unknown EntryID')}: {result.get('Error', 'Unknown error')}")

        # Verify sort order distribution
        async with async_engine.connect() as conn:
            verification_query = text("""
                SELECT 
                    SUM(CASE WHEN t.SortOrder > 0 THEN 1 ELSE 0 END) AS PositiveSortOrderEntries,
                    SUM(CASE WHEN t.SortOrder = 0 THEN 1 ELSE 0 END) AS BrandMatchEntries,
                    SUM(CASE WHEN t.SortOrder < 0 THEN 1 ELSE 0 END) AS NoMatchEntries,
                    SUM(CASE WHEN t.SortOrder IS NULL THEN 1 ELSE 0 END) AS NullSortOrderEntries,
                    SUM(CASE WHEN t.SortOrder = -1 THEN 1 ELSE 0 END) AS UnexpectedSortOrderEntries
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                WHERE r.FileID = :file_id
            """)
            result = await conn.execute(verification_query, {"file_id": file_id})
            verification = result.fetchone()
            result.close()

            logger.info(f"Verification for FileID {file_id}: "
                       f"{verification[0]} entries with model matches, "
                       f"{verification[1]} entries with brand matches only, "
                       f"{verification[2]} entries with no matches, "
                       f"{verification[3]} entries with NULL SortOrder, "
                       f"{verification[4]} entries with unexpected SortOrder")

            # Log sample sort orders
            sample_query = text("""
                SELECT TOP 10 t.EntryID, t.SortOrder, LEFT(t.ImageUrl, 50) AS ImageUrl
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                WHERE r.FileID = :file_id
                ORDER BY t.EntryID
            """)
            result = await conn.execute(sample_query, {"file_id": file_id})
            sort_orders = result.fetchall()
            logger.debug(f"Sample SortOrder values for FileID {file_id}: {[(row[0], row[1], row[2]) for row in sort_orders]}")

        return results
    except SQLAlchemyError as e:
        logger.error(f"Database error in batch SortOrder update for FileID {file_id}: {e}", exc_info=True)
        raise
    except ValueError as ve:
        logger.error(f"Invalid file_id format: {file_id}, error: {ve}")
        return []
    except Exception as e:
        logger.error(f"Error in batch SortOrder update for FileID {file_id}: {e}", exc_info=True)
        return []

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type(SQLAlchemyError),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying update_sort_no_image_entry for FileID {retry_state.kwargs['file_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def update_sort_no_image_entry(file_id: str, logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
    """
    Updates SortOrder for entries with no valid images, deleting placeholders and setting null SortOrder to -2.
    """
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        logger.info(f"Starting per-entry SortOrder update for FileID: {file_id}")

        async with async_engine.begin() as conn:
            # Count null SortOrder entries
            result = await conn.execute(
                text("""
                    SELECT COUNT(*) 
                    FROM utb_ImageScraperResult 
                    WHERE EntryID IN (
                        SELECT EntryID 
                        FROM utb_ImageScraperRecords 
                        WHERE FileID = :file_id
                    ) AND SortOrder IS NULL
                """),
                {"file_id": file_id}
            )
            null_count = result.scalar()
            logger.debug(f"Worker PID {psutil.Process().pid}: {null_count} entries with NULL SortOrder for FileID {file_id}")

            # Delete placeholder entries
            result = await conn.execute(
                text("""
                    DELETE FROM utb_ImageScraperResult
                    WHERE EntryID IN (
                        SELECT r.EntryID
                        FROM utb_ImageScraperRecords r
                        WHERE r.FileID = :file_id
                    ) AND ImageUrl LIKE 'placeholder://%'
                """),
                {"file_id": file_id}
            )
            rows_deleted = result.rowcount
            logger.info(f"Deleted {rows_deleted} placeholder entries for FileID {file_id}")

            # Update null SortOrder entries in batch
            result = await conn.execute(
                text("""
                    UPDATE utb_ImageScraperResult
                    SET SortOrder = -2
                    WHERE EntryID IN (
                        SELECT r.EntryID
                        FROM utb_ImageScraperRecords r
                        WHERE r.FileID = :file_id
                    ) AND SortOrder IS NULL
                """),
                {"file_id": file_id}
            )
            rows_updated = result.rowcount
            logger.info(f"Updated {rows_updated} NULL SortOrder entries to -2 for FileID {file_id}")

        return {"file_id": file_id, "rows_deleted": rows_deleted, "rows_updated": rows_updated}
    except SQLAlchemyError as e:
        logger.error(f"Database error updating entries for FileID {file_id}: {e}", exc_info=True)
        raise
    except ValueError as ve:
        logger.error(f"Invalid file_id format: {file_id}, error: {ve}")
        return {"file_id": file_id, "rows_deleted": 0, "rows_updated": 0, "error": str(ve)}
    except Exception as e:
        logger.error(f"Unexpected error updating entries for FileID {file_id}: {e}", exc_info=True)
        return {"file_id": file_id, "rows_deleted": 0, "rows_updated": 0, "error": str(e)}