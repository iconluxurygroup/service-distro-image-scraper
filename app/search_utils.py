import logging
import re
import urllib.parse # Not explicitly used in these functions, but kept if common.py uses it
import asyncio
from typing import Optional, List, Dict, Any
import uuid # For correlation_id

from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from fastapi import BackgroundTasks # If used, ensure it's actually passed and utilized
import psutil # For PID logging

# Assuming these are correctly defined and importable from your project structure
from database_config import async_engine
from rabbitmq_producer import RabbitMQProducer # YOUR EXISTING CLASS - UNCHANGED
from db_utils import enqueue_db_update       # YOUR EXISTING FUNCTION - UNCHANGED
import aio_pika # For type hinting and exception handling
import aiormq.exceptions # For specific AMQP exceptions like ConnectionNotAllowed

# Assuming common.py contains these utility functions
from common import clean_string, normalize_model, generate_aliases, validate_thumbnail_url, clean_url_string

# Setup default logger
default_logger = logging.getLogger(__name__) # Use __name__ for module-level logger
if not default_logger.handlers: # Configure only if no handlers are already set
    default_logger.setLevel(logging.INFO)
    # BasicConfig should ideally be called once at the application entry point
    # If this is a library module, it's better for the application to configure logging.
    # For now, keeping it as per your snippet for self-contained example.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

MAX_CONCURRENCY = 1 # As defined in your snippet

# --- Function Definitions ---

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type((
        SQLAlchemyError,
        aio_pika.exceptions.AMQPError,          # General aio_pika AMQP errors
        aiormq.exceptions.ConnectionNotAllowed, # Specifically for channel exhaustion
        aiormq.exceptions.ChannelClosed,        # If channel closes unexpectedly
        aiormq.exceptions.ConnectionClosed      # If connection closes unexpectedly
    )),
    before_sleep=lambda retry_state: retry_state.kwargs.get('logger', default_logger).info(
        f"Retrying update_search_sort_order for FileID {retry_state.kwargs.get('file_id', 'unknown')}, "
        f"EntryID {retry_state.kwargs.get('entry_id', 'unknown')} "
        f"(attempt {retry_state.attempt_number}/{retry_state.stop.max_attempt}) after {retry_state.next_action.sleep:.2f}s. "
        f"Error: {retry_state.outcome.exception()}"
    )
)
async def update_search_sort_order(
    file_id: str,
    entry_id: str,
    producer_instance: RabbitMQProducer, # This instance is passed to enqueue_db_update
    brand: Optional[str] = None,
    model: Optional[str] = None,
    color: Optional[str] = None,
    category: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
    brand_rules: Optional[Dict] = None,
    background_tasks: Optional[BackgroundTasks] = None # Ensure this is used if passed
) -> List[Dict]:
    """
    Updates SortOrder for search results based on brand and model matches.
    Uses the provided RabbitMQ producer_instance for enqueuing DB updates.
    Attempts to retry on channel exhaustion errors, relying on RobustConnection to recover.
    """
    logger = logger or default_logger
    process = psutil.Process() # Get current process info

    # Log entry point with more details
    logger.debug(
        f"Worker PID {process.pid}: Starting update_search_sort_order for "
        f"FileID {file_id}, EntryID {entry_id}"
    )

    try:
        # --- Database Fetch Operation ---
        async with async_engine.connect() as conn:
            query = text("""
                SELECT r.ResultID, r.ImageUrl, r.ImageDesc, r.ImageSource, r.ImageUrlThumbnail
                FROM utb_ImageScraperResult r
                INNER JOIN utb_ImageScraperRecords rec ON r.EntryID = rec.EntryID
                WHERE r.EntryID = :entry_id AND rec.FileID = :file_id
            """)
            db_result = await conn.execute(query, {"entry_id": entry_id, "file_id": file_id})
            rows = db_result.fetchall()
            if not rows: # Early exit if no data
                logger.warning(
                    f"Worker PID {process.pid}: No results found for FileID {file_id}, EntryID {entry_id}. Skipping."
                )
                return []
            columns = db_result.keys()

        results = [dict(zip(columns, row)) for row in rows]
        logger.debug(
            f"Worker PID {process.pid}: Fetched {len(results)} rows for FileID {file_id}, EntryID {entry_id}"
        )

        # --- Input Preprocessing and Alias Generation ---
        brand_clean = clean_string(brand).lower() if brand else ""
        model_clean = normalize_model(model) if model else "" # Assuming normalize_model handles None

        brand_aliases = []
        if brand_clean and brand_rules and isinstance(brand_rules, dict) and "brand_rules" in brand_rules:
            for rule in brand_rules.get("brand_rules", []):
                # Ensure names in rule are also cleaned/lowercased for consistent matching
                if any(brand_clean == clean_string(name).lower() for name in rule.get("names", [])):
                    brand_aliases = [clean_string(alias).lower() for alias in rule.get("names", []) if alias]
                    break
        if not brand_aliases and brand_clean: # Fallback aliases
            brand_aliases = [brand_clean, brand_clean.replace(" & ", " and "), brand_clean.replace(" ", "")]
            brand_aliases = [alias for alias in brand_aliases if alias] # Filter out empty strings

        model_aliases = generate_aliases(model_clean) if model_clean else []
        if model_clean and not model_aliases: # Fallback if generate_aliases returns empty
             model_aliases = [model_clean, model_clean.replace("-", ""), model_clean.replace(" ", "")]
             model_aliases = [alias for alias in model_aliases if alias]

        if not brand_aliases and not model_aliases:
            logger.warning(
                f"Worker PID {process.pid}: No valid brand or model aliases generated for "
                f"FileID {file_id}, EntryID {entry_id}. Results may not be prioritized effectively."
            )

        # --- Priority Assignment ---
        for res in results:
            # Ensure all text fields are present and handle None gracefully before lowercasing
            image_desc = clean_string(res.get("ImageDesc", "") or "", preserve_url=False).lower()
            image_source = clean_string(res.get("ImageSource", "") or "", preserve_url=True).lower()
            image_url = clean_string(res.get("ImageUrl", "") or "", preserve_url=True).lower()

            # Use 'in' for substring matching, ensure aliases are not empty
            model_matched = any(alias and alias in image_desc for alias in model_aliases) or \
                            any(alias and alias in image_source for alias in model_aliases) or \
                            any(alias and alias in image_url for alias in model_aliases)
            brand_matched = any(alias and alias in image_desc for alias in brand_aliases) or \
                            any(alias and alias in image_source for alias in brand_aliases) or \
                            any(alias and alias in image_url for alias in brand_aliases)

            if model_matched and brand_matched:
                res["priority"] = 1
            elif model_matched:
                res["priority"] = 2
            elif brand_matched:
                res["priority"] = 3
            else:
                res["priority"] = 4
            logger.debug(
                f"Worker PID {process.pid}: Assigned priority {res['priority']} to ResultID {res['ResultID']} "
                f"for FileID {file_id}, EntryID {entry_id}"
            )

        sorted_results = sorted(results, key=lambda x: x["priority"])

        # --- Enqueue Database Updates ---
        # The producer_instance passed in will be used by enqueue_db_update.
        # enqueue_db_update (UNCHANGED) is expected to call producer_instance.connection.channel()
        # which is the source of potential channel exhaustion.

        update_data = []
        correlation_id = str(uuid.uuid4()) # Unique ID for this batch of updates for an entry
        for index, res in enumerate(sorted_results):
            sort_order = -2 if res["priority"] == 4 else (index + 1)
            params = {
                "sort_order": sort_order,
                "entry_id": entry_id, # Should be string if DB expects string
                "result_id": res["ResultID"] # Ensure type matches DB
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
                background_tasks=background_tasks, # Pass through if provided
                task_type="update_sort_order",
                producer=producer_instance, # This is the key: uses the shared producer instance
                correlation_id=correlation_id,
                logger=logger
            )
            update_data.append({
                "ResultID": res["ResultID"],
                "EntryID": entry_id,
                "SortOrder": sort_order
            })

        logger.info(
            f"Worker PID {process.pid}: Enqueued SortOrder updates for {len(update_data)} rows "
            f"for FileID {file_id}, EntryID {entry_id} with CorrelationID {correlation_id}"
        )
        return update_data

    except aiormq.exceptions.ConnectionNotAllowed as e:
        # This specific exception is caught to be retried by Tenacity
        logger.error(
            f"Worker PID {process.pid}: Channel limit likely reached for FileID {file_id}, EntryID {entry_id}. "
            f"Error: {e}", exc_info=True
        )
        raise # Re-raise for Tenacity
    except Exception as e: # Catch any other unexpected errors
        logger.error(
            f"Worker PID {process.pid}: Unexpected error in update_search_sort_order for "
            f"FileID {file_id}, EntryID {entry_id}: {e}", exc_info=True
        )
        raise # Re-raise for Tenacity or higher-level handler


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type((SQLAlchemyError, aio_pika.exceptions.AMQPError, aiormq.exceptions.AMQPConnectionError)), # General errors for the whole batch
    before_sleep=lambda retry_state: retry_state.kwargs.get('logger', default_logger).info(
        f"Retrying update_sort_order for FileID {retry_state.kwargs.get('file_id', 'unknown')} "
        f"(attempt {retry_state.attempt_number}/{retry_state.stop.max_attempt}) after {retry_state.next_action.sleep:.2f}s. "
        f"Error: {retry_state.outcome.exception()}"
    )
)
async def update_sort_order(
    file_id: str,
    logger: Optional[logging.Logger] = None,
    background_tasks: Optional[BackgroundTasks] = None
) -> List[Dict]:
    """
    Updates SortOrder for all entries in a FileID by applying update_search_sort_order.
    Uses the singleton RabbitMQ producer. The producer's connection is NOT closed here
    to allow robust mechanisms to function and to be managed at app lifecycle level.
    """
    logger = logger or default_logger
    producer_singleton = None # Variable to hold the singleton producer instance

    try:
        file_id_int = int(file_id) # Validate and convert FileID
    except ValueError:
        logger.error(f"Invalid FileID format: {file_id}. Must be an integer.")
        return [] # Or raise error

    logger.info(f"Starting batch SortOrder update for FileID: {file_id_int}")

    # --- Database Fetch for Entries ---
    async with async_engine.connect() as conn:
        query = text("""
            SELECT EntryID, ProductBrand, ProductModel, ProductColor, ProductCategory 
            FROM utb_ImageScraperRecords 
            WHERE FileID = :file_id
        """)
        db_result = await conn.execute(query, {"file_id": file_id_int})
        entries = db_result.fetchall()

    if not entries:
        logger.warning(f"No entries found for FileID: {file_id_int}. Nothing to process.")
        return []

    # --- Get Singleton RabbitMQ Producer ---
    # This producer instance's connection will remain open after this function completes.
    # It's assumed RabbitMQProducer.get_producer() handles its own connection logic.
    try:
        producer_singleton = await RabbitMQProducer.get_producer()
        if not producer_singleton.is_connected: # Check your producer's state attribute
            logger.warning(f"Producer for FileID {file_id_int} reports not connected. Attempting to ensure connection.")
            await producer_singleton.connect() # Or rely on get_producer to have done this
            if not producer_singleton.is_connected:
                 logger.error(f"Failed to establish RabbitMQ connection for FileID {file_id_int}. Aborting.")
                 raise aio_pika.exceptions.AMQPConnectionError("Producer failed to connect.")
    except Exception as e_prod:
        logger.error(f"Failed to get or connect RabbitMQ producer for FileID {file_id_int}: {e_prod}", exc_info=True)
        raise # Re-raise to be caught by Tenacity for update_sort_order

    # --- Process Entries Concurrently ---
    all_processed_results = []
    success_count = 0
    failure_count = 0
    # Ensure MAX_CONCURRENCY is appropriate for your system resources and DB/RMQ capacity
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    async def process_entry_wrapper(entry_data_tuple, shared_producer):
        async with semaphore:
            entry_id_val, brand_val, model_val, color_val, category_val = entry_data_tuple
            try:
                # Pass the single producer_singleton instance to each call
                entry_results = await update_search_sort_order(
                    file_id=str(file_id_int), # Pass as string
                    entry_id=str(entry_id_val), # Pass as string
                    producer_instance=shared_producer,
                    brand=brand_val,
                    model=model_val,
                    color=color_val,
                    category=category_val,
                    logger=logger, # Pass the parent logger
                    background_tasks=background_tasks # Pass through
                    # brand_rules could be fetched once here and passed if static for the FileID
                )
                return {"EntryID": entry_id_val, "Success": True, "Results": entry_results or []}
            except Exception as e_inner: # Catch errors from update_search_sort_order (after its retries)
                logger.error(
                    f"Error processing EntryID {entry_id_val} for FileID {file_id_int} after retries: {e_inner}",
                    exc_info=True
                )
                return {"EntryID": entry_id_val, "Success": False, "Error": str(e_inner), "Results": []}

    tasks = [process_entry_wrapper(entry, producer_singleton) for entry in entries]
    # return_exceptions=True means asyncio.gather won't stop on the first exception
    # but will return the exception object in place of the result.
    gathered_task_results = await asyncio.gather(*tasks, return_exceptions=True)

    for result_item in gathered_task_results:
        if isinstance(result_item, BaseException): # An exception from process_entry_wrapper itself (e.g., semaphore issues)
            logger.error(f"A task wrapper for FileID {file_id_int} failed unexpectedly: {result_item}", exc_info=result_item)
            failure_count += 1 # Count as failure
            # Add a placeholder if you need to track which entry might have caused this
            # all_processed_results.append({"EntryID": "UnknownDueToTaskWrapperError", "Success": False, "Error": str(result_item)})
        elif isinstance(result_item, dict): # Expected dictionary from process_entry_wrapper
            all_processed_results.append(result_item)
            if result_item.get("Success"):
                success_count += 1
            else:
                failure_count += 1
                # Logging for failures within process_entry_wrapper is already done there
        else: # Should not happen if process_entry_wrapper always returns a dict or raises
            logger.error(f"Unexpected result type from task for FileID {file_id_int}: {type(result_item)} - {result_item}")
            failure_count += 1


    logger.info(
        f"Completed batch SortOrder update for FileID {file_id_int}: "
        f"{success_count} entries processed successfully, {failure_count} failed."
    )

    # --- Verification (Optional but good for debugging) ---
    try:
        async with async_engine.connect() as conn:
            verification_query = text("""
                SELECT 
                    SUM(CASE WHEN t.SortOrder > 0 THEN 1 ELSE 0 END) AS PositiveSortOrderEntries,
                    SUM(CASE WHEN t.SortOrder = 0 THEN 1 ELSE 0 END) AS ZeroSortOrderEntries, 
                    SUM(CASE WHEN t.SortOrder = -2 THEN 1 ELSE 0 END) AS NoMatchEntries, 
                    SUM(CASE WHEN t.SortOrder IS NULL THEN 1 ELSE 0 END) AS NullSortOrderEntries,
                    SUM(CASE WHEN t.SortOrder < 0 AND t.SortOrder != -2 THEN 1 ELSE 0 END) AS OtherNegativeSortOrderEntries
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                WHERE r.FileID = :file_id
            """)
            verif_result = await conn.execute(verification_query, {"file_id": file_id_int})
            verification_data = verif_result.fetchone()

            if verification_data:
                logger.info(
                    f"Verification for FileID {file_id_int}: "
                    f"PositiveSortOrder: {verification_data['PositiveSortOrderEntries'] or 0}, "
                    f"ZeroSortOrder: {verification_data['ZeroSortOrderEntries'] or 0}, "
                    f"NoMatch(SortOrder=-2): {verification_data['NoMatchEntries'] or 0}, "
                    f"NullSortOrder: {verification_data['NullSortOrderEntries'] or 0}, "
                    f"OtherNegativeSortOrder: {verification_data['OtherNegativeSortOrderEntries'] or 0}"
                )
            else:
                logger.warning(f"Could not fetch verification data for FileID {file_id_int}")
    except Exception as e_verif:
        logger.error(f"Error during verification step for FileID {file_id_int}: {e_verif}", exc_info=True)


    return all_processed_results

    # Catch specific exceptions if needed, otherwise Tenacity handles retries for specified exceptions
    # except SQLAlchemyError as e_sql: ...
    # except aio_pika.exceptions.AMQPError as e_amqp: ...

    finally:
        # IMPORTANT: The producer_singleton (and its underlying connection) is NOT closed here.
        # It is a shared singleton and should be managed at the application lifecycle level
        # (e.g., closed on application shutdown).
        if producer_singleton:
             logger.info(
                f"Finished batch SortOrder update for FileID {file_id}. "
                "Shared RabbitMQ producer connection remains open for application use."
            )

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type(SQLAlchemyError),
    before_sleep=lambda retry_state: retry_state.kwargs.get('logger', default_logger).info(
        f"Retrying update_sort_no_image_entry for FileID {retry_state.kwargs.get('file_id', 'unknown')} "
        f"(attempt {retry_state.attempt_number}/{retry_state.stop.max_attempt}) after {retry_state.next_action.sleep:.2f}s. "
        f"Error: {retry_state.outcome.exception()}"
    )
)
async def update_sort_no_image_entry(file_id: str, logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
    """
    Updates SortOrder for entries with no valid images by:
    1. Deleting placeholder image entries (ImageUrl LIKE 'placeholder://%').
    2. Setting SortOrder to -2 for any remaining entries within the FileID that still have NULL SortOrder.
    This function does NOT interact with RabbitMQ.
    """
    logger = logger or default_logger
    try:
        file_id_int = int(file_id)
    except ValueError:
        logger.error(f"Invalid FileID format for no-image update: {file_id}. Must be an integer.")
        return {"file_id": file_id, "rows_deleted": 0, "rows_updated": 0, "error": "Invalid FileID format"}

    logger.info(f"Starting SortOrder update for no-image entries in FileID: {file_id_int}")

    rows_deleted = 0
    rows_updated = 0

    try:
        async with async_engine.begin() as conn: # Use a transaction for atomicity
            # Delete placeholder entries
            delete_placeholders_query = text("""
                DELETE FROM utb_ImageScraperResult
                WHERE EntryID IN (
                    SELECT r.EntryID
                    FROM utb_ImageScraperRecords r
                    WHERE r.FileID = :file_id
                ) AND ImageUrl LIKE 'placeholder://%'
            """)
            result_delete = await conn.execute(delete_placeholders_query, {"file_id": file_id_int})
            rows_deleted = result_delete.rowcount
            logger.info(f"Deleted {rows_deleted} placeholder entries for FileID {file_id_int}")

            # Update remaining NULL SortOrder entries to -2 for this FileID
            update_nulls_query = text("""
                UPDATE utb_ImageScraperResult
                SET SortOrder = -2
                WHERE EntryID IN (
                    SELECT r.EntryID
                    FROM utb_ImageScraperRecords r
                    WHERE r.FileID = :file_id
                ) AND SortOrder IS NULL
            """)
            result_update = await conn.execute(update_nulls_query, {"file_id": file_id_int})
            rows_updated = result_update.rowcount
            logger.info(f"Updated {rows_updated} NULL SortOrder entries to -2 for FileID {file_id_int}")

        return {"file_id": file_id_int, "rows_deleted": rows_deleted, "rows_updated": rows_updated, "status": "success"}

    except SQLAlchemyError as e_sql:
        logger.error(
            f"Database error updating no-image entries for FileID {file_id_int}: {e_sql}", exc_info=True
        )
        raise # Re-raise for Tenacity
    except Exception as e_gen: # Catch any other unexpected errors
        logger.error(
            f"Unexpected error updating no-image entries for FileID {file_id_int}: {e_gen}", exc_info=True
        )
        # Depending on policy, you might want to re-raise or return an error dict
        return {"file_id": file_id_int, "rows_deleted": rows_deleted, "rows_updated": rows_updated, "error": str(e_gen)}
