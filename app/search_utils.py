import logging
import os
import datetime
import json
import aiofiles
from typing import Optional, List, Dict
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from database_config import async_engine, sync_engine
from common import clean_string, normalize_model, generate_aliases
from fastapi import BackgroundTasks
import psutil
import pyodbc
import re
import urllib.parse
from rabbitmq_producer import RabbitMQProducer, enqueue_db_update
import pandas as pd
import asyncio
import uuid
import aio_pika

from tenacity import retry, stop_after_attempt, wait_fixed

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

import logging
from typing import Optional
import requests
from requests.exceptions import RequestException, Timeout

# Default logger setup
default_logger = logging.getLogger(__name__)

def validate_url(url: Optional[str], logger: Optional[logging.Logger] = None, timeout: int = 5) -> bool:
    """
    Validate a thumbnail URL by checking its format and accessibility.
    Returns False if the URL is invalid, results in a 404, or times out.
    
    Args:
        url: The URL to validate (optional string).
        logger: Optional logger instance.
        timeout: Timeout for the HTTP request in seconds (default: 5).
    
    Returns:
        bool: True if the URL is valid and accessible, False otherwise.
    """
    logger = logger or default_logger
    
    # Basic string validation
    if not url or url == '' or 'placeholder' in str(url).lower():
        logger.debug(f"Invalid URL: {url}")
        return False
    if not str(url).startswith(('http://', 'https://')):
        logger.debug(f"Non-HTT URL: {url}")
        return False
    
    # HTTP request validation
    try:
        # Use HEAD request to minimize data transfer
        response = requests.head(url, timeout=timeout, allow_redirects=True)
        
        # If HEAD is not allowed (405), fall back to GET
        if response.status_code == 405:
            response = requests.get(url, timeout=timeout, allow_redirects=True)
        
        # Check for 404 or other non-2xx status codes
        if response.status_code == 404:
            logger.debug(f"Thumbnail URL returned 404: {url}")
            return False
        if response.status_code >= 400:
            logger.debug(f"Thumbnail URL returned status {response.status_code}: {url}")
            return False
        
        logger.debug(f"Thumbnail URL validated successfully: {url}")
        return True
    
    except Timeout:
        logger.debug(f"Thumbnail URL timed out: {url}")
        return False
    except RequestException as e:
        logger.debug(f"Failed to validate thumbnail URL {url}: {str(e)}")
        return False

def clean_url_string(value: Optional[str], is_url: bool = True) -> str:
    logger = default_logger
    if not value:
        return ""
    cleaned = str(value).replace('\\', '').replace('%5C', '').replace('%5c', '')
    cleaned = re.sub(r'[\x00-\x1F\x7F]+', '', cleaned).strip()
    if is_url:
        cleaned = urllib.parse.unquote(cleaned)
        try:
            parsed = urllib.parse.urlparse(cleaned)
            if not parsed.scheme or not parsed.netloc:
                return ""
            path = re.sub(r'/+', '/', parsed.path)
            cleaned = f"{parsed.scheme}://{parsed.netloc}{path}"
            if parsed.query:
                cleaned += f"?{parsed.query}"
            if parsed.fragment:
                cleaned += f"#{parsed.fragment}"
        except ValueError:
            logger.debug(f"Invalid URL format: {cleaned}")
            return ""
    return cleaned

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type((SQLAlchemyError, aio_pika.exceptions.AMQPError, asyncio.TimeoutError)),
    before_sleep=lambda retry_state: default_logger.info(
        f"Retrying insert_search_results for FileID {retry_state.kwargs.get('file_id')} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def insert_search_results(
    results: List[Dict],
    logger: Optional[logging.Logger] = None,
    file_id: str = None,
    background_tasks: Optional[BackgroundTasks] = None
) -> bool:
    logger = logger or default_logger
    process = psutil.Process()
    logger.info(f"Worker PID {process.pid}: Starting insert_search_results for FileID {file_id}")

    if not results:
        logger.warning(f"Worker PID {process.pid}: Empty results provided")
        return False

    required_columns = {"EntryID", "ImageUrl", "ImageDesc", "ImageSource", "ImageUrlThumbnail"}
    for res in results:
        if not required_columns.issubset(res.keys()):
            missing_cols = required_columns - set(res.keys())
            logger.error(f"Worker PID {process.pid}: Missing required columns: {missing_cols}")
            return False
    data = []
    errors = []

    for res in results:
        try:
            entry_id = int(res["EntryID"])
        except (ValueError, TypeError) as e:
            errors.append(f"Invalid EntryID value: {res.get('EntryID')}")
            logger.error(f"Worker PID {process.pid}: {errors[-1]}")
            continue

        image_url = clean_url_string(res.get("ImageUrl", ""))
        image_url_thumbnail = clean_url_string(res.get("ImageUrlThumbnail", ""))
        image_desc = clean_string(res.get("ImageDesc", ""), preserve_url=False)
        image_source = clean_url_string(res.get("ImageSource", ""))

        if not image_url or not validate_url(image_url, logger):
            errors.append(f"Invalid ImageUrl skipped: {image_url}")
            logger.debug(f"Worker PID {process.pid}: {errors[-1]}")
            continue
        if image_url_thumbnail and not validate_url(image_url_thumbnail, logger):
            image_url_thumbnail = None

        data.append({
            "EntryID": entry_id,
            "ImageUrl": image_url,
            "ImageDesc": image_desc or None,
            "ImageSource": image_source or None,
            "ImageUrlThumbnail": image_url_thumbnail or None,
            "CreateTime": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    if not data:
        logger.warning(f"Worker PID {process.pid}: No valid rows to insert. Errors: {errors}")
        return False

    producer = RabbitMQProducer()
    connection = None
    try:
        response_queue = f"select_response_{uuid.uuid4().hex}"
        connection = await aio_pika.connect_robust(producer.amqp_url)
        channel = await connection.channel()
        queue = await channel.declare_queue(response_queue, exclusive=True, auto_delete=True)
        response_received = asyncio.Event()
        response_data = []

        async def consume_responses():
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        if message.correlation_id == file_id:
                            response_data.append(json.loads(message.body.decode()))
                            response_received.set()
                            logger.debug(f"Worker PID {process.pid}: Received response for FileID {file_id}")

        entry_ids = list(set(row["EntryID"] for row in data))
        existing_keys = set()
        if entry_ids:
            placeholders = ",".join([f":id{i}" for i in range(len(entry_ids))])
            select_query = f"""
                SELECT EntryID, ImageUrl
                FROM utb_ImageScraperResult
                WHERE EntryID IN ({placeholders})
            """
            params = {f"id{i}": entry_id for i, entry_id in enumerate(entry_ids)}
            await enqueue_db_update(
                file_id=file_id,
                sql=select_query,
                params=params,
                background_tasks=background_tasks,
                task_type="select_deduplication",
                producer=producer,
                response_queue=response_queue
            )
            logger.info(f"Worker PID {process.pid}: Enqueued SELECT query for {len(entry_ids)} EntryIDs")

            consume_task = asyncio.create_task(consume_responses())
            try:
                async with asyncio.timeout(30):
                    await response_received.wait()
                if response_data:
                    existing_keys = {(row["EntryID"], row["ImageUrl"]) for row in response_data[0]["results"]}
                    logger.info(f"Worker PID {process.pid}: Received {len(existing_keys)} deduplication results")
                else:
                    logger.warning(f"Worker PID {process.pid}: No deduplication results received")
            except asyncio.TimeoutError:
                logger.warning(f"Worker PID {process.pid}: Timeout waiting for SELECT results")
            finally:
                consume_task.cancel()
                try:
                    await asyncio.sleep(0.5)  # Allow time for message processing
                    # No need to manually cancel consumer or delete queue; auto_delete=True handles cleanup
                except Exception as e:
                    logger.warning(f"Worker PID {process.pid}: Error during queue cleanup: {e}")

        update_query = """
            UPDATE utb_ImageScraperResult
            SET ImageDesc = :ImageDesc,
                ImageSource = :ImageSource,
                ImageUrlThumbnail = :ImageUrlThumbnail,
                CreateTime = :CreateTime
            WHERE EntryID = :EntryID AND ImageUrl = :ImageUrl
        """
        insert_query = """
            INSERT INTO utb_ImageScraperResult (EntryID, ImageUrl, ImageDesc, ImageSource, ImageUrlThumbnail, CreateTime)
            VALUES (:EntryID, :ImageUrl, :ImageDesc, :ImageSource, :ImageUrlThumbnail, :CreateTime)
        """

        update_batch = []
        insert_batch = []
        for row in data:
            key = (row["EntryID"], row["ImageUrl"])
            if key in existing_keys:
                update_batch.append((update_query, row))
            else:
                insert_batch.append((insert_query, row))

        batch_size = 100
        for i in range(0, len(update_batch), batch_size):
            batch = update_batch[i:i + batch_size]
            for sql, params in batch:
                await enqueue_db_update(
                    file_id=file_id,
                    sql=sql,
                    params=params,
                    background_tasks=background_tasks,
                    task_type="update_search_result",
                    producer=producer,
                )
        logger.info(f"Worker PID {process.pid}: Enqueued {len(update_batch)} updates")

        for i in range(0, len(insert_batch), batch_size):
            batch = insert_batch[i:i + batch_size]
            for sql, params in batch:
                await enqueue_db_update(
                    file_id=file_id,
                    sql=sql,
                    params=params,
                    background_tasks=background_tasks,
                    task_type="insert_search_result",
                    producer=producer,
                )
        logger.info(f"Worker PID {process.pid}: Enqueued {len(insert_batch)} inserts")

        return len(insert_batch) > 0 or len(update_batch) > 0

    except Exception as e:
        logger.error(f"Worker PID {process.pid}: Error enqueuing results for FileID {file_id}: {e}", exc_info=True)
        raise
    finally:
        if connection and not connection.is_closed:
            await connection.close()
        await producer.close()
        logger.info(f"Worker PID {process.pid}: Closed RabbitMQ producer")

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type((SQLAlchemyError, aio_pika.exceptions.AMQPError)),
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
    logger = logger or default_logger
    process = psutil.Process()

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
        if brand and brand_rules and "brand_rules" in brand_rules:
            for rule in brand_rules["brand_rules"]:
                if any(brand.lower() in name.lower() for name in rule.get("names", [])):
                    brand_aliases = rule.get("names", [])
                    break
        if not brand_aliases and brand_clean:
            brand_aliases = [brand_clean, brand_clean.replace(" & ", " and "), brand_clean.replace(" ", "")]
        brand_aliases = [clean_string(alias).lower() for alias in brand_aliases]
        model_aliases = generate_aliases(model_clean) if model_clean else []
        if model_clean and not model_aliases:
            model_aliases = [model_clean, model_clean.replace("-", ""), model_clean.replace(" ", "")]
        logger.debug(f"Worker PID {process.pid}: Brand aliases: {brand_aliases}, Model aliases: {model_aliases}")

        # Assign priorities
        for res in results:
            image_desc = clean_string(res.get("ImageDesc", ""), preserve_url=False).lower()
            image_source = clean_string(res.get("ImageSource", ""), preserve_url=True).lower()
            image_url = clean_string(res.get("ImageUrl", ""), preserve_url=True).lower()

            model_matched = any(alias in image_desc or alias in image_source or alias in image_url for alias in model_aliases)
            brand_matched = any(alias in image_desc or alias in image_source or alias in image_url for alias in brand_aliases)

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
        producer = RabbitMQProducer()
        try:
            update_data = []
            response_queue = f"select_response_{uuid.uuid4().hex}"
            correlation_id = str(uuid.uuid4())
            total_updated = 0

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
                # Enqueue update with response queue to track completion
                await enqueue_db_update(
                    file_id=file_id,
                    sql=update_query,
                    params=params,
                    background_tasks=background_tasks,
                    task_type="update_sort_order",
                    producer=producer,
                    response_queue=response_queue,
                    correlation_id=correlation_id,
                    return_result=True
                )
                update_data.append({
                    "ResultID": res["ResultID"],
                    "EntryID": entry_id,
                    "SortOrder": sort_order
                })
                logger.debug(f"Worker PID {process.pid}: Enqueued UPDATE for ResultID {res['ResultID']} with SortOrder {sort_order}")

            # Wait for responses
            async with aio_pika.connect_robust(producer.amqp_url) as connection:
                channel = await connection.channel()
                queue = await channel.declare_queue(response_queue, exclusive=True, auto_delete=True)
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        async with message.process():
                            if message.correlation_id == correlation_id:
                                response = json.loads(message.body.decode())
                                total_updated += response.get("result", 0)
                                break

            logger.info(f"Worker PID {process.pid}: Processed SortOrder updates for {total_updated} rows for EntryID {entry_id}")
            return update_data if total_updated > 0 else []
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
) -> Optional[List[Dict]]:
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        logger.info(f"Starting batch SortOrder update for FileID: {file_id}")
        
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
        
        for entry in entries:
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
                
                if entry_results:
                    results.append({"EntryID": entry_id, "Success": True})
                    success_count += 1
                else:
                    results.append({"EntryID": entry_id, "Success": False})
                    failure_count += 1
                    logger.warning(f"No results for EntryID {entry_id}")
            except Exception as e:
                results.append({"EntryID": entry_id, "Success": False, "Error": str(e)})
                failure_count += 1
                logger.error(f"Error processing EntryID {entry_id}: {e}", exc_info=True)
        
        logger.info(f"Completed batch SortOrder update for FileID {file_id}: {success_count} entries successful, {failure_count} failed")
        
        async with async_engine.connect() as conn:
            verification = {}
            queries = [
                ("PositiveSortOrderEntries", "t.SortOrder > 0"),
                ("BrandMatchEntries", "t.SortOrder = 0"),
                ("NoMatchEntries", "t.SortOrder < 0"),
                ("NullSortOrderEntries", "t.SortOrder IS NULL"),
                ("UnexpectedSortOrderEntries", "t.SortOrder = -1")
            ]
            for key, condition in queries:
                query = text(f"""
                    SELECT COUNT(DISTINCT t.EntryID)
                    FROM utb_ImageScraperResult t
                    INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                    WHERE r.FileID = :file_id AND {condition}
                """)
                result = await conn.execute(query, {"file_id": file_id})
                verification[key] = result.scalar()
                result.close()
            
            query = text("""
                SELECT t.EntryID, t.SortOrder, t.ImageUrl
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                WHERE r.FileID = :file_id
            """)
            result = await conn.execute(query, {"file_id": file_id})
            sort_orders = result.fetchall()
            logger.info(f"SortOrder values for FileID {file_id}: {[(row[0], row[1], row[2][:50]) for row in sort_orders]}")
            
            logger.info(f"Verification for FileID {file_id}: "
                       f"{verification['PositiveSortOrderEntries']} entries with model matches, "
                       f"{verification['BrandMatchEntries']} entries with brand matches only, "
                       f"{verification['NoMatchEntries']} entries with no matches, "
                       f"{verification['NullSortOrderEntries']} entries with NULL SortOrder, "
                       f"{verification['UnexpectedSortOrderEntries']} entries with unexpected SortOrder")
        
        return results
    except SQLAlchemyError as e:
        logger.error(f"Database error in batch SortOrder update for FileID {file_id}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Error in batch SortOrder update for FileID {file_id}: {e}", exc_info=True)
        return None

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type(SQLAlchemyError),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying update_sort_no_image_entry for FileID {retry_state.kwargs['file_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def update_sort_no_image_entry(file_id: str, logger: Optional[logging.Logger] = None) -> Optional[Dict]:
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        logger.info(f"Starting per-entry SortOrder update for FileID: {file_id}")
        
        async with async_engine.begin() as conn:
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

            result = await conn.execute(
                text("""
                    DELETE FROM utb_ImageScraperResult
                    WHERE EntryID IN (
                        SELECT r.EntryID
                        FROM utb_ImageScraperRecords r
                        WHERE r.FileID = :file_id
                    ) AND ImageUrl = 'placeholder://no-results'
                """),
                {"file_id": file_id}
            )
            rows_deleted = result.rowcount
            logger.info(f"Deleted {rows_deleted} placeholder entries for FileID {file_id}")

            result = await conn.execute(
                text("""
                    SELECT ResultID, EntryID
                    FROM utb_ImageScraperResult
                    WHERE EntryID IN (
                        SELECT r.EntryID
                        FROM utb_ImageScraperRecords r
                        WHERE r.FileID = :file_id
                    ) AND SortOrder IS NULL
                """),
                {"file_id": file_id}
            )
            rows = result.fetchall()
            result.close()

            rows_updated = 0
            for row in rows:
                result_id, entry_id = row
                update_sql = """
                    UPDATE utb_ImageScraperResult
                    SET SortOrder = :sort_order
                    WHERE ResultID = :result_id AND EntryID = :entry_id
                """
                update_params = {
                    "sort_order": -2,
                    "result_id": result_id,
                    "entry_id": entry_id
                }
                result = await conn.execute(text(update_sql), update_params)
                rowcount = result.rowcount if result.rowcount is not None else 0
                rows_updated += rowcount
                logger.debug(
                    f"Updated SortOrder to -2 for FileID: {file_id}, EntryID: {entry_id}, ResultID: {result_id}, affected {rowcount} rows"
                )

            logger.info(f"Updated {rows_updated} NULL SortOrder entries to -2 for FileID: {file_id}")
            
            return {"file_id": file_id, "rows_deleted": rows_deleted, "rows_updated": rows_updated}
    
    except SQLAlchemyError as e:
        logger.error(f"Database error updating entries for FileID {file_id}: {e}", exc_info=True)
        raise
    except ValueError as ve:
        logger.error(f"Invalid file_id format: {file_id}, error: {str(ve)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error updating entries for FileID {file_id}: {e}", exc_info=True)
        return None