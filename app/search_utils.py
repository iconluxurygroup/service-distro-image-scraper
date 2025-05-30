import logging
import os
import datetime
import json
from typing import Optional, List, Dict
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from database_config import async_engine
from common import clean_string, normalize_model, generate_aliases
from fastapi import BackgroundTasks
import psutil
import re
import urllib.parse
from rabbitmq_producer import RabbitMQProducer, enqueue_db_update, get_producer
import asyncio
import uuid
import aio_pika
from config import RABBITMQ_URL
from s3_utils import upload_file_to_space

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

producer = None

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
    log_filename = f"job_logs/job_{file_id}.log"
    
    if not results:
        logger.warning(f"Worker PID {process.pid}: Empty results provided")
        return False

    global producer
    if producer is None or not producer.is_connected or not producer.channel or producer.channel.is_closed:
        logger.warning("RabbitMQ producer not initialized or disconnected, reconnecting")
        try:
            async with asyncio.timeout(60):
                producer = await get_producer(logger)
        except Exception as e:
            logger.error(f"Failed to initialize RabbitMQ producer: {e}", exc_info=True)
            return False

    channel = producer.channel
    if channel is None:
        logger.error(f"Worker PID {process.pid}: RabbitMQ channel is not available for FileID {file_id}")
        return False

    correlation_id = str(uuid.uuid4())
    response_queue = f"select_response_{correlation_id}"

    try:
        # Declare response queue
        try:
            queue = await channel.declare_queue(response_queue, durable=False, exclusive=False, auto_delete=True)
            logger.debug(f"Declared response queue {response_queue}")
        except Exception as e:
            logger.error(f"Failed to declare response queue {response_queue}: {e}", exc_info=True)
            return False
        
        # Deduplicate results
        entry_ids = list(set(row["EntryID"] for row in results))
        existing_keys = set()
        if entry_ids:
            placeholders = ",".join([f":id{i}" for i in range(len(entry_ids))])
            select_query = f"""
                SELECT EntryID, ImageUrl
                FROM utb_ImageScraperResult
                WHERE EntryID IN ({placeholders})
            """
            params = {f"id{i}": entry_id for i, entry_id in enumerate(entry_ids)}
            try:
                await enqueue_db_update(
                    file_id=file_id,
                    sql=select_query,
                    params=params,
                    background_tasks=background_tasks,
                    task_type="select_deduplication",
                    producer=producer,
                    response_queue=response_queue,
                    correlation_id=correlation_id,
                    return_result=True,
                    logger=logger
                )
                logger.info(f"Worker PID {process.pid}: Enqueued SELECT query for {len(entry_ids)} EntryIDs")
            except Exception as e:
                logger.error(f"Failed to enqueue deduplication query: {e}", exc_info=True)
                return False

            response_received = asyncio.Event()
            response_data = None

            @retry(
                stop=stop_after_attempt(3),
                wait=wait_exponential(multiplier=1, min=2, max=10),
                retry=retry_if_exception_type((aio_pika.exceptions.AMQPError, asyncio.TimeoutError)),
                before_sleep=lambda retry_state: logger.info(
                    f"Retrying consume_responses for FileID {file_id} (attempt {retry_state.attempt_number}/3)"
                )
            )
            async def consume_responses(queue):
                nonlocal response_data
                try:
                    async with queue.iterator() as queue_iter:
                        async for message in queue_iter:
                            async with message.process():
                                if message.correlation_id == correlation_id:
                                    response_data = json.loads(message.body.decode())
                                    response_received.set()
                                    break
                except Exception as e:
                    logger.error(f"Error in consume_responses for FileID {file_id}: {e}", exc_info=True)
                    raise

            consume_task = asyncio.create_task(consume_responses(queue))
            try:
                async with asyncio.timeout(120):
                    await response_received.wait()
                if response_data and "results" in response_data:
                    existing_keys = {(row["EntryID"], row["ImageUrl"]) for row in response_data["results"]}
                    logger.info(f"Worker PID {process.pid}: Received {len(existing_keys)} deduplication results")
                else:
                    logger.warning(f"Worker PID {process.pid}: No deduplication results received")
                    return False
            except asyncio.TimeoutError:
                logger.warning(f"Worker PID {process.pid}: Timeout waiting for SELECT results")
                return False
            finally:
                consume_task.cancel()
                await asyncio.sleep(0.1)

        # Prepare update and insert queries
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
        create_time = datetime.datetime.now()
        for row in results:
            key = (row["EntryID"], row["ImageUrl"])
            params = {
                "EntryID": row["EntryID"],
                "ImageUrl": row["ImageUrl"],
                "ImageDesc": row.get("ImageDesc", ""),
                "ImageSource": row.get("ImageSource", ""),
                "ImageUrlThumbnail": row.get("ImageUrlThumbnail", ""),
                "CreateTime": create_time
            }
            if key in existing_keys:
                update_batch.append((update_query, params))
            else:
                insert_batch.append((insert_query, params))

        # Enqueue updates and inserts
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
                    correlation_id=str(uuid.uuid4()),
                    logger=logger
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
                    correlation_id=str(uuid.uuid4()),
                    logger=logger
                )
        logger.info(f"Worker PID {process.pid}: Enqueued {len(insert_batch)} inserts")

        return len(insert_batch) > 0 or len(update_batch) > 0

    except Exception as e:
        logger.error(f"Worker PID {process.pid}: Error in insert_search_results for FileID {file_id}: {e}", exc_info=True)
        return False
    finally:
        try:
            if channel and not channel.is_closed:
                await channel.queue_delete(response_queue)
                logger.debug(f"Deleted response queue {response_queue}")
        except Exception as e:
            logger.warning(f"Worker PID {process.pid}: Failed to delete response queue {response_queue}: {e}")
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
        global producer
        if not producer:
            logger.error("RabbitMQ producer not initialized")
            raise ValueError("RabbitMQ producer not initialized")
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
        except Exception as e:
            logger.error(f"Worker PID {process.pid}: Error in update_search_sort_order for EntryID {entry_id}: {e}", exc_info=True)
        raise
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