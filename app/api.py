from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, APIRouter
from pydantic import BaseModel, Field
import logging
import asyncio
import os
import json
import traceback
import psutil
import datetime
import urllib.parse
import hashlib
import time
import httpx
import pandas as pd
from typing import Optional, List, Dict, Any, Callable
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from contextlib import asynccontextmanager
import signal
import uuid
import math
from collections import defaultdict
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import aio_pika
from multiprocessing import cpu_count
from math import ceil

# Assume these imports are defined in their respective modules
from icon_image_lib.google_parser import process_search_result
from common import generate_search_variations, fetch_brand_rules, preprocess_sku
from logging_config import setup_job_logger
from s3_utils import upload_file_to_space
from ai_utils import batch_vision_reason
from db_utils import (
    update_log_url_in_db,
    get_send_to_email,
    fetch_last_valid_entry,
    update_initial_sort_order,
    get_images_excel_db,
    update_file_generate_complete,
    update_file_location_complete,
)
from search_utils import update_search_sort_order, insert_search_results, update_sort_order, update_sort_no_image_entry
from database_config import async_engine
from config import BRAND_RULES_URL, VERSION, SEARCH_PROXY_API_URL
from email_utils import send_message_email
from urllib.parse import urlparse
from url_extract import extract_thumbnail_url
from rabbitmq_producer import RabbitMQProducer, enqueue_db_update

app = FastAPI(title="super_scraper", version=VERSION)

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

router = APIRouter()

JOB_STATUS = {}
LAST_UPLOAD = {}

# Global RabbitMQ producer
producer = None

class JobStatusResponse(BaseModel):
    status: str = Field(..., description="Job status (e.g., queued, running, completed, failed)")
    message: str = Field(..., description="Descriptive message about the job status")
    public_url: Optional[str] = Field(None, description="R2 URL of the generated Excel file, if available")
    log_url: Optional[str] = Field(None, description="R2 URL of the job log file, if available")
    timestamp: str = Field(..., description="ISO timestamp of the response")

@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    default_logger.info("Starting up FastAPI application")
    producer = RabbitMQProducer()
    try:
        await producer.connect()
        default_logger.info("RabbitMQ producer connected")
        loop = asyncio.get_running_loop()
        shutdown_event = asyncio.Event()

        def handle_shutdown(signal_type):
            default_logger.info(f"Received {signal_type}, initiating graceful shutdown")
            shutdown_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, handle_shutdown, sig.name)

        yield
    except Exception as e:
        default_logger.error(f"Error during application startup: {e}", exc_info=True)
        raise
    finally:
        await asyncio.wait_for(shutdown_event.wait(), timeout=None)
        default_logger.info("Shutting down FastAPI application")
        if producer:
            await producer.close()
            default_logger.info("RabbitMQ producer closed")
        await async_engine.dispose()
        default_logger.info("Database engine disposed")

app.lifespan = lifespan

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

        file_hash = await asyncio.to_thread(hashlib.md5, open(log_filename, "rb").read())
        file_hash = file_hash.hexdigest()
        current_time = time.time()
        key = (log_filename, file_id)

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
            if not upload_url:
                logger.error(f"S3 upload returned empty URL for {log_filename}")
                raise ValueError("Empty upload URL")
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

async def process_restart_batch(
    file_id_db: int,
    entry_id: Optional[int] = None,
    use_all_variations: bool = False,
    logger: Optional[logging.Logger] = None,
    background_tasks: BackgroundTasks = None,
    num_workers: int = 1,
) -> Dict[str, str]:
    log_filename = f"job_logs/job_{file_id_db}.log"
    try:
        if logger is None:
            logger, log_filename = setup_job_logger(job_id=str(file_id_db), log_dir="job_logs", console_output=True)
        logger.setLevel(logging.DEBUG)
        process = psutil.Process()
        logger.debug(f"Logger initialized for FileID: {file_id_db}")

        def log_resource_usage():
            mem_info = process.memory_info()
            cpu_percent = process.cpu_percent(interval=None)
            net_io = psutil.net_io_counters()
            logger.debug(
                f"Resources: RSS={mem_info.rss / 1024**2:.2f} MB, "
                f"CPU={cpu_percent:.1f}%, "
                f"Net Sent={net_io.bytes_sent / 1024**2:.2f} MB, "
                f"Net Recv={net_io.bytes_recv / 1024**2:.2f} MB"
            )
            if mem_info.rss / 1024**2 > 1000:
                logger.warning(f"High memory usage: RSS={mem_info.rss / 1024**2:.2f} MB")
            if cpu_percent > 80:
                logger.warning(f"High CPU usage: {cpu_percent:.1f}%")

        logger.info(f"Starting processing for FileID: {file_id_db}, Workers: {num_workers}, Use all variations: {use_all_variations}")
        log_resource_usage()

        file_id_db_int = file_id_db
        TARGET_THROUGHPUT = 50
        BATCH_TIME = 0.1
        cpu_count_val = cpu_count()
        BATCH_SIZE = max(2, min(5, ceil((TARGET_THROUGHPUT / num_workers) * BATCH_TIME * cpu_count_val / 4)))
        MAX_CONCURRENCY = min(BATCH_SIZE, cpu_count_val * 2)
        MAX_ENTRY_RETRIES = 3
        RELEVANCE_THRESHOLD = 0.9

        logger.debug(f"Config: BATCH_SIZE={BATCH_SIZE}, MAX_CONCURRENCY={MAX_CONCURRENCY}, Workers={num_workers}")

        if not producer:
            logger.error("RabbitMQ producer not initialized")
            raise ValueError("RabbitMQ producer not initialized")

        try:
            async with asyncio.timeout(10):
                await producer.connect()
        except asyncio.TimeoutError as te:
            logger.error(f"Timeout connecting to RabbitMQ: {te}", exc_info=True)
            return {"error": "Failed to connect to RabbitMQ", "log_filename": log_filename, "log_public_url": "", "last_entry_id": str(entry_id or "")}
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}", exc_info=True)
            return {"error": f"RabbitMQ connection error: {str(e)}", "log_filename": log_filename, "log_public_url": "", "last_entry_id": str(entry_id or "")}

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
                        text("SELECT MIN(EntryID) FROM utb_ImageScraperRecords WHERE FileID = :file_id AND EntryID > :entry_id AND Step1 IS NULL"),
                        {"file_id": file_id_db_int, "entry_id": entry_id}
                    )
                    next_entry = result.fetchone()
                    entry_id = next_entry[0] if next_entry and next_entry[0] else None
                    logger.debug(f"Resuming from EntryID: {entry_id}")
                    result.close()

        brand_rules = await fetch_brand_rules(BRAND_RULES_URL, max_attempts=3, timeout=10, logger=logger)
        if not brand_rules:
            logger.warning(f"No brand rules fetched for FileID {file_id_db}")
            return {"message": "Failed to fetch brand rules", "file_id": str(file_id_db), "log_filename": log_filename, "log_public_url": "", "last_entry_id": str(entry_id or "")}

        endpoint = SEARCH_PROXY_API_URL
        logger.debug(f"Using search endpoint: {endpoint}")

        async with async_engine.connect() as conn:
            query = text("""
                SELECT r.EntryID, r.ProductModel, r.ProductBrand, r.ProductColor, r.ProductCategory 
                FROM utb_ImageScraperRecords r
                LEFT JOIN utb_ImageScraperResult t ON r.EntryID = t.EntryID
                WHERE r.FileID = :file_id 
                AND (:entry_id IS NULL OR r.EntryID >= :entry_id)
                AND r.Step1 IS NULL
                AND (t.EntryID IS NULL OR t.SortOrder IS NULL OR t.SortOrder <= 0)
                ORDER BY r.EntryID
            """)
            result = await conn.execute(query, {"file_id": file_id_db_int, "entry_id": entry_id})
            entries = [(row[0], row[1], row[2], row[3], row[4]) for row in result.fetchall() if row[1] is not None]
            logger.info(f"Found {len(entries)} entries needing processing for FileID {file_id_db}")
            result.close()

        if not entries:
            logger.warning(f"No valid EntryIDs found for FileID {file_id_db}")
            return {"error": "No entries found", "log_filename": log_filename, "log_public_url": "", "last_entry_id": str(entry_id or "")}

        entry_batches = [entries[i:i + BATCH_SIZE] for i in range(0, len(entries), BATCH_SIZE)]
        logger.info(f"Created {len(entry_batches)} batches of size {BATCH_SIZE} for FileID {file_id_db}")

        successful_entries = 0
        failed_entries = 0
        last_entry_id_processed = entry_id or 0
        required_columns = ["EntryID", "ImageUrl", "ImageDesc", "ImageSource", "ImageUrlThumbnail"]

        semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
        @retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=1, max=5),
            retry=retry_if_exception_type(Exception)
        )
        async def enqueue_with_retry(sql, params, task_type, correlation_id, response_queue=None, return_result=False):
            try:
                async with asyncio.timeout(10):
                    result = await enqueue_db_update(
                        file_id=str(file_id_db),
                        sql=sql,
                        params=params,
                        background_tasks=background_tasks,
                        task_type=task_type,
                        response_queue=response_queue,
                        correlation_id=correlation_id,
                        return_result=return_result
                    )
                    return result
            except asyncio.TimeoutError as te:
                logger.error(f"Timeout enqueuing TaskType {task_type}, CorrelationID {correlation_id}: {te}", exc_info=True)
                raise
            except Exception as e:
                logger.error(f"Error enqueuing TaskType {task_type}, CorrelationID {correlation_id}: {e}", exc_info=True)
                raise

        async def process_entry(entry):
            entry_id, search_string, brand, color, category = entry
            async with semaphore:
                results_written = False
                try:
                    for attempt in range(1, MAX_ENTRY_RETRIES + 1):
                        try:
                            logger.debug(f"Processing EntryID {entry_id}, Attempt {attempt}/{MAX_ENTRY_RETRIES}")
                            search_string, brand, model, color = await preprocess_sku(
                                search_string=search_string,
                                known_brand=brand,
                                brand_rules=brand_rules,
                                logger=logger
                            )
                            search_terms_dict = await generate_search_variations(search_string, brand, logger=logger)
                            variation_order = [
                                "default", "brand_alias", "model_alias", "delimiter_variations",
                                "color_variations", "no_color", "category_specific"
                            ]
                            search_terms = []
                            for variation_type in variation_order:
                                if variation_type in search_terms_dict:
                                    search_terms.extend(search_terms_dict[variation_type])
                            search_terms = [term.lower().strip() for term in search_terms]
                            logger.debug(f"Generated {len(search_terms)} search terms for EntryID {entry_id}")

                            if not search_terms:
                                logger.warning(f"No search terms for EntryID {entry_id}")
                                return entry_id, False

                            client = SearchClient(endpoint, logger, max_concurrency=1)
                            try:
                                for term in search_terms:
                                    logger.debug(f"Searching term '{term}' for EntryID {entry_id}")
                                    async with asyncio.timeout(30):
                                        results = await client.search(term, brand, entry_id)
                                    if isinstance(results, Exception):
                                        logger.error(f"Search failed for term '{term}' in EntryID {entry_id}: {results}", exc_info=True)
                                        continue
                                    if not results:
                                        logger.debug(f"No results for term '{term}' in EntryID {entry_id}")
                                        continue
                                    processed_results = await process_results(results, entry_id, brand, term, logger)
                                    valid_results = [
                                        res for res in processed_results
                                        if all(col in res for col in required_columns) and
                                        not res["ImageUrl"].startswith("placeholder://")
                                    ]
                                    rejected_results = [
                                        res for res in processed_results
                                        if res not in valid_results
                                    ]
                                    if rejected_results:
                                        logger.debug(f"Rejected {len(rejected_results)} results for EntryID {entry_id}")

                                    if not valid_results:
                                        logger.debug(f"No valid results for term '{term}' in EntryID {entry_id}")
                                        continue

                                    try:
                                        async with asyncio.timeout(30):
                                            success = await insert_search_results(
                                                results=valid_results,
                                                logger=logger,
                                                file_id=str(file_id_db),
                                                background_tasks=background_tasks
                                            )
                                        if success:
                                            results_written = True
                                            logger.info(f"Inserted {len(valid_results)} results for EntryID {entry_id}")
                                        else:
                                            logger.warning(f"Failed to insert results for EntryID {entry_id}")
                                    except asyncio.TimeoutError as te:
                                        logger.error(f"Timeout inserting results for EntryID {entry_id}: {te}", exc_info=True)
                                        continue
                                    except Exception as e:
                                        logger.error(f"Error inserting results for EntryID {entry_id}: {e}", exc_info=True)
                                        continue

                                    if valid_results and not use_all_variations:
                                        break

                                if results_written:
                                    try:
                                        async with asyncio.timeout(30):
                                            sort_results = await update_search_sort_order(
                                                file_id=str(file_id_db),
                                                entry_id=str(entry_id),
                                                brand=brand,
                                                model=search_string,
                                                color=color,
                                                category=category,
                                                logger=logger,
                                                brand_rules=brand_rules,
                                                background_tasks=background_tasks
                                            )
                                        if sort_results:
                                            logger.debug(f"Sort order updated for EntryID {entry_id}")
                                        else:
                                            logger.warning(f"Search sort failed for EntryID {entry_id}")
                                    except asyncio.TimeoutError as te:
                                        logger.error(f"Timeout updating search sort for EntryID {entry_id}: {te}", exc_info=True)
                                        continue
                                    except Exception as e:
                                        logger.error(f"Error updating search sort for EntryID {entry_id}: {e}", exc_info=True)
                                        continue

                                    sql = """
                                        SELECT ResultID, AiJson
                                        FROM utb_ImageScraperResult
                                        WHERE EntryID = :entry_id AND SortOrder > 0
                                    """
                                    params = {"entry_id": entry_id}
                                    correlation_id = str(uuid.uuid4())
                                    try:
                                        async with asyncio.timeout(10):
                                            sorted_results = await enqueue_with_retry(
                                                sql=sql,
                                                params=params,
                                                task_type="select_sorted_results",
                                                correlation_id=correlation_id,
                                                response_queue=f"response_{file_id_db}_{correlation_id}",
                                                return_result=True
                                            )
                                    except asyncio.TimeoutError as te:
                                        logger.error(f"Timeout fetching sorted results for EntryID {entry_id}: {te}", exc_info=True)
                                        continue
                                    except Exception as e:
                                        logger.error(f"Error fetching sorted results for EntryID {entry_id}: {e}", exc_info=True)
                                        continue

                                    if sorted_results:
                                        for sorted_result in sorted_results:
                                            result_id = sorted_result["ResultID"]
                                            logger.debug(f"Running AI analysis for ResultID {result_id}, EntryID {entry_id}")
                                            for ai_attempt in range(1, 3):
                                                try:
                                                    async with asyncio.timeout(60):
                                                        ai_result = await batch_vision_reason(
                                                            file_id=str(file_id_db),
                                                            entry_ids=[entry_id],
                                                            step=0,
                                                            limit=1,
                                                            concurrency=1,
                                                            logger=logger,
                                                            background_tasks=background_tasks
                                                        )
                                                    if ai_result["status_code"] != 200:
                                                        logger.warning(f"AI analysis failed for EntryID {entry_id}, ResultID {result_id}: {ai_result['message']}")
                                                        if ai_attempt < 2:
                                                            await asyncio.sleep(1)
                                                            continue
                                                        break
                                                    ai_data = json.loads(sorted_result["AiJson"]) if sorted_result["AiJson"] else {}
                                                    relevance = float(ai_data.get("scores", {}).get("relevance", 0.0))
                                                    logger.debug(f"Relevance score for ResultID {result_id}: {relevance}")
                                                    if relevance >= RELEVANCE_THRESHOLD:
                                                        logger.info(f"High-relevance image (score: {relevance}) for ResultID {result_id}, EntryID {entry_id}")
                                                        sql = """
                                                            UPDATE utb_ImageScraperResult
                                                            SET SortOrder = :sort_order
                                                            WHERE ResultID = :result_id
                                                        """
                                                        params = {"result_id": result_id, "sort_order": 1}
                                                        await enqueue_with_retry(
                                                            sql=sql,
                                                            params=params,
                                                            task_type="update_sort_order",
                                                            correlation_id=str(uuid.uuid4())
                                                        )
                                                    break
                                                except asyncio.TimeoutError as te:
                                                    logger.error(f"Timeout in AI analysis for EntryID {entry_id}, ResultID {result_id}, attempt {ai_attempt}: {te}", exc_info=True)
                                                    if ai_attempt < 2:
                                                        await asyncio.sleep(1)
                                                        continue
                                                    break
                                                except Exception as e:
                                                    logger.error(f"AI analysis error for EntryID {entry_id}, ResultID {result_id}, attempt {ai_attempt}: {e}", exc_info=True)
                                                    if ai_attempt < 2:
                                                        await asyncio.sleep(1)
                                                        continue
                                                    break

                                    sql = "UPDATE utb_ImageScraperRecords SET Step1 = GETDATE() WHERE EntryID = :entry_id"
                                    params = {"entry_id": entry_id}
                                    await enqueue_with_retry(
                                        sql=sql,
                                        params=params,
                                        task_type="update_step1",
                                        correlation_id=str(uuid.uuid4())
                                    )
                                    return entry_id, True
                            finally:
                                await client.close()
                        except asyncio.TimeoutError as te:
                            logger.error(f"Timeout processing EntryID {entry_id} on attempt {attempt}: {te}", exc_info=True)
                            if attempt < MAX_ENTRY_RETRIES:
                                await asyncio.sleep(2 ** attempt)
                            continue
                        except Exception as e:
                            logger.error(f"Error processing EntryID {entry_id} on attempt {attempt}: {e}", exc_info=True)
                            if attempt < MAX_ENTRY_RETRIES:
                                await asyncio.sleep(2 ** attempt)
                            continue
                    logger.warning(f"Failed to process EntryID {entry_id} after {MAX_ENTRY_RETRIES} attempts")
                    if not results_written:
                        sql = "UPDATE utb_ImageScraperRecords SET Step1 = NULL WHERE EntryID = :entry_id"
                        params = {"entry_id": entry_id}
                        await enqueue_with_retry(
                            sql=sql,
                            params=params,
                            task_type="reset_step1_failed",
                            correlation_id=str(uuid.uuid4())
                        )
                    return entry_id, results_written
                except Exception as e:
                    logger.error(f"Unexpected error processing EntryID {entry_id}: {e}", exc_info=True)
                    return entry_id, results_written

        for batch_idx, batch_entries in enumerate(entry_batches, 1):
            logger.info(f"Processing batch {batch_idx}/{len(entry_batches)} for FileID {file_id_db}")
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

            async with async_engine.connect() as conn:
                result = await conn.execute(
                    text("""
                        SELECT COUNT(*) 
                        FROM utb_ImageScraperRecords 
                        WHERE FileID = :file_id AND Step1 IS NULL
                    """),
                    {"file_id": file_id_db_int}
                )
                remaining_entries = result.fetchone()[0]
                result.close()
                if remaining_entries == 0:
                    logger.info(f"All entries processed for FileID {file_id_db}")
                    break

            elapsed_time = (datetime.datetime.now() - start_time).total_seconds()
            logger.info(f"Completed batch {batch_idx} in {elapsed_time:.2f}s ({len(batch_entries) / elapsed_time:.2f} entries/s)")
            log_resource_usage()
            await asyncio.sleep(0.5)

        if successful_entries > 0:
            logger.info(f"Updating ImageCompleteTime for FileID {file_id_db}")
            sql = """
                UPDATE utb_ImageScraperFiles
                SET ImageCompleteTime = GETDATE()
                WHERE ID = :file_id
            """
            params = {"file_id": file_id_db_int}
            await enqueue_with_retry(
                sql=sql,
                params=params,
                task_type="update_file_image_complete_time",
                correlation_id=str(uuid.uuid4())
            )

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
                f"{positive_entries} with positive SortOrder, {null_entries} with NULL SortOrder for FileID {file_id_db}"
            )
            result.close()

        to_emails = await get_send_to_email(file_id_db, logger=logger)
        if to_emails:
            subject = f"Processing Completed for FileID: {file_id_db}"
            message = (
                f"Processing for FileID {file_id_db} completed.\n"
                f"Successful entries: {successful_entries}/{len(entries)}\n"
                f"Failed entries: {failed_entries}\n"
                f"Last EntryID: {last_entry_id_processed}\n"
                f"Log file: {log_filename}\n"
                f"Used all variations: {use_all_variations}\n"
                f"Workers: {num_workers}"
            )
            await send_message_email(to_emails, subject=subject, message=message, logger=logger)

        log_public_url = await upload_file_to_space(
            file_src=log_filename,
            save_as=f"job_logs/job_{file_id_db}.log",
            is_public=True,
            logger=logger,
            file_id=str(file_id_db)
        )

        logger.info(f"Starting search sort for FileID: {file_id_db}")
        try:
            sort_result = await update_sort_order(str(file_id_db), logger=logger, background_tasks=background_tasks)
            logger.info(f"Search sort completed for FileID {file_id_db}. Result: {sort_result}")
        except Exception as e:
            logger.error(f"Error running search sort for FileID {file_id_db}: {e}", exc_info=True)
            sort_result = {"status_code": 500, "message": str(e)}

        logger.info(f"Queuing download file generation for FileID: {file_id_db}")
        try:
            if background_tasks is None:
                background_tasks = BackgroundTasks()
            await run_generate_download_file(str(file_id_db), logger, log_filename, background_tasks)
            logger.info(f"Download file generation queued for FileID {file_id_db}")
            if to_emails:
                subject = f"File Generation Queued for FileID: {file_id_db}"
                message = (
                    f"Excel file generation for FileID {file_id_db} has been queued.\n"
                    f"Batch processing results:\n"
                    f"Successful entries: {successful_entries}/{len(entries)}\n"
                    f"Failed entries: {failed_entries}\n"
                    f"Last EntryID: {last_entry_id_processed}\n"
                    f"Log file: {log_filename}\n"
                    f"Log URL: {log_public_url or 'Not available'}\n"
                    f"Used all variations: {use_all_variations}\n"
                    f"Workers: {num_workers}"
                )
                await send_message_email(to_emails, subject=subject, message=message, logger=logger)
        except Exception as e:
            logger.error(f"Error queuing download file generation for FileID {file_id_db}: {e}", exc_info=True)
            if to_emails:
                subject = f"File Generation Failed for FileID: {file_id_db}"
                message = (
                    f"Excel file generation for FileID {file_id_db} failed.\n"
                    f"Error: {str(e)}\n"
                    f"Batch processing results:\n"
                    f"Successful entries: {successful_entries}/{len(entries)}\n"
                    f"Failed entries: {failed_entries}\n"
                    f"Last EntryID: {last_entry_id_processed}\n"
                    f"Search sort status: {'Success' if sort_result.get('status_code') == 200 else 'Failed'}\n"
                    f"Log file: {log_filename}\n"
                    f"Log URL: {log_public_url or 'Not available'}\n"
                    f"Used all variations: {use_all_variations}\n"
                    f"Workers: {num_workers}"
                )
                await send_message_email(to_emails, subject=subject, message=message, logger=logger)

        return {
            "message": "Search processing completed",
            "file_id": str(file_id_db),
            "successful_entries": str(successful_entries),
            "total_entries": str(len(entries)),
            "failed_entries": str(failed_entries),
            "log_filename": log_filename,
            "log_public_url": log_public_url or "",
            "last_entry_id": str(last_entry_id_processed),
            "use_all_variations": str(use_all_variations),
            "num_workers": str(num_workers)
        }
    except Exception as e:
        logger.error(f"Error processing FileID {file_id_db}: {e}", exc_info=True)
        log_public_url = await upload_file_to_space(
            file_src=log_filename,
            save_as=f"job_logs/job_{file_id_db}.log",
            is_public=True,
            logger=logger,
            file_id=str(file_id_db)
        )
        return {"error": str(e), "log_filename": log_filename, "log_public_url": log_public_url or "", "last_entry_id": str(entry_id or "")}
    finally:
        await async_engine.dispose()
        logger.info(f"Disposed database engines for FileID {file_id_db}")

@router.post("/clear-ai-json/{file_id}", tags=["Database"])
async def api_clear_ai_json(
    file_id: str,
    entry_ids: Optional[List[int]] = Query(None, description="List of EntryIDs to clear AI data for, if not all"),
    background_tasks: BackgroundTasks = None
):
    logger, log_filename = setup_job_logger(job_id=file_id, console_output=True)
    logger.info(f"Clearing AiJson and AiCaption for FileID: {file_id}" + (f", EntryIDs: {entry_ids}" if entry_ids else ""))

    try:
        async with async_engine.connect() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) FROM utb_ImageScraperFiles WHERE ID = :file_id"),
                {"file_id": int(file_id)}
            )
            if result.fetchone()[0] == 0:
                logger.error(f"FileID {file_id} does not exist")
                log_public_url = await upload_log_file(file_id, log_filename, logger)
                raise HTTPException(status_code=404, detail=f"FileID {file_id} not found")
            result.close()

        sql = """
            UPDATE utb_ImageScraperResult
            SET AiJson = NULL, AiCaption = NULL
            FROM utb_ImageScraperResult t
            INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
            WHERE r.FileID = :file_id
            AND (t.AiJson IS NOT NULL OR t.AiCaption IS NOT NULL)
        """
        params = {"file_id": int(file_id)}
        if entry_ids:
            sql += " AND t.EntryID IN :entry_ids"
            params["entry_ids"] = tuple(entry_ids)

        if not producer:
            logger.error("RabbitMQ producer not initialized")
            raise ValueError("RabbitMQ producer not initialized")

        correlation_id = str(uuid.uuid4())
        rows_affected = await enqueue_db_update(
            file_id=file_id,
            sql=sql,
            params=params,
            background_tasks=background_tasks,
            task_type="clear_ai_json",
            correlation_id=correlation_id,
            return_result=True
        )
        logger.info(f"Enqueued AiJson and AiCaption clear for FileID: {file_id}, CorrelationID: {correlation_id}, Rows affected: {rows_affected}")

        verify_sql = """
            SELECT COUNT(*) 
            FROM utb_ImageScraperResult t
            INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
            WHERE r.FileID = :file_id 
            AND (t.AiJson IS NOT NULL OR t.AiCaption IS NOT NULL)
        """
        verify_params = {"file_id": int(file_id)}
        if entry_ids:
            verify_sql += " AND t.EntryID IN :entry_ids"
            verify_params["entry_ids"] = tuple(entry_ids)

        verify_correlation_id = str(uuid.uuid4())
        await enqueue_db_update(
            file_id=file_id,
            sql=verify_sql,
            params=verify_params,
            background_tasks=background_tasks,
            task_type="verify_clear_ai_json",
            correlation_id=verify_correlation_id,
            return_result=True
        )
        logger.info(f"Enqueued verification for FileID: {file_id}, CorrelationID: {verify_correlation_id}")

        log_public_url = await upload_log_file(file_id, log_filename, logger)
        return {
            "status": "success",
            "status_code": 200,
            "message": f"Enqueued AiJson and AiCaption clear for {rows_affected} rows for FileID: {file_id}",
            "log_url": log_public_url,
            "data": {
                "file_id": file_id,
                "entry_ids": entry_ids,
                "rows_affected": rows_affected,
                "correlation_id": correlation_id
            }
        }
    except Exception as e:
        logger.error(f"Error clearing AiJson and AiCaption for FileID {file_id}: {e}", exc_info=True)
        log_public_url = await upload_log_file(file_id, log_filename, logger)
        raise HTTPException(status_code=500, detail=f"Error clearing AiJson and AiCaption for FileID {file_id}: {str(e)}")
    finally:
        await async_engine.dispose()
        logger.info(f"Disposed database engine for FileID {file_id}")

# Update other endpoints similarly, removing local producer instances
@router.post("/validate-images/{file_id}", tags=["Validation"])
async def api_validate_images(
    file_id: str,
    entry_ids: Optional[List[int]] = Query(None, description="List of EntryIDs to validate, if not all"),
    concurrency: int = Query(10, description="Maximum concurrent image download tasks"),
    background_tasks: BackgroundTasks = None
):
    logger, log_filename = setup_job_logger(job_id=file_id, console_output=True)
    logger.info(f"Validating images for FileID: {file_id}, EntryIDs: {entry_ids}, Concurrency: {concurrency}")

    try:
        async with async_engine.connect() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) FROM utb_ImageScraperFiles WHERE ID = :file_id"),
                {"file_id": int(file_id)}
            )
            if result.fetchone()[0] == 0:
                logger.error(f"FileID {file_id} does not exist")
                log_public_url = await upload_log_file(file_id, log_filename, logger)
                raise HTTPException(status_code=404, detail=f"FileID {file_id} not found")
            result.close()

        query = """
            SELECT ResultID, EntryID, ImageUrl, ImageUrlThumbnail
            FROM utb_ImageScraperResult
            WHERE EntryID IN (
                SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = :file_id
            )
            AND ImageUrl IS NOT NULL
            AND SortOrder >= 0
        """
        params = {"file_id": int(file_id)}
        if entry_ids:
            query += " AND EntryID IN :entry_ids"
            params["entry_ids"] = tuple(entry_ids)

        async with async_engine.connect() as conn:
            result = await conn.execute(text(query), params)
            results = [
                {
                    "ResultID": row[0],
                    "EntryID": row[1],
                    "ImageUrl": row[2],
                    "ImageUrlThumbnail": row[3]
                }
                for row in result.fetchall()
            ]
            result.close()

        if not results:
            logger.warning(f"No valid images found for FileID {file_id}" + (f", EntryIDs {entry_ids}" if entry_ids else ""))
            log_public_url = await upload_log_file(file_id, log_filename, logger)
            return {
                "status": "success",
                "status_code": 200,
                "message": "No valid images found to validate",
                "log_url": log_public_url,
                "data": {"validated": 0, "invalid": 0}
            }

        logger.info(f"Found {len(results)} images to validate for FileID {file_id}")

        if not producer:
            logger.error("RabbitMQ producer not initialized")
            raise ValueError("RabbitMQ producer not initialized")

        semaphore = asyncio.Semaphore(concurrency)
        async def validate_image(result: dict) -> dict:
            async with semaphore:
                result_id = result["ResultID"]
                entry_id = result["EntryID"]
                image_url = result["ImageUrl"]
                thumbnail_url = result["ImageUrlThumbnail"]
                logger.debug(f"Validating ResultID {result_id}, EntryID {entry_id}, ImageUrl: {image_url}")

                async with httpx.AsyncClient(timeout=10.0) as client:
                    is_valid = False
                    try:
                        response = await client.get(image_url, follow_redirects=True)
                        if response.status_code == 200 and "image" in response.headers.get("content-type", "").lower():
                            logger.debug(f"Valid image for ResultID {result_id}: {image_url}")
                            is_valid = True
                        else:
                            logger.warning(f"Invalid image for ResultID {result_id}: Status {response.status_code}")
                    except Exception as e:
                        logger.warning(f"Failed to download image for ResultID {result_id}: {e}")

                    if not is_valid and thumbnail_url and thumbnail_url != image_url:
                        try:
                            response = await client.get(thumbnail_url, follow_redirects=True)
                            if response.status_code == 200 and "image" in response.headers.get("content-type", "").lower():
                                logger.debug(f"Valid thumbnail for ResultID {result_id}: {thumbnail_url}")
                                is_valid = True
                            else:
                                logger.warning(f"Invalid thumbnail for ResultID {result_id}: Status {response.status_code}")
                        except Exception as e:
                            logger.warning(f"Failed to download thumbnail for ResultID {result_id}: {e}")

                    if not is_valid:
                        sql = """
                            UPDATE utb_ImageScraperResult
                            SET SortOrder = -5
                            WHERE ResultID = :result_id
                        """
                        params = {"result_id": result_id}
                        correlation_id = str(uuid.uuid4())
                        await enqueue_db_update(
                            file_id=file_id,
                            sql=sql,
                            params=params,
                            background_tasks=background_tasks,
                            task_type="mark_invalid_image",
                            correlation_id=correlation_id
                        )
                        logger.info(f"Enqueued SortOrder=-5 for ResultID {result_id}, EntryID {entry_id}, CorrelationID: {correlation_id}")
                        return {"ResultID": result_id, "EntryID": entry_id, "valid": False}
                    return {"ResultID": result_id, "EntryID": entry_id, "valid": True}

        validation_results = await asyncio.gather(
            *(validate_image(result) for result in results),
            return_exceptions=True
        )

        validated_count = 0
        invalid_count = 0
        for res in validation_results:
            if isinstance(res, Exception):
                logger.error(f"Error validating image: {res}", exc_info=True)
                invalid_count += 1
                continue
            if res["valid"]:
                validated_count += 1
            else:
                invalid_count += 1

        logger.info(f"Validation complete for FileID {file_id}: {validated_count} valid, {invalid_count} invalid")

        if background_tasks:
            sql = """
                SELECT ResultID, EntryID, SortOrder
                FROM utb_ImageScraperResult
                WHERE EntryID IN (
                    SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = :file_id
                )
                AND SortOrder = -5
            """
            params = {"file_id": int(file_id)}
            correlation_id = str(uuid.uuid4())
            await enqueue_db_update(
                file_id=file_id,
                sql=sql,
                params=params,
                background_tasks=background_tasks,
                task_type="verify_invalid_images",
                correlation_id=correlation_id
            )
            logger.info(f"Enqueued verification of invalid images for FileID {file_id}, CorrelationID: {correlation_id}")

        log_public_url = await upload_log_file(file_id, log_filename, logger)
        return {
            "status": "success",
            "status_code": 200,
            "message": f"Validated {len(results)} images for FileID {file_id}: {validated_count} valid, {invalid_count} invalid",
            "log_url": log_public_url,
            "data": {
                "validated": validated_count,
                "invalid": invalid_count,
                "results": [res for res in validation_results if not isinstance(res, Exception)]
            }
        }
    except Exception as e:
        logger.error(f"Error validating images for FileID {file_id}: {e}", exc_info=True)
        log_public_url = await upload_log_file(file_id, log_filename, logger)
        raise HTTPException(status_code=500, detail=f"Error validating images for FileID {file_id}: {str(e)}")
    finally:
        await async_engine.dispose()
        logger.info(f"Disposed database engine for FileID {file_id}")

@router.post("/reset-step1-no-results/{file_id}", tags=["Database"])
async def api_reset_step1_no_results(
    file_id: str,
    entry_ids: Optional[List[int]] = Query(None, description="List of EntryIDs to check and reset Step1 for, if not all"),
    background_tasks: BackgroundTasks = None
):
    logger, log_filename = setup_job_logger(job_id=file_id, console_output=True)
    logger.info(f"Resetting Step1 for entries with no results for FileID: {file_id}" + (f", EntryIDs: {entry_ids}" if entry_ids else ""))

    try:
        async with async_engine.connect() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) FROM utb_ImageScraperFiles WHERE ID = :file_id"),
                {"file_id": int(file_id)}
            )
            if result.fetchone()[0] == 0:
                logger.error(f"FileID {file_id} does not exist")
                log_public_url = await upload_file_to_space(
                    file_src=log_filename,
                    save_as=f"job_logs/job_{file_id}.log",
                    is_public=True,
                    logger=logger,
                    file_id=file_id
                )
                raise HTTPException(status_code=404, detail=f"FileID {file_id} not found")
            result.close()

        query = """
            SELECT r.EntryID
            FROM utb_ImageScraperRecords r
            LEFT JOIN utb_ImageScraperResult t ON r.EntryID = t.EntryID
            WHERE r.FileID = :file_id
            AND t.EntryID IS NULL
        """
        params = {"file_id": int(file_id)}
        if entry_ids:
            placeholders = ",".join(f":entry_id_{i}" for i in range(len(entry_ids)))
            query += f" AND r.EntryID IN ({placeholders})"
            for i, entry_id in enumerate(entry_ids):
                params[f"entry_id_{i}"] = entry_id

        async with async_engine.connect() as conn:
            result = await conn.execute(text(query), params)
            entries_to_reset = [row[0] for row in result.fetchall()]
            result.close()

        if not entries_to_reset:
            logger.info(f"No entries with zero results found for FileID: {file_id}")
            log_public_url = await upload_file_to_space(
                file_src=log_filename,
                save_as=f"job_logs/job_{file_id}.log",
                is_public=True,
                logger=logger,
                file_id=file_id
            )
            return {
                "status": "success",
                "status_code": 200,
                "message": "No entries with zero results found to reset",
                "log_url": log_public_url,
                "data": {"entries_reset": 0, "entry_ids": []}
            }

        logger.info(f"Found {len(entries_to_reset)} entries with zero results to reset Step1 for FileID: {file_id}: {entries_to_reset}")

        if not producer:
            logger.error("RabbitMQ producer not initialized")
            raise ValueError("RabbitMQ producer not initialized")

        sql = """
            UPDATE utb_ImageScraperRecords
            SET Step1 = NULL
            WHERE EntryID = :entry_id
        """
        correlation_id = str(uuid.uuid4())
        rows_affected = 0
        for entry_id in entries_to_reset:
            params = {"entry_id": entry_id}
            result = await enqueue_db_update(
                file_id=file_id,
                sql=sql,
                params=params,
                background_tasks=background_tasks,
                task_type="reset_step1_no_results",
                correlation_id=correlation_id,
                return_result=True
            )
            rows_affected += result or 0
            logger.debug(f"Enqueued Step1 reset for EntryID: {entry_id}, CorrelationID: {correlation_id}")

        async def verify_updates():
            async with async_engine.connect() as conn:
                verify_query = """
                    SELECT r.EntryID
                    FROM utb_ImageScraperRecords r
                    LEFT JOIN utb_ImageScraperResult t ON r.EntryID = t.EntryID
                    WHERE r.FileID = :file_id
                    AND r.Step1 IS NULL
                    AND t.EntryID IS NULL
                """
                verify_params = {"file_id": int(file_id)}
                if entry_ids:
                    placeholders = ",".join(f":entry_id_{i}" for i in range(len(entry_ids)))
                    verify_query += f" AND r.EntryID IN ({placeholders})"
                    for i, entry_id in enumerate(entry_ids):
                        verify_params[f"entry_id_{i}"] = entry_id
                result = await conn.execute(text(verify_query), verify_params)
                verified_entries = [row[0] for row in result.fetchall()]
                result.close()
                if set(entries_to_reset).issubset(set(verified_entries)):
                    logger.info(f"Verified {len(verified_entries)} entries with Step1 = NULL: {verified_entries}")
                    return verified_entries
                logger.warning(f"Verification failed: Expected {entries_to_reset}, got {verified_entries}")
                raise Exception(f"Verification failed: Expected {entries_to_reset}, got {verified_entries}")

        verified_entries = await verify_updates()

        log_public_url = await upload_file_to_space(
            file_src=log_filename,
            save_as=f"job_logs/job_{file_id}.log",
            is_public=True,
            logger=logger,
            file_id=file_id
        )
        return {
            "status": "success",
            "status_code": 200,
            "message": f"Enqueued Step1 reset for {rows_affected} entries with no results for FileID: {file_id}",
            "log_url": log_public_url,
            "data": {
                "file_id": file_id,
                "entries_reset": rows_affected,
                "entry_ids": entries_to_reset,
                "verified_entries": verified_entries,
                "correlation_id": correlation_id
            }
        }
    except Exception as e:
        logger.error(f"Error resetting Step1 for FileID {file_id}: {e}", exc_info=True)
        log_public_url = await upload_file_to_space(
            file_src=log_filename,
            save_as=f"job_logs/job_{file_id}.log",
            is_public=True,
            logger=logger,
            file_id=file_id
        )
        raise HTTPException(status_code=500, detail=f"Error resetting Step1 for FileID {file_id}: {str(e)}")
    finally:
        await async_engine.dispose()
        logger.info(f"Disposed database engine for FileID {file_id}")

app.include_router(router, prefix="/api/v4")