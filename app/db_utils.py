import logging
import pandas as pd
import pyodbc
import asyncio
import json
import datetime,uuid
import aio_pika
import os,aiormq
import aiofiles
import psutil
from fastapi import BackgroundTasks
from typing import Optional, List, Dict
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from rabbitmq_producer import RabbitMQProducer
from rabbitmq_consumer import RabbitMQConsumer
from database_config import conn_str, async_engine
from typing import Any
from common import clean_string, normalize_model, generate_aliases,validate_thumbnail_url,clean_url_string
from s3_utils import upload_file_to_space
producer = None
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

async def get_records_to_search(file_id: str, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        async with async_engine.connect() as conn:
            query = text("""
                SELECT EntryID, ProductModel AS SearchString, 'model_only' AS SearchType, FileID
                FROM utb_ImageScraperRecords 
                WHERE FileID = :file_id AND Step1 IS NULL
                ORDER BY EntryID, SearchType
            """)
            logger.debug(f"Executing query: {query} with FileID: {file_id}")
            result = await conn.execute(query, {"file_id": file_id})
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            result.close()
            if not df.empty and (df["FileID"] != file_id).any():
                logger.error(f"Found rows with incorrect FileID for {file_id}")
                df = df[df["FileID"] == file_id]
            logger.info(f"Got {len(df)} search records for FileID: {file_id}")
            return df[["EntryID", "SearchString", "SearchType"]]
    except Exception as e:
        logger.error(f"Error getting records for FileID {file_id}: {e}", exc_info=True)
        return pd.DataFrame()

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(pyodbc.Error),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying get_images_excel_db for FileID {retry_state.kwargs['file_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def get_images_excel_db(file_id: str, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    logger = logger or default_logger
    expected_columns = ["ExcelRowID", "ImageUrl", "ImageUrlThumbnail", "Brand", "Style", "Color", "Category"]
    
    try:
        file_id = int(file_id)
        async with async_engine.connect() as conn:
            query = text("""
                SELECT DISTINCT
                    CAST(s.ExcelRowID AS INT) AS ExcelRowID,
                    ISNULL(r.ImageUrl, '') AS ImageUrl,
                    ISNULL(r.ImageUrlThumbnail, '') AS ImageUrlThumbnail,
                    ISNULL(s.ProductBrand, '') AS Brand,
                    ISNULL(s.ProductModel, '') AS Style,
                    ISNULL(s.ProductColor, '') AS Color,
                    ISNULL(s.ProductCategory, '') AS Category
                FROM utb_ImageScraperFiles f
                INNER JOIN utb_ImageScraperRecords s ON s.FileID = f.ID
                LEFT JOIN (
                    SELECT EntryID, ImageUrl, ImageUrlThumbnail
                    FROM utb_ImageScraperResult
                    WHERE SortOrder = 1
                ) r ON r.EntryID = s.EntryID
                WHERE f.ID = :file_id
                ORDER BY ExcelRowID
            """)
            logger.debug(f"Executing query: {query} with ID: {file_id}")
            result = await conn.execute(query, {"file_id": file_id})
            rows = result.fetchall()
            columns = result.keys()
            result.close()

        if not rows:
            logger.warning(f"No valid rows returned for ID {file_id}")
            return pd.DataFrame(columns=expected_columns)

        df = pd.DataFrame(rows, columns=columns)
        if list(df.columns) != expected_columns:
            logger.error(f"Invalid columns returned for ID {file_id}. Got: {list(df.columns)}, Expected: {expected_columns}")
            return pd.DataFrame(columns=expected_columns)

        df['ExcelRowID'] = pd.to_numeric(df['ExcelRowID'], errors='coerce').astype('Int64')
        df = df[df['ImageUrl'].notnull() & (df['ImageUrl'] != '')]
        logger.info(f"Fetched {len(df)} rows for Excel export for ID {file_id}")
        
        if df.empty:
            logger.warning(f"No valid images found for ID {file_id} after filtering")
        return df
    except SQLAlchemyError as e:
        logger.error(f"Database error in get_images_excel_db for ID {file_id}: {e}", exc_info=True)
        return pd.DataFrame(columns=expected_columns)
    except Exception as e:
        logger.error(f"Unexpected error in get_images_excel_db for ID {file_id}: {e}", exc_info=True)
        return pd.DataFrame(columns=expected_columns)

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(SQLAlchemyError),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying fetch_last_valid_entry for FileID {retry_state.kwargs['file_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def fetch_last_valid_entry(file_id: str, logger: Optional[logging.Logger] = None) -> Optional[int]:
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        async with async_engine.connect() as conn:
            query = text("""
                SELECT MAX(t.EntryID)
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                WHERE r.FileID = :file_id AND t.SortOrder IS NOT NULL
            """)
            logger.debug(f"Executing query: {query} with FileID: {file_id}")
            result = await conn.execute(query, {"file_id": file_id})
            row = result.fetchone()
            result.close()
            if row and row[0]:
                logger.info(f"Last valid EntryID for FileID {file_id}: {row[0]}")
                return row[0]
            logger.info(f"No valid EntryIDs found for FileID {file_id}")
            return None
    except Exception as e:
        logger.error(f"Error fetching last valid EntryID for FileID {file_id}: {e}", exc_info=True)
        return None

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(pyodbc.Error),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying update_file_location_complete for FileID {retry_state.kwargs['file_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def update_file_location_complete(file_id: str, file_location: str, logger: Optional[logging.Logger] = None) -> None:
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE utb_ImageScraperFiles SET FileLocationURLComplete = ? WHERE ID = ?", (file_location, file_id))
            conn.commit()
            cursor.close()
            logger.info(f"Updated file location for FileID: {file_id}")
    except pyodbc.Error as e:
        logger.error(f"Database error in update_file_location_complete: {e}", exc_info=True)
        raise
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")
        raise

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(pyodbc.Error),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying update_file_generate_complete for FileID {retry_state.kwargs['file_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def update_file_generate_complete(file_id: str, logger: Optional[logging.Logger] = None) -> None:
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE utb_ImageScraperFiles SET CreateFileCompleteTime = GETDATE() WHERE ID = ?", (file_id,))
            conn.commit()
            cursor.close()
            logger.info(f"Marked file generation complete for FileID: {file_id}")
    except pyodbc.Error as e:
        logger.error(f"Database error in update_file_generate_complete: {e}", exc_info=True)
        raise
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")
        raise
import signal
import asyncio

import signal
import asyncio

def register_shutdown_handler(producer, consumer, logger, correlation_id):
    async def handle_shutdown():
        try:
            tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
            for task in tasks:
                task.cancel()
            if consumer:
                await consumer.close()
                logger.debug(f"[{correlation_id}] Closed RabbitMQ consumer on shutdown")
            if producer:
                await producer.close()
                logger.debug(f"[{correlation_id}] Closed RabbitMQ producer on shutdown")
        except Exception as e:
            logger.error(f"[{correlation_id}] Error during shutdown: {e}", exc_info=True)

    def shutdown_callback():
        asyncio.create_task(handle_shutdown())

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown_callback)
from tenacity import retry_if_exception
import logging
import pandas as pd
import pyodbc
import asyncio
import json
import datetime
import uuid
import aio_pika
import os
import aiormq
import psutil
from fastapi import BackgroundTasks
from typing import Optional, List, Dict
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from rabbitmq_producer import RabbitMQProducer
from rabbitmq_consumer import RabbitMQConsumer
from database_config import conn_str, async_engine
from typing import Any
from common import clean_string, clean_url_string, validate_thumbnail_url


def flatten_entry_ids(entry_ids, logger, correlation_id):
    flat_ids = []
    for id in entry_ids:
        if isinstance(id, (list, tuple)):
            flat_ids.extend(flatten_entry_ids(id, logger, correlation_id))
        elif isinstance(id, int):
            flat_ids.append(id)
        else:
            logger.warning(f"[{correlation_id}] Invalid EntryID skipped: {id}, type: {type(id)}")
    return flat_ids

def is_retryable_exception(exception):
    return isinstance(exception, (SQLAlchemyError, aio_pika.exceptions.AMQPError, asyncio.TimeoutError)) and \
           not isinstance(exception, (aiormq.exceptions.ChannelNotFoundEntity, aiormq.exceptions.ChannelLockedResource))

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception(is_retryable_exception),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
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
    logger = logger or logging.getLogger(__name__)
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    process = psutil.Process()
    correlation_id = str(uuid.uuid4())
    logger.info(f"[{correlation_id}] Worker PID {process.pid}: Starting insert_search_results for FileID {file_id}")
    logger.debug(f"[{correlation_id}] Input results count: {len(results)}, Sample: {results[:1] if results else 'None'}")

    if not results:
        logger.warning(f"[{correlation_id}] Worker PID {process.pid}: Empty results provided")
        return False

    data = []
    errors = []

    logger.debug(f"[{correlation_id}] Processing input results")
    for res in results:
        try:
            entry_id = int(res["EntryID"])
            logger.debug(f"[{correlation_id}] EntryID: {entry_id}, type: {type(entry_id)}")
        except (ValueError, TypeError) as e:
            errors.append(f"Invalid EntryID value: {res.get('EntryID')}")
            logger.error(f"[{correlation_id}] Worker PID {process.pid}: {errors[-1]}")
            continue

        category = res.get("ProductCategory", "").lower()
        image_url = clean_url_string(res.get("ImageUrl", ""), logger=logger)
        image_url_thumbnail = clean_url_string(res.get("ImageUrlThumbnail", ""), logger=logger)
        image_desc = clean_string(res.get("ImageDesc", ""), preserve_url=False)
        image_source = clean_url_string(res.get("ImageSource", ""), logger=logger)

        logger.debug(f"[{correlation_id}] Validating ImageUrl: {image_url}")
        if not image_url or not validate_thumbnail_url(image_url, logger):
            errors.append(f"Invalid ImageUrl skipped: {image_url}")
            logger.warning(f"[{correlation_id}] Worker PID {process.pid}: {errors[-1]}")
            continue
        if image_url_thumbnail and not validate_thumbnail_url(image_url_thumbnail, logger):
            logger.debug(f"[{correlation_id}] Invalid thumbnail URL, setting to None: {image_url_thumbnail}")
            image_url_thumbnail = None

        data.append({
            "EntryID": entry_id,
            "ImageUrl": image_url,
            "ImageDesc": image_desc or None,
            "ImageSource": image_source or None,
            "ImageUrlThumbnail": image_url_thumbnail or None,
            "CreateTime": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    logger.info(f"[{correlation_id}] Valid rows to insert: {len(data)}, Errors: {len(errors)}")
    if errors:
        logger.warning(f"[{correlation_id}] Skipped rows due to errors: {errors}")

    if not data:
        logger.warning(f"[{correlation_id}] Worker PID {process.pid}: No valid rows to insert")
        return False

    producer = await RabbitMQProducer.get_producer(logger=logger)
    register_shutdown_handler(producer, None, logger, correlation_id)

    try:
        entry_ids = list(set(row["EntryID"] for row in data))
        logger.debug(f"[{correlation_id}] Raw EntryIDs: {entry_ids}")
        flat_entry_ids = flatten_entry_ids(entry_ids, logger, correlation_id)
        logger.debug(f"[{correlation_id}] Flattened EntryIDs: {flat_entry_ids}")

        if not flat_entry_ids:
            logger.warning(f"[{correlation_id}] No valid EntryIDs after flattening")
            return False

        insert_query = """
            INSERT INTO utb_ImageScraperResult (EntryID, ImageUrl, ImageDesc, ImageSource, ImageUrlThumbnail, CreateTime)
            VALUES (:EntryID, :ImageUrl, :ImageDesc, :ImageSource, :ImageUrlThumbnail, :CreateTime)
        """

        batch_size = 100
        insert_cids = []
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            for row in batch:
                cid = str(uuid.uuid4())
                insert_cids.append(cid)
                await enqueue_db_update(
                    file_id=file_id,
                    sql=insert_query,
                    params=row,
                    background_tasks=background_tasks,
                    task_type="insert",
                    producer=producer,
                    correlation_id=cid,
                    logger=logger
                )
        logger.info(f"[{correlation_id}] Enqueued {len(data)} inserts with {len(insert_cids)} correlation IDs")

        async with async_engine.connect() as conn:
            try:
                if flat_entry_ids:
                    placeholders = ",".join(f":entry_id_{i}" for i in range(len(flat_entry_ids)))
                    query = text(f"""
                        SELECT COUNT(*)
                        FROM utb_ImageScraperResult
                        WHERE EntryID IN ({placeholders}) AND CreateTime >= :create_time
                    """)
                    params = {
                        "create_time": min(row["CreateTime"] for row in data),
                        **{f"entry_id_{i}": eid for i, eid in enumerate(flat_entry_ids)}
                    }
                else:
                    query = text("""
                        SELECT COUNT(*)
                        FROM utb_ImageScraperResult
                        WHERE CreateTime >= :create_time
                    """)
                    params = {"create_time": min(row["CreateTime"] for row in data)}
                
                result = await conn.execute(query, params)
                inserted_count = result.scalar()
                logger.info(f"[{correlation_id}] Verified {inserted_count} records in database for FileID {file_id}")
            except SQLAlchemyError as e:
                logger.error(f"[{correlation_id}] Failed to verify inserted records: {e}", exc_info=True)
                inserted_count = 0

        return len(data) > 0

    except Exception as e:
        logger.error(f"[{correlation_id}] Error enqueuing results for FileID {file_id}: {e}", exc_info=True)
        raise
    finally:
        if producer:
            await producer.close()
            logger.debug(f"[{correlation_id}] Closed RabbitMQ producer")
        logger.info(f"[{correlation_id}] Worker PID {process.pid}: Completed insert_search_results for FileID {file_id}")
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(pyodbc.Error),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying get_send_to_email for FileID {retry_state.kwargs['file_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def get_send_to_email(file_id: int, logger: Optional[logging.Logger] = None) -> str:
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT UserEmail FROM utb_ImageScraperFiles WHERE ID = ?", (file_id,))
            result = cursor.fetchone()
            cursor.close()
            if result and result[0]:
                logger.info(f"Retrieved email for FileID {file_id}: {result[0]}")
                return result[0]
            logger.warning(f"No email found for FileID {file_id}")
            return "nik@accessx.com"
    except pyodbc.Error as e:
        logger.error(f"Database error fetching email for FileID {file_id}: {e}", exc_info=True)
        return "nik@accessx.com"
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")
        return "nik@accessx.com"

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(SQLAlchemyError),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying update_log_url_in_db for FileID {retry_state.kwargs['file_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def update_log_url_in_db(file_id: str, log_url: str, logger: Optional[logging.Logger] = None) -> bool:
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        async with async_engine.connect() as conn:
            await conn.execute(
                text("""
                    IF NOT EXISTS (
                        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_NAME = 'utb_ImageScraperFiles' 
                        AND COLUMN_NAME = 'LogFileUrl'
                    )
                    BEGIN
                        ALTER TABLE utb_ImageScraperFiles 
                        ADD LogFileUrl NVARCHAR(MAX)
                    END
                """)
            )
            await conn.execute(
                text("UPDATE utb_ImageScraperFiles SET LogFileUrl = :log_url WHERE ID = :file_id"),
                {"log_url": log_url, "file_id": file_id}
            )
            await conn.commit()
            logger.info(f"Updated log URL '{log_url}' for FileID {file_id}")
            return True
    except SQLAlchemyError as e:
        logger.error(f"Database error updating log URL for FileID {file_id}: {e}", exc_info=True)
        return False
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error updating log URL for FileID {file_id}: {e}", exc_info=True)
        return False

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(SQLAlchemyError),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying export_dai_json for FileID {retry_state.kwargs['file_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def export_dai_json(file_id: int, entry_ids: Optional[List[int]], logger: logging.Logger) -> str:
    try:
        json_urls = []
        async with async_engine.connect() as conn:
            query = text("""
                SELECT t.ResultID, t.EntryID, t.AiJson, t.AiCaption, t.ImageIsFashion
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                WHERE r.FileID = :file_id AND t.AiJson IS NOT NULL AND t.AiCaption IS NOT NULL
            """)
            params = {"file_id": file_id}
            if entry_ids:
                query = text(query.text + " AND t.EntryID IN :entry_ids")
                params["entry_ids"] = tuple(entry_ids)
            
            logger.debug(f"Executing query: {query} with params: {params}")
            result = await conn.execute(query, params)
            entry_results = {}
            for row in result.fetchall():
                entry_id = row[1]
                result_dict = {
                    "ResultID": row[0],
                    "EntryID": row[1],
                    "AiJson": json.loads(row[2]) if row[2] else {},
                    "AiCaption": row[3],
                    "ImageIsFashion": bool(row[4])
                }
                if entry_id not in entry_results:
                    entry_results[entry_id] = []
                entry_results[entry_id].append(result_dict)
            result.close()

        if not entry_results:
            logger.warning(f"No valid AI results to export for FileID {file_id}")
            return ""

        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        temp_json_dir = f"temp_json_{file_id}"
        os.makedirs(temp_json_dir, exist_ok=True)

        for entry_id, results in entry_results.items():
            json_filename = f"result_{entry_id}_{timestamp}.json"
            local_json_path = os.path.join(temp_json_dir, json_filename)

            async with aiofiles.open(local_json_path, 'w') as f:
                await f.write(json.dumps(results, indent=2))
            
            logger.debug(f"Saved JSON to {local_json_path}, size: {os.path.getsize(local_json_path)} bytes")
            logger.debug(f"JSON content sample for EntryID {entry_id}: {json.dumps(results[:2], indent=2)}")

            s3_key = f"super_scraper/jobs/{file_id}/{json_filename}"
            public_url = await upload_file_to_space(
                local_json_path, s3_key, is_public=True, logger=logger, file_id=file_id
            )
            
            if public_url:
                logger.info(f"Exported JSON for EntryID {entry_id} to {public_url}")
                json_urls.append(public_url)
            else:
                logger.error(f"Failed to upload JSON for EntryID {entry_id}")
            
            os.remove(local_json_path)

        os.rmdir(temp_json_dir)
        return json_urls[0] if json_urls else ""

    except Exception as e:
        logger.error(f"Error exporting DAI JSON for FileID {file_id}: {e}", exc_info=True)
        return ""

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(pyodbc.Error),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying remove_endpoint for endpoint {retry_state.kwargs['endpoint']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def remove_endpoint(endpoint: str, logger: Optional[logging.Logger] = None) -> None:
    logger = logger or default_logger
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE utb_Endpoints SET EndpointIsBlocked = 1 WHERE EndpointURL = ?", (endpoint,))
            conn.commit()
            cursor.close()
            logger.info(f"Marked endpoint as blocked: {endpoint}")
    except pyodbc.Error as e:
        logger.error(f"Error marking endpoint as blocked: {e}", exc_info=True)
        raise

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(pyodbc.Error),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying update_initial_sort_order for FileID {retry_state.kwargs['file_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def update_initial_sort_order(file_id: str, logger: Optional[logging.Logger] = None) -> Optional[List[Dict]]:
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        logger.info(f"Starting initial SortOrder update for FileID: {file_id}")

        with pyodbc.connect(conn_str, autocommit=False) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT 
                    t.ResultID,
                    t.EntryID,
                    CASE WHEN ISJSON(t.AiJson) = 1 
                         THEN ISNULL(TRY_CAST(JSON_VALUE(t.AiJson, '$.match_score') AS FLOAT), 0)
                         ELSE 0 END AS match_score,
                    t.ImageDesc,
                    t.ImageSource,
                    t.ImageUrl,
                    t.SortOrder
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                WHERE r.FileID = ?
                """,
                (file_id,)
            )
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            if not rows:
                logger.warning(f"No data found for FileID: {file_id}")
                return []

            results = [dict(zip(columns, row)) for row in rows]
            updates = []
            for result in results:
                result_id = result['ResultID']
                match_score = result['match_score']
                new_sort_order = int(match_score * 100) if match_score > 0 else -2
                updates.append((result_id, new_sort_order))

            cursor.execute(
                "UPDATE utb_ImageScraperResult SET SortOrder = NULL WHERE EntryID IN "
                "(SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?)",
                (file_id,)
            )

            if updates:
                values_clause = ", ".join(f"({result_id}, {sort_order})" for result_id, sort_order in updates)
                cursor.execute(
                    f"""
                    UPDATE utb_ImageScraperResult
                    SET SortOrder = v.sort_order
                    FROM (VALUES {values_clause}) AS v(result_id, sort_order)
                    WHERE utb_ImageScraperResult.ResultID = v.result_id
                    """
                )
                logger.info(f"Updated {len(updates)} rows for FileID: {file_id}")

            cursor.execute(
                """
                SELECT 
                    COUNT(*) AS total_count,
                    SUM(CASE WHEN SortOrder > 0 THEN 1 ELSE 0 END) AS positive_count,
                    SUM(CASE WHEN SortOrder IS NULL THEN 1 ELSE 0 END) AS null_count
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                WHERE r.FileID = ?
                """,
                (file_id,)
            )
            row = cursor.fetchone()
            total_count, positive_count, null_count = row
            logger.info(f"Verification for FileID {file_id}: {total_count} total rows, "
                       f"{positive_count} positive SortOrder, {null_count} NULL SortOrder")
            if null_count > 0:
                logger.warning(f"Found {null_count} rows with NULL SortOrder for FileID {file_id}")
                cursor.execute(
                    "UPDATE utb_ImageScraperResult SET SortOrder = -2 WHERE EntryID IN "
                    "(SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?) AND SortOrder IS NULL",
                    (file_id,)
                )
                logger.info(f"Set {null_count} NULL SortOrder rows to -2 for FileID: {file_id}")

            cursor.execute(
                """
                SELECT ResultID, EntryID, SortOrder, ImageDesc, ImageSource, ImageUrl
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                WHERE r.FileID = ?
                ORDER BY SortOrder DESC
                """,
                (file_id,)
            )
            final_results = [
                {
                    "ResultID": r[0],
                    "EntryID": r[1],
                    "SortOrder": r[2],
                    "ImageDesc": r[3],
                    "ImageSource": r[4],
                    "ImageUrl": r[5]
                }
                for r in cursor.fetchall()
            ]

            conn.commit()
            logger.info(f"Completed initial SortOrder update for FileID: {file_id} with {len(final_results)} results")
            return final_results

    except pyodbc.Error as e:
        logger.error(f"Database error for FileID {file_id}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error for FileID {file_id}: {e}", exc_info=True)
        return None
    
import aio_pika
import aiormq.exceptions
from sqlalchemy.sql import text # Make sure to import TextClause and text
from sqlalchemy.exc import SQLAlchemyError # Make sure to import SQLAlchemyError
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type((
        aio_pika.exceptions.AMQPError,
        # aio_pika.exceptions.ChannelClosed, # Generally covered by AMQPError
        # aiormq.exceptions.ChannelAccessRefused, # Should be caught on connect by producer
        # aiormq.exceptions.ChannelPreconditionFailed, # Should be handled by producer
        SQLAlchemyError, # For direct DB operations
        RuntimeError, # Catch-all for unexpected issues including RMQ connection
        asyncio.TimeoutError, # For timeouts during RMQ operations
    )),
)
async def enqueue_db_update(
    file_id: str,
    sql: Any,
    params: dict,
    task_type: str = "db_update",
    producer_instance: Optional[RabbitMQProducer] = None,
    response_queue: Optional[str] = None,
    correlation_id: Optional[str] = None,
    return_result: bool = False,
    logger_param: Optional[logging.Logger] = None
) -> Any:
    logger = logger_param or default_logger
    correlation_id = correlation_id or str(uuid.uuid4())

    try:
        if isinstance(sql, text):
            sql = str(sql)

        serializable_params = {
            key: list(value) if isinstance(value, tuple) else value
            for key, value in params.items()
        }

        if return_result and sql.strip().upper().startswith("SELECT"):
            logger.debug(f"[{correlation_id}] Executing SELECT query directly for FileID {file_id}")
            async with async_engine.connect() as conn:
                db_result = await conn.execute(text(sql), serializable_params) # Use serializable_params
                rows = [dict(row) for row in db_result.mappings()] # Use .mappings() for dicts
                # result.close() # Not needed with async with async_engine.connect() and execute
                logger.info(f"[{correlation_id}] Retrieved {len(rows)} rows for FileID {file_id}")
                return rows

        # Get a producer instance if not provided or if the provided one is not connected
        current_producer = producer_instance
        if current_producer is None or not current_producer.is_connected:
            logger.info(f"[{correlation_id}] Producer not provided or not connected. Getting/re-getting instance.")
            # Pass logger to get_producer if your RabbitMQProducer.get_producer supports it
            # and you want its logs to use this specific logger instance.
            # For simplicity, assuming get_producer uses its own internal logger.
            current_producer = await RabbitMQProducer.get_producer()
        
        # Verify connection again after attempting to get/create
        if not current_producer or not current_producer.is_connected:
            logger.error(f"[{correlation_id}] Failed to get a connected RabbitMQ producer for FileID {file_id}.")
            raise RuntimeError("Failed to establish/use RabbitMQ connection.")

        task = {
            "file_id": file_id,
            "sql": sql,
            "params": serializable_params, # Use serializable_params
            "task_type": task_type,
            "correlation_id": correlation_id,
            "return_result": return_result,
            "timestamp": datetime.datetime.now().isoformat()
        }

        queue_to_publish_to = response_queue or current_producer.queue_name # Use producer's default or response_queue

        await current_producer.publish_message(
            message=task,
            routing_key=queue_to_publish_to,
            correlation_id=correlation_id
        )
        logger.info(f"[{correlation_id}] Successfully enqueued task via producer.publish_message to {queue_to_publish_to} for FileID {file_id}: {json.dumps(task)[:200]}")

        return queue_to_publish_to if not return_result else 1 # Indicate 1 "row affected" for non-SELECT

    except Exception as e:
        # The queue_name variable might not be defined if error occurs before it's set
        # So, log generically or ensure it's defined.
        q_name_for_log = response_queue or (producer_instance.queue_name if producer_instance else "db_update_queue")
        logger.error(f"[{correlation_id}] Error enqueuing task to {q_name_for_log} for FileID {file_id}: {e}", exc_info=True)
        raise
