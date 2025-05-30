import logging
import pandas as pd
import pyodbc
import asyncio
import json
import datetime,uuid
import aio_pika
import os
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
# Configuration (consider moving to environment variables or config file)
RESPONSE_TIMEOUT = 60  # seconds
BATCH_SIZE = 100
MAX_CONCURRENCY = 10
CATEGORY_FILTERS = {
    "footwear": ["appliance", "whirlpool", "parts"],
    # Add more categories as needed
}

def validate_thumbnail_url(url: Optional[str], logger: Optional[logging.Logger] = None) -> bool:
    """
    Validates a thumbnail URL by checking for HTTP/HTTPS scheme and absence of placeholder values.
    """
    logger = logger or default_logger
    if not url or not isinstance(url, str) or re.search(r'placeholder', url, re.IGNORECASE):
        logger.debug(f"Invalid thumbnail URL: {url}")
        return False
    if not url.startswith(('http://', 'https://')):
        logger.debug(f"Non-HTTP thumbnail URL: {url}")
        return False
    return True

def clean_url_string(value: Optional[str], is_url: bool = True, logger: Optional[logging.Logger] = None) -> str:
    """
    Cleans a string, particularly URLs, by removing invalid characters and normalizing structure.
    """
    logger = logger or default_logger
    if not value:
        return ""
    cleaned = str(value).replace('\\', '').replace('%5C', '').replace('%5c', '')
    cleaned = re.sub(r'[\x00-\x1F\x7F]+', '', cleaned).strip()
    if is_url:
        cleaned = urllib.parse.unquote(cleaned)
        try:
            parsed = urllib.parse.urlparse(cleaned)
            if not parsed.scheme or not parsed.netloc:
                logger.debug(f"Invalid URL format: {cleaned[:100]}")
                return ""
            path = re.sub(r'/+', '/', parsed.path)
            cleaned = f"{parsed.scheme}://{parsed.netloc}{path}"
            if parsed.query:
                cleaned += f"?{parsed.query}"
            if parsed.fragment:
                cleaned += f"#{parsed.fragment}"
        except ValueError as e:
            logger.debug(f"Invalid URL format: {cleaned[:100]}, error: {e}")
            return ""
    return cleaned
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
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type((SQLAlchemyError, aio_pika.exceptions.AMQPError, asyncio.TimeoutError)),
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
    logger = logger or default_logger
    process = psutil.Process()
    logger.info(f"Worker PID {process.pid}: Starting insert_search_results for FileID {file_id}")

    if not results:
        logger.warning(f"Worker PID {process.pid}: Empty results provided")
        return False

    data = []
    errors = []
    category_filters = {
        "footwear": ["appliance", "whirlpool", "parts"]
    }

    for res in results:
        try:
            entry_id = int(res["EntryID"])
        except (ValueError, TypeError) as e:
            errors.append(f"Invalid EntryID value: {res.get('EntryID')}")
            logger.error(f"Worker PID {process.pid}: {errors[-1]}")
            continue

        category = res.get("ProductCategory", "").lower()
        image_url = clean_url_string(res.get("ImageUrl", ""), logger=logger)
        image_url_thumbnail = clean_url_string(res.get("ImageUrlThumbnail", ""), logger=logger)
        image_desc = clean_string(res.get("ImageDesc", ""), preserve_url=False)
        image_source = clean_url_string(res.get("ImageSource", ""), logger=logger)

        if not image_url or not validate_thumbnail_url(image_url, logger):
            errors.append(f"Invalid ImageUrl skipped: {image_url}")
            logger.debug(f"Worker PID {process.pid}: {errors[-1]}")
            continue
        if image_url_thumbnail and not validate_thumbnail_url(image_url_thumbnail, logger):
            image_url_thumbnail = None

        if category in category_filters and any(
            keyword in image_url.lower() for keyword in category_filters[category]
        ):
            logger.debug(f"Worker PID {process.pid}: Filtered out irrelevant URL: {image_url}")
            continue

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

    producer = await RabbitMQProducer.get_producer()
    try:
        correlation_id = str(uuid.uuid4())
        response_queue = f"select_response_{uuid.uuid4().hex}"
        async with aio_pika.connect_robust(producer.amqp_url) as connection:
            channel = await connection.channel()
            queue = await channel.declare_queue(response_queue, exclusive=True, auto_delete=True)
            response_received = asyncio.Event()
            response_data = []

            async def consume_responses():
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        async with message.process():
                            if message.correlation_id == correlation_id:
                                response_data.append(json.loads(message.body.decode()))
                                response_received.set()
                                logger.debug(f"Worker PID {process.pid}: Received response for correlation_id {correlation_id}")

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
                    response_queue=response_queue,
                    correlation_id=correlation_id
                )
                logger.info(f"Worker PID {process.pid}: Enqueued SELECT query for {len(entry_ids)} EntryIDs")

                consume_task = asyncio.create_task(consume_responses())
                try:
                    async with asyncio.timeout(60):  # Increased timeout
                        await response_received.wait()
                    if response_data:
                        existing_keys = {(row["EntryID"], row["ImageUrl"]) for row in response_data[0]["results"]}
                        logger.info(f"Worker PID {process.pid}: Received {len(existing_keys)} deduplication results")
                    else:
                        logger.warning(f"Worker PID {process.pid}: No deduplication results received")
                except asyncio.TimeoutError:
                    logger.error(f"Worker PID {process.pid}: Timeout waiting for SELECT results")
                    raise
                finally:
                    consume_task.cancel()
                    await asyncio.sleep(0.5)

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
            update_cids = []
            insert_cids = []
            for i in range(0, len(update_batch), batch_size):
                batch = update_batch[i:i + batch_size]
                for sql, params in batch:
                    cid = str(uuid.uuid4())
                    update_cids.append(cid)
                    await enqueue_db_update(
                        file_id=file_id,
                        sql=sql,
                        params=params,
                        background_tasks=background_tasks,
                        task_type="update_search_result",
                        producer=producer,
                        correlation_id=cid
                    )
            logger.info(f"Worker PID {process.pid}: Enqueued {len(update_batch)} updates with {len(update_cids)} correlation IDs")

            for i in range(0, len(insert_batch), batch_size):
                batch = insert_batch[i:i + batch_size]
                for sql, params in batch:
                    cid = str(uuid.uuid4())
                    insert_cids.append(cid)
                    await enqueue_db_update(
                        file_id=file_id,
                        sql=sql,
                        params=params,
                        background_tasks=background_tasks,
                        task_type="insert_search_result",
                        producer=producer,
                        correlation_id=cid
                    )
            logger.info(f"Worker PID {process.pid}: Enqueued {len(insert_batch)} inserts with {len(insert_cids)} correlation IDs")

            # Verify enqueued tasks (optional)
            async with async_engine.connect() as conn:
                query = text("""
                    SELECT COUNT(*) FROM utb_ImageScraperResult
                    WHERE EntryID IN :entry_ids AND CreateTime >= :create_time
                """)
                result = await conn.execute(query, {
                    "entry_ids": tuple(entry_ids),
                    "create_time": min(row["CreateTime"] for row in data)
                })
                inserted_count = result.scalar()
                logger.info(f"Worker PID {process.pid}: Verified {inserted_count} records in database for FileID {file_id}")

            return len(insert_batch) > 0 or len(update_batch) > 0

    except Exception as e:
        logger.error(f"Worker PID {process.pid}: Error enqueuing results for FileID {file_id}: {e}", exc_info=True)
        raise
    finally:
        await producer.close()
        logger.info(f"Worker PID {process.pid}: Closed RabbitMQ producer")
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
    
async def enqueue_db_update(
    file_id: str,
    sql: str,
    params: Dict[str, Any],
    background_tasks: BackgroundTasks,
    task_type: str = "db_update",
    producer: Optional[RabbitMQProducer] = None,
    response_queue: Optional[str] = None,
    correlation_id: Optional[str] = None,
    return_result: bool = False,
    logger: Optional[logging.Logger] = None,
):
    should_close = False
    if producer is None:
        producer = RabbitMQProducer()
        should_close = True

    try:
        await producer.connect()
        sql_str = str(sql) if hasattr(sql, '__clause_element__') else sql
        update_task = {
            "file_id": file_id,
            "task_type": task_type,
            "sql": sql_str,
            "params": params,
            "timestamp": datetime.datetime.now().isoformat(),
            "response_queue": response_queue,
        }
        if return_result and response_queue:
            # Create a temporary consumer to wait for the response
            consumer = RabbitMQConsumer(
                amqp_url=producer.amqp_url,
                queue_name=response_queue,
                connection_timeout=10.0,
                operation_timeout=5.0,
            )
            await consumer.connect()
            result_future = asyncio.Future()

            async def temp_callback(message: aio_pika.IncomingMessage):
                async with message.process():
                    if message.correlation_id == correlation_id:
                        response = json.loads(message.body.decode())
                        result_future.set_result(response.get("results", []))
                        logger.info(f"Received response for correlation_id {correlation_id} from {response_queue}")

            await consumer.channel.declare_queue(response_queue, durable=False, exclusive=True, auto_delete=True)
            queue = await consumer.channel.get_queue(response_queue)
            await queue.consume(temp_callback)

            await producer.publish_update(update_task, routing_key=producer.queue_name, correlation_id=correlation_id)
            logger.info(f"Enqueued database update for FileID: {file_id}, TaskType: {task_type}, SQL: {sql_str[:100]}")

            # Wait for the response with a timeout
            try:
                async with asyncio.timeout(30):  # 30-second timeout
                    result = await result_future
                    logger.info(f"Received deduplication result for FileID: {file_id}")
                    return result
            except asyncio.TimeoutError:
                logger.error(f"Timeout waiting for deduplication response for FileID: {file_id}")
                raise
            finally:
                await consumer.close()
        else:
            await producer.publish_update(update_task, routing_key=producer.queue_name, correlation_id=correlation_id)
            logger.info(f"Enqueued database update for FileID: {file_id}, TaskType: {task_type}, SQL: {sql_str[:100]}")
    except Exception as e:
        logger.error(f"Error enqueuing database update for FileID: {file_id}: {e}", exc_info=True)
        raise
    finally:
        if should_close:
            await producer.close()
            logger.info(f"Closed RabbitMQ producer for FileID: {file_id}")