import logging
import os
import datetime
import json
import aiofiles
import signal
import atexit
from typing import Optional, List, Dict, Callable, Any
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, retry_if_exception
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection
from database_config import async_engine
from common import clean_string, normalize_model, generate_aliases
import psutil
import pyodbc
import re
import urllib.parse
import asyncio
import traceback

logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)


# DatabaseQueue class
class DatabaseQueue:
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        self.queue = asyncio.Queue()
        self._running = False
        self._task = None

    async def start(self):
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._worker())
            self.logger.info("Database queue worker started")

    async def stop(self):
        if self._running:
            self._running = False
            await self.queue.join()
            if self._task:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            self.logger.info("Database queue worker stopped")

    async def enqueue(self, operation: Callable, params: Dict[str, Any], connection_type: str = "write") -> Any:
        future = asyncio.Future()
        await self.queue.put((operation, params, connection_type, future))
        return await future

    async def _worker(self):
        while self._running:
            try:
                operation, params, connection_type, future = await self.queue.get()
                try:
                    for attempt in range(3):  # Retry up to 3 times
                        try:
                            if connection_type == "read":
                                async with async_engine.connect() as conn:
                                    if not await self._is_connection_healthy(conn):
                                        raise SQLAlchemyError("Connection unhealthy")
                                    result = await operation(conn, params)
                                    if not future.done():
                                        future.set_result(result)
                                    else:
                                        self.logger.warning(f"Future already done for operation {operation.__name__}")
                                    break
                            else:
                                async with async_engine.begin() as conn:
                                    if not await self._is_connection_healthy(conn):
                                        raise SQLAlchemyError("Connection unhealthy")
                                    result = await operation(conn, params)
                                    await conn.commit()
                                    if not future.done():
                                        future.set_result(result)
                                    else:
                                        self.logger.warning(f"Future already done for operation {operation.__name__}")
                                    break
                        except SQLAlchemyError as e:
                            if ("Connection is busy" in str(e) or "Communication link failure" in str(e)) and attempt < 2:
                                self.logger.warning(f"Connection error, retrying attempt {attempt + 1}/3: {e}")
                                await asyncio.sleep(2 ** attempt)  # Exponential backoff
                                continue
                            raise
                    else:
                        self.logger.error(f"Failed to execute operation {operation.__name__} after 3 attempts")
                        if not future.done():
                            try:
                                future.set_exception(SQLAlchemyError("Max retries reached for connection error"))
                            except asyncio.InvalidStateError:
                                self.logger.error(f"Cannot set exception on future for operation {operation.__name__}: invalid state", exc_info=True)
                except SQLAlchemyError as e:
                    self.logger.error(f"SQLAlchemy error processing database task: {e}", exc_info=True)
                    if not future.done():
                        try:
                            future.set_exception(e)
                        except asyncio.InvalidStateError:
                            self.logger.error(f"Cannot set exception on future for operation {operation.__name__}: invalid state", exc_info=True)
                except Exception as e:
                    self.logger.error(f"Unexpected error processing database task: {e}", exc_info=True)
                    if not future.done():
                        try:
                            future.set_exception(e)
                        except asyncio.InvalidStateError:
                            self.logger.error(f"Cannot set exception on future for operation {operation.__name__}: invalid state", exc_info=True)
                finally:
                    self.queue.task_done()
            except asyncio.CancelledError:
                self.logger.info("Queue worker cancelled")
                break
            except Exception as e:
                self.logger.error(f"Unexpected error in queue worker: {e}", exc_info=True)
    async def _is_connection_healthy(self, conn: AsyncConnection) -> bool:
        try:
            await conn.execute(text("SELECT 1"))
            return True
        except SQLAlchemyError as e:
            self.logger.warning(f"Connection health check failed: {e}")
            return {"error": str(e)}
# Global DatabaseQueue instance
global_db_queue = None

# Signal handler for graceful shutdown
def setup_signal_handlers():
    def handle_shutdown(signum, frame):
        logger.info(f"Received signal {signum}, initiating graceful shutdown")
        if global_db_queue:
            asyncio.create_task(global_db_queue.stop())
        asyncio.create_task(async_engine.dispose())
        logger.info("Shutdown complete")

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

# Initialize application
def initialize_app():
    global global_db_queue
    global_db_queue = DatabaseQueue(logger=logger)
    setup_signal_handlers()
    atexit.register(lambda: asyncio.run(async_engine.dispose()))

initialize_app()

def validate_thumbnail_url(url: Optional[str], logger: Optional[logging.Logger] = None) -> bool:
    logger = logger or logger
    if not url or url == '' or 'placeholder' in str(url).lower():
        logger.debug(f"Invalid thumbnail URL: {url}")
        return False
    if not str(url).startswith(('http://', 'https://')):
        logger.debug(f"Non-HTTP thumbnail URL: {url}")
        return False
    return True

def clean_url_string(value: Optional[str], is_url: bool = True) -> str:
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
            return ""
    return cleaned

async def insert_search_results(
    results: List[Dict],
    logger: Optional[logging.Logger] = None,
    file_id: str = None,
    db_queue: Optional[DatabaseQueue] = None
) -> bool:
    logger = logger or logger
    process = psutil.Process()
    db_queue = db_queue or global_db_queue or DatabaseQueue(logger=logger)
    await db_queue.start()

    try:
        if not results:
            logger.warning(f"Worker PID {process.pid}: Empty results provided for insert_search_results, FileID {file_id}")
            return False

        required_columns = ["EntryID", "ImageUrl", "ImageDesc", "ImageSource", "ImageUrlThumbnail"]
        valid_results = []
        for res in results:
            if not all(col in res for col in required_columns):
                missing_cols = set(required_columns) - set(res.keys())
                logger.error(f"Worker PID {process.pid}: Missing columns {missing_cols} in result: {res}")
                continue
            if "placeholder://error" in res["ImageUrl"]:
                logger.warning(f"Worker PID {process.pid}: Skipping error placeholder result for EntryID {res['EntryID']}: {res['ImageUrl']}")
                continue
            logger.debug(f"Worker PID {process.pid}: Valid result for EntryID {res['EntryID']}: {res['ImageUrl']}")
            valid_results.append(res)

        if not valid_results:
            logger.warning(f"Worker PID {process.pid}: No valid results to insert for FileID {file_id}")
            return False

        seen = set()
        deduped_results = []
        for res in valid_results:
            image_url = clean_url_string(res.get("ImageUrl", ""), is_url=True)
            key = (res["EntryID"], image_url)
            if key not in seen:
                seen.add(key)
                deduped_results.append(res)
        logger.info(f"Worker PID {process.pid}: Deduplicated from {len(valid_results)} to {len(deduped_results)} rows")

        parameters = []
        for res in deduped_results:
            try:
                entry_id = int(res["EntryID"])
            except (ValueError, TypeError):
                logger.error(f"Worker PID {process.pid}: Invalid EntryID value: {res.get('EntryID')}")
                continue

            category = res.get("ProductCategory", "").lower()
            image_url = clean_url_string(res.get("ImageUrl", ""), is_url=True)
            image_url_thumbnail = clean_url_string(res.get("ImageUrlThumbnail", ""), is_url=True)
            image_desc = clean_url_string(res.get("ImageDesc", ""), is_url=False)
            image_source = clean_url_string(res.get("ImageSource", ""), is_url=True)

            if not image_url:
                logger.debug(f"Worker PID {process.pid}: Invalid ImageUrl skipped: {image_url}")
                continue
            if image_url_thumbnail and not validate_thumbnail_url(image_url_thumbnail, logger):
                logger.debug(f"Worker PID {process.pid}: Invalid ImageUrlThumbnail, setting to empty: {image_url_thumbnail}")
                image_url_thumbnail = ""

            if category == "footwear" and any(keyword in image_url.lower() for keyword in ["appliance", "whirlpool", "parts"]):
                logger.debug(f"Worker PID {process.pid}: Filtered out irrelevant URL: {image_url}")
                continue

            param = {
                "entry_id": entry_id,
                "image_url": image_url,
                "image_desc": image_desc or None,
                "image_source": image_source or None,
                "image_url_thumbnail": image_url_thumbnail or None
            }
            parameters.append(param)
            logger.debug(f"Worker PID {process.pid}: Prepared row for EntryID {entry_id}: {param}")

        if not parameters:
            logger.warning(f"Worker PID {process.pid}: No valid rows to insert for FileID {file_id} after validation")
            return False

        async def insert_or_update_result(conn: AsyncConnection, params: List[Dict]):
            try:
                update_query = text("""
                    UPDATE utb_ImageScraperResult
                    SET ImageDesc = t.image_desc,
                        ImageSource = t.image_source,
                        ImageUrlThumbnail = t.image_url_thumbnail,
                        CreateTime = CURRENT_TIMESTAMP
                    FROM (VALUES :update_values) AS t(entry_id, image_url, image_desc, image_source, image_url_thumbnail)
                    WHERE utb_ImageScraperResult.EntryID = t.entry_id AND utb_ImageScraperResult.ImageUrl = t.image_url
                """)
                update_values = [(p["entry_id"], p["image_url"], p["image_desc"], p["image_source"], p["image_url_thumbnail"]) for p in params]
                result = await conn.execute(update_query, {"update_values": update_values})
                updated_count = result.rowcount

                insert_query = text("""
                    INSERT INTO utb_ImageScraperResult (EntryID, ImageUrl, ImageDesc, ImageSource, ImageUrlThumbnail, CreateTime, SortOrder)
                    SELECT t.entry_id, t.image_url, t.image_desc, t.image_source, t.image_url_thumbnail, CURRENT_TIMESTAMP, t.sort_order
                    FROM (VALUES :insert_values) AS t(entry_id, image_url, image_desc, image_source, image_url_thumbnail, sort_order)
                    WHERE NOT EXISTS (
                        SELECT 1 FROM utb_ImageScraperResult
                        WHERE EntryID = t.entry_id AND ImageUrl = t.image_url
                    )
                """)
                insert_values = [(p["entry_id"], p["image_url"], p["image_desc"], p["image_source"], p["image_url_thumbnail"], -1 if p["image_url"] == "placeholder://no-results" else None) for p in params]
                result = await conn.execute(insert_query, {"insert_values": insert_values})
                inserted_count = result.rowcount
                return inserted_count, updated_count
            except SQLAlchemyError as e:
                logger.error(f"Worker PID {process.pid}: SQLAlchemy error in insert_or_update_result: {e}", exc_info=True)
                raise
            except Exception as e:
                logger.error(f"Worker PID {process.pid}: Unexpected error in insert_or_update_result: {e}", exc_info=True)
                raise

        inserted_count, updated_count = await db_queue.enqueue(
            insert_or_update_result,
            parameters,
            connection_type="write"
        )
        logger.info(f"Worker PID {process.pid}: Inserted {inserted_count} and updated {updated_count} of {len(parameters)} rows for FileID {file_id}")
        return inserted_count > 0 or updated_count > 0

    except SQLAlchemyError as e:
        logger.error(f"Worker PID {process.pid}: Database error inserting results for FileID {file_id}: {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Worker PID {process.pid}: Unexpected error inserting results for FileID {file_id}: {e}", exc_info=True)
        return False
    finally:
        await db_queue.stop()

from pyodbc import Error as PyodbcError

def is_connection_busy_error(exception):
    if isinstance(exception, SQLAlchemyError):
        orig = getattr(exception, "orig", None)
        if isinstance(orig, PyodbcError):
            return "HY000" in str(orig) and "Connection is busy" in str(orig)
    return False
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type((PyodbcError, SQLAlchemyError)) | retry_if_exception(is_connection_busy_error),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying update_search_sort_order for EntryID {retry_state.kwargs['entry_id']} "
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
    db_queue: Optional[DatabaseQueue] = None
) -> Dict[str, Any]:
    logger = logger or logging.getLogger(__name__)
    process = psutil.Process()
    db_queue = db_queue or global_db_queue or DatabaseQueue(logger=logger)

    try:
        # Fetch results
        async def fetch_results(conn: AsyncConnection, params: Dict[str, Any]):
            query = text("""
                SELECT r.ResultID, r.ImageUrl, r.ImageDesc, r.ImageSource, r.ImageUrlThumbnail
                FROM utb_ImageScraperResult r
                INNER JOIN utb_ImageScraperRecords rec ON r.EntryID = rec.EntryID
                WHERE r.EntryID = :entry_id AND rec.FileID = :file_id
            """)
            result = await conn.execute(query, params)
            try:
                rows = result.fetchall()
                columns = result.keys()
            finally:
                result.close()  # Ensure cursor is closed
            return rows, columns

        try:
            rows, columns = await db_queue.enqueue(
                fetch_results,
                {"entry_id": entry_id, "file_id": file_id},
                connection_type="read"
            ) or (None, None)
        except Exception as e:
            logger.error(f"Failed to fetch results for EntryID {entry_id}: {e}", exc_info=True)
            return {"success": False, "error": str(e), "entry_id": entry_id}

        if rows is None or columns is None:
            logger.error(f"Queue returned None for EntryID {entry_id}")
            return {"success": False, "error": "Queue operation failed", "entry_id": entry_id}

        results = [dict(zip(columns, row)) for row in rows]

        # Handle no results
        if not rows:
            logger.warning(f"Worker PID {process.pid}: No results found for FileID {file_id}, EntryID {entry_id}")
            async def insert_placeholder(conn: AsyncConnection, params: Dict[str, Any]):
                query = text("""
                    INSERT INTO utb_ImageScraperResult
                    (EntryID, ImageUrl, ImageDesc, ImageSource, ImageUrlThumbnail, SortOrder, CreateTime)
                    VALUES (:entry_id, :image_url, :image_desc, :image_source, :image_url_thumbnail, -1, CURRENT_TIMESTAMP)
                """)
                await conn.execute(query, params)

            try:
                await db_queue.enqueue(
                    insert_placeholder,
                    {
                        "entry_id": entry_id,
                        "image_url": "placeholder://no-results",
                        "image_desc": f"No results found for {model or 'unknown'}",
                        "image_source": "N/A",
                        "image_url_thumbnail": "placeholder://no-results"
                    },
                    connection_type="write"
                )
                logger.info(f"Worker PID {process.pid}: Inserted placeholder result with SortOrder = -1 for EntryID {entry_id}")
                return {"success": True, "entry_id": entry_id}
            except Exception as e:
                logger.error(f"Failed to insert placeholder for EntryID {entry_id}: {e}", exc_info=True)
                return {"success": False, "error": str(e), "entry_id": entry_id}

        # Process brand and model aliases
        brand_clean = clean_string(brand).lower() if brand else ""
        model_clean = normalize_model(model) if model else ""
        logger.debug(f"Worker PID {process.pid}: Cleaned brand: {brand_clean}, Cleaned model: {model_clean}")

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
            logger.debug(f"Worker PID {process.pid}: ImageDesc: {image_desc[:100]}, ImageSource: {image_source[:100]}, ImageUrl: {image_url[:100]}")

            model_matched = any(alias in image_desc or alias in image_source or alias in image_url for alias in model_aliases)
            brand_matched = any(alias in image_desc or alias in image_source or alias in image_url for alias in brand_aliases)
            logger.debug(f"Worker PID {process.pid}: Model matched: {model_matched}, Brand matched: {brand_matched}")

            res["priority"] = 1 if model_matched and brand_matched else 2 if model_matched else 3 if brand_matched else 4
            logger.debug(f"Worker PID {process.pid}: Assigned priority {res['priority']} to ResultID {res['ResultID']}")

        sorted_results = sorted(results, key=lambda x: x["priority"])
        logger.debug(f"Worker PID {process.pid}: Sorted {len(sorted_results)} results for EntryID {entry_id}")

        # Perform bulk update using a temporary table
        import pandas as pd
        from sqlalchemy.ext.asyncio import AsyncConnection
        from sqlalchemy.sql import text
        import logging
        import psutil

        logger = logging.getLogger(__name__)

        async def update_sort_order(conn: AsyncConnection, params: List[Dict]):
            try:
                # Convert params to DataFrame
                update_df = pd.DataFrame(params, columns=["result_id", "entry_id", "sort_order"])
                
                # Ensure consistent data types
                update_df["result_id"] = update_df["result_id"].astype(int)
                update_df["entry_id"] = update_df["entry_id"].astype(str)
                update_df["sort_order"] = update_df["sort_order"].astype(int)
                
                # Fetch existing utb_ImageScraperResult data for relevant EntryIDs
                entry_ids = tuple(update_df["entry_id"].unique())
                if not entry_ids:
                    logger.warning(f"Worker PID {psutil.Process().pid}: No entry IDs provided for update")
                    return 0
                
                query = text("""
                    SELECT ResultID, EntryID, SortOrder
                    FROM utb_ImageScraperResult
                    WHERE EntryID IN :entry_ids
                """).bindparams(entry_ids=entry_ids)
                result = await conn.execute(query)
                rows = result.fetchall()
                result.close()
                
                # Create DataFrame from existing data
                existing_df = pd.DataFrame(
                    rows,
                    columns=["ResultID", "EntryID", "SortOrder"]
                )
                
                # Ensure consistent data types for existing data
                existing_df["ResultID"] = existing_df["ResultID"].astype(int)
                existing_df["EntryID"] = existing_df["EntryID"].astype(str)
                existing_df["SortOrder"] = existing_df["SortOrder"].astype(int, errors="ignore")
                
                # Merge update_df with existing_df to apply SortOrder updates
                merged_df = existing_df.merge(
                    update_df,
                    left_on=["ResultID", "EntryID"],
                    right_on=["result_id", "entry_id"],
                    how="left"
                )
                
                # Update SortOrder: use new sort_order where available, keep existing otherwise
                merged_df["SortOrder"] = merged_df["sort_order"].fillna(merged_df["SortOrder"]).astype(int)
                
                # Filter rows where SortOrder needs updating (i.e., new SortOrder differs from existing)
                update_rows = merged_df[
                    merged_df["sort_order"].notnull() & 
                    (merged_df["SortOrder"] != merged_df["sort_order"])
                ][["ResultID", "EntryID", "SortOrder"]]
                
                if update_rows.empty:
                    logger.debug(f"Worker PID {psutil.Process().pid}: No SortOrder updates needed")
                    return 0
                
                # Prepare and execute UPDATE statements
                update_query = text("""
                    UPDATE utb_ImageScraperResult
                    SET SortOrder = :SortOrder
                    WHERE ResultID = :ResultID AND EntryID = :EntryID
                """)
                row_count = 0
                for _, row in update_rows.iterrows():
                    result = await conn.execute(update_query, {
                        "ResultID": row["ResultID"],
                        "EntryID": row["EntryID"],
                        "SortOrder": row["SortOrder"]
                    })
                    row_count += result.rowcount
                
                logger.debug(f"Worker PID {psutil.Process().pid}: Updated {row_count} rows with new SortOrder")
                return max(0, row_count)
            
            except Exception as e:
                logger.error(f"Worker PID {psutil.Process().pid}: Error in update_sort_order: {e}", exc_info=True)
                raise

        update_params = []
        match_count = 0
        for res in sorted_results:
            sort_order = -2 if res["priority"] == 4 else (match_count := match_count + 1)
            update_params.append({
                "sort_order": sort_order,
                "result_id": res["ResultID"],
                "entry_id": entry_id
            })

        if update_params:
            logger.debug(f"Worker PID {process.pid}: Update params: {update_params}")
            try:
                row_count = await db_queue.enqueue(
                    update_sort_order,
                    update_params,
                    connection_type="write"
                )
                row_count = int(row_count or 0)
                if row_count < 0:
                    logger.error(f"Worker PID {process.pid}: Negative row_count {row_count} detected")
                    row_count = 0
                logger.info(f"Worker PID {process.pid}: Bulk updated SortOrder for {row_count} rows for EntryID {entry_id}")
            except Exception as e:
                logger.error(f"Failed to update sort order for EntryID {entry_id}: {e}", exc_info=True)
                return {"success": False, "error": str(e), "entry_id": entry_id}
        else:
            logger.warning(f"Worker PID {process.pid}: No results to update SortOrder for EntryID {entry_id}")

        logger.info(f"Worker PID {process.pid}: Successfully updated sort order for EntryID {entry_id}")
        return {"success": True, "entry_id": entry_id}
    except SQLAlchemyError as e:
        logger.error(f"Worker PID {process.pid}: Database error in update_search_sort_order for EntryID {entry_id}: {e}", exc_info=True)
        return {"success": False, "error": str(e), "entry_id": entry_id}
    except Exception as e:
        logger.error(f"Worker PID {process.pid}: Unexpected error in update_search_sort_order for EntryID {entry_id}: {e}", exc_info=True)
        return {"success": False, "error": str(e), "entry_id": entry_id}
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type(SQLAlchemyError) | retry_if_exception(is_connection_busy_error),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying update_sort_no_image_entry for FileID {retry_state.kwargs['file_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def update_sort_no_image_entry(
    file_id: str,
    logger: Optional[logging.Logger] = None,
    db_queue: Optional[DatabaseQueue] = None
) -> Optional[Dict]:
    logger = logger or logger
    db_queue = db_queue or global_db_queue or DatabaseQueue(logger=logger)
    await db_queue.start()

    try:
        file_id = int(file_id)
        logger.info(f"Starting per-entry SortOrder update for FileID: {file_id}")

        async def fetch_entry_ids(conn: AsyncConnection, params: Dict[str, Any]):
            query = text("""
                SELECT EntryID 
                FROM utb_ImageScraperRecords 
                WHERE FileID = :file_id
            """)
            result = await conn.execute(query, params)
            entry_ids = [row[0] for row in result.fetchall()]
            result.close()
            return entry_ids

        entry_ids = await db_queue.enqueue(
            fetch_entry_ids,
            {"file_id": file_id},
            connection_type="read"
        )
        logger.debug(f"Fetched {len(entry_ids)} EntryIDs for FileID {file_id}")

        if not entry_ids:
            logger.warning(f"No entries found for FileID {file_id}")
            return {"file_id": file_id, "rows_deleted": 0, "rows_updated": 0}

        async def count_null_sort_order(conn: AsyncConnection, params: Dict[str, Any]):
            query = text("""
                SELECT COUNT(*) 
                FROM utb_ImageScraperResult 
                WHERE EntryID IN :entry_ids AND SortOrder IS NULL
            """).bindparams(entry_ids=tuple(params["entry_ids"]))
            result = await conn.execute(query, params)
            count = result.scalar()
            result.close()
            return count

        null_count = await db_queue.enqueue(
            count_null_sort_order,
            {"entry_ids": entry_ids},
            connection_type="read"
        )
        logger.debug(f"Worker PID {psutil.Process().pid}: {null_count} entries with NULL SortOrder for FileID {file_id}")

        async def delete_placeholders(conn: AsyncConnection, params: Dict[str, Any]):
            query = text("""
                DELETE FROM utb_ImageScraperResult
                WHERE EntryID IN :entry_ids AND ImageUrl = 'placeholder://no-results'
            """).bindparams(entry_ids=tuple(params["entry_ids"]))
            result = await conn.execute(query, params)
            return result.rowcount

        rows_deleted = await db_queue.enqueue(
            delete_placeholders,
            {"entry_ids": entry_ids},
            connection_type="write"
        )
        logger.info(f"Deleted {rows_deleted} placeholder entries for FileID {file_id}")

        async def update_null_sort_order(conn: AsyncConnection, params: Dict[str, Any]):
            query = text("""
                UPDATE utb_ImageScraperResult
                SET SortOrder = -2
                WHERE EntryID IN :entry_ids AND SortOrder IS NULL
            """).bindparams(entry_ids=tuple(params["entry_ids"]))
            result = await conn.execute(query, params)
            return result.rowcount

        rows_updated = await db_queue.enqueue(
            update_null_sort_order,
            {"entry_ids": entry_ids},
            connection_type="write"
        )
        rows_updated = int(rows_updated or 0)
        logger.info(f"Updated {rows_updated} NULL SortOrder entries to -2 for FileID {file_id}")
        return {"file_id": file_id, "rows_deleted": rows_deleted, "rows_updated": rows_updated}

    except SQLAlchemyError as e:
        logger.error(f"Database error updating entries for FileID {file_id}: {e}", exc_info=True)
        raise
    except ValueError as ve:
        logger.error(f"Invalid file_id format: {file_id}, error: {str(ve)}")
        return {"error": str(ve)}
    except Exception as e:
        logger.error(f"Unexpected error updating entries for FileID {file_id}: {e}", exc_info=True)
        return {"error": str(e)}
    finally:
        await db_queue.stop()

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
    db_queue: Optional[DatabaseQueue] = None
) -> Dict[str, Any]:
    logger = logger or logging.getLogger(__name__)
    db_queue = db_queue or global_db_queue or DatabaseQueue(logger=logger)
    await db_queue.start()

    try:
        file_id = int(file_id)
        logger.info(f"Starting batch SortOrder update for FileID: {file_id}")
        
        async def fetch_entries(conn: AsyncConnection, params: Dict[str, Any]):
            query = text("""
                SELECT EntryID, ProductBrand, ProductModel, ProductColor, ProductCategory 
                FROM utb_ImageScraperRecords 
                WHERE FileID = :file_id
            """)
            result = await conn.execute(query, params)
            entries = result.fetchall()
            result.close()
            logger.debug(f"Fetched {len(entries)} entries for FileID: {file_id}")
            return entries

        try:
            entries = await db_queue.enqueue(
                fetch_entries,
                {"file_id": file_id},
                connection_type="read"
            ) or None
        except Exception as e:
            logger.error(f"Failed to fetch entries for FileID: {file_id}: {e}", exc_info=True)
            return {"error": f"Failed to fetch entries: {str(e)}", "results": [], "success_count": 0, "failure_count": 0}
        
        if entries is None:
            logger.error(f"Queue returned None for FileID: {file_id}")
            return {"error": "Queue operation failed", "results": [], "success_count": 0, "failure_count": 0}
        
        if not entries:
            logger.warning(f"No entries found for FileID: {file_id}")
            result = {"results": [], "success_count": 0, "failure_count": 0}
            logger.debug(f"Returning empty result for FileID: {file_id}: {result}")
            return result
        
        results = []
        success_count = 0
        failure_count = 0

        for entry in entries:
            entry_id, brand, model, color, category = entry
            logger.debug(f"Processing EntryID {entry_id}, Brand: {brand}, Model: {model}")
            try:
                entry_results = await update_search_sort_order(
                    file_id=str(file_id),
                    entry_id=str(entry_id),
                    brand=brand,
                    model=model,
                    color=color,
                    category=category,
                    logger=logger,
                    db_queue=db_queue
                )
                results.append({"EntryID": entry_id, "Success": entry_results["success"], "Error": entry_results.get("error")})
                if entry_results["success"]:
                    success_count += 1
                else:
                    failure_count += 1
                    logger.warning(f"Failed to update EntryID {entry_id}: {entry_results.get('error', 'Unknown error')}")
            except Exception as e:
                logger.error(f"Error processing EntryID {entry_id}: {e}", exc_info=True)
                results.append({"EntryID": entry_id, "Success": False, "Error": str(e)})
                failure_count += 1

        logger.info(f"Completed batch SortOrder update for FileID {file_id}: {success_count} entries successful, {failure_count} failed")

        async def verify_sort_order(conn: AsyncConnection, params: Dict[str, Any]):
            query = text("""
                SELECT 
                    COUNT(DISTINCT CASE WHEN t.SortOrder > 0 THEN t.EntryID END) AS PositiveSortOrderEntries,
                    COUNT(DISTINCT CASE WHEN t.SortOrder = 0 THEN t.EntryID END) AS BrandMatchEntries,
                    COUNT(DISTINCT CASE WHEN t.SortOrder < 0 THEN t.EntryID END) AS NoMatchEntries,
                    COUNT(DISTINCT CASE WHEN t.SortOrder IS NULL THEN t.EntryID END) AS NullSortOrderEntries,
                    COUNT(DISTINCT CASE WHEN t.SortOrder = -1 THEN t.EntryID END) AS UnexpectedSortOrderEntries,
                    COUNT(*) AS NonPlaceholderResults
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                WHERE r.FileID = :file_id AND t.ImageUrl NOT LIKE 'placeholder://%'
            """)
            result = await conn.execute(query, params)
            verification = dict(zip(result.keys(), result.fetchone()))
            result.close()
            return verification

        entry_ids = tuple(e[0] for e in entries)
        try:
            verification = await db_queue.enqueue(
                verify_sort_order,
                {"file_id": file_id, "entry_ids": entry_ids},
                connection_type="read"
            ) or {}
        except Exception as e:
            logger.error(f"Failed to verify sort order for FileID: {file_id}: {e}", exc_info=True)
            verification = {}

        async def fetch_sort_orders(conn: AsyncConnection, params: Dict[str, Any]):
            query = text("""
                SELECT t.EntryID, t.SortOrder, t.ImageUrl
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                WHERE r.FileID = :file_id
            """)
            result = await conn.execute(query, params)
            sort_orders = result.fetchall()
            result.close()
            return sort_orders

        try:
            sort_orders = await db_queue.enqueue(
                fetch_sort_orders,
                {"file_id": file_id},
                connection_type="read"
            ) or []
        except Exception as e:
            logger.error(f"Failed to fetch sort orders for FileID: {file_id}: {e}", exc_info=True)
            sort_orders = []

        logger.info(f"SortOrder values for FileID {file_id}: {[(row[0], row[1], row[2][:50]) for row in sort_orders]}")
        logger.info(f"Verification for FileID {file_id}: "
                   f"{verification.get('PositiveSortOrderEntries', 0)} entries with model matches, "
                   f"{verification.get('BrandMatchEntries', 0)} entries with brand matches only, "
                   f"{verification.get('NoMatchEntries', 0)} entries with no matches, "
                   f"{verification.get('NullSortOrderEntries', 0)} entries with NULL SortOrder, "
                   f"{verification.get('UnexpectedSortOrderEntries', 0)} entries with unexpected SortOrder, "
                   f"{verification.get('NonPlaceholderResults', 0)} non-placeholder results")

        result = {
            "results": results,
            "success_count": success_count,
            "failure_count": failure_count,
            "verification": verification
        }
        logger.debug(f"Returning final result for FileID: {file_id}: {result}")
        return result
    except SQLAlchemyError as e:
        logger.error(f"Database error in batch SortOrder update for FileID: {file_id}: {e}", exc_info=True)
        return {"error": str(e), "results": [], "success_count": 0, "failure_count": 0}
    except ValueError as ve:
        logger.error(f"Invalid file_id format for FileID: {file_id}: {ve}", exc_info=True)
        return {"error": str(ve), "results": [], "success_count": 0, "failure_count": 0}
    except Exception as e:
        logger.error(f"Unexpected error in batch SortOrder update for FileID: {file_id}: {e}", exc_info=True)
        return {"error": str(e), "results": [], "success_count": 0, "failure_count": 0}
    finally:
        try:
            await db_queue.stop()
        except Exception as e:
            logger.error(f"Failed to stop database queue for FileID: {file_id}: {e}", exc_info=True)