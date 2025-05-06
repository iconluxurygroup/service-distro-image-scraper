import logging
import pandas as pd
import pyodbc
import requests
import json
import re
import base64
import zlib
import urllib.parse
from typing import List, Optional, Dict, Any
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from urllib3.util.retry import Retry
from config import conn_str
from requests import Session
from icon_image_lib.google_parser import process_search_result
from config import conn_str, engine
# Default logger
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

import pandas as pd
import pyodbc
import logging
from typing import Optional
import pandas as pd

import pandas as pd
import pyodbc


from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text
import pandas as pd
import logging
from typing import Optional

def insert_search_results(df: pd.DataFrame, logger: Optional[logging.Logger] = None) -> bool:
    logger = logger or logging.getLogger(__name__)
    if df.empty:
        logger.info("No rows to insert: DataFrame is empty")
        return False

    required = ['EntryID', 'ImageUrl', 'ImageDesc', 'ImageSource', 'ImageUrlThumbnail']
    if not all(col in df.columns for col in required):
        missing_cols = [col for col in required if col not in df.columns]
        logger.error(f"Missing columns: {missing_cols}")
        return False

    # Validate data
    try:
        df = df.copy()
        df['EntryID'] = df['EntryID'].astype(int)
        for col in required[1:]:
            df.loc[:, col] = df[col].astype(str).replace('', None)
            if df[col].isnull().any():
                logger.warning(f"Null values in {col}: {df[df[col].isnull()].to_dict()}")
    except Exception as e:
        logger.error(f"Data validation failed: {e}")
        return False

    df = df.drop_duplicates(subset=['EntryID', 'ImageUrl'], keep='first')
    logger.info(f"Deduplicated to {len(df)} rows")

    try:
        with engine.begin() as conn:  # Use SQLAlchemy transaction
            inserted_count = 0
            updated_count = 0
            for index, row in df.iterrows():
                try:
                    # Try updating existing row
                    update_query = text("""
                        UPDATE utb_ImageScraperResult
                        SET ImageDesc = :image_desc, ImageSource = :image_source, 
                            ImageUrlThumbnail = :image_url_thumbnail, CreateTime = GETDATE()
                        WHERE EntryID = :entry_id AND ImageUrl = :image_url
                    """)
                    result = conn.execute(
                        update_query,
                        {
                            'image_desc': row['ImageDesc'],
                            'image_source': row['ImageSource'],
                            'image_url_thumbnail': row['ImageUrlThumbnail'],
                            'entry_id': row['EntryID'],
                            'image_url': row['ImageUrl']
                        }
                    )
                    updated_count += result.rowcount

                    # If not updated, insert new row
                    if result.rowcount == 0:
                        insert_query = text("""
                            INSERT INTO utb_ImageScraperResult (EntryID, ImageUrl, ImageDesc, ImageSource, ImageUrlThumbnail, CreateTime)
                            SELECT :entry_id, :image_url, :image_desc, :image_source, :image_url_thumbnail, GETDATE()
                            WHERE NOT EXISTS (
                                SELECT 1 FROM utb_ImageScraperResult 
                                WHERE EntryID = :entry_id AND ImageUrl = :image_url
                            )
                        """)
                        result = conn.execute(
                            insert_query,
                            {
                                'entry_id': row['EntryID'],
                                'image_url': row['ImageUrl'],
                                'image_desc': row['ImageDesc'],
                                'image_source': row['ImageSource'],
                                'image_url_thumbnail': row['ImageUrlThumbnail']
                            }
                        )
                        inserted_count += result.rowcount
                except SQLAlchemyError as e:
                    logger.error(f"Failed to process row {index} for EntryID {row['EntryID']}: {e}", exc_info=True)
                    logger.debug(f"Row data: {row.to_dict()}")
                    continue
            logger.info(f"Inserted {inserted_count} and updated {updated_count} of {len(df)} rows")
            if inserted_count == 0 and updated_count == 0:
                logger.warning("No rows inserted or updated; likely all rows are duplicates")
            return inserted_count > 0 or updated_count > 0
    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}", exc_info=True)
        return False
async def update_log_url_in_db(file_id: str, log_url: str, logger: Optional[logging.Logger] = None) -> bool:
    """Update the log URL in the database."""
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("""
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
            cursor.execute("UPDATE utb_ImageScraperFiles SET LogFileUrl = ? WHERE ID = ?", (log_url, file_id))
            conn.commit()
            logger.info(f"✅ Updated log URL '{log_url}' for FileID {file_id}")
            return True
    except pyodbc.Error as e:
        logger.error(f"🔴 Database error updating log URL for FileID {file_id}: {e}", exc_info=True)
        return False
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")
        return False

def unpack_content(encoded_content: str, logger: Optional[logging.Logger] = None) -> Optional[bytes]:
    """Unpack base64-encoded and zlib-compressed content."""
    logger = logger or default_logger
    try:
        if encoded_content:
            compressed_content = base64.b64decode(encoded_content)
            return zlib.decompress(compressed_content)
        return None
    except Exception as e:
        logger.error(f"Error unpacking content: {e}")
        return None

def get_records_to_search(file_id: str, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """Fetch records to search from the database."""
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        query = """
            SELECT EntryID, ProductModel AS SearchString, 'model_only' AS SearchType, FileID
            FROM utb_ImageScraperRecords 
            WHERE FileID = ? AND Step1 IS NULL
            ORDER BY EntryID, SearchType
        """
        with pyodbc.connect(conn_str) as conn:
            df = pd.read_sql_query(query, conn, params=[file_id])
            if not df.empty and (df["FileID"] != file_id).any():
                logger.error(f"Found rows with incorrect FileID for {file_id}")
                df = df[df["FileID"] == file_id]
            logger.info(f"Got {len(df)} search records for FileID: {file_id}")
            return df[["EntryID", "SearchString", "SearchType"]]
    except pyodbc.Error as e:
        logger.error(f"Database error getting records for FileID {file_id}: {e}")
        return pd.DataFrame()
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")
        return pd.DataFrame()

def check_endpoint_health(endpoint: str, timeout: int = 5, logger: Optional[logging.Logger] = None) -> bool:
    """Check if an endpoint is healthy by querying its Google health check."""
    logger = logger or default_logger
    health_url = f"{endpoint}/health/google"
    try:
        response = requests.get(health_url, timeout=timeout)
        response.raise_for_status()
        return "Google is reachable" in response.json().get("status", "")
    except RequestException as e:
        logger.warning(f"Endpoint {endpoint} health check failed: {e}")
        return False

def get_healthy_endpoint(endpoints: List[str], logger: Optional[logging.Logger] = None) -> Optional[str]:
    """Return the first healthy endpoint."""
    logger = logger or default_logger
    for endpoint in endpoints:
        if check_endpoint_health(endpoint, logger=logger):
            logger.info(f"Selected healthy endpoint: {endpoint}")
            return endpoint
    logger.error("No healthy endpoints found")
    return None

def process_search_row_gcloud(search_string: str, entry_id: int, logger: logging.Logger = None, remaining_retries: int = 15, total_attempts: list = None) -> pd.DataFrame:
    """
    Process a search row using TheDataProxy API with multiple regions and x-api-key, with a retry limit tied to the parent process.
    
    Args:
        search_string (str): The query string to search for.
        entry_id (int): The entry ID associated with the search.
        logger (logging.Logger, optional): Logger instance for logging messages.
        remaining_retries (int): Number of retries remaining from the parent process.
        total_attempts (list): Shared counter for total attempts across primary and GCloud.
    
    Returns:
        pd.DataFrame: DataFrame containing search results, or empty if all attempts fail.
    """
    logger = logger or logging.getLogger(__name__)
    if not search_string or len(search_string.strip()) < 3:
        logger.warning(f"Invalid search string for EntryID {entry_id}: '{search_string}'")
        return pd.DataFrame()

    total_attempts = total_attempts or [0]
    base_url = "https://api.thedataproxy.com/v2/proxy/fetch"
    api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiMGRkZTIwZjAtNjlmZS00ODc2LWE0MmItMTY1YzM1YTk4MzMyIiwiaWF0IjoxNzQ2NDcyMDQzLjc0Njk3NiwiZXhwIjoxNzc4MDA4MDQzLjc0Njk4MX0.MduHUL3BeVw9k6Tk3mbOVHuZyT7k49d01ddeFqnmU8k"
    regions = [
        "northamerica-northeast", "southamerica", "us-central", "us-east",
        "us-west", "europe", "australia"
    ]
    headers = {
        "accept": "application/json",
        "x-api-key": api_key,
        "Content-Type": "application/json"
    }

    session = Session()
    retry_strategy = Retry(total=3, status_forcelist=[500, 502, 503, 504], backoff_factor=1)
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
    session.headers.update(headers)

    def log_gcloud_retry(attempt: int) -> bool:
        total_attempts[0] += 1
        if total_attempts[0] > remaining_retries:
            logger.error(f"Exceeded remaining retries ({remaining_retries}) for EntryID {entry_id} at GCloud attempt {attempt}")
            return False
        logger.info(f"GCloud attempt {attempt} (Total attempts: {total_attempts[0]}/{remaining_retries}) for EntryID {entry_id}")
        return True

    search_url = f"https://www.google.com/search?q={urllib.parse.quote(search_string)}&tbm=isch"
    for attempt, region in enumerate(regions, 1):
        if not log_gcloud_retry(attempt):
            break
        fetch_endpoint = f"{base_url}?region={region}"
        try:
            logger.info(f"Attempt {attempt}: Fetching {search_url} via {fetch_endpoint} with region {region}")
            response = session.post(fetch_endpoint, json={"url": search_url}, timeout=30)
            response.raise_for_status()
            result = response.json().get("result")
            if not result:
                logger.warning(f"No result returned for EntryID {entry_id} in region {region}")
                continue
            results_html_bytes = result if isinstance(result, bytes) else result.encode("utf-8")
            df = process_search_result(results_html_bytes, results_html_bytes, entry_id, logger)
            if not df.empty:
                irrelevant_keywords = ['wallpaper', 'sofa', 'furniture', 'decor', 'stock photo']
                df = df[~df['ImageDesc'].str.lower().str.contains('|'.join(irrelevant_keywords), na=False)]
                logger.info(f"Filtered out irrelevant results, kept {len(df)} rows")
            return df
        except RequestException as e:
            logger.warning(f"Attempt {attempt} failed for {fetch_endpoint} in region {region}: {e}")
            continue
    logger.error(f"All GCloud attempts failed for EntryID {entry_id} after {total_attempts[0]} total attempts")
    return pd.DataFrame()

import pandas as pd
import re
import urllib.parse
import requests
from requests import Session
from requests.exceptions import RequestException
from urllib3.util.retry import Retry
from typing import Optional
import logging
import pandas as pd
import re
import urllib.parse
import requests
from requests import Session
from requests.exceptions import RequestException
from urllib3.util.retry import Retry
from typing import Optional
import logging

def process_search_row(
    search_string: str,
    endpoint: str,
    entry_id: int,
    logger: logging.Logger = None,
    search_type: str = "default",
    max_retries: int = 15,
    brand: Optional[str] = None,
    category: Optional[str] = None
) -> pd.DataFrame:
    """
    Process a search row with the specified search_type, falling back to GCloud on errors.
    
    Args:
        search_string (str): The query string to search for.
        endpoint (str): The primary endpoint URL for fetching search results.
        entry_id (int): The entry ID associated with the search.
        logger (logging.Logger, optional): Logger instance for logging messages.
        search_type (str): Type of search to perform ("default", "brand", "retry_with_alternative").
        max_retries (int): Maximum number of total retries (primary + GCloud).
        brand (Optional[str]): Brand name to include in the query.
        category (Optional[str]): Category to include in the query (e.g., "sneakers").
    
    Returns:
        pd.DataFrame: DataFrame containing search results, or empty if all attempts fail.
    """
    logger = logger or logging.getLogger(__name__)
    if not search_string or not endpoint:
        logger.warning(f"Invalid input for EntryID {entry_id}: search_string={search_string}, endpoint={endpoint}")
        return pd.DataFrame()

    total_attempts = [0]
    session = Session()
    retry_strategy = Retry(total=3, status_forcelist=[500, 502, 503, 504], backoff_factor=1)
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))

    def log_retry_status(attempt_type: str, attempt_num: int) -> bool:
        total_attempts[0] += 1
        if total_attempts[0] > max_retries:
            logger.error(f"Exceeded max retries ({max_retries}) for EntryID {entry_id} after {attempt_type} attempt {attempt_num}")
            return False
        logger.info(f"{attempt_type} attempt {attempt_num} (Total attempts: {total_attempts[0]}/{max_retries}) for EntryID {entry_id}")
        return True

    try:
        if search_type == "retry_with_alternative":
            delimiters = r'[\s\-_,]+'
            words = re.split(delimiters, search_string.strip())
            for i in range(len(words), 0, -1):
                partial_search = ' '.join(words[:i])
                query = partial_search  # Remove quotes to avoid 400 errors
                if brand:
                    query += f" {brand}"
                if category:
                    query += f" {category}"
                search_url = f"https://www.google.com/search?q={urllib.parse.quote(query)}&tbm=isch"
                fetch_endpoint = f"{endpoint}/fetch"
                attempt_num = 1
                while attempt_num <= retry_strategy.total + 1:
                    if not log_retry_status("Primary", attempt_num):
                        return pd.DataFrame()
                    try:
                        logger.info(f"Fetching URLs via {fetch_endpoint} with query: {query}")
                        response = session.post(fetch_endpoint, json={"url": search_url}, timeout=60)
                        response.raise_for_status()
                        result = response.json().get("result")
                        if result:
                            results_html_bytes = result if isinstance(result, bytes) else result.encode("utf-8")
                            df = process_search_result(results_html_bytes, results_html_bytes, entry_id, logger)
                            if not df.empty:
                                # Enhanced filtering
                                irrelevant_keywords = ['wallpaper', 'furniture', 'decor', 'stock photo', 'pistol', 'mattress', 'trunk', 'clutch', 'solenoid']
                                df = df[df['ImageDesc'].str.lower().str.contains('scotch|soda|sneaker|shoe', na=False) &
                                       ~df['ImageDesc'].str.lower().str.contains('|'.join(irrelevant_keywords), na=False)]
                                logger.info(f"Filtered out irrelevant results, kept {len(df)} rows")
                                return df
                        logger.warning(f"No results for query: {query}, moving to next attempt or split")
                        break
                    except RequestException as e:
                        logger.warning(f"Primary attempt {attempt_num} failed for {fetch_endpoint} with query '{query}': {e}")
                        attempt_num += 1
                        if attempt_num > retry_strategy.total + 1:
                            break
                if total_attempts[0] < max_retries:
                    gcloud_df = process_search_row_gcloud(partial_search, entry_id, logger, max_retries - total_attempts[0], total_attempts)
                    if not gcloud_df.empty:
                        logger.info(f"GCloud fallback succeeded for EntryID {entry_id} with {len(gcloud_df)} images using query: {partial_search}")
                        return gcloud_df
                    logger.warning(f"GCloud fallback failed for query: {partial_search}, trying next split")
            logger.warning(f"No valid data for EntryID {entry_id} after all splits and retries")
            return pd.DataFrame()
        else:
            query = search_string
            if brand:
                query += f" {brand}"
            if category:
                query += f" {category}"
            search_url = f"https://www.google.com/search?q={urllib.parse.quote(query + ' images')}&tbm=isch"
            attempt_num = 1
            while attempt_num <= retry_strategy.total + 1:
                if not log_retry_status("Primary", attempt_num):
                    return pd.DataFrame()
                try:
                    logger.info(f"Searching {search_url}")
                    response = session.get(search_url, timeout=60)
                    response.raise_for_status()
                    result = response.json().get("body")
                    if not result:
                        logger.warning(f"No body in response for {search_url}")
                        raise RequestException("Empty response body")
                    unpacked_html = unpack_content(result, logger)
                    if not unpacked_html or len(unpacked_html) < 100:
                        logger.warning(f"Invalid HTML for {search_url}")
                        raise RequestException("Invalid HTML content")
                    df = process_search_result(unpacked_html, unpacked_html, entry_id, logger)
                    if not df.empty:
                        # Enhanced filtering
                        irrelevant_keywords = ['wallpaper', 'furniture', 'decor', 'stock photo', 'pistol', 'mattress', 'trunk', 'clutch', 'solenoid']
                        df = df[df['ImageDesc'].str.lower().str.contains('scotch|soda|sneaker|shoe', na=False) &
                               ~df['ImageDesc'].str.lower().str.contains('|'.join(irrelevant_keywords), na=False)]
                        logger.info(f"Filtered out irrelevant results, kept {len(df)} rows")
                        return df
                    logger.warning(f"No valid data for EntryID {entry_id}")
                    return df
                except RequestException as e:
                    logger.warning(f"Primary attempt {attempt_num} failed for {search_url}: {e}")
                    attempt_num += 1
                    if attempt_num > retry_strategy.total + 1:
                        break
            if total_attempts[0] < max_retries:
                gcloud_df = process_search_row_gcloud(search_string, entry_id, logger, max_retries - total_attempts[0], total_attempts)
                if not gcloud_df.empty:
                    logger.info(f"GCloud fallback succeeded for EntryID {entry_id} with {len(gcloud_df)} images")
                    return gcloud_df
                logger.error(f"GCloud fallback also failed for EntryID {entry_id} after {total_attempts[0]} total attempts")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Unexpected error processing EntryID {entry_id} after {total_attempts[0]} attempts: {e}", exc_info=True)
        return pd.DataFrame()
def update_sort_order(file_id: str, logger: Optional[logging.Logger] = None) -> Optional[list[dict]]:
    """
    Update sort order for all entries in a file using the same logic as update_search_sort_order.
    This is a batch version that processes all entries in a file.
    """
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        logger.info(f"🔄 Starting batch SortOrder update for FileID: {file_id}")
        
        # Get all entries for this file
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT EntryID, ProductBrand, ProductModel, ProductColor, ProductCategory 
                FROM utb_ImageScraperRecords 
                WHERE FileID = ?
                """, 
                (file_id,)
            )
            entries = cursor.fetchall()
        
        if not entries:
            logger.warning(f"No entries found for FileID: {file_id}")
            return []
            
        # Process each entry using update_search_sort_order
        results = []
        success_count = 0
        failure_count = 0
        
        for entry in entries:
            entry_id, brand, model, color, category = entry
            try:
                entry_results = update_search_sort_order(
                    file_id=str(file_id),
                    entry_id=str(entry_id),
                    brand=brand,
                    model=model,
                    color=color,
                    category=category,
                    logger=logger
                )
                
                if entry_results:
                    results.extend(entry_results)
                    success_count += 1
                else:
                    failure_count += 1
                    logger.warning(f"No results for EntryID {entry_id}")
            except Exception as e:
                failure_count += 1
                logger.error(f"Error processing EntryID {entry_id}: {e}", exc_info=True)
        
        logger.info(f"Completed batch SortOrder update for FileID {file_id}: {success_count} entries successful, {failure_count} failed")
        
        # Verify overall results
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT COUNT(DISTINCT t.EntryID)
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                WHERE r.FileID = ? AND t.SortOrder > 0
                """,
                (file_id,)
            )
            positive_entries = cursor.fetchone()[0]
            cursor.execute(
                """
                SELECT COUNT(DISTINCT t.EntryID)
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                WHERE r.FileID = ? AND t.SortOrder = 0
                """,
                (file_id,)
            )
            brand_match_entries = cursor.fetchone()[0]
            cursor.execute(
                """
                SELECT COUNT(DISTINCT t.EntryID)
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                WHERE r.FileID = ? AND t.SortOrder < 0
                """,
                (file_id,)
            )
            no_match_entries = cursor.fetchone()[0]
            
            logger.info(f"Verification for FileID {file_id}: "
                       f"{positive_entries} entries with model matches, "
                       f"{brand_match_entries} entries with brand matches only, "
                       f"{no_match_entries} entries with no matches")
        
        return results
    except Exception as e:
        logger.error(f"Error in batch SortOrder update for FileID {file_id}: {e}", exc_info=True)
        return None
def set_sort_order_negative_four_for_zero_match(file_id: str, logger: Optional[logging.Logger] = None) -> Optional[list[dict]]:
    """
    Set SortOrder to -4 for records with match_score = 0 for a given file_id.
    Handles malformed JSON gracefully.
    
    Args:
        file_id (str): Identifier for the file
        logger (Optional[logging.Logger]): Logger instance for logging messages
    
    Returns:
        Optional[List[Dict]]: List of updated records with ResultID, EntryID, and new SortOrder,
                            or None if a critical error occurs
    """
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            logger.info(f"🔄 Setting SortOrder to -4 for match_score = 0 for FileID: {file_id}")
            
            query = """
                SELECT 
                    t.ResultID, 
                    t.EntryID,
                    CASE 
                        WHEN ISJSON(t.AiJson) = 1 
                        THEN ISNULL(TRY_CAST(JSON_VALUE(t.AiJson, '$.match_score') AS FLOAT), 0)
                        ELSE 0 
                    END AS match_score,
                    t.SortOrder,
                    t.AiJson
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                WHERE r.FileID = ?
            """
            df = pd.read_sql_query(query, conn, params=(file_id,))
            
            if df.empty:
                logger.info(f"No records found for FileID: {file_id}")
                return []

            zero_match_df = df[df['match_score'] == 0].copy()
            invalid_json_df = df[df['AiJson'].notnull() & (df['AiJson'].str.strip() != '') & (df['match_score'] == 0)]
            for _, row in invalid_json_df.iterrows():
                if row['AiJson'] and not pd.isna(row['AiJson']):
                    logger.warning(f"Record with potentially malformed JSON - ResultID: {row['ResultID']}, AiJson: {row['AiJson'][:100]}...")

            if zero_match_df.empty:
                logger.info(f"No records found with match_score = 0 for FileID: {file_id}")
                return []

            updates = []
            for _, row in zero_match_df.iterrows():
                if row["SortOrder"] != -4:
                    updates.append((-4, int(row["ResultID"])))

            if updates:
                cursor.executemany(
                    "UPDATE utb_ImageScraperResult SET SortOrder = ? WHERE ResultID = ?",
                    updates
                )
                conn.commit()
                logger.info(f"Updated SortOrder to -4 for {len(updates)} records with match_score = 0 for FileID: {file_id}")
            else:
                logger.info(f"No SortOrder updates needed for FileID: {file_id} (all already -4)")

            cursor.execute(
                """
                SELECT ResultID, EntryID, SortOrder
                FROM utb_ImageScraperResult
                WHERE EntryID IN (SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?)
                AND (
                    (ISJSON(AiJson) = 1 AND ISNULL(TRY_CAST(JSON_VALUE(AiJson, '$.match_score') AS FLOAT), 0) = 0)
                    OR (ISJSON(AiJson) = 0 AND AiJson IS NOT NULL)
                )
                """, (file_id,)
            )
            results = [{"ResultID": r[0], "EntryID": r[1], "SortOrder": r[2]} for r in cursor.fetchall()]
            return results if results else []

    except pyodbc.Error as e:
        logger.error(f"Critical error setting SortOrder to -4 for match_score = 0 for FileID {file_id}: {e}", exc_info=True)
        return None
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")
        return None

def fetch_missing_images(file_id: str, limit: int = 1000, ai_analysis_only: bool = True, logger=None) -> pd.DataFrame:
    """
    Fetch images needing AI analysis for a given file_id, excluding negative SortOrder values.

    Args:
        file_id (str): Identifier for the file.
        limit (int): Maximum number of images to fetch (default: 1000).
        ai_analysis_only (bool): If True, fetch images needing AI analysis; if False, fetch records missing ImageUrl.
        logger (Optional[logging.Logger]): Logger instance for logging messages.

    Returns:
        pd.DataFrame: DataFrame with image data to process.
    """
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as conn:
            if ai_analysis_only:
                query = """
                    SELECT t.ResultID, t.EntryID, t.ImageUrl, t.ImageUrlThumbnail,
                           r.ProductBrand, r.ProductCategory, r.ProductColor
                    FROM utb_ImageScraperResult t
                    INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                    WHERE r.FileID = ?
                    AND (t.AiJson IS NULL OR t.AiJson = '' OR ISJSON(t.AiJson) = 0)
                    AND t.ImageUrl IS NOT NULL AND t.ImageUrl <> ''
                    AND t.SortOrder > 0
                    ORDER BY t.ResultID
                    OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
                """
                params = (file_id, limit)
            else:
                query = """
                    SELECT r.EntryID, r.FileID, r.ProductBrand, r.ProductCategory, r.ProductColor,
                           t.ResultID, t.ImageUrl, t.ImageUrlThumbnail
                    FROM utb_ImageScraperRecords r
                    LEFT JOIN utb_ImageScraperResult t ON r.EntryID = t.EntryID AND t.SortOrder >= 0
                    WHERE r.FileID = ? AND t.ResultID IS NULL
                    ORDER BY r.EntryID
                    OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
                """
                params = (file_id, limit)

            df = pd.read_sql_query(query, conn, params=params)
            logger.info(f"Fetched {len(df)} images for FileID {file_id}, ai_analysis_only={ai_analysis_only}")
            return df
    except pyodbc.Error as e:
        logger.error(f"Database error fetching missing images for FileID {file_id}: {e}", exc_info=True)
        return pd.DataFrame()
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")
        return pd.DataFrame()
import logging
import pandas as pd
import pyodbc
import re
from typing import List, Dict, Optional, Tuple
from unidecode import unidecode

def get_endpoint(logger: Optional[logging.Logger] = None) -> str:
    """Get a random unblocked endpoint from the database."""
    logger = logger or default_logger
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT TOP 1 EndpointURL FROM utb_Endpoints WHERE EndpointIsBlocked = 0 ORDER BY NEWID()")
            result = cursor.fetchone()
            endpoint = result[0] if result else "No EndpointURL"
            logger.info(f"Retrieved endpoint: {endpoint}")
            return endpoint
    except pyodbc.Error as e:
        logger.error(f"Database error getting endpoint: {e}")
        return "No EndpointURL"

def remove_endpoint(endpoint: str, logger: Optional[logging.Logger] = None) -> None:
    """Mark an endpoint as blocked in the database."""
    logger = logger or default_logger
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE utb_Endpoints SET EndpointIsBlocked = 1 WHERE EndpointURL = ?", (endpoint,))
            conn.commit()
            logger.info(f"Marked endpoint as blocked: {endpoint}")
    except pyodbc.Error as e:
        logger.error(f"Error marking endpoint as blocked: {e}")

import re
import logging
import pandas as pd
import pyodbc
from typing import List, Dict, Optional, Tuple
from unidecode import unidecode  # Make sure to install this package

def clean_string(text):
    """Clean and normalize a string for better matching."""
    if pd.isna(text) or text is None:
        return ''
    try:
        text = unidecode(str(text).encode().decode('unicode_escape'))
        return re.sub(r'[^\w\d\-_]', '', text.upper()).strip()
    except:
        # Fallback if unidecode fails
        if not isinstance(text, str):
            return ''
        return re.sub(r'[^\w\d\-_]', '', str(text).upper()).strip()

def normalize_model(model):
    """Normalize model numbers/names."""
    if not isinstance(model, str):
        return str(model).strip().lower()
    return model.strip().lower()

def generate_aliases(model):
    """Generate different variations of a model name."""
    if not isinstance(model, str):
        model = str(model)
    
    if not model or model.strip() == '':
        return []
        
    aliases = {model, model.lower(), model.upper()}
    aliases.add(model.replace("_", "-"))
    aliases.add(model.replace("_", ""))
    aliases.add(model.replace("_", " "))
    aliases.add(model.replace("_", "/"))
    aliases.add(model.replace("_", "."))
    digits_only = re.sub(r'[_/.-]', '', model)
    if digits_only.isdigit():
        aliases.add(digits_only)
    return [a for a in aliases if a and len(a) >= len(model) - 2]

def filter_model_results(df: pd.DataFrame, debug: bool = False, logger: Optional[logging.Logger] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    logger = logger or logging.getLogger(__name__)
    try:    
        if debug:
            logger.debug("\nDebugging and Filtering Model Results:")

        # Validate required columns
        required_columns = ['ProductModel', 'ImageSource', 'ImageUrl', 'ImageDesc']
        if not all(col in df.columns for col in required_columns):
            missing_cols = [col for col in required_columns if col not in df.columns]
            raise ValueError(f"DataFrame missing required columns: {missing_cols}")

        # Initialize lists for indices
        keep_indices = []
        discarded_indices = []
        
        # Handle case where df is empty
        if df.empty:
            logger.warning("Empty DataFrame provided to filter_model_results")
            return df.copy(), df.copy()
        
        for idx, row in df.iterrows():
            # Validate ProductModel
            if pd.isna(row['ProductModel']) or not str(row['ProductModel']).strip():
                logger.warning(f"Skipping row {idx} due to missing or empty ProductModel")
                discarded_indices.append(idx)
                continue
                
            model = str(row['ProductModel'])
            # Generate aliases for the model
            aliases = generate_aliases(model)
            if not aliases:
                logger.warning(f"No valid aliases generated for model '{model}' in row {idx}")
                discarded_indices.append(idx)
                continue

            # Get column content - handle nulls/NA values safely
            source = "" if pd.isna(row['ImageSource']) else str(row['ImageSource'])
            url = "" if pd.isna(row['ImageUrl']) else str(row['ImageUrl'])
            desc = "" if pd.isna(row['ImageDesc']) else str(row['ImageDesc'])

            if debug:
                logger.debug(f"\nDebugging Row {idx}: Model='{model}', Aliases={aliases}")
                logger.debug(f"  ImageSource: {source}")
                logger.debug(f"  ImageUrl: {url}")
                logger.debug(f"  ImageDesc: {desc}")

            # Check for exact matches
            has_exact_match = False
            for alias in aliases:
                try:
                    # Use strict regex with word boundaries for whole-word exact match
                    alias_pattern = rf'\b{re.escape(alias.lower())}\b'
                    if source and re.search(alias_pattern, source.lower()):
                        has_exact_match = True
                        if debug:
                            logger.debug(f"  Exact match found: '{alias}' in ImageSource")
                    if url and re.search(alias_pattern, url.lower()):
                        has_exact_match = True
                        if debug:
                            logger.debug(f"  Exact match found: '{alias}' in ImageUrl")
                    if desc and re.search(alias_pattern, desc.lower()):
                        has_exact_match = True
                        if debug:
                            logger.debug(f"  Exact match found: '{alias}' in ImageDesc")
                except re.error as e:
                    logger.warning(f"Regex error for alias '{alias}' in row {idx}: {e}")

            # Decide whether to keep the row
            if has_exact_match:
                keep_indices.append(idx)
                if debug:
                    logger.debug(f"  Keeping row {idx} in exact matches: Exact match found")
            else:
                discarded_indices.append(idx)
                if debug:
                    logger.debug(f"  Discarding row {idx}: No exact match found")

        # Create output DataFrames
        exact_df = df.loc[keep_indices].copy() if keep_indices else pd.DataFrame(columns=df.columns)
        discarded_df = df.loc[discarded_indices].copy() if discarded_indices else pd.DataFrame(columns=df.columns)
        
        logger.info(f"Filtered {len(exact_df)} rows with exact matches and {len(discarded_df)} rows discarded")
        return exact_df, discarded_df
    except Exception as e:
            logger.error(f"Error in filter_model_results: {e}", exc_info=True)
            return pd.DataFrame(columns=df.columns), df.copy()

def calculate_priority(row, exact_df, model_clean, model_aliases, brand_clean, brand_aliases, logger):
    """
    Calculate priority for a search result, prioritizing exact model matches and brand matches.
    
    Returns:
        int: Priority score:
            1 = Exact model match
            2 = Brand match only
            3 = No match
    """
    try:
        model_matched = False
        brand_matched = False

        desc_clean = row['ImageDesc_clean'].lower() if 'ImageDesc_clean' in row else ''
        source_clean = row['ImageSource_clean'].lower() if 'ImageSource_clean' in row else ''
        url_clean = row['ImageUrl_clean'].lower() if 'ImageUrl_clean' in row else ''
        
        # Check if row index is in exact_df index for model match
        if model_clean and row.name in exact_df.index:
            model_matched = True
            logger.debug(f"ResultID {row.get('ResultID', 'unknown')}: Model match found in exact_df")

        # Check for brand match in any of the fields
        if brand_clean:
            for alias in brand_aliases:
                if alias and (
                    alias.lower() in desc_clean or
                    alias.lower() in source_clean or
                    alias.lower() in url_clean or
                    ('ProductBrand_clean' in row and alias.lower() in row['ProductBrand_clean'].lower())
                ):
                    brand_matched = True
                    logger.debug(f"ResultID {row.get('ResultID', 'unknown')}: Brand match found for alias '{alias}'")
                    break

        logger.debug(f"ResultID {row.get('ResultID', 'unknown')}: model_matched={model_matched}, brand_matched={brand_matched}")

        if model_matched:
            return 1  # Model match (with or without brand)
        if brand_matched:
            return 2  # Brand match only
        return 3  # No match
    except Exception as e:
        logger.error(f"Error in calculate_priority for ResultID {row.get('ResultID', 'unknown')}: {e}")
        return 3  # Default to low priority on error

def update_search_sort_order(
    file_id: str,
    entry_id: str,
    brand: Optional[str] = None,
    model: Optional[str] = None,
    color: Optional[str] = None,
    category: Optional[str] = None,
    logger: Optional[logging.Logger] = None
) -> Optional[List[Dict]]:
    logger = logger or logging.getLogger(__name__)
    try:
        file_id = int(file_id)
        entry_id = int(entry_id)
        logger.info(f"🔄 Starting SortOrder update for FileID: {file_id}, EntryID: {entry_id}, ...")
        with engine.begin() as conn:
            conn.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))

            # Fetch attributes if not provided
            if not all([brand, model]):
                result = conn.execute(
                    text("""
                        SELECT ProductBrand, ProductModel, ProductColor, ProductCategory
                        FROM utb_ImageScraperRecords
                        WHERE FileID = :file_id AND EntryID = :entry_id
                    """),
                    {"file_id": file_id, "entry_id": entry_id}
                ).fetchone()
                if result:
                    brand, model, color, category = result
                    logger.info(f"Fetched attributes - Brand: {brand}, Model: {model}, ...")
                else:
                    logger.warning(f"No attributes found for FileID: {file_id}, EntryID: {entry_id}")
                    brand = brand or ''
                    model = model or ''
                    color = color or ''
                    category = category or ''

            # Fetch results
            query = text("""
                SELECT 
                    t.ResultID, 
                    t.EntryID,
                    ISNULL(CAST(JSON_VALUE(t.AiJson, '$.match_score') AS FLOAT), 0) AS match_score,
                    ISNULL(t.SortOrder, 0) AS SortOrder,
                    t.ImageDesc, 
                    t.ImageSource, 
                    t.ImageUrl,
                    r.ProductBrand, 
                    r.ProductModel
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                WHERE r.FileID = :file_id AND t.EntryID = :entry_id
            """)
            df = pd.read_sql_query(query, conn, params={"file_id": file_id, "entry_id": entry_id})
            if df.empty:
                logger.warning(f"No eligible data found for FileID: {file_id}, EntryID: {entry_id}")
                return []

            logger.info(f"Fetched {len(df)} rows for EntryID {entry_id}")
            logger.debug(f"DataFrame columns after fetch: {df.columns.tolist()}")

            # Define brand aliases
            brand_aliases_dict = {
                "Scotch & Soda": ["Scotch and Soda", "Scotch Soda", "Scotch&Soda", "ScotchAndSoda", "Scotch", "S&S"],
                "Adidas": ["Adidas AG", "Adidas Originals"],
                "BAPE": ["A Bathing Ape", "BATHING APE"]
            }
            
            # Clean data
            brand_clean = clean_string(brand) if brand else ''
            model_clean = normalize_model(model) if model else ''
            model_aliases = generate_aliases(model) if model else []
            brand_aliases = []
            for b, aliases in brand_aliases_dict.items():
                if clean_string(b) == brand_clean:
                    brand_aliases = [clean_string(b)] + [clean_string(a) for a in aliases]
                    break
            if not brand_aliases and brand_clean:
                brand_aliases = [brand_clean]

            # Process column cleaning
            required_cols = ["ImageDesc", "ImageSource", "ImageUrl", "ProductBrand", "ProductModel"]
            for col in required_cols:
                if col not in df.columns:
                    logger.warning(f"Column {col} missing; adding empty column")
                    df[col] = ''
                df[f"{col}_clean"] = df[col].fillna('').apply(clean_string)

            # Filter exact matches
            exact_df, discarded_df = filter_model_results(df, debug=True, logger=logger)
            logger.info(f"Filtered {len(exact_df)} exact matches and {len(discarded_df)} discarded rows for EntryID {entry_id}")

            # Apply priority
            df['priority'] = df.apply(
                lambda row: calculate_priority(
                    row, exact_df, model_clean, model_aliases, brand_clean, brand_aliases, logger
                ), axis=1
            )
            logger.info(f"Priority distribution: {df['priority'].value_counts().to_dict()}")
            logger.debug(f"DataFrame columns after priority: {df.columns.tolist()}")

            # Assign SortOrder
            df['new_sort_order'] = -2  # Default for no match (priority 3)
            logger.debug(f"DataFrame columns after new_sort_order init: {df.columns.tolist()}")

            # Set brand matches to 0
            brand_match_rows = df[df['priority'] == 2]
            if not brand_match_rows.empty:
                df.loc[brand_match_rows.index, 'new_sort_order'] = 0

            # Set model matches to positive, sequential values
            model_match_rows = df[df['priority'] == 1]
            if not model_match_rows.empty:
                model_match_df = model_match_rows.sort_values('match_score', ascending=False)
                df.loc[model_match_df.index, 'new_sort_order'] = range(1, len(model_match_df) + 1)

            logger.debug(f"DataFrame columns before validation: {df.columns.tolist()}")

            # Validate model matches
            for _, row in df[(df['new_sort_order'] > 0)].iterrows():
                model_verified = False
                for alias in model_aliases:
                    alias_lower = alias.lower()
                    if ((not pd.isna(row['ImageDesc']) and alias_lower in row['ImageDesc'].lower()) or
                        (not pd.isna(row['ImageSource']) and alias_lower in row['ImageSource'].lower()) or
                        (not pd.isna(row['ImageUrl']) and alias_lower in row['ImageUrl'].lower())):
                        model_verified = True
                        break
                if not model_verified:
                    logger.warning(f"Validation failed for ResultID {row['ResultID']}: Model match not verified, downgrading")
                    brand_found = False
                    for alias in brand_aliases:
                        alias_lower = alias.lower()
                        if ((not pd.isna(row['ImageDesc']) and alias_lower in row['ImageDesc'].lower()) or
                            (not pd.isna(row['ImageSource']) and alias_lower in row['ImageSource'].lower()) or
                            (not pd.isna(row['ImageUrl']) and alias_lower in row['ImageUrl'].lower())):
                            brand_found = True
                            break
                    df.at[row.name, 'new_sort_order'] = 0 if brand_found else -2
                    df.at[row.name, 'priority'] = 2 if brand_found else 3

            logger.debug(f"DataFrame columns before update loop: {df.columns.tolist()}")

            if 'new_sort_order' not in df.columns:
                logger.error(f"new_sort_order column missing in DataFrame. Columns: {df.columns.tolist()}")
                return []

            # Update database
            update_count = 0
            success_count = 0
            for _, row in df[['ResultID', 'new_sort_order']].iterrows():
                if row['new_sort_order'] == row.get('SortOrder', None):
                    continue
                try:
                    result_id = int(row['ResultID'])
                    new_sort_order = int(row['new_sort_order'])
                    conn.execute(
                        text("UPDATE utb_ImageScraperResult SET SortOrder = :sort_order WHERE ResultID = :result_id"),
                        {"sort_order": new_sort_order, "result_id": result_id}
                    )
                    update_count += 1
                    success_count += 1
                    logger.debug(f"Updated SortOrder to {new_sort_order} for ResultID {result_id}")
                except SQLAlchemyError as e:
                    logger.error(f"Failed to update ResultID {row['ResultID']}: {e}")
                    raise

            logger.info(f"Updated {success_count}/{update_count} rows for EntryID {entry_id}")

            # Final verification
            verify_query = text("""
                SELECT ResultID, EntryID, SortOrder, ImageDesc, ImageSource, ImageUrl
                FROM utb_ImageScraperResult
                WHERE EntryID = :entry_id
                ORDER BY SortOrder DESC
            """)
            results = [
                {
                    "ResultID": r[0], 
                    "EntryID": r[1], 
                    "SortOrder": r[2], 
                    "ImageDesc": r[3], 
                    "ImageSource": r[4], 
                    "ImageUrl": r[5]
                }
                for r in conn.execute(verify_query, {"entry_id": entry_id}).fetchall()
            ]

            positive_count = sum(1 for r in results if r['SortOrder'] is not None and r['SortOrder'] > 0)
            zero_count = sum(1 for r in results if r['SortOrder'] == 0)
            negative_count = sum(1 for r in results if r['SortOrder'] is not None and r['SortOrder'] < 0)
            null_count = sum(1 for r in results if r['SortOrder'] is None)
            logger.info(f"Verification for EntryID {entry_id}: ...")
            if null_count > 0:
                logger.warning(f"Found {null_count} rows with NULL SortOrder ...")
            return results

    except SQLAlchemyError as e:
        logger.error(f"Database error updating SortOrder ...: {e}", exc_info=True)
        return None
    except ValueError as e:
        logger.error(f"ValueError updating SortOrder ...: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Unexpected error updating SortOrder ...: {e}", exc_info=True)
        return None
def update_initial_sort_order(file_id: str, logger: Optional[logging.Logger] = None) -> Optional[list[dict]]:
    """
    Set initial sort order for results, preserving existing positive SortOrder values.
    """
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN TRANSACTION")
            cursor.execute("UPDATE utb_ImageScraperRecords SET Step1 = NULL WHERE FileID = ?", (file_id,))
            cursor.execute(
                """
                UPDATE utb_ImageScraperResult 
                SET SortOrder = NULL 
                WHERE EntryID IN (SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?)
                AND SortOrder < 0
                """,
                (file_id,)
            )
            cursor.execute("""
                WITH duplicates AS (
                    SELECT ResultID, EntryID,
                           ROW_NUMBER() OVER (PARTITION BY EntryID, ImageUrl ORDER BY ResultID) AS row_num
                    FROM utb_ImageScraperResult
                    WHERE EntryID IN (SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?)
                    AND (SortOrder IS NULL OR SortOrder < 0)
                )
                UPDATE utb_ImageScraperResult
                SET SortOrder = -1
                FROM utb_ImageScraperResult r
                INNER JOIN duplicates d ON r.ResultID = d.ResultID AND r.EntryID = d.EntryID
                WHERE d.row_num > 1
            """, (file_id,))
            cursor.execute("""
                WITH toupdate AS (
                    SELECT t.*, 
                           ROW_NUMBER() OVER (PARTITION BY t.EntryID ORDER BY t.ResultID) AS seqnum
                    FROM utb_ImageScraperResult t 
                    INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID 
                    WHERE r.FileID = ? AND t.SortOrder IS NULL
                ) 
                UPDATE toupdate 
                SET SortOrder = seqnum
            """, (file_id,))
            cursor.execute("COMMIT")
            results = cursor.execute(
                """
                SELECT ResultID, EntryID, SortOrder 
                FROM utb_ImageScraperResult 
                WHERE EntryID IN (SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?) 
                ORDER BY EntryID, SortOrder
                """,
                (file_id,)
            ).fetchall()
            logger.info(f"Set initial SortOrder for FileID: {file_id}")
            return [{"ResultID": r[0], "EntryID": r[1], "SortOrder": r[2]} for r in results]
    except pyodbc.Error as e:
        logger.error(f"Database error setting initial SortOrder: {e}", exc_info=True)
        if 'cursor' in locals():
            cursor.execute("ROLLBACK")
        return None
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()

def get_images_excel_db(file_id, logger=None):
    """Retrieve images for Excel export."""
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as conn:
            query = """
                SELECT 
                    s.ExcelRowID, 
                    r.ImageUrl, 
                    r.ImageUrlThumbnail, 
                    s.ProductBrand AS Brand, 
                    s.ProductModel AS Style, 
                    s.ProductColor AS Color, 
                    s.ProductCategory AS Category
                FROM utb_ImageScraperFiles f
                INNER JOIN utb_ImageScraperRecords s ON s.FileID = f.ID
                INNER JOIN utb_ImageScraperResult r ON r.EntryID = s.EntryID
                WHERE f.ID = ? AND r.SortOrder = 1
                GROUP BY 
                    s.ExcelRowID, 
                    r.ImageUrl, 
                    r.ImageUrlThumbnail, 
                    s.ProductBrand, 
                    s.ProductModel, 
                    s.ProductColor, 
                    s.ProductCategory
                ORDER BY s.ExcelRowID
            """
            df = pd.read_sql_query(query, conn, params=(file_id,))
            logger.info(f"Fetched {len(df)} images for Excel export for FileID {file_id}")
            return df
    except pyodbc.Error as e:
        logger.error(f"Database error in get_images_excel_db: {e}")
        return pd.DataFrame()
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")
        return pd.DataFrame()

def get_send_to_email(file_id: int, logger: Optional[logging.Logger] = None) -> str:
    """Retrieve the email address associated with a file ID."""
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            query = "SELECT UserEmail FROM utb_ImageScraperFiles WHERE ID = ?"
            cursor.execute(query, (file_id,))
            result = cursor.fetchone()
            if result and result[0]:
                return result[0]
            logger.warning(f"No email found for FileID {file_id}")
            return "nik@accessx.com"
    except pyodbc.Error as e:
        logger.error(f"Database error fetching email for FileID {file_id}: {e}")
        return "nik@accessx.com"
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")
        return "nik@accessx.com"

def update_file_generate_complete(file_id, logger=None):
    """Mark file generation as complete."""
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            query = "UPDATE utb_ImageScraperFiles SET CreateFileCompleteTime = GETDATE() WHERE ID = ?"
            cursor.execute(query, (file_id,))
            conn.commit()
            logger.info(f"Marked file generation complete for FileID: {file_id}")
    except pyodbc.Error as e:
        logger.error(f"Database error in update_file_generate_complete: {e}")
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")

def update_file_location_complete(file_id, file_location, logger=None):
    """Update file location in the database."""
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            query = "UPDATE utb_ImageScraperFiles SET FileLocationURLComplete = ? WHERE ID = ?"
            cursor.execute(query, (file_location, file_id))
            conn.commit()
            logger.info(f"Updated file location for FileID: {file_id}")
    except pyodbc.Error as e:
        logger.error(f"Database error in update_file_location_complete: {e}")
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")

def call_fetch_missing_images(file_id: str, limit: int = 1000, ai_analysis_only: bool = True, logger=None) -> Dict[str, Any]:
    """Test wrapper for fetch_missing_images."""
    logger = logger or default_logger
    try:
        logger.info(f"Testing fetch_missing_images with FileID: {file_id}, limit: {limit}, ai_analysis_only: {ai_analysis_only}")
        result = fetch_missing_images(file_id, limit, ai_analysis_only, logger)
        if result.empty:
            logger.info("No missing images found")
            return {"success": True, "output": [], "message": "No missing images found"}
        return {"success": True, "output": result.to_dict(orient='records'), "message": "Fetched missing images successfully"}
    except Exception as e:
        logger.error(f"Test failed for fetch_missing_images: {e}", exc_info=True)
        return {"success": False, "error": str(e), "message": "Failed to fetch missing images"}

def call_get_images_excel_db(file_id: str, logger=None) -> Dict[str, Any]:
    """Test wrapper for get_images_excel_db."""
    logger = logger or default_logger
    try:
        logger.info(f"Testing get_images_excel_db with FileID: {file_id}")
        result = get_images_excel_db(file_id, logger)
        if result.empty:
            logger.info("No images found for Excel export")
            return {"success": True, "output": [], "message": "No images found for Excel export"}
        return {"success": True, "output": result.to_dict(orient='records'), "message": "Fetched Excel images successfully"}
    except Exception as e:
        logger.error(f"Test failed for get_images_excel_db: {e}", exc_info=True)
        return {"success": False, "error": str(e), "message": "Failed to fetch Excel images"}

def call_get_send_to_email(file_id: int, logger=None) -> Dict[str, Any]:
    """Test wrapper for get_send_to_email."""
    logger = logger or default_logger
    try:
        logger.info(f"Testing get_send_to_email with FileID: {file_id}")
        result = get_send_to_email(file_id, logger)
        return {"success": True, "output": result, "message": "Retrieved email successfully"}
    except Exception as e:
        logger.error(f"Test failed for get_send_to_email: {e}", exc_info=True)
        return {"success": False, "error": str(e), "message": "Failed to retrieve email"}

def call_update_file_generate_complete(file_id: str, logger=None) -> Dict[str, Any]:
    """Test wrapper for update_file_generate_complete."""
    logger = logger or default_logger
    try:
        logger.info(f"Testing update_file_generate_complete with FileID: {file_id}")
        update_file_generate_complete(file_id, logger)
        return {"success": True, "output": None, "message": "Updated file generate complete successfully"}
    except Exception as e:
        logger.error(f"Test failed for update_file_generate_complete: {e}", exc_info=True)
        return {"success": False, "error": str(e), "message": "Failed to update file generate complete"}

async def call_update_file_location_complete(file_id: str, file_location: str, logger=None) -> Dict[str, Any]:
    """Test wrapper for update_file_location_complete."""
    logger = logger or default_logger
    try:
        logger.info(f"Testing update_file_location_complete with FileID: {file_id}, file_location: {file_location}")
        update_file_location_complete(file_id, file_location, logger)
        return {"success": True, "output": None, "message": "Updated file location successfully"}
    except Exception as e:
        logger.error(f"Test failed for update_file_location_complete: {e}", exc_info=True)
        return {"success": False, "error": str(e), "message": "Failed to update file location"}