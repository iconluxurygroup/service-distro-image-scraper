# database.py
import pyodbc
import pandas as pd
import logging
import requests,zlib
import urllib
import base64
from icon_image_lib.google_parser import get_original_images as GP
from config import conn_str, engine
from typing import List, Optional
import urllib.parse
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, Timeout, ConnectionError
import logging
import logging
import json
import requests

# Fallback logger for standalone calls
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

async def update_log_url_in_db(file_id, log_url, logger=None):
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            
            # Add LogFileUrl column if it doesn't exist
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
            
            # Update the log file URL
            update_query = """
                UPDATE utb_ImageScraperFiles 
                SET LogFileUrl = ? 
                WHERE ID = ?
            """
            cursor.execute(update_query, (log_url, file_id))
            conn.commit()
            
            logger.info(f"✅ Successfully updated log URL '{log_url}' for FileID {file_id}")
            return True
    except Exception as e:
        logger.error(f"🔴 Error updating log URL for FileID {file_id}: {e}", exc_info=True)
        return False

def unpack_content(encoded_content, logger=None):
    logger = logger or default_logger
    try:
        if encoded_content:
            compressed_content = base64.b64decode(encoded_content)
            original_content = zlib.decompress(compressed_content)
            return original_content
        return None
    except Exception as e:
        logger.error(f"Error unpacking content: {e}")
        return None

def get_records_to_search(file_id, logger=None):
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        sql_query = """
            SELECT EntryID, ProductModel AS SearchString, 'model_only' AS SearchType, FileID
            FROM utb_ImageScraperRecords 
            WHERE FileID = ? AND Step1 IS NULL
            ORDER BY EntryID, SearchType
        """
        with pyodbc.connect(conn_str) as connection:
            df = pd.read_sql_query(sql_query, connection, params=[file_id])
        
        if not df.empty:
            invalid_rows = df[df['FileID'] != file_id]
            if not invalid_rows.empty:
                logger.error(f"Found {len(invalid_rows)} rows with incorrect FileID for requested FileID {file_id}: {invalid_rows[['EntryID', 'FileID']].to_dict()}")
                df = df[df['FileID'] == file_id]
        
        logger.info(f"Got {len(df)} search recordss for FileID: {file_id}")
        return df[['EntryID', 'SearchString', 'SearchType']]
    except Exception as e:
        logger.error(f"Error getting records to search for FileID {file_id}: {e}")
        return pd.DataFrame()

def check_endpoint_health(endpoint: str, timeout: int = 5) -> bool:
    """Check if the endpoint can reach Google using /health/google."""
    health_url = f"{endpoint}/health/google"  # Base URL + /health/google
    try:
        response = requests.get(health_url, timeout=timeout)
        if response.status_code != 200:
            return False
        status = response.json().get("status", "")
        return "Google is reachable" in status
    except (requests.RequestException, json.JSONDecodeError):
        return False

def get_healthy_endpoint(endpoints: List[str], logger) -> Optional[str]:
    """Return the first healthy endpoint where Google is reachable."""
    for endpoint in endpoints:
        logger.debug(f"Checking health of {endpoint}")
        if check_endpoint_health(endpoint):
            logger.info(f"Selected healthy endpoint: {endpoint}")
            return endpoint
    logger.error("No healthy endpoints found")
    return None
def process_search_row_gcloud(search_string: str, entry_id: int, logger=None):
    logger = logger or default_logger
    logger.debug(f"Entering process_search_row_gcloud with search_string={search_string}, entry_id={entry_id}")
    logger.info(f"🔎 Search started for {search_string}")
    logger.info(f"📓 Entry ID: {entry_id}")

    try:
        if not search_string or len(search_string.strip()) < 3:
            logger.warning(f"Invalid search string for EntryID {entry_id}: '{search_string}'")
            return False

        fetch_endpoints = [
            "https://southamerica-west1-image-scraper-451516.cloudfunctions.net/main",
            "https://us-central1-image-scraper-451516.cloudfunctions.net/main",
            "https://us-east1-image-scraper-451516.cloudfunctions.net/main",
            "https://us-east4-image-scraper-451516.cloudfunctions.net/main",
            "https://us-west1-image-scraper-451516.cloudfunctions.net/main",
            "https://europe-west4-image-scraper-451516.cloudfunctions.net/main",
            "https://us-west4-image-proxy-453319.cloudfunctions.net/main",
"https://europe-west1-image-proxy-453319.cloudfunctions.net/main",
   "https://europe-north1-image-proxy-453319.cloudfunctions.net/main",
    "https://asia-east1-image-proxy-453319.cloudfunctions.net/main",
    "https://us-south1-gen-lang-client-0697423475.cloudfunctions.net/main",
    "https://us-west3-gen-lang-client-0697423475.cloudfunctions.net/main",
    "https://us-east5-gen-lang-client-0697423475.cloudfunctions.net/main",
     "https://asia-southeast1-gen-lang-client-0697423475.cloudfunctions.net/main",
   "https://us-west2-gen-lang-client-0697423475.cloudfunctions.net/main",
    "https://northamerica-northeast2-image-proxy2-453320.cloudfunctions.net/main",
   "https://southamerica-east1-image-proxy2-453320.cloudfunctions.net/main", 
  "https://europe-west8-icon-image3.cloudfunctions.net/main",
 "https://europe-southwest1-icon-image3.cloudfunctions.net/main",
    "https://europe-west6-icon-image3.cloudfunctions.net/main",
   "https://europe-west3-icon-image3.cloudfunctions.net/main",
   "https://europe-west2-icon-image3.cloudfunctions.net/main",
"https://europe-west9-image-proxy2-453320.cloudfunctions.net/main",
"https://me-west1-image-proxy4.cloudfunctions.net/main",
   "https://me-central1-image-proxy4.cloudfunctions.net/main",
   "https://europe-west12-image-proxy4.cloudfunctions.net/main",
   "https://europe-west10-image-proxy4.cloudfunctions.net/main",
"https://asia-northeast2-image-proxy4.cloudfunctions.net/main",  
        ]

        available_endpoints = fetch_endpoints.copy()
        attempt_count = 0
        max_attempts = len(fetch_endpoints)  # Try all endpoints

        while attempt_count < max_attempts:
            base_endpoint = get_healthy_endpoint(available_endpoints, logger)
            if not base_endpoint:
                logger.error("No healthy endpoints available after exhausting all options")
                return False

            fetch_endpoint = f"{base_endpoint}/fetch"
            search_url = f"https://www.google.com/search?q={urllib.parse.quote(search_string)}&tbm=isch"
            logger.info(f"Fetching URL via proxy: {search_url} using {fetch_endpoint}")

            session = requests.Session()
            retry_strategy = Retry(
                total=5,
                status_forcelist=[500, 502, 503, 504],
                allowed_methods=["POST"],
                backoff_factor=1,
                raise_on_status=False
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount("https://", adapter)

            try:
                # Use a shorter timeout to test if 500 is timeout-related
                response = session.post(fetch_endpoint, json={"url": search_url}, timeout=30)
                status_code = response.status_code
                logger.debug(f"Fetch response status: {status_code} from {fetch_endpoint}")
                logger.debug(f"Response headers: {dict(response.headers)}")
                if response.content:
                    logger.debug(f"Response content preview: {response.content[:200]}")
                else:
                    logger.debug("No response content received")

                if status_code in [500, 502, 503, 504]:
                    logger.warning(
                        f"Fetch failed with status {status_code} after 5 retries from {fetch_endpoint}, "
                        f"trace: projects/image-scraper-451516/traces/{response.headers.get('X-Cloud-Trace-Context', 'unknown')}"
                    )
                    attempt_count += 1
                    available_endpoints.remove(base_endpoint)
                    if attempt_count < max_attempts:
                        logger.info(f"Retrying with next endpoint, attempts remaining: {max_attempts - attempt_count}")
                        continue
                    logger.error("All endpoints failed with server errors")
                    return False
                elif status_code != 200 or not response.json().get("result"):
                    logger.warning(f"Fetch failed with status {status_code} or no result from {fetch_endpoint}")
                    return False
                break  # Success, exit loop

            except Timeout:
                logger.error(f"Fetch timed out after 30s from {fetch_endpoint}")
                attempt_count += 1
                available_endpoints.remove(base_endpoint)
                if attempt_count < max_attempts:
                    logger.info(f"Retrying with next endpoint, attempts remaining: {max_attempts - attempt_count}")
                    continue
                logger.error("All endpoints timed out")
                return False
            except ConnectionError as e:
                logger.error(f"Connection error: {e} from {fetch_endpoint}")
                attempt_count += 1
                available_endpoints.remove(base_endpoint)
                if attempt_count < max_attempts:
                    logger.info(f"Retrying with next endpoint, attempts remaining: {max_attempts - attempt_count}")
                    continue
                logger.error("All endpoints failed with connection errors")
                return False
            except RequestException as e:
                logger.error(
                    f"Request error: {e} from {fetch_endpoint}, "
                    f"trace: projects/image-scraper-451516/traces/{getattr(e.response, 'headers', {}).get('X-Cloud-Trace-Context', 'unknown')}"
                )
                attempt_count += 1
                available_endpoints.remove(base_endpoint)
                if attempt_count < max_attempts:
                    logger.info(f"Retrying with next endpoint, attempts remaining: {max_attempts - attempt_count}")
                    continue
                logger.error("All endpoints failed with request errors")
                return False

        try:
            response_json = response.json()
            result = response_json.get("result", None)
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON response from {fetch_endpoint}")
            return False

        if not result:
            logger.warning(f"No 'result' in response from {fetch_endpoint}")
            return False

        if isinstance(result, str):
            result = result.encode('utf-8')
        parsed_data = GP(result)
        if not parsed_data or not isinstance(parsed_data, tuple) or not parsed_data[0]:
            logger.warning(f"No valid parsed data for {search_url}")
            return False

        urls, descriptions, sources, thumbnails = parsed_data
        if urls and urls[0] != 'No google image results found':
            df = pd.DataFrame({
                'EntryID': [entry_id] * len(urls),
                'ImageURL': urls,
                'ImageDesc': descriptions,
                'ImageSource': sources,
                'ImageURLThumbnail': thumbnails
            })
            df.to_sql(name='utb_ImageScraperResult', con=engine, index=False, if_exists='append')

            with pyodbc.connect(conn_str) as connection:
                cursor = connection.cursor()
                cursor.execute(f"UPDATE utb_ImageScraperRecords SET Step1 = GETDATE() WHERE EntryID = {entry_id}")
                connection.commit()

            logger.info(f"Processed EntryID {entry_id} with {len(df)} images using {fetch_endpoint}")
            return df

        logger.warning('No valid image URL found')
        return False

    except Exception as e:
        logger.error(f"Error processing search row: {e}")
        return False
def process_search_row(search_string, endpoint, entry_id, logger=None):
    logger = logger or default_logger
    logger.debug(f"Entering process_search_row with search_string={search_string}, endpoint={endpoint}, entry_id={entry_id}")
    logger.info(f"🔎Search started for {search_string}")
    logger.info(f"👺Search Proxy: {endpoint}")
    logger.info(f"📓Entry ID: {entry_id}")
    try:
        if not search_string or len(search_string.strip()) < 3:
            logger.warning(f"Invalid search string for EntryID {entry_id}: '{search_string}'")
            return False
        
        search_url = f"{endpoint}?query={urllib.parse.quote(search_string)}"
        logger.info(f"Searching URL: {search_url}")
        
        response = requests.get(search_url, timeout=60)
        if response.status_code != 200:
            logger.warning(f"Non-200 status {response.status_code} for {search_url}")
            remove_endpoint(endpoint, logger=logger)
            new_endpoint = get_endpoint(logger=logger)
            return process_search_row_gcloud(search_string, entry_id, logger=logger)
        
        try:
            response_json = response.json()
            result = response_json.get('body', None)
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON response for {search_url}")
            remove_endpoint(endpoint, logger=logger)
            new_endpoint = get_endpoint(logger=logger)
            return process_search_row(search_string, new_endpoint, entry_id, logger=logger)
        
        if not result:
            logger.warning(f"No body in response for {search_url}")
            remove_endpoint(endpoint, logger=logger)
            new_endpoint = get_endpoint(logger=logger)
            return process_search_row_gcloud(search_string, entry_id, logger=logger)
        
        unpacked_html = unpack_content(result, logger=logger)
        if not unpacked_html or len(unpacked_html) < 100:
            logger.warning(f"Invalid unpacked HTML for {search_url}")
            remove_endpoint(endpoint, logger=logger)
            new_endpoint = get_endpoint(logger=logger)
            return process_search_row(search_string, new_endpoint, entry_id, logger=logger)
        
        parsed_data = GP(unpacked_html)
        if not parsed_data or not isinstance(parsed_data, tuple) or not parsed_data[0]:
            logger.warning(f"No valid parsed data for {search_url}")
            remove_endpoint(endpoint, logger=logger)
            new_endpoint = get_endpoint(logger=logger)
            return process_search_row_gcloud(search_string, entry_id, logger=logger)
        
        urls, descriptions, sources, thumbnails = parsed_data
        if urls and urls[0] != 'No google image results found':
            df = pd.DataFrame({
                'EntryId': [entry_id] * len(urls),
                'ImageURL': urls,
                'ImageDesc': descriptions,
                'ImageSource': sources,
                'ImageURLThumbnail': thumbnails
            })
            df.to_sql(name='utb_ImageScraperResult', con=engine, index=False, if_exists='append')
            
            with pyodbc.connect(conn_str) as connection:
                cursor = connection.cursor()
                cursor.execute(f"UPDATE utb_ImageScraperRecords SET Step1 = GETDATE() WHERE EntryID = {entry_id}")
                connection.commit()
            logger.info(f"Processed EntryID {entry_id} with {len(df)} images")
            return df  # Return DataFrame for Ray to use result_count
        
        logger.warning('No valid image URL, trying again with new endpoint')
        remove_endpoint(endpoint, logger=logger)
        new_endpoint = get_endpoint(logger=logger)
        return process_search_row_gcloud(search_string, entry_id, logger=logger)
    
    except requests.RequestException as e:
        logger.error(f"Request error: {e}")
        remove_endpoint(endpoint, logger=logger)
        new_endpoint = get_endpoint(logger=logger)
        logger.info(f"Trying again with new endpoint: {new_endpoint}")
        return process_search_row_gcloud(search_string, entry_id, logger=logger)
    
    except Exception as e:
        logger.error(f"Error processing search row: {e}")
        remove_endpoint(endpoint, logger=logger)
        new_endpoint = get_endpoint(logger=logger)
        logger.info(f"Trying again with new endpoint: {new_endpoint}")
        return process_search_row_gcloud(search_string, entry_id, logger=logger)

def fetch_images_by_file_id(file_id, logger=None):
    logger = logger or default_logger
    query = """
        SELECT rr.ResultID, rr.EntryID, rr.ImageURL, 
               r.ProductBrand, r.ProductCategory, r.ProductColor,
               rr.aijson
        FROM utb_ImageScraperRecords r
        INNER JOIN utb_ImageScraperResult rr ON r.EntryID = rr.EntryID
        WHERE r.FileID = ?
    """
    try:
        with pyodbc.connect(conn_str) as conn:
            df = pd.read_sql(query, conn, params=[file_id])
            logger.info(f"Fetched {len(df)} images for FileID {file_id}")
            return df
    except Exception as e:
        logger.error(f"Error fetching images for FileID {file_id}: {e}")
        return pd.DataFrame()
# database.py (corrected)
def fetch_missing_images(file_id=None, limit=8, ai_analysis_only=True, exclude_excel_row_ids=[1], logger=None):
    logger = logger or default_logger
    if limit is None or not isinstance(limit, int) or limit < 1:
        limit = 1000
    logger.info(f"Fetching missing images with file_id={file_id}, limit={limit}, exclude_excel_row_ids={exclude_excel_row_ids}")

    try:
        connection = pyodbc.connect(conn_str)
        query_params = []

        # Build exclusion condition dynamically
        exclude_condition = ""
        if exclude_excel_row_ids:
            placeholders = ",".join("?" * len(exclude_excel_row_ids))
            exclude_condition = f"AND r.ExcelRowID NOT IN ({placeholders})"

        if ai_analysis_only:
            query = f"""
                SELECT MIN(t.ResultID) AS ResultID, t.EntryID, t.ImageURL, t.ImageURLThumbnail,
                    r.ProductBrand, r.ProductCategory, r.ProductColor
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                WHERE (
                    t.ImageURL IS NOT NULL 
                    AND t.ImageURL <> ''
                    AND (
                        ISJSON(t.aijson) = 0 
                        OR t.aijson IS NULL
                        OR JSON_VALUE(t.aijson, '$.similarity_score') IS NULL
                        OR JSON_VALUE(t.aijson, '$.similarity_score') IN ('NaN', 'null', 'undefined')
                        OR JSON_VALUE(t.aijson, '$.match_score') IS NULL
                        OR JSON_VALUE(t.aijson, '$.match_score') IN ('NaN', 'null', 'undefined')
                    )
                )
                AND (r.FileID = ? OR ? IS NULL)
                {exclude_condition}
                GROUP BY t.EntryID, t.ImageURL, t.ImageURLThumbnail, r.ProductBrand, r.ProductCategory, r.ProductColor
            """
            query_params = [file_id, file_id]
            if exclude_excel_row_ids:
                query_params.extend(exclude_excel_row_ids)
        else:
            query = f"""
            -- Records missing in result table completely
            SELECT 
                NULL as ResultID,
                r.EntryID,
                NULL as ImageURL,
                NULL as ImageURLThumbnail,
                r.ProductBrand,
                r.ProductCategory,
                r.ProductColor
            FROM utb_ImageScraperRecords r
            LEFT JOIN utb_ImageScraperResult t ON r.EntryID = t.EntryID
            WHERE t.EntryID IS NULL
            AND (r.FileID = ? OR ? IS NULL)
            {exclude_condition}
            UNION ALL
            -- Records with empty or NULL ImageURL
            SELECT 
                t.ResultID,
                t.EntryID,
                t.ImageURL,
                t.ImageURLThumbnail,
                r.ProductBrand,
                r.ProductCategory,
                r.ProductColor
            FROM utb_ImageScraperResult t
            INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
            WHERE (t.ImageURL IS NULL OR t.ImageURL = '')
            AND (r.FileID = ? OR ? IS NULL)
            {exclude_condition}
            UNION ALL
            -- Records with URLs but missing AI analysis
            SELECT 
                MIN(t.ResultID) AS ResultID,
                t.EntryID,
                t.ImageURL,
                t.ImageURLThumbnail,
                r.ProductBrand,
                r.ProductCategory,
                r.ProductColor
            FROM utb_ImageScraperResult t
            INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
            WHERE (
                t.ImageURL IS NOT NULL
                AND t.ImageURL <> ''
                AND (
                    ISJSON(t.aijson) = 0
                    OR t.aijson IS NULL
                    OR JSON_VALUE(t.aijson, '$.similarity_score') IS NULL
                    OR JSON_VALUE(t.aijson, '$.similarity_score') IN ('NaN', 'null', 'undefined')
                    OR JSON_VALUE(t.aijson, '$.match_score') IS NULL
                    OR JSON_VALUE(t.aijson, '$.match_score') IN ('NaN', 'null', 'undefined')
                )
            )
            AND (r.FileID = ? OR ? IS NULL)
            {exclude_condition}
            GROUP BY t.EntryID, t.ImageURL, t.ImageURLThumbnail, r.ProductBrand, r.ProductCategory, r.ProductColor
            """
            query_params = [file_id, file_id, file_id, file_id, file_id, file_id]
            if exclude_excel_row_ids:
                query_params.extend(exclude_excel_row_ids * 3)

        query += " ORDER BY EntryID ASC OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY"
        query_params.append(limit)

       
        # Pass params as a tuple to match positional placeholders
        df = pd.read_sql_query(query, engine, params=tuple(query_params))

        connection.close()

        if df.empty:
            logger.info(f"No missing images found" + (f" for FileID: {file_id}" if file_id else ""))
        else:
            logger.info(f"Found {len(df)} missing images" + (f" for FileID: {file_id}" if file_id else ""))
        
        return df

    except Exception as e:
        logger.error(f"Error fetching missing images: {e}")
        return pd.DataFrame()
    
def update_initial_sort_order(file_id, logger=None):
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as connection:
            connection.timeout = 300
            cursor = connection.cursor()
            logger.info(f"🔄 Setting initial SortOrder for FileID: {file_id}")
            
            cursor.execute("BEGIN TRANSACTION")
            
            # Reset Step1 and SortOrder as in the original function
            cursor.execute("UPDATE utb_ImageScraperRecords SET Step1 = NULL WHERE FileID = ?", (file_id,))
            reset_step1_count = cursor.rowcount
            logger.info(f"Reset Step1 for {reset_step1_count} rows in utb_ImageScraperRecords")
            
            cursor.execute("UPDATE utb_ImageScraperResult SET SortOrder = NULL WHERE EntryID IN (SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?)", (file_id,))
            reset_sort_count = cursor.rowcount
            logger.info(f"Reset SortOrder for {reset_sort_count} rows in utb_ImageScraperResult")
            
            # Identify and mark duplicates with SortOrder = -1
            deduplicate_query = """
                WITH duplicates AS (
                    SELECT ResultID, EntryID,
                           ROW_NUMBER() OVER (PARTITION BY EntryID ORDER BY ResultID) AS row_num
                    FROM utb_ImageScraperResult
                    WHERE EntryID IN (SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?)
                )
                UPDATE utb_ImageScraperResult
                SET SortOrder = CASE WHEN d.row_num > 1 THEN -1 ELSE NULL END
                FROM utb_ImageScraperResult r
                INNER JOIN duplicates d ON r.ResultID = d.ResultID AND r.EntryID = d.EntryID;
            """
            cursor.execute(deduplicate_query, (file_id,))
            dedup_count = cursor.rowcount
            logger.info(f"Marked {dedup_count} rows as duplicates with SortOrder = -1")
            
            # Set initial SortOrder for unique records (where SortOrder is still NULL)
            initial_sort_query = """
                WITH toupdate AS (
                    SELECT t.*, 
                           ROW_NUMBER() OVER (PARTITION BY t.EntryID ORDER BY t.ResultID) AS seqnum
                    FROM utb_ImageScraperResult t 
                    INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID 
                    WHERE r.FileID = ? AND t.SortOrder IS NULL
                ) 
                UPDATE toupdate 
                SET SortOrder = seqnum;
            """
            cursor.execute(initial_sort_query, (file_id,))
            update_count = cursor.rowcount
            logger.info(f"Set initial SortOrder for {update_count} unique rows")
            
            cursor.execute("COMMIT")
            
            # Verify the results
            verify_query = """
                SELECT t.ResultID, t.EntryID, t.SortOrder
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                WHERE r.FileID = ?
                ORDER BY t.EntryID, t.SortOrder
            """
            cursor.execute(verify_query, (file_id,))
            results = cursor.fetchall()
            for record in results:
                logger.info(f"Initial - EntryID: {record[1]}, ResultID: {record[0]}, SortOrder: {record[2]}")
            
            # Count total results and unique results
            count_query = "SELECT COUNT(*) FROM utb_ImageScraperResult WHERE EntryID IN (SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?)"
            cursor.execute(count_query, (file_id,))
            total_results = cursor.fetchone()[0]
            unique_count_query = """
                SELECT COUNT(*) 
                FROM utb_ImageScraperResult 
                WHERE EntryID IN (SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?) 
                AND SortOrder > 0
            """
            cursor.execute(unique_count_query, (file_id,))
            unique_results = cursor.fetchone()[0]
            logger.info(f"Total results: {total_results}, Unique results: {unique_results}")
            if unique_results < 16:
                logger.warning(f"Expected at least 16 unique results (8 per search type), got {unique_results}")
            
            return [{"ResultID": row[0], "EntryID": row[1], "SortOrder": row[2]} for row in results]
    except Exception as e:
        logger.error(f"Error setting initial SortOrder: {e}")
        if 'cursor' in locals():
            cursor.execute("ROLLBACK")
        return None
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()
def update_search_sort_order(file_id, logger=None):
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as connection:
            connection.timeout = 300
            cursor = connection.cursor()
            logger.info(f"🔄 Setting Search SortOrder for FileID: {file_id}")
            
            sort_query = """
                WITH categorized AS (
                    SELECT t.ResultID, t.EntryID, t.ImageDesc, r.ProductBrand, r.ProductModel,
                        CASE 
                            -- Both ProductBrand and ProductModel match (normalized)
                            WHEN (UPPER(REPLACE(REPLACE(t.ImageDesc, '-', ''), ' ', '')) LIKE '%' + UPPER(REPLACE(REPLACE(r.ProductBrand, '-', ''), ' ', '')) + '%' 
                                  AND r.ProductBrand IS NOT NULL)
                                 AND (UPPER(REPLACE(REPLACE(t.ImageDesc, '-', ''), ' ', '')) LIKE '%' + UPPER(REPLACE(REPLACE(r.ProductModel, '-', ''), ' ', '')) + '%' 
                                      AND r.ProductModel IS NOT NULL) THEN 1
                            -- Only ProductModel matches (normalized)
                            WHEN UPPER(REPLACE(REPLACE(t.ImageDesc, '-', ''), ' ', '')) LIKE '%' + UPPER(REPLACE(REPLACE(r.ProductModel, '-', ''), ' ', '')) + '%' 
                                 AND r.ProductModel IS NOT NULL THEN 2
                            -- Brand with common model terms
                            WHEN (UPPER(t.ImageDesc) LIKE '%' + UPPER(r.ProductBrand) + '%' AND r.ProductBrand IS NOT NULL)
                                 AND (t.ImageDesc LIKE '%Bag%' OR t.ImageDesc LIKE '%Robinson%' OR t.ImageDesc LIKE '%Shoulder%') THEN 3
                            -- Unrelated (no brand or model relevance)
                            ELSE 4
                        END AS priority
                    FROM utb_ImageScraperResult t
                    INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                    WHERE r.FileID = ?
                ),
                toupdate AS (
                    SELECT c.*,
                        CASE 
                            WHEN c.priority = 4 THEN -2  -- Completely unrelated
                            ELSE ROW_NUMBER() OVER (PARTITION BY c.EntryID ORDER BY c.priority, c.ResultID)  -- Sequential ranking
                        END AS new_sort_order
                    FROM categorized c
                )
                UPDATE utb_ImageScraperResult
                SET SortOrder = t.new_sort_order
                FROM utb_ImageScraperResult r
                INNER JOIN toupdate t ON r.ResultID = t.ResultID;
            """
            cursor.execute("BEGIN TRANSACTION")
            cursor.execute(sort_query, (file_id,))
            update_count = cursor.rowcount
            cursor.execute("COMMIT")
            logger.info(f"Set Search SortOrder for {update_count} rows")
            
            verify_query = """
                SELECT TOP 20 t.ResultID, t.EntryID, t.ImageDesc, t.SortOrder, r.ProductBrand, r.ProductModel
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                WHERE r.FileID = ?
                ORDER BY t.EntryID, t.SortOrder
            """
            cursor.execute(verify_query, (file_id,))
            results = cursor.fetchall()
            for record in results:
                logger.info(f"Search - EntryID: {record[1]}, ResultID: {record[0]}, ImageDesc: {record[2]}, SortOrder: {record[3]}, ProductBrand: {record[4]}, ProductModel: {record[5]}")
            
            return [{"ResultID": row[0], "EntryID": row[1], "ImageDesc": row[2], "SortOrder": row[3], "ProductBrand": row[4], "ProductModel": row[5]} for row in results]
    except Exception as e:
        logger.error(f"Error setting Search SortOrder: {e}")
        if 'cursor' in locals():
            cursor.execute("ROLLBACK")
        return None
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

def update_file_location_complete(file_id, file_location, logger=None):
    logger = logger or default_logger
    file_id = int(file_id)
    try:
        with pyodbc.connect(conn_str) as connection:
            cursor = connection.cursor()
            cursor.execute("BEGIN TRANSACTION")
            cursor.execute("UPDATE utb_ImageScraperFiles SET FileLocationURLComplete = ? WHERE ID = ?", (file_location, file_id))
            cursor.execute("COMMIT")
            logger.info(f"Updated file location URL for FileID: {file_id}")
    except Exception as e:
        logger.error(f"Error updating file location URL: {e}")
        if 'cursor' in locals():
            cursor.execute("ROLLBACK")
        raise

def update_file_generate_complete(file_id, logger=None):
    logger = logger or default_logger
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            query = "UPDATE utb_ImageScraperFiles SET CreateFileCompleteTime = GETDATE() WHERE ID = ?"
            cursor.execute(query, (file_id,))
            conn.commit()
            logger.info(f"Marked file generation as complete for FileID: {file_id}")
    except Exception as e:
        logger.error(f"Error updating file generation completion time: {e}")
        raise

def get_send_to_email(file_id, logger=None):
    logger = logger or default_logger
    try:
        with pyodbc.connect(conn_str, timeout=60) as conn:
            cursor = conn.cursor()
            query = "SELECT UserEmail FROM utb_ImageScraperFiles WHERE ID = ?"
            cursor.execute(query, (file_id,))
            send_to_email = cursor.fetchone()
            if send_to_email:
                logger.info(f"Got email address for FileID: {file_id}: {send_to_email[0]}")
                return send_to_email[0]
            logger.warning(f"No email address found for FileID: {file_id}")
            return "No Email Found"
    except Exception as e:
        logger.error(f"Error getting email address: {e}")
        return "nik@iconluxurygroup.com"

def get_images_excel_db(file_id, logger=None):
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as connection:
            cursor = connection.cursor()
            cursor.execute("UPDATE utb_ImageScraperFiles SET CreateFileStartTime = GETDATE() WHERE ID = ?", (file_id,))
            connection.commit()
            
            query = """
            SELECT s.ExcelRowID AS ExcelRowID, 
                    r.ImageURL AS ImageURL, 
                    r.ImageURLThumbnail AS ImageURLThumbnail 
                FROM utb_ImageScraperFiles f
                INNER JOIN utb_ImageScraperRecords s ON s.FileID = f.ID 
                INNER JOIN utb_ImageScraperResult r ON r.EntryID = s.EntryID 
                WHERE f.ID = ? AND r.SortOrder = 1
                ORDER BY s.ExcelRowID
            """
            df = pd.read_sql_query(query, connection, params=[file_id])
            logger.info(f"Queried FileID: {file_id}, retrieved {len(df)} images for Excel export")
        return df
    except Exception as e:
        logger.error(f"Error getting images for Excel export: {e}")
        return pd.DataFrame()

def get_lm_products(file_id, logger=None):
    logger = logger or default_logger
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            query = f"EXEC usp_ImageScrapergetMatchFromRetail {file_id}"
            cursor.execute(query)
            conn.commit()
            logger.info(f"Executed stored procedure to match products for FileID: {file_id}")
    except Exception as e:
        logger.error(f"Error executing stored procedure to match products: {e}")

def get_endpoint(logger=None):
    logger = logger or default_logger
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            sql_query = "SELECT TOP 1 EndpointURL FROM utb_Endpoints WHERE EndpointIsBlocked = 0 ORDER BY NEWID()"
            cursor.execute(sql_query)
            endpoint_url = cursor.fetchone()
            if endpoint_url:
                endpoint = endpoint_url[0]
                logger.info(f"Got endpoint URL: {endpoint}")
                return endpoint
            else:
                logger.warning("No endpoint URL found")
                return "No EndpointURL"
    except Exception as e:
        logger.error(f"Error getting endpoint URL: {e}")
        return "No EndpointURL"

def remove_endpoint(endpoint, logger=None):
    logger = logger or default_logger
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            sql_query = f"UPDATE utb_Endpoints SET EndpointIsBlocked = 1 WHERE EndpointURL = '{endpoint}'"
            cursor.execute(sql_query)
            conn.commit()
            logger.info(f"Marked endpoint as blocked: {endpoint}")
    except Exception as e:
        logger.error(f"Error marking endpoint as blocked: {e}")

