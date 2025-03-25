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
from config import conn_str, engine
from icon_image_lib.google_parser import process_search_result
from skimage import io as skio
from skimage.metrics import structural_similarity as ssim
from skimage.transform import resize
from io import BytesIO
import numpy as np
from PIL import Image
from config import conn_str
# Default logger
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

def insert_search_results(df, logger=None):
    """
    Insert search results into the database.

    Args:
        df (pd.DataFrame): DataFrame containing search results.
        logger (Optional[logging.Logger]): Logger instance for logging messages. Defaults to default_logger if None.

    Returns:
        bool: True if insertion succeeds, False otherwise.
    """
    # Use default_logger if no logger is provided
    logger = logger or default_logger

    # Check if DataFrame is empty
    if df.empty:
        logger.info("No rows to insert: DataFrame is empty")
        return False

    # Verify required columns
    required = ['EntryID', 'ImageURL', 'ImageDesc', 'ImageSource', 'ImageURLThumbnail']
    if not all(col in df.columns for col in required):
        missing_cols = [col for col in required if col not in df.columns]
        logger.error(f"Missing columns: {missing_cols}")
        return False

    # Log the insertion attempt
    logger.info(f"Inserting {len(df)} rows: {df.iloc[0].to_dict()}")

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            for index, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO utb_ImageScraperResult (EntryID, ImageUrl, ImageDesc, ImageSource, ImageUrlThumbnail, CreateTime)
                    VALUES (?, ?, ?, ?, ?, GETDATE())
                """, (int(row['EntryID']), row['ImageURL'], row['ImageDesc'], row['ImageSource'], row['ImageURLThumbnail']))
            conn.commit()
            logger.info(f"Successfully inserted {len(df)} rows")
            return True
    except pyodbc.Error as e:
        logger.error(f"Database error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
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

def check_endpoint_health(endpoint: str, timeout: int = 5, logger: Optional[logging.Logger] = None) -> bool:
    """Check if an endpoint is healthy by querying its Google health check."""
    logger = logger or default_logger
    health_url = f"{endpoint}/health/google"
    try:
        response = requests.get(health_url, timeout=timeout)
        response.raise_for_status()
        return "Google is reachable" in response.json().get("status", "")
    except RequestException:
        logger.warning(f"Endpoint {endpoint} health check failed")
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

def process_search_row_gcloud(search_string: str, entry_id: int, logger: Optional[logging.Logger] = None) -> Optional[pd.DataFrame]:
    """Process a search row using Google Cloud endpoints with retries."""
    logger = logger or default_logger
    if not search_string or len(search_string.strip()) < 3:
        logger.warning(f"Invalid search string for EntryID {entry_id}: '{search_string}'")
        return pd.DataFrame()

    endpoints = [
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
            "https://asia-northeast2-image-proxy4.cloudfunctions.net/main"
        ]
    session = requests.Session()
    retry_strategy = Retry(total=5, status_forcelist=[500, 502, 503, 504], backoff_factor=1)
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))

    for attempt, endpoint in enumerate(endpoints, 1):
        fetch_endpoint = f"{endpoint}/fetch"
        search_url = f"https://www.google.com/search?q={urllib.parse.quote(search_string)}&tbm=isch"
        try:
            logger.info(f"Attempt {attempt}: Fetching {search_url} via {fetch_endpoint}")
            response = session.post(fetch_endpoint, json={"url": search_url}, timeout=30)
            response.raise_for_status()
            result = response.json().get("result")
            if not result:
                continue
            results_html_bytes = result if isinstance(result, bytes) else result.encode("utf-8")
            df = process_search_result(results_html_bytes, results_html_bytes, entry_id, logger)
            if not df.empty:
                logger.info(f"Processed EntryID {entry_id} with {len(df)} images")
                return df
        except RequestException as e:
            logger.warning(f"Attempt {attempt} failed for {fetch_endpoint}: {e}")
            continue
    logger.error(f"All attempts failed for EntryID {entry_id}")
    return pd.DataFrame()

def process_search_row(search_string: str, endpoint: str, entry_id: int, logger: Optional[logging.Logger] = None) -> Optional[pd.DataFrame]:
    """Process a search row with fallback to Google Cloud."""
    logger = logger or default_logger
    if not search_string or not endpoint:
        logger.warning(f"Invalid input for EntryID {entry_id}: search_string={search_string}, endpoint={endpoint}")
        return process_search_row_gcloud(search_string, entry_id, logger)

    session = requests.Session()
    retry_strategy = Retry(total=3, status_forcelist=[500, 502, 503, 504], backoff_factor=1)
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))

    try:
        search_url = f"{endpoint}?query={urllib.parse.quote(search_string)}"
        logger.info(f"Searching {search_url}")
        response = session.get(search_url, timeout=60)
        response.raise_for_status()
        result = response.json().get("body")
        if not result:
            logger.warning(f"No body in response for {search_url}")
            return process_search_row_gcloud(search_string, entry_id, logger)

        unpacked_html = unpack_content(result, logger)
        if not unpacked_html or len(unpacked_html) < 100:
            logger.warning(f"Invalid HTML for {search_url}")
            return process_search_row_gcloud(search_string, entry_id, logger)

        df = process_search_result(unpacked_html, unpacked_html, entry_id, logger)
        if df.empty:
            logger.warning(f"No valid data for {search_url}")
            return process_search_row_gcloud(search_string, entry_id, logger)

        logger.info(f"Processed EntryID {entry_id} with {len(df)} images")
        return df
    except RequestException as e:
        logger.error(f"Request error for {search_url}: {e}")
        return process_search_row_gcloud(search_string, entry_id, logger)
    
def update_sort_order_based_on_match_score(file_id: str, logger: Optional[logging.Logger] = None) -> Optional[List[Dict]]:

    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            logger.info(f"🔄 Updating SortOrder for FileID: {file_id}")

            # Fetch data from the database, including existing SortOrder
            query = """
                SELECT t.ResultID, t.EntryID,
                       ISNULL(CAST(JSON_VALUE(t.aijson, '$.match_score') AS FLOAT), 0) AS match_score,
                       ISNULL(CAST(JSON_VALUE(t.aijson, '$.ssim_score') AS FLOAT), -1) AS ssim_score,
                       ISNULL(t.SortOrder, 0) AS SortOrder
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                WHERE r.FileID = ?
            """
            df = pd.read_sql_query(query, conn, params=[file_id])
            df["match_score"] = pd.to_numeric(df["match_score"], errors="coerce").fillna(0)
            df["ssim_score"] = pd.to_numeric(df["ssim_score"], errors="coerce").fillna(-1)
            df["SortOrder"] = pd.to_numeric(df["SortOrder"], errors="coerce").fillna(0)

            # Process each EntryID group
            for entry_id, group in df.groupby("EntryID"):
                # Filter out rows with SortOrder -1 or -2
                sortable_group = group[~group["SortOrder"].isin([-1, -2])]
                
                if not sortable_group.empty:
                    # Sort by match_score descending, then ssim_score descending
                    sorted_group = sortable_group.sort_values(by=["match_score", "ssim_score"], ascending=[False, False])
                    sorted_group["new_sort_order"] = range(1, len(sorted_group) + 1)
                    
                    # Update the database with new sort order for sorted rows only
                    for _, row in sorted_group.iterrows():
                        cursor.execute(
                            "UPDATE utb_ImageScraperResult SET SortOrder = ? WHERE ResultID = ?",
                            (row["new_sort_order"], row["ResultID"])
                        )
                else:
                    logger.info(f"No sortable images for EntryID: {entry_id}")

            conn.commit()
            logger.info(f"Successfully updated SortOrder for FileID: {file_id}")
            
            # Return only the updated sort order details for sorted images
            return [{"ResultID": r[0], "EntryID": r[1], "SortOrder": r[2]} for r in cursor.execute(
                """
                SELECT ResultID, EntryID, SortOrder 
                FROM utb_ImageScraperResult 
                WHERE EntryID IN (SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?) 
                AND SortOrder NOT IN (-1, -2)
                """, (file_id,)
            ).fetchall()]
    
    except Exception as e:
        logger.error(f"Error updating SortOrder for FileID {file_id}: {e}", exc_info=True)
        return None
    
def update_initial_sort_order(file_id: str, logger: Optional[logging.Logger] = None) -> Optional[List[Dict]]:
    """Set initial sort order for results."""
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN TRANSACTION")
            cursor.execute("UPDATE utb_ImageScraperRecords SET Step1 = NULL WHERE FileID = ?", (file_id,))
            cursor.execute(
                "UPDATE utb_ImageScraperResult SET SortOrder = NULL WHERE EntryID IN (SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?)",
                (file_id,)
            )
            cursor.execute("""
                WITH duplicates AS (
                    SELECT ResultID, EntryID,
                           ROW_NUMBER() OVER (PARTITION BY EntryID ORDER BY ResultID) AS row_num
                    FROM utb_ImageScraperResult
                    WHERE EntryID IN (SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?)
                )
                UPDATE utb_ImageScraperResult
                SET SortOrder = CASE WHEN d.row_num > 1 THEN -1 ELSE NULL END
                FROM utb_ImageScraperResult r
                INNER JOIN duplicates d ON r.ResultID = d.ResultID AND r.EntryID = d.EntryID
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
                "SELECT ResultID, EntryID, SortOrder FROM utb_ImageScraperResult WHERE EntryID IN (SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?) ORDER BY EntryID, SortOrder",
                (file_id,)
            ).fetchall()
            logger.info(f"Set initial SortOrder for FileID: {file_id}")
            return [{"ResultID": r[0], "EntryID": r[1], "SortOrder": r[2]} for r in results]
    except pyodbc.Error as e:
        logger.error(f"Database error setting initial SortOrder: {e}", exc_info=True)
        if "cursor" in locals():
            cursor.execute("ROLLBACK")
        return None
    finally:
        if 'conn' in locals():
            conn.close()

# ... (other imports and functions remain unchanged)

def update_search_sort_order(file_id: str, logger: Optional[logging.Logger] = None) -> Optional[List[Dict]]:
    """Set search-based sort order."""
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            logger.info(f"🔄 Setting Search SortOrder for FileID: {file_id}")
            fetch_query = """
                SELECT 
                    t.ResultID, 
                    t.EntryID, 
                    t.ImageDesc, 
                    t.ImageUrl, 
                    t.ImageSource, 
                    r.ProductBrand, 
                    r.ProductModel
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                WHERE r.FileID = ?
            """
            df = pd.read_sql(fetch_query, conn, params=[file_id])

            def clean_string(text):
                """Clean text by removing delimiters including '&' and converting to uppercase."""
                if pd.isna(text):
                    return ''
                return re.sub(r'[- _.,;:/&]', '', str(text).upper())

            brand_aliases = {"Scotch & Soda": ["Scotch and Soda", "Scotch Soda"], "Adidas": ["Adidas AG", "Adidas Originals"]}
            clean_brand_aliases = {
                clean_string(brand): [clean_string(brand)] + [clean_string(alias) for alias in aliases]
                for brand, aliases in brand_aliases.items()
            }

            for col in ["ImageDesc", "ImageSource", "ImageUrl", "ProductBrand", "ProductModel"]:
                df[f"{col}_clean"] = df[col].apply(clean_string)

            df['brand_aliases_clean'] = df['ProductBrand_clean'].apply(
                lambda x: clean_brand_aliases.get(x, [x])
            )

            def calculate_priority(row):
                brand_match = any(
                    alias in row['ImageDesc_clean'] or
                    alias in row['ImageSource_clean'] or
                    alias in row['ImageUrl_clean']
                    for alias in row['brand_aliases_clean']
                )
                model_match = (
                    (row['ProductModel_clean'] in row['ImageDesc_clean'] if row['ProductModel_clean'] else False) or
                    (row['ProductModel_clean'] in row['ImageSource_clean'] if row['ProductModel_clean'] else False) or
                    (row['ProductModel_clean'] in row['ImageUrl_clean'] if row['ProductModel_clean'] else False)
                )
                if brand_match and model_match and pd.notna(row['ProductBrand']) and pd.notna(row['ProductModel']):
                    return 1
                elif model_match and pd.notna(row['ProductModel']):
                    return 2
                elif brand_match and pd.notna(row['ProductBrand']):
                    return 3
                return 4  # Reverted to 4 for no matches

            df['priority'] = df.apply(calculate_priority, axis=1)

            # Sort by priority ascending, but assign SortOrder so lower priority gets higher numbers
            df = df.sort_values(by=['EntryID', 'priority', 'ResultID'], ascending=[True, True, True])
            df['new_sort_order'] = df.groupby('EntryID').cumcount() + 1

            # Directly set SortOrder = -2 for non-matching rows (priority 4)
            df['new_sort_order'] = df.apply(lambda row: -2 if row['priority'] == 4 else row['new_sort_order'], axis=1)

            cursor.execute("BEGIN TRANSACTION")
            update_query = "UPDATE utb_ImageScraperResult SET SortOrder = ? WHERE ResultID = ?"
            updates = df[['ResultID', 'new_sort_order']]

            for _, row in updates.iterrows():
                cursor.execute(update_query, (int(row['new_sort_order']), int(row['ResultID'])))

            update_count = len(updates)
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
                logger.info(f"Search - EntryID: {record[1]}, ResultID: {record[0]}, ImageDesc: {record[2]}, SortOrder: {record[3]}")
            cursor.execute("COMMIT")

            return [{"ResultID": row[0], "EntryID": row[1], "ImageDesc": row[2], "SortOrder": row[3], "ProductBrand": row[4], "ProductModel": row[5]} for row in results]
    except Exception as e:
        logger.error(f"Error setting Search SortOrder for FileID {file_id}: {e}", exc_info=True)
        if 'cursor' in locals():
            cursor.execute("ROLLBACK")
        return None
    finally:
        if 'conn' in locals():
            conn.close()

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
        with pyodbc.connect(conn_str) as conn:
            if ai_analysis_only:
                # Fetch images needing AI analysis with positive SortOrder only
                query = """
                    SELECT t.ResultID, t.EntryID, t.ImageUrl, t.ImageUrlThumbnail,
                           r.ProductBrand, r.ProductCategory, r.ProductColor
                    FROM utb_ImageScraperResult t
                    INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                    WHERE r.FileID = ?
                    AND (t.AiJson IS NULL OR t.AiJson = '' OR ISJSON(t.AiJson) = 0)
                    AND t.ImageUrl IS NOT NULL AND t.ImageUrl <> ''
                    AND t.SortOrder > 0  -- Only fetch positive SortOrder values
                    ORDER BY t.ResultID
                    OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
                """
                params = (file_id, limit)
            else:
                # Fetch records missing ImageUrl (unchanged for non-AI case)
                query = """
                    SELECT r.EntryID, r.FileID, r.ProductBrand, r.ProductCategory, r.ProductColor,
                           t.ResultID, t.ImageUrl, t.ImageUrlThumbnail
                    FROM utb_ImageScraperRecords r
                    LEFT JOIN utb_ImageScraperResult t ON r.EntryID = t.EntryID
                    WHERE r.FileID = ?
                    AND (t.ImageUrl IS NULL OR t.ImageUrl = '')
                    ORDER BY r.EntryID
                    OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
                """
                params = (file_id, limit)

            df = pd.read_sql(query, conn, params=params)
            logger.info(f"Fetched {len(df)} images for FileID {file_id}, ai_analysis_only={ai_analysis_only}")
            return df
    except pyodbc.Error as e:
        logger.error(f"Database error fetching missing images for FileID {file_id}: {e}", exc_info=True)
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Unexpected error fetching missing images for FileID {file_id}: {e}", exc_info=True)
        return pd.DataFrame()

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
import pyodbc
import pandas as pd
from config import conn_str
import logging

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)

def get_images_excel_db(file_id, logger=None):
    logger = logger or default_logger
    try:
        with pyodbc.connect(conn_str) as conn:
            query = """
                SELECT s.ExcelRowID, r.ImageUrl, r.ImageUrlThumbnail
                FROM utb_ImageScraperFiles f
                INNER JOIN utb_ImageScraperRecords s ON s.FileID = f.ID
                INNER JOIN utb_ImageScraperResult r ON r.EntryID = s.EntryID
                WHERE f.ID = ?
                ORDER BY s.ExcelRowID
            """
            df = pd.read_sql(query, conn, params=(file_id,))
            logger.info(f"Fetched {len(df)} images for Excel export for FileID {file_id}")
            return df
    except pyodbc.Error as e:
        logger.error(f"Error in get_images_excel_db: {e}")
        return pd.DataFrame()
# In database.py
def get_send_to_email(file_id: int, logger: Optional[logging.Logger] = None) -> str:
    """Retrieve the email address associated with a file ID."""
    logger = logger or logging.getLogger(__name__)
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            query = "SELECT UserEmail FROM utb_ImageScraperFiles WHERE ID = ?"
            cursor.execute(query, (file_id,))
            result = cursor.fetchone()
            if result and result[0]:
                return result[0]
            logger.warning(f"No email found for FileID {file_id}")
            return "nik@accessx.com"
    except Exception as e:
        logger.error(f"Error fetching email for FileID {file_id}: {e}")
        return "nik@accessx.com"
def update_file_generate_complete(file_id, logger=None):
    logger = logger or default_logger
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            query = "UPDATE utb_ImageScraperFiles SET CreateFileCompleteTime = GETDATE() WHERE ID = ?"
            cursor.execute(query, (file_id,))
            conn.commit()
            logger.info(f"Marked file generation complete for FileID: {file_id}")
    except pyodbc.Error as e:
        logger.error(f"Error in update_file_generate_complete: {e}")
def update_file_location_complete(file_id, file_location, logger=None):
    logger = logger or default_logger
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            query = "UPDATE utb_ImageScraperFiles SET FileLocationURLComplete = ? WHERE ID = ?"
            cursor.execute(query, (file_location, file_id))
            conn.commit()
            logger.info(f"Updated file location for FileID: {file_id}")
    except pyodbc.Error as e:
        logger.error(f"Error in update_file_location_complete: {e}")
# Test Wrapper Functions
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