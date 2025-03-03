# database.py
import pyodbc
import pandas as pd
import logging
import time
import traceback
import requests
import urllib
import re
import chardet
import json
import math
import asyncio
from fastapi import BackgroundTasks
from icon_image_lib.google_parser import get_original_images as GP
from image_processing import get_image_data, analyze_image_with_gemini,evaluate_with_grok_text
from config import conn_str, engine
from logging_config import setup_job_logger
import base64  # Added import
import zlib    # Added import for unpack_content
# Fallback logger for standalone calls

from config import GOOGLE_API_KEY
default_logger = logging.getLogger(__name__)

if not default_logger.handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def insert_file_db(file_name, file_source, send_to_email="nik@iconluxurygroup.com", logger=None):
    logger = logger or default_logger
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            insert_query = """
                INSERT INTO utb_ImageScraperFiles (FileName, FileLocationUrl, UserEmail) 
                OUTPUT INSERTED.Id 
                VALUES (?, ?, ?)
            """
            values = (file_name, file_source, send_to_email)
            cursor.execute(insert_query, values)
            file_id = cursor.fetchval()
            conn.commit()
            logger.info(f"Inserted new file record with ID: {file_id}")
            return file_id
    except Exception as e:
        logger.error(f"🔴 Error inserting file record: {e}")
        raise

def update_database(result_id, ai_json, ai_caption=None, logger=None):
    logger = logger or default_logger
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            update_query = """
                UPDATE utb_ImageScraperResult 
                SET aijson = ?, aicaption = ? 
                WHERE ResultID = ?
            """
            cursor.execute(update_query, (ai_json, ai_caption, result_id))
            conn.commit()
            logger.info(f"Updated database for ResultID: {result_id}")
    except Exception as e:
        logger.error(f"🔴 Error updating database: {e}")
        raise

def load_payload_db(rows, file_id, logger=None):
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM utb_ImageScraperFiles WHERE ID = ?", (file_id,))
            if cursor.fetchone()[0] == 0:
                raise ValueError(f"FileID {file_id} does not exist in utb_ImageScraperFiles")
            
            df = pd.DataFrame(rows).rename(columns={
                'absoluteRowIndex': 'ExcelRowID',
                'searchValue': 'ProductModel',
                'brandValue': 'ProductBrand',
                'colorValue': 'ProductColor',
                'CategoryValue': 'ProductCategory'
            })
            
            if 'ProductBrand' not in df.columns or df['ProductBrand'].isnull().all():
                logger.warning(f"⚠️ No valid ProductBrand data found in payload for FileID {file_id}")
                df['ProductBrand'] = df.get('ProductBrand', '')
            
            df.insert(0, 'FileID', file_id)
            if 'imageValue' in df.columns:
                df.drop(columns=['imageValue'], inplace=True)
            
            logger.debug(f"Inserting data for FileID {file_id}: {df[['ProductBrand', 'ProductModel']].head().to_dict()}")
            for _, row in df.iterrows():
                cursor.execute(
                    f"INSERT INTO utb_ImageScraperRecords ({', '.join(df.columns)}) VALUES ({', '.join(['?'] * len(df.columns))})",
                    tuple(row)
                )
            connection.commit()
        logger.info(f"Loaded {len(df)} rows into utb_ImageScraperRecords for FileID: {file_id}")
        return df
    except Exception as e:
        logger.error(f"🔴 Error loading payload data: {e}")
        raise

def unpack_content(encoded_content, logger=None):
    logger = logger or default_logger
    try:
        if encoded_content:
            compressed_content = base64.b64decode(encoded_content)
            original_content = zlib.decompress(compressed_content)
            return original_content
        return None
    except Exception as e:
        logger.error(f"🔴 Error unpacking content: {e}")
        return None
# database.py (partial update)
def get_records_to_search(file_id, logger=None):
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        sql_query = """
            SELECT EntryID, ProductModel AS SearchString, 'model_only' AS SearchType, FileID
            FROM utb_ImageScraperRecords 
            WHERE FileID = ? AND Step1 IS NULL
            UNION ALL
            SELECT EntryID, 
                   CASE 
                       WHEN ProductBrand IS NULL OR ProductBrand = '' 
                       THEN ProductModel 
                       ELSE ProductModel + ' ' + ProductBrand 
                   END AS SearchString, 
                   'model_plus_brand' AS SearchType, 
                   FileID
            FROM utb_ImageScraperRecords 
            WHERE FileID = ? AND Step1 IS NULL
            ORDER BY EntryID, SearchType
        """
        with pyodbc.connect(conn_str) as connection:
            # Debug: Check total records for FileID
            cursor = connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM utb_ImageScraperRecords WHERE FileID = ?", (file_id,))
            total_records = cursor.fetchone()[0]
            logger.debug(f"Total records for FileID {file_id}: {total_records}")
            
            # Debug: Check records with Step1 IS NULL
            cursor.execute("SELECT COUNT(*) FROM utb_ImageScraperRecords WHERE FileID = ? AND Step1 IS NULL", (file_id,))
            unprocessed_records = cursor.fetchone()[0]
            logger.debug(f"Unprocessed records (Step1 IS NULL) for FileID {file_id}: {unprocessed_records}")
            
            df = pd.read_sql_query(sql_query, connection, params=[file_id, file_id])
        
        if not df.empty:
            invalid_rows = df[df['FileID'] != file_id]
            if not invalid_rows.empty:
                logger.error(f"Found {len(invalid_rows)} rows with incorrect FileID for requested FileID {file_id}: {invalid_rows[['EntryID', 'FileID']].to_dict()}")
                df = df[df['FileID'] == file_id]
        
        logger.info(f"📋 Got {len(df)} search records (2 per EntryID) for FileID: {file_id}")
        return df[['EntryID', 'SearchString', 'SearchType']]
    except Exception as e:
        logger.error(f"🔴 Error getting records to search for FileID {file_id}: {e}", exc_info=True)
        return pd.DataFrame()

def process_search_row(search_string, endpoint, entry_id, retries=0, max_retries=10,logger=None):
    logger = logger or default_logger
    logger.debug(f"Entering process_search_row with search_string={search_string}, endpoint={endpoint}, entry_id={entry_id}")
    logger.info(f"🔎 Search started for {search_string}")
    logger.info(f"👺 Search Proxy: {endpoint}")
    logger.info(f"📘Entry ID: {entry_id}")
    try:
        if not search_string or len(search_string.strip()) < 3:
            logger.warning(f"⚠️ Invalid search string for EntryID {entry_id}: '{search_string}'")
            return False
        
        search_url = f"{endpoint}?query={urllib.parse.quote(search_string)}"
        logger.info(f"🔍 Searching URL: {search_url}")
        
        response = requests.get(search_url, timeout=60)
        if response.status_code != 200:
            logger.warning(f"⚠️ Non-200 status {response.status_code} for {search_url}")
            remove_endpoint(endpoint, logger=logger)
            new_endpoint = get_endpoint(logger=logger)
            return process_search_row(search_string, new_endpoint, entry_id, logger=logger)
        
        try:
            response_json = response.json()
            result = response_json.get('body', None)
        except json.JSONDecodeError:
            logger.warning(f"⚠️ Invalid JSON response for {search_url}")
            remove_endpoint(endpoint, logger=logger)
            new_endpoint = get_endpoint(logger=logger)
            return process_search_row(search_string, new_endpoint, entry_id, logger=logger)
        
        if not result:
            logger.warning(f"⚠️ No body in response for {search_url}")
            remove_endpoint(endpoint, logger=logger)
            new_endpoint = get_endpoint(logger=logger)
            return process_search_row(search_string, new_endpoint, entry_id, logger=logger)
        
        unpacked_html = unpack_content(result, logger=logger)
        if not unpacked_html or len(unpacked_html) < 100:
            logger.warning(f"⚠️ Invalid unpacked HTML for {search_url} (length: {len(unpacked_html) if unpacked_html else 'None'})")
            remove_endpoint(endpoint, logger=logger)
            new_endpoint = get_endpoint(logger=logger)
            return process_search_row(search_string, new_endpoint, entry_id, logger=logger)
    
        parsed_data = GP(unpacked_html)  # unpacked_html is bytes
        
        if not parsed_data or not isinstance(parsed_data, tuple) or not parsed_data[0]:
            logger.warning(f"⚠️ No valid parsed data for {search_url}")
            remove_endpoint(endpoint, logger=logger)
            new_endpoint = get_endpoint(logger=logger)
            return process_search_row(search_string, new_endpoint, entry_id, logger=logger, retries=retries)
        
        urls, descriptions, sources, thumbnails = parsed_data
        
        if urls and urls[0] != 'No google image results found':
            df = pd.DataFrame({
                'EntryId': [entry_id] * len(urls),
                'ImageUrl': urls,
                'ImageDesc': descriptions,
                'ImageSource': sources,
                'ImageUrlThumbnail': thumbnails
            })
            df.to_sql(name='utb_ImageScraperResult', con=engine, index=False, if_exists='append')
            
            with pyodbc.connect(conn_str) as connection:
                cursor = connection.cursor()
                cursor.execute(f"UPDATE utb_ImageScraperRecords SET Step1 = GETDATE() WHERE EntryID = {entry_id}")
                connection.commit()
            
            logger.info(f"✅ Processed EntryID {entry_id} with {len(df)} images")
            return df
        
        # No valid image URLs found
        if retries < max_retries:
            logger.warning(f"⚠️ No valid image URL, retrying ({retries + 1}/{max_retries}) with new endpoint")
            remove_endpoint(endpoint, logger=logger)
            new_endpoint = get_endpoint(logger=logger)
            return process_search_row(search_string, new_endpoint, entry_id, logger=logger, retries=retries + 1)
        else:
            # Max retries reached, update Step1 and skip
            with pyodbc.connect(conn_str) as connection:
                cursor = connection.cursor()
                cursor.execute(f"UPDATE utb_ImageScraperRecords SET Step1 = GETDATE() WHERE EntryID = {entry_id}")
                connection.commit()
            logger.warning(f"⚠️ Max retries ({max_retries}) reached for EntryID {entry_id}, marked as completed with no results")
            return None  # Or pd.DataFrame() if you prefer an empty DataFrame

    except requests.RequestException as e:
        logger.error(f"🔴 Request error: {e}", exc_info=True)
        remove_endpoint(endpoint, logger=logger)
        new_endpoint = get_endpoint(logger=logger)
        logger.info(f"Trying again with new endpoint: {new_endpoint}")
        return process_search_row(search_string, new_endpoint, entry_id, logger=logger, retries=retries)
    except pyodbc.Error as e:
        logger.error(f"🔴 Database error: {e}", exc_info=True)
        raise  # Re-raise the exception to let the caller handle it


def convert_sets_to_lists(obj):
    """Recursively convert sets to lists in a dictionary."""
    if isinstance(obj, set):
        return list(obj)
    elif isinstance(obj, dict):
        return {k: convert_sets_to_lists(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_sets_to_lists(item) for item in obj]
    return obj
# database.py
async def batch_process_images(file_id, limit=1000, logger=None, rpm_limit=15):
    logger = logger or default_logger
    logger.info(f"Starting AI Batch for FileID: {file_id}")
    try:
        missing_df = fetch_missing_images(file_id=file_id, limit=limit, ai_analysis_only=True, logger=logger)
        if missing_df.empty:
            logger.info(f"No images need AI processing for FileID: {file_id}")
            return

        processed_count = 0
        delay_between_requests = 60 / rpm_limit
        completed_entry_ids = set()

        for _, row in missing_df.iterrows():
            result_id = row['ResultID']
            entry_id = row['EntryID']
            image_url = row['ImageUrl']

            logger.debug(f"Processing ResultID: {result_id}, EntryID: {entry_id}, URL: {image_url}")
            try:
                image_data = get_image_data(image_url)
                if not image_data:
                    logger.warning(f"⚠️ No image data retrieved for ResultID {result_id}")
                    ai_json = json.dumps({"error": "Failed to fetch image"})
                    update_database(result_id, ai_json, "Failed to fetch image", logger=logger)
                    continue

                image_data_base64 = base64.b64encode(image_data).decode('utf-8')
                features = await analyze_image_with_gemini(image_data_base64, api_key=GOOGLE_API_KEY)
                logger.debug(f"Gemini result for ResultID {result_id}: {features}")

                if features["success"]:
                    extracted_features = features["features"]["extracted_features"]
                    description = features["features"]["description"]
                    
                    product_details = {
                        "brand": row.get('ProductBrand', ''),
                        "category": row.get('ProductCategory', ''),
                        "color": row.get('ProductColor', '')
                    }
                    logger.debug(f"Product details for ResultID {result_id}: {product_details}")

                    scores = await evaluate_with_grok_text(extracted_features, product_details)
                    if scores.get("match_score") is None or scores.get("linesheet_score") is None:
                        logger.warning(f"Grok returned invalid scores for ResultID {result_id}: {scores}")
                        scores = {
                            "match_score": None,
                            "linesheet_score": None,
                            "reasoning_match": "Invalid or missing score",
                            "reasoning_linesheet": "Invalid or missing score"
                        }

                    ai_json_dict = {
                        "description": description,
                        "user_provided": product_details,
                        "extracted_features": extracted_features,
                        "match_score": scores.get("match_score"),
                        "reasoning_match": scores.get("reasoning_match", ""),
                        "linesheet_score": scores.get("linesheet_score"),
                        "reasoning_linesheet": scores.get("reasoning_linesheet", "")
                    }
                    ai_json = json.dumps(convert_sets_to_lists(ai_json_dict))
                    update_database(result_id, ai_json, description, logger=logger)
                    processed_count += 1

                    match_score = scores.get("match_score")
                    linesheet_score = scores.get("linesheet_score")
                    if (match_score is not None and linesheet_score is not None and
                        match_score >= 95 and linesheet_score >= 80):
                        completed_entry_ids.add(entry_id)
                        logger.info(f"EntryID {entry_id} marked complete: match={match_score}, linesheet={linesheet_score}")
                else:
                    logger.warning(f"Gemini failed for ResultID {result_id}: {features['text']}")
                    ai_json_dict = {"error": features["text"]}
                    ai_json = json.dumps(convert_sets_to_lists(ai_json_dict))
                    update_database(result_id, ai_json, features["text"], logger=logger)

                await asyncio.sleep(delay_between_requests)

            except Exception as e:
                logger.error(f"Failed to process ResultID {result_id} (URL: {image_url}): {e}")
                ai_json_dict = {"error": str(e)}
                ai_json = json.dumps(convert_sets_to_lists(ai_json_dict))
                update_database(result_id, ai_json, "AI analysis failed", logger=logger)
                continue

        logger.info(f"Processed {processed_count} out of {len(missing_df)} images for FileID: {file_id}")
    except Exception as e:
        logger.error(f"🔴 Error in batch_process_images for FileID {file_id}: {e}")
        raise

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
        logger.error(f"🔴 Error fetching images for FileID {file_id}: {e}")
        return pd.DataFrame()
def fetch_missing_images(file_id, limit=8, ai_analysis_only=True, logger=None):
    """
    Fetch missing images for a specific file identified by file_id.
    
    Args:
        file_id (int): The ID of the file to fetch missing images for (required).
        limit (int): Maximum number of records to return (default: 8).
        ai_analysis_only (bool): If True, only fetch records with an existing ImageUrl but invalid or missing AI analysis.
                                 If False, fetch records missing in the result table, with empty ImageUrl, or with invalid AI analysis.
                                 (default: True).
        logger: Logger instance (default: None, uses default_logger).
    
    Returns:
        pd.DataFrame: DataFrame containing missing image records for the specified file.
    """
    logger = logger or default_logger
    try:
        connection = pyodbc.connect(conn_str, timeout=60)
        query_params = []

        if ai_analysis_only:
            query = """
            SELECT t.ResultID, t.EntryID, t.ImageUrl AS ImageUrl, r.ProductBrand, r.ProductCategory, r.ProductColor
            FROM utb_ImageScraperResult t
            INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
            WHERE t.ImageUrl IS NOT NULL 
              AND t.ImageUrl <> ''
              AND (
                  ISJSON(t.aijson) = 0 
                  OR t.aijson IS NULL
                  OR JSON_VALUE(t.aijson, '$.linesheet_score') IS NULL
                  OR JSON_VALUE(t.aijson, '$.linesheet_score') IN ('NaN', 'null', 'undefined')
                  OR JSON_VALUE(t.aijson, '$.match_score') IS NULL
                  OR JSON_VALUE(t.aijson, '$.match_score') IN ('NaN', 'null', 'undefined')
              )
              AND r.FileID = ?
            """
            query_params.append(file_id)
        else:
            query = """
            -- Records missing in result table completely
            SELECT NULL AS ResultID, r.EntryID, NULL AS ImageUrl, 
                   r.ProductBrand, r.ProductCategory, r.ProductColor
            FROM utb_ImageScraperRecords r
            LEFT JOIN utb_ImageScraperResult t ON r.EntryID = t.EntryID
            WHERE t.EntryID IS NULL AND r.FileID = ?
            
            UNION ALL
            
            -- Records with empty or NULL ImageUrl
            SELECT t.ResultID, t.EntryID, t.ImageUrl AS ImageUrl, 
                   r.ProductBrand, r.ProductCategory, r.ProductColor
            FROM utb_ImageScraperResult t
            INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
            WHERE (t.ImageUrl IS NULL OR t.ImageUrl = '') AND r.FileID = ?
            
            UNION ALL
            
            -- Records with URLs but missing AI analysis
            SELECT t.ResultID, t.EntryID, t.ImageUrl AS ImageUrl, 
                   r.ProductBrand, r.ProductCategory, r.ProductColor
            FROM utb_ImageScraperResult t
            INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
            WHERE t.ImageUrl IS NOT NULL 
              AND t.ImageUrl <> ''
              AND (
                  ISJSON(t.aijson) = 0 
                  OR t.aijson IS NULL
                  OR JSON_VALUE(t.aijson, '$.linesheet_score') IS NULL
                  OR JSON_VALUE(t.aijson, '$.linesheet_score') IN ('NaN', 'null', 'undefined')
                  OR JSON_VALUE(t.aijson, '$.match_score') IS NULL
                  OR JSON_VALUE(t.aijson, '$.match_score') IN ('NaN', 'null', 'undefined')
              )
              AND r.FileID = ?
            """
            query_params.extend([file_id, file_id, file_id])

        query += " ORDER BY EntryID ASC OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY"
        query_params.append(limit)

        df = pd.read_sql_query(query, connection, params=query_params)
        connection.close()
        
        if df.empty:
            logger.info(f"🟩 No missing images found for FileID: {file_id}")
        else:
            logger.info(f"📒 Found {len(df)} missing images for FileID: {file_id}")
            logger.debug(f"Fetched DataFrame columns: {df.columns.tolist()}")
        
        # Ensure 'ImageUrl' column consistency
        if 'ImageURL' in df.columns:
            df.rename(columns={'ImageURL': 'ImageUrl'}, inplace=True)
            logger.debug(f"Renamed 'ImageURL' to 'ImageUrl' in DataFrame")
        elif 'ImageUrl' not in df.columns:
            logger.error(f"🔴 Expected 'ImageUrl' or 'ImageURL' column not found: {df.columns.tolist()}")
            raise KeyError("ImageUrl column missing in DataFrame")
        
        return df
    except Exception as e:
        logger.error(f"🔴 Error fetching missing images for FileID {file_id}: {e}")
        return pd.DataFrame()
    
def update_initial_sort_order(file_id, logger=None):
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as connection:
            connection.timeout = 300
            cursor = connection.cursor()
            logger.info(f"🔀 Setting initial SortOrder for FileID: {file_id}")
            
            cursor.execute("BEGIN TRANSACTION")
            cursor.execute("UPDATE utb_ImageScraperRecords SET Step1 = NULL WHERE FileID = ?", (file_id,))
            reset_step1_count = cursor.rowcount
            logger.info(f"🔄Reset Step1 for {reset_step1_count} rows in utb_ImageScraperRecords")
            
            cursor.execute("UPDATE utb_ImageScraperResult SET SortOrder = NULL WHERE EntryID IN (SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?)", (file_id,))
            reset_sort_count = cursor.rowcount
            logger.info(f"🔄Reset SortOrder for {reset_sort_count} rows in utb_ImageScraperResult")
            
            initial_sort_query = """
                WITH toupdate AS (
                    SELECT t.*, 
                           ROW_NUMBER() OVER (PARTITION BY t.EntryID ORDER BY t.ResultID) AS seqnum
                    FROM utb_ImageScraperResult t 
                    INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID 
                    WHERE r.FileID = ?
                ) 
                UPDATE toupdate 
                SET SortOrder = seqnum;
            """
            cursor.execute(initial_sort_query, (file_id,))
            update_count = cursor.rowcount
            logger.info(f"🟢 Set initial SortOrder for {update_count} rows")
            
            cursor.execute("COMMIT")
            
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
                logger.info(f"✍️ Initial - EntryID: {record[1]}, ResultID: {record[0]}, SortOrder: {record[2]}")
            
            count_query = "SELECT COUNT(*) FROM utb_ImageScraperResult WHERE EntryID IN (SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?)"
            cursor.execute(count_query, (file_id,))
            total_results = cursor.fetchone()[0]
            logger.info(f"🟩 Total results after initial sort for FileID {file_id}: {total_results}")
            if total_results < 16:
                logger.warning(f"⚠️  Expected at least 16 results (8 per search type), got {total_results}")
            
            return [{"ResultID": row[0], "EntryID": row[1], "SortOrder": row[2]} for row in results]
    except Exception as e:
        logger.error(f"🔴 Error setting initial SortOrder: {e}")
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
            logger.debug(f"🔍🔀 Updating Search SortOrder for FileID: {file_id}")
            
            initial_sort_query = """
                WITH toupdate AS (
                    SELECT t.*, 
                        ROW_NUMBER() OVER (PARTITION BY t.EntryID ORDER BY t.ResultID) AS seqnum
                    FROM utb_ImageScraperResult t 
                    INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID 
                    WHERE r.FileID = ?
                ) 
                UPDATE toupdate 
                SET SortOrder = seqnum;
            """
            cursor.execute("BEGIN TRANSACTION")
            cursor.execute(initial_sort_query, (file_id,))
            update_count = cursor.rowcount
            cursor.execute("COMMIT")
            logger.info(f"🟢 Set Search SortOrder for {update_count} rows")
            
            verify_query = """
                SELECT TOP 20 t.ResultID, t.EntryID, t.SortOrder
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                WHERE r.FileID = ?
                ORDER BY t.EntryID, t.SortOrder
            """
            cursor.execute(verify_query, (file_id,))
            results = cursor.fetchall()
            for record in results:
                logger.info(f"✍️ Search - EntryID: {record[1]}, ResultID: {record[0]}, SortOrder: {record[2]}")
            
            return [{"ResultID": row[0], "EntryID": row[1], "SortOrder": row[2]} for row in results]
    except Exception as e:
        logger.error(f"🔴 Error setting Search SortOrder: {e}")
        return None
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

def update_ai_sort_order(file_id, logger=None):
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as connection:
            connection.timeout = 300
            cursor = connection.cursor()
            logger.info(f"🔀 Updating AI-based SortOrder for FileID: {file_id}")
            
            debug_query = """
                SELECT TOP 20 t.ResultID, t.EntryID, t.SortOrder, t.aijson,
                       CASE WHEN ISJSON(t.aijson) = 1 THEN JSON_VALUE(t.aijson, '$.match_score') ELSE '-1' END AS match_score,
                       CASE WHEN ISJSON(t.aijson) = 1 THEN JSON_VALUE(t.aijson, '$.linesheet_score') ELSE '-1' END AS linesheet_score
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                WHERE r.FileID = ?
                ORDER BY t.EntryID, t.SortOrder
            """
            cursor.execute(debug_query, (file_id,))
            current_order = cursor.fetchall()
            for record in current_order:
                logger.info(f"✍️ Pre-AI - EntryID: {record[1]}, ResultID: {record[0]}, SortOrder: {record[2]}, MatchScore: {record[4]}, LinesheetScore: {record[5]}, aijson: {record[3][:100] if record[3] else 'None'}")
            
            ai_sort_query = """
                WITH RankedResults AS (
                    SELECT 
                        t.ResultID,
                        t.EntryID,
                        ROW_NUMBER() OVER (
                            PARTITION BY t.EntryID 
                            ORDER BY 
                                CASE WHEN ISJSON(t.aijson) = 1 AND ISNUMERIC(JSON_VALUE(t.aijson, '$.match_score')) = 1 
                                     THEN CAST(JSON_VALUE(t.aijson, '$.match_score') AS FLOAT) ELSE -1 END DESC,
                                CASE WHEN ISJSON(t.aijson) = 1 AND ISNUMERIC(JSON_VALUE(t.aijson, '$.linesheet_score')) = 1 
                                     THEN CAST(JSON_VALUE(t.aijson, '$.linesheet_score') AS FLOAT) ELSE -1 END DESC,
                                t.ResultID ASC
                        ) AS rank
                    FROM utb_ImageScraperResult t
                    INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                    WHERE r.FileID = ?
                )
                UPDATE utb_ImageScraperResult
                SET SortOrder = rr.rank
                FROM utb_ImageScraperResult t
                INNER JOIN RankedResults rr ON t.ResultID = rr.ResultID;
            """
            cursor.execute("BEGIN TRANSACTION")
            cursor.execute(ai_sort_query, (file_id,))
            update_count = cursor.rowcount
            cursor.execute("COMMIT")
            logger.info(f"Updated AI SortOrder for {update_count} rows")
            
            cursor.execute(debug_query, (file_id,))
            updated_order = cursor.fetchall()
            for record in updated_order:
                logger.info(f"✍️ Post-AI - EntryID: {record[1]}, ResultID: {record[0]}, SortOrder: {record[2]}, MatchScore: {record[4]}, LinesheetScore: {record[5]}, aijson: {record[3][:100] if record[3] else 'None'}")
            
            return [{"ResultID": row[0], "EntryID": row[1], "SortOrder": row[2]} for row in updated_order]
    except Exception as e:
        logger.error(f"🔴 Error updating AI SortOrder: {e}")
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
        logger.error(f"🔴 Error updating file location URL: {e}")
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
        logger.error(f"🔴 Error updating file generation completion time: {e}")
        raise

def get_file_location(file_id, logger=None):
    logger = logger or default_logger
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            query = "SELECT FileLocationUrl FROM utb_ImageScraperFiles WHERE ID = ?"
            cursor.execute(query, (file_id,))
            file_location_url = cursor.fetchone()
            if file_location_url:
                logger.info(f"Got file location URL for FileID: {file_id}: {file_location_url[0]}")
                return file_location_url[0]
            logger.warning(f"⚠️ No file location URL found for FileID: {file_id}")
            return "No File Found"
    except Exception as e:
        logger.error(f"🔴 Error getting file location URL: {e}")
        return "Error retrieving file location"

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
            logger.warning(f"⚠️ No email address found for FileID: {file_id}")
            return "No Email Found"
    except Exception as e:
        logger.error(f"🔴 Error getting email address: {e}")
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
                SELECT s.ExcelRowID, r.ImageUrl, r.ImageUrlThumbnail 
                FROM utb_ImageScraperFiles f
                INNER JOIN utb_ImageScraperRecords s ON s.FileID = f.ID 
                INNER JOIN utb_ImageScraperResult r ON r.EntryID = s.EntryID 
                WHERE f.ID = ? AND r.SortOrder = 1
                ORDER BY s.ExcelRowID
            """
            df = pd.read_sql_query(query, connection, params=[file_id])
            logger.info(f"🕵️ Queried FileID: {file_id}, retrieved {len(df)} images for Excel export")
        return df
    except Exception as e:
        logger.error(f"🔴 Error getting images for Excel export: {e}")
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
        logger.error(f"🔴 Error executing stored procedure to match products: {e}")

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
                logger.debug(f"Got endpoint URL: {endpoint}")
                return endpoint
            else:
                logger.warning("No endpoint URL found")
                return "No EndpointURL"
    except Exception as e:
        logger.error(f"🔴 Error getting endpoint URL: {e}")
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
        logger.error(f"🔴 Error marking endpoint as blocked: {e}")

def check_json_status(file_id, logger=None):
    logger = logger or default_logger
    try:
        connection = pyodbc.connect(conn_str)
        cursor = connection.cursor()
        
        total_query = """
            SELECT COUNT(*) 
            FROM utb_ImageScraperResult t
            INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
            WHERE r.FileID = ?
        """
        cursor.execute(total_query, (file_id,))
        total_count = cursor.fetchone()[0]
        
        if total_count == 0:
            cursor.close()
            connection.close()
            return {
                "file_id": file_id,
                "status": "no_records",
                "message": "No records found for this file ID"
            }
        
        issues_data = {}
        
        null_query = """
            SELECT COUNT(*) 
            FROM utb_ImageScraperResult t
            INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
            WHERE r.FileID = ? AND t.aijson IS NULL
        """
        cursor.execute(null_query, (file_id,))
        issues_data["null_json"] = cursor.fetchone()[0]
        
        empty_query = """
            SELECT COUNT(*) 
            FROM utb_ImageScraperResult t
            INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
            WHERE r.FileID = ? AND t.aijson = ''
        """
        cursor.execute(empty_query, (file_id,))
        issues_data["empty_json"] = cursor.fetchone()[0]
        
        invalid_format_query = """
            SELECT COUNT(*) 
            FROM utb_ImageScraperResult t
            INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
            WHERE r.FileID = ? AND t.aijson IS NOT NULL AND ISJSON(t.aijson) = 0
        """
        cursor.execute(invalid_format_query, (file_id,))
        issues_data["invalid_format"] = cursor.fetchone()[0]
        
        invalid_match_score_query = """
            SELECT COUNT(*) 
            FROM utb_ImageScraperResult t
            INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
            WHERE r.FileID = ? 
            AND t.aijson IS NOT NULL 
            AND ISJSON(t.aijson) = 1
            AND (
                JSON_VALUE(t.aijson, '$.match_score') IS NULL
                OR JSON_VALUE(t.aijson, '$.match_score') IN ('NaN', 'null', 'undefined')
                OR ISNUMERIC(JSON_VALUE(t.aijson, '$.match_score')) = 0
            )
        """
        cursor.execute(invalid_match_score_query, (file_id,))
        issues_data["invalid_match_score"] = cursor.fetchone()[0]
        
        invalid_linesheet_score_query = """
            SELECT COUNT(*) 
            FROM utb_ImageScraperResult t
            INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
            WHERE r.FileID = ? 
            AND t.aijson IS NOT NULL 
            AND ISJSON(t.aijson) = 1
            AND (
                JSON_VALUE(t.aijson, '$.linesheet_score') IS NULL
                OR JSON_VALUE(t.aijson, '$.linesheet_score') IN ('NaN', 'null', 'undefined')
                OR ISNUMERIC(JSON_VALUE(t.aijson, '$.linesheet_score')) = 0
            )
        """
        cursor.execute(invalid_linesheet_score_query, (file_id,))
        issues_data["invalid_linesheet_score"] = cursor.fetchone()[0]
        
        total_issues = sum(val for val in issues_data.values() if isinstance(val, int))
        
        sample_query = """
            SELECT TOP 5 t.ResultID, t.aijson
            FROM utb_ImageScraperResult t
            INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
            WHERE r.FileID = ? AND (
                t.aijson IS NULL
                OR t.aijson = ''
                OR ISJSON(t.aijson) = 0
            )
        """
        cursor.execute(sample_query, (file_id,))
        samples = cursor.fetchall()
        sample_data = [{"ResultID": row[0], "aijson_prefix": str(row[1])[:100] if row[1] else None} for row in samples]
        
        cursor.close()
        connection.close()
        
        issue_percentage = (total_issues / total_count * 100) if total_count > 0 else 0
        
        return {
            "file_id": file_id,
            "total_records": total_count,
            "total_issues": total_issues,
            "issue_percentage": round(issue_percentage, 2),
            "status": "needs_fixing" if issue_percentage > 0 else "healthy",
            "issue_breakdown": issues_data,
            "sample_issues": sample_data
        }
    except Exception as e:
        logger.error(f"🔴 Error checking JSON status: {e}")
        return {
            "file_id": file_id,
            "status": "error",
            "error_message": str(e)
        }

def fix_json_data(background_tasks: BackgroundTasks, file_id=None, limit=1000, logger=None):
    logger = logger or default_logger
    def background_fix_json():
        try:
            connection = pyodbc.connect(conn_str)
            cursor = connection.cursor()
            
            if file_id:
                query = f"""
                    SELECT TOP {limit} t.ResultID, t.aijson 
                    FROM utb_ImageScraperResult t
                    INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                    WHERE r.FileID = ? AND (
                        t.aijson IS NULL
                        OR ISJSON(t.aijson) = 0 
                        OR t.aijson = ''
                        OR LEFT(t.aijson, 1) = '.'
                        OR LEFT(t.aijson, 1) = ','
                        OR JSON_VALUE(t.aijson, '$.match_score') IN ('NaN', 'null', 'undefined')
                        OR JSON_VALUE(t.aijson, '$.linesheet_score') IN ('NaN', 'null', 'undefined')
                    )
                """
                try:
                    cursor.execute(query, (file_id,))
                except Exception as e:
                    logger.warning(f"⚠️ 🔴 Error in complex query: {e}, falling back to simpler query")
                    query = f"""
                        SELECT TOP {limit} t.ResultID, t.aijson 
                        FROM utb_ImageScraperResult t
                        INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                        WHERE r.FileID = ?
                    """
                    cursor.execute(query, (file_id,))
            else:
                query = """
                    SELECT TOP ? t.ResultID, t.aijson 
                    FROM utb_ImageScraperResult t
                    WHERE 
                        t.aijson IS NULL
                        OR ISJSON(t.aijson) = 0 
                        OR t.aijson = ''
                        OR LEFT(t.aijson, 1) = '.'
                        OR LEFT(t.aijson, 1) = ','
                """
                try:
                    cursor.execute(query, (limit,))
                except Exception as e:
                    logger.warning(f"⚠️ 🔴 Error in complex query: {e}, falling back to simpler query")
                    query = "SELECT TOP ? ResultID, aijson FROM utb_ImageScraperResult"
                    cursor.execute(query, (limit,))
            
            rows = cursor.fetchall()
            logger.info(f"Found {len(rows)} records with potentially invalid JSON")
            
            batch_size = 100
            total_fixed = 0
            error_count = 0
            
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                updates = []
                
                for row in batch:
                    result_id, aijson_value = row
                    try:
                        cleaned_json = clean_json(aijson_value, logger=logger)
                        if cleaned_json:
                            updates.append((cleaned_json, result_id))
                    except Exception as e:
                        logger.error(f"🔴 Error cleaning JSON for ResultID {result_id}: {e}")
                        error_count += 1
                
                if updates:
                    try:
                        cursor.executemany(
                            "UPDATE utb_ImageScraperResult SET aijson = ? WHERE ResultID = ?",
                            updates
                        )
                        connection.commit()
                        batch_fixed = len(updates)
                        total_fixed += batch_fixed
                        logger.info(f"Fixed {batch_fixed} records in batch (total: {total_fixed})")
                    except Exception as batch_error:
                        logger.error(f"🔴 Error in batch update: {batch_error}")
                        for cleaned_json, result_id in updates:
                            try:
                                cursor.execute(
                                    "UPDATE utb_ImageScraperResult SET aijson = ? WHERE ResultID = ?",
                                    (cleaned_json, result_id)
                                )
                                connection.commit()
                                total_fixed += 1
                            except Exception as row_error:
                                logger.error(f"🔴 Error updating ResultID {result_id}: {row_error}")
                                error_count += 1
            
            logger.info(f"JSON fix operation completed. Fixed: {total_fixed}, Errors: {error_count}")
            cursor.close()
            connection.close()
            
            return {
                "status": "completed",
                "records_processed": len(rows),
                "records_fixed": total_fixed,
                "errors": error_count
            }
        except Exception as e:
            logger.error(f"🔴 Error in background JSON fix: {e}")
            logger.error(traceback.format_exc())
            return {
                "status": "error",
                "error_message": str(e)
            }
    
    background_tasks.add_task(background_fix_json)
    return {
        "message": f"JSON fix operation initiated in background" + (f" for FileID: {file_id}" if file_id else " across all files"),
        "status": "processing",
        "limit": limit
    }

def clean_json(value, logger=None):
    logger = logger or default_logger
    if not value or not isinstance(value, str) or value.strip() in ["None", "null", "NaN", "undefined"]:
        return json.dumps({
            "description": "",
            "user_provided": {"brand": "", "category": "", "color": ""},
            "extracted_features": {"brand": "", "category": "", "color": ""},
            "match_score": None,
            "reasoning_match": "",
            "linesheet_score": None,
            "reasoning_linesheet": ""
        })
    
    value = value.strip()
    if value and value[0] not in ['{', '[', '"']:
        logger.warning(f"⚠️ Invalid JSON starting with '{value[0:10]}...' - replacing with default")
        return json.dumps({
            "description": "",
            "user_provided": {"brand": "", "category": "", "color": ""},
            "extracted_features": {"brand": "", "category": "", "color": ""},
            "match_score": None,
            "reasoning_match": "",
            "linesheet_score": None,
            "reasoning_linesheet": ""
        })
    
    try:
        parsed = json.loads(value)
        if not isinstance(parsed, dict):
            logger.warning("JSON is not a dictionary - replacing with default structure")
            return json.dumps({
                "description": "",
                "user_provided": {"brand": "", "category": "", "color": ""},
                "extracted_features": {"brand": "", "category": "", "color": ""},
                "match_score": None,
                "reasoning_match": "",
                "linesheet_score": None,
                "reasoning_linesheet": ""
            })
        
        if "linesheet_score" in parsed:
            if isinstance(parsed["linesheet_score"], float) and math.isnan(parsed["linesheet_score"]):
                parsed["linesheet_score"] = None
            elif parsed["linesheet_score"] in ["NaN", "null", "undefined", ""]:
                parsed["linesheet_score"] = None
                
        if "match_score" in parsed:
            if isinstance(parsed["match_score"], float) and math.isnan(parsed["match_score"]):
                parsed["match_score"] = None
            elif parsed["match_score"] in ["NaN", "null", "undefined", ""]:
                parsed["match_score"] = None
        
        if "description" not in parsed:
            parsed["description"] = ""
        if "user_provided" not in parsed:
            parsed["user_provided"] = {"brand": "", "category": "", "color": ""}
        if "extracted_features" not in parsed:
            parsed["extracted_features"] = {"brand": "", "category": "", "color": ""}
        if "match_score" not in parsed:
            parsed["match_score"] = None
        if "reasoning_match" not in parsed:
            parsed["reasoning_match"] = ""
        if "linesheet_score" not in parsed:
            parsed["linesheet_score"] = None
        if "reasoning_linesheet" not in parsed:
            parsed["reasoning_linesheet"] = ""
            
        for field_dict in ["user_provided", "extracted_features"]:
            if field_dict in parsed and isinstance(parsed[field_dict], dict):
                for field in ["brand", "category", "color"]:
                    if field not in parsed[field_dict]:
                        parsed[field_dict][field] = ""
        
        return json.dumps(parsed)
    except json.JSONDecodeError as e:
        logger.warning(f"⚠️ JSON decoding error: {e} for value: {value[:50]}...")
        return json.dumps({
            "description": "",
            "user_provided": {"brand": "", "category": "", "color": ""},
            "extracted_features": {"brand": "", "category": "", "color": ""},
            "match_score": None,
            "reasoning_match": "",
            "linesheet_score": None,
            "reasoning_linesheet": ""
        })