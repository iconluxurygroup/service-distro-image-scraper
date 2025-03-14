# database.py
import pyodbc
import pandas as pd
import logging
import time
import traceback
import requests,zlib
import urllib
import re,base64
import chardet
import json
import math
from fastapi import BackgroundTasks
from icon_image_lib.google_parser import get_original_images as GP
from image_processing import get_image_data, analyze_image_with_gemini,evaluate_with_grok_text
from config import conn_str, engine
from logging_config import setup_job_logger
import json
from typing import List, Optional, Tuple
import urllib.parse
import json
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from requests.exceptions import RequestException, Timeout, ConnectionError
import logging

# Fallback logger for standalone calls
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
        logger.error(f"Error inserting file record: {e}")
        raise
import os
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
        logger.error(f"Error updating database: {e}")
        raise

def load_payload_db(rows, file_id, column_map, logger=None):
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM utb_ImageScraperFiles WHERE ID = ?", (file_id,))
            if cursor.fetchone()[0] == 0:
                raise ValueError(f"FileID {file_id} does not exist in utb_ImageScraperFiles")

            # Create DataFrame from rows
            df = pd.DataFrame(rows).rename(columns={
                column_map['search']: 'ProductModel',
                column_map['brand']: 'ProductBrand',
                column_map.get('color', ''): 'ProductColor',
                column_map.get('category', ''): 'ProductCategory'
            })

            if 'ProductBrand' not in df.columns or df['ProductBrand'].isnull().all():
                logger.warning(f"No valid ProductBrand data found in payload for FileID {file_id}")
                df['ProductBrand'] = df.get('ProductBrand', '')

            # Add FileID and ExcelRowID
            df.insert(0, 'FileID', file_id)
            df.insert(1, 'ExcelRowID', range(1, len(df) + 1))

            # Handle image URL using ExcelRowImageRef (keep as-is)
            if column_map.get('ExcelRowImageRef') in df.columns:
                logger.debug(f"Image URLs loaded in ExcelRowImageRef for FileID {file_id}: {df[column_map['ExcelRowImageRef']].head().tolist()}")
            else:
                logger.warning(f"No ExcelRowImageRef column found in data for FileID {file_id}")
                df['ExcelRowImageRef'] = None  # Add ExcelRowImageRef column with NULL if not present

            logger.debug(f"Inserting data for FileID {file_id}: {df[['ProductBrand', 'ProductModel']].head().to_dict()}")

            # Define expected columns, including ExcelRowImageRef
            expected_cols = ['FileID', 'ExcelRowID', 'ProductModel', 'ProductBrand', 'ProductColor', 'ProductCategory', 'ExcelRowImageRef']

            # Normalize columns
            for col in expected_cols:
                if col not in df.columns:
                    df[col] = None
                elif col in ['ProductColor', 'ProductCategory']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    df[col] = df[col].where(df[col].notna(), None)

            # Insert into database
            for _, row in df.iterrows():
                row_values = [None if pd.isna(val) else val for val in row[expected_cols]]
                cursor.execute(
                    f"INSERT INTO utb_ImageScraperRecords ({', '.join(expected_cols)}) VALUES ({', '.join(['?'] * len(expected_cols))})",
                    tuple(row_values)
                )
            connection.commit()
        logger.info(f"Loaded {len(df)} rows into utb_ImageScraperRecords for FileID: {file_id}")
        return df
    except Exception as e:
        logger.error(f"Error loading payload data: {e}")
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
import asyncio
import base64
import json
import base64
import json
import logging
import cv2
import numpy as np
from skimage.metrics import structural_similarity as ssim

# Assuming these are defined elsewhere
from image_processing import get_image_data, analyze_image_with_gemini, evaluate_with_grok_text
from database import update_database

# Default logger
default_logger = logging.getLogger(__name__)
reference_images = {
    "group1": {
        "gender": "Men’s",
        "categories": {
            "category1": {
                "name": "Sneakers",
                "url": "https://scotchandsoda.com/cdn/shop/files/Hires_PNG-99831930_S29-SDE_750x.png",
                "terms": ["sneakers", "sneaker", "shoes", "shoe", "footwears", "footwear", "kicks", "kick"]
            },
            "category2": {
                "name": "Pants",
                "url": "https://scotchandsoda.com/cdn/shop/products/144785-Kimono_20Yes-FNT_750x.png",
                "terms": ["pants", "pant", "trousers", "trouser", "slacks", "slack", "leggings", "legging", "bottoms", "bottom","chino"]
            },
            "category3": {
                "name": "Jeans",
                "url": "https://scotchandsoda.com/cdn/shop/products/144785-Kimono_20Yes-FNT_750x.png",
                "terms": ["jeans", "jean", "denims", "denim", "blue jeans", "blue jean"]
            },
            "category4": {
                "name": "Shirts",
                "url": "https://scotchandsoda.com/cdn/shop/files/96172852-FNT_75dc908a-7955-44f0-a60b-623b8102ae34_750x.png",
                "terms": ["shirts", "shirt", "tops", "top", "blouses", "blouse", "tees", "tee", "t-shirts", "t-shirt","apparel"]
            }
        }
    },
    "group2": {
        "gender": "Women’s",
        "categories": {
            "category1": {
                "name": "Sneakers",
                "url": "https://scotchandsoda.com/cdn/shop/files/Hires_PNG-99831930_S29-SDE_750x.png",
                "terms": ["sneakers", "sneaker", "shoes", "shoe", "footwears", "footwear", "kicks", "kick"]
            },
            "category2": {
                "name": "Pants",
                "url": "https://scotchandsoda.com/cdn/shop/products/144785-Kimono_20Yes-FNT_750x.png",
                "terms": ["pants", "pant", "trousers", "trouser", "slacks", "slack", "leggings", "legging", "bottoms", "bottom"]
            },
            "category3": {
                "name": "Jeans",
                "url": "https://scotchandsoda.com/cdn/shop/products/144785-Kimono_20Yes-FNT_750x.png",
                "terms": ["jeans", "jean", "denims", "denim", "blue jeans", "blue jean"]
            },
            "category4": {
                "name": "Shirts",
                "url": "https://scotchandsoda.com/cdn/shop/files/96172852-FNT_75dc908a-7955-44f0-a60b-623b8102ae34_750x.png",
                "terms": ["shirts", "shirt", "tops", "top", "blouses", "blouse", "tees", "tee", "t-shirts", "t-shirt","apparel"]
            }
        }
    }
}
def decode_image_data(image_data):
    """Decode image data into a grayscale NumPy array for similarity comparison."""
    img_array = np.frombuffer(image_data, dtype=np.uint8)
    img = cv2.imdecode(img_array, cv2.IMREAD_GRAYSCALE)
    if img is None:
        raise ValueError("Could not decode image")
    img = cv2.resize(img, (100, 100))  # Resize for uniform comparison
    return img
import base64
import json
import requests
from urllib.error import HTTPError

# Assuming these are defined elsewhere
# from PIL import Image
# from skimage.metrics import structural_similarity as ssim
# async def analyze_image_with_gemini(base64_image_data)
# async def evaluate_with_grok_text(features, user_provided)
# def get_image_data(url)
# def decode_image_data(image_data)
# def update_database(result_id, ai_json, ai_caption, logger)

# Singular/plural helpers (unchanged)
def pluralize(word):
    if word.endswith('s') or word.endswith('x') or word.endswith('z') or word.endswith('ch') or word.endswith('sh'):
        return word + 'es'
    elif word.endswith('y') and word[-2] not in 'aeiou':
        return word[:-1] + 'ies'
    else:
        return word + 's'

def singularize(word):
    if word.endswith('ies') and word[:-3] + 'y' not in ['a', 'e', 'i', 'o', 'u']:
        return word[:-3] + 'y'
    elif word.endswith('es') and (word[:-2].endswith('s') or word[:-2].endswith('x') or word[:-2].endswith('z') or 
                                 word[:-2].endswith('ch') or word[:-2].endswith('sh')):
        return word[:-2]
    elif word.endswith('s'):
        return word[:-1]
    return word

def find_category(extracted_category, reference_images, gender=None):
    """Check for category or term, considering singular and plural forms, with optional gender filter."""
    singular_form = singularize(extracted_category.lower())
    plural_form = pluralize(extracted_category.lower())
    forms_to_check = {singular_form, plural_form}

    results = []
    for group_key, group_data in reference_images.items():
        current_gender = group_data["gender"]
        if gender and current_gender.lower() != gender.lower():
            continue
        
        for cat_key, cat_data in group_data["categories"].items():
            cat_name_lower = cat_data["name"].lower()
            terms_lower = [term.lower() for term in cat_data["terms"]]
            
            if (cat_name_lower in forms_to_check or 
                pluralize(cat_name_lower) in forms_to_check or 
                singularize(cat_name_lower) in forms_to_check or 
                any(term in forms_to_check for term in terms_lower)):
                results.append({
                    "gender": current_gender,
                    "category": cat_data["name"],
                    "reference_image": cat_data["url"],
                    "terms": cat_data["terms"]
                })
    
    return results if results else None

async def process_single_image(result_id, image_url, thumbnail_url, user_provided, logger=None):
    logger = logger or default_logger
    try:
        # Step 1: Try processing the primary image
        image_data = get_image_data(image_url)
        if image_data:
            return await analyze_and_compute_similarity(result_id, image_url, image_data, user_provided, logger)
        else:
            logger.warning(f"No image data for primary URL: {image_url}, falling back to thumbnail")
            # Step 2: Fallback to thumbnail
            thumbnail_data = get_image_data(thumbnail_url)
            if thumbnail_data:
                return await analyze_and_compute_similarity(result_id, thumbnail_url, thumbnail_data, user_provided, logger)
            else:
                logger.warning(f"No image data for thumbnail URL: {thumbnail_url}")
                # Step 3: Handle complete failure
                return await handle_failure(result_id, user_provided, "No image data available", logger)
    except Exception as e:
        logger.error(f"Error processing image for ResultID {result_id}: {str(e)}")
        return await handle_failure(result_id, user_provided, str(e), logger)

async def analyze_and_compute_similarity(result_id, image_url, image_data, user_provided, logger):
    """Analyze image and compute similarity score."""
    base64_image_data = base64.b64encode(image_data).decode('utf-8')
    gemini_result = await analyze_image_with_gemini(base64_image_data)
    
    if gemini_result['success']:
        features = gemini_result['features']
        grok_result = await evaluate_with_grok_text(features['extracted_features'], user_provided)
        
        combined_result = {
            "description": features['description'],
            "user_provided": user_provided,
            "extracted_features": features['extracted_features'],
            "gemini_confidence_score": features['gemini_confidence_score'],
            "reasoning_confidence": features['reasoning_confidence'],
            "match_score": grok_result['match_score'],
            "reasoning_match": grok_result['reasoning_match'],
        }
        
        # Compute similarity score if category matches a reference
        extracted_category = features['extracted_features'].get('category', '').lower()
        mapped_categories = find_category(extracted_category, reference_images)
        
        if mapped_categories:
            # Default to Men’s if multiple matches; take the first match for Men’s
            target_gender = "Men’s"
            reference_result = next((r for r in mapped_categories if r["gender"] == target_gender), mapped_categories[0])
            reference_url = reference_result["reference_image"]
            reference_data = get_image_data(reference_url)
            
            if reference_data:
                try:
                    current_img = decode_image_data(image_data)
                    reference_img = decode_image_data(reference_data)
                    similarity_score = ssim(current_img, reference_img)
                    combined_result["similarity_score"] = similarity_score
                    logger.info(f"Computed similarity score {similarity_score} for {target_gender} {reference_result['category']} (ResultID: {result_id})")
                except Exception as e:
                    logger.warning(f"Failed to compute similarity for '{extracted_category}' (ResultID: {result_id}): {str(e)}")
                    combined_result["similarity_score"] = -1
            else:
                logger.warning(f"Failed to retrieve reference image for '{extracted_category}' at {reference_url} (ResultID: {result_id})")
                combined_result["similarity_score"] = -1
        else:
            logger.warning(f"No reference category found for '{extracted_category}' (ResultID: {result_id})")
            combined_result["similarity_score"] = -1
        
        ai_json = json.dumps(combined_result)
        ai_caption = features['description']
    else:
        logger.warning(f"Gemini analysis failed for {image_url} (ResultID: {result_id})")
        ai_json = json.dumps({
            "description": "Failed to analyze",
            "user_provided": user_provided,
            "extracted_features": {"brand": "", "category": "", "color": "", "composition": ""},
            "match_score": None,
            "similarity_score": -1
        })
        ai_caption = "AI analysis failed"
    
    update_database(result_id, ai_json, ai_caption, logger=logger)
    return True

async def handle_failure(result_id, user_provided, error_message, logger):
    """Handle processing failure with default values."""
    ai_json = json.dumps({
        "description": "Download or API error",
        "user_provided": user_provided,
        "extracted_features": {"brand": "", "category": "", "color": "", "composition": ""},
        "match_score": None,
        "similarity_score": -1,
        "error": error_message
    })
    update_database(result_id, ai_json, "AI analysis failed", logger=logger)
    return False
async def batch_process_images(file_id, limit=None, logger=None):
    logger = logger or default_logger
    if not limit:
        limit = 100
    
    semaphore = asyncio.Semaphore(limit)  # Limit to 100 concurrent tasks

    async def sem_process(result_id, image_url,image_thumb, user_provided):
        async with semaphore:
            return await process_single_image(result_id, image_url,image_thumb, user_provided, logger)

    try:
        missing_df = fetch_missing_images(file_id=file_id, limit=None, ai_analysis_only=True, logger=logger)
        if missing_df.empty:
            logger.info(f"No images need AI processing for FileID: {file_id}")
            return

        tasks = []
        for _, row in missing_df.iterrows():
            user_provided = {
            "brand": row['ProductBrand'],
            "category": row['ProductCategory'],
            "color": row['ProductColor']
        }
            tasks.append(sem_process(
            row['ResultID'], 
            row['ImageURL'], 
            row['ImageURLThumbnail'],  # Pass thumbnail URL
            user_provided
        ))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        processed_count = 0
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Exception in processing image {i} (ResultID: {missing_df.iloc[i]['ResultID']}): {result}")
            elif result is True:
                processed_count += 1
            else:
                logger.warning(f"Failed to process image {i} (ResultID: {missing_df.iloc[i]['ResultID']})")
        logger.info(f"Processed {processed_count} out of {len(missing_df)} images for AI analysis for FileID: {file_id}")
    except Exception as e:
        logger.error(f"Error in batch_process_images: {e}")
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
def update_ai_sort_order(file_id, logger=None):
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as connection:
            with connection.cursor() as cursor:
                logger.info(f"Updating AI-based SortOrder for FileID: {file_id}")
                
                ai_sort_query = """
                    WITH RankedResults AS (
                        SELECT 
                            t.ResultID,
                            t.EntryID,
                            ROW_NUMBER() OVER (
                                PARTITION BY t.EntryID 
                                ORDER BY 
                                    CASE 
                                        WHEN ISJSON(t.aijson) = 1 
                                        AND ISNUMERIC(JSON_VALUE(t.aijson, '$.match_score')) = 1 
                                        AND CAST(JSON_VALUE(t.aijson, '$.match_score') AS FLOAT) > 0 
                                        THEN 1 
                                        ELSE 0 
                                    END DESC,
                                    CASE 
                                        WHEN ISJSON(t.aijson) = 1 
                                        AND ISNUMERIC(JSON_VALUE(t.aijson, '$.match_score')) = 1 
                                        AND CAST(JSON_VALUE(t.aijson, '$.match_score') AS FLOAT) > 0 
                                        THEN CAST(JSON_VALUE(t.aijson, '$.similarity_score') AS FLOAT) 
                                        ELSE -1 
                                    END DESC,
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
                if update_count == 0:
                    logger.warning(f"No rows updated for FileID: {file_id}")
                cursor.execute("COMMIT")
                logger.info(f"Updated AI SortOrder for {update_count} rows")
    except Exception as e:
        logger.error(f"Error updating AI SortOrder: {e}")
        if 'cursor' in locals():
            cursor.execute("ROLLBACK")
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
            logger.warning(f"No file location URL found for FileID: {file_id}")
            return "No File Found"
    except Exception as e:
        logger.error(f"Error getting file location URL: {e}")
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
        logger.error(f"Error checking JSON status: {e}")
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
                    logger.warning(f"Error in complex query: {e}, falling back to simpler query")
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
                    logger.warning(f"Error in complex query: {e}, falling back to simpler query")
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
                        logger.error(f"Error cleaning JSON for ResultID {result_id}: {e}")
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
                        logger.error(f"Error in batch update: {batch_error}")
                        for cleaned_json, result_id in updates:
                            try:
                                cursor.execute(
                                    "UPDATE utb_ImageScraperResult SET aijson = ? WHERE ResultID = ?",
                                    (cleaned_json, result_id)
                                )
                                connection.commit()
                                total_fixed += 1
                            except Exception as row_error:
                                logger.error(f"Error updating ResultID {result_id}: {row_error}")
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
            logger.error(f"Error in background JSON fix: {e}")
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
        logger.warning(f"Invalid JSON starting with '{value[0:10]}...' - replacing with default")
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
        logger.warning(f"JSON decoding error: {e} for value: {value[:50]}...")
        return json.dumps({
            "description": "",
            "user_provided": {"brand": "", "category": "", "color": ""},
            "extracted_features": {"brand": "", "category": "", "color": ""},
            "match_score": None,
            "reasoning_match": "",
            "linesheet_score": None,
            "reasoning_linesheet": ""
        })