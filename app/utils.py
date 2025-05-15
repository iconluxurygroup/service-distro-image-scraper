# utils.py
# Contains search processing and endpoint utility functions

import logging
import asyncio
import os
import pandas as pd
import time
import pyodbc
import ray
from typing import List, Dict, Optional, Tuple, Any
from config import conn_str
from common import (
    clean_string,
    generate_aliases,
    fetch_brand_rules,
    normalize_model,
    generate_brand_aliases,
    validate_model,
    validate_brand,
    filter_model_results,
    calculate_priority
)
from db_utils import get_endpoint
from image_utils import download_all_images
from excel_utils import write_excel_image
from email_utils import send_email, send_message_email
from file_utils import create_temp_dirs, cleanup_temp_dirs
from aws_s3 import upload_file_to_space
import httpx
import aiofiles
import requests
import json
import re
import base64
import zlib
import urllib.parse
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from urllib3.util.retry import Retry
from requests import Session
from icon_image_lib.google_parser import process_search_result

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

BRAND_RULES_URL = "https://raw.githubusercontent.com/iconluxurygroup/legacy-icon-product-api/refs/heads/main/task_settings/brand_settings.json"

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

def check_endpoint_health(endpoint: str, timeout: int = 5, logger: Optional[logging.Logger] = None) -> bool:
    """Check if an endpoint is healthy by querying its health check URL."""
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
    """Find and return a healthy endpoint from a list."""
    logger = logger or default_logger
    for endpoint in endpoints:
        if check_endpoint_health(endpoint, logger=logger):
            logger.info(f"Selected healthy endpoint: {endpoint}")
            return endpoint
    logger.error("No healthy endpoints found")
    return None

def process_search_row(
    search_string: str,
    endpoint: str,
    entry_id: int,
    logger: Optional[logging.Logger] = None,
    search_type: str = "default",
    max_retries: int = 15,
    brand: Optional[str] = None,
    category: Optional[str] = None
) -> pd.DataFrame:
    logger = logger or default_logger
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
                query = partial_search
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
                                irrelevant_keywords = ['wallpaper', 'furniture', 'decor', 'stock photo', 'pistol', 'mattress', 'trunk', 'clutch', 'solenoid', 'card', 'pokemon']
                                df = df[df['ImageDesc'].str.lower().str.contains('scotch|soda|sneaker|shoe|hoodie|shirt|jacket|pants|apparel|clothing', na=False) &
                                       ~df['ImageDesc'].str.lower().str.contains('|'.join(irrelevant_keywords), na=False)]
                                logger.info(f"Filtered out irrelevant results, kept {len(df)} rows for EntryID {entry_id}")
                                return df
                        logger.warning(f"No results for query: {query}, moving to next attempt or split")
                        break
                    except (RequestException, json.JSONDecodeError) as e:
                        logger.warning(f"Primary attempt {attempt_num} failed for {fetch_endpoint} with query '{query}': {e}")
                        logger.debug(f"Response content: {response.text[:1000]}")
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
            search_url = f"https://www.google.com/search?q={urllib.parse.quote(query)}&tbm=isch"
            attempt_num = 1
            while attempt_num <= retry_strategy.total + 1:
                if not log_retry_status("Primary", attempt_num):
                    return pd.DataFrame()
                try:
                    logger.info(f"Searching {search_url}")
                    response = session.get(search_url, timeout=60)
                    response.raise_for_status()
                    try:
                        result = response.json().get("body")
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON decode error for {search_url}: {e}")
                        logger.debug(f"Response content: {response.text[:1000]}")
                        raise ValueError("Invalid JSON response")
                    if not result:
                        logger.warning(f"No body in response for {search_url}")
                        raise ValueError("Empty response body")
                    unpacked_html = unpack_content(result, logger)
                    if not unpacked_html or len(unpacked_html) < 100:
                        logger.warning(f"Invalid HTML for {search_url}")
                        raise ValueError("Invalid HTML content")
                    df = process_search_result(unpacked_html, unpacked_html, entry_id, logger)
                    if not df.empty:
                        irrelevant_keywords = ['wallpaper', 'furniture', 'decor', 'stock photo', 'pistol', 'mattress', 'trunk', 'clutch', 'solenoid', 'card', 'pokemon']
                        df = df[df['ImageDesc'].str.lower().str.contains('scotch|soda|sneaker|shoe|hoodie|shirt|jacket|pants|apparel|clothing', na=False) &
                               ~df['ImageDesc'].str.lower().str.contains('|'.join(irrelevant_keywords), na=False)]
                        logger.info(f"Filtered out irrelevant results, kept {len(df)} rows for EntryID {entry_id}")
                        return df
                    logger.warning(f"No valid data for EntryID {entry_id}")
                    return df
                except (RequestException, json.JSONDecodeError, ValueError) as e:
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

def process_search_row_gcloud(search_string: str, entry_id: int, logger: Optional[logging.Logger] = None, remaining_retries: int = 5, total_attempts: Optional[List[int]] = None) -> pd.DataFrame:
    logger = logger or default_logger
    if not search_string or len(search_string.strip()) < 3:
        logger.warning(f"Invalid search string for EntryID {entry_id}: '{search_string}'")
        return pd.DataFrame()

    total_attempts = total_attempts or [0]
    base_url = "https://api.thedataproxy.com/v2/proxy/fetch"
    api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiMGRkZTIwZjAtNjlmZS00ODc2LWE0MmItMTY1YzM1YTk4MzMyIiwiaWF0IjoxNzQ3MDg5NzQ2LjgzMjU3OCwiZXhwIjoxNzc4NjI1NzQ2LjgzMjU4M30.pvPx3K8AIrV3gPnQqAC0BLGrlugWhLYLeYrgARkBG-g"
    regions = ['northamerica-northeast', 'southamerica', 'us-central', 'us-east', 'us-west', 'europe', 'australia','asia','middle-east']
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
                irrelevant_keywords = ['wallpaper', 'sofa', 'furniture', 'decor', 'stock photo', 'card', 'pokemon']
                df = df[~df['ImageDesc'].str.lower().str.contains('|'.join(irrelevant_keywords), na=False)]
                logger.info(f"Filtered out irrelevant results, kept {len(df)} rows for EntryID {entry_id}")
                return df
        except (RequestException, json.JSONDecodeError) as e:
            logger.warning(f"Attempt {attempt} failed for {fetch_endpoint} in region {region}: {e}")
            continue
    logger.error(f"All GCloud attempts failed for EntryID {entry_id} after {total_attempts[0]} total attempts")
    return pd.DataFrame()
from typing import Dict, List, Optional
import logging

def generate_search_variations(
    search_string: str,
    brand: Optional[str] = None,
    model: Optional[str] = None,
    brand_rules: Optional[Dict] = None,
    logger: Optional[logging.Logger] = None
) -> Dict[str, List[str]]:
    logger = logger or logging.getLogger(__name__)
    variations = {
        "default": [],
        "delimiter_variations": [],
        "color_delimiter": [],
        "brand_alias": [],
        "no_color": []
    }
    
    if not search_string:
        logger.warning("Empty search string provided")
        return variations
    
    search_string = search_string.lower()
    brand = clean_string(brand).lower() if brand else None
    model = clean_string(model).lower() if model else search_string
    
    variations["default"].append(search_string)
    
    delimiters = [' ', '-', '_', '/']
    delimiter_variations = []
    for delim in delimiters:
        if delim in search_string:
            delimiter_variations.append(search_string.replace(delim, ' '))
            delimiter_variations.append(search_string.replace(delim, '-'))
            delimiter_variations.append(search_string.replace(delim, '_'))
    variations["delimiter_variations"] = delimiter_variations
    
    variations["color_delimiter"].append(search_string)
    
    if brand:
        brand_aliases = generate_aliases(brand)
        variations["brand_alias"] = [f"{alias} {search_string}" for alias in brand_aliases]
    
    no_color_string = search_string
    if brand and brand_rules and "brand_rules" in brand_rules:
        for rule in brand_rules["brand_rules"]:
            if any(brand in name.lower() for name in rule.get("names", [])):
                sku_format = rule.get("sku_format", {})
                color_separator = sku_format.get("color_separator", "_")
                expected_length = rule.get("expected_length", {})
                base_length = expected_length.get("base", [6])[0]
                with_color_length = expected_length.get("with_color", [10])[0]
                
                if color_separator in search_string:
                    parts = search_string.split(color_separator)
                    base_part = parts[0]
                    if len(base_part) == base_length and len(search_string) <= with_color_length:
                        no_color_string = base_part
                        logger.debug(f"Brand rule applied for {brand}: Extracted no_color='{no_color_string}' from '{search_string}'")
                        break
                elif len(search_string) == base_length:
                    no_color_string = search_string
                    logger.debug(f"Brand rule applied for {brand}: No color suffix, no_color='{no_color_string}'")
                    break
    
    if no_color_string == search_string:
        for delim in ['_', '-', ' ']:
            if delim in search_string:
                no_color_string = search_string.rsplit(delim, 1)[0]
                logger.debug(f"Delimiter fallback: Extracted no_color='{no_color_string}' from '{search_string}' using delimiter '{delim}'")
                break
    
    variations["no_color"].append(no_color_string if no_color_string else search_string)
    if no_color_string != search_string:
        logger.info(f"Generated no_color variation: '{no_color_string}' from original '{search_string}'")
    else:
        logger.debug(f"No color suffix detected, no_color variation same as original: '{search_string}'")
    
    return variations

def search_variation(
    variation: str,
    endpoint: str,
    entry_id: int,
    search_type: str,
    brand: Optional[str] = None,
    category: Optional[str] = None,
    logger: Optional[logging.Logger] = None
) -> Dict:
    logger = logger or default_logger
    try:
        regions = ['northamerica-northeast', 'us-east', 'southamerica', 'us-central', 'us-west', 'europe', 'australia']
        max_attempts = 5
        total_attempts = [0]

        def log_retry_status(attempt_type: str, attempt_num: int) -> bool:
            total_attempts[0] += 1
            if total_attempts[0] > max_attempts:
                logger.error(f"Exceeded max retries ({max_attempts}) for EntryID {entry_id} after {attempt_type} attempt {attempt_num}")
                return False
            logger.info(f"{attempt_type} attempt {attempt_num} (Total attempts: {total_attempts[0]}/{max_attempts}) for EntryID {entry_id}")
            return True

        for region in regions:
            if not log_retry_status("GCloud", total_attempts[0] + 1):
                break
            result = process_search_row_gcloud(variation, entry_id, logger, remaining_retries=5, total_attempts=total_attempts)
            if not result.empty:
                logger.info(f"GCloud attempt succeeded for EntryID {entry_id} with {len(result)} images in region {region}")
                return {"variation": variation, "result": result, "status": "success", "result_count": len(result)}
            logger.warning(f"GCloud attempt failed in region {region}")

        for attempt in range(3):
            if not log_retry_status("Primary", attempt + 1):
                break
            result = process_search_row(variation, endpoint, entry_id, logger, search_type, max_retries=15, brand=brand, category=category)
            if not result.empty:
                logger.info(f"Primary attempt succeeded for EntryID {entry_id} with {len(result)} images")
                return {"variation": variation, "result": result, "status": "success", "result_count": len(result)}
            logger.warning(f"Primary attempt {attempt + 1} failed")

        logger.error(f"All attempts failed for EntryID {entry_id} after {total_attempts[0]} total attempts")
        return {
            "variation": variation,
            "result": pd.DataFrame([{
                "EntryID": entry_id,
                "ImageUrl": "placeholder://no-results",
                "ImageDesc": f"No results found for {variation}",
                "ImageSource": "N/A",
                "ImageUrlThumbnail": "placeholder://no-results"
            }]),
            "status": "failed",
            "result_count": 1,
            "error": "All search attempts failed"
        }
    except Exception as e:
        logger.error(f"Error searching variation '{variation}' for EntryID {entry_id}: {e}", exc_info=True)
        return {
            "variation": variation,
            "result": pd.DataFrame([{
                "EntryID": entry_id,
                "ImageUrl": "placeholder://no-results",
                "ImageDesc": f"Error for {variation}: {str(e)}",
                "ImageSource": "N/A",
                "ImageUrlThumbnail": "placeholder://no-results"
            }]),
            "status": "failed",
            "result_count": 1,
            "error": str(e)
        }

def process_single_all(
    entry_id: int,
    search_string: str,
    max_row_retries: int,
    file_id_db: int,
    brand_rules: dict,
    endpoint: str,
    brand: Optional[str] = None,
    model: Optional[str] = None,
    color: Optional[str] = None,
    category: Optional[str] = None,
    logger: Optional[logging.Logger] = None
) -> bool:
    from db_utils import insert_search_results, sync_get_endpoint, sync_update_search_sort_order
    logger = logger or default_logger
    
    try:
        entry_id = int(entry_id)
        file_id_db = int(file_id_db)
        if not search_string or not isinstance(search_string, str):
            logger.error(f"Invalid search string for EntryID {entry_id}")
            return False
        model = model or search_string
    except (ValueError, TypeError) as e:
        logger.error(f"Invalid input parameters for EntryID {entry_id}: {e}", exc_info=True)
        return False

    search_types = [
        "default", "delimiter_variations", "color_delimiter",
        "brand_alias", "brand_name", "no_color"
    ]
    all_results = []
    result_brand = brand
    result_model = model
    result_color = color
    result_category = category

    api_to_db_mapping = {
        'image_url': 'ImageUrl', 'thumbnail_url': 'ImageUrlThumbnail', 'url': 'ImageUrl',
        'thumb': 'ImageUrlThumbnail', 'image': 'ImageUrl', 'thumbnail': 'ImageUrlThumbnail',
        'img_url': 'ImageUrl', 'thumb_url': 'ImageUrlThumbnail', 'imageURL': 'ImageUrl',
        'imageUrl': 'ImageUrl', 'thumbnailURL': 'ImageUrlThumbnail', 'thumbnailUrl': 'ImageUrlThumbnail',
        'brand': 'Brand', 'model': 'Model', 'brand_name': 'Brand', 'product_model': 'Model'
    }
    required_columns = ["EntryID", "ImageUrl", "ImageDesc", "ImageSource", "ImageUrlThumbnail"]

    if not brand or not model or not color or not category:
        try:
            with pyodbc.connect(conn_str, autocommit=False) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT ProductBrand, ProductModel, ProductColor, ProductCategory FROM utb_ImageScraperRecords WHERE FileID = ? AND EntryID = ?",
                    (file_id_db, entry_id)
                )
                result = cursor.fetchone()
                if result:
                    result_brand = result_brand or result[0]
                    result_model = result_model or result[1]
                    result_color = result_color or result[2]
                    result_category = result_category or result[3]
                    logger.info(f"Fetched attributes for EntryID {entry_id}: Brand={result_brand}, Model={result_model}, Color={result_color}, Category={result_category}")
                else:
                    logger.warning(f"No attributes found for FileID {file_id_db}, EntryID {entry_id}")
        except pyodbc.Error as e:
            logger.error(f"Failed to fetch attributes for EntryID {entry_id}: {e}", exc_info=True)

    variations = generate_search_variations(search_string, result_brand, result_model, brand_rules, logger)
    
    # Use synchronous endpoint retrieval
    endpoint = sync_get_endpoint(logger=logger)
    if not endpoint:
        logger.error(f"No healthy endpoint available for EntryID {entry_id}")
        return False

    for search_type in search_types:
        if search_type not in variations:
            logger.warning(f"Search type '{search_type}' not found in variations for EntryID {entry_id}")
            continue
        logger.info(f"Processing search type '{search_type}' for EntryID {entry_id} with variations: {variations[search_type]}")
        futures = [search_variation.remote(variation, endpoint, entry_id, search_type, result_brand, result_category, logger)
                   for variation in variations[search_type]]
        results = ray.get(futures)
        successful_results = [res for res in results if res["status"] == "success" and not res["result"].empty]
        if successful_results:
            result_count = sum(len(res["result"]) for res in successful_results)
            all_results.extend([res["result"] for res in successful_results])
            logger.info(f"Found {len(successful_results)} successful variations with {result_count} total results for search type '{search_type}' for EntryID {entry_id}")
        else:
            logger.info(f"No successful results for search type '{search_type}' for EntryID {entry_id}")

    if all_results:
        try:
            combined_df = pd.concat(all_results, ignore_index=True)
            logger.info(f"Combined {len(combined_df)} results from all search types for EntryID {entry_id}")
            for api_col, db_col in api_to_db_mapping.items():
                if api_col in combined_df.columns and db_col not in combined_df.columns:
                    combined_df.rename(columns={api_col: db_col}, inplace=True)
            if not all(col in combined_df.columns for col in required_columns):
                logger.error(f"Missing columns {set(required_columns) - set(combined_df.columns)} in result for EntryID {entry_id}")
                return False
            deduplicated_df = combined_df.drop_duplicates(subset=['EntryID', 'ImageUrl'], keep='first')
            logger.info(f"Deduplicated to {len(deduplicated_df)} rows for EntryID {entry_id}")
            insert_success = insert_search_results(deduplicated_df, logger=logger)
            if not insert_success:
                logger.error(f"Failed to insert deduplicated results for EntryID {entry_id}")
                return False
            logger.info(f"Inserted {len(deduplicated_df)} results for EntryID {entry_id}")
            
            # Use synchronous sort order update
            update_result = sync_update_search_sort_order(
                str(file_id_db), str(entry_id), result_brand, result_model, result_color, result_category, logger, brand_rules=brand_rules
            )
            if update_result is None:
                logger.error(f"SortOrder update failed for EntryID {entry_id}")
                return False
            logger.info(f"Updated sort order for EntryID {entry_id} with Brand: {result_brand}, Model: {result_model}, Color: {result_color}, Category: {result_category}")
            try:
                with pyodbc.connect(conn_str, autocommit=False) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT COUNT(*) FROM utb_ImageScraperResult WHERE EntryID = ?", (entry_id,))
                    total_count = cursor.fetchone()[0]
                    logger.info(f"Verification: Found {total_count} total rows for EntryID {entry_id}")
                    cursor.execute(
                        "SELECT COUNT(*) FROM utb_ImageScraperResult WHERE EntryID = ? AND SortOrder > 0",
                        (entry_id,)
                    )
                    count = cursor.fetchone()[0]
                    cursor.execute(
                        "SELECT COUNT(*) FROM utb_ImageScraperResult WHERE EntryID = ? AND SortOrder IS NULL",
                        (entry_id,)
                    )
                    null_count = cursor.fetchone()[0]
                    logger.info(f"Verification: Found {count} rows with positive SortOrder, {null_count} rows with NULL SortOrder for EntryID {entry_id}")
                    if null_count > 0:
                        logger.error(f"Found {null_count} rows with NULL SortOrder after update for EntryID {entry_id}")
                    if total_count == 0:
                        logger.error(f"No rows found in utb_ImageScraperResult for EntryID {entry_id} after insertion")
            except pyodbc.Error as e:
                logger.error(f"Failed to verify SortOrder for EntryID {entry_id}: {e}", exc_info=True)
            return True
        except Exception as e:
            logger.error(f"Error during batch database update for EntryID {entry_id}: {e}", exc_info=True)
            return False

    logger.info(f"No results to insert for EntryID {entry_id}")
    placeholder_df = pd.DataFrame([{
        "EntryID": entry_id,
        "ImageUrl": "placeholder://no-results",
        "ImageDesc": f"No results found for {search_string}",
        "ImageSource": "N/A",
        "ImageUrlThumbnail": "placeholder://no-results"
    }])
    try:
        insert_success = insert_search_results(placeholder_df, logger=logger)
        if insert_success:
            logger.info(f"Inserted placeholder row for EntryID {entry_id}")
            with pyodbc.connect(conn_str, autocommit=False) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE utb_ImageScraperResult
                    SET SortOrder = 1
                    WHERE EntryID = ? AND ImageUrl = ?
                    """,
                    (entry_id, "placeholder://no-results")
                )
                conn.commit()
                logger.info(f"Fallback applied: Set SortOrder=1 for placeholder row for EntryID {entry_id}")
            return True
        else:
            logger.error(f"Failed to insert placeholder row for EntryID {entry_id}")
            return False
    except pyodbc.Error as e:
        logger.error(f"Failed to insert placeholder row for EntryID {entry_id}: {e}", exc_info=True)
        return False

def process_single_row(
    entry_id: int,
    search_string: str,
    max_row_retries: int,
    file_id_db: int,
    brand_rules: dict,
    endpoint: str,
    brand: Optional[str] = None,
    model: Optional[str] = None,
    color: Optional[str] = None,
    category: Optional[str] = None,
    logger: Optional[logging.Logger] = None
) -> bool:
    from db_utils import insert_search_results, sync_get_endpoint, sync_update_search_sort_order
    import pandas as pd
    logger = logger or default_logger
    
    try:
        entry_id = int(entry_id)
        file_id_db = int(file_id_db)
        if not search_string or not isinstance(search_string, str):
            logger.error(f"Invalid search string for EntryID {entry_id}")
            return False
        model = model or search_string
    except (ValueError, TypeError) as e:
        logger.error(f"Invalid input parameters for EntryID {entry_id}: {e}", exc_info=True)
        return False

    search_types = [
        "default", "delimiter_variations", "color_delimiter",
        "brand_alias", "brand_name", "no_color"
    ]
    all_results = []
    result_brand = brand
    result_model = model
    result_color = color
    result_category = category

    api_to_db_mapping = {
        'image_url': 'ImageUrl', 'thumbnail_url': 'ImageUrlThumbnail', 'url': 'ImageUrl',
        'thumb': 'ImageUrlThumbnail', 'image': 'ImageUrl', 'thumbnail': 'ImageUrlThumbnail',
        'img_url': 'ImageUrl', 'thumb_url': 'ImageUrlThumbnail', 'imageURL': 'ImageUrl',
        'imageUrl': 'ImageUrl', 'thumbnailURL': 'ImageUrlThumbnail', 'thumbnailUrl': 'ImageUrlThumbnail',
        'brand': 'Brand', 'model': 'Model', 'brand_name': 'Brand', 'product_model': 'Model'
    }
    required_columns = ["EntryID", "ImageUrl", "ImageDesc", "ImageSource", "ImageUrlThumbnail"]

    if not brand or not model or not color or not category:
        try:
            with pyodbc.connect(conn_str, autocommit=False) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT ProductBrand, ProductModel, ProductColor, ProductCategory FROM utb_ImageScraperRecords WHERE FileID = ? AND EntryID = ?",
                    (file_id_db, entry_id)
                )
                result = cursor.fetchone()
                if result:
                    result_brand = result_brand or result[0]
                    result_model = result_model or result[1]
                    result_color = result_color or result[2]
                    result_category = result_category or result[3]
                    logger.info(f"Fetched attributes for EntryID {entry_id}: Brand={result_brand}, Model={result_model}, Color={result_color}, Category={result_category}")
                else:
                    logger.warning(f"No attributes found for FileID {file_id_db}, EntryID {entry_id}")
        except pyodbc.Error as e:
            logger.error(f"Failed to fetch attributes for EntryID {entry_id}: {e}", exc_info=True)

    variations = generate_search_variations(search_string, result_brand, result_model, brand_rules, logger)
    
    # Use synchronous endpoint retrieval
    endpoint = sync_get_endpoint(logger=logger)
    if not endpoint:
        logger.error(f"No healthy endpoint available for EntryID {entry_id}")
        return False

    for search_type in search_types:
        if search_type not in variations:
            logger.warning(f"Search type '{search_type}' not found in variations for EntryID {entry_id}")
            continue
        logger.info(f"Processing search type '{search_type}' for EntryID {entry_id} with variations: {variations[search_type]}")
        futures = [search_variation.remote(variation, endpoint, entry_id, search_type, result_brand, result_category, logger)
                   for variation in variations[search_type]]
        results = ray.get(futures)
        successful_results = [res for res in results if res["status"] == "success" and not res["result"].empty]
        if successful_results:
            all_results.extend([res["result"] for res in successful_results])
            break

    if all_results:
        try:
            combined_df = pd.concat(all_results, ignore_index=True)
            logger.info(f"Combined {len(combined_df)} results for EntryID {entry_id} for batch insertion")
            for api_col, db_col in api_to_db_mapping.items():
                if api_col in combined_df.columns and db_col not in combined_df.columns:
                    combined_df.rename(columns={api_col: db_col}, inplace=True)
            if not all(col in combined_df.columns for col in required_columns):
                logger.error(f"Missing columns {set(required_columns) - set(combined_df.columns)} in result for EntryID {entry_id}")
                return False
            deduplicated_df = combined_df.drop_duplicates(subset=['EntryID', 'ImageUrl'], keep='first')
            logger.info(f"Deduplicated to {len(deduplicated_df)} rows")
            insert_success = insert_search_results(deduplicated_df, logger=logger)
            if not insert_success:
                logger.error(f"Failed to insert deduplicated results for EntryID {entry_id}")
                return False
            logger.info(f"Inserted {len(deduplicated_df)} results for EntryID {entry_id}")
            
            # Use synchronous sort order update
            update_result = sync_update_search_sort_order(
                str(file_id_db), str(entry_id), result_brand, result_model, result_color, result_category, logger, brand_rules=brand_rules
            )
            if update_result is None:
                logger.error(f"SortOrder update failed for EntryID {entry_id}")
                return False
            logger.info(f"Updated sort order for EntryID {entry_id} with Brand: {result_brand}, Model: {result_model}, Color: {result_color}, Category: {result_category}")
            try:
                with pyodbc.connect(conn_str, autocommit=False) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT COUNT(*) FROM utb_ImageScraperResult WHERE EntryID = ?", (entry_id,))
                    total_count = cursor.fetchone()[0]
                    logger.info(f"Verification: Found {total_count} total rows for EntryID {entry_id}")
                    cursor.execute(
                        "SELECT COUNT(*) FROM utb_ImageScraperResult WHERE EntryID = ? AND SortOrder > 0",
                        (entry_id,)
                    )
                    count = cursor.fetchone()[0]
                    cursor.execute(
                        "SELECT COUNT(*) FROM utb_ImageScraperResult WHERE EntryID = ? AND SortOrder IS NULL",
                        (entry_id,)
                    )
                    null_count = cursor.fetchone()[0]
                    logger.info(f"Verification: Found {count} rows with positive SortOrder, {null_count} rows with NULL SortOrder for EntryID {entry_id}")
                    if null_count > 0:
                        logger.error(f"Found {null_count} rows with NULL SortOrder after update for EntryID {entry_id}")
                    if total_count == 0:
                        logger.error(f"No rows found in utb_ImageScraperResult for EntryID {entry_id} after insertion")
            except pyodbc.Error as e:
                logger.error(f"Failed to verify SortOrder for EntryID {entry_id}: {e}", exc_info=True)
            return True
        except Exception as e:
            logger.error(f"Error during batch database update for EntryID {entry_id}: {e}", exc_info=True)
            return False

    logger.info(f"No results to insert for EntryID {entry_id}")
    placeholder_df = pd.DataFrame([{
        "EntryID": entry_id,
        "ImageUrl": "placeholder://no-results",
        "ImageDesc": f"No results found for {search_string}",
        "ImageSource": "N/A",
        "ImageUrlThumbnail": "placeholder://no-results"
    }])
    try:
        insert_success = insert_search_results(placeholder_df, logger=logger)
        if insert_success:
            logger.info(f"Inserted placeholder row for EntryID {entry_id}")
            with pyodbc.connect(conn_str, autocommit=False) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE utb_ImageScraperResult
                    SET SortOrder = 1
                    WHERE EntryID = ? AND ImageUrl = ?
                    """,
                    (entry_id, "placeholder://no-results")
                )
                conn.commit()
                logger.info(f"Fallback applied: Set SortOrder=1 for placeholder row for EntryID {entry_id}")
            return True
        else:
            logger.error(f"Failed to insert placeholder row for EntryID {entry_id}")
            return False
    except pyodbc.Error as e:
        logger.error(f"Failed to insert placeholder row for EntryID {entry_id}: {e}", exc_info=True)
        return False
async def process_and_tag_results(search_string, brand, model, endpoint, entry_id, logger, use_all_variations: bool = False, file_id_db: int = None):
    logger = logger or logging.getLogger("default")
    try:
        logger.debug(f"Starting process_and_tag_results for EntryID {entry_id}")
        brand_rules = await fetch_brand_rules(BRAND_RULES_URL, max_attempts=3, timeout=10, logger=logger)
        if not brand_rules:
            logger.warning(f"No brand rules fetched for EntryID {entry_id}")
            brand_rules = {"brand_rules": []}

        if file_id_db is None:
            logger.error(f"FileID not provided for EntryID {entry_id}")
            return [pd.DataFrame([{
                "EntryID": entry_id,
                "ImageUrl": "placeholder://error",
                "ImageDesc": "Error: FileID not provided",
                "ImageSource": "N/A",
                "ImageUrlThumbnail": "placeholder://error",
                "search_type": "default",
                "priority": 4
            }])]

        max_row_retries = 3
        process_func = process_single_all if use_all_variations else process_single_row
        logger.debug(f"Calling process_func for EntryID {entry_id}")
        success = process_func(
            entry_id=entry_id,
            search_string=search_string,
            max_row_retries=max_row_retries,
            file_id_db=file_id_db,
            brand_rules=brand_rules,
            endpoint=endpoint,
            brand=brand,
            model=model,
            logger=logger
        )
        logger.debug(f"Process func result for EntryID {entry_id}: {success}")

        if not success:
            logger.error(f"Processing failed for EntryID {entry_id}")
            return [pd.DataFrame([{
                "EntryID": entry_id,
                "ImageUrl": "placeholder://error",
                "ImageDesc": "Error: Processing failed",
                "ImageSource": "N/A",
                "ImageUrlThumbnail": "placeholder://error",
                "search_type": "default",
                "priority": 4
            }])]

        try:
            with pyodbc.connect(conn_str, autocommit=False, timeout=30) as conn:
                query = """
                    SELECT 
                        r.EntryID, 
                        r.ImageUrl, 
                        r.ImageDesc, 
                        r.ImageSource, 
                        r.ImageUrlThumbnail, 
                        r.ResultID, 
                        rec.ProductModel, 
                        rec.ProductBrand
                    FROM utb_ImageScraperResult r
                    INNER JOIN utb_ImageScraperRecords rec
                        ON r.EntryID = rec.EntryID
                    WHERE r.EntryID = ?
                """
                logger.debug(f"Executing database query for EntryID {entry_id}")
                df = pd.read_sql(query, conn, params=(entry_id,), timeout=60)
                logger.debug(f"Retrieved {len(df)} rows for EntryID {entry_id}")
        except pyodbc.Error as e:
            logger.error(f"Failed to retrieve results for EntryID {entry_id}: {e}", exc_info=True)
            return [pd.DataFrame([{
                "EntryID": entry_id,
                "ImageUrl": "placeholder://error",
                "ImageDesc": f"Error: Database retrieval failed: {str(e)}",
                "ImageSource": "N/A",
                "ImageUrlThumbnail": "placeholder://error",
                "search_type": "default",
                "priority": 4
            }])]

        all_dfs = []
        if not df.empty:
            df['search_type'] = 'default'
            df['ImageDesc_clean'] = df['ImageDesc'].apply(clean_string, preserve_url=False)
            df['ImageSource_clean'] = df['ImageSource'].apply(clean_string, preserve_url=True)
            df['ImageUrl_clean'] = df['ImageUrl'].apply(clean_string, preserve_url=True)
            df['ProductBrand_clean'] = df.get('ProductBrand', '').apply(clean_string, preserve_url=False)

            brand_aliases = []
            for rule in brand_rules["brand_rules"]:
                if any(brand.lower() in name.lower() for name in rule.get("names", [])):
                    brand_aliases = rule.get("names", [])
                    break
            exact_df, _ = await filter_model_results(
                df,
                debug=False,
                logger=logger,
                brand_aliases=brand_aliases
            )

            def calculate_priority_vectorized(df, exact_df, model_clean, model_aliases, brand_clean, brand_aliases):
                model_matched = df.index.isin(exact_df.index)
                brand_matched = df['ImageDesc_clean'].str.contains('|'.join(brand_aliases), case=False, na=False) | \
                                df['ImageSource_clean'].str.contains('|'.join(brand_aliases), case=False, na=False) | \
                                df['ImageUrl_clean'].str.contains('|'.join(brand_aliases), case=False, na=False)
                priority = pd.Series(4, index=df.index)
                priority[model_matched & brand_matched] = 1
                priority[model_matched & ~brand_matched] = 2
                priority[~model_matched & brand_matched] = 3
                return priority

            model_clean = normalize_model(model or search_string)
            model_aliases = generate_aliases(model_clean)
            brand_clean = clean_string(brand).lower() if brand else ''
            brand_aliases = await generate_brand_aliases(brand_clean, {}) if brand else []
            df['priority'] = calculate_priority_vectorized(
                df, exact_df, model_clean, model_aliases, brand_clean, brand_aliases
            )

            all_dfs.append(df)
        else:
            placeholder_df = pd.DataFrame([{
                "EntryID": entry_id,
                "ImageUrl": "placeholder://no-results",
                "ImageDesc": f"No results found for {search_string}",
                "ImageSource": "N/A",
                "ImageUrlThumbnail": "placeholder://no-results",
                "search_type": "default",
                "priority": 4
            }])
            all_dfs.append(placeholder_df)

        return all_dfs

    except Exception as e:
        logger.error(f"Unexpected error in process_and_tag_results for EntryID {entry_id}: {e}", exc_info=True)
        return [pd.DataFrame([{
            "EntryID": entry_id,
            "ImageUrl": "placeholder://error",
            "ImageDesc": f"Error: {str(e)}",
            "ImageSource": "N/A",
            "ImageUrlThumbnail": "placeholder://error",
            "search_type": "default",
            "priority": 4
        }])]

def sync_process_and_tag_results(*args, **kwargs):
    """Synchronous wrapper for process_and_tag_results."""
    return asyncio.run(process_and_tag_results(*args, **kwargs))