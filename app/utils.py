import logging
import asyncio
import os
import json
import re
import base64
import zlib
import urllib.parse
from typing import List, Dict, Optional, Tuple, Any
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
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
from database_config import async_engine
from search_utils import update_search_sort_order, insert_search_results
from endpoint_utils import sync_get_endpoint
from image_utils import download_all_images
from excel_utils import write_excel_image
from email_utils import send_message_email
from ai_utils import batch_vision_reason
import httpx
import aiofiles
import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from requests.exceptions import RequestException
from icon_image_lib.google_parser import process_search_result
import psutil

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

BRAND_RULES_URL = "https://raw.githubusercontent.com/iconluxurygroup/legacy-icon-product-api/refs/heads/main/task_settings/brand_settings.json"

async def create_temp_dirs(file_id: int, logger: Optional[logging.Logger] = None) -> Tuple[str, str]:
    logger = logger or default_logger
    temp_images_dir = f"temp_images_{file_id}"
    temp_excel_dir = f"temp_excel_{file_id}"
    os.makedirs(temp_images_dir, exist_ok=True)
    os.makedirs(temp_excel_dir, exist_ok=True)
    logger.debug(f"Created temp directories: {temp_images_dir}, {temp_excel_dir}")
    return temp_images_dir, temp_excel_dir

async def cleanup_temp_dirs(dirs: List[str], logger: Optional[logging.Logger] = None):
    logger = logger or default_logger
    for dir_path in dirs:
        if os.path.exists(dir_path):
            try:
                for root, _, files in os.walk(dir_path, topdown=False):
                    for file in files:
                        os.remove(os.path.join(root, file))
                    os.rmdir(root)
                logger.debug(f"Cleaned up directory: {dir_path}")
            except Exception as e:
                logger.warning(f"Failed to clean up directory {dir_path}: {e}")

def unpack_content(encoded_content: str, logger: Optional[logging.Logger] = None) -> Optional[bytes]:
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
    logger = logger or default_logger
    health_url = f"{endpoint}/health/google"
    try:
        response = httpx.get(health_url, timeout=timeout)
        response.raise_for_status()
        status = response.json().get("status", "")
        logger.debug(f"Health check for {endpoint}: Status={status}, Headers={response.headers}")
        return "Google is reachable" in status
    except httpx.RequestException as e:
        logger.warning(f"Endpoint {endpoint} health check failed: {e}, Headers={getattr(e.response, 'headers', 'N/A')}")
        return False

def get_healthy_endpoint(endpoints: List[str], logger: Optional[logging.Logger] = None) -> Optional[str]:
    logger = logger or default_logger
    for endpoint in endpoints:
        if check_endpoint_health(endpoint, logger=logger):
            logger.info(f"Selected healthy endpoint: {endpoint}")
            return endpoint
    logger.error("No healthy endpoints found")
    return None

async def process_search_row_gcloud(
    search_string: str,
    entry_id: int,
    logger: Optional[logging.Logger] = None,
    remaining_retries: int = 5,
    total_attempts: Optional[List[int]] = None
) -> List[Dict]:
    logger = logger or default_logger
    if not search_string or len(search_string.strip()) < 3:
        logger.warning(f"Invalid search string for EntryID {entry_id}: '{search_string}'")
        return []

    total_attempts = total_attempts or [0]
    base_url = "https://api.thedataproxy.com/v2/proxy/fetch"
    api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiMGRkZTIwZjAtNjlmZS00ODc2LWE0MmItMTY1YzM1YTk4MzMyIiwiaWF0IjoxNzQ3MDg5NzQ2LjgzMjU3OCwiZXhwIjoxNzc4NjI1NzQ2LjgzMjU4M30.pvPx3K8AIrV3gPnQqAC0BLGrlugWhLYLeYrgARkBG-g"
    regions = ['northamerica-northeast', 'us-east', 'southamerica', 'us-central', 'us-west', 'europe', 'australia', 'asia', 'middle-east']
    headers = {
        "accept": "application/json",
        "x-api-key": api_key,
        "Content-Type": "application/json"
    }

    process = psutil.Process()
    async def log_gcloud_retry(attempt: int, region: str) -> bool:
        total_attempts[0] += 1
        if total_attempts[0] > remaining_retries:
            logger.error(f"Worker PID {process.pid}: Exceeded remaining retries ({remaining_retries}) for EntryID {entry_id} at GCloud attempt {attempt}")
            return False
        logger.info(f"Worker PID {process.pid}: GCloud attempt {attempt} (Total attempts: {total_attempts[0]}/{remaining_retries}) for EntryID {entry_id} in region {region}")
        return True

    search_url = f"https://www.google.com/search?q={urllib.parse.quote(search_string)}&tbm=isch"
    async with aiohttp.ClientSession(headers=headers) as session:
        for attempt, region in enumerate(regions, 1):
            if not await log_gcloud_retry(attempt, region):
                break
            fetch_endpoint = f"{base_url}?region={region}"
            mem_info = process.memory_info()
            logger.debug(f"Worker PID {process.pid}: Memory before API call: RSS={mem_info.rss / 1024**2:.2f} MB")
            try:
                logger.info(f"Worker PID {process.pid}: Attempt {attempt}: Fetching {search_url} via {fetch_endpoint} with region {region}")
                async with session.post(fetch_endpoint, json={"url": search_url}, timeout=60) as response:
                    body_text = await response.text()
                    body_preview = body_text[:200] if body_text else ""
                    logger.debug(f"Worker PID {process.pid}: GCloud response: status={response.status}, headers={response.headers}, body={body_preview}")
                    response.raise_for_status()
                    result = await response.json()
                    result_data = result.get("result")
                    if not result_data:
                        logger.warning(f"Worker PID {process.pid}: No result returned for EntryID {entry_id} in region {region}")
                        continue
                    results_html_bytes = result_data if isinstance(result_data, bytes) else result_data.encode("utf-8")
                    results = process_search_result(results_html_bytes, results_html_bytes, entry_id, logger)
                    mem_info = process.memory_info()
                    logger.debug(f"Worker PID {process.pid}: Memory after API call: RSS={mem_info.rss / 1024**2:.2f} MB")
                    if results:
                        irrelevant_keywords = ['wallpaper', 'sofa', 'furniture', 'decor', 'stock photo', 'card', 'pokemon']
                        filtered_results = [
                            res for res in results
                            if not any(kw.lower() in res.get('ImageDesc', '').lower() for kw in irrelevant_keywords)
                        ]
                        logger.info(f"Worker PID {process.pid}: Filtered out irrelevant results, kept {len(filtered_results)} rows for EntryID {entry_id}")
                        return filtered_results
                    logger.warning(f"Worker PID {process.pid}: Empty results for EntryID {entry_id} in region {region}")
            except (aiohttp.ClientError, json.JSONDecodeError) as e:
                logger.warning(f"Worker PID {process.pid}: Attempt {attempt} failed for {fetch_endpoint} in region {region}: {e}")
                continue
    logger.error(f"Worker PID {process.pid}: All GCloud attempts failed for EntryID {entry_id} after {total_attempts[0]} total attempts")
    return []

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((aiohttp.ClientError, httpx.HTTPStatusError, TimeoutError, json.JSONDecodeError)),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Worker PID {psutil.Process().pid}: Retrying process_search_row for EntryID {retry_state.kwargs['entry_id']} (attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def process_search_row(
    search_string: str,
    endpoint: str,
    entry_id: int,
    logger: Optional[logging.Logger] = None,
    search_type: str = "default",
    max_retries: int = 15,
    brand: Optional[str] = None,
    category: Optional[str] = None
) -> List[Dict]:
    logger = logger or default_logger
    process = psutil.Process()
    if not search_string or not endpoint:
        logger.warning(f"Worker PID {process.pid}: Invalid input for EntryID {entry_id}: search_string={search_string}, endpoint={endpoint}")
        return []

    total_attempts = [0]
    
    async def log_retry_status(attempt_type: str, attempt_num: int) -> bool:
        total_attempts[0] += 1
        if total_attempts[0] > max_retries:
            logger.error(f"Worker PID {process.pid}: Exceeded max retries ({max_retries}) for EntryID {entry_id} after {attempt_type} attempt {attempt_num}")
            return False
        logger.info(f"Worker PID {process.pid}: {attempt_type} attempt {attempt_num} (Total attempts: {total_attempts[0]}/{max_retries}) for EntryID {entry_id}")
        return True

    query = search_string
    if brand:
        query += f" {brand}"
    if category:
        query += f" {category}"
    search_url = f"https://www.google.com/search?q={urllib.parse.quote(query)}&tbm=isch"
    fetch_endpoint = f"{endpoint}/fetch"

    attempt_num = 1
    while attempt_num <= 3:
        if not await log_retry_status("Primary", attempt_num):
            break
        mem_info = process.memory_info()
        logger.debug(f"Worker PID {process.pid}: Memory before API call: RSS={mem_info.rss / 1024**2:.2f} MB")
        try:
            logger.info(f"Worker PID {process.pid}: Fetching {search_url} via {fetch_endpoint}")
            async with aiohttp.ClientSession() as session:
                async with session.post(fetch_endpoint, json={"url": search_url}, timeout=60) as response:
                    logger.debug(f"Worker PID {process.pid}: Endpoint response: status={response.status}, headers={response.headers}, body={await response.text()[:200]}")
                    if response.status in (429, 503):
                        logger.warning(f"Worker PID {process.pid}: Rate limit or service unavailable (status {response.status}) for {fetch_endpoint}")
                        raise aiohttp.ClientError(f"Rate limit or service unavailable: {response.status}")
                    response.raise_for_status()
                    try:
                        result = await response.json()
                        result_data = result.get("result")
                    except json.JSONDecodeError as e:
                        logger.error(f"Worker PID {process.pid}: JSON decode error for {search_url}: {e}")
                        raise
                    if not result_data:
                        logger.warning(f"Worker PID {process.pid}: No result data in response for {search_url}")
                        raise ValueError("Empty response result")
                    unpacked_html = unpack_content(result_data, logger)
                    if not unpacked_html or len(unpacked_html) < 100:
                        logger.warning(f"Worker PID {process.pid}: Invalid HTML for {search_url}")
                        raise ValueError("Invalid HTML content")
                    results = process_search_result(unpacked_html, unpacked_html, entry_id, logger)
                    mem_info = process.memory_info()
                    logger.debug(f"Worker PID {process.pid}: Memory after API call: RSS={mem_info.rss / 1024**2:.2f} MB")
                    if results:
                        irrelevant_keywords = ['wallpaper', 'furniture', 'decor', 'stock photo', 'pistol', 'mattress', 'trunk', 'clutch', 'solenoid', 'card', 'pokemon']
                        filtered_results = [
                            res for res in results
                            if any(kw.lower() in res.get('ImageDesc', '').lower() for kw in ['scotch', 'soda', 'sneaker', 'shoe', 'hoodie', 'shirt', 'jacket', 'pants', 'apparel', 'clothing'])
                            and not any(kw.lower() in res.get('ImageDesc', '').lower() for kw in irrelevant_keywords)
                        ]
                        logger.info(f"Worker PID {process.pid}: Filtered out irrelevant results, kept {len(filtered_results)} rows for EntryID {entry_id}")
                        return filtered_results
                    logger.warning(f"Worker PID {process.pid}: No valid data for EntryID {entry_id}")
                    return []
        except (aiohttp.ClientError, json.JSONDecodeError, ValueError) as e:
            logger.warning(f"Worker PID {process.pid}: Primary attempt {attempt_num} failed for {fetch_endpoint}: {e}")
            attempt_num += 1
            if attempt_num > 3:
                break

    if total_attempts[0] < max_retries:
        gcloud_results = await process_search_row_gcloud(search_string, entry_id, logger, max_retries - total_attempts[0], total_attempts)
        if gcloud_results:
            logger.info(f"Worker PID {process.pid}: GCloud fallback succeeded for EntryID {entry_id} with {len(gcloud_results)} images")
            return gcloud_results
        logger.error(f"Worker PID {process.pid}: GCloud fallback also failed for EntryID {entry_id} after {total_attempts[0]} total attempts")
    
    return []

def generate_search_variations(
    search_string: str,
    brand: Optional[str] = None,
    model: Optional[str] = None,
    brand_rules: Optional[Dict] = None,
    logger: Optional[logging.Logger] = None
) -> Dict[str, List[str]]:
    logger = logger or default_logger
    process = psutil.Process()
    variations = {
        "default": [],
        "delimiter_variations": [],
        "color_delimiter": [],
        "brand_alias": [],
        "no_color": []
    }
    
    if not search_string:
        logger.warning(f"Worker PID {process.pid}: Empty search string provided")
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
    variations["delimiter_variations"] = list(set(delimiter_variations))
    
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
                
                if not color_separator:
                    logger.warning(f"Worker PID {process.pid}: Empty color_separator for brand {brand}, skipping color split")
                    no_color_string = search_string
                    logger.debug(f"Worker PID {process.pid}: Brand rule applied for {brand}: No color split, no_color='{no_color_string}'")
                    break
                
                if color_separator in search_string:
                    logger.debug(f"Worker PID {process.pid}: Applying color_separator '{color_separator}' to search_string '{search_string}'")
                    parts = search_string.split(color_separator)
                    base_part = parts[0]
                    if len(base_part) == base_length and len(search_string) <= with_color_length:
                        no_color_string = base_part
                        logger.debug(f"Worker PID {process.pid}: Brand rule applied for {brand}: Extracted no_color='{no_color_string}' from '{search_string}'")
                        break
                elif len(search_string) == base_length:
                    no_color_string = search_string
                    logger.debug(f"Worker PID {process.pid}: Brand rule applied for {brand}: No color suffix, no_color='{no_color_string}'")
                    break
    
    if no_color_string == search_string:
        for delim in ['_', '-', ' ']:
            if delim in search_string:
                no_color_string = search_string.rsplit(delim, 1)[0]
                logger.debug(f"Worker PID {process.pid}: Delimiter fallback: Extracted no_color='{no_color_string}' from '{search_string}' using delimiter '{delim}'")
                break
    
    variations["no_color"].append(no_color_string if no_color_string else search_string)
    if no_color_string != search_string:
        logger.info(f"Worker PID {process.pid}: Generated no_color variation: '{no_color_string}' from original '{search_string}'")
    else:
        logger.debug(f"Worker PID {process.pid}: No color suffix detected, no_color variation same as original: '{search_string}'")
    
    return variations

async def search_variation(
    variation: str,
    endpoint: str,
    entry_id: int,
    search_type: str,
    brand: Optional[str] = None,
    category: Optional[str] = None,
    logger: Optional[logging.Logger] = None
) -> Dict:
    logger = logger or default_logger
    process = psutil.Process()
    try:
        regions = ['northamerica-northeast', 'us-east', 'southamerica', 'us-central', 'us-west', 'europe', 'australia']
        max_attempts = 5
        total_attempts = [0]

        async def log_retry_status(attempt_type: str, attempt_num: int) -> bool:
            total_attempts[0] += 1
            if total_attempts[0] > max_attempts:
                logger.error(f"Worker PID {process.pid}: Exceeded max retries ({max_attempts}) for EntryID {entry_id} after {attempt_type} attempt {attempt_num}")
                return False
            logger.info(f"Worker PID {process.pid}: {attempt_type} attempt {attempt_num} (Total attempts: {total_attempts[0]}/{max_attempts}) for EntryID {entry_id}")
            return True

        for region in regions:
            if not await log_retry_status("GCloud", total_attempts[0] + 1):
                break
            result = await process_search_row_gcloud(variation, entry_id, logger, remaining_retries=5, total_attempts=total_attempts)
            if result:
                logger.info(f"Worker PID {process.pid}: GCloud attempt succeeded for EntryID {entry_id} with {len(result)} images in region {region}")
                return {"variation": variation, "result": result, "status": "success", "result_count": len(result)}
            logger.warning(f"Worker PID {process.pid}: GCloud attempt failed in region {region}")

        for attempt in range(3):
            if not await log_retry_status("Primary", attempt + 1):
                break
            result = await process_search_row(variation, endpoint, entry_id, search_type, max_retries=15, brand=brand, category=category, logger=logger)
            if result:
                logger.info(f"Worker PID {process.pid}: Primary attempt succeeded for EntryID {entry_id} with {len(result)} images")
                return {"variation": variation, "result": result, "status": "success", "result_count": len(result)}
            logger.warning(f"Worker PID {process.pid}: Primary attempt {attempt + 1} failed")

        logger.error(f"Worker PID {process.pid}: All attempts failed for EntryID {entry_id} after {total_attempts[0]} total attempts")
        return {
            "variation": variation,
            "result": [{
                "EntryID": entry_id,
                "ImageUrl": "placeholder://no-results",
                "ImageDesc": f"No results found for {variation}",
                "ImageSource": "N/A",
                "ImageUrlThumbnail": "placeholder://no-results"
            }],
            "status": "failed",
            "result_count": 1,
            "error": "All search attempts failed"
        }
    except Exception as e:
        logger.error(f"Worker PID {process.pid}: Error searching variation '{variation}' for EntryID {entry_id}: {e}", exc_info=True)
        return {
            "variation": variation,
            "result": [{
                "EntryID": entry_id,
                "ImageUrl": "placeholder://error",
                "ImageDesc": f"Error for {variation}: {str(e)}",
                "ImageSource": "N/A",
                "ImageUrlThumbnail": "placeholder://error"
            }],
            "status": "failed",
            "result_count": 1,
            "error": str(e)
        }

async def process_single_all(
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
    logger = logger or default_logger
    process = psutil.Process()
    
    try:
        entry_id = int(entry_id)
        file_id_db = int(file_id_db)
        if not search_string or not isinstance(search_string, str):
            logger.error(f"Worker PID {process.pid}: Invalid search string for EntryID {entry_id}")
            return False
        model = model or search_string
    except (ValueError, TypeError) as e:
        logger.error(f"Worker PID {process.pid}: Invalid input parameters for EntryID {entry_id}: {e}", exc_info=True)
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
            async with async_engine.connect() as conn:
                result = await conn.execute(
                    text("""
                        SELECT ProductBrand, ProductModel, ProductColor, ProductCategory 
                        FROM utb_ImageScraperRecords 
                        WHERE FileID = :file_id AND EntryID = :entry_id
                    """),
                    {"file_id": file_id_db, "entry_id": entry_id}
                )
                row = result.fetchone()
                result.close()
                if row:
                    result_brand = result_brand or row[0]
                    result_model = result_model or row[1]
                    result_color = result_color or row[2]
                    result_category = result_category or row[3]
                    logger.info(f"Worker PID {process.pid}: Fetched attributes for EntryID {entry_id}: Brand={result_brand}, Model={result_model}, Color={result_color}, Category={result_category}")
                else:
                    logger.warning(f"Worker PID {process.pid}: No attributes found for FileID {file_id_db}, EntryID {entry_id}")
        except SQLAlchemyError as e:
            logger.error(f"Worker PID {process.pid}: Failed to fetch attributes for EntryID {entry_id}: {e}", exc_info=True)

    mem_info = process.memory_info()
    logger.debug(f"Worker PID {process.pid}: Memory before generating variations: RSS={mem_info.rss / 1024**2:.2f} MB")
    variations = generate_search_variations(search_string, result_brand, result_model, brand_rules, logger)
    
    if not endpoint:
        logger.error(f"Worker PID {process.pid}: No healthy endpoint available for EntryID {entry_id}")
        return False

    for search_type in search_types:
        if search_type not in variations:
            logger.warning(f"Worker PID {process.pid}: Search type '{search_type}' not found in variations for EntryID {entry_id}")
            continue
        logger.info(f"Worker PID {process.pid}: Processing search type '{search_type}' for EntryID {entry_id}")
        for variation in variations[search_type]:
            logger.debug(f"Worker PID {process.pid}: Searching variation '{variation}' for EntryID {entry_id}")
            search_result = await search_variation(
                variation=variation,
                endpoint=endpoint,
                entry_id=entry_id,
                search_type=search_type,
                brand=result_brand,
                category=result_category,
                logger=logger
            )
            if search_result["status"] == "success" and search_result["result"]:
                logger.info(f"Worker PID {process.pid}: Found {search_result['result_count']} results for variation '{variation}' in search type '{search_type}'")
                all_results.extend(search_result["result"])
            else:
                logger.warning(f"Worker PID {process.pid}: No valid results for variation '{variation}' in search type '{search_type}'")

        if all_results:
            logger.info(f"Worker PID {process.pid}: Found {len(all_results)} total results for search type '{search_type}' for EntryID {entry_id}")
            break  # Stop after finding results for one search type

    if not all_results:
        logger.error(f"Worker PID {process.pid}: No results found across all search types for EntryID {entry_id}")
        return False

    logger.info(f"Worker PID {process.pid}: Combined {len(all_results)} results from all search types for EntryID {entry_id}")

    deduplicated_results = []
    seen = set()
    for res in all_results:
        key = (res['EntryID'], res['ImageUrl'])
        if key not in seen:
            seen.add(key)
            deduplicated_results.append(res)
    logger.info(f"Worker PID {process.pid}: Deduplicated to {len(deduplicated_results)} rows for EntryID {entry_id}")

    insert_success = await insert_search_results(deduplicated_results, logger=logger, file_id=str(file_id_db))
    if not insert_success:
        logger.error(f"Worker PID {process.pid}: Failed to insert results for EntryID {entry_id}")
        return False

    logger.info(f"Worker PID {process.pid}: Inserted {len(deduplicated_results)} results for EntryID {entry_id}")

    update_result = await update_search_sort_order(
        str(file_id_db), str(entry_id), result_brand, result_model, result_color, result_category, logger, brand_rules=brand_rules
    )
    if update_result is None or not update_result:
        logger.error(f"Worker PID {process.pid}: SortOrder update failed for EntryID {entry_id}")
        return False

    return True