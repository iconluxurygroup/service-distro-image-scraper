import logging
from typing import Optional, List, Dict
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from database_config import async_engine
from common import clean_string
import psutil
import pyodbc

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

def validate_thumbnail_url(url: Optional[str], logger: Optional[logging.Logger] = None) -> bool:
    logger = logger or logging.getLogger(__name__)
    if not url or url == '' or 'placeholder' in str(url).lower():
        logger.debug(f"Invalid thumbnail URL: {url}")
        return False
    if not str(url).startswith(('http://', 'https://')):
        logger.debug(f"Non-HTTP thumbnail URL: {url}")
        return False
    return True

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type(SQLAlchemyError),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying insert_search_results for FileID {retry_state.kwargs.get('file_id', 'unknown')} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def insert_search_results(
    results: List[Dict],
    logger: Optional[logging.Logger] = None,
    file_id: str = None
) -> bool:
    logger = logger or logging.getLogger(__name__)
    process = psutil.Process()

    if not results:
        logger.warning(f"Worker PID {process.pid}: Empty results provided for insert_search_results")
        return False

    required_columns = ["EntryID", "ImageUrl", "ImageDesc", "ImageSource", "ImageUrlThumbnail"]
    for res in results:
        if not all(col in res for col in required_columns):
            missing_cols = set(required_columns) - set(res.keys())
            logger.error(f"Worker PID {process.pid}: Missing required columns: {missing_cols}")
            return False

    try:
        # Validate and cast EntryID to integer
        parameters = []
        for res in results:
            try:
                entry_id = int(res["EntryID"])
            except (ValueError, TypeError):
                logger.error(f"Worker PID {process.pid}: Invalid EntryID value: {res.get('EntryID')}")
                return False

            # Filter irrelevant URLs based on category
            category = res.get("ProductCategory", "")
            image_url = str(res.get("ImageUrl", "")) if res.get("ImageUrl") else ""
            if category.lower() == "footwear" and any(keyword in image_url.lower() for keyword in ["appliance", "whirlpool", "parts"]):
                logger.debug(f"Filtered out irrelevant URL: {image_url}")
                continue

            parameters.append({
                "entry_id": entry_id,
                "image_url": image_url,
                "image_desc": str(res.get("ImageDesc", "")) if res.get("ImageDesc") else "",
                "image_source": str(res.get("ImageSource", "")) if res.get("ImageSource") else "",
                "image_url_thumbnail": str(res.get("ImageUrlThumbnail", "")) if res.get("ImageUrlThumbnail") else ""
            })

        logger.debug(f"Worker PID {process.pid}: Inserting {len(parameters)} rows into utb_ImageScraperResult")
        if not parameters:
            logger.warning(f"Worker PID {process.pid}: No valid rows to insert for FileID {file_id}")
            return False

        async with async_engine.connect() as conn:
            # Clear connection state
            try:
                await conn.execute(text("SELECT 1"))
                await conn.commit()
            except Exception as e:
                logger.warning(f"Worker PID {process.pid}: Failed to clear connection state: {e}")

            await conn.execute(
                text("""
                    INSERT INTO utb_ImageScraperResult (EntryID, ImageUrl, ImageDesc, ImageSource, ImageUrlThumbnail)
                    VALUES (:entry_id, :image_url, :image_desc, :image_source, :image_url_thumbnail)
                """),
                parameters
            )
            await conn.commit()
            logger.info(f"Worker PID {process.pid}: Successfully inserted {len(parameters)} rows for FileID {file_id}")
            return True
    except SQLAlchemyError as e:
        logger.error(f"Worker PID {process.pid}: Database error inserting results for FileID {file_id}: {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Worker PID {process.pid}: Unexpected error inserting results for FileID {file_id}: {e}", exc_info=True)
        return False

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type((pyodbc.Error, ValueError)),
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
    brand_rules: Optional[Dict] = None
) -> Optional[bool]:
    logger = logger or logging.getLogger(__name__)
    process = psutil.Process()
    
    try:
        async with async_engine.connect() as conn:
            query = text("""
                SELECT ResultID, ImageUrl, ImageDesc, ImageSource, ImageUrlThumbnail
                FROM utb_ImageScraperResult
                WHERE EntryID = :entry_id AND FileID = :file_id
            """)
            result = await conn.execute(query, {"entry_id": entry_id, "file_id": file_id})
            rows = result.fetchall()
            columns = result.keys()
            result.close()

        if not rows:
            logger.warning(f"Worker PID {process.pid}: No results found for FileID {file_id}, EntryID {entry_id}")
            return False

        results = [dict(zip(columns, row)) for row in rows]
        logger.debug(f"Worker PID {process.pid}: Fetched {len(results)} rows for EntryID {entry_id}")

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

        for res in results:
            image_desc = clean_string(res.get("ImageDesc", ""), preserve_url=False).lower()
            image_source = clean_string(res.get("ImageSource", ""), preserve_url=True).lower()
            image_url = clean_string(res.get("ImageUrl", ""), preserve_url=True).lower()
            logger.debug(f"Worker PID {process.pid}: ImageDesc: {image_desc[:100]}, ImageSource: {image_source[:100]}, ImageUrl: {image_url[:100]}")

            model_matched = any(alias in image_desc or alias in image_source or alias in image_url for alias in model_aliases)
            brand_matched = any(alias in image_desc or alias in image_source or alias in image_url for alias in brand_aliases)
            logger.debug(f"Worker PID {process.pid}: Model matched: {model_matched}, Brand matched: {brand_matched}")

            if model_matched and brand_matched:
                res["priority"] = 1
            elif model_matched:
                res["priority"] = 2
            elif brand_matched:
                res["priority"] = 3
            else:
                res["priority"] = 4
            logger.debug(f"Worker PID {process.pid}: Assigned priority {res['priority']} to ResultID {res['ResultID']}")

        sorted_results = sorted(results, key=lambda x: x["priority"])
        logger.debug(f"Worker PID {process.pid}: Sorted {len(sorted_results)} results for EntryID {entry_id}")

        async with async_engine.connect() as conn:
            for index, res in enumerate(sorted_results, 1):
                try:
                    await conn.execute(
                        text("""
                            UPDATE utb_ImageScraperResult
                            SET SortOrder = :sort_order
                            WHERE ResultID = :result_id AND EntryID = :entry_id
                        """),
                        {"sort_order": index, "result_id": res["ResultID"], "entry_id": entry_id}
                    )
                    logger.debug(f"Worker PID {process.pid}: Updated SortOrder to {index} for ResultID {res['ResultID']}")
                except SQLAlchemyError as e:
                    logger.error(f"Worker PID {process.pid}: Failed to update SortOrder for ResultID {res['ResultID']}, EntryID {entry_id}: {e}")
                    return False
            await conn.commit()
            logger.info(f"Worker PID {process.pid}: Updated SortOrder for {len(sorted_results)} rows for EntryID {entry_id}")

        return True

    except SQLAlchemyError as e:
        logger.error(f"Worker PID {process.pid}: Database error in update_search_sort_order for EntryID {entry_id}: {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Worker PID {process.pid}: Unexpected error in update_search_sort_order for EntryID {entry_id}: {e}", exc_info=True)
        return False

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type(SQLAlchemyError),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying update_sort_order for FileID {retry_state.kwargs['file_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def update_sort_order(file_id: str, logger: Optional[logging.Logger] = None) -> Optional[List[Dict]]:
    logger = logger or logging.getLogger(__name__)
    try:
        file_id = int(file_id)
        logger.info(f"Starting batch SortOrder update for FileID: {file_id}")
        
        async with async_engine.connect() as conn:
            query = text("""
                SELECT EntryID, ProductBrand, ProductModel, ProductColor, ProductCategory 
                FROM utb_ImageScraperRecords 
                WHERE FileID = :file_id
            """)
            logger.debug(f"Executing query: {query} with FileID: {file_id}")
            result = await conn.execute(query, {"file_id": file_id})
            entries = result.fetchall()
            result.close()
        
        if not entries:
            logger.warning(f"No entries found for FileID: {file_id}")
            return []
            
        results = []
        success_count = 0
        failure_count = 0
        
        for entry in entries:
            entry_id, brand, model, color, category = entry
            logger.debug(f"Worker PID {process.pid}: Processing EntryID {entry_id}, Brand: {brand}, Model: {model}")
            try:
                entry_results = await update_search_sort_order(
                    file_id=str(file_id),
                    entry_id=str(entry_id),
                    brand=brand,
                    model=model,
                    color=color,
                    category=category,
                    logger=logger
                )
                
                if entry_results:
                    results.append({"EntryID": entry_id, "Success": True})
                    success_count += 1
                else:
                    results.append({"EntryID": entry_id, "Success": False})
                    failure_count += 1
                    logger.warning(f"No results for EntryID {entry_id}")
            except Exception as e:
                results.append({"EntryID": entry_id, "Success": False, "Error": str(e)})
                failure_count += 1
                logger.error(f"Error processing EntryID {entry_id}: {e}", exc_info=True)
        
        logger.info(f"Completed batch SortOrder update for FileID {file_id}: {success_count} entries successful, {failure_count} failed")
        
        async with async_engine.connect() as conn:
            verification = {}
            queries = [
                ("PositiveSortOrderEntries", "t.SortOrder > 0"),
                ("BrandMatchEntries", "t.SortOrder = 0"),
                ("NoMatchEntries", "t.SortOrder < 0"),
                ("NullSortOrderEntries", "t.SortOrder IS NULL"),
                ("UnexpectedSortOrderEntries", "t.SortOrder = -1")
            ]
            for key, condition in queries:
                query = text(f"""
                    SELECT COUNT(DISTINCT t.EntryID)
                    FROM utb_ImageScraperResult t
                    INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                    WHERE r.FileID = :file_id AND {condition}
                """)
                result = await conn.execute(query, {"file_id": file_id})
                verification[key] = result.scalar()
                result.close()
            
            query = text("""
                SELECT t.EntryID, t.SortOrder, t.ImageUrl
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                WHERE r.FileID = :file_id
            """)
            result = await conn.execute(query, {"file_id": file_id})
            sort_orders = result.fetchall()
            logger.info(f"SortOrder values for FileID {file_id}: {[(row[0], row[1], row[2][:50]) for row in sort_orders]}")
            
            logger.info(f"Verification for FileID {file_id}: "
                       f"{verification['PositiveSortOrderEntries']} entries with model matches, "
                       f"{verification['BrandMatchEntries']} entries with brand matches only, "
                       f"{verification['NoMatchEntries']} entries with no matches, "
                       f"{verification['NullSortOrderEntries']} entries with NULL SortOrder, "
                       f"{verification['UnexpectedSortOrderEntries']} entries with unexpected SortOrder")
        
        return results
    except SQLAlchemyError as e:
        logger.error(f"Database error in batch SortOrder update for FileID {file_id}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Error in batch SortOrder update for FileID {file_id}: {e}", exc_info=True)
        return None

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type(SQLAlchemyError),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying update_sort_no_image_entry for FileID {retry_state.kwargs['file_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def update_sort_no_image_entry(file_id: str, logger: Optional[logging.Logger] = None) -> Optional[Dict]:
    logger = logger or logging.getLogger(__name__)
    try:
        file_id = int(file_id)
        logger.info(f"Starting per-entry SortOrder update for FileID: {file_id}")
        
        async with async_engine.begin() as conn:
            result = await conn.execute(
                text("""
                    SELECT COUNT(*) 
                    FROM utb_ImageScraperResult 
                    WHERE FileID = :file_id AND SortOrder IS NULL
                """),
                {"file_id": file_id}
            )
            null_count = result.scalar()
            logger.debug(f"Worker PID {process.pid}: {null_count} entries with NULL SortOrder for FileID {file_id}")

            result = await conn.execute(
                text("""
                    DELETE FROM utb_ImageScraperResult
                    WHERE EntryID IN (
                        SELECT r.EntryID
                        FROM utb_ImageScraperRecords r
                        WHERE r.FileID = :file_id
                        AND utb_ImageScraperResult.ImageUrl = 'placeholder://no-results'
                    )
                """),
                {"file_id": file_id}
            )
            rows_deleted = result.rowcount
            logger.info(f"Deleted {rows_deleted} placeholder entries for FileID {file_id}")

            result = await conn.execute(
                text("""
                    UPDATE utb_ImageScraperResult
                    SET SortOrder = -2
                    WHERE EntryID IN (
                        SELECT r.EntryID
                        FROM utb_ImageScraperRecords r
                        WHERE r.FileID = :file_id
                    ) AND SortOrder IS NULL
                """),
                {"file_id": file_id}
            )
            rows_updated = result.rowcount
            logger.info(f"Updated {rows_updated} NULL SortOrder entries to -2 for FileID {file_id}")
            
            return {"file_id": file_id, "rows_deleted": rows_deleted, "rows_updated": rows_updated}
    
    except SQLAlchemyError as e:
        logger.error(f"Database error updating entries for FileID {file_id}: {e}", exc_info=True)
        raise
    except ValueError as ve:
        logger.error(f"Invalid file_id format: {file_id}, error: {str(ve)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error updating entries for FileID {file_id}: {e}", exc_info=True)
        return None

import logging
import asyncio
import os
import time
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
from database_config import conn_str, async_engine
from search_utils import update_search_sort_order, insert_search_results
from endpoint_utils import get_endpoint, sync_get_endpoint
from image_utils import download_all_images
from excel_utils import write_excel_image
from email_utils import send_email, send_message_email
from file_utils import create_temp_dirs, cleanup_temp_dirs
import httpx
import aiofiles
import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import json
import re
import base64
import zlib
import urllib.parse
from requests.exceptions import RequestException
from icon_image_lib.google_parser import process_search_result
import psutil
from ai_utils import batch_vision_reason

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
        response = httpx.get(health_url, timeout=timeout)
        response.raise_for_status()
        status = response.json().get("status", "")
        logger.debug(f"Health check for {endpoint}: Status={status}, Headers={response.headers}")
        return "Google is reachable" in status
    except httpx.RequestException as e:
        logger.warning(f"Endpoint {endpoint} health check failed: {e}, Headers={getattr(e.response, 'headers', 'N/A')}")
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
            result = await process_search_row(variation, entry_id, logger, search_type, max_retries=15, brand=brand, category=category)
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
    logger = logger or logging.getLogger(__name__)
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
        logger.info(f"Worker PID {process.pid}: Processing search type '{search_type}' for EntryID {entry_id} with variations: {variations[search_type]}")
        results = []
        for variation in variations[search_type]:
            mem_info = process.memory_info()
            logger.debug(f"Worker PID {process.pid}: Memory before searching variation: RSS={mem_info.rss / 1024**2:.2f} MB")
            result = await search_variation(variation, endpoint, entry_id, search_type, result_brand, result_category, logger)
            results.append(result)
        successful_results = [res for res in results if res["status"] == "success" and res["result"]]
        if successful_results:
            result_count = sum(len(res["result"]) for res in successful_results)
            all_results.extend([item for res in successful_results for item in res["result"]])
            logger.info(f"Worker PID {process.pid}: Found {len(successful_results)} successful variations with {result_count} total results for search type '{search_type}' for EntryID {entry_id}")
        else:
            logger.info(f"Worker PID {process.pid}: No successful results for search type '{search_type}' for EntryID {entry_id}")

    if all_results:
        try:
            mem_info = process.memory_info()
            logger.debug(f"Worker PID {process.pid}: Memory before combining results: RSS={mem_info.rss / 1024**2:.2f} MB")
            combined_results = []
            for res in all_results:
                new_res = {}
                for key, value in res.items():
                    new_key = api_to_db_mapping.get(key, key)
                    new_res[new_key] = value
                combined_results.append(new_res)
            logger.info(f"Worker PID {process.pid}: Combined {len(combined_results)} results from all search types for EntryID {entry_id}")
            
            for res in combined_results:
                for col in required_columns:
                    if col not in res:
                        logger.warning(f"Worker PID {process.pid}: Missing column {col} in result for EntryID {entry_id}")
                        res[col] = ''
            
            deduplicated_results = []
            seen = set()
            for res in combined_results:
                key = (res['EntryID'], res['ImageUrl'])
                if key not in seen:
                    seen.add(key)
                    deduplicated_results.append(res)
            logger.info(f"Worker PID {process.pid}: Deduplicated to {len(deduplicated_results)} rows for EntryID {entry_id}")
            
            insert_success = await insert_search_results(deduplicated_results, logger=logger, file_id=str(file_id_db))
            if not insert_success:
                logger.error(f"Worker PID {process.pid}: Failed to insert deduplicated results for EntryID {entry_id}")
                return False
            logger.info(f"Worker PID {process.pid}: Inserted {len(deduplicated_results)} results for EntryID {entry_id}")
            
            update_result = await update_search_sort_order(
                file_id=str(file_id_db),
                entry_id=str(entry_id),
                brand=result_brand,
                model=result_model,
                color=result_color,
                category=result_category,
                logger=logger,
                brand_rules=brand_rules
            )
            if update_result is None or not update_result:
                logger.error(f"Worker PID {process.pid}: SortOrder update failed for EntryID {entry_id}")
                return False
            logger.info(f"Worker PID {process.pid}: Updated sort order for EntryID {entry_id}")

            logger.info(f"Worker PID {process.pid}: Starting AI analysis for EntryID {entry_id}")
            ai_result = await batch_vision_reason(
                file_id=str(file_id_db),
                entry_ids=[entry_id],
                step=0,
                limit=1000,
                concurrency=5,
                logger=logger
            )
            if ai_result.get("status_code") != 200:
                logger.error(f"Worker PID {process.pid}: AI analysis failed for EntryID {entry_id}: {ai_result.get('message')}")
                return False
            logger.info(f"Worker PID {process.pid}: Completed AI analysis for EntryID {entry_id} with {len(ai_result.get('data', []))} results")

            try:
                async with async_engine.connect() as conn:
                    result = await conn.execute(
                        text("""
                            SELECT 
                                COUNT(*) AS total_count,
                                SUM(CASE WHEN SortOrder > 0 THEN 1 ELSE 0 END) AS positive_count,
                                SUM(CASE WHEN SortOrder IS NULL THEN 1 ELSE 0 END) AS null_count
                            FROM utb_ImageScraperResult 
                            WHERE EntryID = :entry_id
                        """),
                        {"entry_id": entry_id}
                    )
                    row = result.fetchone()
                    total_count, positive_count, null_count = row
                    logger.info(f"Worker PID {process.pid}: Verification: {total_count} total rows, {positive_count} positive SortOrder, {null_count} NULL SortOrder for EntryID {entry_id}")
                    if null_count > 0:
                        logger.warning(f"Worker PID {process.pid}: Found {null_count} rows with NULL SortOrder for EntryID {entry_id}")
                        await conn.execute(
                            text("UPDATE utb_ImageScraperResult SET SortOrder = -2 WHERE EntryID = :entry_id AND SortOrder IS NULL"),
                            {"entry_id": entry_id}
                        )
                        await conn.commit()
                        logger.info(f"Worker PID {process.pid}: Set {null_count} NULL SortOrder rows to -2 for EntryID {entry_id}")
                    if total_count == 0:
                        logger.error(f"Worker PID {process.pid}: No rows found in utb_ImageScraperResult for EntryID {entry_id} after insertion")
                        return False
                    result.close()
            except SQLAlchemyError as e:
                logger.error(f"Worker PID {process.pid}: Failed to verify SortOrder for EntryID {entry_id}: {e}", exc_info=True)
                return False
            mem_info = process.memory_info()
            logger.debug(f"Worker PID {process.pid}: Memory after processing: RSS={mem_info.rss / 1024**2:.2f} MB")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Worker PID {process.pid}: Database error during batch update for EntryID {entry_id}: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Worker PID {process.pid}: Error during batch database update for EntryID {entry_id}: {e}", exc_info=True)
            return False

    logger.info(f"Worker PID {process.pid}: No results to insert for EntryID {entry_id}")
    placeholder_results = [{
        "EntryID": entry_id,
        "ImageUrl": "placeholder://no-results",
        "ImageDesc": f"No results found for {search_string}",
        "ImageSource": "N/A",
        "ImageUrlThumbnail": "placeholder://no-results"
    }]
    try:
        insert_success = await insert_search_results(placeholder_results, logger=logger, file_id=str(file_id_db))
        if insert_success:
            logger.info(f"Worker PID {process.pid}: Inserted placeholder row for EntryID {entry_id}")
            async with async_engine.connect() as conn:
                await conn.execute(
                    text("UPDATE utb_ImageScraperResult SET SortOrder = -2 WHERE EntryID = :entry_id AND ImageUrl = :image_url"),
                    {"entry_id": entry_id, "image_url": "placeholder://no-results"}
                )
                await conn.commit()
                logger.info(f"Worker PID {process.pid}: Set SortOrder=-2 for placeholder row for EntryID {entry_id}")
            return True
        else:
            logger.error(f"Worker PID {process.pid}: Failed to insert placeholder row for EntryID {entry_id}")
            return False
    except SQLAlchemyError as e:
        logger.error(f"Worker PID {process.pid}: Database error inserting placeholder row for EntryID {entry_id}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Worker PID {process.pid}: Unexpected error inserting placeholder row for EntryID {entry_id}: {e}", exc_info=True)
        return False


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((aiohttp.ClientError, TimeoutError, ValueError)),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Worker PID {psutil.Process().pid}: Retrying process_and_tag_results for EntryID {retry_state.kwargs['entry_id']} (attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def process_and_tag_results(
    search_string,
    brand,
    model,
    endpoint,
    entry_id,
    logger,
    use_all_variations: bool = False,
    file_id_db: int = None
) -> List[Dict]:
    logger = logger or default_logger
    process = psutil.Process()
    try:
        logger.debug(f"Worker PID {process.pid}: Starting process_and_tag_results for EntryID {entry_id}")
        mem_info = process.memory_info()
        logger.debug(f"Worker PID {process.pid}: Memory before processing: RSS={mem_info.rss / 1024**2:.2f} MB")
        
        brand_rules = await fetch_brand_rules(BRAND_RULES_URL, max_attempts=3, timeout=10, logger=logger)
        if not brand_rules:
            logger.warning(f"Worker PID {process.pid}: No brand rules fetched for EntryID {entry_id}")
            brand_rules = {"brand_rules": []}

        if file_id_db is None:
            logger.error(f"Worker PID {process.pid}: FileID not provided for EntryID {entry_id}")
            return [{
                "EntryID": entry_id,
                "ImageUrl": "placeholder://error",
                "ImageDesc": "Error: FileID not provided",
                "ImageSource": "N/A",
                "ImageUrlThumbnail": "placeholder://error",
                "search_type": "default",
                "priority": 4
            }]

        max_row_retries = 3
        process_func = process_single_all if use_all_variations else process_single_row
        logger.debug(f"Worker PID {process.pid}: Calling process_func for EntryID {entry_id}")
        success = await process_func(
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
        logger.debug(f"Worker PID {process.pid}: Process func result for EntryID {entry_id}: {success}")

        if not success:
            logger.error(f"Worker PID {process.pid}: Processing failed for EntryID {entry_id}")
            return [{
                "EntryID": entry_id,
                "ImageUrl": "placeholder://error",
                "ImageDesc": "Error: Processing failed",
                "ImageSource": "N/A",
                "ImageUrlThumbnail": "placeholder://error",
                "search_type": "default",
                "priority": 4
            }]

        try:
            async with async_engine.connect() as conn:
                query = text("""
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
                    WHERE r.EntryID = :entry_id
                """)
                logger.debug(f"Worker PID {process.pid}: Executing database query for EntryID {entry_id}")
                result = await conn.execute(query, {"entry_id": entry_id})
                rows = result.fetchall()
                columns = result.keys()
                results = [dict(zip(columns, row)) for row in rows]
                result.close()
                logger.debug(f"Worker PID {process.pid}: Retrieved {len(results)} rows for EntryID {entry_id}")
        except SQLAlchemyError as e:
            logger.error(f"Worker PID {process.pid}: Failed to retrieve results for EntryID {entry_id}: {e}", exc_info=True)
            return [{
                "EntryID": entry_id,
                "ImageUrl": "placeholder://error",
                "ImageDesc": f"Error: Database retrieval failed: {str(e)}",
                "ImageSource": "N/A",
                "ImageUrlThumbnail": "placeholder://error",
                "search_type": "default",
                "priority": 4
            }]

        all_results = []
        if results:
            for res in results:
                res['search_type'] = 'default'
                res['ImageDesc_clean'] = clean_string(res.get('ImageDesc', ''), preserve_url=False)
                res['ImageSource_clean'] = clean_string(res.get('ImageSource', ''), preserve_url=True)
                res['ImageUrl_clean'] = clean_string(res.get('ImageUrl', ''), preserve_url=True)
                res['ProductBrand_clean'] = clean_string(res.get('ProductBrand', ''), preserve_url=False)

            brand_aliases = []
            for rule in brand_rules["brand_rules"]:
                if any(brand.lower() in name.lower() for name in rule.get("names", [])):
                    brand_aliases = rule.get("names", [])
                    break
            exact_results, _ = await filter_model_results(
                results,
                debug=False,
                logger=logger,
                brand_aliases=brand_aliases
            )

            def calculate_priority_list(results, exact_results, model_clean, model_aliases, brand_clean, brand_aliases):
                exact_ids = {res['ResultID'] for res in exact_results}
                prioritized_results = []
                for res in results:
                    model_matched = res['ResultID'] in exact_ids
                    brand_matched = (
                        any(alias.lower() in res.get('ImageDesc_clean', '').lower() for alias in brand_aliases) or
                        any(alias.lower() in res.get('ImageSource_clean', '').lower() for alias in brand_aliases) or
                        any(alias.lower() in res.get('ImageUrl_clean', '').lower() for alias in brand_aliases)
                    )
                    if model_matched and brand_matched:
                        priority = 1
                    elif model_matched:
                        priority = 2
                    elif brand_matched:
                        priority = 3
                    else:
                        priority = 4
                    res['priority'] = priority
                    prioritized_results.append(res)
                return prioritized_results

            model_clean = normalize_model(model or search_string)
            model_aliases = generate_aliases(model_clean)
            brand_clean = clean_string(brand).lower() if brand else ''
            brand_aliases = await generate_brand_aliases(brand_clean, {}) if brand else []
            all_results = calculate_priority_list(
                results, exact_results, model_clean, model_aliases, brand_clean, brand_aliases
            )
        else:
            all_results = [{
                "EntryID": entry_id,
                "ImageUrl": "placeholder://no-results",
                "ImageDesc": f"No results found for {search_string}",
                "ImageSource": "N/A",
                "ImageUrlThumbnail": "placeholder://no-results",
                "search_type": "default",
                "priority": 4
            }]

        mem_info = process.memory_info()
        logger.debug(f"Worker PID {process.pid}: Memory after processing: RSS={mem_info.rss / 1024**2:.2f} MB")
        return all_results

    except SQLAlchemyError as e:
        logger.error(f"Worker PID {process.pid}: Database error in process_and_tag_results for EntryID {entry_id}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Worker PID {process.pid}: Unexpected error in process_and_tag_results for EntryID {entry_id}: {e}", exc_info=True)
        return [{
            "EntryID": entry_id,
            "ImageUrl": "placeholder://error",
            "ImageDesc": f"Error: {str(e)}",
            "ImageSource": "N/A",
            "ImageUrlThumbnail": "placeholder://error",
            "search_type": "default",
            "priority": 4
        }]
