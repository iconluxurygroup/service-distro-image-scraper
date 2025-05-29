from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, APIRouter
from pydantic import BaseModel, Field
import logging
import asyncio
import os
import json
import traceback
import psutil
import pyodbc
import datetime
import urllib.parse
import hashlib
import time
import httpx
import aiohttp
import pandas as pd
from typing import Optional, List, Dict, Any, Callable
from icon_image_lib.google_parser import process_search_result
from common import generate_search_variations,fetch_brand_rules,preprocess_sku
from logging_config import setup_job_logger
from s3_utils import upload_file_to_space
from ai_utils import batch_vision_reason
from db_utils import (
    update_log_url_in_db,
    get_send_to_email,
    fetch_last_valid_entry,
    update_initial_sort_order,
    get_images_excel_db,
    update_file_generate_complete,
    update_file_location_complete,
)
from search_utils import update_search_sort_order, insert_search_results, update_sort_order, update_sort_no_image_entry
from database_config import conn_str, async_engine
from config import BRAND_RULES_URL, VERSION, SEARCH_PROXY_API_URL
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from email_utils import send_message_email
from urllib.parse import urlparse
from url_extract import extract_thumbnail_url
from openpyxl import Workbook
from openpyxl.styles import PatternFill
from openpyxl.utils import get_column_letter
from rabbitmq_consumer import RabbitMQConsumer

from contextlib import asynccontextmanager
import signal
import asyncio


app = FastAPI(title="super_scraper", version=VERSION)

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

router = APIRouter()

JOB_STATUS = {}
LAST_UPLOAD = {}

class JobStatusResponse(BaseModel):
    status: str = Field(..., description="Job status (e.g., queued, running, completed, failed)")
    message: str = Field(..., description="Descriptive message about the job status")
    public_url: Optional[str] = Field(None, description="R2 URL of the generated Excel file, if available")
    log_url: Optional[str] = Field(None, description="R2 URL of the job log file, if available")
    timestamp: str = Field(..., description="ISO timestamp of the response")

class SearchClient:
    def __init__(self, endpoint: str, logger: logging.Logger, max_concurrency: int = 10):
        self.endpoint = endpoint
        self.logger = logger
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiMGRkZTIwZjAtNjlmZS00ODc2LWE0MmItMTY1YzM1YTk4MzMyIiwiaWF0IjoxNzQ3MDg5NzQ2LjgzMjU3OCwiZXhwIjoxNzc4NjI1NzQ2LjgzMjU4M30.pvPx3K8AIrV3gPnQqAC0BLGrlugWhLYLeYrgARkBG-g"
        self.headers = {
            "accept": "application/json",
            "x-api-key": self.api_key,
            "Content-Type": "application/json"
        }
        self.regions = ['northamerica-northeast', 'us-east', 'southamerica', 'us-central', 'us-west', 'europe', 'australia', 'asia', 'middle-east']

    async def close(self):
        pass  # aiohttp.ClientSession is managed per request
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, json.JSONDecodeError))
    )
    async def search(self, term: str, brand: str, entry_id: int) -> List[Dict]:
        async with self.semaphore:
            process = psutil.Process()
            search_url = f"https://www.google.com/search?q={urllib.parse.quote(term)}&tbm=isch"
            for region in self.regions:
                fetch_endpoint = f"{self.endpoint}?region={region}"
                self.logger.info(f"Worker PID {process.pid}: Fetching {search_url} via {fetch_endpoint} with region {region}")
                try:
                    async with aiohttp.ClientSession(headers=self.headers) as session:
                        async with session.post(fetch_endpoint, json={"url": search_url}, timeout=60) as response:
                            body_text = await response.text()
                            body_preview = body_text[:200] if body_text else ""
                            self.logger.debug(f"Worker PID {process.pid}: Response: status={response.status}, headers={response.headers}, body={body_preview}")
                            if response.status in (429, 503):
                                self.logger.warning(f"Worker PID {process.pid}: Rate limit or service unavailable (status {response.status}) for {fetch_endpoint}")
                                raise aiohttp.ClientError(f"Rate limit or service unavailable: {response.status}")
                            response.raise_for_status()
                            result = await response.json()
                            results = result.get("result")
                            self.logger.debug(f"Worker PID {process.pid}: API result: {results[:200] if results else 'None'}")
                            if not results:
                                self.logger.warning(f"Worker PID {process.pid}: No results for term '{term}' in region {region}")
                                continue
                            results_html_bytes = results if isinstance(results, bytes) else results.encode("utf-8")
                            formatted_results = process_search_result(results_html_bytes, results_html_bytes, entry_id, self.logger)
                            self.logger.debug(f"Worker PID {process.pid}: Formatted results: {formatted_results.to_dict() if not formatted_results.empty else 'Empty'}")
                            if not formatted_results.empty:
                                self.logger.info(f"Worker PID {process.pid}: Found {len(formatted_results)} results for term '{term}' in region {region}")
                                return [
                                    {
                                        "EntryID": entry_id,
                                        "ImageUrl": res.get("ImageUrl", "placeholder://no-image"),
                                        "ImageDesc": res.get("ImageDesc", ""),
                                        "ImageSource": res.get("ImageSource", "N/A"),
                                        "ImageUrlThumbnail": res.get("ImageUrlThumbnail", res.get("ImageUrl", "placeholder://no-thumbnail"))
                                    }
                                    for _, res in formatted_results.iterrows()
                                ]
                            self.logger.warning(f"Worker PID {process.pid}: Empty results for term '{term}' in region {region}")
                except (aiohttp.ClientError, json.JSONDecodeError) as e:
                    self.logger.warning(f"Worker PID {process.pid}: Failed for term '{term}' in region {region}: {e}")
                    continue
            self.logger.error(f"Worker PID {process.pid}: All regions failed for term '{term}'")
            return []
async def process_and_tag_results(
    search_string: str,
    brand: Optional[str] = None,
    model: Optional[str] = None,
    endpoint: str = None,
    entry_id: int = None,
    logger: Optional[logging.Logger] = None,
    use_all_variations: bool = False,
    file_id_db: Optional[int] = None
) -> List[Dict]:
    logger = logger or default_logger
    process = psutil.Process()
    
    try:
        logger.debug(f"Worker PID {process.pid}: Processing results for EntryID {entry_id}, Search: {search_string}")
        
        # Generate search variations
        variations = await generate_search_variations(
            search_string=search_string,
            brand=brand,
            model=model,
            logger=logger
        )
        
        all_results = []
        search_types = [
            "default", "delimiter_variations", "color_variations",
            "brand_alias", "no_color", "model_alias", "category_specific"
        ]
        
        required_columns = ["EntryID", "ImageUrl", "ImageDesc", "ImageSource", "ImageUrlThumbnail"]
        
        # Initialize SearchClient
        endpoint = endpoint or SEARCH_PROXY_API_URL
        client = SearchClient(endpoint=endpoint, logger=logger)
        
        try:
            for search_type in search_types:
                if search_type not in variations:
                    logger.warning(f"Worker PID {process.pid}: Search type '{search_type}' not found for EntryID {entry_id}")
                    continue
                
                logger.info(f"Worker PID {process.pid}: Processing search type '{search_type}' for EntryID {entry_id}")
                for variation in variations[search_type]:
                    logger.debug(f"Worker PID {process.pid}: Searching variation '{variation}' for EntryID {entry_id}")
                    
                    # Use SearchClient.search instead of search_variation
                    search_results = await client.search(
                        term=variation,
                        brand=brand or "",
                        entry_id=entry_id
                    )
                    
                    if search_results:
                        logger.info(f"Worker PID {process.pid}: Found {len(search_results)} results for variation '{variation}'")
                        tagged_results = []
                        for res in search_results:
                            tagged_result = {
                                "EntryID": entry_id,
                                "ImageUrl": res.get("ImageUrl", "placeholder://no-image"),
                                "ImageDesc": res.get("ImageDesc", ""),
                                "ImageSource": res.get("ImageSource", "N/A"),
                                "ImageUrlThumbnail": res.get("ImageUrlThumbnail", res.get("ImageUrl", "placeholder://no-thumbnail")),
                                "ProductCategory": res.get("ProductCategory", "")
                            }
                            if all(col in tagged_result for col in required_columns):
                                tagged_results.append(tagged_result)
                            else:
                                logger.warning(f"Worker PID {process.pid}: Skipping result with missing columns for EntryID {entry_id}")
                        
                        all_results.extend(tagged_results)
                        logger.info(f"Worker PID {process.pid}: Added {len(tagged_results)} valid results for variation '{variation}'")
                    else:
                        logger.warning(f"Worker PID {process.pid}: No valid results for variation '{variation}' in search type '{search_type}'")
                
                if all_results and not use_all_variations:
                    logger.info(f"Worker PID {process.pid}: Stopping after {len(all_results)} results from '{search_type}' for EntryID {entry_id}")
                    break
        finally:
            await client.close()
        
        if not all_results:
            logger.error(f"Worker PID {process.pid}: No valid results found across all search types for EntryID {entry_id}")
            return [{
                "EntryID": entry_id,
                "ImageUrl": "placeholder://no-results",
                "ImageDesc": f"No results found for {search_string}",
                "ImageSource": "N/A",
                "ImageUrlThumbnail": "placeholder://no-results",
                "ProductCategory": ""
            }]
        
        # Deduplicate results
        deduplicated_results = []
        seen_urls = set()
        for res in all_results:
            image_url = res["ImageUrl"]
            if image_url not in seen_urls and image_url != "placeholder://no-results":
                seen_urls.add(image_url)
                deduplicated_results.append(res)
        
        logger.info(f"Worker PID {process.pid}: Deduplicated to {len(deduplicated_results)} results for EntryID {entry_id}")
        
        # Filter irrelevant results
        irrelevant_keywords = ['wallpaper', 'sofa', 'furniture', 'decor', 'stock photo', 'card', 'pokemon']
        filtered_results = [
            res for res in deduplicated_results
            if not any(kw.lower() in res.get("ImageDesc", "").lower() for kw in irrelevant_keywords)
        ]
        
        logger.info(f"Worker PID {process.pid}: Filtered to {len(filtered_results)} results after removing irrelevant items for EntryID {entry_id}")
        
        return filtered_results
    
    except Exception as e:
        logger.error(f"Worker PID {process.pid}: Error processing results for EntryID {entry_id}: {e}", exc_info=True)
        return [{
            "EntryID": entry_id,
            "ImageUrl": "placeholder://error",
            "ImageDesc": f"Error processing: {str(e)}",
            "ImageSource": "N/A",
            "ImageUrlThumbnail": "placeholder://error",
            "ProductCategory": ""
        }]

async def process_results(
    raw_results: List[Dict],
    entry_id: int,
    brand: str,
    search_string: str,  # Renamed from search_term to match process_and_tag_results
    logger: logging.Logger
) -> List[Dict]:
    results = []
    required_columns = ["EntryID", "ImageUrl", "ImageDesc", "ImageSource", "ImageUrlThumbnail"]

    # Call process_and_tag_results with correct parameters
    tagged_results = await process_and_tag_results(
        search_string=search_string,
        brand=brand,
        entry_id=entry_id,
        logger=logger,
        use_all_variations=False  # Default value, adjust if needed
    )
    for result in tagged_results:
        image_url = result.get("ImageUrl")  # Adjusted to match output of process_and_tag_results
        if not image_url:
            continue

        thumbnail_url = await extract_thumbnail_url(image_url, logger=logger) or image_url
        parsed_url = urlparse(image_url)
        image_source = parsed_url.netloc or "unknown"

        formatted_result = {
            "EntryID": entry_id,
            "ImageUrl": image_url,
            "ImageDesc": result.get("ImageDesc", ""),
            "ImageSource": image_source,
            "ImageUrlThumbnail": thumbnail_url
        }

        if all(col in formatted_result for col in required_columns):
            results.append(formatted_result)
        else:
            logger.warning(f"Result missing required columns for EntryID {entry_id}: {formatted_result}")

    return results

async def async_process_entry_search(
    search_string: str,
    brand: str,
    endpoint: str,
    entry_id: int,
    use_all_variations: bool,
    file_id_db: int,
    logger: logging.Logger
) -> List[Dict]:
    logger.debug(f"Processing search for EntryID {entry_id}, FileID {file_id_db}, Use all variations: {use_all_variations}")
    
    search_terms_dict = await generate_search_variations(search_string, brand, logger=logger)
    
    search_terms = []
    variation_types = [
        "default", "delimiter_variations", "color_variations", "brand_alias",
        "no_color", "model_alias", "category_specific"
    ]
    
    for variation_type in variation_types:
        if variation_type in search_terms_dict:
            search_terms.extend(search_terms_dict[variation_type])
    
    search_terms = list(dict.fromkeys([term.lower().strip() for term in search_terms]))
    logger.info(f"Generated {len(search_terms)} unique search terms for EntryID {entry_id}")

    if not search_terms:
        logger.warning(f"No search terms for EntryID {entry_id}")
        return []

    client = SearchClient(endpoint, logger)
    try:
        all_results = []
        required_columns = ["EntryID", "ImageUrl", "ImageDesc", "ImageSource", "ImageUrlThumbnail"]
        for term in search_terms:
            logger.debug(f"Searching term '{term}' for EntryID {entry_id}")
            results = await client.search(term, brand, entry_id)
            if isinstance(results, Exception):
                logger.error(f"Error for term '{term}' in EntryID {entry_id}: {results}")
                continue
            if not results:
                logger.debug(f"No results for term '{term}' in EntryID {entry_id}")
                continue
            processed_results = await process_results(results, entry_id, brand, term, logger)
            if not processed_results:
                logger.debug(f"No valid processed results for term '{term}' in EntryID {entry_id}")
                continue
            # Validate results have required columns and non-placeholder URLs
            valid_results = [
                res for res in processed_results
                if all(col in res for col in required_columns) and not res["ImageUrl"].startswith("placeholder://")
            ]
            all_results.extend(valid_results)
            logger.info(f"Found {len(valid_results)} valid results for term '{term}' in EntryID {entry_id}")
            # Stop after valid results unless use_all_variations is True and more results are needed
            if valid_results and (not use_all_variations or len(all_results) >= 10):  # Arbitrary threshold
                logger.info(f"Stopping search after valid results for term '{term}' in EntryID {entry_id}")
                break
        logger.info(f"Processed {len(all_results)} total valid results for EntryID {entry_id}")
        return all_results
    finally:
        await client.close()
        

async def run_job_with_logging(job_func: Callable[..., Any], file_id: str, **kwargs) -> Dict:
    file_id_str = str(file_id)
    logger, log_file = setup_job_logger(job_id=file_id_str, console_output=True)
    result = None
    debug_info = {"memory_usage": {}, "log_file": log_file, "endpoint_errors": []}
    
    try:
        func_name = getattr(job_func, '_name', 'unknown_function') if hasattr(job_func, '_remote') else job_func.__name__
        logger.info(f"Starting job {func_name} for FileID: {file_id}")
        
        process = psutil.Process()
        debug_info["memory_usage"]["before"] = process.memory_info().rss / 1024 / 1024
        logger.debug(f"Memory before job {func_name}: RSS={debug_info['memory_usage']['before']:.2f} MB")
        
        if asyncio.iscoroutinefunction(job_func) or hasattr(job_func, '_remote'):
            result = await job_func(file_id, logger=logger, **kwargs)  # Pass kwargs including background_tasks
        else:
            result = job_func(file_id, logger=logger, **kwargs)
        
        debug_info["memory_usage"]["after"] = process.memory_info().rss / 1024 / 1024
        logger.debug(f"Memory after job {func_name}: RSS={debug_info['memory_usage']['after']:.2f} MB")
        if debug_info["memory_usage"]["after"] > 1000:
            logger.warning(f"High memory usage after job {func_name}: RSS={debug_info['memory_usage']['after']:.2f} MB")
        
        logger.info(f"Completed job {func_name} for FileID: {file_id}")
        return {
            "status_code": 200,
            "message": f"Job {func_name} completed successfully for FileID: {file_id}",
            "data": result,
            "debug_info": debug_info
        }
    except Exception as e:
        func_name = getattr(job_func, '_name', 'unknown_function') if hasattr(job_func, '_remote') else job_func.__name__
        logger.error(f"Error in job {func_name} for FileID: {file_id}: {e}")
        logger.debug(f"Traceback: {traceback.format_exc()}")
        debug_info["error_traceback"] = traceback.format_exc()
        if "placeholder://error" in str(e):
            debug_info["endpoint_errors"].append({"error": str(e), "timestamp": datetime.datetime.now().isoformat()})
            logger.warning(f"Detected placeholder error in job {func_name} for FileID: {file_id}")
        return {
            "status_code": 500,
            "message": f"Error in job {func_name} for FileID {file_id}: {str(e)}",
            "data": None,
            "debug_info": debug_info
        }
    finally:
        debug_info["log_url"] = await upload_log_file(file_id_str, log_file, logger)
import httpx

async def run_generate_download_file(file_id: str, logger: logging.Logger, log_filename: str, background_tasks: BackgroundTasks):
    try:
        JOB_STATUS[file_id] = {
            "status": "running",
            "message": "Job is running",
            "timestamp": datetime.datetime.now().isoformat()
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"https://icon7-8001.iconluxury.today/generate-download-file/?file_id={file_id}",
                headers={"accept": "application/json"},
                data=""
            )
            response.raise_for_status()  # Raise an exception for non-2xx status codes
            result = response.json()
        
        if "error" in result:
            JOB_STATUS[file_id] = {
                "status": "failed",
                "message": f"Error: {result['error']}",
                "log_url": result.get("log_filename") if os.path.exists(result.get("log_filename", "")) else None,
                "timestamp": datetime.datetime.now().isoformat()
            }
            logger.error(f"Job failed for FileID {file_id}: {result['error']}")
        else:
            JOB_STATUS[file_id] = {
                "status": "completed",
                "message": "Job completed successfully",
                "public_url": result.get("public_url"),
                "log_url": result.get("log_filename") if os.path.exists(result.get("log_filename", "")) else None,
                "timestamp": datetime.datetime.now().isoformat()
            }
            logger.info(f"Job completed for FileID {file_id}")
    except Exception as e:
        logger.error(f"Unexpected error in job for FileID {file_id}: {e}", exc_info=True)
        log_public_url = await upload_log_file(file_id, log_filename, logger)
        JOB_STATUS[file_id] = {
            "status": "failed",
            "message": f"Unexpected error: {str(e)}",
            "log_url": log_public_url or None,
            "timestamp": datetime.datetime.now().isoformat()
        }
async def upload_log_file(file_id: str, log_filename: str, logger: logging.Logger) -> Optional[str]:
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(Exception),
        before_sleep=lambda retry_state: logger.info(
            f"Retrying log upload for FileID {file_id} (attempt {retry_state.attempt_number}/3)"
        )
    )
    async def try_upload():
        if not os.path.exists(log_filename):
            logger.warning(f"Log file {log_filename} does not exist, skipping upload")
            return None

        file_hash = await asyncio.to_thread(hashlib.md5, open(log_filename, "rb").read())
        file_hash = file_hash.hexdigest()
        current_time = time.time()
        key = (log_filename, file_id)

        if key in LAST_UPLOAD and LAST_UPLOAD[key]["hash"] == file_hash and current_time - LAST_UPLOAD[key]["time"] < 60:
            logger.info(f"Skipping redundant upload for {log_filename}")
            return LAST_UPLOAD[key]["url"]

        try:
            upload_url = await upload_file_to_space(
                file_src=log_filename,
                save_as=f"job_logs/job_{file_id}.log",
                is_public=True,
                logger=logger,
                file_id=file_id
            )
            if not upload_url:
                logger.error(f"S3 upload returned empty URL for {log_filename}")
                raise ValueError("Empty upload URL")
            await update_log_url_in_db(file_id, upload_url, logger)
            LAST_UPLOAD[key] = {"hash": file_hash, "time": current_time, "url": upload_url}
            logger.info(f"Log uploaded to R2: {upload_url}")
            return upload_url
        except Exception as e:
            logger.error(f"Failed to upload log for FileID {file_id}: {e}", exc_info=True)
            raise

    try:
        return await try_upload()
    except Exception as e:
        logger.error(f"Failed to upload log for FileID {file_id} after retries: {e}", exc_info=True)
        return None
async def async_process_entry_search(
    search_string: str,
    brand: str,
    endpoint: str,
    entry_id: int,
    use_all_variations: bool,
    file_id_db: int,
    logger: logging.Logger
) -> List[Dict]:
    logger.debug(f"Processing search for EntryID {entry_id}, FileID {file_id_db}, Use all variations: {use_all_variations}")
    
    search_terms_dict = await generate_search_variations(search_string, brand, logger=logger)
    
    search_terms = []
    variation_types = [
        "default", "delimiter_variations", "color_variations", "brand_alias",
        "no_color", "model_alias", "category_specific"
    ]
    
    for variation_type in variation_types:
        if variation_type in search_terms_dict:
            search_terms.extend(search_terms_dict[variation_type])
    
    search_terms = list(dict.fromkeys([term.lower().strip() for term in search_terms]))
    logger.info(f"Generated {len(search_terms)} unique search terms for EntryID {entry_id}")

    if not search_terms:
        logger.warning(f"No search terms for EntryID {entry_id}")
        return [{
            "EntryID": entry_id,
            "ImageUrl": "placeholder://no-search-terms",
            "ImageDesc": f"No search terms generated for {search_string}",
            "ImageSource": "N/A",
            "ImageUrlThumbnail": "placeholder://no-search-terms",
        }]

    client = SearchClient(endpoint, logger)
    try:
        all_results = []
        required_columns = ["EntryID", "ImageUrl", "ImageDesc", "ImageSource", "ImageUrlThumbnail"]
        for term in search_terms:
            logger.debug(f"Searching term '{term}' for EntryID {entry_id}")
            results = await client.search(term, brand, entry_id)
            if isinstance(results, Exception):
                logger.error(f"Error for term '{term}' in EntryID {entry_id}: {results}")
                continue
            if not results:
                logger.debug(f"No results for term '{term}' in EntryID {entry_id}")
                continue
            processed_results = await process_results(results, entry_id, brand, term, logger)
            if not processed_results:
                logger.debug(f"No processed results for term '{term}' in EntryID {entry_id}")
                continue
            valid_results = [
                res for res in processed_results
                if all(col in res for col in required_columns) and not res["ImageUrl"].startswith("placeholder://")
            ]
            if not valid_results:
                logger.warning(f"No valid results for term '{term}' in EntryID {entry_id}. Sample result: {processed_results[0] if processed_results else 'None'}")
                continue
            all_results.extend(valid_results)
            logger.info(f"Found {len(valid_results)} valid results for term '{term}' in EntryID {entry_id}")
            if valid_results and (not use_all_variations or len(all_results) >= 10):
                logger.info(f"Stopping search after valid results for term '{term}' in EntryID {entry_id}")
                break
        
        if not all_results:
            logger.error(f"No valid results found across all search terms for EntryID {entry_id}")
            return [{
                "EntryID": entry_id,
                "ImageUrl": "placeholder://no-results",
                "ImageDesc": f"No results found for {search_string}",
                "ImageSource": "N/A",
                "ImageUrlThumbnail": "placeholder://no-results",
            }]
        
        logger.info(f"Processed {len(all_results)} total valid results for EntryID {entry_id}")
        return all_results
    finally:
        await client.close()

from rabbitmq_producer import enqueue_db_update, RabbitMQProducer
import json
import uuid

from rabbitmq_producer import enqueue_db_update, RabbitMQProducer
import json
import uuid

from fastapi import BackgroundTasks
from sqlalchemy.sql import text
import asyncio
from typing import Optional, List, Dict, Any
from collections import defaultdict
import uuid
import json
from rabbitmq_producer import RabbitMQProducer, enqueue_db_update

from fastapi import BackgroundTasks
from sqlalchemy.sql import text
import asyncio
from typing import Optional, List, Dict, Any
import uuid
import json
from rabbitmq_producer import RabbitMQProducer, enqueue_db_update
import httpx
from fastapi import BackgroundTasks
from sqlalchemy.sql import text
import asyncio
from typing import Optional, List, Dict, Any
import uuid
import json
from rabbitmq_producer import RabbitMQProducer, enqueue_db_update

from fastapi import BackgroundTasks
from sqlalchemy.sql import text
import asyncio
from typing import Optional, List, Dict, Any
import uuid
import json
from rabbitmq_producer import RabbitMQProducer, enqueue_db_update

async def process_restart_batch(
    file_id_db: int,
    entry_id: Optional[int] = None,
    use_all_variations: bool = False,
    logger: Optional[logging.Logger] = None,
    background_tasks: BackgroundTasks = None,
) -> Dict[str, str]:
    log_filename = f"job_logs/job_{file_id_db}.log"
    try:
        if logger is None:
            logger, log_filename = setup_job_logger(job_id=str(file_id_db), log_dir="job_logs", console_output=True)
        logger.setLevel(logging.DEBUG)
        process = psutil.Process()
        logger.debug(f"Logger initialized")

        def log_memory_usage():
            mem_info = process.memory_info()
            logger.debug(f"Memory: RSS={mem_info.rss / 1024**2:.2f} MB")
            if mem_info.rss / 1024**2 > 1000:
                logger.warning(f"High memory usage")

        logger.info(f"Starting processing for FileID: {file_id_db}, Use all variations: {use_all_variations}")
        log_memory_usage()

        file_id_db_int = file_id_db
        BATCH_SIZE = 50  # Process 50 entries per batch for 50 entries/s
        MAX_CONCURRENCY = 50  # One task per entry
        MAX_ENTRY_RETRIES = 3  # Limit retries to reduce latency
        RELEVANCE_THRESHOLD = 0.9  # Stop if relevance >= 0.9

        # Validate FileID
        async with async_engine.connect() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) FROM utb_ImageScraperFiles WHERE ID = :file_id"),
                {"file_id": file_id_db_int}
            )
            if result.fetchone()[0] == 0:
                logger.error(f"FileID {file_id_db} does not exist")
                return {"error": f"FileID {file_id_db} does not exist", "log_filename": log_filename, "log_public_url": "", "last_entry_id": str(entry_id or "")}
            result.close()

        # Fetch starting entry_id
        if entry_id is None:
            entry_id = await fetch_last_valid_entry(str(file_id_db_int), logger)
            if entry_id is not None:
                async with async_engine.connect() as conn:
                    result = await conn.execute(
                        text("SELECT MIN(EntryID) FROM utb_ImageScraperRecords WHERE FileID = :file_id AND EntryID > :entry_id AND Step1 IS NULL"),
                        {"file_id": file_id_db_int, "entry_id": entry_id}
                    )
                    next_entry = result.fetchone()
                    entry_id = next_entry[0] if next_entry and next_entry[0] else None
                    logger.debug(f"Resuming from EntryID: {entry_id}")
                    result.close()

        # Cache brand rules
        brand_rules = await fetch_brand_rules(BRAND_RULES_URL, max_attempts=3, timeout=10, logger=logger)
        if not brand_rules:
            logger.warning(f"No brand rules fetched")
            return {"message": "Failed to fetch brand rules", "file_id": str(file_id_db), "log_filename": log_filename, "log_public_url": "", "last_entry_id": str(entry_id or "")}

        endpoint = SEARCH_PROXY_API_URL
        logger.debug(f"Using endpoint: {endpoint}")

        # Fetch entries
        async with async_engine.connect() as conn:
            query = text("""
                SELECT r.EntryID, r.ProductModel, r.ProductBrand, r.ProductColor, r.ProductCategory 
                FROM utb_ImageScraperRecords r
                LEFT JOIN utb_ImageScraperResult t ON r.EntryID = t.EntryID
                WHERE r.FileID = :file_id 
                AND (:entry_id IS NULL OR r.EntryID >= :entry_id)
                AND r.Step1 IS NULL
                AND (t.EntryID IS NULL OR t.SortOrder IS NULL OR t.SortOrder <= 0)
                ORDER BY r.EntryID
            """)
            result = await conn.execute(query, {"file_id": file_id_db_int, "entry_id": entry_id})
            entries = [(row[0], row[1], row[2], row[3], row[4]) for row in result.fetchall() if row[1] is not None]
            logger.info(f"Found {len(entries)} entries needing processing")
            result.close()

        if not entries:
            logger.warning(f"No valid EntryIDs found for FileID {file_id_db}")
            return {"error": "No entries found", "log_filename": log_filename, "log_public_url": "", "last_entry_id": str(entry_id or "")}

        entry_batches = [entries[i:i + BATCH_SIZE] for i in range(0, len(entries), BATCH_SIZE)]
        logger.info(f"Created {len(entry_batches)} batches")

        successful_entries = 0
        failed_entries = 0
        last_entry_id_processed = entry_id or 0
        required_columns = ["EntryID", "ImageUrl", "ImageDesc", "ImageSource", "ImageUrlThumbnail"]

        # Initialize RabbitMQ producer
        producer = RabbitMQProducer()
        try:
            semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
            async def process_entry(entry):
                entry_id, search_string, brand, color, category = entry
                async with semaphore:
                    try:
                        for attempt in range(1, MAX_ENTRY_RETRIES + 1):
                            try:
                                logger.debug(f"Processing EntryID {entry_id}, Attempt {attempt}/{MAX_ENTRY_RETRIES}")
                                search_string, brand, model, color = await preprocess_sku(
                                    search_string=search_string,
                                    known_brand=brand,
                                    brand_rules=brand_rules,
                                    logger=logger
                                )
                                # Generate and order search terms (worst to best)
                                search_terms_dict = await generate_search_variations(search_string, brand, logger=logger)
                                variation_order = [
                                    "default", "brand_alias", "model_alias", "delimiter_variations",
                                    "color_variations", "no_color", "category_specific"
                                ]
                                search_terms = []
                                for variation_type in variation_order:
                                    if variation_type in search_terms_dict:
                                        search_terms.extend(search_terms_dict[variation_type])
                                search_terms = [term.lower().strip() for term in search_terms]
                                logger.debug(f"Generated {len(search_terms)} search terms for EntryID {entry_id}: {search_terms}")

                                if not search_terms:
                                    logger.warning(f"No search terms for EntryID {entry_id}")
                                    return entry_id, False

                                client = SearchClient(endpoint, logger, max_concurrency=1)  # Single search at a time
                                try:
                                    for term in search_terms:
                                        logger.debug(f"Searching term '{term}' for EntryID {entry_id}")
                                        results = await client.search(term, brand, entry_id)
                                        if isinstance(results, Exception):
                                            logger.debug(f"Error for term '{term}' in EntryID {entry_id}: {results}")
                                            continue
                                        if not results:
                                            logger.debug(f"No results for term '{term}' in EntryID {entry_id}")
                                            continue
                                        processed_results = await process_results(results, entry_id, brand, term, logger)
                                        valid_results = [
                                            res for res in processed_results
                                            if all(col in res for col in required_columns) and
                                            not res["ImageUrl"].startswith("placeholder://") and
                                            not any(kw in res.get("ImageDesc", "").lower() for kw in ['wallpaper', 'stock photo', 'decor'])
                                        ]
                                        if not valid_results:
                                            logger.debug(f"No valid results for term '{term}' in EntryID {entry_id}")
                                            continue

                                        # Process each result individually
                                        for result in valid_results:
                                            # Enqueue result insertion
                                            sql = """
                                                INSERT INTO utb_ImageScraperResult (EntryID, ImageUrl, ImageDesc, ImageSource, ImageUrlThumbnail)
                                                VALUES (:entry_id, :image_url, :image_desc, :image_source, :image_url_thumbnail);
                                                SELECT SCOPE_IDENTITY() AS ResultID;
                                            """
                                            params = {
                                                "entry_id": result["EntryID"],
                                                "image_url": result["ImageUrl"],
                                                "image_desc": result["ImageDesc"],
                                                "image_source": result["ImageSource"],
                                                "image_url_thumbnail": result["ImageUrlThumbnail"]
                                            }
                                            correlation_id = str(uuid.uuid4())
                                            result_id = await enqueue_db_update(
                                                file_id=str(file_id_db),
                                                sql=sql,
                                                params=params,
                                                background_tasks=background_tasks,
                                                task_type="insert_result",
                                                producer=producer,
                                                correlation_id=correlation_id,
                                                return_result=True
                                            )
                                            logger.debug(f"Enqueued result for EntryID {entry_id}, CorrelationID: {correlation_id}")

                                            if not result_id:
                                                logger.warning(f"Failed to insert result for EntryID {entry_id}")
                                                continue

                                            # Run AI analysis immediately
                                            logger.debug(f"Running AI analysis for ResultID {result_id}, EntryID {entry_id}")
                                            ai_result = await batch_vision_reason(
                                                file_id=str(file_id_db),
                                                entry_ids=[entry_id],
                                                step=0,
                                                limit=1,  # One result at a time
                                                concurrency=1,  # Minimize overhead
                                                logger=logger,
                                                background_tasks=background_tasks
                                            )
                                            if ai_result["status_code"] != 200:
                                                logger.warning(f"AI analysis failed for EntryID {entry_id}: {ai_result['message']}")
                                                continue

                                            # Check relevance score
                                            async with async_engine.connect() as conn:
                                                result = await conn.execute(
                                                    text("SELECT AiJson FROM utb_ImageScraperResult WHERE ResultID = :result_id"),
                                                    {"result_id": result_id}
                                                )
                                                row = result.fetchone()
                                                result.close()
                                                if row and row[0]:
                                                    try:
                                                        ai_data = json.loads(row[0])
                                                        relevance = float(ai_data.get("scores", {}).get("relevance", 0.0))
                                                        logger.debug(f"Relevance score for ResultID {result_id}: {relevance}")
                                                        if relevance >= RELEVANCE_THRESHOLD:
                                                            logger.info(f"High-relevance image (score: {relevance}) for ResultID {result_id}, EntryID {entry_id}")
                                                            # Enqueue SortOrder update
                                                            sql = """
                                                                UPDATE utb_ImageScraperResult
                                                                SET SortOrder = 1
                                                                WHERE ResultID = :result_id
                                                            """
                                                            params = {"result_id": result_id}
                                                            await enqueue_db_update(
                                                                file_id=str(file_id_db),
                                                                sql=sql,
                                                                params=params,
                                                                background_tasks=background_tasks,
                                                                task_type="update_sort_order",
                                                                producer=producer
                                                            )
                                                            # Enqueue Step1 update
                                                            sql = "UPDATE utb_ImageScraperRecords SET Step1 = GETDATE() WHERE EntryID = :entry_id"
                                                            params = {"entry_id": entry_id}
                                                            await enqueue_db_update(
                                                                file_id=str(file_id_db),
                                                                sql=sql,
                                                                params=params,
                                                                background_tasks=background_tasks,
                                                                task_type="update_step1",
                                                                producer=producer
                                                            )
                                                            return entry_id, True
                                                    except (json.JSONDecodeError, ValueError) as e:
                                                        logger.debug(f"Invalid AiJson for ResultID {result_id}: {e}")
                                                        continue

                                        # If no threshold match, continue to next term (unless use_all_variations)
                                        if not use_all_variations:
                                            logger.debug(f"No threshold match for term '{term}' in EntryID {entry_id}, stopping")
                                            break

                                    # Update sort order for non-threshold results
                                    sort_results = await update_search_sort_order(
                                        file_id=str(file_id_db),
                                        entry_id=str(entry_id),
                                        brand=brand,
                                        model=search_string,
                                        color=color,
                                        category=category,
                                        logger=logger,
                                        brand_rules=brand_rules,
                                        background_tasks=background_tasks
                                    )
                                    if sort_results:
                                        logger.debug(f"Sort order updated for EntryID {entry_id}")
                                        # Enqueue Step1 update
                                        sql = "UPDATE utb_ImageScraperRecords SET Step1 = GETDATE() WHERE EntryID = :entry_id"
                                        params = {"entry_id": entry_id}
                                        await enqueue_db_update(
                                            file_id=str(file_id_db),
                                            sql=sql,
                                            params=params,
                                            background_tasks=background_tasks,
                                            task_type="update_step1",
                                            producer=producer
                                        )
                                        return entry_id, True
                                finally:
                                    await client.close()
                            except Exception as e:
                                logger.debug(f"Error processing EntryID {entry_id} on attempt {attempt}: {e}")
                                if attempt < MAX_ENTRY_RETRIES:
                                    await asyncio.sleep(2 ** attempt)
                                continue
                        # All retries failed
                        logger.warning(f"Failed to process EntryID {entry_id} after {MAX_ENTRY_RETRIES} attempts")
                        sql = "UPDATE utb_ImageScraperRecords SET Step1 = NULL WHERE EntryID = :entry_id"
                        params = {"entry_id": entry_id}
                        await enqueue_db_update(
                            file_id=str(file_id_db),
                            sql=sql,
                            params=params,
                            background_tasks=background_tasks,
                            task_type="reset_step1_failed",
                            producer=producer
                        )
                        return entry_id, False
                    except Exception as e:
                        logger.warning(f"Unexpected error processing EntryID {entry_id}: {e}")
                        return entry_id, False

            for batch_idx, batch_entries in enumerate(entry_batches, 1):
                logger.info(f"Processing batch {batch_idx}/{len(entry_batches)}")
                start_time = datetime.datetime.now()

                # Process batch
                results = await asyncio.gather(
                    *(process_entry(entry) for entry in batch_entries),
                    return_exceptions=True
                )

                for entry, result in zip(batch_entries, results):
                    entry_id = entry[0]
                    if isinstance(result, Exception):
                        logger.warning(f"Error processing EntryID {entry_id}: {result}")
                        failed_entries += 1
                        continue
                    entry_id_result, success = result
                    if success:
                        successful_entries += 1
                        last_entry_id_processed = entry_id
                    else:
                        failed_entries += 1

                # Check remaining entries
                async with async_engine.connect() as conn:
                    result = await conn.execute(
                        text("""
                            SELECT COUNT(*) 
                            FROM utb_ImageScraperRecords 
                            WHERE FileID = :file_id AND Step1 IS NULL
                        """),
                        {"file_id": file_id_db_int}
                    )
                    remaining_entries = result.fetchone()[0]
                    result.close()
                    if remaining_entries == 0:
                        logger.info(f"All entries processed for FileID {file_id_db}")
                        break

                elapsed_time = (datetime.datetime.now() - start_time).total_seconds()
                logger.info(f"Completed batch {batch_idx} in {elapsed_time:.2f}s ({successful_entries / elapsed_time:.2f} entries/s)")
                log_memory_usage()

            # Update ImageCompleteTime
            if successful_entries > 0:
                logger.info(f"Updating ImageCompleteTime for FileID {file_id_db}")
                sql = """
                    UPDATE utb_ImageScraperFiles
                    SET ImageCompleteTime = GETDATE()
                    WHERE ID = :file_id
                """
                params = {"file_id": file_id_db_int}
                await enqueue_db_update(
                    file_id=str(file_id_db),
                    sql=sql,
                    params=params,
                    background_tasks=background_tasks,
                    task_type="update_file_image_complete_time",
                    producer=producer
                )

            # Verify results
            async with async_engine.connect() as conn:
                result = await conn.execute(
                    text("""
                        SELECT COUNT(DISTINCT t.EntryID), 
                               SUM(CASE WHEN t.SortOrder > 0 THEN 1 ELSE 0 END) AS positive_count,
                               SUM(CASE WHEN t.SortOrder IS NULL THEN 1 ELSE 0 END) AS null_count
                        FROM utb_ImageScraperResult t
                        INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                        WHERE r.FileID = :file_id
                    """),
                    {"file_id": file_id_db_int}
                )
                row = result.fetchone()
                total_entries = row[0] if row else 0
                positive_entries = row[1] if row and row[1] is not None else 0
                null_entries = row[2] if row and row[2] is not None else 0
                logger.info(
                    f"Verification: {total_entries} total entries, "
                    f"{positive_entries} with positive SortOrder, {null_entries} with NULL SortOrder"
                )
                result.close()

            # Send email notification
            to_emails = await get_send_to_email(file_id_db, logger=logger)
            if to_emails:
                subject = f"Processing Completed for FileID: {file_id_db}"
                message = (
                    f"Processing for FileID {file_id_db} completed.\n"
                    f"Successful entries: {successful_entries}/{len(entries)}\n"
                    f"Failed entries: {failed_entries}\n"
                    f"Last EntryID: {last_entry_id_processed}\n"
                    f"Log file: {log_filename}\n"
                    f"Used all variations: {use_all_variations}"
                )
                await send_message_email(to_emails, subject=subject, message=message, logger=logger)

            # Upload log
            log_public_url = await upload_file_to_space(
                file_src=log_filename,
                save_as=f"job_logs/job_{file_id_db}.log",
                is_public=True,
                logger=logger,
                file_id=str(file_id_db)
            )

            # Run final sort
            logger.info(f"Starting search sort for FileID: {file_id_db}")
            try:
                sort_result = await update_sort_order(str(file_id_db), logger=logger)
                logger.info(f"Search sort completed for FileID {file_id_db}. Result: {sort_result}")
            except Exception as e:
                logger.error(f"Error running search sort for FileID {file_id_db}: {e}")
                sort_result = {"status_code": 500, "message": str(e)}

            # Queue file generation
            logger.info(f"Queuing download file generation for FileID: {file_id_db}")
            try:
                if background_tasks is None:
                    background_tasks = BackgroundTasks()
                await run_generate_download_file(str(file_id_db), logger, log_filename, background_tasks)
                logger.info(f"Download file generation queued for FileID {file_id_db}")
                if to_emails:
                    subject = f"File Generation Queued for FileID: {file_id_db}"
                    message = (
                        f"Excel file generation for FileID {file_id_db} has been queued.\n"
                        f"Batch processing results:\n"
                        f"Successful entries: {successful_entries}/{len(entries)}\n"
                        f"Failed entries: {failed_entries}\n"
                        f"Last EntryID: {last_entry_id_processed}\n"
                        f"Log file: {log_filename}\n"
                        f"Log URL: {log_public_url or 'Not available'}\n"
                        f"Used all variations: {use_all_variations}"
                    )
                    await send_message_email(to_emails, subject=subject, message=message, logger=logger)
            except Exception as e:
                logger.error(f"Error queuing download file generation for FileID {file_id_db}: {e}")
                if to_emails:
                    subject = f"File Generation Failed for FileID: {file_id_db}"
                    message = (
                        f"Excel file generation for FileID {file_id_db} failed.\n"
                        f"Error: {str(e)}\n"
                        f"Batch processing results:\n"
                        f"Successful entries: {successful_entries}/{len(entries)}\n"
                        f"Failed entries: {failed_entries}\n"
                        f"Last EntryID: {last_entry_id_processed}\n"
                        f"Search sort status: {'Success' if sort_result.get('status_code') == 200 else 'Failed'}\n"
                        f"Log file: {log_filename}\n"
                        f"Log URL: {log_public_url or 'Not available'}\n"
                        f"Used all variations: {use_all_variations}"
                    )
                    await send_message_email(to_emails, subject=subject, message=message, logger=logger)

            return {
                "message": "Search processing completed",
                "file_id": str(file_id_db),
                "successful_entries": str(successful_entries),
                "total_entries": str(len(entries)),
                "failed_entries": str(failed_entries),
                "log_filename": log_filename,
                "log_public_url": log_public_url or "",
                "last_entry_id": str(last_entry_id_processed),
                "use_all_variations": str(use_all_variations)
            }
        finally:
            producer.close()
            logger.info(f"Closed RabbitMQ producer for FileID {file_id_db}")
    except Exception as e:
        logger.error(f"Error processing FileID {file_id_db}: {e}")
        log_public_url = await upload_file_to_space(
            file_src=log_filename,
            save_as=f"job_logs/job_{file_id_db}.log",
            is_public=True,
            logger=logger,
            file_id=str(file_id_db)
        )
        return {"error": str(e), "log_filename": log_filename, "log_public_url": log_public_url or "", "last_entry_id": str(entry_id or "")}
    finally:
        await async_engine.dispose()
        logger.info(f"Disposed database engines")

        
@router.get("/sort-by-search/{file_id}", tags=["Sorting"])
async def api_match_and_search_sort(file_id: str):
    logger, log_filename = setup_job_logger(job_id=file_id, console_output=True)
    result = await run_job_with_logging(update_sort_order, file_id)
    if result["status_code"] != 200:
        log_public_url = await upload_log_file(file_id, log_filename, logger)
        raise HTTPException(status_code=result["status_code"], detail=result["message"])
    return result

@router.get("/initial-sort/{file_id}", tags=["Sorting"])
async def api_initial_sort(file_id: str):
    logger, log_filename = setup_job_logger(job_id=file_id, console_output=True)
    result = await run_job_with_logging(update_initial_sort_order, file_id)
    if result["status_code"] != 200:
        log_public_url = await upload_log_file(file_id, log_filename, logger)
        raise HTTPException(status_code=result["status_code"], detail=result["message"])
    return result

@router.get("/no-image-sort/{file_id}", tags=["Sorting"])
async def api_no_image_sort(file_id: str):
    logger, log_filename = setup_job_logger(job_id=file_id, console_output=True)
    result = await run_job_with_logging(update_sort_no_image_entry, file_id)
    if result["status_code"] != 200:
        log_public_url = await upload_log_file(file_id, log_filename, logger)
        raise HTTPException(status_code=result["status_code"], detail=result["message"])
    return result

@router.post("/restart-job/{file_id}", tags=["Processing"])
async def api_process_restart(file_id: str, entry_id: Optional[int] = None, background_tasks: BackgroundTasks = None):
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Queueing restart of batch for FileID: {file_id}" + (f", EntryID: {entry_id}" if entry_id else ""))
    try:
        if not entry_id:
            entry_id = await fetch_last_valid_entry(file_id, logger)
            logger.info(f"Retrieved last EntryID: {entry_id} for FileID: {file_id}")
        
        result = await process_restart_batch(
            file_id_db=int(file_id),
            logger=logger,
            entry_id=entry_id
        )
        if "error" in result:
            logger.error(f"Failed to process restart batch for FileID {file_id}: {result['error']}")
            log_public_url = await upload_log_file(file_id, log_filename, logger)
            raise HTTPException(status_code=500, detail=result["error"])
        
        logger.info(f"Completed restart batch for FileID: {file_id}. Result: {result}")
        return {"status_code": 200, "message": f"Processing restart completed for FileID: {file_id}", "data": result}
    except Exception as e:
        logger.error(f"Error queuing restart batch for FileID {file_id}: {e}", exc_info=True)
        log_public_url = await upload_log_file(file_id, log_filename, logger)
        raise HTTPException(status_code=500, detail=f"Error restarting batch for FileID {file_id}: {str(e)}")

@router.post("/restart-search-all/{file_id}", tags=["Processing"])
async def api_restart_search_all(
    file_id: str,
    entry_id: Optional[int] = None,
    background_tasks: BackgroundTasks = None
):
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Queueing restart of batch for FileID: {file_id}" + (f", EntryID: {entry_id}" if entry_id else "") + " with all variations")
    
    try:
        if not entry_id:
            entry_id = await fetch_last_valid_entry(file_id, logger)
            logger.info(f"Retrieved last EntryID: {entry_id} for FileID: {file_id}")
        
        result = await run_job_with_logging(
            process_restart_batch,
            file_id,
            entry_id=entry_id,
            use_all_variations=True
        )
        
        if result["status_code"] != 200:
            logger.error(f"Failed to process restart batch for FileID {file_id}: {result['message']}")
            if "placeholder://error" in result["message"]:
                logger.warning(f"Placeholder error detected; check endpoint logs for FileID {file_id}")
                debug_info = result.get("debug_info", {})
                endpoint_errors = debug_info.get("endpoint_errors", [])
                for error in endpoint_errors:
                    logger.error(f"Endpoint error: {error['error']} at {error['timestamp']}")
            log_public_url = await upload_log_file(file_id, log_filename, logger)
            raise HTTPException(status_code=result["status_code"], detail=result["message"])
        
        logger.info(f"Completed restart batch for FileID: {file_id}")
        return {
            "status": "success",
            "status_code": 200,
            "message": f"Processing restart with all variations completed for FileID: {file_id}",
            "data": result["data"]
        }
    except Exception as e:
        logger.error(f"Error queuing restart batch for FileID {file_id}: {e}", exc_info=True)
        log_public_url = await upload_log_file(file_id, log_filename, logger)
        raise HTTPException(status_code=500, detail=f"Error restarting batch with all variations for FileID {file_id}: {str(e)}")

@router.post("/process-images-ai/{file_id}", tags=["Processing"])
async def api_process_ai_images(
    file_id: str,
    entry_ids: Optional[List[int]] = Query(None, description="List of EntryIDs to process"),
    step: int = Query(0, description="Retry step for logging"),
    limit: int = Query(5000, description="Maximum number of images to process"),
    concurrency: int = Query(10, description="Maximum concurrent threads"),
    background_tasks: BackgroundTasks = None
):
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Queueing AI image processing for FileID: {file_id}, EntryIDs: {entry_ids}, Step: {step}")
    
    try:
        result = await run_job_with_logging(
            batch_vision_reason,
            file_id,
            entry_ids=entry_ids,
            step=step,
            limit=limit,
            concurrency=concurrency
            # Removed logger=logger to prevent duplicate argument
        )
        
        if result["status_code"] != 200:
            logger.error(f"Failed to process AI images for FileID {file_id}: {result['message']}")
            log_public_url = await upload_log_file(file_id, log_filename, logger)
            raise HTTPException(status_code=result["status_code"], detail=result["message"])
        
        logger.info(f"Completed AI image processing for FileID {file_id}")
        return {
            "status": "success",
            "status_code": 200,
            "message": f"AI image processing completed for FileID: {file_id}",
            "data": result["data"]
        }
    except Exception as e:
        logger.error(f"Error queuing AI image processing for FileID {file_id}: {e}", exc_info=True)
        log_public_url = await upload_log_file(file_id, log_filename, logger)
        raise HTTPException(status_code=500, detail=f"Error processing AI images for FileID {file_id}: {str(e)}")
    
@router.get("/get-images-excel-db/{file_id}", tags=["Database"])
async def get_images_excel_db_endpoint(file_id: str):
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Fetching Excel images for FileID: {file_id}")
    try:
        result = await get_images_excel_db(file_id, logger)
        if result.empty:
            return {"status_code": 200, "message": f"No images found for Excel export for FileID: {file_id}", "data": []}
        return {"status_code": 200, "message": f"Fetched Excel images successfully for FileID: {file_id}", "data": result.to_dict(orient='records')}
    except Exception as e:
        logger.error(f"Error fetching Excel images for FileID {file_id}: {e}", exc_info=True)
        log_public_url = await upload_log_file(file_id, log_filename, logger)
        raise HTTPException(status_code=500, detail=f"Error fetching Excel images for FileID {file_id}: {str(e)}")

@router.get("/get-send-to-email/{file_id}", tags=["Database"])
async def get_send_to_email_endpoint(file_id: str):
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Retrieving email for FileID: {file_id}")
    try:
        result = await get_send_to_email(int(file_id), logger)
        return {"status_code": 200, "message": f"Retrieved email successfully for FileID: {file_id}", "data": result}
    except Exception as e:
        logger.error(f"Error retrieving email for FileID {file_id}: {e}", exc_info=True)
        log_public_url = await upload_log_file(file_id, log_filename, logger)
        raise HTTPException(status_code=500, detail=f"Error retrieving email for FileID {file_id}: {str(e)}")

@router.post("/update-file-generate-complete/{file_id}", tags=["Database"])
async def update_file_generate_complete_endpoint(file_id: str):
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Updating file generate complete for FileID: {file_id}")
    try:
        await update_file_generate_complete(file_id, logger)
        return {"status_code": 200, "message": f"Updated file generate complete successfully for FileID: {file_id}", "data": None}
    except Exception as e:
        logger.error(f"Error updating file generate complete for FileID {file_id}: {e}", exc_info=True)
        log_public_url = await upload_log_file(file_id, log_filename, logger)
        raise HTTPException(status_code=500, detail=f"Error updating file generate complete for FileID {file_id}: {str(e)}")

@router.post("/update-file-location-complete/{file_id}", tags=["Database"])
async def update_file_location_complete_endpoint(file_id: str, file_location: str):
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Updating file location complete for FileID: {file_id}, file_location: {file_location}")
    try:
        await update_file_location_complete(file_id, file_location, logger)
        return {"status_code": 200, "message": f"Updated file location successfully for FileID: {file_id}", "data": None}
    except Exception as e:
        logger.error(f"Error updating file location for FileID {file_id}: {e}", exc_info=True)
        log_public_url = await upload_log_file(file_id, log_filename, logger)
        raise HTTPException(status_code=500, detail=f"Error updating file location for FileID {file_id}: {str(e)}")
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, APIRouter
from sqlalchemy.sql import text
import json

# Add to existing router
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, APIRouter
from sqlalchemy.sql import text
import json
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, APIRouter
from sqlalchemy.sql import text
import json
from collections import defaultdict

# Add to existing router in api.py
@router.post("/sort-by-relevance/{file_id}", tags=["Sorting"])
async def api_sort_by_relevance(
    file_id: str,
    entry_ids: Optional[List[int]] = Query(None, description="List of EntryIDs to sort, if not all"),
    background_tasks: BackgroundTasks = None
):
    """
    Sort results in utb_ImageScraperResult by relevance score from AiJson for a given FileID,
    grouping by EntryID to assign consecutive SortOrder (1, 2, 3, ...) within each EntryID.
    Only updates records with a non-NULL AiJson field.
    """
    logger, log_filename = setup_job_logger(job_id=file_id, console_output=True)
    logger.info(f"Sorting results by relevance per EntryID for FileID: {file_id}, EntryIDs: {entry_ids}")

    try:
        # Validate FileID exists
        async with async_engine.connect() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) FROM utb_ImageScraperFiles WHERE ID = :file_id"),
                {"file_id": int(file_id)}
            )
            if result.fetchone()[0] == 0:
                logger.error(f"FileID {file_id} does not exist")
                log_public_url = await upload_log_file(file_id, log_filename, logger)
                raise HTTPException(status_code=404, detail=f"FileID {file_id} not found")
            result.close()

        # Fetch results with non-NULL AiJson
        query = """
            SELECT ResultID, EntryID, AiJson
            FROM utb_ImageScraperResult
            WHERE EntryID IN (
                SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = :file_id
            )
            AND AiJson IS NOT NULL
        """
        params = {"file_id": int(file_id)}
        if entry_ids:
            query += " AND EntryID IN :entry_ids"
            params["entry_ids"] = tuple(entry_ids)

        async with async_engine.connect() as conn:
            result = await conn.execute(text(query), params)
            rows = result.fetchall()
            result.close()

        if not rows:
            logger.warning(
                f"No results with non-NULL AiJson found for FileID {file_id}" +
                (f", EntryIDs {entry_ids}" if entry_ids else "")
            )
            log_public_url = await upload_log_file(file_id, log_filename, logger)
            return {
                "status": "success",
                "status_code": 200,
                "message": "No results with AiJson found to sort",
                "log_url": log_public_url
            }

        # Group results by EntryID
        results_by_entry = defaultdict(list)
        for row in rows:
            result_id, entry_id, ai_json = row
            try:
                ai_data = json.loads(ai_json)
                relevance = float(ai_data.get("scores", {}).get("relevance", 0.0))
                results_by_entry[entry_id].append({
                    "ResultID": result_id,
                    "relevance": relevance
                })
            except (json.JSONDecodeError, ValueError) as e:
                logger.warning(f"Invalid AiJson for ResultID {result_id}, EntryID {entry_id}: {e}")
                # Skip records with invalid AiJson
                continue

        if not results_by_entry:
            logger.warning(f"No valid AiJson data found for sorting in FileID {file_id}")
            log_public_url = await upload_log_file(file_id, log_filename, logger)
            return {
                "status": "success",
                "status_code": 200,
                "message": "No valid AiJson data found to sort",
                "log_url": log_public_url
            }

        # Sort and assign SortOrder per EntryID
        updates = []
        for entry_id, results in results_by_entry.items():
            # Sort by relevance (descending), then by ResultID (ascending) for stable sorting
            results.sort(key=lambda x: (x["relevance"], -x["ResultID"]), reverse=True)
            # Assign consecutive SortOrder (1, 2, 3, ...)
            for sort_order, result in enumerate(results, 1):
                updates.append({
                    "result_id": result["ResultID"],
                    "sort_order": sort_order,
                    "entry_id": entry_id
                })
            logger.debug(f"Assigned SortOrder for {len(results)} results in EntryID {entry_id}")

        # Update SortOrder in database
        async with async_engine.connect() as conn:
            for update in updates:
                await conn.execute(
                    text("""
                        UPDATE utb_ImageScraperResult
                        SET SortOrder = :sort_order
                        WHERE ResultID = :result_id
                    """),
                    {
                        "sort_order": update["sort_order"],
                        "result_id": update["result_id"]
                    }
                )
            await conn.commit()
            logger.info(f"Updated SortOrder for {len(updates)} results across {len(results_by_entry)} EntryIDs for FileID {file_id}")

        # Enqueue verification task
        if background_tasks:
            sql = """
                SELECT EntryID, COUNT(*) 
                FROM utb_ImageScraperResult 
                WHERE EntryID IN (
                    SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = :file_id
                ) 
                AND AiJson IS NOT NULL 
                AND SortOrder IS NOT NULL
                GROUP BY EntryID
            """
            params = {"file_id": int(file_id)}
            await enqueue_db_update(
                file_id=file_id,
                sql=sql,
                params=params,
                background_tasks=background_tasks,
                task_type="verify_sort_order",
                producer=RabbitMQProducer()
            )
            logger.info(f"Enqueued SortOrder verification for FileID {file_id}")

        log_public_url = await upload_log_file(file_id, log_filename, logger)
        return {
            "status": "success",
            "status_code": 200,
            "message": f"Sorted {len(updates)} results with AiJson by relevance across {len(results_by_entry)} EntryIDs for FileID {file_id}",
            "log_url": log_public_url,
            "data": {
                "sorted_results": [
                    {"ResultID": u["result_id"], "EntryID": u["entry_id"], "SortOrder": u["sort_order"]}
                    for u in updates
                ]
            }
        }

    except Exception as e:
        logger.error(f"Error sorting by relevance for FileID {file_id}: {e}", exc_info=True)
        log_public_url = await upload_log_file(file_id, log_filename, logger)
        raise HTTPException(status_code=500, detail=f"Error sorting by relevance: {str(e)}")

@router.post("/reset-step1/{file_id}", tags=["Database"])
async def api_reset_step1(file_id: str, background_tasks: BackgroundTasks):
    logger, log_filename = setup_job_logger(job_id=file_id, console_output=True)
    logger.info(f"Resetting Step1 for FileID: {file_id}")
    
    try:
        # Validate FileID exists
        async with async_engine.connect() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) FROM utb_ImageScraperFiles WHERE ID = :file_id"),
                {"file_id": int(file_id)}
            )
            if result.fetchone()[0] == 0:
                logger.error(f"FileID {file_id} does not exist")
                log_public_url = await upload_log_file(file_id, log_filename, logger)
                raise HTTPException(status_code=404, detail=f"FileID {file_id} not found")
            result.close()

        # Enqueue Step1 reset
        sql = """
            UPDATE utb_ImageScraperRecords
            SET Step1 = NULL
            WHERE FileID = :file_id
        """
        params = {"file_id": int(file_id)}
        await enqueue_db_update(
            file_id=file_id,
            sql=sql,
            params=params,
            background_tasks=background_tasks,
            task_type="reset_step1",
        )
        logger.info(f"Enqueued Step1 reset for FileID: {file_id}")

        # Note: Verification is tricky since the update is queued and not immediately applied.
        # We can skip verification or implement a separate mechanism to check queue processing status.
        log_public_url = await upload_log_file(file_id, log_filename, logger)
        
        return {
            "status_code": 200,
            "message": f"Successfully enqueued Step1 reset for FileID: {file_id}",
            "log_url": log_public_url or None,
            "timestamp": datetime.datetime.now().isoformat()
        }
    
    except Exception as e:
        logger.error(f"Error enqueuing Step1 reset for FileID {file_id}: {e}", exc_info=True)
        log_public_url = await upload_log_file(file_id, log_filename, logger)
        raise HTTPException(status_code=500, detail=f"Error enqueuing Step1 reset for FileID {file_id}: {str(e)}")

from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from sqlalchemy.sql import text
import httpx
from typing import Optional, List
import uuid

@router.post("/validate-images/{file_id}", tags=["Validation"])
async def api_validate_images(
    file_id: str,
    entry_ids: Optional[List[int]] = Query(None, description="List of EntryIDs to validate, if not all"),
    concurrency: int = Query(10, description="Maximum concurrent image download tasks"),
    background_tasks: BackgroundTasks = None
):
    """
    Validates images in utb_ImageScraperResult for a given FileID by attempting to download them.
    If the main ImageUrl fails, tries ImageUrlThumbnail. If both fail, sets SortOrder to -5 (invalid).
    Only processes results with non-NULL ImageUrl and SortOrder >= 0.
    """
    logger, log_filename = setup_job_logger(job_id=file_id, console_output=True)
    logger.info(f"Validating images for FileID: {file_id}, EntryIDs: {entry_ids}, Concurrency: {concurrency}")

    try:
        # Validate FileID exists
        async with async_engine.connect() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) FROM utb_ImageScraperFiles WHERE ID = :file_id"),
                {"file_id": int(file_id)}
            )
            if result.fetchone()[0] == 0:
                logger.error(f"FileID {file_id} does not exist")
                log_public_url = await upload_log_file(file_id, log_filename, logger)
                raise HTTPException(status_code=404, detail=f"FileID {file_id} not found")
            result.close()

        # Fetch results to validate (non-NULL ImageUrl, SortOrder >= 0)
        query = """
            SELECT ResultID, EntryID, ImageUrl, ImageUrlThumbnail
            FROM utb_ImageScraperResult
            WHERE EntryID IN (
                SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = :file_id
            )
            AND ImageUrl IS NOT NULL
            AND SortOrder >= 0
        """
        params = {"file_id": int(file_id)}
        if entry_ids:
            query += " AND EntryID IN :entry_ids"
            params["entry_ids"] = tuple(entry_ids)

        async with async_engine.connect() as conn:
            result = await conn.execute(text(query), params)
            results = [
                {
                    "ResultID": row[0],
                    "EntryID": row[1],
                    "ImageUrl": row[2],
                    "ImageUrlThumbnail": row[3]
                }
                for row in result.fetchall()
            ]
            result.close()

        if not results:
            logger.warning(
                f"No valid images found for FileID {file_id}" +
                (f", EntryIDs {entry_ids}" if entry_ids else "")
            )
            log_public_url = await upload_log_file(file_id, log_filename, logger)
            return {
                "status": "success",
                "status_code": 200,
                "message": "No valid images found to validate",
                "log_url": log_public_url,
                "data": {"validated": 0, "invalid": 0}
            }

        logger.info(f"Found {len(results)} images to validate for FileID {file_id}")

        # Initialize RabbitMQ producer
        producer = RabbitMQProducer()
        try:
            # Semaphore to limit concurrency
            semaphore = asyncio.Semaphore(concurrency)

            async def validate_image(result: dict) -> dict:
                async with semaphore:
                    result_id = result["ResultID"]
                    entry_id = result["EntryID"]
                    image_url = result["ImageUrl"]
                    thumbnail_url = result["ImageUrlThumbnail"]
                    logger.debug(f"Validating ResultID {result_id}, EntryID {entry_id}, ImageUrl: {image_url}")

                    async with httpx.AsyncClient(timeout=10.0) as client:
                        # Try main image
                        is_valid = False
                        try:
                            response = await client.get(image_url, follow_redirects=True)
                            if response.status_code == 200 and "image" in response.headers.get("content-type", "").lower():
                                logger.debug(f"Valid image for ResultID {result_id}: {image_url}")
                                is_valid = True
                            else:
                                logger.warning(
                                    f"Invalid image for ResultID {result_id}: "
                                    f"Status {response.status_code}, Content-Type {response.headers.get('content-type')}"
                                )
                        except Exception as e:
                            logger.warning(f"Failed to download image for ResultID {result_id}: {e}")

                        # If main image fails, try thumbnail
                        if not is_valid and thumbnail_url and thumbnail_url != image_url:
                            try:
                                response = await client.get(thumbnail_url, follow_redirects=True)
                                if response.status_code == 200 and "image" in response.headers.get("content-type", "").lower():
                                    logger.debug(f"Valid thumbnail for ResultID {result_id}: {thumbnail_url}")
                                    is_valid = True
                                else:
                                    logger.warning(
                                        f"Invalid thumbnail for ResultID {result_id}: "
                                        f"Status {response.status_code}, Content-Type {response.headers.get('content-type')}"
                                    )
                            except Exception as e:
                                logger.warning(f"Failed to download thumbnail for ResultID {result_id}: {e}")

                        if not is_valid:
                            # Enqueue update to set SortOrder = -5
                            sql = """
                                UPDATE utb_ImageScraperResult
                                SET SortOrder = -5
                                WHERE ResultID = :result_id
                            """
                            params = {"result_id": result_id}
                            correlation_id = str(uuid.uuid4())
                            await enqueue_db_update(
                                file_id=file_id,
                                sql=sql,
                                params=params,
                                background_tasks=background_tasks,
                                task_type="mark_invalid_image",
                                producer=producer,
                                correlation_id=correlation_id  # Now supported
                            )
                            logger.info(
                                f"Enqueued SortOrder=-5 for ResultID {result_id}, "
                                f"EntryID {entry_id}, CorrelationID: {correlation_id}"
                            )
                            return {"ResultID": result_id, "EntryID": entry_id, "valid": False}
                        return {"ResultID": result_id, "EntryID": entry_id, "valid": True}

            # Process images concurrently
            validation_results = await asyncio.gather(
                *(validate_image(result) for result in results),
                return_exceptions=True
            )

            validated_count = 0
            invalid_count = 0
            for res in validation_results:
                if isinstance(res, Exception):
                    logger.error(f"Error validating image: {res}", exc_info=True)
                    invalid_count += 1
                    continue
                if res["valid"]:
                    validated_count += 1
                else:
                    invalid_count += 1

            logger.info(
                f"Validation complete for FileID {file_id}: "
                f"{validated_count} valid, {invalid_count} invalid"
            )

            # Enqueue verification task
            if background_tasks:
                sql = """
                    SELECT ResultID, EntryID, SortOrder
                    FROM utb_ImageScraperResult
                    WHERE EntryID IN (
                        SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = :file_id
                    )
                    AND SortOrder = -5
                """
                params = {"file_id": int(file_id)}
                correlation_id = str(uuid.uuid4())
                await enqueue_db_update(
                    file_id=file_id,
                    sql=sql,
                    params=params,
                    background_tasks=background_tasks,
                    task_type="verify_invalid_images",
                    producer=producer,
                    correlation_id=correlation_id  # Now supported
                )
                logger.info(f"Enqueued verification of invalid images for FileID {file_id}, CorrelationID: {correlation_id}")

            log_public_url = await upload_log_file(file_id, log_filename, logger)
            return {
                "status": "success",
                "status_code": 200,
                "message": f"Validated {len(results)} images for FileID {file_id}: {validated_count} valid, {invalid_count} invalid",
                "log_url": log_public_url,
                "data": {
                    "validated": validated_count,
                    "invalid": invalid_count,
                    "results": [
                        res for res in validation_results if not isinstance(res, Exception)
                    ]
                }
            }
        finally:
            await producer.close()  # Properly await the close coroutine
            logger.info(f"Closed RabbitMQ producer for FileID {file_id}")
    except Exception as e:
        logger.error(f"Error validating images for FileID {file_id}: {e}", exc_info=True)
        log_public_url = await upload_log_file(file_id, log_filename, logger)
        raise HTTPException(status_code=500, detail=f"Error validating images for FileID {file_id}: {str(e)}")
    finally:
        await async_engine.dispose()
        logger.info(f"Disposed database engine for FileID {file_id}")
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    default_logger.info("Starting up FastAPI application")

    # Signal handlers
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def handle_shutdown(signal_type):
        default_logger.info(f"Received {signal_type}, initiating graceful shutdown")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_shutdown, sig.name)

    try:
        yield
    finally:
        # Wait for shutdown signal or continue with cleanup
        await asyncio.wait_for(shutdown_event.wait(), timeout=None)
        default_logger.info("Shutting down FastAPI application")
        await async_engine.dispose()
        default_logger.info("Database engine disposed")

app.include_router(router, prefix="/api/v4")