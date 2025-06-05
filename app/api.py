from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, APIRouter, Request
from pydantic import BaseModel, Field
import logging
import asyncio
import os
import json
import traceback
import psutil
import datetime
import urllib.parse
import hashlib
import time
import httpx
import aiohttp
import pandas as pd
from typing import Optional, List, Dict, Any, Callable
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from contextlib import asynccontextmanager
import signal
import uuid
import math
import sys
from collections import defaultdict
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import aio_pika
from multiprocessing import cpu_count
from math import ceil

# Assume these are defined in their respective modules
from icon_image_lib.google_parser import process_search_result
from common import generate_search_variations, fetch_brand_rules, preprocess_sku # Removed unused imports
from logging_config import setup_job_logger
from s3_utils import upload_file_to_space
from ai_utils import batch_vision_reason
from db_utils import (
    update_log_url_in_db,
    get_send_to_email,
    fetch_last_valid_entry,
    insert_search_results,
    update_initial_sort_order,
    get_images_excel_db,
    enqueue_db_update, # This is the key function we've been discussing
    update_file_generate_complete,
    update_file_location_complete,
)
from search_utils import update_search_sort_order, update_sort_order, update_sort_no_image_entry
from database_config import async_engine
from config import BRAND_RULES_URL, VERSION, SEARCH_PROXY_API_URL, RABBITMQ_URL, DATAPROXY_API_KEY
from email_utils import send_message_email
from urllib.parse import urlparse
from url_extract import extract_thumbnail_url
from rabbitmq_producer import RabbitMQProducer

app = FastAPI(title="super_scraper", version=VERSION)

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

router = APIRouter()
global_producer: Optional[RabbitMQProducer] = None # Renamed from 'producer' to avoid conflict if 'producer' is used locally
JOB_STATUS = {}
LAST_UPLOAD = {}

# --- Constants (assuming these are correctly defined elsewhere or here) ---
SCRAPER_RECORDS_TABLE_NAME = "utb_ImageScraperRecords"
SCRAPER_RECORDS_PK_COLUMN = "EntryID"
SCRAPER_RECORDS_FILE_ID_FK_COLUMN = "FileID"
SCRAPER_RECORDS_EXCEL_ROW_ID_COLUMN = "ExcelRowID"
SCRAPER_RECORDS_PRODUCT_MODEL_COLUMN = "ProductModel"
SCRAPER_RECORDS_PRODUCT_BRAND_COLUMN = "ProductBrand"
SCRAPER_RECORDS_CREATE_TIME_COLUMN = "CreateTime"
SCRAPER_RECORDS_STEP1_COLUMN = "Step1"
SCRAPER_RECORDS_STEP2_COLUMN = "Step2"
SCRAPER_RECORDS_STEP3_COLUMN = "Step3"
SCRAPER_RECORDS_STEP4_COLUMN = "Step4"
SCRAPER_RECORDS_COMPLETE_TIME_COLUMN = "CompleteTime"
SCRAPER_RECORDS_PRODUCT_COLOR_COLUMN = "ProductColor"
SCRAPER_RECORDS_PRODUCT_CATEGORY_COLUMN = "ProductCategory"
SCRAPER_RECORDS_EXCEL_ROW_IMAGE_REF_COLUMN = "ExcelRowImageRef"
SCRAPER_RECORDS_ENTRY_STATUS_COLUMN = "EntryStatus"
SCRAPER_RECORDS_WAREHOUSE_MATCH_TIME_COLUMN = "WarehouseMatchTime"

IMAGE_SCRAPER_RESULT_TABLE_NAME = "utb_ImageScraperResult"
IMAGE_SCRAPER_RESULT_PK_COLUMN = "ResultID"
IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN = "EntryID"
IMAGE_SCRAPER_RESULT_IMAGE_URL_COLUMN = "ImageUrl"
IMAGE_SCRAPER_RESULT_IMAGE_DESC_COLUMN = "ImageDesc"
IMAGE_SCRAPER_RESULT_IMAGE_SOURCE_COLUMN = "ImageSource"
IMAGE_SCRAPER_RESULT_CREATE_TIME_COLUMN = "CreateTime"
IMAGE_SCRAPER_RESULT_IMAGE_URL_THUMBNAIL_COLUMN = "ImageUrlThumbnail"
IMAGE_SCRAPER_RESULT_SORT_ORDER_COLUMN = "SortOrder"
IMAGE_SCRAPER_RESULT_IMAGE_IS_FASHION_COLUMN = "ImageIsFashion"
IMAGE_SCRAPER_RESULT_AI_CAPTION_COLUMN = "AiCaption"
IMAGE_SCRAPER_RESULT_AI_JSON_COLUMN = "AiJson"
IMAGE_SCRAPER_RESULT_AI_LABEL_COLUMN = "AiLabel"
IMAGE_SCRAPER_RESULT_UPDATE_TIME_COLUMN = "UpdateTime"
IMAGE_SCRAPER_RESULT_SOURCE_TYPE_COLUMN = "SourceType"

WAREHOUSE_IMAGES_TABLE_NAME = "utb_IconWarehouseImages"
WAREHOUSE_IMAGES_PK_COLUMN = "ID"
WAREHOUSE_IMAGES_MODEL_NUMBER_COLUMN = "ModelNumber"
WAREHOUSE_IMAGES_MODEL_CLEAN_COLUMN = "ModelClean"
WAREHOUSE_IMAGES_CREATE_DATE_COLUMN = "CreateDate"
WAREHOUSE_IMAGES_UPDATE_DATE_COLUMN = "UpdateDate"
WAREHOUSE_IMAGES_MODEL_IMAGE_COLUMN = "ModelImage"
WAREHOUSE_IMAGES_MODEL_SOURCE_COLUMN = "ModelSource"
WAREHOUSE_IMAGES_MODEL_FOLDER_COLUMN = "ModelFolder"

IMAGE_SCRAPER_FILES_TABLE_NAME = "utb_ImageScraperFiles"
IMAGE_SCRAPER_FILES_PK_COLUMN = "ID"
IMAGE_SCRAPER_FILES_LOG_FILE_URL_COLUMN = "LogFileUrl"
IMAGE_SCRAPER_FILES_LOG_UPLOAD_TIME_COLUMN = "LogUploadTime"
IMAGE_SCRAPER_FILES_IMAGE_COMPLETE_TIME_COLUMN = "ImageCompleteTime"

STATUS_PENDING_WAREHOUSE_CHECK = 0
STATUS_WAREHOUSE_CHECK_NO_MATCH = 1
STATUS_WAREHOUSE_RESULT_POPULATED = 2
STATUS_PENDING_GOOGLE_SEARCH = 3
STATUS_GOOGLE_SEARCH_COMPLETE = 4
# --- End Constants ---


@asynccontextmanager
async def lifespan(app: FastAPI):
    default_logger.info("Starting up FastAPI application")
    global global_producer # Use the renamed global variable

    try:
        async with asyncio.timeout(20):
            # Assign to the global variable
            global_producer = await RabbitMQProducer.get_producer()
        if not global_producer or not global_producer.is_connected:
            default_logger.error("Failed to initialize global RabbitMQ producer, shutting down")
            sys.exit(1) # Consider raising an exception instead of sys.exit for better testability
        default_logger.info("Global RabbitMQ producer initialized")

        # Run test insertion
        default_logger.info("Running test insertion of search result on startup")
        test_file_id = "test_file_123" # Renamed for clarity
        logger, log_filename = setup_job_logger(job_id=test_file_id, console_output=True)
        try:
            sample_result = [
                {
                    "EntryID": 9999,
                    "ImageUrl": "https://example.com/test_image.jpg",
                    "ImageDesc": "Test image description",
                    "ImageSource": "https://example.com", # Made it a valid URL
                    "ImageUrlThumbnail": "https://example.com/test_thumbnail.jpg"
                }
            ]
            background_tasks = BackgroundTasks() # Create a local instance
            # insert_search_results is assumed to use enqueue_db_update internally,
            # which should get the global_producer if no instance is passed.
            success = await insert_search_results(
                results=sample_result,
                logger=logger,
                file_id=test_file_id,
                background_tasks=background_tasks
            )
            if success:
                logger.info(f"Test search result enqueued/inserted successfully for EntryID 9999, FileID {test_file_id}")
            else:
                logger.error(f"Failed to enqueue/insert test search result for EntryID 9999, FileID {test_file_id}")

            # Verification (consider doing this after a short delay if relying on async processing)
            # For immediate check, this might reflect an empty state if consumer hasn't run.
            # This is more of an integration test.
            async with async_engine.connect() as conn:
                result = await conn.execute(
                    text("SELECT COUNT(*) FROM utb_ImageScraperResult WHERE EntryID = :entry_id"),
                    {"entry_id": 9999}
                )
                count_obj = result.one_or_none() # Use one_or_none for safety
                count = count_obj[0] if count_obj else 0
                result.close()
                if count > 0:
                    logger.info(f"Verification: Found {count} record(s) for EntryID 9999")
                else:
                    logger.warning(f"Verification: No records found for EntryID 9999 (this might be okay if consumer is async)")
        except Exception as e:
            logger.error(f"Error during test insertion: {e}", exc_info=True)
            # Consider not exiting if test fails, but log it as a critical startup issue
            # sys.exit(1) # Commented out to allow app to start even if test fails

        # Set up shutdown handling
        loop = asyncio.get_running_loop()
        shutdown_event = asyncio.Event()

        def handle_shutdown_signal(signal_type_name: str): # Renamed param for clarity
            default_logger.info(f"Received {signal_type_name}, initiating graceful shutdown")
            shutdown_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            # Pass sig.name for logging clarity
            loop.add_signal_handler(sig, handle_shutdown_signal, sig.name)

        yield # FastAPI app runs here

    finally:
        default_logger.info("Shutting down FastAPI application")
        if global_producer and global_producer.is_connected:
            await global_producer.close()
            default_logger.info("Global RabbitMQ producer closed")
        if async_engine: # Check if engine exists before disposing
            await async_engine.dispose()
            default_logger.info("Database engine disposed")

app.lifespan = lifespan # Assign the lifespan context manager

class SearchClient:
    def __init__(self, endpoint: str, logger: logging.Logger, max_concurrency: int = 10):
        self.endpoint = endpoint
        self.logger = logger
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.api_key = DATAPROXY_API_KEY
        self.headers = {
            "accept": "application/json",
            "x-api-key": self.api_key,
            "Content-Type": "application/json"
        }
        self.regions = ['northamerica-northeast', 'us-east', 'southamerica', 'us-central', 'us-west', 'europe', 'australia', 'asia', 'middle-east']
        self._session: Optional[aiohttp.ClientSession] = None # For managing session lifecycle

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(headers=self.headers)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None # Clear the session
            self.logger.debug("SearchClient aiohttp session closed.")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, json.JSONDecodeError, asyncio.TimeoutError)) # Added TimeoutError
    )
    async def search(self, term: str, brand: str, entry_id: int) -> List[Dict]:
        async with self.semaphore:
            process = psutil.Process() # Consider if needed per search or per client init
            search_url = f"https://www.google.com/search?q={urllib.parse.quote(term)}&tbm=isch"
            session = await self._get_session() # Get or create session

            for region_idx, region in enumerate(self.regions):
                fetch_endpoint = f"{self.endpoint}?region={region}"
                self.logger.info(f"Worker PID {process.pid}: Fetching '{term}' (EntryID {entry_id}) via {fetch_endpoint} (region {region}, attempt {region_idx+1}/{len(self.regions)})")
                try:
                    async with asyncio.timeout(60): # Apply timeout to the request
                        async with session.post(fetch_endpoint, json={"url": search_url}) as response: # Removed timeout from session.post as it's handled by asyncio.timeout
                            body_text = await response.text()
                            body_preview = body_text[:200] if body_text else ""
                            self.logger.debug(f"Worker PID {process.pid}: Response from {fetch_endpoint}: status={response.status}, body_preview='{body_preview}'")

                            if response.status in (429, 503):
                                self.logger.warning(f"Worker PID {process.pid}: Rate limit or service unavailable (status {response.status}) for {fetch_endpoint} on term '{term}'. Retrying with next region or failing.")
                                if region_idx == len(self.regions) - 1: # Last region
                                    raise aiohttp.ClientError(f"Rate limit or service unavailable after trying all regions: {response.status}")
                                await asyncio.sleep(1 + region_idx) # Exponential backoff for retrying regions
                                continue # Try next region

                            response.raise_for_status() # Raises for 4xx/5xx other than 429/503 handled above
                            
                            # Try to parse JSON, handle potential non-JSON response
                            try:
                                result_json = await response.json()
                            except json.JSONDecodeError as json_err:
                                self.logger.error(f"Worker PID {process.pid}: JSONDecodeError for term '{term}' in region {region}. Body: {body_preview}. Error: {json_err}")
                                if region_idx == len(self.regions) - 1:
                                    raise # Re-raise if last region
                                continue # Try next region

                            api_response_content = result_json.get("result") # Changed from "results" to "result" based on typical API patterns
                            
                            if not api_response_content:
                                self.logger.warning(f"Worker PID {process.pid}: 'result' field missing or empty in API response for term '{term}' in region {region}. Full response: {result_json}")
                                if region_idx == len(self.regions) - 1:
                                    continue # Allow to fall through to "All regions failed" if last region
                                continue # Try next region

                            # Assuming process_search_result expects bytes
                            results_html_bytes = api_response_content.encode("utf-8") if isinstance(api_response_content, str) else str(api_response_content).encode("utf-8")
                            
                            formatted_results_df = process_search_result(results_html_bytes, entry_id, self.logger) # Simplified call if second arg is not needed
                            
                            if not formatted_results_df.empty:
                                self.logger.info(f"Worker PID {process.pid}: Found {len(formatted_results_df)} results for term '{term}' in region {region}")
                                return [
                                    {
                                        "EntryID": entry_id, # Already an int
                                        "ImageUrl": res.get("ImageUrl", "placeholder://no-image"),
                                        "ImageDesc": res.get("ImageDesc", ""),
                                        "ImageSource": res.get("ImageSource", "N/A"), # Ensure this is a valid URL if ImageUrl is a HttpUrl Pydantic type
                                        "ImageUrlThumbnail": res.get("ImageUrlThumbnail", res.get("ImageUrl", "placeholder://no-thumbnail"))
                                    }
                                    for _, res in formatted_results_df.iterrows()
                                ]
                            self.logger.warning(f"Worker PID {process.pid}: Formatted results were empty for term '{term}' in region {region}")
                            # No need to continue to next region if formatted_results_df is empty, let the outer loop handle it.

                except asyncio.TimeoutError:
                    self.logger.warning(f"Worker PID {process.pid}: Timeout searching term '{term}' in region {region}.")
                    if region_idx == len(self.regions) - 1: # If timeout on last region
                        self.logger.error(f"Worker PID {process.pid}: Timeout on all regions for term '{term}'.")
                        # Fall through to return []
                except aiohttp.ClientError as e: # Catch other client errors
                    self.logger.warning(f"Worker PID {process.pid}: ClientError for term '{term}' in region {region}: {e}")
                    if region_idx == len(self.regions) - 1:
                        self.logger.error(f"Worker PID {process.pid}: ClientError on all regions for term '{term}'.")
                        # Fall through to return []
                except Exception as e_general: # Catch any other unexpected error
                    self.logger.error(f"Worker PID {process.pid}: Unexpected error searching '{term}' in region {region}: {e_general}", exc_info=True)
                    if region_idx == len(self.regions) - 1:
                        self.logger.error(f"Worker PID {process.pid}: Unexpected error on all regions for term '{term}'.")
                        # Fall through to return []
            
            self.logger.error(f"Worker PID {process.pid}: All regions failed for term '{term}' (EntryID {entry_id}). Returning empty list.")
            return []


async def process_and_tag_results( # This function seems redundant with async_process_entry_search, consider consolidating
    search_string: str,
    brand: Optional[str] = None,
    model: Optional[str] = None, # model is not used
    endpoint: str = None,        # endpoint is not used here, but in SearchClient
    entry_id: int = None,
    logger: Optional[logging.Logger] = None,
    use_all_variations: bool = False,
    file_id_db: Optional[int] = None # file_id_db is not used
) -> List[Dict]:
    logger = logger or default_logger
    process = psutil.Process()
    try:
        logger.debug(f"Worker PID {process.pid}: Processing and tagging results for EntryID {entry_id}, Search: '{search_string}'")
        variations_dict = await generate_search_variations( # Renamed from 'variations' to 'variations_dict'
            search_string=search_string,
            brand=brand,
            # model=model, # model not passed to generate_search_variations if not used by it
            logger=logger
        )
        all_results_collected = [] # Renamed from all_results
        # Define order of variation types if specific priority is needed
        search_variation_types = [ # Renamed from search_types
            "default", "delimiter_variations", "color_variations",
            "brand_alias", "no_color", "model_alias", "category_specific"
        ]
        
        # Use the provided endpoint or default from config
        actual_endpoint = endpoint or SEARCH_PROXY_API_URL
        
        # Initialize SearchClient here to manage its lifecycle properly
        client = SearchClient(endpoint=actual_endpoint, logger=logger)
        try:
            for search_type_key in search_variation_types: # Renamed from search_type
                if search_type_key not in variations_dict:
                    logger.debug(f"Worker PID {process.pid}: Search type '{search_type_key}' not found in variations_dict for EntryID {entry_id}")
                    continue
                
                logger.info(f"Worker PID {process.pid}: Processing search type '{search_type_key}' for EntryID {entry_id}")
                for variation_term in variations_dict[search_type_key]: # Renamed from variation
                    logger.debug(f"Worker PID {process.pid}: Searching variation '{variation_term}' for EntryID {entry_id} (Type: {search_type_key})")
                    
                    # search_results_raw is a list of dicts from client.search
                    search_results_raw = await client.search(
                        term=variation_term,
                        brand=brand or "", # Pass brand
                        entry_id=entry_id  # Pass entry_id
                    )

                    if search_results_raw: # Check if the list is not empty
                        logger.info(f"Worker PID {process.pid}: Found {len(search_results_raw)} raw results for variation '{variation_term}'")
                        
                        # The client.search already returns results in the desired dictionary format.
                        # No need for additional tagging if client.search is already comprehensive.
                        # The required_columns check is also effectively done if client.search populates all fields.
                        all_results_collected.extend(search_results_raw)
                        logger.info(f"Worker PID {process.pid}: Added {len(search_results_raw)} results for variation '{variation_term}'")
                    else:
                        logger.debug(f"Worker PID {process.pid}: No raw results for variation '{variation_term}' in search type '{search_type_key}'")
                
                if all_results_collected and not use_all_variations:
                    logger.info(f"Worker PID {process.pid}: Stopping after finding {len(all_results_collected)} results from '{search_type_key}' for EntryID {entry_id} (use_all_variations=False).")
                    break # Stop after the first search type yields results if not using all variations
        finally:
            await client.close() # Ensure client is closed

        if not all_results_collected:
            logger.warning(f"Worker PID {process.pid}: No valid results found across all search types for EntryID {entry_id}, Search: '{search_string}'")
            # Return a single placeholder result
            return [{
                "EntryID": entry_id,
                "ImageUrl": "placeholder://no-results-found", # More specific placeholder
                "ImageDesc": f"No results found for '{search_string}'",
                "ImageSource": "N/A",
                "ImageUrlThumbnail": "placeholder://no-results-found",
                "ProductCategory": "" # Ensure this key exists if expected downstream
            }]
        
        logger.info(f"Worker PID {process.pid}: Total {len(all_results_collected)} results collected for EntryID {entry_id}, Search: '{search_string}'")
        return all_results_collected

    except Exception as e:
        logger.error(f"Worker PID {process.pid}: Error in process_and_tag_results for EntryID {entry_id}, Search: '{search_string}': {e}", exc_info=True)
        return [{ # Return a single placeholder error result
            "EntryID": entry_id,
            "ImageUrl": "placeholder://error-in-processing",
            "ImageDesc": f"Error processing '{search_string}': {str(e)}",
            "ImageSource": "N/A",
            "ImageUrlThumbnail": "placeholder://error-in-processing",
            "ProductCategory": ""
        }]


async def process_results( # This function seems to duplicate logic from process_and_tag_results and async_process_entry_search
    raw_results: List[Dict], # raw_results here are already processed by client.search if called from async_process_entry_search
    entry_id: int,
    brand: str, # brand not used if raw_results are already specific
    search_string: str, # search_string not used if raw_results are already specific
    logger: logging.Logger
) -> List[Dict]:
    logger.debug(f"Post-processing {len(raw_results)} raw_results for EntryID {entry_id}. Original search_string: '{search_string}', Brand: '{brand}'")
    final_results = []
    
    # If raw_results come directly from client.search, they should already be in a good format.
    # This function might be more about final validation or minor transformations.
    for result_item in raw_results:
        image_url = result_item.get("ImageUrl")
        if not image_url or image_url.startswith("placeholder://"): # Skip placeholders
            logger.debug(f"Skipping result for EntryID {entry_id} due to missing or placeholder ImageUrl: {image_url}")
            continue

        # Thumbnail extraction logic can be kept if needed, but client.search might already provide it
        thumbnail_url_from_result = result_item.get("ImageUrlThumbnail")
        if not thumbnail_url_from_result or thumbnail_url_from_result.startswith("placeholder://"):
            # Attempt to extract if not provided or is a placeholder
            thumbnail_url = await extract_thumbnail_url(image_url, logger=logger) or image_url # Fallback to main image
        else:
            thumbnail_url = thumbnail_url_from_result

        parsed_url = urlparse(image_url)
        image_source = result_item.get("ImageSource")
        if not image_source or image_source == "N/A": # If source is missing or N/A, try to parse from URL
            image_source = parsed_url.netloc or "unknown_source"
        
        formatted_result = {
            "EntryID": entry_id, # Ensure this matches the entry_id for this processing pass
            "ImageUrl": image_url,
            "ImageDesc": result_item.get("ImageDesc", ""),
            "ImageSource": image_source,
            "ImageUrlThumbnail": thumbnail_url,
            # "ProductCategory": result_item.get("ProductCategory", "") # Carry over if present
        }
        final_results.append(formatted_result)
    
    logger.debug(f"Finalized {len(final_results)} results for EntryID {entry_id} after post-processing.")
    return final_results


async def async_process_entry_search( # This function orchestrates search for a single entry
    search_string: str, # This is the original model/SKU from the DB record
    brand: str,         # Original brand from DB record
    endpoint: str,      # Search proxy endpoint
    entry_id: int,
    use_all_variations: bool,
    file_id_db: int,    # For logging context
    logger: logging.Logger
) -> List[Dict]:
    logger.debug(f"Starting async_process_entry_search for EntryID {entry_id} (FileID {file_id_db}). Search: '{search_string}', Brand: '{brand}', AllVariations: {use_all_variations}")
    
    # `process_and_tag_results` already handles variation generation and iteration.
    # We can call it directly.
    # `model` parameter is not used by `process_and_tag_results` as shown.
    all_processed_results = await process_and_tag_results(
        search_string=search_string, # Original search string from DB
        brand=brand,                 # Original brand from DB
        # model=None, # Not used by process_and_tag_results
        endpoint=endpoint,           # Pass endpoint for SearchClient
        entry_id=entry_id,
        logger=logger,
        use_all_variations=use_all_variations,
        # file_id_db=file_id_db # Not used by process_and_tag_results
    )

    # Filter out any placeholder results returned by process_and_tag_results in case of no hits or errors
    final_valid_results = [
        res for res in all_processed_results 
        if res.get("ImageUrl") and not res["ImageUrl"].startswith("placeholder://")
    ]

    if not final_valid_results:
        logger.warning(f"No valid image results found for EntryID {entry_id} (FileID {file_id_db}) after all processing. Original: '{search_string}'.")
        # Return a specific placeholder if nothing was found, this matches the behavior of process_and_tag_results
        return [{
            "EntryID": entry_id,
            "ImageUrl": "placeholder://no-final-valid-results",
            "ImageDesc": f"No valid results for '{search_string}' after all variations.",
            "ImageSource": "N/A",
            "ImageUrlThumbnail": "placeholder://no-final-valid-results",
            # "ProductCategory": "" # Ensure consistent schema
        }]

    logger.info(f"async_process_entry_search completed for EntryID {entry_id} (FileID {file_id_db}), returning {len(final_valid_results)} valid results.")
    return final_valid_results


async def run_job_with_logging(job_func: Callable[..., Any], file_id: str, **kwargs) -> Dict:
    file_id_str = str(file_id) # Ensure file_id is a string for logging/paths
    job_specific_logger, log_file_path = setup_job_logger(job_id=file_id_str, console_output=True) # Renamed vars
    
    result_data: Optional[Any] = None # For storing the actual return value of job_func
    debug_info = {"memory_usage": {}, "log_file_path": log_file_path, "endpoint_errors": []}
    status_code = 500 # Default to error
    message = "Job failed unexpectedly."

    try:
        # Determine function name for logging
        func_name = getattr(job_func, '__name__', 'unknown_function')

        job_specific_logger.info(f"Starting job '{func_name}' for FileID: {file_id_str}")
        process = psutil.Process()
        debug_info["memory_usage"]["before_rss_mb"] = process.memory_info().rss / (1024 * 1024)
        job_specific_logger.debug(f"Memory before job '{func_name}': RSS={debug_info['memory_usage']['before_rss_mb']:.2f} MB")

        # Ensure logger is passed if job_func expects it
        if 'logger' in kwargs or 'logger' in job_func.__code__.co_varnames:
             kwargs['logger'] = job_specific_logger # Pass the job-specific logger

        if asyncio.iscoroutinefunction(job_func):
            result_data = await job_func(file_id_str, **kwargs) # Pass file_id_str
        else: # For synchronous functions if any
            result_data = await asyncio.to_thread(job_func, file_id_str, **kwargs)
        
        debug_info["memory_usage"]["after_rss_mb"] = process.memory_info().rss / (1024 * 1024)
        job_specific_logger.debug(f"Memory after job '{func_name}': RSS={debug_info['memory_usage']['after_rss_mb']:.2f} MB")
        
        if debug_info["memory_usage"]["after_rss_mb"] > 1000: # Configurable threshold
            job_specific_logger.warning(f"High memory usage after job '{func_name}': RSS={debug_info['memory_usage']['after_rss_mb']:.2f} MB")

        job_specific_logger.info(f"Completed job '{func_name}' for FileID: {file_id_str}")
        status_code = 200
        message = f"Job '{func_name}' completed successfully for FileID: {file_id_str}"
        
        return {
            "status_code": status_code,
            "message": message,
            "data": result_data, # Actual result from the job
            "debug_info": debug_info
        }
    except Exception as e:
        func_name = getattr(job_func, '__name__', 'unknown_function')
        job_specific_logger.error(f"Error in job '{func_name}' for FileID: {file_id_str}: {str(e)}", exc_info=True) # Log with exc_info
        # debug_info["error_traceback"] = traceback.format_exc() # exc_info=True in logger does this

        # Specific handling for placeholder errors (example)
        if "placeholder://error" in str(e): # Or a more specific exception type
            debug_info["endpoint_errors"].append({"error_message": str(e), "timestamp": datetime.datetime.now().isoformat()})
            job_specific_logger.warning(f"Detected placeholder error in job '{func_name}' for FileID: {file_id_str}")
        
        message = f"Error in job '{func_name}' for FileID {file_id_str}: {str(e)}"
        
        return { # Return a structured error response
            "status_code": status_code, # Remains 500
            "message": message,
            "data": None,
            "debug_info": debug_info
        }
    finally:
        # Ensure log upload happens even if job_func itself fails
        # upload_log_file should ideally handle its own retries and errors gracefully
        log_upload_url = await upload_log_file(file_id_str, log_file_path, job_specific_logger)
        if "debug_info" in locals() and isinstance(debug_info, dict): # Ensure debug_info is initialized
             debug_info["log_upload_url"] = log_upload_url # Add upload URL to debug_info
        else: # Should not happen if try block was entered
             job_specific_logger.error("debug_info was not initialized prior to finally block in run_job_with_logging.")


async def run_generate_download_file(
    file_id: str, 
    job_logger: logging.Logger, # Renamed from logger to avoid conflict
    log_file_path_for_job: str, # Renamed from log_filename
    background_tasks: BackgroundTasks # background_tasks not used in this version
):
    try:
        JOB_STATUS[file_id] = {
            "status": "running_download_generation", # More specific status
            "message": "Excel file generation job is running",
            "timestamp": datetime.datetime.now().isoformat()
        }
        # Assuming the external service is hosted correctly and accessible
        # The URL seems to be calling itself if this code is also at icon7-8001.iconluxury.today
        # This needs to call the actual /generate-download-file endpoint.
        # If this function *is* the implementation of /generate-download-file, this is a recursive call.
        # Let's assume it's calling a *different* service or endpoint meant for file generation.
        
        # This should ideally be an internal call to another function if it's part of the same app,
        # or a configured URL for an external service.
        # For now, using the provided URL pattern:
        file_generation_service_url = f"https://icon7-8001.iconluxury.today/generate-download-file/?file_id={file_id}" # Example, ensure this is correct
        
        job_logger.info(f"Requesting file generation from: {file_generation_service_url} for FileID {file_id}")

        async with httpx.AsyncClient(timeout=300.0) as client: # Increased timeout for potentially long operations
            response = await client.post(
                file_generation_service_url,
                headers={"accept": "application/json"}
                # Removed data="" as POST without data might not be intended unless service expects it
            )
            response.raise_for_status() # Will raise an exception for 4xx/5xx responses
            result = response.json()
        
        # Check the structure of 'result' from the external service
        if result.get("status_code") and result["status_code"] != 200 or "error" in result:
            error_message = result.get("error", result.get("message", "Unknown error from file generation service"))
            JOB_STATUS[file_id] = {
                "status": "failed_download_generation",
                "message": f"Error during file generation: {error_message}",
                "log_url": await upload_log_file(file_id, log_file_path_for_job, job_logger), # Upload current job's log
                "timestamp": datetime.datetime.now().isoformat()
            }
            job_logger.error(f"File generation job failed for FileID {file_id}: {error_message}. Response: {result}")
        else:
            public_url = result.get("public_url", result.get("data", {}).get("public_url")) # Adapt to actual response structure
            JOB_STATUS[file_id] = {
                "status": "completed_download_generation",
                "message": "File generation job completed successfully",
                "public_url": public_url,
                "log_url": await upload_log_file(file_id, log_file_path_for_job, job_logger),
                "timestamp": datetime.datetime.now().isoformat()
            }
            job_logger.info(f"File generation job completed for FileID {file_id}. Public URL: {public_url}")

    except httpx.HTTPStatusError as hse:
        job_logger.error(f"HTTP error calling file generation service for FileID {file_id}: {hse.response.status_code} - {hse.response.text}", exc_info=True)
        JOB_STATUS[file_id] = {
            "status": "failed_download_generation",
            "message": f"HTTP error: {hse.response.status_code}",
            "log_url": await upload_log_file(file_id, log_file_path_for_job, job_logger),
            "timestamp": datetime.datetime.now().isoformat()
        }
    except Exception as e:
        job_logger.error(f"Unexpected error in file generation job for FileID {file_id}: {e}", exc_info=True)
        JOB_STATUS[file_id] = {
            "status": "failed_download_generation",
            "message": f"Unexpected error: {str(e)}",
            "log_url": await upload_log_file(file_id, log_file_path_for_job, job_logger),
            "timestamp": datetime.datetime.now().isoformat()
        }

async def upload_log_file(
    job_id_for_log: str, # Renamed from file_id to avoid confusion with db_record_file_id_to_update
    log_file_path: str,  # Renamed from log_filename
    logger_instance: logging.Logger, # Renamed from logger
    db_record_file_id_to_update: Optional[str] = None # The FileID in utb_ImageScraperFiles to update
) -> Optional[str]:
    
    # Determine the FileID to use for the S3 path and for updating the DB log URL.
    # If db_record_file_id_to_update is provided, it takes precedence for the DB update.
    # The S3 path can use job_id_for_log for uniqueness if they differ.
    s3_path_file_id = job_id_for_log # Use the job-specific ID for the S3 path for uniqueness

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(Exception), # Catches all exceptions for retry
        before_sleep=lambda retry_state: logger_instance.info(
            f"Retrying log upload for JobID {job_id_for_log} (attempt {retry_state.attempt_number}/3). Log file: {log_file_path}"
        )
    )
    async def try_upload_log_async(): # Renamed to indicate it's an async def
        if not await asyncio.to_thread(os.path.exists, log_file_path): # Use async os.path.exists
            logger_instance.warning(f"Log file {log_file_path} does not exist for JobID {job_id_for_log}, skipping upload.")
            return None

        # Hashing can be time-consuming for large files, run in a thread
        with open(log_file_path, "rb") as f:
            log_content_bytes = f.read()
        file_hash_hex = await asyncio.to_thread(hashlib.md5, log_content_bytes).hexdigest() # Corrected hexdigest call
        
        current_time_epoch = time.time() # Renamed for clarity
        # Use a more descriptive cache key
        cache_key_tuple = (log_file_path, job_id_for_log) 
        
        if cache_key_tuple in LAST_UPLOAD and \
           LAST_UPLOAD[cache_key_tuple]["hash"] == file_hash_hex and \
           current_time_epoch - LAST_UPLOAD[cache_key_tuple]["time"] < 60: # 60s debounce window
            logger_instance.info(f"Skipping redundant upload for log {log_file_path} (JobID {job_id_for_log}), hash matched.")
            return LAST_UPLOAD[cache_key_tuple]["url"]

        try:
            # Construct S3 key using s3_path_file_id (which is job_id_for_log)
            s3_log_key = f"job_logs/job_{s3_path_file_id}.log" # Ensure this path is desired
            
            uploaded_log_url = await upload_file_to_space( # This function needs to be async or run in a thread
                file_src=log_file_path,
                save_as=s3_log_key,
                is_public=True,
                logger=logger_instance, # Pass the correct logger
                file_id=s3_path_file_id # Pass context for S3 uploader logging
            )

            if not uploaded_log_url:
                logger_instance.error(f"S3 upload returned empty URL for log {log_file_path} (JobID {job_id_for_log}).")
                # Do not raise ValueError here to allow retry logic to handle it, or specific exception.
                # Let the retry mechanism handle it if it's a transient issue.
                return None # Indicate failure for this attempt

            # Update the log URL in the database for the *original file's record* if specified
            if db_record_file_id_to_update:
                update_success = await update_log_url_in_db(db_record_file_id_to_update, uploaded_log_url, logger_instance)
                if not update_success:
                    logger_instance.warning(f"Failed to update log URL in DB for original FileID {db_record_file_id_to_update}, but S3 upload was successful: {uploaded_log_url}")
            else:
                # If no specific DB record ID, update based on job_id_for_log if they are meant to be the same
                # This depends on how job_id_for_log relates to the primary file ID in the DB.
                # For now, only update if db_record_file_id_to_update is explicitly given.
                logger_instance.debug(f"No db_record_file_id_to_update provided, DB log URL not updated for JobID {job_id_for_log}.")


            LAST_UPLOAD[cache_key_tuple] = {"hash": file_hash_hex, "time": current_time_epoch, "url": uploaded_log_url}
            logger_instance.info(f"Log for JobID {job_id_for_log} uploaded to S3: {uploaded_log_url}")
            return uploaded_log_url
        
        except Exception as e_upload: # Catch specific exceptions if possible
            logger_instance.error(f"S3 upload failed for log {log_file_path} (JobID {job_id_for_log}): {e_upload}", exc_info=True)
            raise # Re-raise to trigger tenacity retry

    try:
        return await try_upload_log_async() # Call the async retry-wrapped function
    except Exception as e_final: # Catch exception if all retries fail
        logger_instance.error(f"Failed to upload log for JobID {job_id_for_log} ({log_file_path}) after all retries: {e_final}", exc_info=True)
        return None

async def process_restart_batch(
    file_id_db_str: str, # Expect string, convert to int internally if needed for DB
    entry_id: Optional[int] = None,
    use_all_variations: bool = False,
    logger: Optional[logging.Logger] = None, # Will be replaced by job-specific logger
    background_tasks: Optional[BackgroundTasks] = None, # Pass through if used by callees
    num_workers: int = 1, # Default to 1, can be overridden
) -> Dict[str, Any]: # Adjusted return type annotation
    
    # Setup job-specific logger if not provided (though run_job_with_logging should do this)
    # This function might be called directly in tests, so good to have a fallback.
    if logger is None:
        logger, log_filename = setup_job_logger(job_id=file_id_db_str, log_dir="job_logs", console_output=True)
    else: # If logger is provided (e.g., by run_job_with_logging)
        log_filename = getattr(logger.handlers[0], 'baseFilename', f"job_logs/job_{file_id_db_str}.log") if logger.handlers else f"job_logs/job_{file_id_db_str}.log"

    logger.setLevel(logging.DEBUG) # Ensure detailed logging for this job
    process = psutil.Process()
    
    # Helper for logging resource usage
    def log_resource_usage(context_msg: str = ""):
        mem_info = process.memory_info()
        cpu_percent = process.cpu_percent(interval=0.1) # Short interval for quick check
        # net_io = psutil.net_io_counters() # Net I/O can be verbose
        logger.debug(
            f"Resources {context_msg}: RSS={mem_info.rss / 1024**2:.2f} MB, CPU={cpu_percent:.1f}%"
        )
        if mem_info.rss / 1024**2 > 1000: # Configurable threshold
            logger.warning(f"High memory usage {context_msg}: RSS={mem_info.rss / 1024**2:.2f} MB")
        if cpu_percent > 80: # Configurable threshold
            logger.warning(f"High CPU usage {context_msg}: {cpu_percent:.1f}%")

    logger.info(f"Starting process_restart_batch for FileID: {file_id_db_str}. Workers: {num_workers}, AllVariations: {use_all_variations}, Initial EntryID: {entry_id}")
    log_resource_usage("at start")

    # Convert file_id_db_str to int for database operations, handle potential ValueError
    try:
        file_id_db_int = int(file_id_db_str)
    except ValueError:
        logger.error(f"Invalid FileID format: '{file_id_db_str}'. Must be an integer.")
        # Upload current log before raising/returning error
        log_public_url = await upload_log_file(file_id_db_str, log_filename, logger, db_record_file_id_to_update=file_id_db_str)
        return {
            "error": f"Invalid FileID format: '{file_id_db_str}'",
            "log_filename": log_filename,
            "log_public_url": log_public_url or "",
            "last_entry_id": str(entry_id or "")
        }

    # Constants for processing logic
    # These could be moved to a config file or module
    TARGET_THROUGHPUT_PER_WORKER = 10 # Example: target items per worker per unit of time for BATCH_TIME
    BATCH_EFFECTIVE_TIME_SECONDS = 0.5 # How long a batch is "expected" to take to influence size
    # BATCH_SIZE calculated to aim for BATCH_EFFECTIVE_TIME_SECONDS processing time per batch per worker
    # More sophisticated: BATCH_SIZE = max(1, ceil(TARGET_THROUGHPUT_PER_WORKER * BATCH_EFFECTIVE_TIME_SECONDS))
    # Simpler: Use a fixed sensible BATCH_SIZE or one based on num_workers
    # BATCH_SIZE = max(1, min(10, ceil(num_workers * 2))) # Example: scales slightly with workers
    BATCH_SIZE = max(1, 5) # Fixed small batch size for better concurrency control example
    
    # Max concurrency for I/O bound tasks (like calling search API)
    # Can be higher than num_workers (which might relate to CPU-bound limits or logical worker units)
    MAX_CONCURRENT_SEARCH_TASKS = max(num_workers * 2, 10) # Example: allow more concurrent API calls
    
    MAX_ENTRY_PROCESSING_RETRIES = 3
    # RELEVANCE_THRESHOLD = 0.9 # Not used in this snippet directly

    logger.debug(f"Processing Config: BATCH_SIZE={BATCH_SIZE}, MAX_CONCURRENT_SEARCH_TASKS={MAX_CONCURRENT_SEARCH_TASKS}, NumWorkers (logical)={num_workers}")

    # Get RabbitMQ producer (should use global_producer if available and initialized by lifespan)
    # local_producer_instance: Optional[RabbitMQProducer] = global_producer
    # if not local_producer_instance or not local_producer_instance.is_connected:
    #     logger.warning("Global RabbitMQ producer not available or disconnected. Attempting to get a new instance for this job.")
    try:
        # This will get the singleton global_producer or create it if it's the first call
        # We rely on lifespan to have initialized it. If not, get_producer will try.
        local_producer_instance = await RabbitMQProducer.get_producer() # Get the global/singleton instance
        if not local_producer_instance.is_connected: # Should be connected if lifespan worked
            await local_producer_instance.connect() # Attempt to connect if not
        logger.info(f"RabbitMQ producer obtained/verified for job {file_id_db_str}. Connected: {local_producer_instance.is_connected}")
    except Exception as e_producer:
        logger.error(f"Fatal: Failed to initialize/connect RabbitMQ producer for FileID {file_id_db_str}: {e_producer}", exc_info=True)
        log_public_url = await upload_log_file(file_id_db_str, log_filename, logger, db_record_file_id_to_update=file_id_db_str)
        return {
            "error": f"RabbitMQ producer initialization failed: {str(e_producer)}",
            "log_filename": log_filename, "log_public_url": log_public_url or "", "last_entry_id": str(entry_id or "")
        }


    # Validate FileID in the database
    try:
        async with async_engine.connect() as conn_validate:
            result_val = await conn_validate.execute(
                text(f"SELECT COUNT(*) FROM {IMAGE_SCRAPER_FILES_TABLE_NAME} WHERE {IMAGE_SCRAPER_FILES_PK_COLUMN} = :file_id"),
                {"file_id": file_id_db_int}
            )
            file_exists = result_val.scalar_one() > 0
            result_val.close() # Explicitly close cursor result
            if not file_exists:
                logger.error(f"FileID {file_id_db_str} (int: {file_id_db_int}) does not exist in {IMAGE_SCRAPER_FILES_TABLE_NAME}.")
                log_public_url = await upload_log_file(file_id_db_str, log_filename, logger, db_record_file_id_to_update=file_id_db_str)
                return {"error": f"FileID {file_id_db_str} not found.", "log_filename": log_filename, "log_public_url": log_public_url or "", "last_entry_id": str(entry_id or "")}
    except Exception as db_e:
        logger.error(f"Database error validating FileID {file_id_db_str}: {db_e}", exc_info=True)
        log_public_url = await upload_log_file(file_id_db_str, log_filename, logger, db_record_file_id_to_update=file_id_db_str)
        return {"error": f"DB error: {str(db_e)}", "log_filename": log_filename, "log_public_url": log_public_url or "", "last_entry_id": str(entry_id or "")}


    # Determine starting entry_id if not provided
    if entry_id is None:
        last_valid_db_entry_id = await fetch_last_valid_entry(file_id_db_str, logger)
        current_start_entry_id = 0 # Default to start from the beginning
        if last_valid_db_entry_id is not None:
            try:
                async with async_engine.connect() as conn_next_entry:
                    # Find the smallest EntryID > last_valid_db_entry_id that hasn't been processed (Step1 IS NULL)
                    query_next_entry = text(f"""
                        SELECT MIN({SCRAPER_RECORDS_PK_COLUMN}) 
                        FROM {SCRAPER_RECORDS_TABLE_NAME} 
                        WHERE {SCRAPER_RECORDS_FILE_ID_FK_COLUMN} = :file_id 
                          AND {SCRAPER_RECORDS_PK_COLUMN} > :last_processed_entry_id 
                          AND {SCRAPER_RECORDS_STEP1_COLUMN} IS NULL
                    """)
                    result_next = await conn_next_entry.execute(query_next_entry, {"file_id": file_id_db_int, "last_processed_entry_id": last_valid_db_entry_id})
                    next_entry_to_process = result_next.scalar_one_or_none()
                    result_next.close()
                    if next_entry_to_process is not None:
                        current_start_entry_id = next_entry_to_process
                    else:
                        # All entries after last_valid_db_entry_id might have Step1 populated,
                        # or there are no more entries. Check for any unprocessed from beginning.
                        query_any_unprocessed = text(f"""
                            SELECT MIN({SCRAPER_RECORDS_PK_COLUMN})
                            FROM {SCRAPER_RECORDS_TABLE_NAME}
                            WHERE {SCRAPER_RECORDS_FILE_ID_FK_COLUMN} = :file_id AND {SCRAPER_RECORDS_STEP1_COLUMN} IS NULL
                        """)
                        result_any = await conn_next_entry.execute(query_any_unprocessed, {"file_id": file_id_db_int})
                        current_start_entry_id = result_any.scalar_one_or_none() or 0 # Start from 0 if nothing found
                        result_any.close()
                logger.info(f"Resuming/starting from EntryID: {current_start_entry_id} for FileID {file_id_db_str}.")
                entry_id = current_start_entry_id # Update entry_id to be used for fetching entries
            except Exception as e_fetch_entry:
                logger.error(f"Error determining start EntryID for FileID {file_id_db_str}: {e_fetch_entry}", exc_info=True)
                # Proceed with entry_id as None or 0, which will fetch from beginning or based on query logic
                entry_id = 0 # Fallback
    
    # Fetch brand rules (cached or from URL)
    # This should be robust and have a fallback.
    brand_rules_dict = await fetch_brand_rules(BRAND_RULES_URL, max_attempts=3, timeout=10, logger=logger)
    if not brand_rules_dict:
        logger.warning(f"No brand rules fetched for FileID {file_id_db_str}. Using minimal fallback.")
        brand_rules_dict = {"Scotch & Soda": {"aliases": ["Scotch and Soda", "S&S"], "sku_pattern": r"^\d{6,8}[a-zA-Z0-9]*$"}}
    # Ensure "Scotch & Soda" default is present if not fetched
    if "Scotch & Soda" not in brand_rules_dict:
        brand_rules_dict["Scotch & Soda"] = {"aliases": ["Scotch and Soda", "S&S"], "sku_pattern": r"^\d{6,8}[a-zA-Z0-9]*$"}
        logger.info(f"Added default 'Scotch & Soda' to brand rules for FileID {file_id_db_str}.")


    search_api_endpoint = SEARCH_PROXY_API_URL # From config
    logger.debug(f"Using search API endpoint: {search_api_endpoint} for FileID {file_id_db_str}")

    # Fetch entries to process
    db_entries_to_process = []
    try:
        async with async_engine.connect() as conn_fetch_entries:
            # Query entries that need processing: Step1 is NULL OR (related result has SortOrder <= 0 or NULL)
            # This ensures re-processing if previous results were poor.
            query_entries = text(f"""
                SELECT r.{SCRAPER_RECORDS_PK_COLUMN}, r.{SCRAPER_RECORDS_PRODUCT_MODEL_COLUMN}, 
                       r.{SCRAPER_RECORDS_PRODUCT_BRAND_COLUMN}, r.{SCRAPER_RECORDS_PRODUCT_COLOR_COLUMN}, 
                       r.{SCRAPER_RECORDS_PRODUCT_CATEGORY_COLUMN}
                FROM {SCRAPER_RECORDS_TABLE_NAME} r
                LEFT JOIN (
                    SELECT DISTINCT {IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN} AS EntryID_Result
                    FROM {IMAGE_SCRAPER_RESULT_TABLE_NAME}
                    WHERE ({IMAGE_SCRAPER_RESULT_SORT_ORDER_COLUMN} IS NOT NULL AND {IMAGE_SCRAPER_RESULT_SORT_ORDER_COLUMN} > 0)
                ) t_good_results ON r.{SCRAPER_RECORDS_PK_COLUMN} = t_good_results.EntryID_Result
                WHERE r.{SCRAPER_RECORDS_FILE_ID_FK_COLUMN} = :file_id
                  AND (:entry_id_start_val IS NULL OR r.{SCRAPER_RECORDS_PK_COLUMN} >= :entry_id_start_val)
                  AND r.{SCRAPER_RECORDS_STEP1_COLUMN} IS NULL -- Primarily target those not yet attempted
                  AND t_good_results.EntryID_Result IS NULL   -- And those that don't have a good result yet
                ORDER BY r.{SCRAPER_RECORDS_PK_COLUMN}
            """)
            # entry_id here is the determined starting point
            result_entries = await conn_fetch_entries.execute(query_entries, {"file_id": file_id_db_int, "entry_id_start_val": entry_id})
            # Filter out entries where ProductModel (search_string) is None, as they can't be searched
            db_entries_to_process = [
                (row_tuple[0], row_tuple[1], row_tuple[2], row_tuple[3], row_tuple[4]) 
                for row_tuple in result_entries.fetchall() if row_tuple[1] is not None
            ]
            result_entries.close()
            logger.info(f"Fetched {len(db_entries_to_process)} entries requiring processing for FileID {file_id_db_str} (starting from EntryID {entry_id}).")
    except Exception as e_fetch:
        logger.error(f"Database error fetching entries for FileID {file_id_db_str}: {e_fetch}", exc_info=True)
        log_public_url = await upload_log_file(file_id_db_str, log_filename, logger, db_record_file_id_to_update=file_id_db_str)
        return {"error": f"DB error fetching entries: {str(e_fetch)}", "log_filename": log_filename, "log_public_url": log_public_url or "", "last_entry_id": str(entry_id or "")}

    if not db_entries_to_process:
        logger.warning(f"No valid entries (with ProductModel) found for processing for FileID {file_id_db_str} from EntryID {entry_id}.")
        log_public_url = await upload_log_file(file_id_db_str, log_filename, logger, db_record_file_id_to_update=file_id_db_str)
        return {
            "message": "No entries found requiring processing.", # Changed from error to message
            "success": True, # Indicate no error, just no work
            "log_filename": log_filename, "log_public_url": log_public_url or "", "last_entry_id": str(entry_id or "")
        }

    # Batching logic
    entry_batches_list = []
    if len(db_entries_to_process) < BATCH_SIZE:
        entry_batches_list = [[entry_item] for entry_item in db_entries_to_process]
        logger.info(f"Total entries ({len(db_entries_to_process)}) < BATCH_SIZE ({BATCH_SIZE}). Creating {len(entry_batches_list)} individual entry batches for FileID {file_id_db_str}.")
    else:
        entry_batches_list = [db_entries_to_process[i:i + BATCH_SIZE] for i in range(0, len(db_entries_to_process), BATCH_SIZE)]
        logger.info(f"Created {len(entry_batches_list)} batches for {len(db_entries_to_process)} entries (BATCH_SIZE {BATCH_SIZE}) for FileID {file_id_db_str}.")


    total_successful_entries = 0
    total_failed_entries = 0
    # Initialize with the starting entry_id or 0 if none was determined (means start from very beginning)
    current_last_entry_id_processed = entry_id if entry_id is not None else 0


    # Semaphore for controlling concurrency of process_entry tasks
    # This limits how many process_entry coroutines run "simultaneously" at the asyncio level.
    # MAX_CONCURRENT_SEARCH_TASKS applies *within* each process_entry call to the SearchClient.
    # This process_entry_semaphore limits how many entry_id's are processed in parallel.
    process_entry_semaphore = asyncio.Semaphore(MAX_CONCURRENT_SEARCH_TASKS) # Or a different value like num_workers

    # Retry wrapper for enqueuing DB updates
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min_retry_interval_s=1, max_retry_interval_s=5), # Renamed params
        retry=retry_if_exception_type(Exception), # General retry for enqueue
        reraise=True # Reraise the exception if all retries fail
    )
    async def enqueue_db_update_with_retry(
        sql_query: str, 
        query_params: Dict, 
        task_description: str, # Renamed from task_type
        op_correlation_id: str, # Renamed from correlation_id
        resp_queue: Optional[str] = None, # Renamed from response_queue
        expect_return: bool = False # Renamed from return_result
    ):
        try:
            # Ensure producer is connected before each use (get_producer should handle this if singleton)
            # producer_for_enqueue = await RabbitMQProducer.get_producer() # Get global
            # if not producer_for_enqueue.is_connected: await producer_for_enqueue.connect()

            # Use local_producer_instance which was verified at the start of process_restart_batch
            if not local_producer_instance.is_connected: # Re-check just in case
                await local_producer_instance.connect()

            return await enqueue_db_update( # Call the original enqueue function
                file_id=file_id_db_str, # Context for the task
                sql=sql_query,
                params=query_params,
                task_type=task_description, # Use the descriptive task type
                producer_instance=local_producer_instance, # Pass the verified producer
                response_queue=resp_queue,
                correlation_id=op_correlation_id,
                return_result=expect_return,
                logger_param=logger # Pass the job-specific logger
            )
        except asyncio.TimeoutError as te_enqueue:
            logger.error(f"Timeout enqueuing DB update for Task '{task_description}', CorrelationID {op_correlation_id}: {te_enqueue}", exc_info=True)
            raise # Reraise to be caught by tenacity for retry
        except Exception as e_enqueue:
            logger.error(f"Error enqueuing DB update for Task '{task_description}', CorrelationID {op_correlation_id}: {e_enqueue}", exc_info=True)
            raise # Reraise for tenacity

    # Inner function to process a single entry
    async def process_single_entry(entry_data_tuple: tuple):
        # Unpack entry data
        current_entry_id, original_search_str, original_brand_str, original_color_str, original_category_str = entry_data_tuple
        
        async with process_entry_semaphore: # Control concurrency for processing entries
            results_successfully_written_for_entry = False
            try:
                for attempt_num in range(1, MAX_ENTRY_PROCESSING_RETRIES + 1):
                    logger.debug(f"Processing EntryID {current_entry_id} (FileID {file_id_db_str}), Attempt {attempt_num}/{MAX_ENTRY_PROCESSING_RETRIES}")
                    try:
                        # Preprocess SKU (this might modify search_string, brand, etc.)
                        # Ensure preprocess_sku returns all necessary components or use original values
                        proc_search_str, proc_brand_str, _, _ = await preprocess_sku(
                            search_string=original_search_str,
                            known_brand=original_brand_str,
                            brand_rules=brand_rules_dict,
                            logger=logger
                        )
                        
                        # Call the refactored search and processing logic
                        # async_process_entry_search handles variations and returns a list of valid results
                        search_results_list = await async_process_entry_search(
                            search_string=proc_search_str, # Use preprocessed search string
                            brand=proc_brand_str,         # Use preprocessed brand
                            endpoint=search_api_endpoint,
                            entry_id=current_entry_id,
                            use_all_variations=use_all_variations,
                            file_id_db=file_id_db_int, # Pass int version
                            logger=logger
                        )
                        
                        # Filter out placeholders again, just in case async_process_entry_search structure changes
                        valid_search_results = [
                            res for res in search_results_list 
                            if res.get("ImageUrl") and not res["ImageUrl"].startswith("placeholder://")
                        ]

                        if not valid_search_results:
                            logger.warning(f"No valid search results after async_process_entry_search for EntryID {current_entry_id} (Attempt {attempt_num}).")
                            # If no results on the last attempt, don't mark Step1, allow re-processing later.
                            # Or, if this means "search exhausted, no results", Step1 could be set. Depends on logic.
                            # For now, if no valid results, we effectively fail this attempt for the entry.
                            if attempt_num == MAX_ENTRY_PROCESSING_RETRIES:
                                logger.error(f"EntryID {current_entry_id} yielded no valid results after all search attempts.")
                                # Optionally, mark Step1 as NULL here to signify a hard failure for this run,
                                # if the main loop doesn't already handle un-touched entries.
                                # For now, just returning False.
                            # Continue to next attempt or fail the entry
                            if attempt_num < MAX_ENTRY_PROCESSING_RETRIES: await asyncio.sleep(1 * attempt_num)
                            continue # To next attempt for this entry_id

                        # Insert the valid search results
                        # insert_search_results should use enqueue_db_update internally
                        insertion_success = await insert_search_results(
                            results=valid_search_results,
                            logger=logger,
                            file_id=file_id_db_str, # Pass string FileID
                            background_tasks=background_tasks # Pass along if provided/needed
                        )

                        if insertion_success:
                            results_successfully_written_for_entry = True
                            logger.info(f"Successfully inserted/enqueued {len(valid_search_results)} results for EntryID {current_entry_id} (FileID {file_id_db_str}).")
                            
                            # Update Step1 timestamp for this EntryID as it's successfully processed
                            update_step1_sql = f"UPDATE {SCRAPER_RECORDS_TABLE_NAME} SET {SCRAPER_RECORDS_STEP1_COLUMN} = GETUTCDATE() WHERE {SCRAPER_RECORDS_PK_COLUMN} = :entry_id_param"
                            await enqueue_db_update_with_retry(
                                sql_query=update_step1_sql,
                                query_params={"entry_id_param": current_entry_id},
                                task_description="update_entry_step1_timestamp",
                                op_correlation_id=str(uuid.uuid4())
                            )
                            return current_entry_id, True # Signal success for this entry
                        else:
                            logger.warning(f"Failed to insert/enqueue search results for EntryID {current_entry_id} (Attempt {attempt_num}).")
                            # If insertion fails, retry the whole entry processing if attempts remain
                            if attempt_num < MAX_ENTRY_PROCESSING_RETRIES: await asyncio.sleep(1 * attempt_num)
                            continue # To next attempt for this entry_id

                    except asyncio.TimeoutError as e_timeout:
                        logger.error(f"Timeout during processing EntryID {current_entry_id} (Attempt {attempt_num}): {e_timeout}", exc_info=True)
                        if attempt_num == MAX_ENTRY_PROCESSING_RETRIES:
                            logger.error(f"EntryID {current_entry_id} failed due to timeout after all retries.")
                        else: await asyncio.sleep(1 * attempt_num) # Wait before next attempt
                        # Continue to next attempt or fail the entry after max retries
                    except Exception as e_inner_proc:
                        logger.error(f"Error processing EntryID {current_entry_id} (Attempt {attempt_num}): {e_inner_proc}", exc_info=True)
                        if attempt_num == MAX_ENTRY_PROCESSING_RETRIES:
                            logger.error(f"EntryID {current_entry_id} failed due to error '{e_inner_proc}' after all retries.")
                        else: await asyncio.sleep(1 * attempt_num)
                        # Continue to next attempt
                
                # If loop finishes, all attempts for this entry failed
                logger.error(f"Exhausted all {MAX_ENTRY_PROCESSING_RETRIES} attempts for EntryID {current_entry_id}. Marking as failed for this run.")
                # Optionally, if no results were ever written and all attempts failed, reset Step1
                if not results_successfully_written_for_entry:
                    reset_step1_sql = f"UPDATE {SCRAPER_RECORDS_TABLE_NAME} SET {SCRAPER_RECORDS_STEP1_COLUMN} = NULL WHERE {SCRAPER_RECORDS_PK_COLUMN} = :entry_id_param"
                    # This enqueue should be robust. If it fails, the overall job might misreport.
                    try:
                        await enqueue_db_update_with_retry(
                            sql_query=reset_step1_sql,
                            query_params={"entry_id_param": current_entry_id},
                            task_description="reset_step1_after_all_attempts_failed",
                            op_correlation_id=str(uuid.uuid4())
                        )
                        logger.info(f"Reset Step1 for EntryID {current_entry_id} after all processing attempts failed to write results.")
                    except Exception as e_reset_enqueue:
                        logger.error(f"Failed to enqueue Step1 reset for failed EntryID {current_entry_id}: {e_reset_enqueue}", exc_info=True)

                return current_entry_id, False # Signal failure for this entry after all attempts

            except Exception as e_outer_entry_proc: # Catch-all for unexpected issues within process_single_entry
                logger.critical(f"Critical unexpected error in process_single_entry for EntryID {current_entry_id}: {e_outer_entry_proc}", exc_info=True)
                return current_entry_id, False # Signal failure

    # Main batch processing loop
    for batch_num, current_batch_entries in enumerate(entry_batches_list, 1):
        logger.info(f"Processing Batch {batch_num}/{len(entry_batches_list)} (Size: {len(current_batch_entries)}) for FileID {file_id_db_str}.")
        batch_start_time = datetime.datetime.now()
        
        # Process entries in the current batch concurrently
        # gather will collect results or exceptions from each process_single_entry call
        batch_processing_results = await asyncio.gather(
            *(process_single_entry(entry_tuple) for entry_tuple in current_batch_entries),
            return_exceptions=True # Exceptions from tasks are returned as results
        )

        # Process results of the batch
        for entry_tuple, outcome in zip(current_batch_entries, batch_processing_results):
            processed_entry_id = entry_tuple[0] # Get EntryID from the original tuple
            
            if isinstance(outcome, asyncio.CancelledError): # Should not happen if not cancelled externally
                logger.warning(f"Task for EntryID {processed_entry_id} in batch {batch_num} was cancelled unexpectedly.", exc_info=True)
                total_failed_entries += 1
            elif isinstance(outcome, Exception): # Exception was returned by gather
                logger.error(f"Task for EntryID {processed_entry_id} in batch {batch_num} failed with exception: {outcome}", exc_info=outcome)
                total_failed_entries += 1
            else: # Expected (entry_id, success_flag) tuple
                try:
                    _, success_flag = outcome # Unpack the tuple: (returned_entry_id, success_bool)
                    if success_flag:
                        total_successful_entries += 1
                        current_last_entry_id_processed = max(current_last_entry_id_processed, processed_entry_id)
                    else:
                        total_failed_entries += 1
                except (TypeError, ValueError) as e_unpack: # Safety for unpacking
                    logger.error(f"Invalid result structure for EntryID {processed_entry_id} in batch {batch_num}: {outcome}. Error: {e_unpack}", exc_info=True)
                    total_failed_entries += 1
        
        # Log progress and resource usage after each batch
        batch_elapsed_seconds = (datetime.datetime.now() - batch_start_time).total_seconds()
        entries_per_second = len(current_batch_entries) / batch_elapsed_seconds if batch_elapsed_seconds > 0 else float('inf')
        logger.info(
            f"Batch {batch_num} completed in {batch_elapsed_seconds:.2f}s ({entries_per_second:.2f} entries/s). "
            f"Total Successful: {total_successful_entries}, Total Failed: {total_failed_entries}. FileID: {file_id_db_str}."
        )
        log_resource_usage(f"after Batch {batch_num}")
        
        # Check if all entries for the FileID have been processed (Step1 is not NULL)
        # This check can be costly if done frequently. Consider doing it less often or at the end.
        # For now, let's assume we proceed through all fetched db_entries_to_process.
        # A more robust "all done" check would query the DB for remaining Step1 IS NULL.
        # if total_successful_entries + total_failed_entries == len(db_entries_to_process):
        #     logger.info(f"All initially fetched entries have been attempted for FileID {file_id_db_str}.")
        #     break # Exit batch loop if all fetched entries are processed

        await asyncio.sleep(0.1) # Small pause between batches, if desired

    # After all batches are processed
    logger.info(f"Finished processing all batches for FileID {file_id_db_str}. Successful: {total_successful_entries}, Failed: {total_failed_entries}.")

    # Update master file record's ImageCompleteTime if any entries were successfully processed
    if total_successful_entries > 0:
        logger.info(f"Attempting to update {IMAGE_SCRAPER_FILES_IMAGE_COMPLETE_TIME_COLUMN} for FileID {file_id_db_str}.")
        update_file_complete_sql = f"""
            UPDATE {IMAGE_SCRAPER_FILES_TABLE_NAME}
            SET {IMAGE_SCRAPER_FILES_IMAGE_COMPLETE_TIME_COLUMN} = GETUTCDATE()
            WHERE {IMAGE_SCRAPER_FILES_PK_COLUMN} = :file_id_param
        """
        try:
            await enqueue_db_update_with_retry(
                sql_query=update_file_complete_sql,
                query_params={"file_id_param": file_id_db_int},
                task_description="update_file_imageprocessing_complete_time",
                op_correlation_id=str(uuid.uuid4())
            )
            logger.info(f"{IMAGE_SCRAPER_FILES_IMAGE_COMPLETE_TIME_COLUMN} update enqueued for FileID {file_id_db_str}.")
        except Exception as e_enqueue_complete:
            logger.error(f"Failed to enqueue {IMAGE_SCRAPER_FILES_IMAGE_COMPLETE_TIME_COLUMN} update for FileID {file_id_db_str}: {e_enqueue_complete}", exc_info=True)
            # Continue, as this is a secondary update.

    # Final verification log (counts from DB might be more accurate if consumer is fast)
    try:
        async with async_engine.connect() as conn_final_verify:
            query_final_stats = text(f"""
                SELECT 
                    COUNT(CASE WHEN {SCRAPER_RECORDS_STEP1_COLUMN} IS NOT NULL THEN 1 END) as ProcessedStep1,
                    COUNT(DISTINCT res.{IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN}) as EntriesWithResults
                FROM {SCRAPER_RECORDS_TABLE_NAME} rec
                LEFT JOIN {IMAGE_SCRAPER_RESULT_TABLE_NAME} res ON rec.{SCRAPER_RECORDS_PK_COLUMN} = res.{IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN}
                WHERE rec.{SCRAPER_RECORDS_FILE_ID_FK_COLUMN} = :file_id
            """)
            result_stats = await conn_final_verify.execute(query_final_stats, {"file_id": file_id_db_int})
            stats_row = result_stats.one_or_none()
            result_stats.close()
            if stats_row:
                logger.info(f"DB Verification for FileID {file_id_db_str}: Entries with Step1 set: {stats_row.ProcessedStep1}, Entries with results in ScraperResult table: {stats_row.EntriesWithResults}")
    except Exception as e_final_verify:
        logger.error(f"Error during final DB verification for FileID {file_id_db_str}: {e_final_verify}", exc_info=True)


    # Send completion email
    email_recipients = await get_send_to_email(file_id_db_int, logger=logger)
    log_public_url_final = await upload_log_file(file_id_db_str, log_filename, logger, db_record_file_id_to_update=file_id_db_str)

    if email_recipients:
        email_subject = f"Image Scraping Batch Processing Completed for FileID: {file_id_db_str}"
        email_message = (
            f"Image scraping batch processing for FileID {file_id_db_str} has completed.\n\n"
            f"Total entries attempted: {len(db_entries_to_process)}\n"
            f"Successfully processed entries: {total_successful_entries}\n"
            f"Failed entries: {total_failed_entries}\n"
            f"Last EntryID processed in this run: {current_last_entry_id_processed}\n"
            f"Log file available at: {log_public_url_final or 'Not uploaded'}\n"
            f"Settings: Use All Variations={use_all_variations}, Logical Workers={num_workers}"
        )
        # Ensure send_message_email is robust
        try :
            await send_message_email(email_recipients, subject=email_subject, message=email_message, logger=logger)
            logger.info(f"Completion email sent to {email_recipients} for FileID {file_id_db_str}.")
        except Exception as e_email:
            logger.error(f"Failed to send completion email for FileID {file_id_db_str}: {e_email}", exc_info=True)


    # Trigger subsequent steps like sorting and file generation
    logger.info(f"Attempting to trigger post-processing (sort, generate download) for FileID: {file_id_db_str}")
    try:
        # update_sort_order might use BackgroundTasks, ensure it's available or handled if None
        # Pass the job-specific logger to these utility functions
        sort_result_info = await update_sort_order(file_id_db_str, logger=logger, background_tasks=background_tasks)
        logger.info(f"Sort order update initiated/completed for FileID {file_id_db_str}. Info: {sort_result_info}")
        
        # run_generate_download_file
        # Pass the job-specific logger and its log_filename
        await run_generate_download_file(file_id_db_str, logger, log_filename, background_tasks) # Pass logger and log_filename
        logger.info(f"Download file generation queued/initiated for FileID {file_id_db_str}")

    except Exception as e_post_proc:
        logger.error(f"Error during post-processing steps (sort/generate) for FileID {file_id_db_str}: {e_post_proc}", exc_info=True)
        # Email about post-processing failure if critical
        if email_recipients:
            subject_fail = f"Post-Processing Failed for FileID: {file_id_db_str}"
            message_fail = (
                f"Post-processing (sorting/file generation) failed for FileID {file_id_db_str} after batch completion.\n"
                f"Error: {str(e_post_proc)}\n"
                f"Batch processing results: {total_successful_entries}/{len(db_entries_to_process)} successful.\n"
                f"Log URL: {log_public_url_final or 'Not available'}"
            )
            await send_message_email(email_recipients, subject=subject_fail, message=message_fail, logger=logger)


    return { # Return a structured dictionary
        "message": "Search processing batch completed.",
        "file_id": file_id_db_str,
        "total_entries_fetched_for_processing": len(db_entries_to_process),
        "successful_entries_processed": total_successful_entries,
        "failed_entries_processed": total_failed_entries,
        "last_entry_id_successfully_processed_in_run": current_last_entry_id_processed,
        "log_filename": log_filename, # Path on server
        "log_public_url": log_public_url_final or "", # Public URL if uploaded
        "settings": {"use_all_variations": use_all_variations, "num_workers": num_workers}
    }

    # The `finally` block for disposing async_engine is removed as engine lifecycle
    # is typically managed globally by lifespan or per-request, not per-job function call.
    # If this function creates its own engine, then a finally block would be needed.
    # Assuming async_engine is a global/shared instance.


# --- API Endpoints ---
@router.post("/populate-results-from-warehouse/{file_id}", tags=["Processing", "Warehouse"])
async def api_populate_results_from_warehouse(
    request: Request, # Keep if used, e.g. for client IP
    file_id: str,
    limit: Optional[int] = Query(1000, description=f"Max records from {SCRAPER_RECORDS_TABLE_NAME} to process for this FileID"),
    base_image_url: str = Query("https://cms.rtsplusdev.com/files/icon_warehouse_images", description="Base URL for constructing image paths from warehouse data")
):
    job_specific_log_id = f"warehouse_populate_{file_id}_{uuid.uuid4().hex[:8]}"
    logger, log_filename_path = setup_job_logger(job_id=job_specific_log_id, console_output=True)

    logger.info(f"[{job_specific_log_id}] API Endpoint: Starting population of '{IMAGE_SCRAPER_RESULT_TABLE_NAME}' from '{WAREHOUSE_IMAGES_TABLE_NAME}' for original FileID: {file_id}. Limit: {limit}")

    # Initialize counters and flags
    inserted_or_merged_count = 0
    scraper_records_matched_in_db = 0 # Renamed for clarity
    # warehouse_matches_found = 0 # This seems same as scraper_records_matched_in_db
    scraper_status_updates_enqueued_count = 0 # Renamed
    data_preparation_errors = 0 # Renamed

    # Get a local producer instance for this API call if needed by enqueue_db_update
    # However, enqueue_db_update should use the global_producer if producer_instance is None
    # For operations within this endpoint that might call enqueue_db_update directly and want to ensure
    # a specific producer, it can be obtained. Here, relying on enqueue_db_update's default.
    # local_api_producer: Optional[RabbitMQProducer] = None # To be used if specific instance needed

    try:
        # local_api_producer = await RabbitMQProducer.get_producer() # Uses global/singleton
        # if not local_api_producer or not local_api_producer.is_connected:
        #     await local_api_producer.connect() # Ensure connected
        # logger.info(f"[{job_specific_log_id}] API: Obtained RabbitMQ producer for task. Connected: {local_api_producer.is_connected if local_api_producer else False}")
        
        # Validate original FileID exists in the master files table
        try:
            file_id_int = int(file_id)
        except ValueError:
            logger.error(f"[{job_specific_log_id}] Invalid FileID format: '{file_id}'. Must be an integer.")
            await upload_log_file(job_specific_log_id, log_filename_path, logger, db_record_file_id_to_update=file_id)
            raise HTTPException(status_code=400, detail=f"Invalid FileID format: {file_id}")

        async with async_engine.connect() as conn_validate_file:
            validate_file_id_sql = text(f"SELECT COUNT(*) FROM {IMAGE_SCRAPER_FILES_TABLE_NAME} WHERE {IMAGE_SCRAPER_FILES_PK_COLUMN} = :file_id_val_param")
            validation_cursor = await conn_validate_file.execute(validate_file_id_sql, {"file_id_val_param": file_id_int})
            if validation_cursor.scalar_one() == 0:
                logger.error(f"[{job_specific_log_id}] Original FileID '{file_id}' does not exist in {IMAGE_SCRAPER_FILES_TABLE_NAME}.")
                await upload_log_file(job_specific_log_id, log_filename_path, logger, db_record_file_id_to_update=file_id)
                raise HTTPException(status_code=404, detail=f"Original FileID {file_id} not found.")
            validation_cursor.close() # Close cursor

        # Fetch scraper records that match warehouse items and need processing
        async with async_engine.connect() as conn_fetch_matches:
            # SQL to find ScraperRecords matching WarehouseImages, not yet processed for warehouse population
            # (EntryStatus = STATUS_PENDING_WAREHOUSE_CHECK or NULL)
            fetch_matches_sql = text(f"""
                SELECT TOP (:limit_query_param)
                    isr.{SCRAPER_RECORDS_PK_COLUMN} AS MatchedScraperEntryID,
                    isr.{SCRAPER_RECORDS_PRODUCT_MODEL_COLUMN} AS MatchedScraperProductModel,
                    isr.{SCRAPER_RECORDS_FILE_ID_FK_COLUMN} AS MatchedScraperFileID_fk, # Should match file_id_int
                    isr.{SCRAPER_RECORDS_PRODUCT_BRAND_COLUMN} AS MatchedScraperProductBrand,
                    iwi.{WAREHOUSE_IMAGES_PK_COLUMN} AS MatchedWarehousePK_ID,
                    iwi.{WAREHOUSE_IMAGES_MODEL_NUMBER_COLUMN} AS MatchedWarehouseModelNumber,
                    iwi.{WAREHOUSE_IMAGES_MODEL_CLEAN_COLUMN} AS MatchedWarehouseModelClean,
                    iwi.{WAREHOUSE_IMAGES_MODEL_FOLDER_COLUMN} AS MatchedWarehouseModelFolder,
                    iwi.{WAREHOUSE_IMAGES_MODEL_SOURCE_COLUMN} AS MatchedWarehouseModelSource
                FROM {SCRAPER_RECORDS_TABLE_NAME} isr
                INNER JOIN {WAREHOUSE_IMAGES_TABLE_NAME} iwi
                    ON isr.{SCRAPER_RECORDS_PRODUCT_MODEL_COLUMN} = iwi.{WAREHOUSE_IMAGES_MODEL_CLEAN_COLUMN} -- Join condition
                WHERE isr.{SCRAPER_RECORDS_FILE_ID_FK_COLUMN} = :file_id_query_param -- Filter by original FileID
                  AND (isr.{SCRAPER_RECORDS_ENTRY_STATUS_COLUMN} = {STATUS_PENDING_WAREHOUSE_CHECK} OR isr.{SCRAPER_RECORDS_ENTRY_STATUS_COLUMN} IS NULL)
                  AND iwi.{WAREHOUSE_IMAGES_MODEL_CLEAN_COLUMN} IS NOT NULL AND iwi.{WAREHOUSE_IMAGES_MODEL_CLEAN_COLUMN} <> '' -- Ensure warehouse data is usable
                  AND iwi.{WAREHOUSE_IMAGES_MODEL_FOLDER_COLUMN} IS NOT NULL AND iwi.{WAREHOUSE_IMAGES_MODEL_FOLDER_COLUMN} <> ''
                ORDER BY isr.{SCRAPER_RECORDS_PK_COLUMN}; -- Process in order
            """)
            matched_records_cursor = await conn_fetch_matches.execute(
                fetch_matches_sql,
                {"file_id_query_param": file_id_int, "limit_query_param": limit}
            )
            # Fetchall into a list of RowProxy objects or similar, then convert to dicts if needed
            records_to_create_results_for = matched_records_cursor.mappings().fetchall() # Get list of dict-like rows
            matched_records_cursor.close() # Close cursor
            
            scraper_records_matched_in_db = len(records_to_create_results_for)

        if not records_to_create_results_for:
            logger.info(f"[{job_specific_log_id}] API: No records in {SCRAPER_RECORDS_TABLE_NAME} for FileID '{file_id}' are pending warehouse check or match warehouse criteria for new results.")
            log_public_url_no_recs = await upload_log_file(job_specific_log_id, log_filename_path, logger, db_record_file_id_to_update=file_id)
            return {
                "status": "no_action_needed", # More descriptive status
                "message": f"No new/pending records for FileID '{file_id}' found for warehouse-to-result processing.",
                "scraper_records_matched_in_db": 0,
                "results_prepared_for_insertion": 0,
                "scraper_status_updates_enqueued": 0,
                "log_url": log_public_url_no_recs
            }

        logger.info(f"[{job_specific_log_id}] API: Found {scraper_records_matched_in_db} records from {SCRAPER_RECORDS_TABLE_NAME} (Original FileID '{file_id}') with warehouse matches to populate into {IMAGE_SCRAPER_RESULT_TABLE_NAME}.")

        # Prepare data for batch insertion into utb_ImageScraperResult
        results_for_batch_insertion = []
        for db_record_map in records_to_create_results_for: # Iterate over list of dict-like rows
            try:
                scraper_entry_id_val = db_record_map['MatchedScraperEntryID'] # Access by key
                
                # Validate essential warehouse data for URL construction
                warehouse_model_clean_val = db_record_map.get('MatchedWarehouseModelClean')
                warehouse_model_folder_val = db_record_map.get('MatchedWarehouseModelFolder')

                if not warehouse_model_clean_val or not warehouse_model_folder_val:
                    logger.warning(f"[{job_specific_log_id}] Skipping ScraperEntryID {scraper_entry_id_val}: WarehouseModelClean or Folder missing (WarehousePK_ID {db_record_map.get('MatchedWarehousePK_ID')}).")
                    data_preparation_errors += 1
                    continue
                
                # Construct ImageUrl, ensuring no double slashes and extension handling
                model_clean_for_url_part = warehouse_model_clean_val.replace('.png', '').replace('.jpg', '').replace('.jpeg', '')
                image_url_from_warehouse_val = f"{base_image_url.rstrip('/')}/{warehouse_model_folder_val.strip('/')}/{model_clean_for_url_part}.png" # Default to .png
                
                image_desc_val = f"{db_record_map.get('MatchedScraperProductBrand') or 'Brand'} {db_record_map.get('MatchedWarehouseModelNumber') or db_record_map.get('MatchedScraperProductModel') or 'Product'}"
                
                image_source_domain_parsed = urlparse(base_image_url).netloc if base_image_url else "warehouse_internal_source"
                image_actual_source_val = db_record_map.get('MatchedWarehouseModelSource') or image_source_domain_parsed

                results_for_batch_insertion.append({
                    IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN: scraper_entry_id_val,
                    IMAGE_SCRAPER_RESULT_IMAGE_URL_COLUMN: image_url_from_warehouse_val,
                    IMAGE_SCRAPER_RESULT_IMAGE_DESC_COLUMN: image_desc_val,
                    IMAGE_SCRAPER_RESULT_IMAGE_SOURCE_COLUMN: image_actual_source_val,
                    IMAGE_SCRAPER_RESULT_IMAGE_URL_THUMBNAIL_COLUMN: image_url_from_warehouse_val, # Thumbnail same as main for warehouse
                    IMAGE_SCRAPER_RESULT_SORT_ORDER_COLUMN: 1, # Default sort order for warehouse results
                    IMAGE_SCRAPER_RESULT_SOURCE_TYPE_COLUMN: 'Warehouse', # Mark source type
                    # CreateTime will be set by insert_search_results or the DB consumer
                })
            except Exception as e_loop_prepare:
                data_preparation_errors += 1
                scraper_id_for_log = db_record_map.get('MatchedScraperEntryID', 'Unknown ScraperEntryID')
                logger.error(f"[{job_specific_log_id}] Error preparing data for ScraperEntryID {scraper_id_for_log}: {e_loop_prepare}", exc_info=True)

        # Perform batch insertion if there's data
        if results_for_batch_insertion:
            logger.info(f"[{job_specific_log_id}] Prepared {len(results_for_batch_insertion)} new results for insertion into {IMAGE_SCRAPER_RESULT_TABLE_NAME}.")
            
            # insert_search_results uses enqueue_db_update, which should handle producer logic.
            # No need to pass background_tasks if insert_search_results doesn't use it.
            insertion_successful_flag = await insert_search_results(
                results=results_for_batch_insertion, # Pass the prepared list of dicts
                logger=logger,
                file_id=file_id, # Pass original file_id for context in insert_search_results
                # background_tasks=None # Or pass if needed by insert_search_results
            )

            if insertion_successful_flag: # insert_search_results should return a bool
                inserted_or_merged_count = len(results_for_batch_insertion) # Assume all prepared were "inserted" (enqueued)
                logger.info(f"[{job_specific_log_id}] Successfully enqueued/processed batch insertion of {inserted_or_merged_count} results from warehouse via insert_search_results.")
                
                # Update status for the processed ScraperRecords
                entry_ids_that_were_processed = list(set(res[IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN] for res in results_for_batch_insertion))
                
                if entry_ids_that_were_processed:
                    update_scraper_record_status_sql = f"""
                        UPDATE {SCRAPER_RECORDS_TABLE_NAME}
                        SET {SCRAPER_RECORDS_ENTRY_STATUS_COLUMN} = {STATUS_WAREHOUSE_RESULT_POPULATED},
                            {SCRAPER_RECORDS_WAREHOUSE_MATCH_TIME_COLUMN} = GETUTCDATE()
                        WHERE {SCRAPER_RECORDS_PK_COLUMN} IN :entry_ids_list_param; 
                    """
                    # Ensure entry_ids_list_param is a list for the IN clause
                    scraper_status_update_params = {"entry_ids_list_param": entry_ids_that_were_processed}
                    
                    status_update_op_correlation_id = str(uuid.uuid4())
                    
                    logger.info(f"[{job_specific_log_id}] Enqueuing update of {SCRAPER_RECORDS_TABLE_NAME} status for {len(entry_ids_that_were_processed)} EntryIDs. Correlation ID: {status_update_op_correlation_id}")

                    # enqueue_db_update uses its own producer logic (global or new)
                    await enqueue_db_update(
                        file_id=job_specific_log_id, # Context for this operation
                        sql=update_scraper_record_status_sql,
                        params=scraper_status_update_params,
                        task_type="batch_update_scraper_status_warehouse_populated", # Descriptive task type
                        # producer_instance=local_api_producer, # Pass if specific instance needed, else enqueue_db_update handles it
                        correlation_id=status_update_op_correlation_id,
                        logger_param=logger
                    )
                    scraper_status_updates_enqueued_count = len(entry_ids_that_were_processed) # Or 1 if it's one task for many IDs
                else: # Should not happen if results_for_batch_insertion was populated
                    logger.warning(f"[{job_specific_log_id}] No EntryIDs extracted from results_for_batch_insertion, though insertion was reported successful. This is unexpected.")
            else: # insertion_successful_flag is False
                logger.error(f"[{job_specific_log_id}] Batch insertion/enqueuing of warehouse results via insert_search_results reported failure.")
                # Status of ScraperRecords remains PENDING_WAREHOUSE_CHECK
        
        # Construct final response message
        final_api_message = (
            f"Warehouse-to-Result processing for original FileID '{file_id}' completed. "
            f"Scraper Records Matched in DB: {scraper_records_matched_in_db}, "
            f"Results Prepared for Insertion: {len(results_for_batch_insertion)}, "
            f"Actual Insertions/Merges (via insert_search_results): {inserted_or_merged_count}, "
            f"Scraper Status Updates Enqueued: {scraper_status_updates_enqueued_count}, "
            f"Data Preparation Errors: {data_preparation_errors}."
        )
        logger.info(f"[{job_specific_log_id}] API: {final_api_message}")
        
        log_public_url_final_api = await upload_log_file(
            job_specific_log_id, log_filename_path, logger, db_record_file_id_to_update=file_id
        )
        
        return {
            "status": "success_enqueued" if inserted_or_merged_count > 0 else "no_new_insertions",
            "message": final_api_message,
            "job_specific_id": job_specific_log_id,
            "original_file_id": file_id,
            "scraper_records_matched_in_db": scraper_records_matched_in_db,
            "results_prepared_for_insertion": len(results_for_batch_insertion),
            "results_actually_inserted_or_merged": inserted_or_merged_count, # This reflects what insert_search_results did
            "scraper_status_updates_enqueued": scraper_status_updates_enqueued_count,
            "data_preparation_errors": data_preparation_errors,
            "log_url": log_public_url_final_api
        }

    except HTTPException as http_exc: # Re-raise HTTPExceptions
        # Log file upload might have happened if error was after producer init.
        # If producer was the cause, log might not be updated in DB by upload_log_file.
        # This specific check is complex due to where producer might fail.
        # upload_log_file itself will log if DB update part fails.
        logger.warning(f"[{job_specific_log_id}] API: HTTPException occurred: {http_exc.detail}")
        # Do not re-upload log here if it's already part of the HTTPException raising path (e.g. FileID not found)
        raise http_exc
    except Exception as e_api_critical:
        logger.critical(f"[{job_specific_log_id}] API: Critical error during warehouse-to-result processing for FileID '{file_id}': {e_api_critical}", exc_info=True)
        critical_error_log_url = await upload_log_file(
            job_specific_log_id, log_filename_path, logger, db_record_file_id_to_update=file_id
        )
        raise HTTPException(status_code=500, detail=f"Critical internal error. JobID: {job_specific_log_id}. Log: {critical_error_log_url or 'Log upload failed or N/A'}")
    finally:
        # Close local_api_producer if it was specifically created and used within this endpoint
        # if local_api_producer and local_api_producer.is_connected:
        #     await local_api_producer.close()
        #     logger.info(f"[{job_specific_log_id}] API: Closed explicitly obtained local RabbitMQ producer.")
        # else:
        #     logger.debug(f"[{job_specific_log_id}] API: No explicit local producer to close or was not connected.")
        
        logger.info(f"[{job_specific_log_id}] API: Endpoint finished processing for original FileID: {file_id}.")


# --- Other Endpoints (Simplified stubs for brevity, assume they are well-defined) ---
@router.post("/clear-ai-json/{file_id}", tags=["Database"])
async def api_clear_ai_json(
    file_id: str,
    entry_ids: Optional[List[int]] = Query(None, description="List of EntryIDs to clear AI data for, if not all"),
    # background_tasks: BackgroundTasks = None # Not used in this simplified version
):
    logger, log_filename = setup_job_logger(job_id=f"clear_ai_{file_id}", console_output=True)
    logger.info(f"API: Clearing AiJson and AiCaption for FileID: {file_id}" + (f", EntryIDs: {entry_ids}" if entry_ids else ""))
    try:
        # Validate FileID
        file_id_int = int(file_id)
        async with async_engine.connect() as conn:
            # ... (validation logic as before) ...
            pass # Placeholder

        # Construct SQL
        sql_clear = f"""
            UPDATE {IMAGE_SCRAPER_RESULT_TABLE_NAME}
            SET {IMAGE_SCRAPER_RESULT_AI_JSON_COLUMN} = NULL, {IMAGE_SCRAPER_RESULT_AI_CAPTION_COLUMN} = NULL
            /* FROM clause might be needed depending on DB for aliasing if joining for FileID filter */
            WHERE {IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN} IN (
                SELECT r.{SCRAPER_RECORDS_PK_COLUMN} 
                FROM {SCRAPER_RECORDS_TABLE_NAME} r 
                WHERE r.{SCRAPER_RECORDS_FILE_ID_FK_COLUMN} = :file_id_param
            )
        """
        params_clear = {"file_id_param": file_id_int}
        if entry_ids:
            sql_clear += f" AND {IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN} IN :entry_ids_param"
            # enqueue_db_update expects a list for IN clauses, not a tuple for pyodbc directly.
            params_clear["entry_ids_param"] = entry_ids # Pass as list
        
        # Ensure global producer is used by enqueue_db_update
        # producer_to_use = global_producer # Or let enqueue_db_update get it
        
        correlation_id_clear = str(uuid.uuid4())
        # enqueue_db_update now returns queue name or 1, not rows_affected for async tasks
        await enqueue_db_update(
            file_id=file_id, # Context
            sql=sql_clear,
            params=params_clear,
            task_type="clear_ai_data_task", # Descriptive
            # producer_instance=producer_to_use,
            correlation_id=correlation_id_clear,
            logger_param=logger
            # return_result=True # Only for direct SELECTs
        )
        logger.info(f"Enqueued AiJson/AiCaption clear task for FileID: {file_id}. CorrelationID: {correlation_id_clear}")
        
        # Verification can be a separate monitoring step or a delayed task, not immediate.
        log_public_url = await upload_log_file(f"clear_ai_{file_id}", log_filename, logger, db_record_file_id_to_update=file_id)
        return {
            "status": "success_enqueued",
            "message": f"Task to clear AI data for FileID: {file_id} has been enqueued.",
            "log_url": log_public_url,
            "correlation_id": correlation_id_clear
        }
    except ValueError as ve: # E.g. int(file_id) fails
        logger.error(f"API Clear AI Data: Value error for FileID {file_id}: {ve}", exc_info=True)
        await upload_log_file(f"clear_ai_{file_id}", log_filename, logger, db_record_file_id_to_update=file_id)
        raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException as http_e:
        raise http_e # Re-raise if already an HTTPException (like FileID not found)
    except Exception as e:
        logger.error(f"API Clear AI Data: Error for FileID {file_id}: {e}", exc_info=True)
        await upload_log_file(f"clear_ai_{file_id}", log_filename, logger, db_record_file_id_to_update=file_id)
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")
    # finally:
        # Global engine disposal is handled by lifespan
        # logger.info(f"API Clear AI Data: Disposed database engine for FileID {file_id}")


# ... (Other endpoints like /sort-by-search, /initial-sort, /restart-job etc. would follow a similar pattern:
#      - Setup job-specific logger
#      - Call the core logic function (e.g., update_sort_order, process_restart_batch)
#      - Handle results and exceptions
#      - Upload log
#      - Return response / HTTPException )

# Example for /restart-job, focusing on calling process_restart_batch correctly
@router.post("/restart-job/{file_id}", tags=["Processing"])
async def api_process_restart(
    file_id: str, 
    entry_id: Optional[int] = Query(None, description="Optional EntryID to start from."), # Use Query for docs
    background_tasks: Optional[BackgroundTasks] = None # Keep if used by callees
):
    job_log_id = f"restart_job_{file_id}_{entry_id or 'all'}"
    logger, log_filename_path = setup_job_logger(job_id=job_log_id, console_output=True)
    logger.info(f"API: Received request to restart batch processing for FileID: {file_id}" + (f", starting from EntryID: {entry_id}" if entry_id else ", starting from determined point."))

    try:
        # Validate file_id format (int) before passing to process_restart_batch
        try:
            file_id_int = int(file_id)
        except ValueError:
            logger.error(f"API RestartJob: Invalid FileID format '{file_id}'. Must be an integer.")
            await upload_log_file(job_log_id, log_filename_path, logger, db_record_file_id_to_update=file_id)
            raise HTTPException(status_code=400, detail="Invalid FileID format.")

        # Call the main processing function (process_restart_batch)
        # This function now handles its own producer logic.
        # It will use the global logger if one is not explicitly passed,
        # but run_job_with_logging (if used) would pass the job-specific one.
        # Here, we're calling it directly, so pass the job-specific logger.
        
        # If not using run_job_with_logging wrapper:
        processing_result_dict = await process_restart_batch(
            file_id_db_str=file_id, # Pass as string, it converts internally
            entry_id=entry_id,
            use_all_variations=False, # Default for this endpoint
            logger=logger, # Pass the job-specific logger
            background_tasks=background_tasks, # Pass along
            num_workers=4 # Example, make configurable if needed
        )

        log_final_url = await upload_log_file(job_log_id, log_filename_path, logger, db_record_file_id_to_update=file_id)
        processing_result_dict["log_public_url"] = log_final_url or processing_result_dict.get("log_public_url", "") # Update with latest URL

        if "error" in processing_result_dict:
            logger.error(f"API RestartJob: Batch processing for FileID {file_id} failed. Error: {processing_result_dict['error']}")
            raise HTTPException(status_code=500, detail=processing_result_dict['error'])
        
        logger.info(f"API RestartJob: Batch processing for FileID: {file_id} completed successfully. Result: {processing_result_dict}")
        return {
            "status_code": 200, 
            "message": f"Processing restart batch queued/completed for FileID: {file_id}", 
            "data": processing_result_dict # Return the detailed result from the batch processor
        }

    except HTTPException as http_e_restart: # Catch HTTPExceptions raised by process_restart_batch or validation
        raise http_e_restart
    except Exception as e_restart_api:
        logger.critical(f"API RestartJob: Unhandled critical error for FileID {file_id}: {e_restart_api}", exc_info=True)
        final_log_url_error = await upload_log_file(job_log_id, log_filename_path, logger, db_record_file_id_to_update=file_id)
        raise HTTPException(status_code=500, detail=f"Critical error during job restart for FileID {file_id}. Log URL: {final_log_url_error or 'N/A'}")
    # finally:
        # Global engine disposal by lifespan
        # logger.info(f"API RestartJob: Disposed database engine for FileID {file_id}")

# Pydantic models for /test-insert-results (if not already defined globally)
class SearchResult(BaseModel):
    EntryID: int = Field(..., description="Unique identifier for the entry")
    ImageUrl: Any #HttpUrl = Field(..., description="URL of the main image") # Relaxed for testing placeholders
    ImageDesc: Optional[str] = Field(None, description="Description of the image")
    ImageSource: Optional[Any] = Field(None, description="Source URL of the image") # Relaxed
    ImageUrlThumbnail: Optional[Any] = Field(None, description="URL of the thumbnail image") # Relaxed
    ProductCategory: Optional[str] = Field(None, description="Category of the product")

class InsertResultsRequest(BaseModel):
    results: List[SearchResult] = Field(..., description="List of search results to insert")
    # entry_ids: Optional[List[int]] = Field(None, description="Optional list of EntryIDs to restrict insertion") # Not used by insert_search_results

@router.post("/test-insert-results/{file_id}", tags=["Testing"])
async def api_test_insert_results(
    file_id: str,
    request_body: InsertResultsRequest, # Renamed from request to avoid conflict with FastAPI Request
    background_tasks: Optional[BackgroundTasks] = None # Keep if insert_search_results uses it
):
    job_id_test_insert = f"test_insert_{file_id}_{uuid.uuid4().hex[:8]}"
    logger, log_filename_test_insert = setup_job_logger(job_id=job_id_test_insert, console_output=True)
    
    logger.info(f"[{job_id_test_insert}] API TestInsert: Received request for FileID: {file_id}, Number of results: {len(request_body.results)}")

    try:
        file_id_as_int = int(file_id) # Validate format early

        # Validate FileID exists in utb_ImageScraperFiles
        async with async_engine.connect() as conn_val_file:
            val_file_q = text(f"SELECT COUNT(*) FROM {IMAGE_SCRAPER_FILES_TABLE_NAME} WHERE {IMAGE_SCRAPER_FILES_PK_COLUMN} = :fid")
            file_count_res = await conn_val_file.execute(val_file_q, {"fid": file_id_as_int})
            if file_count_res.scalar_one() == 0:
                logger.error(f"[{job_id_test_insert}] FileID {file_id} not found in {IMAGE_SCRAPER_FILES_TABLE_NAME}.")
                await upload_log_file(job_id_test_insert, log_filename_test_insert, logger, db_record_file_id_to_update=file_id)
                raise HTTPException(status_code=404, detail=f"FileID {file_id} not found.")
            file_count_res.close()

        # Prepare results for insert_search_results (map Pydantic models to dicts)
        results_to_insert_list = []
        for res_item in request_body.results:
            results_to_insert_list.append({
                "EntryID": res_item.EntryID,
                "ImageUrl": str(res_item.ImageUrl), # Convert HttpUrl to str
                "ImageDesc": res_item.ImageDesc,
                "ImageSource": str(res_item.ImageSource) if res_item.ImageSource else None,
                "ImageUrlThumbnail": str(res_item.ImageUrlThumbnail) if res_item.ImageUrlThumbnail else None,
                "ProductCategory": res_item.ProductCategory
                # Add CreateTime if insert_search_results expects it, or if it should be set here.
                # "CreateTime": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        
        logger.debug(f"[{job_id_test_insert}] Prepared {len(results_to_insert_list)} results for insertion. Sample: {results_to_insert_list[:1] if results_to_insert_list else 'None'}")

        # Call insert_search_results
        # This function should handle enqueuing the actual DB insert operations.
        insertion_enqueued_successfully = await insert_search_results(
            results=results_to_insert_list,
            logger=logger,
            file_id=file_id, # Pass original file_id for context
            background_tasks=background_tasks # Pass along
        )

        log_url_after_insert = await upload_log_file(job_id_test_insert, log_filename_test_insert, logger, db_record_file_id_to_update=file_id)

        if not insertion_enqueued_successfully:
            logger.warning(f"[{job_id_test_insert}] insert_search_results reported failure to enqueue/process for FileID {file_id}.")
            # It's possible insert_search_results handles partial successes/failures internally.
            # The boolean return might indicate overall attempt status.
            return { # Return a success response but indicate potential issues if flag is False
                "status": "partial_warning_or_failure_in_enqueue",
                "message": f"Test insertion task enqueuing reported issues for FileID: {file_id}. Check logs.",
                "log_url": log_url_after_insert,
                "details": {"file_id": file_id, "results_provided_count": len(results_to_insert_list)}
            }

        logger.info(f"[{job_id_test_insert}] Test insertion tasks successfully enqueued for FileID {file_id} via insert_search_results.")
        return {
            "status": "success_enqueued",
            "message": f"Test insertion tasks for {len(results_to_insert_list)} results for FileID: {file_id} were successfully enqueued.",
            "log_url": log_url_after_insert,
            "details": {"file_id": file_id, "results_enqueued_count": len(results_to_insert_list)}
        }

    except ValueError as ve_test_insert: # For int(file_id)
        logger.error(f"[{job_id_test_insert}] API TestInsert: Value error for FileID {file_id}: {ve_test_insert}", exc_info=True)
        await upload_log_file(job_id_test_insert, log_filename_test_insert, logger, db_record_file_id_to_update=file_id)
        raise HTTPException(status_code=400, detail=str(ve_test_insert))
    except HTTPException as http_e_test_insert:
        raise http_e_test_insert
    except Exception as e_test_insert:
        logger.critical(f"[{job_id_test_insert}] API TestInsert: Critical error for FileID {file_id}: {e_test_insert}", exc_info=True)
        final_log_url_error_test = await upload_log_file(job_id_test_insert, log_filename_test_insert, logger, db_record_file_id_to_update=file_id)
        raise HTTPException(status_code=500, detail=f"Critical error during test insertion for FileID {file_id}. Log: {final_log_url_error_test or 'N/A'}")
    # finally:
        # Global engine disposal by lifespan
        # logger.info(f"[{job_id_test_insert}] API TestInsert: Disposed database engine for FileID {file_id}")


app.include_router(router, prefix="/api/v5")