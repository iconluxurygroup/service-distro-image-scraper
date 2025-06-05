from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, APIRouter, Request
from pydantic import BaseModel, Field, HttpUrl as PydanticHttpUrl # Alias to avoid confusion
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
from typing import Optional, List, Dict, Any, Callable, Union
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from contextlib import asynccontextmanager
import signal
import uuid
import math
import sys
from collections import defaultdict
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
import aio_pika # Assuming RabbitMQProducer uses this
from multiprocessing import cpu_count
from math import ceil

# Assume these are defined in their respective modules
from icon_image_lib.google_parser import process_search_result
from common import generate_search_variations, fetch_brand_rules, preprocess_sku
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
    enqueue_db_update,
    update_file_generate_complete,
    update_file_location_complete,
)
from search_utils import update_search_sort_order, update_sort_order, update_sort_no_image_entry
from database_config import async_engine # Assuming this is your SQLAlchemy async engine
from config import BRAND_RULES_URL, VERSION, SEARCH_PROXY_API_URL, RABBITMQ_URL, DATAPROXY_API_KEY # VERSION should be '6.0.0' or similar
from email_utils import send_message_email
from urllib.parse import urlparse
from url_extract import extract_thumbnail_url
from rabbitmq_producer import RabbitMQProducer # Assuming RabbitMQProducer.get_producer() is singleton

# --- Constants ---
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
IMAGE_SCRAPER_RESULT_SOURCE_TYPE_COLUMN = "SourceType" # New field for Warehouse/Google

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
# Add other columns as needed: FileName, UploadTime, UserID, SendToEmail, FileGenerateCompleteTime, FileLocationCompleteTime etc.

# Status Values for SCRAPER_RECORDS_ENTRY_STATUS_COLUMN
STATUS_PENDING_WAREHOUSE_CHECK = 0
STATUS_WAREHOUSE_CHECK_NO_MATCH = 1 # Warehouse check done, no match found
STATUS_WAREHOUSE_RESULT_POPULATED = 2 # Warehouse result found and populated into ScraperResult
STATUS_PENDING_GOOGLE_SEARCH = 3 # Ready for Google Search (e.g., if warehouse had no match or to supplement)
STATUS_GOOGLE_SEARCH_COMPLETE = 4 # Google search done and results populated
# --- End Constants ---

app = FastAPI(title="Super Scraper API", version=VERSION) # VERSION should be like "6.0.0"

# Setup default logger for the application itself
default_logger = logging.getLogger("super_scraper_api")
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    default_logger.addHandler(ch)

router = APIRouter()
global_producer: Optional[RabbitMQProducer] = None
JOB_STATUS: Dict[str, Dict[str, Any]] = {} # For tracking job statuses if needed by endpoints
LAST_UPLOAD: Dict[tuple, Dict[str, Any]] = {} # Cache for log uploads


@asynccontextmanager
async def lifespan(app_instance: FastAPI): # Renamed app to app_instance
    default_logger.info(f"FastAPI application startup sequence initiated (Version: {VERSION}).")
    global global_producer

    try:
        default_logger.info("Initializing global RabbitMQ producer...")
        async with asyncio.timeout(30): # Increased timeout for robust connection
            global_producer = await RabbitMQProducer.get_producer() # Should be singleton
        if not global_producer or not global_producer.is_connected:
            default_logger.critical("CRITICAL: Failed to initialize or connect global RabbitMQ producer. Application will not function correctly.")
            # Depending on policy, might sys.exit(1) or raise an error that stops FastAPI
            # For now, log critical and continue, but most operations will fail.
            # sys.exit(1) # Uncomment if this is a hard requirement for startup
        else:
            default_logger.info("Global RabbitMQ producer initialized and connected successfully.")

        # Startup Test Insertion (optional, can be disabled for production)
        RUN_STARTUP_TEST_INSERT = os.getenv("RUN_STARTUP_TEST_INSERT", "false").lower() == "true"
        if RUN_STARTUP_TEST_INSERT:
            default_logger.info("Running test insertion of search result on startup...")
            test_file_id = f"startup_test_{uuid.uuid4().hex[:8]}"
            test_logger, test_log_filename = setup_job_logger(job_id=test_file_id, console_output=False) # Usually don't want console output for this
            try:
                # Ensure a corresponding FileID exists or handle it gracefully
                # For a true test, you might insert a temporary FileID into utb_ImageScraperFiles
                # Here, we assume insert_search_results can handle file_id context without DB FK strictness for this test,
                # or that the test_file_id is just for logging/grouping RabbitMQ tasks.

                sample_result_data = [
                    {
                        "EntryID": 99999, # Use a distinct EntryID for tests
                        "ImageUrl": "https://via.placeholder.com/150/0000FF/808080?Text=TestImage",
                        "ImageDesc": "Automated startup test image description",
                        "ImageSource": "https://placeholder.com",
                        "ImageUrlThumbnail": "https://via.placeholder.com/50/0000FF/808080?Text=Thumb"
                    }
                ]
                # `insert_search_results` uses `enqueue_db_update` which relies on `global_producer`
                success_flag = await insert_search_results(
                    results=sample_result_data,
                    logger=test_logger,
                    file_id=test_file_id, # Context for the task, not necessarily a DB FK
                    background_tasks=BackgroundTasks() # Provide if signature requires
                )
                if success_flag: # This indicates successful enqueuing
                    test_logger.info(f"Test search result enqueued successfully for EntryID 99999, Context FileID {test_file_id}.")
                    # Verification would require checking RabbitMQ queue or waiting for consumer and checking DB.
                    # For startup, enqueuing success is usually sufficient.
                else:
                    test_logger.error(f"Failed to enqueue test search result for EntryID 99999, Context FileID {test_file_id}.")
            except Exception as e_startup_test:
                test_logger.error(f"Error during startup test insertion: {e_startup_test}", exc_info=True)
                # Do not exit app for a failed test, but log as critical if desired.
            finally:
                if os.path.exists(test_log_filename):
                    await upload_log_file(test_file_id, test_log_filename, test_logger, db_record_file_id_to_update=None) # No DB update for this ephemeral test log
        else:
            default_logger.info("Skipping startup test insertion (RUN_STARTUP_TEST_INSERT is not true).")


        # Setup signal handlers for graceful shutdown
        loop = asyncio.get_running_loop()
        # shutdown_event = asyncio.Event() # Not strictly needed if handlers directly call cleanup

        def handle_shutdown_signal(signal_name: str):
            default_logger.info(f"Received shutdown signal: {signal_name}. Initiating graceful shutdown...")
            # Perform any immediate shutdown preparations if needed before the 'finally' block
            # For example, stop accepting new requests (FastAPI handles this mostly)
            # shutdown_event.set() # If other parts of the app need to react to this event

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, handle_shutdown_signal, sig.name)

        default_logger.info("FastAPI application startup sequence completed.")
        yield # Application runs here

    finally:
        default_logger.info("FastAPI application shutdown sequence initiated.")
        if global_producer and global_producer.is_connected:
            default_logger.info("Closing global RabbitMQ producer connection...")
            await global_producer.close()
            default_logger.info("Global RabbitMQ producer closed.")
        
        if async_engine: # Check if engine was initialized
            default_logger.info("Disposing database engine...")
            await async_engine.dispose()
            default_logger.info("Database engine disposed.")
        default_logger.info("FastAPI application shutdown sequence completed.")

app.lifespan = lifespan

class SearchClient:
    def __init__(self, endpoint: str, logger: logging.Logger, max_concurrency: int = 10):
        self.endpoint = endpoint
        self.logger = logger
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.api_key = DATAPROXY_API_KEY
        # Headers should be set per-session or per-request if they can change
        self._aiohttp_session: Optional[aiohttp.ClientSession] = None
        self.regions = ['northamerica-northeast', 'us-east', 'southamerica', 'us-central', 'us-west', 'europe', 'australia', 'asia', 'middle-east']
        self.request_headers = { # Store headers for session creation
            "accept": "application/json",
            "x-api-key": self.api_key,
            "Content-Type": "application/json"
        }

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._aiohttp_session is None or self._aiohttp_session.closed:
            self.logger.debug("Creating new aiohttp.ClientSession for SearchClient.")
            # You can configure connector limits here if needed, e.g., aiohttp.TCPConnector(limit_per_host=...)
            self._aiohttp_session = aiohttp.ClientSession(headers=self.request_headers)
        return self._aiohttp_session

    async def close(self):
        if self._aiohttp_session and not self._aiohttp_session.closed:
            self.logger.debug("Closing SearchClient's aiohttp.ClientSession.")
            await self._aiohttp_session.close()
            self._aiohttp_session = None
        else:
            self.logger.debug("SearchClient's aiohttp.ClientSession already closed or not initialized.")

    @retry(
        stop=stop_after_attempt(3), # Total attempts for this specific search call (across regions)
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, json.JSONDecodeError, asyncio.TimeoutError)),
        before_sleep=before_sleep_log(default_logger, logging.WARNING) # Log tenacity retries
    )
    async def search_single_term_all_regions(self, term: str, brand: str, entry_id: int) -> List[Dict]: # Renamed for clarity
        async with self.semaphore: # Limits concurrent calls to this method
            process_info = psutil.Process() # Info about current process
            search_url_google = f"https://www.google.com/search?q={urllib.parse.quote(term)}&tbm=isch"
            
            current_session = await self._get_session()

            for region_idx, region_name in enumerate(self.regions):
                current_fetch_endpoint = f"{self.endpoint}?region={region_name}"
                self.logger.info(
                    f"PID {process_info.pid}: Attempting search for term='{term}' (EntryID {entry_id}, Brand='{brand}') "
                    f"via {current_fetch_endpoint} (Region {region_idx+1}/{len(self.regions)}: {region_name})"
                )
                try:
                    # Timeout for each individual region request
                    async with asyncio.timeout(45): # Slightly shorter timeout per region
                        async with current_session.post(current_fetch_endpoint, json={"url": search_url_google}) as response:
                            response_text_content = await response.text()
                            response_preview = response_text_content[:250] if response_text_content else "[EMPTY RESPONSE BODY]"
                            
                            self.logger.debug(
                                f"PID {process_info.pid}: Response from {current_fetch_endpoint} for term='{term}': "
                                f"Status={response.status}, Preview='{response_preview}'"
                            )

                            if response.status in (429, 503): # Rate limit or service unavailable
                                self.logger.warning(
                                    f"PID {process_info.pid}: Service temporarily unavailable (Status {response.status}) for '{term}' in region {region_name}. "
                                    f"Content: {response_preview}"
                                )
                                if region_idx == len(self.regions) - 1: # Last region attempt
                                    self.logger.error(f"PID {process_info.pid}: Service unavailable for '{term}' after trying all regions.")
                                    # Let retry policy handle this if it's a ClientError, or fall through to empty list.
                                    # Raising a specific error can trigger tenacity if it's in retry_if_exception_type
                                    raise aiohttp.ClientResponseError(response.request_info, response.history, status=response.status, message="Service unavailable after all regions", headers=response.headers)
                                await asyncio.sleep(1 + region_idx * 0.5) # Small delay before trying next region
                                continue # Try next region

                            response.raise_for_status() # Raises for other 4xx/5xx errors

                            try:
                                api_json_response = await response.json()
                            except json.JSONDecodeError as json_e:
                                self.logger.error(
                                    f"PID {process_info.pid}: JSONDecodeError for term='{term}' in region {region_name}. "
                                    f"Status: {response.status}, Body Preview: {response_preview}. Error: {json_e}", exc_info=True
                                )
                                if region_idx == len(self.regions) - 1: raise # Re-raise if last region to trigger retry
                                continue # Try next region
                            
                            # Assuming 'result' contains the HTML string or similar content
                            html_content_from_api = api_json_response.get("result")
                            if not html_content_from_api:
                                self.logger.warning(
                                    f"PID {process_info.pid}: 'result' field missing or empty in API response for term='{term}' "
                                    f"in region {region_name}. Full response: {api_json_response}"
                                )
                                # If no content, but 200 OK, this region yielded no data. Try next.
                                continue

                            # Ensure content is bytes for process_search_result
                            html_bytes = html_content_from_api.encode('utf-8') if isinstance(html_content_from_api, str) else str(html_content_from_api).encode('utf-8')
                            
                            # process_search_result expects (html_bytes, entry_id, logger)
                            formatted_results_df = process_search_result(html_bytes, entry_id, self.logger)
                            
                            if not formatted_results_df.empty:
                                self.logger.info(
                                    f"PID {process_info.pid}: Successfully found {len(formatted_results_df)} results for term='{term}' "
                                    f"in region {region_name}."
                                )
                                # Convert DataFrame rows to list of dicts
                                return [
                                    {
                                        "EntryID": entry_id, # Comes from parameter
                                        "ImageUrl": row_data.get("ImageUrl", "placeholder://no-image-url-in-df"),
                                        "ImageDesc": row_data.get("ImageDesc", "N/A"),
                                        "ImageSource": row_data.get("ImageSource", "placeholder://no-source-in-df"), # Ensure HttpUrl compatible
                                        "ImageUrlThumbnail": row_data.get("ImageUrlThumbnail", row_data.get("ImageUrl", "placeholder://no-thumbnail-in-df"))
                                    }
                                    for _, row_data in formatted_results_df.iterrows()
                                ]
                            else:
                                self.logger.warning(f"PID {process_info.pid}: `process_search_result` returned empty DataFrame for term='{term}' in region {region_name}.")
                                # Try next region if results are empty from parsing.

                except asyncio.TimeoutError:
                    self.logger.warning(f"PID {process_info.pid}: Request timeout for term='{term}' in region {region_name}.")
                    if region_idx == len(self.regions) - 1: # Timeout on last region
                        self.logger.error(f"PID {process_info.pid}: Search for '{term}' timed out after trying all regions.")
                        # Let tenacity handle retry if configured for TimeoutError
                        raise # Reraise to be caught by tenacity
                except aiohttp.ClientError as client_e: # Catch other client errors not handled above
                    self.logger.warning(f"PID {process_info.pid}: ClientError for term='{term}' in region {region_name}: {client_e}", exc_info=True)
                    if region_idx == len(self.regions) - 1:
                        self.logger.error(f"PID {process_info.pid}: Search for '{term}' failed with ClientError after all regions.")
                        raise # Reraise for tenacity
                # General exception catcher for unexpected issues within a region's attempt
                except Exception as e_region:
                    self.logger.error(f"PID {process_info.pid}: Unexpected error processing term='{term}' in region {region_name}: {e_region}", exc_info=True)
                    if region_idx == len(self.regions) - 1:
                        self.logger.error(f"PID {process_info.pid}: Search for '{term}' failed with an unexpected error after all regions.")
                        # Consider if this should reraise for tenacity or just return empty
            
            # If loop completes without returning, all regions failed for this term
            self.logger.error(
                f"PID {process_info.pid}: All regions failed to yield results for term='{term}' (EntryID {entry_id}). "
                "This might trigger a retry by Tenacity or return an empty list if retries exhausted."
            )
            return [] # Return empty list if all regions (and retries for this call) failed

async def orchestrate_entry_search(
    original_search_term: str, # e.g., ProductModel from DB
    original_brand: Optional[str],
    entry_id: int,
    search_api_endpoint: str,
    use_all_variations: bool,
    file_id_context: Union[str, int], # For logging
    logger: logging.Logger,
    brand_rules: Dict # Pass fetched brand_rules
) -> List[Dict]:
    """
    Orchestrates the search for a single entry:
    1. Preprocesses SKU.
    2. Generates search variations.
    3. Iterates variations, calling SearchClient.
    4. Collects and returns valid results.
    """
    logger.info(f"Orchestrating search for EntryID {entry_id} (FileID {file_id_context}). Original term: '{original_search_term}', Brand: '{original_brand}'.")
    
    # 1. Preprocess SKU to get the base search string and brand to use
    # preprocess_sku returns (search_string, brand, model, color)
    # We primarily need the processed search_string and brand for generating variations.
    processed_search_term, processed_brand, _, _ = await preprocess_sku(
        search_string=original_search_term,
        known_brand=original_brand,
        brand_rules=brand_rules, # Pass pre-fetched brand rules
        logger=logger
    )
    logger.debug(f"EntryID {entry_id}: Preprocessed to Term='{processed_search_term}', Brand='{processed_brand}'.")

    # 2. Generate search variations based on the processed terms
    search_variations_map = await generate_search_variations(
        search_string=processed_search_term,
        brand=processed_brand,
        logger=logger
    )

    if not search_variations_map:
        logger.warning(f"EntryID {entry_id}: No search variations generated for Term='{processed_search_term}', Brand='{processed_brand}'.")
        return [_create_placeholder_result(entry_id, "no-search-variations-generated", f"No variations for '{processed_search_term}'")]

    all_valid_results_collected: List[Dict] = []
    
    # Define order of variation types if specific priority is desired
    variation_type_priority = [
        "default", "brand_alias", "model_alias", "delimiter_variations",
        "color_variations", "no_color", "category_specific"
    ]

    search_client = SearchClient(endpoint=search_api_endpoint, logger=logger)
    try:
        for variation_type in variation_type_priority:
            terms_for_type = search_variations_map.get(variation_type, [])
            if not terms_for_type:
                logger.debug(f"EntryID {entry_id}: No terms for variation type '{variation_type}'.")
                continue

            logger.info(f"EntryID {entry_id}: Processing {len(terms_for_type)} terms for variation type '{variation_type}'.")
            for specific_search_term in terms_for_type:
                logger.debug(f"EntryID {entry_id}: Searching term '{specific_search_term}' (type: {variation_type}).")
                
                # `search_single_term_all_regions` handles retries and region iteration for this term
                term_results = await search_client.search_single_term_all_regions(
                    term=specific_search_term,
                    brand=processed_brand, # Use the processed brand for context if needed by search endpoint/logic
                    entry_id=entry_id
                )

                if term_results: # term_results is List[Dict]
                    logger.info(f"EntryID {entry_id}: Found {len(term_results)} results for term '{specific_search_term}'.")
                    # Filter out any placeholder results that might have slipped through `search_single_term_all_regions`
                    # and ensure required fields (though search_single_term_all_regions should format correctly)
                    for res_dict in term_results:
                        if res_dict.get("ImageUrl") and not str(res_dict["ImageUrl"]).startswith("placeholder://"):
                            # Perform final lightweight processing if necessary (e.g., thumbnail extraction if not done)
                            # For now, assume search_client already provides a good thumbnail.
                            # If ImageSource needs to be HttpUrl, ensure it's valid or None
                            if isinstance(res_dict.get("ImageSource"), str) and not res_dict["ImageSource"].startswith("http"):
                                res_dict["ImageSource"] = None # Or a placeholder valid URL

                            all_valid_results_collected.append(res_dict)
                else:
                    logger.debug(f"EntryID {entry_id}: No results for term '{specific_search_term}'.")
            
            # If results found and not using all variations, break after this type
            if all_valid_results_collected and not use_all_variations:
                logger.info(f"EntryID {entry_id}: Found results and `use_all_variations` is false. Stopping after type '{variation_type}'.")
                break
        
    except Exception as e_orchestrate: # Catch errors during the orchestration loop itself
        logger.error(f"EntryID {entry_id}: Error during search orchestration: {e_orchestrate}", exc_info=True)
        # Fall through to return collected results or placeholder if none
    finally:
        await search_client.close() # Ensure client's session is closed

    if not all_valid_results_collected:
        logger.warning(f"EntryID {entry_id}: No valid image results found after all search attempts for Original='{original_search_term}'.")
        return [_create_placeholder_result(entry_id, "no-valid-results-all-searches", f"No results for '{original_search_term}'")]

    logger.info(f"EntryID {entry_id}: Orchestration complete. Total {len(all_valid_results_collected)} valid results found for Original='{original_search_term}'.")
    return all_valid_results_collected

def _create_placeholder_result(entry_id: int, type_suffix: str, description: str) -> Dict:
    """Helper to create consistent placeholder results."""
    return {
        "EntryID": entry_id,
        "ImageUrl": f"placeholder://{type_suffix}",
        "ImageDesc": description,
        "ImageSource": "N/A", # Or make it placeholder PydanticHttpUrl compatible
        "ImageUrlThumbnail": f"placeholder://{type_suffix}-thumb",
        "ProductCategory": "" # Ensure all expected keys are present
    }

# `process_results` function can be removed if its logic is integrated into `orchestrate_entry_search` or `SearchClient`.
# `process_and_tag_results` is effectively replaced by `orchestrate_entry_search`.

async def run_job_with_logging(
    job_func: Callable[..., Any], 
    file_id_context: str, # The FileID context for this job run (logging, etc.)
    **kwargs: Any # Arguments for the job_func
) -> Dict[str, Any]:
    job_specific_logger, log_file_path = setup_job_logger(job_id=file_id_context, console_output=True)
    
    result_payload: Optional[Any] = None
    debug_info_dict = {"memory_usage_mb": {}, "log_file_server_path": log_file_path, "errors_encountered": []}
    http_status_code = 500 # Default to internal server error
    response_message = "Job execution failed unexpectedly."

    # Determine function name for clearer logging
    job_function_name = getattr(job_func, '__name__', 'unnamed_job_function')

    try:
        job_specific_logger.info(f"Starting job '{job_function_name}' for context FileID: {file_id_context}")
        current_process = psutil.Process()
        debug_info_dict["memory_usage_mb"]["before_job"] = round(current_process.memory_info().rss / (1024 * 1024), 2)
        job_specific_logger.debug(f"Memory before job '{job_function_name}': {debug_info_dict['memory_usage_mb']['before_job']:.2f} MB")

        # Pass the job-specific logger to the job function if it accepts a 'logger' argument
        if 'logger' in job_func.__code__.co_varnames:
            kwargs['logger'] = job_specific_logger
        
        # Pass file_id_context as 'file_id' or 'file_id_db_str' if the job_func expects it by that name
        # This assumes job_func takes file_id as its first positional arg after logger, or a kwarg
        # Example: job_func(file_id, logger=logger, **other_kwargs)
        # If file_id is always the first arg, it could be: job_func(file_id_context, **kwargs)
        # For now, assume 'file_id' or 'file_id_db_str' is a kwarg or handled by **kwargs if positional.
        # The specific functions like process_restart_batch now take file_id_db_str.

        if asyncio.iscoroutinefunction(job_func):
            # If 'file_id' is a common first arg for these jobs:
            # result_payload = await job_func(file_id_context, **kwargs)
            # If it's passed via kwargs:
            if 'file_id' in job_func.__code__.co_varnames: kwargs['file_id'] = file_id_context
            elif 'file_id_db_str' in job_func.__code__.co_varnames: kwargs['file_id_db_str'] = file_id_context
            
            result_payload = await job_func(**kwargs) # Pass file_id_context via kwargs if job_func expects it
        else: # For synchronous functions, run in a thread pool
            # Similar logic for passing file_id_context
            if 'file_id' in job_func.__code__.co_varnames: kwargs['file_id'] = file_id_context
            elif 'file_id_db_str' in job_func.__code__.co_varnames: kwargs['file_id_db_str'] = file_id_context

            result_payload = await asyncio.to_thread(job_func, **kwargs)
        
        debug_info_dict["memory_usage_mb"]["after_job"] = round(current_process.memory_info().rss / (1024 * 1024), 2)
        job_specific_logger.debug(f"Memory after job '{job_function_name}': {debug_info_dict['memory_usage_mb']['after_job']:.2f} MB")
        
        mem_increase = debug_info_dict["memory_usage_mb"]["after_job"] - debug_info_dict["memory_usage_mb"]["before_job"]
        if mem_increase > 500: # Configurable threshold for significant memory increase
            job_specific_logger.warning(
                f"Job '{job_function_name}' resulted in a memory increase of {mem_increase:.2f} MB. "
                f"Before: {debug_info_dict['memory_usage_mb']['before_job']:.2f} MB, After: {debug_info_dict['memory_usage_mb']['after_job']:.2f} MB."
            )

        job_specific_logger.info(f"Successfully completed job '{job_function_name}' for context FileID: {file_id_context}")
        http_status_code = 200
        response_message = f"Job '{job_function_name}' for context FileID '{file_id_context}' completed successfully."
        
    except Exception as e:
        job_specific_logger.error(f"Error during execution of job '{job_function_name}' for context FileID {file_id_context}: {str(e)}", exc_info=True)
        # debug_info_dict["error_traceback"] = traceback.format_exc() # Already logged with exc_info
        debug_info_dict["errors_encountered"].append({
            "error_message": str(e), 
            "error_type": type(e).__name__,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        })
        response_message = f"Job '{job_function_name}' for context FileID {file_id_context} failed: {str(e)}"
        # http_status_code remains 500
        
    finally:
        # Upload the log file regardless of job success or failure
        # db_record_file_id_to_update should be the *actual* FileID from utb_ImageScraperFiles table.
        # file_id_context might be a more specific job run ID.
        # Assuming file_id_context is the one to use for DB record update if they are the same.
        uploaded_log_url = await upload_log_file(
            job_id_for_log=file_id_context, # S3 path uses this job-specific ID
            log_file_path=log_file_path, 
            logger_instance=job_specific_logger,
            db_record_file_id_to_update=file_id_context # Assume file_id_context is the PK in utb_ImageScraperFiles
        )
        debug_info_dict["log_s3_url"] = uploaded_log_url

    return {
        "status_code": http_status_code,
        "message": response_message,
        "data": result_payload, # This is the actual result from job_func
        "debug_info": debug_info_dict
    }


async def run_generate_download_file(
    file_id: str, 
    parent_logger: logging.Logger, # Logger from the calling context
    # log_file_path_of_parent: str, # This log path is for the parent job, not this specific task
    background_tasks: Optional[BackgroundTasks] # Keep if needed, though seems unused for remote call
):
    # This function acts as a client to another service/endpoint.
    # It should have its own logging context if it's complex, or use parent_logger.
    # For simplicity, using parent_logger with a prefix.
    log_prefix = f"[GenerateDownloadFile Client | FileID {file_id}]"
    parent_logger.info(f"{log_prefix} Initiating request to generate download file.")

    job_key = f"generate_download_{file_id}" # Key for JOB_STATUS
    JOB_STATUS[job_key] = {
        "status": "initiating_generation_request",
        "message": "Requesting download file generation.",
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "file_id": file_id
    }

    try:
        # URL of the service that actually generates the file
        # This needs to be configured and should not be the same host/port if this function IS that service.
        # Example: os.getenv("FILE_GENERATION_SERVICE_URL")
        # Using placeholder from original code, assuming it's a different microservice.
        # IMPORTANT: Ensure this doesn't cause recursion if it calls back to this app's endpoint.
        file_generation_endpoint = f"https://icon7-8001.iconluxury.today/generate-download-file/?file_id={file_id}" # External endpoint
        parent_logger.info(f"{log_prefix} Calling external file generation service: {file_generation_endpoint}")

        async with httpx.AsyncClient(timeout=300.0) as client: # Long timeout for potentially slow generation
            response = await client.post(
                file_generation_endpoint,
                headers={"accept": "application/json"}
                # No data="" payload unless the target service expects an empty POST body explicitly.
            )
            response.raise_for_status() # Raises HTTPStatusError for 4xx/5xx
            service_response_data = response.json()
        
        parent_logger.info(f"{log_prefix} Response from file generation service: {service_response_data}")

        # Interpret service_response_data (this depends on the contract with that service)
        if service_response_data.get("status_code") == 200 and service_response_data.get("public_url"):
            JOB_STATUS[job_key].update({
                "status": "generation_successful_reported_by_service",
                "message": "File generation service reported success.",
                "public_url": service_response_data["public_url"],
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
            })
            parent_logger.info(f"{log_prefix} File generation successful. URL: {service_response_data['public_url']}")
            # Potentially update DB record for file_id with this public_url if applicable
            await update_file_location_complete(file_id, service_response_data["public_url"], parent_logger)

        else: # Error or unexpected response from service
            error_detail = service_response_data.get("error", service_response_data.get("message", "Unknown issue from generation service."))
            JOB_STATUS[job_key].update({
                "status": "generation_failed_reported_by_service",
                "message": f"File generation service reported failure: {error_detail}",
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
            })
            parent_logger.error(f"{log_prefix} File generation service reported failure: {error_detail}. Full response: {service_response_data}")

    except httpx.HTTPStatusError as hse:
        parent_logger.error(f"{log_prefix} HTTP error calling file generation service: Status {hse.response.status_code}, Response: {hse.response.text}", exc_info=True)
        JOB_STATUS[job_key].update({
            "status": "generation_request_http_error",
            "message": f"HTTP error with generation service: {hse.response.status_code}",
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        })
    except Exception as e:
        parent_logger.error(f"{log_prefix} Unexpected error during file generation request: {e}", exc_info=True)
        JOB_STATUS[job_key].update({
            "status": "generation_request_unexpected_error",
            "message": f"Unexpected error: {str(e)}",
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        })

async def upload_log_file(
    job_id_for_s3_path: str, # ID used for constructing the S3 object key (e.g., unique job run ID)
    local_log_file_path: str,
    logger_instance: logging.Logger, # The logger to use for messages from this function
    db_record_file_id_to_update: Optional[str] = None # The FileID (PK) in utb_ImageScraperFiles to update with the URL
) -> Optional[str]:
    
    log_prefix = f"[LogUpload | JobS3PathID {job_id_for_s3_path} | DBFileID {db_record_file_id_to_update or 'N/A'}]"
    logger_instance.debug(f"{log_prefix} Attempting to upload log file: {local_log_file_path}")

    # Define the retryable upload logic as an inner async function
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10), # Exponential backoff: 2s, 4s, 8s
        retry=retry_if_exception_type(Exception), # Retry on any exception during upload attempt
        before_sleep=lambda rs: logger_instance.warning(
            f"{log_prefix} Retrying log upload for '{local_log_file_path}' (Attempt {rs.attempt_number}/{rs.retry_state.stop.max_attempt_number}). Waiting {rs.next_action.sleep:.2f}s. Reason: {type(rs.outcome.exception()).__name__}"
        )
    )
    async def _try_upload_log_to_s3_async():
        if not await asyncio.to_thread(os.path.exists, local_log_file_path):
            logger_instance.warning(f"{log_prefix} Log file '{local_log_file_path}' does not exist. Skipping upload.")
            return None

        # Read file content and calculate hash asynchronously
        try:
            async with aiofiles.open(local_log_file_path, "rb") as f: # Requires `aiofiles` library
                log_content_bytes = await f.read()
        except ImportError: # Fallback if aiofiles not installed (should be a dependency)
             with open(local_log_file_path, "rb") as f:
                log_content_bytes = await asyncio.to_thread(f.read)

        current_file_hash_hex = await asyncio.to_thread(lambda b: hashlib.md5(b).hexdigest(), log_content_bytes)
        
        # Check cache to avoid redundant uploads of identical, recently uploaded files
        cache_key = (local_log_file_path, job_id_for_s3_path) # Cache key based on path and job ID
        current_timestamp = time.time()
        if cache_key in LAST_UPLOAD:
            cached_info = LAST_UPLOAD[cache_key]
            if cached_info["hash"] == current_file_hash_hex and \
               (current_timestamp - cached_info["time"]) < 120: # 2-minute debounce window
                logger_instance.info(f"{log_prefix} Log file '{local_log_file_path}' unchanged and recently uploaded. Using cached URL: {cached_info['url']}")
                return cached_info["url"]

        # S3 object key for the log file
        s3_log_object_key = f"job_logs/job_{job_id_for_s3_path}.log" # Ensure consistent naming
        
        logger_instance.debug(f"{log_prefix} Uploading '{local_log_file_path}' to S3 as '{s3_log_object_key}'.")

        # `upload_file_to_space` should be async or wrapped if it's blocking
        # Assuming it's already async as per typical modern library usage.
        uploaded_s3_url = await upload_file_to_space(
            file_src=local_log_file_path, # Source local file path
            save_as=s3_log_object_key,    # Target S3 key
            is_public=True,
            logger=logger_instance,       # Pass logger for its internal use
            file_id=job_id_for_s3_path    # Context for upload_file_to_space logging
        )

        if not uploaded_s3_url:
            logger_instance.error(f"{log_prefix} S3 upload via `upload_file_to_space` returned an empty URL for '{s3_log_object_key}'.")
            # This will cause a retry due to the @retry decorator if it's an exception or if we raise one.
            # For now, returning None will be handled by the outer try/except.
            # To make tenacity retry this, this function should raise an exception here.
            raise ValueError(f"S3 upload returned empty URL for {s3_log_object_key}")


        logger_instance.info(f"{log_prefix} Log file successfully uploaded to S3: {uploaded_s3_url}")

        # Update the database record if a specific FileID for DB update is provided
        if db_record_file_id_to_update:
            logger_instance.debug(f"{log_prefix} Updating log URL in database for FileID '{db_record_file_id_to_update}'.")
            # `update_log_url_in_db` should use `enqueue_db_update` for async DB operation.
            # It returns a boolean indicating if the enqueue was successful.
            enqueue_success = await update_log_url_in_db(
                file_id_to_update_in_db=db_record_file_id_to_update, 
                log_url=uploaded_s3_url, 
                logger=logger_instance
            )
            if not enqueue_success:
                logger_instance.warning(
                    f"{log_prefix} Failed to enqueue database update for log URL for FileID '{db_record_file_id_to_update}'. "
                    f"S3 upload was successful: {uploaded_s3_url}"
                )
                # Decide if this is critical enough to mark the overall log upload as failed.
                # For now, S3 success is primary.
        else:
            logger_instance.debug(f"{log_prefix} No 'db_record_file_id_to_update' provided. Skipping database log URL update.")

        # Cache the successful upload details
        LAST_UPLOAD[cache_key] = {"hash": current_file_hash_hex, "time": current_timestamp, "url": uploaded_s3_url}
        return uploaded_s3_url

    try:
        final_uploaded_url = await _try_upload_log_to_s3_async()
        return final_uploaded_url
    except Exception as e_all_retries_failed:
        logger_instance.error(
            f"{log_prefix} All attempts to upload log file '{local_log_file_path}' failed: {e_all_retries_failed}", 
            exc_info=True
        )
        return None

async def process_restart_batch(
    file_id_db_str: str,
    entry_id: Optional[int] = None, # Starting EntryID for processing
    use_all_variations: bool = False,
    logger: logging.Logger = default_logger, # Provided by run_job_with_logging usually
    background_tasks: Optional[BackgroundTasks] = None, # Pass through
    num_workers: int = 4, # Logical workers, influences batching/concurrency hints
) -> Dict[str, Any]:
    
    # log_filename is derived from logger's handlers if possible, or default pattern
    # This helps if logger is already configured by run_job_with_logging.
    current_log_filename = "unknown_log_file.log"
    if logger.handlers and hasattr(logger.handlers[0], 'baseFilename'):
        current_log_filename = logger.handlers[0].baseFilename
    else: # Fallback if logger has no file handler or baseFilename attribute
        current_log_filename = os.path.join("job_logs", f"job_{file_id_db_str}_process_restart.log") # Ensure "job_logs" dir exists
        # If logger was passed without a file handler, this path might not match where it's actually logging.
        # This is a best-effort guess. setup_job_logger should be the source of truth.

    logger.info(
        f"Initiating 'process_restart_batch' for FileID: {file_id_db_str}. "
        f"Start EntryID: {entry_id or 'Determine Automatically'}, UseAllVariations: {use_all_variations}, Workers: {num_workers}."
    )
    
    current_process_info = psutil.Process()
    def _log_resource_usage(context_message: str = ""):
        mem_info_rss_mb = current_process_info.memory_info().rss / (1024**2)
        cpu_percent_val = current_process_info.cpu_percent(interval=0.05) # Non-blocking after first call
        logger.debug(
            f"Resource Usage {context_message} (PID {current_process_info.pid}): RSS={mem_info_rss_mb:.2f} MB, CPU={cpu_percent_val:.1f}%"
        )
        # Add warnings for high usage if needed (e.g., >1GB RAM, >80% CPU sustained)

    _log_resource_usage("at start of process_restart_batch")

    try:
        file_id_for_db = int(file_id_db_str)
    except ValueError:
        error_msg = f"Invalid FileID format: '{file_id_db_str}'. Must be an integer."
        logger.error(error_msg)
        # Log upload attempt before returning error
        log_url = await upload_log_file(file_id_db_str, current_log_filename, logger, db_record_file_id_to_update=file_id_db_str)
        return {
            "error": error_msg, "log_filename": current_log_filename, 
            "log_public_url": log_url or "", "last_entry_id_processed": str(entry_id or "")
        }

    # --- Configuration for this batch run ---
    # BATCH_SIZE: Number of entries to process in one "logical" batch by asyncio.gather
    # MAX_CONCURRENT_ENTRY_PROCESSING: Limits how many `_process_single_entry_with_retry` run concurrently.
    # MAX_SEARCH_CLIENT_CONCURRENCY: Max concurrent SearchClient HTTP requests (within SearchClient via its semaphore).
    # MAX_ENTRY_ATTEMPTS: Retries for processing a single EntryID if it fails.
    
    BATCH_SIZE_PER_GATHER = max(1, min(20, num_workers * 2)) # Adjust based on typical entry processing time
    MAX_CONCURRENT_ENTRY_PROCESSING = max(num_workers, 5) # How many entries are in flight at once
    MAX_ENTRY_ATTEMPTS = 3
    # Search API endpoint from config
    configured_search_endpoint = SEARCH_PROXY_API_URL
    
    logger.debug(
        f"Batch Config for FileID {file_id_for_db}: BATCH_SIZE_PER_GATHER={BATCH_SIZE_PER_GATHER}, "
        f"MAX_CONCURRENT_ENTRY_PROCESSING={MAX_CONCURRENT_ENTRY_PROCESSING}, MAX_ENTRY_ATTEMPTS={MAX_ENTRY_ATTEMPTS}."
    )

    # --- RabbitMQ Producer ---
    # Should be available globally via RabbitMQProducer.get_producer() after lifespan init.
    # enqueue_db_update uses this implicitly if no producer_instance is passed.
    # No need to explicitly get/connect it here if all DB writes use enqueue_db_update's default.

    # --- Validate FileID in Database ---
    try:
        async with async_engine.connect() as db_conn_validate:
            file_check_result = await db_conn_validate.execute(
                text(f"SELECT COUNT(*) FROM {IMAGE_SCRAPER_FILES_TABLE_NAME} WHERE {IMAGE_SCRAPER_FILES_PK_COLUMN} = :fid"),
                {"fid": file_id_for_db}
            )
            if file_check_result.scalar_one() == 0:
                error_msg = f"FileID {file_id_for_db} not found in {IMAGE_SCRAPER_FILES_TABLE_NAME}."
                logger.error(error_msg)
                log_url = await upload_log_file(file_id_db_str, current_log_filename, logger, db_record_file_id_to_update=file_id_db_str)
                return {"error": error_msg, "log_filename": current_log_filename, "log_public_url": log_url or "", "last_entry_id_processed": str(entry_id or "")}
            file_check_result.close()
    except SQLAlchemyError as db_exc_validate:
        error_msg = f"Database error validating FileID {file_id_for_db}: {db_exc_validate}"
        logger.error(error_msg, exc_info=True)
        log_url = await upload_log_file(file_id_db_str, current_log_filename, logger, db_record_file_id_to_update=file_id_db_str)
        return {"error": error_msg, "log_filename": current_log_filename, "log_public_url": log_url or "", "last_entry_id_processed": str(entry_id or "")}

    # --- Determine Starting EntryID ---
    # If entry_id is None, determine where to resume or start from.
    # Logic: if last_valid_entry exists, find MIN(EntryID) > last_valid_entry with Step1 IS NULL.
    # If that's not found, find MIN(EntryID) with Step1 IS NULL for the whole file.
    # If still not found, or if entry_id was 0, start from very beginning (EntryID >= 0 or actual min).
    actual_start_entry_id = entry_id # Use provided entry_id if not None
    if actual_start_entry_id is None:
        logger.debug(f"FileID {file_id_for_db}: `entry_id` is None. Determining resume point.")
        # ... (fetch_last_valid_entry and subsequent queries as in original, simplified here) ...
        # This logic should query for the lowest EntryID that has Step1 IS NULL and optionally no good results.
        # For now, assume entry_id passed is the correct start, or it means "from beginning of unprocessed".
        # Query for entries will handle "EntryID >= :start_id" and "Step1 IS NULL".
        actual_start_entry_id = 0 # Default to start from beginning of unprocessed if not specified
        logger.info(f"FileID {file_id_for_db}: No specific start `entry_id`. Will process unprocessed entries from ID {actual_start_entry_id}.")


    # --- Fetch Brand Rules ---
    # This should be robust, with retries and fallbacks.
    brand_rules_data = await fetch_brand_rules(BRAND_RULES_URL, max_attempts=3, timeout=15, logger=logger)
    if not brand_rules_data:
        logger.warning(f"FileID {file_id_for_db}: Failed to fetch brand rules. Using minimal fallback: Scotch & Soda.")
        brand_rules_data = {"Scotch & Soda": {"aliases": ["Scotch and Soda", "S&S"], "sku_pattern": r"^\d{6,8}[a-zA-Z0-9]*$"}}
    if "Scotch & Soda" not in brand_rules_data: # Ensure default exists
        brand_rules_data["Scotch & Soda"] = {"aliases": ["Scotch and Soda", "S&S"], "sku_pattern": r"^\d{6,8}[a-zA-Z0-9]*$"}
    logger.debug(f"FileID {file_id_for_db}: Using {len(brand_rules_data)} brand rules.")


    # --- Fetch Entries to Process from Database ---
    entries_to_process_list: List[tuple] = []
    try:
        async with async_engine.connect() as db_conn_fetch:
            # Query for entries:
            # - Belonging to file_id_for_db
            # - EntryID >= actual_start_entry_id
            # - ProductModel (search term) is not NULL
            # - Step1 IS NULL (primary condition: not yet processed)
            # - OR (has no results in utb_ImageScraperResult OR all results have SortOrder <= 0) - for reprocessing
            # Order by EntryID to process sequentially.
            sql_fetch_entries = text(f"""
                SELECT r.{SCRAPER_RECORDS_PK_COLUMN}, r.{SCRAPER_RECORDS_PRODUCT_MODEL_COLUMN}, 
                       r.{SCRAPER_RECORDS_PRODUCT_BRAND_COLUMN}, r.{SCRAPER_RECORDS_PRODUCT_COLOR_COLUMN}, 
                       r.{SCRAPER_RECORDS_PRODUCT_CATEGORY_COLUMN}
                FROM {SCRAPER_RECORDS_TABLE_NAME} r
                LEFT JOIN ( -- Subquery to check if an entry has any "good" results
                    SELECT DISTINCT res_in.{IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN}
                    FROM {IMAGE_SCRAPER_RESULT_TABLE_NAME} res_in
                    WHERE res_in.{IMAGE_SCRAPER_RESULT_SORT_ORDER_COLUMN} IS NOT NULL AND res_in.{IMAGE_SCRAPER_RESULT_SORT_ORDER_COLUMN} > 0
                ) good_res ON r.{SCRAPER_RECORDS_PK_COLUMN} = good_res.{IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN}
                WHERE r.{SCRAPER_RECORDS_FILE_ID_FK_COLUMN} = :fid
                  AND r.{SCRAPER_RECORDS_PK_COLUMN} >= :start_eid
                  AND r.{SCRAPER_RECORDS_PRODUCT_MODEL_COLUMN} IS NOT NULL AND r.{SCRAPER_RECORDS_PRODUCT_MODEL_COLUMN} <> ''
                  AND (r.{SCRAPER_RECORDS_STEP1_COLUMN} IS NULL OR good_res.{IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN} IS NULL) -- Process if Step1 NULL or no good results
                ORDER BY r.{SCRAPER_RECORDS_PK_COLUMN};
            """)
            db_result_entries = await db_conn_fetch.execute(sql_fetch_entries, {"fid": file_id_for_db, "start_eid": actual_start_entry_id})
            entries_to_process_list = db_result_entries.fetchall() # List of tuples
            db_result_entries.close()
        logger.info(f"FileID {file_id_for_db}: Fetched {len(entries_to_process_list)} entries for processing (starting from actual_start_entry_id {actual_start_entry_id}).")
    except SQLAlchemyError as db_exc_fetch:
        error_msg = f"Database error fetching entries for FileID {file_id_for_db}: {db_exc_fetch}"
        logger.error(error_msg, exc_info=True)
        log_url = await upload_log_file(file_id_db_str, current_log_filename, logger, db_record_file_id_to_update=file_id_db_str)
        return {"error": error_msg, "log_filename": current_log_filename, "log_public_url": log_url or "", "last_entry_id_processed": str(entry_id or "")}

    if not entries_to_process_list:
        success_msg = f"No entries require processing for FileID {file_id_for_db} (from start_id {actual_start_entry_id})."
        logger.info(success_msg)
        log_url = await upload_log_file(file_id_db_str, current_log_filename, logger, db_record_file_id_to_update=file_id_db_str)
        return {
            "message": success_msg, "file_id": file_id_db_str, "total_entries_fetched_for_processing": 0,
            "successful_entries_processed": 0, "failed_entries_processed": 0,
            "log_filename": current_log_filename, "log_public_url": log_url or "",
            "last_entry_id_processed": str(entry_id or actual_start_entry_id or 0) # Last requested or determined start
        }

    # --- Batching Logic ---
    batched_entry_groups = [
        entries_to_process_list[i:i + BATCH_SIZE_PER_GATHER] 
        for i in range(0, len(entries_to_process_list), BATCH_SIZE_PER_GATHER)
    ]
    logger.info(f"FileID {file_id_for_db}: Divided {len(entries_to_process_list)} entries into {len(batched_entry_groups)} batches of (up to) {BATCH_SIZE_PER_GATHER} each.")

    # --- Counters and State ---
    count_successful_entries = 0
    count_failed_entries = 0
    # Keep track of the highest EntryID that was *successfully* processed in this run.
    max_successful_entry_id_this_run = actual_start_entry_id or 0

    # --- Semaphore for Concurrent Entry Processing ---
    # Limits how many _process_single_entry_with_retry tasks run in parallel via asyncio.gather.
    entry_processing_semaphore = asyncio.Semaphore(MAX_CONCURRENT_ENTRY_PROCESSING)

    # --- Helper: Process a Single Entry (with retries) ---
    async def _process_single_entry_with_retry(entry_data_tuple: tuple) -> tuple[int, bool]:
        entry_id_curr, model_curr, brand_curr, color_curr, category_curr = entry_data_tuple
        async with entry_processing_semaphore: # Acquire semaphore for this entry's processing lifecycle
            for attempt in range(1, MAX_ENTRY_ATTEMPTS + 1):
                logger.info(f"FileID {file_id_for_db}, EntryID {entry_id_curr}: Attempt {attempt}/{MAX_ENTRY_ATTEMPTS} processing.")
                try:
                    # orchestrate_entry_search encapsulates preprocessing, variation gen, search, result formatting
                    search_results = await orchestrate_entry_search(
                        original_search_term=model_curr,
                        original_brand=brand_curr,
                        entry_id=entry_id_curr,
                        search_api_endpoint=configured_search_endpoint,
                        use_all_variations=use_all_variations,
                        file_id_context=file_id_for_db, # Pass FileID for logging inside
                        logger=logger,
                        brand_rules=brand_rules_data # Pass fetched rules
                    )
                    
                    # Filter out placeholders if orchestrate_entry_search can return them on total failure
                    valid_results = [r for r in search_results if not r["ImageUrl"].startswith("placeholder://")]

                    if not valid_results:
                        logger.warning(f"FileID {file_id_for_db}, EntryID {entry_id_curr}: No valid results after orchestration (Attempt {attempt}).")
                        # If last attempt and still no results, this entry is considered failed for this run.
                        # No Step1 update, will be picked up again if logic allows.
                        if attempt == MAX_ENTRY_ATTEMPTS:
                             logger.error(f"FileID {file_id_for_db}, EntryID {entry_id_curr}: Failed to get valid results after {MAX_ENTRY_ATTEMPTS} attempts.")
                             return entry_id_curr, False # Failed
                        await asyncio.sleep(attempt * 1.5) # Exponential backoff before retry
                        continue # Next attempt for this entry

                    # Enqueue results for insertion (insert_search_results uses enqueue_db_update)
                    enqueue_success = await insert_search_results(
                        results=valid_results,
                        logger=logger,
                        file_id=file_id_db_str, # Pass string FileID for context
                        background_tasks=background_tasks # Pass along
                    )

                    if enqueue_success:
                        logger.info(f"FileID {file_id_for_db}, EntryID {entry_id_curr}: Successfully enqueued {len(valid_results)} results.")
                        # Mark Step1 as complete for this entry
                        step1_update_sql = f"UPDATE {SCRAPER_RECORDS_TABLE_NAME} SET {SCRAPER_RECORDS_STEP1_COLUMN} = GETUTCDATE() WHERE {SCRAPER_RECORDS_PK_COLUMN} = :eid"
                        # enqueue_db_update is fire-and-forget here unless return_result=True for a SELECT
                        await enqueue_db_update(
                            file_id=file_id_db_str, sql=step1_update_sql, params={"eid": entry_id_curr},
                            task_type=f"update_step1_col_entry_{entry_id_curr}", correlation_id=str(uuid.uuid4()),
                            logger_param=logger
                        )
                        return entry_id_curr, True # Success for this entry
                    else: # Enqueue for insertion failed
                        logger.error(f"FileID {file_id_for_db}, EntryID {entry_id_curr}: Failed to enqueue results (Attempt {attempt}).")
                        if attempt == MAX_ENTRY_ATTEMPTS: return entry_id_curr, False # Failed
                        await asyncio.sleep(attempt * 1.5)
                        continue # Next attempt

                except Exception as e_entry_proc:
                    logger.error(f"FileID {file_id_for_db}, EntryID {entry_id_curr}: Exception during attempt {attempt}: {e_entry_proc}", exc_info=True)
                    if attempt == MAX_ENTRY_ATTEMPTS:
                        logger.critical(f"FileID {file_id_for_db}, EntryID {entry_id_curr}: Failed critically after {MAX_ENTRY_ATTEMPTS} attempts.")
                        return entry_id_curr, False # Critical failure for this entry
                    await asyncio.sleep(attempt * 2) # Longer backoff for general exceptions
            
            # Should not be reached if attempts loop correctly returns
            return entry_id_curr, False 

    # --- Main Batch Processing Loop ---
    for batch_idx, entry_group in enumerate(batched_entry_groups, 1):
        logger.info(f"FileID {file_id_for_db}: Processing batch {batch_idx}/{len(batched_entry_groups)} with {len(entry_group)} entries.")
        batch_start_time = time.monotonic()
        
        # Concurrently process all entries in this group
        # asyncio.gather collects results or exceptions.
        entry_processing_outcomes = await asyncio.gather(
            *[_process_single_entry_with_retry(entry_tuple) for entry_tuple in entry_group],
            return_exceptions=True # Store exceptions instead of raising immediately
        )

        # Evaluate outcomes for this batch
        for original_entry_tuple, outcome_result in zip(entry_group, entry_processing_outcomes):
            entry_id_processed = original_entry_tuple[0] # PK from the tuple
            if isinstance(outcome_result, Exception): # An exception occurred in _process_single_entry_with_retry
                logger.error(f"FileID {file_id_for_db}, EntryID {entry_id_processed}: Processing failed with unhandled exception: {outcome_result}", exc_info=outcome_result)
                count_failed_entries += 1
            elif isinstance(outcome_result, tuple) and len(outcome_result) == 2:
                _, success_status = outcome_result
                if success_status:
                    count_successful_entries += 1
                    max_successful_entry_id_this_run = max(max_successful_entry_id_this_run, entry_id_processed)
                else:
                    count_failed_entries += 1
            else: # Unexpected outcome format
                logger.error(f"FileID {file_id_for_db}, EntryID {entry_id_processed}: Received unexpected outcome: {outcome_result}")
                count_failed_entries += 1
        
        batch_duration_s = time.monotonic() - batch_start_time
        logger.info(
            f"FileID {file_id_for_db}: Batch {batch_idx} completed in {batch_duration_s:.2f}s. "
            f"Current totals - Successful: {count_successful_entries}, Failed: {count_failed_entries}."
        )
        _log_resource_usage(f"after batch {batch_idx}")
        await asyncio.sleep(0.2) # Small pause between batches if desired

    logger.info(f"FileID {file_id_for_db}: All batches processed. Final - Successful: {count_successful_entries}, Failed: {count_failed_entries} out of {len(entries_to_process_list)} attempted.")

    # --- Post-Processing Steps ---
    if count_successful_entries > 0:
        logger.info(f"FileID {file_id_for_db}: Enqueuing update for ImageCompleteTime as successful entries were processed.")
        complete_time_sql = f"UPDATE {IMAGE_SCRAPER_FILES_TABLE_NAME} SET {IMAGE_SCRAPER_FILES_IMAGE_COMPLETE_TIME_COLUMN} = GETUTCDATE() WHERE {IMAGE_SCRAPER_FILES_PK_COLUMN} = :fid"
        await enqueue_db_update(
            file_id=file_id_db_str, sql=complete_time_sql, params={"fid": file_id_for_db},
            task_type=f"update_img_complete_time_file_{file_id_for_db}", correlation_id=str(uuid.uuid4()),
            logger_param=logger
        )

    # Trigger sort order update and download file generation
    logger.info(f"FileID {file_id_for_db}: Initiating post-search tasks (sorting, file generation).")
    try:
        # `update_sort_order` should ideally be robust and log its own progress/errors.
        # It might also use `enqueue_db_update` for its operations.
        await update_sort_order(file_id_db_str, logger=logger, background_tasks=background_tasks)
        logger.info(f"FileID {file_id_for_db}: Sort order update task initiated/completed.")

        # `run_generate_download_file` calls an external service.
        await run_generate_download_file(file_id_db_str, logger, background_tasks) # Removed parent log path
        logger.info(f"FileID {file_id_for_db}: Download file generation task initiated.")
    except Exception as e_post_tasks:
        logger.error(f"FileID {file_id_for_db}: Error during post-search tasks: {e_post_tasks}", exc_info=True)


    # --- Final Reporting and Email ---
    final_log_s3_url = await upload_log_file(file_id_db_str, current_log_filename, logger, db_record_file_id_to_update=file_id_db_str)
    
    email_to_list = await get_send_to_email(file_id_for_db, logger=logger)
    if email_to_list:
        subject = f"SuperScraper Batch Processing Report for FileID: {file_id_db_str}"
        body = (
            f"Batch processing for FileID {file_id_db_str} has finished.\n\n"
            f"Total entries fetched for this run: {len(entries_to_process_list)}\n"
            f"Successfully processed entries: {count_successful_entries}\n"
            f"Failed entries this run: {count_failed_entries}\n"
            f"Highest EntryID successfully processed: {max_successful_entry_id_this_run}\n"
            f"Settings: Use All Variations={use_all_variations}, Num Workers (Hint)={num_workers}\n"
            f"Detailed log available at: {final_log_s3_url or 'Log upload failed or not configured.'}\n\n"
            "Post-search tasks (sorting, file generation) have been initiated."
        )
        try :
            await send_message_email(email_to_list, subject=subject, message=body, logger=logger)
            logger.info(f"FileID {file_id_for_db}: Completion email sent to: {email_to_list}")
        except Exception as e_email_send:
            logger.error(f"FileID {file_id_for_db}: Failed to send completion email: {e_email_send}", exc_info=True)

    return {
        "message": "Search processing batch completed.",
        "file_id": file_id_db_str,
        "total_entries_fetched_for_processing": len(entries_to_process_list),
        "successful_entries_processed": count_successful_entries,
        "failed_entries_processed": count_failed_entries,
        "last_entry_id_successfully_processed_in_run": max_successful_entry_id_this_run,
        "log_filename_on_server": current_log_filename,
        "log_public_url": final_log_s3_url or "N/A",
        "settings_used": {"use_all_variations": use_all_variations, "num_workers_hint": num_workers}
    }

@router.post("/populate-results-from-warehouse/{file_id}", tags=["Processing", "Warehouse v6"])
async def api_populate_results_from_warehouse(
    # request: Request, # Keep if client IP or other request details are needed
    file_id: str, 
    limit: Optional[int] = Query(1000, ge=1, le=10000, description=f"Max records from {SCRAPER_RECORDS_TABLE_NAME} to process."),
    base_image_url: str = Query("https://cms.rtsplusdev.com/files/icon_warehouse_images", description="Base URL for warehouse image paths.")
):
    job_run_id = f"warehouse_populate_{file_id}_{uuid.uuid4().hex[:6]}"
    logger, log_file_path = setup_job_logger(job_id=job_run_id, console_output=True)
    logger.info(f"[{job_run_id}] API Call: Populate results from warehouse for FileID '{file_id}'. Limit: {limit}.")

    # Counters
    num_scraper_records_matched = 0
    num_results_prepared = 0
    num_results_enqueued_for_insertion = 0
    num_status_updates_enqueued = 0
    num_data_prep_errors = 0

    try:
        try:
            file_id_int = int(file_id)
        except ValueError:
            logger.error(f"[{job_run_id}] Invalid FileID format: '{file_id}'. Must be an integer.")
            await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
            raise HTTPException(status_code=400, detail=f"Invalid FileID format: {file_id}")

        # Validate FileID exists
        async with async_engine.connect() as conn:
            file_exists_q = await conn.execute(text(f"SELECT 1 FROM {IMAGE_SCRAPER_FILES_TABLE_NAME} WHERE {IMAGE_SCRAPER_FILES_PK_COLUMN} = :fid"), {"fid": file_id_int})
            if not file_exists_q.scalar_one_or_none():
                logger.error(f"[{job_run_id}] FileID '{file_id_int}' not found in {IMAGE_SCRAPER_FILES_TABLE_NAME}.")
                await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id) # Log for original file_id
                raise HTTPException(status_code=404, detail=f"FileID {file_id_int} not found.")
            file_exists_q.close()

        # Fetch records from SCRAPER_RECORDS_TABLE_NAME that match WAREHOUSE_IMAGES_TABLE_NAME criteria
        # and are pending warehouse check (EntryStatus = STATUS_PENDING_WAREHOUSE_CHECK or NULL)
        async with async_engine.connect() as conn:
            fetch_sql = text(f"""
                SELECT TOP (:limit_val)
                    isr.{SCRAPER_RECORDS_PK_COLUMN} AS ScraperEntryID,
                    isr.{SCRAPER_RECORDS_PRODUCT_MODEL_COLUMN} AS ScraperProductModel,
                    isr.{SCRAPER_RECORDS_PRODUCT_BRAND_COLUMN} AS ScraperProductBrand,
                    iwi.{WAREHOUSE_IMAGES_MODEL_NUMBER_COLUMN} AS WarehouseModelNumber,
                    iwi.{WAREHOUSE_IMAGES_MODEL_CLEAN_COLUMN} AS WarehouseModelClean,
                    iwi.{WAREHOUSE_IMAGES_MODEL_FOLDER_COLUMN} AS WarehouseModelFolder,
                    iwi.{WAREHOUSE_IMAGES_MODEL_SOURCE_COLUMN} AS WarehouseModelSource
                FROM {SCRAPER_RECORDS_TABLE_NAME} isr
                JOIN {WAREHOUSE_IMAGES_TABLE_NAME} iwi ON isr.{SCRAPER_RECORDS_PRODUCT_MODEL_COLUMN} = iwi.{WAREHOUSE_IMAGES_MODEL_CLEAN_COLUMN}
                WHERE isr.{SCRAPER_RECORDS_FILE_ID_FK_COLUMN} = :fid
                  AND (isr.{SCRAPER_RECORDS_ENTRY_STATUS_COLUMN} = {STATUS_PENDING_WAREHOUSE_CHECK} OR isr.{SCRAPER_RECORDS_ENTRY_STATUS_COLUMN} IS NULL)
                  AND iwi.{WAREHOUSE_IMAGES_MODEL_CLEAN_COLUMN} IS NOT NULL AND iwi.{WAREHOUSE_IMAGES_MODEL_CLEAN_COLUMN} <> ''
                  AND iwi.{WAREHOUSE_IMAGES_MODEL_FOLDER_COLUMN} IS NOT NULL AND iwi.{WAREHOUSE_IMAGES_MODEL_FOLDER_COLUMN} <> ''
                ORDER BY isr.{SCRAPER_RECORDS_PK_COLUMN};
            """)
            matched_records_cursor = await conn.execute(fetch_sql, {"fid": file_id_int, "limit_val": limit})
            records_for_processing = matched_records_cursor.mappings().fetchall() # List of dict-like RowProxy
            matched_records_cursor.close()
            num_scraper_records_matched = len(records_for_processing)

        if not records_for_processing:
            msg = f"No records pending warehouse check found for FileID '{file_id_int}' matching warehouse criteria."
            logger.info(f"[{job_run_id}] {msg}")
            log_s3_url = await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
            return {"status": "no_action_required", "message": msg, "log_url": log_s3_url}

        logger.info(f"[{job_run_id}] Found {num_scraper_records_matched} scraper records with warehouse matches to process.")

        # Prepare result dicts for insertion
        results_to_insert_payload: List[Dict] = []
        processed_entry_ids_for_status_update: List[int] = []

        for rec_map in records_for_processing:
            try:
                scraper_eid = rec_map['ScraperEntryID']
                model_clean = rec_map.get('WarehouseModelClean')
                model_folder = rec_map.get('WarehouseModelFolder')

                if not model_clean or not model_folder: # Should be caught by SQL, but defensive check
                    logger.warning(f"[{job_run_id}] Skipping ScraperEntryID {scraper_eid}: Critical warehouse data missing (ModelClean/Folder).")
                    num_data_prep_errors += 1
                    continue
                
                # Construct ImageUrl from warehouse data
                # Remove common image extensions from model_clean for URL part, then add .png default
                model_url_part = model_clean
                for ext in ['.png', '.jpg', '.jpeg', '.webp']:
                    if model_url_part.lower().endswith(ext):
                        model_url_part = model_url_part[:-len(ext)]
                        break
                
                img_url = f"{base_image_url.rstrip('/')}/{model_folder.strip('/')}/{model_url_part}.png"
                
                desc = f"{rec_map.get('ScraperProductBrand') or 'Brand'} {rec_map.get('WarehouseModelNumber') or rec_map.get('ScraperProductModel') or 'Product'}"
                source_domain = urlparse(base_image_url).netloc or "warehouse.internal"
                actual_source = rec_map.get('WarehouseModelSource') or source_domain

                results_to_insert_payload.append({
                    IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN: scraper_eid,
                    IMAGE_SCRAPER_RESULT_IMAGE_URL_COLUMN: img_url,
                    IMAGE_SCRAPER_RESULT_IMAGE_DESC_COLUMN: desc,
                    IMAGE_SCRAPER_RESULT_IMAGE_SOURCE_COLUMN: actual_source,
                    IMAGE_SCRAPER_RESULT_IMAGE_URL_THUMBNAIL_COLUMN: img_url, # Thumbnail is same as main for warehouse
                    IMAGE_SCRAPER_RESULT_SORT_ORDER_COLUMN: 1, # Prioritize warehouse results
                    IMAGE_SCRAPER_RESULT_SOURCE_TYPE_COLUMN: 'Warehouse'
                })
                processed_entry_ids_for_status_update.append(scraper_eid)
                num_results_prepared += 1
            except Exception as e_prep:
                logger.error(f"[{job_run_id}] Error preparing result data for ScraperEntryID {rec_map.get('ScraperEntryID', 'UNKNOWN')}: {e_prep}", exc_info=True)
                num_data_prep_errors += 1
        
        # Batch insert results if any were prepared
        if results_to_insert_payload:
            logger.info(f"[{job_run_id}] Enqueuing insertion of {len(results_to_insert_payload)} results from warehouse.")
            # `insert_search_results` uses `enqueue_db_update` internally.
            insertion_enqueued = await insert_search_results(
                results=results_to_insert_payload,
                logger=logger,
                file_id=file_id # Original FileID for context within insert_search_results
            )
            if insertion_enqueued:
                num_results_enqueued_for_insertion = len(results_to_insert_payload)
                logger.info(f"[{job_run_id}] Successfully enqueued {num_results_enqueued_for_insertion} warehouse results for insertion.")

                # Enqueue status update for the processed ScraperRecords
                if processed_entry_ids_for_status_update:
                    unique_entry_ids = list(set(processed_entry_ids_for_status_update))
                    status_update_sql = f"""
                        UPDATE {SCRAPER_RECORDS_TABLE_NAME}
                        SET {SCRAPER_RECORDS_ENTRY_STATUS_COLUMN} = {STATUS_WAREHOUSE_RESULT_POPULATED},
                            {SCRAPER_RECORDS_WAREHOUSE_MATCH_TIME_COLUMN} = GETUTCDATE()
                        WHERE {SCRAPER_RECORDS_PK_COLUMN} IN :eids_list;
                    """
                    # enqueue_db_update expects list for IN clause, not tuple for pyodbc
                    await enqueue_db_update(
                        file_id=job_run_id, # Log context for this task
                        sql=status_update_sql,
                        params={"eids_list": unique_entry_ids}, # Pass as list
                        task_type="batch_update_scraper_status_warehouse_populated",
                        correlation_id=str(uuid.uuid4()),
                        logger_param=logger
                    )
                    num_status_updates_enqueued = len(unique_entry_ids)
                    logger.info(f"[{job_run_id}] Enqueued status update for {num_status_updates_enqueued} scraper records.")
            else:
                logger.error(f"[{job_run_id}] `insert_search_results` reported failure to enqueue warehouse results.")
        
        final_message = (
            f"Warehouse-to-Result processing for FileID '{file_id}' summary: "
            f"Scraper Records Matched: {num_scraper_records_matched}, Results Prepared: {num_results_prepared}, "
            f"Results Enqueued for Insertion: {num_results_enqueued_for_insertion}, "
            f"Status Updates Enqueued: {num_status_updates_enqueued}, Data Prep Errors: {num_data_prep_errors}."
        )
        logger.info(f"[{job_run_id}] {final_message}")
        final_log_s3_url = await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
        
        return {
            "status": "processing_enqueued" if num_results_enqueued_for_insertion > 0 else "no_new_insertions_made",
            "message": final_message,
            "job_run_id": job_run_id,
            "original_file_id": file_id,
            "counts": {
                "scraper_records_matched": num_scraper_records_matched,
                "results_prepared": num_results_prepared,
                "results_enqueued_for_insertion": num_results_enqueued_for_insertion,
                "status_updates_enqueued": num_status_updates_enqueued,
                "data_preparation_errors": num_data_prep_errors
            },
            "log_url": final_log_s3_url
        }

    except HTTPException as http_exc:
        # Log already uploaded by the code raising HTTPException or handled before this point
        logger.warning(f"[{job_run_id}] Caught HTTPException: {http_exc.detail}")
        raise http_exc
    except Exception as e_main:
        logger.critical(f"[{job_run_id}] Critical unhandled error in warehouse population API for FileID '{file_id}': {e_main}", exc_info=True)
        critical_err_log_url = await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
        raise HTTPException(status_code=500, detail=f"Critical internal error. Job Run ID: {job_run_id}. Log: {critical_err_log_url or 'Log upload failed.'}")


@router.post("/clear-ai-json/{file_id}", tags=["Database v6"])
async def api_clear_ai_json(
    file_id: str,
    entry_ids: Optional[List[int]] = Query(None, description="Optional list of EntryIDs to clear AI data for. If None, clears for all under FileID."),
    # background_tasks: BackgroundTasks # Not used if enqueue_db_update is direct RabbitMQ
):
    job_run_id = f"clear_ai_data_{file_id}_{uuid.uuid4().hex[:6]}"
    logger, log_file_path = setup_job_logger(job_id=job_run_id, console_output=True)
    logger.info(f"[{job_run_id}] API Call: Clear AI JSON/Caption for FileID: {file_id}" + (f", specific EntryIDs: {entry_ids}" if entry_ids else "."))

    try:
        file_id_int = int(file_id) # Validate format

        # Validate FileID exists (optional, can be skipped if DB handles FK constraints or if task is idempotent)
        async with async_engine.connect() as conn:
            # ... (FileID validation logic as in previous endpoint) ...
            pass

        # Construct SQL to update utb_ImageScraperResult
        # Clear AiJson and AiCaption for results linked to records under the given FileID
        base_sql_clear = f"""
            UPDATE {IMAGE_SCRAPER_RESULT_TABLE_NAME}
            SET {IMAGE_SCRAPER_RESULT_AI_JSON_COLUMN} = NULL, 
                {IMAGE_SCRAPER_RESULT_AI_CAPTION_COLUMN} = NULL
            WHERE {IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN} IN (
                SELECT r.{SCRAPER_RECORDS_PK_COLUMN} 
                FROM {SCRAPER_RECORDS_TABLE_NAME} r 
                WHERE r.{SCRAPER_RECORDS_FILE_ID_FK_COLUMN} = :fid_param
            )
        """
        params_for_clear: Dict[str, Any] = {"fid_param": file_id_int}

        if entry_ids: # If specific EntryIDs are provided, add them to the WHERE clause
            base_sql_clear += f" AND {IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN} IN :eids_list_param"
            params_for_clear["eids_list_param"] = entry_ids # Pass as list for enqueue_db_update

        task_correlation_id = str(uuid.uuid4())
        await enqueue_db_update(
            file_id=job_run_id, # Context for the RabbitMQ task log
            sql=base_sql_clear,
            params=params_for_clear,
            task_type="clear_ai_data_for_file_entries",
            correlation_id=task_correlation_id,
            logger_param=logger # Pass logger to enqueue_db_update for its internal logging
            # return_result=False for UPDATE statements
        )
        
        success_msg = f"Task to clear AI data for FileID '{file_id}' (EntryIDs: {'All' if not entry_ids else entry_ids}) enqueued successfully."
        logger.info(f"[{job_run_id}] {success_msg} CorrelationID: {task_correlation_id}")
        
        final_log_s3_url = await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
        return {
            "status": "task_enqueued",
            "message": success_msg,
            "correlation_id": task_correlation_id,
            "log_url": final_log_s3_url
        }

    except ValueError: # For int(file_id)
        err_msg = f"Invalid FileID format: '{file_id}'."
        logger.error(f"[{job_run_id}] {err_msg}", exc_info=True)
        await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
        raise HTTPException(status_code=400, detail=err_msg)
    except Exception as e_main:
        logger.critical(f"[{job_run_id}] Critical error in clear AI data API for FileID '{file_id}': {e_main}", exc_info=True)
        crit_err_log_url = await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
        raise HTTPException(status_code=500, detail=f"Internal server error. Job Run ID: {job_run_id}. Log: {crit_err_log_url or 'Log upload failed.'}")


@router.post("/restart-job/{file_id}", tags=["Processing"])
async def api_process_restart_job( # Renamed function slightly for clarity
    file_id: str, 
    entry_id: Optional[int] = Query(None, description="Optional EntryID to start processing from. If None, determined automatically."),
    use_all_variations: bool = Query(False, description="Whether to use all search variations or stop after first success."),
    num_workers_hint: int = Query(4, ge=1, le=16, description="Hint for number of logical workers/concurrency level."),
    background_tasks: Optional[BackgroundTasks] = None # Pass along if core functions use it
):
    job_run_id = f"restart_job_{file_id}_{entry_id or 'auto'}_{'allvars' if use_all_variations else 'stdvars'}"
    # This endpoint directly calls a complex function, so run_job_with_logging might be redundant
    # if process_restart_batch itself handles all the logging and error structuring.
    # However, for consistency, run_job_with_logging provides a standard wrapper.
    
    # Using run_job_with_logging to standardize job execution, logging, and error responses.
    # process_restart_batch is the 'job_func'.
    # file_id from path is the main context, also passed as `file_id_db_str` to process_restart_batch.
    job_result = await run_job_with_logging(
        job_func=process_restart_batch,
        file_id_context=job_run_id, # For logger and S3 path uniqueness
        # kwargs for process_restart_batch:
        file_id_db_str=file_id, # The actual FileID for DB operations
        entry_id=entry_id,
        use_all_variations=use_all_variations,
        background_tasks=background_tasks,
        num_workers=num_workers_hint
    )

    if job_result["status_code"] != 200:
        # Error already logged by run_job_with_logging. Log URL is in job_result["debug_info"]["log_s3_url"]
        raise HTTPException(status_code=job_result["status_code"], detail=job_result["message"])
    
    # Success case
    return {
        "status_code": 200,
        "message": f"Job restart for FileID '{file_id}' initiated and processing result captured.",
        "details": job_result["data"], # This is the dict returned by process_restart_batch
        "log_url": job_result.get("debug_info", {}).get("log_s3_url", "N/A")
    }


# Pydantic Models for Test Insert Endpoint
class TestableSearchResult(BaseModel): # Renamed to avoid conflict if SearchResult is used elsewhere
    EntryID: int
    ImageUrl: Union[PydanticHttpUrl, str] # Allow string for placeholders
    ImageDesc: Optional[str] = None
    ImageSource: Optional[Union[PydanticHttpUrl, str]] = None # Allow string
    ImageUrlThumbnail: Optional[Union[PydanticHttpUrl, str]] = None # Allow string
    ProductCategory: Optional[str] = None

class TestInsertResultsRequest(BaseModel): # Renamed
    results: List[TestableSearchResult]

@router.post("/test-insert-results/{file_id}", tags=["Testing v6"])
async def api_test_insert_search_results( # Renamed function
    file_id: str,
    payload: TestInsertResultsRequest, # Use new model name
    background_tasks: Optional[BackgroundTasks] = None
):
    job_run_id = f"test_insert_results_{file_id}_{uuid.uuid4().hex[:6]}"
    logger, log_file_path = setup_job_logger(job_id=job_run_id, console_output=True)
    logger.info(f"[{job_run_id}] API Call: Test insert search results for FileID '{file_id}'. Results count: {len(payload.results)}")

    try:
        file_id_int = int(file_id) # Validate format

        # Optional: Validate FileID exists in utb_ImageScraperFiles
        # ... (FileID validation as in other endpoints) ...

        # Convert Pydantic models to simple dicts for insert_search_results
        results_for_insertion = [
            {
                "EntryID": item.EntryID,
                "ImageUrl": str(item.ImageUrl), # Ensure string conversion
                "ImageDesc": item.ImageDesc,
                "ImageSource": str(item.ImageSource) if item.ImageSource else None,
                "ImageUrlThumbnail": str(item.ImageUrlThumbnail) if item.ImageUrlThumbnail else None,
                "ProductCategory": item.ProductCategory
            } for item in payload.results
        ]
        
        if not results_for_insertion:
            logger.info(f"[{job_run_id}] No results provided in payload for FileID '{file_id}'.")
            await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
            return {"status": "no_action", "message": "No results provided in payload.", "log_url": "N/A"}

        logger.debug(f"[{job_run_id}] Prepared {len(results_for_insertion)} results for enqueuing. Sample: {results_for_insertion[0] if results_for_insertion else '{}'}")

        # insert_search_results enqueues the DB operation
        enqueue_op_successful = await insert_search_results(
            results=results_for_insertion,
            logger=logger,
            file_id=file_id, # Original FileID for context
            background_tasks=background_tasks # Pass if used by insert_search_results
        )

        final_log_s3_url = await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)

        if enqueue_op_successful:
            msg = f"Successfully enqueued {len(results_for_insertion)} test results for FileID '{file_id}' for insertion."
            logger.info(f"[{job_run_id}] {msg}")
            return {"status": "enqueued_successfully", "message": msg, "log_url": final_log_s3_url}
        else:
            msg = f"Failed to enqueue test results for FileID '{file_id}'. Check logs for details from insert_search_results."
            logger.error(f"[{job_run_id}] {msg}")
            # Still return 200 as API call was accepted, but operation within had issues.
            # Or choose 500 if enqueue failure is critical.
            return {"status": "enqueue_failed", "message": msg, "log_url": final_log_s3_url}

    except ValueError: # For int(file_id)
        err_msg = f"Invalid FileID format: '{file_id}'."
        logger.error(f"[{job_run_id}] {err_msg}", exc_info=True)
        await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
        raise HTTPException(status_code=400, detail=err_msg)
    except Exception as e_main:
        logger.critical(f"[{job_run_id}] Critical error in test insert API for FileID '{file_id}': {e_main}", exc_info=True)
        crit_err_log_url = await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
        raise HTTPException(status_code=500, detail=f"Internal server error. Job Run ID: {job_run_id}. Log: {crit_err_log_url or 'Log upload failed.'}")

# Include other endpoints following similar refactoring patterns for logging, error handling,
# use of run_job_with_logging or direct calls to core logic with proper parameter passing.

# Example for a simpler endpoint like /get-send-to-email
@router.get("/get-send-to-email/{file_id}", tags=["Database v6"])
async def api_get_send_to_email_address(file_id: str): # Renamed function
    job_run_id = f"get_email_{file_id}"
    # For simple GETs, job_specific_logger might be overkill unless complex logic involved.
    # Using default_logger or a quick setup if needed.
    logger, log_file_path = setup_job_logger(job_id=job_run_id, console_output=False) # Minimal logging for GET usually
    
    try:
        file_id_int = int(file_id)
        email_address_or_list = await get_send_to_email(file_id_int, logger) # Pass logger
        
        if email_address_or_list is None:
            logger.info(f"[{job_run_id}] No email address configured for FileID '{file_id_int}'.")
            # No log upload needed for simple successful GET with no data.
            return {"status_code": 200, "message": "No email address configured.", "data": None}
        
        logger.info(f"[{job_run_id}] Successfully retrieved email(s) for FileID '{file_id_int}'.")
        return {"status_code": 200, "message": "Email address(es) retrieved.", "data": email_address_or_list}

    except ValueError:
        err_msg = f"Invalid FileID format: '{file_id}'."
        logger.error(f"[{job_run_id}] {err_msg}", exc_info=True)
        # Log upload for errors even in GETs if stateful or complex
        # await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
        raise HTTPException(status_code=400, detail=err_msg)
    except Exception as e_get_email:
        logger.error(f"[{job_run_id}] Error retrieving email for FileID '{file_id}': {e_get_email}", exc_info=True)
        # await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
        raise HTTPException(status_code=500, detail=f"Internal error retrieving email: {str(e_get_email)}")


# ... (Implement other endpoints like /sort-by-relevance, /reset-step1, /validate-images, etc.
#      following these patterns:
#      - Use setup_job_logger for a unique job_run_id.
#      - Convert file_id to int with error handling.
#      - Call core logic functions (often via run_job_with_logging or directly if simple).
#      - Ensure DB writes are enqueued via enqueue_db_update.
#      - Upload logs with upload_log_file.
#      - Return structured JSON or HTTPException.
# )

# --- Mount the router with the new version prefix ---
app.include_router(router, prefix="/api/v6")