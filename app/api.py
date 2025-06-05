import asyncio
import datetime
import hashlib
import json
import logging
import math
import os
import signal
import sys
import time
import traceback
import urllib.parse
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager
from math import ceil
from multiprocessing import cpu_count
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

import aio_pika # For RabbitMQProducer type hinting if not directly used here
import aiofiles # For async file operations
import aiohttp
import httpx
import pandas as pd
import psutil
from fastapi import (APIRouter, BackgroundTasks, FastAPI, HTTPException, Query,
                     Request)
from pydantic import BaseModel, Field
from pydantic import HttpUrl as PydanticHttpUrl # Alias for clarity
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text
from tenacity import (RetryError, before_sleep_log, retry,
                      retry_if_exception_type, stop_after_attempt,
                      wait_exponential)

# Assume these are defined in their respective modules
from ai_utils import batch_vision_reason
from common import (fetch_brand_rules, generate_search_variations,
                    preprocess_sku)
from config import (BRAND_RULES_URL, DATAPROXY_API_KEY, RABBITMQ_URL,
                    SEARCH_PROXY_API_URL) # VERSION removed, will define locally
from database_config import async_engine
from db_utils import (enqueue_db_update, fetch_last_valid_entry,
                      get_images_excel_db, get_send_to_email,
                      insert_search_results, update_file_generate_complete,
                      update_file_location_complete, update_initial_sort_order,
                      update_log_url_in_db)
from email_utils import send_message_email
from icon_image_lib.google_parser import process_search_result
from logging_config import setup_job_logger
from rabbitmq_producer import RabbitMQProducer
from s3_utils import upload_file_to_space
from search_utils import (update_search_sort_order, update_sort_no_image_entry,
                          update_sort_order)
from url_extract import extract_thumbnail_url

VERSION = "6.0.1" # Updated version

# --- Constants ---
SCRAPER_RECORDS_TABLE_NAME = "utb_ImageScraperRecords"
SCRAPER_RECORDS_PK_COLUMN = "EntryID"
SCRAPER_RECORDS_FILE_ID_FK_COLUMN = "FileID"
# ... (other SCRAPER_RECORDS constants)
SCRAPER_RECORDS_PRODUCT_MODEL_COLUMN = "ProductModel"
SCRAPER_RECORDS_PRODUCT_BRAND_COLUMN = "ProductBrand"
SCRAPER_RECORDS_STEP1_COLUMN = "Step1"
SCRAPER_RECORDS_ENTRY_STATUS_COLUMN = "EntryStatus"
SCRAPER_RECORDS_WAREHOUSE_MATCH_TIME_COLUMN = "WarehouseMatchTime"
SCRAPER_RECORDS_PRODUCT_COLOR_COLUMN = "ProductColor"
SCRAPER_RECORDS_PRODUCT_CATEGORY_COLUMN = "ProductCategory"


IMAGE_SCRAPER_RESULT_TABLE_NAME = "utb_ImageScraperResult"
IMAGE_SCRAPER_RESULT_PK_COLUMN = "ResultID"
IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN = "EntryID"
IMAGE_SCRAPER_RESULT_IMAGE_URL_COLUMN = "ImageUrl"
IMAGE_SCRAPER_RESULT_IMAGE_DESC_COLUMN = "ImageDesc"
IMAGE_SCRAPER_RESULT_IMAGE_SOURCE_COLUMN = "ImageSource"
IMAGE_SCRAPER_RESULT_IMAGE_URL_THUMBNAIL_COLUMN = "ImageUrlThumbnail"
IMAGE_SCRAPER_RESULT_SORT_ORDER_COLUMN = "SortOrder"
IMAGE_SCRAPER_RESULT_AI_JSON_COLUMN = "AiJson"
IMAGE_SCRAPER_RESULT_AI_CAPTION_COLUMN = "AiCaption"
IMAGE_SCRAPER_RESULT_SOURCE_TYPE_COLUMN = "SourceType"
IMAGE_SCRAPER_RESULT_CREATE_TIME_COLUMN = "CreateTime" # Added for test insert verification


WAREHOUSE_IMAGES_TABLE_NAME = "utb_IconWarehouseImages"
WAREHOUSE_IMAGES_PK_COLUMN = "ID"
WAREHOUSE_IMAGES_MODEL_NUMBER_COLUMN = "ModelNumber"
WAREHOUSE_IMAGES_MODEL_CLEAN_COLUMN = "ModelClean"
WAREHOUSE_IMAGES_MODEL_FOLDER_COLUMN = "ModelFolder"
WAREHOUSE_IMAGES_MODEL_SOURCE_COLUMN = "ModelSource"

IMAGE_SCRAPER_FILES_TABLE_NAME = "utb_ImageScraperFiles"
IMAGE_SCRAPER_FILES_PK_COLUMN = "ID"
IMAGE_SCRAPER_FILES_IMAGE_COMPLETE_TIME_COLUMN = "ImageCompleteTime"

# Status Values for SCRAPER_RECORDS_ENTRY_STATUS_COLUMN
STATUS_PENDING_WAREHOUSE_CHECK = 0
STATUS_WAREHOUSE_CHECK_NO_MATCH = 1
STATUS_WAREHOUSE_RESULT_POPULATED = 2
STATUS_PENDING_GOOGLE_SEARCH = 3
STATUS_GOOGLE_SEARCH_COMPLETE = 4
# --- End Constants ---

app = FastAPI(title="Super Scraper API", version=VERSION)

default_logger = logging.getLogger("super_scraper_api")
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    default_logger.addHandler(ch)

router = APIRouter()
global_producer: Optional[RabbitMQProducer] = None
JOB_STATUS: Dict[str, Dict[str, Any]] = {}
LAST_UPLOAD: Dict[tuple, Dict[str, Any]] = {}


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    default_logger.info(f"FastAPI application startup sequence initiated (Version: {VERSION}).")
    global global_producer

    try:
        default_logger.info("Initializing global RabbitMQ producer...")
        async with asyncio.timeout(30):
            global_producer = await RabbitMQProducer.get_producer()
        if not global_producer or not global_producer.is_connected:
            default_logger.critical("CRITICAL: Failed to initialize or connect global RabbitMQ producer. Application may not function correctly.")
            # Consider sys.exit(1) if RabbitMQ is absolutely essential for startup
        else:
            default_logger.info("Global RabbitMQ producer initialized and connected successfully.")

        RUN_STARTUP_TEST_INSERT = os.getenv("RUN_STARTUP_TEST_INSERT", "true").lower() == "true" # Default to true
        if RUN_STARTUP_TEST_INSERT:
            default_logger.info("Running test insertion of search result on startup...")
            test_file_id = f"startup_test_{uuid.uuid4().hex[:8]}"
            test_logger, test_log_filename = setup_job_logger(job_id=test_file_id, console_output=False)
            try:
                # Ensure a corresponding FileID exists in utb_ImageScraperFiles for the test, or that insert_search_results can handle it.
                # For a simple test, we might need to insert a temporary FileID.
                # The following assumes insert_search_results is robust for testing.
                async with async_engine.connect() as conn:
                    await conn.execute(
                        text(f"MERGE INTO {IMAGE_SCRAPER_FILES_TABLE_NAME} AS target "
                             f"USING (SELECT :file_id AS ID, :filename AS FileName) AS source "
                             f"ON target.ID = source.ID "
                             f"WHEN NOT MATCHED THEN INSERT (ID, FileName, UploadTime) VALUES (source.ID, source.FileName, GETUTCDATE());"), # Adjusted for SQL Server
                        {"file_id": int(test_file_id.split('_')[-1], 16) % 100000, "filename": f"{test_file_id}.xlsx"} # Example: use part of UUID for int ID
                    ) # Ensure ID is int for DB
                    await conn.commit()


                sample_result_data = [
                    {
                        "EntryID": 99999,
                        "ImageUrl": "https://via.placeholder.com/150/0000FF/808080?Text=TestImage",
                        "ImageDesc": "Automated startup test image description",
                        "ImageSource": "https://placeholder.com",
                        "ImageUrlThumbnail": "https://via.placeholder.com/50/0000FF/808080?Text=Thumb"
                    }
                ]
                success_flag = await insert_search_results(
                    results=sample_result_data,
                    logger=test_logger,
                    file_id=test_file_id, # Context for the task
                    background_tasks=BackgroundTasks()
                )
                if success_flag:
                    test_logger.info(f"Test search result enqueued successfully for EntryID 99999, Context FileID {test_file_id}.")
                else:
                    test_logger.error(f"Failed to enqueue test search result for EntryID 99999, Context FileID {test_file_id}.")
            except Exception as e_startup_test:
                test_logger.error(f"Error during startup test insertion: {e_startup_test}", exc_info=True)
            finally:
                if os.path.exists(test_log_filename): # Ensure file exists before attempting upload
                   await upload_log_file(test_file_id, test_log_filename, test_logger, db_record_file_id_to_update=None)
        else:
            default_logger.info("Skipping startup test insertion (RUN_STARTUP_TEST_INSERT is not true).")

        loop = asyncio.get_running_loop()
        def handle_shutdown_signal(signal_name: str):
            default_logger.info(f"Received shutdown signal: {signal_name}. Initiating graceful shutdown...")
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, handle_shutdown_signal, sig.name)

        default_logger.info("FastAPI application startup sequence completed.")
        yield
    finally:
        default_logger.info("FastAPI application shutdown sequence initiated.")
        if global_producer and global_producer.is_connected:
            default_logger.info("Closing global RabbitMQ producer connection...")
            await global_producer.close()
            default_logger.info("Global RabbitMQ producer closed.")
        if async_engine:
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
        self._aiohttp_session: Optional[aiohttp.ClientSession] = None
        self.regions = ['northamerica-northeast', 'us-east', 'southamerica', 'us-central', 'us-west', 'europe', 'australia', 'asia', 'middle-east']
        self.request_headers = {
            "accept": "application/json",
            "x-api-key": self.api_key,
            "Content-Type": "application/json"
        }

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._aiohttp_session is None or self._aiohttp_session.closed:
            self.logger.debug("Creating new aiohttp.ClientSession for SearchClient.")
            self._aiohttp_session = aiohttp.ClientSession(headers=self.request_headers)
        return self._aiohttp_session

    async def close(self):
        if self._aiohttp_session and not self._aiohttp_session.closed:
            self.logger.debug("Closing SearchClient's aiohttp.ClientSession.")
            await self._aiohttp_session.close()
            self._aiohttp_session = None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, json.JSONDecodeError, asyncio.TimeoutError)),
        before_sleep=before_sleep_log(default_logger, logging.WARNING)
    )
    async def search_single_term_all_regions(self, term: str, brand: str, entry_id: int) -> List[Dict]:
        async with self.semaphore:
            process_info = psutil.Process()
            search_url_google = f"https://www.google.com/search?q={urllib.parse.quote(term)}&tbm=isch"
            current_session = await self._get_session()

            for region_idx, region_name in enumerate(self.regions):
                current_fetch_endpoint = f"{self.endpoint}?region={region_name}"
                self.logger.info(
                    f"PID {process_info.pid}: Attempting search for term='{term}' (EntryID {entry_id}, Brand='{brand}') "
                    f"via {current_fetch_endpoint} (Region {region_idx+1}/{len(self.regions)}: {region_name})"
                )
                try:
                    async with asyncio.timeout(45):
                        async with current_session.post(current_fetch_endpoint, json={"url": search_url_google}) as response:
                            response_text_content = await response.text()
                            response_preview = response_text_content[:250] if response_text_content else "[EMPTY RESPONSE BODY]"
                            self.logger.debug(
                                f"PID {process_info.pid}: Response from {current_fetch_endpoint} for term='{term}': "
                                f"Status={response.status}, Preview='{response_preview}'"
                            )
                            if response.status in (429, 503):
                                self.logger.warning(
                                    f"PID {process_info.pid}: Service temporarily unavailable (Status {response.status}) for '{term}' in region {region_name}. "
                                ) # Removed content preview from warning for brevity
                                if region_idx == len(self.regions) - 1:
                                    raise aiohttp.ClientResponseError(response.request_info, response.history, status=response.status, message="Service unavailable after all regions", headers=response.headers)
                                await asyncio.sleep(1 + region_idx * 0.5)
                                continue
                            response.raise_for_status()
                            try:
                                api_json_response = await response.json()
                            except json.JSONDecodeError as json_e:
                                self.logger.error(
                                    f"PID {process_info.pid}: JSONDecodeError for term='{term}' in region {region_name}. "
                                    f"Status: {response.status}, Body Preview: {response_preview}. Error: {json_e}", exc_info=True
                                )
                                if region_idx == len(self.regions) - 1: raise
                                continue
                            html_content_from_api = api_json_response.get("result")
                            if not html_content_from_api:
                                self.logger.warning(
                                    f"PID {process_info.pid}: 'result' field missing or empty for term='{term}' in region {region_name}."
                                )
                                continue
                            #html_bytes = html_content_from_api.encode('utf-8') if isinstance(html_content_from_api, str) else str(html_content_from_api).encode('utf-8')
                            # process_search_result expects (html_bytes, entry_id, logger)
                            formatted_results_df = process_search_result(html_content_from_api, entry_id, self.logger) # Corrected arguments
                            if not formatted_results_df.empty:
                                self.logger.info(
                                    f"PID {process_info.pid}: Successfully found {len(formatted_results_df)} results for term='{term}' "
                                    f"in region {region_name}."
                                )
                                return [
                                    {
                                        "EntryID": entry_id,
                                        "ImageUrl": str(row_data.get("ImageUrl", "placeholder://no-image-url-in-df")),
                                        "ImageDesc": str(row_data.get("ImageDesc", "N/A")),
                                        "ImageSource": str(row_data.get("ImageSource", "placeholder://no-source-in-df")),
                                        "ImageUrlThumbnail": str(row_data.get("ImageUrlThumbnail", row_data.get("ImageUrl", "placeholder://no-thumbnail-in-df")))
                                    }
                                    for _, row_data in formatted_results_df.iterrows()
                                ]
                            else:
                                self.logger.warning(f"PID {process_info.pid}: `process_search_result` returned empty for term='{term}' in region {region_name}.")
                except asyncio.TimeoutError:
                    self.logger.warning(f"PID {process_info.pid}: Request timeout for term='{term}' in region {region_name}.")
                    if region_idx == len(self.regions) - 1: raise
                except aiohttp.ClientError as client_e:
                    self.logger.warning(f"PID {process_info.pid}: ClientError for term='{term}' in region {region_name}: {client_e}", exc_info=True)
                    if region_idx == len(self.regions) - 1: raise
                except Exception as e_region:
                    self.logger.error(f"PID {process_info.pid}: Unexpected error processing term='{term}' in region {region_name}: {e_region}", exc_info=True)
                    if region_idx == len(self.regions) - 1: raise # Reraise to be caught by tenacity or caller
            self.logger.error(
                f"PID {process_info.pid}: All regions failed for term='{term}' (EntryID {entry_id})."
            )
            return []

def _create_placeholder_result(entry_id: int, type_suffix: str, description: str) -> Dict:
    return {
        "EntryID": entry_id,
        "ImageUrl": f"placeholder://{type_suffix}",
        "ImageDesc": description,
        "ImageSource": "placeholder://N/A", # Placeholder for HttpUrl
        "ImageUrlThumbnail": f"placeholder://{type_suffix}-thumb",
        "ProductCategory": ""
    }

async def orchestrate_entry_search(
    original_search_term: str,
    original_brand: Optional[str],
    entry_id: int,
    search_api_endpoint: str,
    use_all_variations: bool,
    file_id_context: Union[str, int],
    logger: logging.Logger,
    brand_rules: Dict
) -> List[Dict]:
    logger.info(f"Orchestrating search for EntryID {entry_id} (FileID {file_id_context}). Term: '{original_search_term}', Brand: '{original_brand}'.")
    processed_search_term, processed_brand, _, _ = await preprocess_sku(
        search_string=original_search_term,
        known_brand=original_brand,
        brand_rules=brand_rules,
        logger=logger
    )
    logger.debug(f"EntryID {entry_id}: Preprocessed to Term='{processed_search_term}', Brand='{processed_brand}'.")
    search_variations_map = await generate_search_variations(
        search_string=processed_search_term,
        brand=processed_brand,
        logger=logger
    )
    if not search_variations_map:
        logger.warning(f"EntryID {entry_id}: No search variations for Term='{processed_search_term}', Brand='{processed_brand}'.")
        return [_create_placeholder_result(entry_id, "no-search-variations", f"No variations for '{processed_search_term}'")]

    all_valid_results_collected: List[Dict] = []
    variation_type_priority = ["default", "brand_alias", "model_alias", "delimiter_variations", "color_variations", "no_color", "category_specific"]
    search_client = SearchClient(endpoint=search_api_endpoint, logger=logger)
    try:
        for variation_type in variation_type_priority:
            terms_for_type = search_variations_map.get(variation_type, [])
            if not terms_for_type: continue
            logger.info(f"EntryID {entry_id}: Processing {len(terms_for_type)} terms for variation type '{variation_type}'.")
            for specific_search_term in terms_for_type:
                logger.debug(f"EntryID {entry_id}: Searching term '{specific_search_term}' (type: {variation_type}).")
                term_results = await search_client.search_single_term_all_regions(
                    term=specific_search_term, brand=processed_brand, entry_id=entry_id
                )
                if term_results:
                    logger.info(f"EntryID {entry_id}: Found {len(term_results)} results for term '{specific_search_term}'.")
                    for res_dict in term_results:
                        if res_dict.get("ImageUrl") and not str(res_dict["ImageUrl"]).startswith("placeholder://"):
                            # Ensure ImageSource is a valid URL string or None before Pydantic validation elsewhere
                            img_src = res_dict.get("ImageSource")
                            if isinstance(img_src, str) and not (img_src.startswith("http://") or img_src.startswith("https://") or img_src.startswith("placeholder://")):
                                res_dict["ImageSource"] = None # Or "placeholder://invalid-source"
                            elif not isinstance(img_src, str): # If not a string (e.g. Pydantic URL obj already)
                                res_dict["ImageSource"] = str(img_src) if img_src else None


                            # Attempt to extract better thumbnail if main image is not placeholder
                            if not str(res_dict["ImageUrl"]).startswith("placeholder://"):
                                extracted_thumb = await extract_thumbnail_url(str(res_dict["ImageUrl"]), logger)
                                if extracted_thumb:
                                    res_dict["ImageUrlThumbnail"] = extracted_thumb
                                elif not res_dict.get("ImageUrlThumbnail") or str(res_dict["ImageUrlThumbnail"]).startswith("placeholder://"): # if no good thumb yet
                                    res_dict["ImageUrlThumbnail"] = str(res_dict["ImageUrl"]) # fallback to main image

                            all_valid_results_collected.append(res_dict)
            if all_valid_results_collected and not use_all_variations:
                logger.info(f"EntryID {entry_id}: Results found & not `use_all_variations`. Stopping after type '{variation_type}'.")
                break
    except Exception as e_orchestrate:
        logger.error(f"EntryID {entry_id}: Error during search orchestration: {e_orchestrate}", exc_info=True)
    finally:
        await search_client.close()

    if not all_valid_results_collected:
        logger.warning(f"EntryID {entry_id}: No valid results after all searches for Original='{original_search_term}'.")
        return [_create_placeholder_result(entry_id, "no-valid-results-final", f"No results for '{original_search_term}'")]
    logger.info(f"EntryID {entry_id}: Orchestration complete. Total {len(all_valid_results_collected)} valid results for Original='{original_search_term}'.")
    return all_valid_results_collected

async def run_job_with_logging(
    job_func: Callable[..., Any],
    file_id_context: str,
    **kwargs: Any
) -> Dict[str, Any]:
    job_specific_logger, log_file_path = setup_job_logger(job_id=file_id_context, console_output=True)
    result_payload: Optional[Any] = None
    debug_info_dict = {"memory_usage_mb": {}, "log_file_server_path": log_file_path, "errors_encountered": []}
    http_status_code = 500
    response_message = "Job execution failed unexpectedly."
    job_function_name = getattr(job_func, '__name__', 'unnamed_job_function')

    try:
        job_specific_logger.info(f"Starting job '{job_function_name}' for context FileID: {file_id_context}")
        current_process = psutil.Process()
        mem_before = round(current_process.memory_info().rss / (1024 * 1024), 2)
        debug_info_dict["memory_usage_mb"]["before_job"] = mem_before
        job_specific_logger.debug(f"Memory before job '{job_function_name}': {mem_before:.2f} MB")

        func_params = job_func.__code__.co_varnames
        if 'logger' in func_params: kwargs['logger'] = job_specific_logger
        if 'file_id' in func_params and 'file_id' not in kwargs: kwargs['file_id'] = file_id_context
        if 'file_id_db_str' in func_params and 'file_id_db_str' not in kwargs : kwargs['file_id_db_str'] = file_id_context # Specific for process_restart_batch
        if 'file_id_db' in func_params and 'file_id_db' not in kwargs: # for process_restart_batch original naming
             try: kwargs['file_id_db'] = int(file_id_context)
             except ValueError: kwargs['file_id_db'] = file_id_context # Pass as string if not int-like


        if asyncio.iscoroutinefunction(job_func):
            result_payload = await job_func(**kwargs)
        else:
            result_payload = await asyncio.to_thread(job_func, **kwargs)
        
        mem_after = round(current_process.memory_info().rss / (1024 * 1024), 2)
        debug_info_dict["memory_usage_mb"]["after_job"] = mem_after
        job_specific_logger.debug(f"Memory after job '{job_function_name}': {mem_after:.2f} MB")
        if (mem_after - mem_before) > 500:
            job_specific_logger.warning(f"Job '{job_function_name}' memory increase: {(mem_after - mem_before):.2f} MB.")

        job_specific_logger.info(f"Successfully completed job '{job_function_name}' for context FileID: {file_id_context}")
        http_status_code = 200
        response_message = f"Job '{job_function_name}' for context FileID '{file_id_context}' completed successfully."
    except Exception as e:
        job_specific_logger.error(f"Error in job '{job_function_name}' for FileID {file_id_context}: {str(e)}", exc_info=True)
        debug_info_dict["errors_encountered"].append({"error_message": str(e), "error_type": type(e).__name__, "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()})
        response_message = f"Job '{job_function_name}' for context FileID {file_id_context} failed: {str(e)}"
    finally:
        uploaded_log_url = await upload_log_file(
            job_id_for_s3_path=file_id_context,
            local_log_file_path=log_file_path,
            logger_instance=job_specific_logger,
            db_record_file_id_to_update=file_id_context # Assuming context ID is the DB FileID
        )
        debug_info_dict["log_s3_url"] = uploaded_log_url

    return {"status_code": http_status_code, "message": response_message, "data": result_payload, "debug_info": debug_info_dict}

async def run_generate_download_file(
    file_id: str,
    parent_logger: logging.Logger,
    background_tasks: Optional[BackgroundTasks]
):
    log_prefix = f"[GenerateDownloadFile Client | FileID {file_id}]"
    parent_logger.info(f"{log_prefix} Initiating request to generate download file.")
    job_key = f"generate_download_{file_id}"
    JOB_STATUS[job_key] = {"status": "initiating_generation_request", "message": "Requesting download file generation.", "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(), "file_id": file_id}

    try:
        # Ensure this URL is configurable and points to the correct service endpoint.
        file_generation_endpoint = os.getenv("FILE_GENERATION_SERVICE_URL", f"https://icon7-8001.iconluxury.today/generate-download-file/?file_id={file_id}")
        parent_logger.info(f"{log_prefix} Calling file generation service: {file_generation_endpoint}")

        async with httpx.AsyncClient(timeout=300.0) as client:
            response = await client.post(file_generation_endpoint, headers={"accept": "application/json"})
            response.raise_for_status()
            service_response_data = response.json()
        
        parent_logger.info(f"{log_prefix} Response from file generation service: {service_response_data}")

        # Assuming the service returns a structure like {"status_code": 200, "public_url": "..."} or {"error": "..."}
        if service_response_data.get("public_url"): # Simplified check
            JOB_STATUS[job_key].update({"status": "generation_successful_reported", "message": "File generation service reported success.", "public_url": service_response_data["public_url"], "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()})
            parent_logger.info(f"{log_prefix} File generation successful. URL: {service_response_data['public_url']}")
            await update_file_location_complete(file_id, service_response_data["public_url"], parent_logger) # Enqueues DB update
        else:
            error_detail = service_response_data.get("error", service_response_data.get("message", "Unknown issue from generation service."))
            JOB_STATUS[job_key].update({"status": "generation_failed_reported", "message": f"File generation service failed: {error_detail}", "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()})
            parent_logger.error(f"{log_prefix} File generation service reported failure: {error_detail}. Response: {service_response_data}")
    except httpx.HTTPStatusError as hse:
        parent_logger.error(f"{log_prefix} HTTP error calling file generation service: Status {hse.response.status_code}, Response: {hse.response.text}", exc_info=True)
        JOB_STATUS[job_key].update({"status": "generation_request_http_error", "message": f"HTTP error with generation service: {hse.response.status_code}", "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()})
    except Exception as e:
        parent_logger.error(f"{log_prefix} Unexpected error during file generation request: {e}", exc_info=True)
        JOB_STATUS[job_key].update({"status": "generation_request_unexpected_error", "message": f"Unexpected error: {str(e)}", "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()})

async def upload_log_file(
    job_id_for_s3_path: str,
    local_log_file_path: str,
    logger_instance: logging.Logger,
    db_record_file_id_to_update: Optional[str] = None
) -> Optional[str]:
    log_prefix = f"[LogUpload | JobS3PathID {job_id_for_s3_path} | DBFileID {db_record_file_id_to_update or 'N/A'}]"
    logger_instance.debug(f"{log_prefix} Attempting log upload: {local_log_file_path}")

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(Exception),
        before_sleep=lambda rs: logger_instance.warning(
            f"{log_prefix} Retrying log upload for '{local_log_file_path}' (Attempt {rs.attempt_number}). Waiting {getattr(rs.next_action, 'sleep', 0):.2f}s. Reason: {type(rs.outcome.exception()).__name__}"
        ) if rs.outcome else None # Check rs.outcome to prevent error on first attempt logging
    )
    async def _try_upload_log_to_s3_async():
        if not await asyncio.to_thread(os.path.exists, local_log_file_path):
            logger_instance.warning(f"{log_prefix} Log file '{local_log_file_path}' not found. Skipping upload.")
            return None
        try:
            async with aiofiles.open(local_log_file_path, "rb") as f:
                log_content_bytes = await f.read()
        except Exception as e_read: # Fallback for read error or if aiofiles isn't fully robust
            logger_instance.warning(f"{log_prefix} aiofiles read failed for {local_log_file_path}: {e_read}. Falling back to sync read.")
            with open(local_log_file_path, "rb") as f_sync:
                log_content_bytes = await asyncio.to_thread(f_sync.read)

        current_file_hash_hex = await asyncio.to_thread(lambda b: hashlib.md5(b).hexdigest(), log_content_bytes)
        cache_key = (local_log_file_path, job_id_for_s3_path)
        current_timestamp = time.time()
        if cache_key in LAST_UPLOAD:
            cached_info = LAST_UPLOAD[cache_key]
            if cached_info["hash"] == current_file_hash_hex and (current_timestamp - cached_info["time"]) < 120:
                logger_instance.info(f"{log_prefix} Log unchanged and recently uploaded. Cached URL: {cached_info['url']}")
                return cached_info["url"]

        s3_log_object_key = f"job_logs/job_{job_id_for_s3_path}.log"
        logger_instance.debug(f"{log_prefix} Uploading '{local_log_file_path}' to S3 as '{s3_log_object_key}'.")
        uploaded_s3_url = await upload_file_to_space(
            file_src=local_log_file_path, save_as=s3_log_object_key, is_public=True,
            logger=logger_instance, file_id=job_id_for_s3_path
        )
        if not uploaded_s3_url:
            raise ValueError(f"S3 upload returned empty URL for {s3_log_object_key}")
        logger_instance.info(f"{log_prefix} Log uploaded to S3: {uploaded_s3_url}")

        if db_record_file_id_to_update: # db_record_file_id_to_update is the actual FileID PK from DB
            actual_db_file_id = db_record_file_id_to_update
            # If job_id_for_s3_path is like "restart_job_123_..." then extract "123"
            if not db_record_file_id_to_update.isdigit() and "_" in db_record_file_id_to_update:
                 parts = db_record_file_id_to_update.split("_")
                 # Heuristic: find the first numeric part that could be a FileID
                 for part in parts:
                     if part.isdigit():
                         actual_db_file_id = part
                         break
            logger_instance.debug(f"{log_prefix} Updating log URL in DB for FileID '{actual_db_file_id}'.")
            enqueue_success = await update_log_url_in_db(actual_db_file_id, uploaded_s3_url, logger_instance)
            if not enqueue_success:
                logger_instance.warning(f"{log_prefix} Failed to enqueue DB update for log URL for FileID '{actual_db_file_id}'.")
        LAST_UPLOAD[cache_key] = {"hash": current_file_hash_hex, "time": current_timestamp, "url": uploaded_s3_url}
        return uploaded_s3_url
    try:
        return await _try_upload_log_to_s3_async()
    except RetryError as e_retry: # Catches error after all retries are exhausted
        logger_instance.error(f"{log_prefix} All attempts to upload log '{local_log_file_path}' failed: {e_retry.last_attempt.exception()}", exc_info=True)
        return None
    except Exception as e_final: # Should ideally be caught by RetryError if tenacity is configured for all Exception
        logger_instance.error(f"{log_prefix} Unexpected final error uploading log '{local_log_file_path}': {e_final}", exc_info=True)
        return None

async def process_restart_batch(
    file_id_db_str: str, # This is the string representation of the DB FileID
    entry_id: Optional[int] = None,
    use_all_variations: bool = False,
    logger: logging.Logger = default_logger,
    background_tasks: Optional[BackgroundTasks] = None,
    num_workers: int = 4,
) -> Dict[str, Any]:
    current_log_filename = "unknown_log_file.log" # Placeholder
    if logger.handlers and hasattr(logger.handlers[0], 'baseFilename'):
        current_log_filename = logger.handlers[0].baseFilename
    else:
        log_dir = "job_logs"
        if not os.path.exists(log_dir): os.makedirs(log_dir, exist_ok=True)
        current_log_filename = os.path.join(log_dir, f"job_{file_id_db_str}_process_restart.log")

    logger.info(f"Initiating 'process_restart_batch' for FileID: {file_id_db_str}. StartEntryID: {entry_id or 'Auto'}, UseAllVariations: {use_all_variations}, Workers: {num_workers}.")
    current_process_info = psutil.Process()
    def _log_resource_usage(context_message: str = ""):
        mem_rss_mb = current_process_info.memory_info().rss / (1024**2)
        cpu_perc = current_process_info.cpu_percent(interval=0.01)
        logger.debug(f"Resource Usage {context_message} (PID {current_process_info.pid}): RSS={mem_rss_mb:.2f}MB, CPU={cpu_perc:.1f}%")

    _log_resource_usage("at start of process_restart_batch")
    try:
        file_id_for_db = int(file_id_db_str)
    except ValueError:
        error_msg = f"Invalid FileID format: '{file_id_db_str}'. Must be integer."
        logger.error(error_msg)
        log_url = await upload_log_file(file_id_db_str, current_log_filename, logger, db_record_file_id_to_update=file_id_db_str)
        return {"error": error_msg, "log_filename": current_log_filename, "log_public_url": log_url or "", "last_entry_id_processed": str(entry_id or "")}

    BATCH_SIZE_PER_GATHER = max(1, min(20, num_workers * 2))
    MAX_CONCURRENT_ENTRY_PROCESSING = max(num_workers, 5)
    MAX_ENTRY_ATTEMPTS = 3
    configured_search_endpoint = SEARCH_PROXY_API_URL
    logger.debug(f"Batch Config for FileID {file_id_for_db}: BatchSize={BATCH_SIZE_PER_GATHER}, MaxConcurrentEntries={MAX_CONCURRENT_ENTRY_PROCESSING}, MaxEntryAttempts={MAX_ENTRY_ATTEMPTS}.")

    try:
        async with async_engine.connect() as db_conn_validate:
            file_check_res = await db_conn_validate.execute(text(f"SELECT 1 FROM {IMAGE_SCRAPER_FILES_TABLE_NAME} WHERE {IMAGE_SCRAPER_FILES_PK_COLUMN} = :fid"), {"fid": file_id_for_db})
            if not file_check_res.scalar_one_or_none():
                # ... (error handling as before)
                error_msg = f"FileID {file_id_for_db} not found in {IMAGE_SCRAPER_FILES_TABLE_NAME}."
                logger.error(error_msg)
                log_url = await upload_log_file(file_id_db_str, current_log_filename, logger, db_record_file_id_to_update=file_id_db_str)
                return {"error": error_msg, "log_filename": current_log_filename, "log_public_url": log_url or "", "last_entry_id_processed": str(entry_id or "")}

    except SQLAlchemyError as db_exc_validate:
        # ... (error handling as before)
        error_msg = f"Database error validating FileID {file_id_for_db}: {db_exc_validate}"
        logger.error(error_msg, exc_info=True)
        log_url = await upload_log_file(file_id_db_str, current_log_filename, logger, db_record_file_id_to_update=file_id_db_str)
        return {"error": error_msg, "log_filename": current_log_filename, "log_public_url": log_url or "", "last_entry_id_processed": str(entry_id or "")}


    actual_start_entry_id = entry_id
    if actual_start_entry_id is None:
        # Try to fetch the last successfully processed EntryID for this FileID to resume.
        # If fetch_last_valid_entry returns an ID, we want to start *after* it.
        # If it returns None, or if we want to restart from the beginning of unprocessed:
        last_good_entry = await fetch_last_valid_entry(file_id_db_str, logger)
        if last_good_entry is not None:
             actual_start_entry_id = last_good_entry + 1 # Start after the last good one
             logger.info(f"FileID {file_id_for_db}: Resuming after last valid EntryID {last_good_entry}. Starting at {actual_start_entry_id}.")
        else:
            actual_start_entry_id = 0 # Default to start from beginning of unprocessed
            logger.info(f"FileID {file_id_for_db}: No specific start EntryID or last valid entry. Will process from ID {actual_start_entry_id}.")


    brand_rules_data = await fetch_brand_rules(BRAND_RULES_URL, max_attempts=3, timeout=15, logger=logger)
    if not brand_rules_data: brand_rules_data = {} # Ensure it's a dict
    if "Scotch & Soda" not in brand_rules_data: brand_rules_data["Scotch & Soda"] = {"aliases": ["Scotch and Soda", "S&S"], "sku_pattern": r"^\d{6,8}[a-zA-Z0-9]*$"}
    logger.debug(f"FileID {file_id_for_db}: Using {len(brand_rules_data)} brand rules.")

    entries_to_process_list: List[Tuple] = []
    try:
        async with async_engine.connect() as db_conn_fetch:
            sql_fetch_entries = text(f"""
                SELECT r.{SCRAPER_RECORDS_PK_COLUMN}, r.{SCRAPER_RECORDS_PRODUCT_MODEL_COLUMN}, 
                       r.{SCRAPER_RECORDS_PRODUCT_BRAND_COLUMN}, r.{SCRAPER_RECORDS_PRODUCT_COLOR_COLUMN}, 
                       r.{SCRAPER_RECORDS_PRODUCT_CATEGORY_COLUMN}
                FROM {SCRAPER_RECORDS_TABLE_NAME} r
                WHERE r.{SCRAPER_RECORDS_FILE_ID_FK_COLUMN} = :fid
                  AND r.{SCRAPER_RECORDS_PK_COLUMN} >= :start_eid
                  AND r.{SCRAPER_RECORDS_PRODUCT_MODEL_COLUMN} IS NOT NULL AND r.{SCRAPER_RECORDS_PRODUCT_MODEL_COLUMN} <> ''
                  AND (r.{SCRAPER_RECORDS_STEP1_COLUMN} IS NULL 
                       OR NOT EXISTS (SELECT 1 FROM {IMAGE_SCRAPER_RESULT_TABLE_NAME} res 
                                      WHERE res.{IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN} = r.{SCRAPER_RECORDS_PK_COLUMN} 
                                        AND res.{IMAGE_SCRAPER_RESULT_SORT_ORDER_COLUMN} IS NOT NULL 
                                        AND res.{IMAGE_SCRAPER_RESULT_SORT_ORDER_COLUMN} > 0))
                ORDER BY r.{SCRAPER_RECORDS_PK_COLUMN};
            """)
            db_res_entries = await db_conn_fetch.execute(sql_fetch_entries, {"fid": file_id_for_db, "start_eid": actual_start_entry_id or 0})
            entries_to_process_list = db_res_entries.fetchall()
            db_res_entries.close()
        logger.info(f"FileID {file_id_for_db}: Fetched {len(entries_to_process_list)} entries for processing (from actual_start_entry_id {actual_start_entry_id or 0}).")
    except SQLAlchemyError as db_exc_fetch:
        # ... (error handling)
        error_msg = f"Database error fetching entries for FileID {file_id_for_db}: {db_exc_fetch}"
        logger.error(error_msg, exc_info=True)
        log_url = await upload_log_file(file_id_db_str, current_log_filename, logger, db_record_file_id_to_update=file_id_db_str)
        return {"error": error_msg, "log_filename": current_log_filename, "log_public_url": log_url or "", "last_entry_id_processed": str(entry_id or "")}


    if not entries_to_process_list:
        # ... (no entries message)
        success_msg = f"No entries require processing for FileID {file_id_for_db} (from start_id {actual_start_entry_id or 0})."
        logger.info(success_msg)
        log_url = await upload_log_file(file_id_db_str, current_log_filename, logger, db_record_file_id_to_update=file_id_db_str)
        return {
            "message": success_msg, "file_id": file_id_db_str, "total_entries_fetched_for_processing": 0,
            "successful_entries_processed": 0, "failed_entries_processed": 0,
            "log_filename": current_log_filename, "log_public_url": log_url or "",
            "last_entry_id_processed": str(entry_id or actual_start_entry_id or 0)
        }


    batched_entry_groups = [entries_to_process_list[i:i + BATCH_SIZE_PER_GATHER] for i in range(0, len(entries_to_process_list), BATCH_SIZE_PER_GATHER)]
    logger.info(f"FileID {file_id_for_db}: Divided {len(entries_to_process_list)} entries into {len(batched_entry_groups)} batches.")
    count_successful_entries = 0
    count_failed_entries = 0
    max_successful_entry_id_this_run = (actual_start_entry_id or 0) -1 # Ensure it's less than any valid ID

    entry_processing_semaphore = asyncio.Semaphore(MAX_CONCURRENT_ENTRY_PROCESSING)

    async def _process_single_entry_with_retry(entry_data_tuple: tuple) -> tuple[int, bool]:
        entry_id_curr, model_curr, brand_curr, color_curr, category_curr = entry_data_tuple
        async with entry_processing_semaphore:
            for attempt in range(1, MAX_ENTRY_ATTEMPTS + 1):
                logger.info(f"FileID {file_id_for_db}, EntryID {entry_id_curr}: Attempt {attempt}/{MAX_ENTRY_ATTEMPTS} processing.")
                try:
                    search_results = await orchestrate_entry_search(
                        original_search_term=str(model_curr) if model_curr else "", # Ensure string
                        original_brand=str(brand_curr) if brand_curr else None, # Ensure string or None
                        entry_id=entry_id_curr, search_api_endpoint=configured_search_endpoint,
                        use_all_variations=use_all_variations, file_id_context=file_id_for_db,
                        logger=logger, brand_rules=brand_rules_data
                    )
                    valid_results = [r for r in search_results if r.get("ImageUrl") and not str(r["ImageUrl"]).startswith("placeholder://")]
                    if not valid_results:
                        logger.warning(f"FileID {file_id_for_db}, EntryID {entry_id_curr}: No valid results (Attempt {attempt}).")
                        if attempt == MAX_ENTRY_ATTEMPTS: return entry_id_curr, False
                        await asyncio.sleep(attempt * 1.5)
                        continue
                    enqueue_success = await insert_search_results(
                        results=valid_results, logger=logger, file_id=file_id_db_str, background_tasks=background_tasks
                    )
                    if enqueue_success:
                        logger.info(f"FileID {file_id_for_db}, EntryID {entry_id_curr}: Enqueued {len(valid_results)} results.")
                        step1_sql = f"UPDATE {SCRAPER_RECORDS_TABLE_NAME} SET {SCRAPER_RECORDS_STEP1_COLUMN} = GETUTCDATE() WHERE {SCRAPER_RECORDS_PK_COLUMN} = :eid"
                        await enqueue_db_update(
                            file_id=file_id_db_str, sql=step1_sql, params={"eid": entry_id_curr},
                            task_type=f"update_step1_entry_{entry_id_curr}", correlation_id=str(uuid.uuid4()), logger_param=logger
                        )
                        return entry_id_curr, True
                    else:
                        logger.error(f"FileID {file_id_for_db}, EntryID {entry_id_curr}: Failed to enqueue results (Attempt {attempt}).")
                        if attempt == MAX_ENTRY_ATTEMPTS: return entry_id_curr, False
                        await asyncio.sleep(attempt * 1.5)
                        continue
                except Exception as e_entry_proc:
                    logger.error(f"FileID {file_id_for_db}, EntryID {entry_id_curr}: Exception attempt {attempt}: {e_entry_proc}", exc_info=True)
                    if attempt == MAX_ENTRY_ATTEMPTS: return entry_id_curr, False
                    await asyncio.sleep(attempt * 2)
            return entry_id_curr, False # Should not be reached if loop logic is correct

    for batch_idx, entry_group in enumerate(batched_entry_groups, 1):
        logger.info(f"FileID {file_id_for_db}: Processing batch {batch_idx}/{len(batched_entry_groups)} with {len(entry_group)} entries.")
        batch_start_time = time.monotonic()
        entry_processing_outcomes = await asyncio.gather(*[_process_single_entry_with_retry(et) for et in entry_group], return_exceptions=True)
        for original_entry_tuple, outcome_result in zip(entry_group, entry_processing_outcomes):
            entry_id_processed = original_entry_tuple[0]
            if isinstance(outcome_result, Exception):
                logger.error(f"FileID {file_id_for_db}, EntryID {entry_id_processed}: Failed with unhandled exception: {outcome_result}", exc_info=outcome_result)
                count_failed_entries += 1
            elif isinstance(outcome_result, tuple) and len(outcome_result) == 2:
                _, success_status = outcome_result
                if success_status:
                    count_successful_entries += 1
                    max_successful_entry_id_this_run = max(max_successful_entry_id_this_run, entry_id_processed)
                else:
                    count_failed_entries += 1
            else:
                logger.error(f"FileID {file_id_for_db}, EntryID {entry_id_processed}: Unexpected outcome: {outcome_result}")
                count_failed_entries += 1
        batch_duration_s = time.monotonic() - batch_start_time
        logger.info(f"FileID {file_id_for_db}: Batch {batch_idx} completed in {batch_duration_s:.2f}s. Totals - Success: {count_successful_entries}, Fail: {count_failed_entries}.")
        _log_resource_usage(f"after batch {batch_idx}")
        await asyncio.sleep(0.1) # Shorter sleep

    logger.info(f"FileID {file_id_for_db}: All batches processed. Final - Success: {count_successful_entries}, Fail: {count_failed_entries} of {len(entries_to_process_list)} attempted.")

    if count_successful_entries > 0:
        logger.info(f"FileID {file_id_for_db}: Enqueuing update for ImageCompleteTime.")
        complete_time_sql = f"UPDATE {IMAGE_SCRAPER_FILES_TABLE_NAME} SET {IMAGE_SCRAPER_FILES_IMAGE_COMPLETE_TIME_COLUMN} = GETUTCDATE() WHERE {IMAGE_SCRAPER_FILES_PK_COLUMN} = :fid"
        await enqueue_db_update(
            file_id=file_id_db_str, sql=complete_time_sql, params={"fid": file_id_for_db},
            task_type=f"update_img_complete_time_file_{file_id_for_db}", correlation_id=str(uuid.uuid4()), logger_param=logger
        )

    logger.info(f"FileID {file_id_for_db}: Initiating post-search tasks (sorting, file generation).")
    try:
        await update_sort_order(file_id_db_str, logger=logger, background_tasks=background_tasks)
        logger.info(f"FileID {file_id_for_db}: Sort order update task initiated.")
        await run_generate_download_file(file_id_db_str, logger, background_tasks)
        logger.info(f"FileID {file_id_for_db}: Download file generation task initiated.")
    except Exception as e_post_tasks:
        logger.error(f"FileID {file_id_for_db}: Error during post-search tasks: {e_post_tasks}", exc_info=True)

    final_log_s3_url = await upload_log_file(file_id_db_str, current_log_filename, logger, db_record_file_id_to_update=file_id_db_str)
    email_to_list = await get_send_to_email(file_id_for_db, logger=logger)
    if email_to_list:
        subject = f"SuperScraper Batch Report for FileID: {file_id_db_str}"
        body = (f"Batch processing for FileID {file_id_db_str} finished.\n\n"
                f"Total entries fetched for this run: {len(entries_to_process_list)}\n"
                f"Successfully processed: {count_successful_entries}\nFailed this run: {count_failed_entries}\n"
                f"Highest EntryID successfully processed: {max_successful_entry_id_this_run if max_successful_entry_id_this_run >=0 else 'N/A'}\n"
                f"Settings: UseAllVariations={use_all_variations}, NumWorkersHint={num_workers}\n"
                f"Log: {final_log_s3_url or 'Log upload failed.'}\n\nPost-search tasks initiated.")
        try:
            await send_message_email(email_to_list, subject=subject, message=body, logger=logger)
            logger.info(f"FileID {file_id_for_db}: Completion email sent to: {email_to_list}")
        except Exception as e_email: logger.error(f"FileID {file_id_for_db}: Failed to send email: {e_email}", exc_info=True)

    return {
        "message": "Search processing batch completed.", "file_id": file_id_db_str,
        "total_entries_fetched_for_processing": len(entries_to_process_list),
        "successful_entries_processed": count_successful_entries, "failed_entries_processed": count_failed_entries,
        "last_entry_id_successfully_processed_in_run": max_successful_entry_id_this_run if max_successful_entry_id_this_run >=0 else None,
        "log_filename_on_server": current_log_filename, "log_public_url": final_log_s3_url or "N/A",
        "settings_used": {"use_all_variations": use_all_variations, "num_workers_hint": num_workers}
    }

# --- V6 Endpoints ---

@router.post("/populate-results-from-warehouse/{file_id}", tags=["Processing", "Warehouse"])
async def api_populate_results_from_warehouse(
    file_id: str,
    limit: Optional[int] = Query(1000, ge=1, le=10000, description=f"Max records from {SCRAPER_RECORDS_TABLE_NAME} to process."),
    base_image_url: str = Query("https://cms.rtsplusdev.com/files/icon_warehouse_images", description="Base URL for warehouse image paths.")
):
    job_run_id = f"warehouse_populate_{file_id}_{uuid.uuid4().hex[:6]}"
    logger, log_file_path = setup_job_logger(job_id=job_run_id, console_output=True)
    logger.info(f"[{job_run_id}] API Call: Populate results from warehouse for FileID '{file_id}'. Limit: {limit}.")
    num_scraper_records_matched, num_results_prepared, num_results_enqueued, num_status_updates_enqueued, num_data_prep_errors = 0, 0, 0, 0, 0
    try:
        file_id_int = int(file_id)
        async with async_engine.connect() as conn:
            file_exists_q = await conn.execute(text(f"SELECT 1 FROM {IMAGE_SCRAPER_FILES_TABLE_NAME} WHERE {IMAGE_SCRAPER_FILES_PK_COLUMN} = :fid"), {"fid": file_id_int})
            if not file_exists_q.scalar_one_or_none():
                logger.error(f"[{job_run_id}] FileID '{file_id_int}' not found in {IMAGE_SCRAPER_FILES_TABLE_NAME}.")
                await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
                raise HTTPException(status_code=404, detail=f"FileID {file_id_int} not found.")
        async with async_engine.connect() as conn: # New connection for fetch
            fetch_sql = text(f"""
                SELECT TOP (:limit_val)
                    isr.{SCRAPER_RECORDS_PK_COLUMN} AS ScraperEntryID, isr.{SCRAPER_RECORDS_PRODUCT_MODEL_COLUMN} AS ScraperProductModel,
                    isr.{SCRAPER_RECORDS_PRODUCT_BRAND_COLUMN} AS ScraperProductBrand, iwi.{WAREHOUSE_IMAGES_MODEL_NUMBER_COLUMN} AS WarehouseModelNumber,
                    iwi.{WAREHOUSE_IMAGES_MODEL_CLEAN_COLUMN} AS WarehouseModelClean, iwi.{WAREHOUSE_IMAGES_MODEL_FOLDER_COLUMN} AS WarehouseModelFolder,
                    iwi.{WAREHOUSE_IMAGES_MODEL_SOURCE_COLUMN} AS WarehouseModelSource
                FROM {SCRAPER_RECORDS_TABLE_NAME} isr JOIN {WAREHOUSE_IMAGES_TABLE_NAME} iwi ON isr.{SCRAPER_RECORDS_PRODUCT_MODEL_COLUMN} = iwi.{WAREHOUSE_IMAGES_MODEL_CLEAN_COLUMN}
                WHERE isr.{SCRAPER_RECORDS_FILE_ID_FK_COLUMN} = :fid AND (isr.{SCRAPER_RECORDS_ENTRY_STATUS_COLUMN} = {STATUS_PENDING_WAREHOUSE_CHECK} OR isr.{SCRAPER_RECORDS_ENTRY_STATUS_COLUMN} IS NULL)
                  AND iwi.{WAREHOUSE_IMAGES_MODEL_CLEAN_COLUMN} IS NOT NULL AND iwi.{WAREHOUSE_IMAGES_MODEL_CLEAN_COLUMN} <> ''
                  AND iwi.{WAREHOUSE_IMAGES_MODEL_FOLDER_COLUMN} IS NOT NULL AND iwi.{WAREHOUSE_IMAGES_MODEL_FOLDER_COLUMN} <> ''
                ORDER BY isr.{SCRAPER_RECORDS_PK_COLUMN};
            """)
            matched_records_cursor = await conn.execute(fetch_sql, {"fid": file_id_int, "limit_val": limit})
            records_for_processing = matched_records_cursor.mappings().fetchall()
            num_scraper_records_matched = len(records_for_processing)
        if not records_for_processing:
            msg = f"No records pending warehouse check for FileID '{file_id_int}' matching criteria."
            logger.info(f"[{job_run_id}] {msg}")
            log_s3_url = await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
            return {"status": "no_action_required", "message": msg, "log_url": log_s3_url}
        logger.info(f"[{job_run_id}] Found {num_scraper_records_matched} scraper records with warehouse matches.")
        results_to_insert_payload: List[Dict] = []
        processed_entry_ids_for_status_update: List[int] = []
        for rec_map in records_for_processing:
            try:
                scraper_eid = rec_map['ScraperEntryID']
                model_clean, model_folder = rec_map.get('WarehouseModelClean'), rec_map.get('WarehouseModelFolder')
                if not model_clean or not model_folder:
                    logger.warning(f"[{job_run_id}] Skipping ScraperEntryID {scraper_eid}: WarehouseModelClean/Folder missing."); num_data_prep_errors += 1; continue
                model_url_part = model_clean
                for ext in ['.png', '.jpg', '.jpeg', '.webp']:
                    if model_url_part.lower().endswith(ext): model_url_part = model_url_part[:-len(ext)]; break
                img_url = f"{base_image_url.rstrip('/')}/{model_folder.strip('/')}/{model_url_part}.png"
                desc = f"{rec_map.get('ScraperProductBrand') or 'Brand'} {rec_map.get('WarehouseModelNumber') or rec_map.get('ScraperProductModel') or 'Product'}"
                source_domain = urlparse(base_image_url).netloc or "warehouse.internal"
                actual_source = rec_map.get('WarehouseModelSource') or source_domain
                results_to_insert_payload.append({
                    IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN: scraper_eid, IMAGE_SCRAPER_RESULT_IMAGE_URL_COLUMN: img_url,
                    IMAGE_SCRAPER_RESULT_IMAGE_DESC_COLUMN: desc, IMAGE_SCRAPER_RESULT_IMAGE_SOURCE_COLUMN: actual_source,
                    IMAGE_SCRAPER_RESULT_IMAGE_URL_THUMBNAIL_COLUMN: img_url, IMAGE_SCRAPER_RESULT_SORT_ORDER_COLUMN: 1,
                    IMAGE_SCRAPER_RESULT_SOURCE_TYPE_COLUMN: 'Warehouse'})
                processed_entry_ids_for_status_update.append(scraper_eid); num_results_prepared += 1
            except Exception as e_prep: logger.error(f"[{job_run_id}] Error preparing data for ScraperEntryID {rec_map.get('ScraperEntryID', 'UNKNOWN')}: {e_prep}", exc_info=True); num_data_prep_errors += 1
        if results_to_insert_payload:
            logger.info(f"[{job_run_id}] Enqueuing insertion of {len(results_to_insert_payload)} results from warehouse.")
            insertion_enqueued = await insert_search_results(results=results_to_insert_payload, logger=logger, file_id=file_id)
            if insertion_enqueued:
                num_results_enqueued = len(results_to_insert_payload)
                logger.info(f"[{job_run_id}] Successfully enqueued {num_results_enqueued} warehouse results.")
                if processed_entry_ids_for_status_update:
                    unique_eids = list(set(processed_entry_ids_for_status_update))
                    status_update_sql = f"UPDATE {SCRAPER_RECORDS_TABLE_NAME} SET {SCRAPER_RECORDS_ENTRY_STATUS_COLUMN} = {STATUS_WAREHOUSE_RESULT_POPULATED}, {SCRAPER_RECORDS_WAREHOUSE_MATCH_TIME_COLUMN} = GETUTCDATE() WHERE {SCRAPER_RECORDS_PK_COLUMN} IN :eids_list;"
                    await enqueue_db_update(file_id=job_run_id, sql=status_update_sql, params={"eids_list": unique_eids}, task_type="batch_update_scraper_status_warehouse_populated", correlation_id=str(uuid.uuid4()), logger_param=logger)
                    num_status_updates_enqueued = len(unique_eids)
                    logger.info(f"[{job_run_id}] Enqueued status update for {num_status_updates_enqueued} scraper records.")
            else: logger.error(f"[{job_run_id}] `insert_search_results` reported failure to enqueue warehouse results.")
        final_message = (f"Warehouse-to-Result for FileID '{file_id}': Matched={num_scraper_records_matched}, Prepared={num_results_prepared}, EnqueuedInsertions={num_results_enqueued}, EnqueuedStatusUpdates={num_status_updates_enqueued}, PrepErrors={num_data_prep_errors}.")
        logger.info(f"[{job_run_id}] {final_message}")
        final_log_s3_url = await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
        return {"status": "processing_enqueued" if num_results_enqueued > 0 else "no_new_insertions", "message": final_message, "job_run_id": job_run_id, "original_file_id": file_id, "counts": {"matched": num_scraper_records_matched, "prepared": num_results_prepared, "enqueued_insert": num_results_enqueued, "enqueued_status_updates": num_status_updates_enqueued, "prep_errors": num_data_prep_errors}, "log_url": final_log_s3_url}
    except HTTPException as http_exc: logger.warning(f"[{job_run_id}] HTTPException: {http_exc.detail}"); raise http_exc
    except Exception as e_main:
        logger.critical(f"[{job_run_id}] Critical error in warehouse population API for FileID '{file_id}': {e_main}", exc_info=True)
        crit_err_log_url = await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
        raise HTTPException(status_code=500, detail=f"Critical internal error. Job Run ID: {job_run_id}. Log: {crit_err_log_url or 'Log upload failed.'}")

@router.post("/clear-ai-json/{file_id}", tags=["Database"])
async def api_clear_ai_json(file_id: str, entry_ids: Optional[List[int]] = Query(None)):
    job_run_id = f"clear_ai_data_{file_id}_{uuid.uuid4().hex[:6]}"
    logger, log_file_path = setup_job_logger(job_id=job_run_id, console_output=True)
    logger.info(f"[{job_run_id}] API Call: Clear AI JSON/Caption for FileID: {file_id}" + (f", specific EntryIDs: {entry_ids}" if entry_ids else "."))
    try:
        file_id_int = int(file_id)
        # Optional: FileID validation in utb_ImageScraperFiles
        base_sql_clear = f"UPDATE {IMAGE_SCRAPER_RESULT_TABLE_NAME} SET {IMAGE_SCRAPER_RESULT_AI_JSON_COLUMN} = NULL, {IMAGE_SCRAPER_RESULT_AI_CAPTION_COLUMN} = NULL WHERE {IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN} IN (SELECT r.{SCRAPER_RECORDS_PK_COLUMN} FROM {SCRAPER_RECORDS_TABLE_NAME} r WHERE r.{SCRAPER_RECORDS_FILE_ID_FK_COLUMN} = :fid_param)"
        params_for_clear: Dict[str, Any] = {"fid_param": file_id_int}
        if entry_ids:
            base_sql_clear += f" AND {IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN} IN :eids_list_param"
            params_for_clear["eids_list_param"] = entry_ids
        task_corr_id = str(uuid.uuid4())
        await enqueue_db_update(file_id=job_run_id, sql=base_sql_clear, params=params_for_clear, task_type="clear_ai_data_for_file_entries", correlation_id=task_corr_id, logger_param=logger)
        success_msg = f"Task to clear AI data for FileID '{file_id}' (EntryIDs: {'All' if not entry_ids else entry_ids}) enqueued."
        logger.info(f"[{job_run_id}] {success_msg} CorrelationID: {task_corr_id}")
        final_log_s3_url = await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
        return {"status": "task_enqueued", "message": success_msg, "correlation_id": task_corr_id, "log_url": final_log_s3_url}
    except ValueError: err_msg = f"Invalid FileID format: '{file_id}'."; logger.error(f"[{job_run_id}] {err_msg}", exc_info=True); await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id); raise HTTPException(status_code=400, detail=err_msg)
    except Exception as e_main:
        logger.critical(f"[{job_run_id}] Critical error in clear AI data API for FileID '{file_id}': {e_main}", exc_info=True)
        crit_err_log_url = await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
        raise HTTPException(status_code=500, detail=f"Internal server error. Job Run ID: {job_run_id}. Log: {crit_err_log_url or 'Log upload failed.'}")

@router.post("/restart-job/{file_id}", tags=["Processing"], response_model=None)
async def api_process_restart_job(
    file_id: str,
    background_tasks: BackgroundTasks, 
    entry_id: Optional[int] = Query(None),
    use_all_variations: bool = Query(False),
    num_workers_hint: int = Query(4, ge=1, le=cpu_count() * 2),
):
    
    job_run_id = f"restart_job_{file_id}_{entry_id or 'auto'}_{'allvars' if use_all_variations else 'stdvars'}_{num_workers_hint}w"
    job_result = await run_job_with_logging(
        job_func=process_restart_batch, file_id_context=job_run_id,
        file_id_db_str=file_id, entry_id=entry_id, use_all_variations=use_all_variations,
        background_tasks=background_tasks, num_workers=num_workers_hint
    )
    if job_result["status_code"] != 200:
        raise HTTPException(status_code=job_result["status_code"], detail=job_result["message"])
    return {"status_code": 200, "message": f"Job restart for FileID '{file_id}' initiated.", "details": job_result.get("data"), "log_url": job_result.get("debug_info", {}).get("log_s3_url", "N/A")}


class TestableSearchResult(BaseModel):
    EntryID: int
    ImageUrl: Union[PydanticHttpUrl, str]
    ImageDesc: Optional[str] = None
    ImageSource: Optional[Union[PydanticHttpUrl, str]] = None
    ImageUrlThumbnail: Optional[Union[PydanticHttpUrl, str]] = None
    ProductCategory: Optional[str] = None

class TestInsertResultsRequest(BaseModel):
    results: List[TestableSearchResult]

@router.post("/test-insert-results/{file_id}", tags=["Testing"], response_model=None)
async def api_test_insert_search_results(
    file_id: str,
    payload: TestInsertResultsRequest,
    background_tasks: BackgroundTasks,
):
    job_run_id = f"test_insert_results_{file_id}_{uuid.uuid4().hex[:6]}"
    logger, log_file_path = setup_job_logger(job_id=job_run_id, console_output=True)
    logger.info(f"[{job_run_id}] API Call: Test insert results for FileID '{file_id}'. Count: {len(payload.results)}")
    try:
        file_id_int = int(file_id) # Validate format early
        # Optional: FileID validation in utb_ImageScraperFiles
        results_for_insertion = [{"EntryID": item.EntryID, "ImageUrl": str(item.ImageUrl), "ImageDesc": item.ImageDesc, "ImageSource": str(item.ImageSource) if item.ImageSource else None, "ImageUrlThumbnail": str(item.ImageUrlThumbnail) if item.ImageUrlThumbnail else None, "ProductCategory": item.ProductCategory} for item in payload.results]
        if not results_for_insertion:
            logger.info(f"[{job_run_id}] No results in payload for FileID '{file_id}'.")
            # No log upload usually needed for empty valid payload.
            return {"status": "no_action", "message": "No results provided in payload."}
        logger.debug(f"[{job_run_id}] Prepared {len(results_for_insertion)} results. Sample: {results_for_insertion[0] if results_for_insertion else '{}'}")
        enqueue_op_successful = await insert_search_results(results=results_for_insertion, logger=logger, file_id=file_id, background_tasks=background_tasks)
        final_log_s3_url = await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
        if enqueue_op_successful:
            msg = f"Successfully enqueued {len(results_for_insertion)} test results for FileID '{file_id}'."
            logger.info(f"[{job_run_id}] {msg}")
            return {"status": "enqueued_successfully", "message": msg, "log_url": final_log_s3_url}
        else:
            msg = f"Failed to enqueue test results for FileID '{file_id}'. Check logs."
            logger.error(f"[{job_run_id}] {msg}")
            return {"status": "enqueue_failed", "message": msg, "log_url": final_log_s3_url} # Still 200, but operation had issues
    except ValueError: err_msg = f"Invalid FileID format: '{file_id}'."; logger.error(f"[{job_run_id}] {err_msg}", exc_info=True); await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id); raise HTTPException(status_code=400, detail=err_msg)
    except Exception as e_main:
        logger.critical(f"[{job_run_id}] Critical error in test insert API for FileID '{file_id}': {e_main}", exc_info=True)
        crit_err_log_url = await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
        raise HTTPException(status_code=500, detail=f"Internal server error. Job Run ID: {job_run_id}. Log: {crit_err_log_url or 'Log upload failed.'}")

@router.get("/get-send-to-email/{file_id}", tags=["Database"])
async def api_get_send_to_email_address(file_id: str):
    job_run_id = f"get_email_{file_id}"
    logger, _ = setup_job_logger(job_id=job_run_id, console_output=False, log_level=logging.INFO) # Less verbose for GET
    try:
        file_id_int = int(file_id)
        email_address_or_list = await get_send_to_email(file_id_int, logger)
        if email_address_or_list is None:
            logger.info(f"[{job_run_id}] No email configured for FileID '{file_id_int}'.")
            return {"status_code": 200, "message": "No email address configured.", "data": None}
        logger.info(f"[{job_run_id}] Retrieved email(s) for FileID '{file_id_int}'.")
        return {"status_code": 200, "message": "Email address(es) retrieved.", "data": email_address_or_list}
    except ValueError: err_msg = f"Invalid FileID format: '{file_id}'."; logger.error(f"[{job_run_id}] {err_msg}"); raise HTTPException(status_code=400, detail=err_msg)
    except Exception as e_get_email: logger.error(f"[{job_run_id}] Error retrieving email for FileID '{file_id}': {e_get_email}", exc_info=True); raise HTTPException(status_code=500, detail=f"Internal error: {str(e_get_email)}")

@router.post("/run-initial-sort/{file_id}", tags=["Sorting"], response_model=None)
async def api_run_initial_sort(file_id: str, background_tasks: BackgroundTasks):
    job_run_id = f"initial_sort_{file_id}_{uuid.uuid4().hex[:6]}"
    job_result = await run_job_with_logging(job_func=update_initial_sort_order, file_id_context=job_run_id, file_id=file_id, background_tasks=background_tasks)
    if job_result["status_code"] != 200: raise HTTPException(status_code=job_result["status_code"], detail=job_result["message"])
    return {"status_code": 200, "message": "Initial sort job completed.", "details": job_result["data"], "log_url": job_result.get("debug_info", {}).get("log_s3_url", "N/A")}

@router.post("/run-no-image-sort/{file_id}", tags=["Sorting"], response_model=None)
async def api_run_no_image_sort(file_id: str,  background_tasks: BackgroundTasks):
    job_run_id = f"no_image_sort_{file_id}_{uuid.uuid4().hex[:6]}"
    job_result = await run_job_with_logging(job_func=update_sort_no_image_entry, file_id_context=job_run_id, file_id=file_id, background_tasks=background_tasks)
    if job_result["status_code"] != 200: raise HTTPException(status_code=job_result["status_code"], detail=job_result["message"])
    return {"status_code": 200, "message": "No-image sort job completed.", "details": job_result["data"], "log_url": job_result.get("debug_info", {}).get("log_s3_url", "N/A")}

@router.post("/process-images-ai/{file_id}", tags=["AI"], response_model=None)
async def api_run_ai_image_processing(
    file_id: str, 
    background_tasks: BackgroundTasks,
    entry_ids: Optional[List[int]] = Query(None), step: int = Query(0),
    limit: int = Query(5000, ge=1), concurrency: int = Query(10, ge=1, le=50), # Added le for concurrency
):
    job_run_id = f"ai_processing_{file_id}_{uuid.uuid4().hex[:6]}"
    job_result = await run_job_with_logging(
        job_func=batch_vision_reason, file_id_context=job_run_id,
        file_id=file_id, entry_ids=entry_ids, step=step, limit=limit, concurrency=concurrency, background_tasks=background_tasks
    )
    if job_result["status_code"] != 200: raise HTTPException(status_code=job_result["status_code"], detail=job_result["message"])
    return {"status_code": 200, "message": "AI image processing job completed.", "details": job_result["data"], "log_url": job_result.get("debug_info", {}).get("log_s3_url", "N/A")}

@router.get("/get-images-for-excel/{file_id}", tags=["Data Export"])
async def api_get_images_for_excel_export(file_id: str):
    job_run_id = f"get_excel_images_{file_id}"
    logger, log_file_path = setup_job_logger(job_id=job_run_id, console_output=False, log_level=logging.INFO)
    try:
        file_id_int = int(file_id)
        df_results = await get_images_excel_db(str(file_id_int), logger)
        if df_results.empty: return {"status_code": 200, "message": "No images found for Excel export.", "data": []}
        # No log upload for simple success GET.
        return {"status_code": 200, "message": "Image data for Excel export retrieved.", "data": df_results.to_dict(orient='records')}
    except ValueError: raise HTTPException(status_code=400, detail=f"Invalid FileID format: {file_id}")
    except Exception as e:
        logger.error(f"[{job_run_id}] Error fetching images for Excel: {e}", exc_info=True)
        log_s3_url = await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}. Log: {log_s3_url}")

@router.post("/mark-file-generation-complete/{file_id}", tags=["File Status"])
async def api_mark_file_generation_complete(file_id: str):
    job_run_id = f"mark_gen_complete_{file_id}_{uuid.uuid4().hex[:6]}"
    logger, log_file_path = setup_job_logger(job_id=job_run_id, console_output=False, log_level=logging.INFO)
    try:
        file_id_int = int(file_id)
        await update_file_generate_complete(str(file_id_int), logger) # Assumes this enqueues the DB update
        log_s3_url = await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
        return {"status_code": 200, "message": "File generation status marked as complete.", "log_url": log_s3_url}
    except ValueError: err_msg=f"Invalid FileID: {file_id}"; logger.error(f"[{job_run_id}] {err_msg}"); await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id); raise HTTPException(status_code=400, detail=err_msg)
    except Exception as e:
        logger.error(f"[{job_run_id}] Error marking file generation complete: {e}", exc_info=True)
        log_s3_url = await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}. Log: {log_s3_url}")

class FileLocationPayload(BaseModel): file_location_url: PydanticHttpUrl

@router.post("/mark-file-location-complete/{file_id}", tags=["File Status"])
async def api_mark_file_location_complete(file_id: str, payload: FileLocationPayload):
    job_run_id = f"mark_loc_complete_{file_id}_{uuid.uuid4().hex[:6]}"
    logger, log_file_path = setup_job_logger(job_id=job_run_id, console_output=False, log_level=logging.INFO)
    try:
        file_id_int = int(file_id)
        await update_file_location_complete(str(file_id_int), str(payload.file_location_url), logger) # Assumes this enqueues
        log_s3_url = await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
        return {"status_code": 200, "message": "File location status marked as complete.", "log_url": log_s3_url}
    except ValueError: err_msg=f"Invalid FileID: {file_id}"; logger.error(f"[{job_run_id}] {err_msg}"); await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id); raise HTTPException(status_code=400, detail=err_msg)
    except Exception as e:
        logger.error(f"[{job_run_id}] Error marking file location complete: {e}", exc_info=True)
        log_s3_url = await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}. Log: {log_s3_url}")

@router.post("/sort-by-ai-relevance/{file_id}", tags=["Sorting"])
async def api_sort_by_ai_relevance(
    file_id: str, entry_ids: Optional[List[int]] = Query(None),
    use_softmax_normalization: bool = Query(False)
):
    job_run_id = f"sort_ai_relevance_{file_id}_{uuid.uuid4().hex[:6]}"
    logger, log_file_path = setup_job_logger(job_id=job_run_id, console_output=True)
    logger.info(f"[{job_run_id}] API Call: Sort by AI relevance for FileID: {file_id}, EntryIDs: {entry_ids}, Softmax: {use_softmax_normalization}")
    try:
        file_id_int = int(file_id)
        # Optional: FileID validation
        score_weights = {"relevance": 0.4, "category": 0.2, "color": 0.15, "brand": 0.15, "sentiment": 0.05, "model": 0.05} # Example weights
        logger.debug(f"[{job_run_id}] Using score weights: {score_weights}")
        fetch_ai_json_sql = f"SELECT {IMAGE_SCRAPER_RESULT_PK_COLUMN}, {IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN}, {IMAGE_SCRAPER_RESULT_AI_JSON_COLUMN} FROM {IMAGE_SCRAPER_RESULT_TABLE_NAME} WHERE {IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN} IN (SELECT r.{SCRAPER_RECORDS_PK_COLUMN} FROM {SCRAPER_RECORDS_TABLE_NAME} r WHERE r.{SCRAPER_RECORDS_FILE_ID_FK_COLUMN} = :fid) AND {IMAGE_SCRAPER_RESULT_AI_JSON_COLUMN} IS NOT NULL"
        fetch_params: Dict[str, Any] = {"fid": file_id_int}
        if entry_ids: fetch_ai_json_sql += f" AND {IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN} IN :eids_list"; fetch_params["eids_list"] = tuple(entry_ids) # Use tuple for IN clause with SQLAlchemy
        results_with_ai_json: List[Dict[str, Any]] = []
        async with async_engine.connect() as conn:
            db_cursor = await conn.execute(text(fetch_ai_json_sql), fetch_params)
            results_with_ai_json = [dict(row._mapping) for row in db_cursor.fetchall()] # Correctly get dict from RowProxy
        if not results_with_ai_json: msg = "No results with AiJson found to sort."; logger.info(f"[{job_run_id}] {msg}"); return {"status": "no_action_required", "message": msg}
        scores_by_entry_id = defaultdict(list)
        for item_row in results_with_ai_json:
            try:
                ai_data_dict = json.loads(item_row[IMAGE_SCRAPER_RESULT_AI_JSON_COLUMN])
                raw_scores = ai_data_dict.get("scores", {})
                normalized_scores = {key: max(0.0, min(1.0, float(raw_scores.get(key, 0.0)))) for key in score_weights}
                composite_score_val = sum(score_weights[k] * normalized_scores[k] for k in score_weights)
                scores_by_entry_id[item_row[IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN]].append({"ResultID": item_row[IMAGE_SCRAPER_RESULT_PK_COLUMN], "composite_score": composite_score_val})
            except (json.JSONDecodeError, TypeError, ValueError) as e_score_proc: logger.warning(f"[{job_run_id}] Error processing AiJson for ResultID {item_row[IMAGE_SCRAPER_RESULT_PK_COLUMN]}: {e_score_proc}")
        update_tasks_payloads = []
        for current_entry_id, entry_results_list in scores_by_entry_id.items():
            if not entry_results_list: continue
            if use_softmax_normalization and len(entry_results_list) > 1:
                comp_scores = [res['composite_score'] for res in entry_results_list]
                exp_s = [math.exp(s - max(comp_scores)) for s in comp_scores] # Stability improvement for softmax
                sum_exp_s = sum(exp_s)
                softmax_s = [es / sum_exp_s for es in exp_s] if sum_exp_s > 0 else [1.0/len(comp_scores)] * len(comp_scores) # Uniform if sum is 0
                for res, sm_score in zip(entry_results_list, softmax_s): res['composite_score'] = sm_score
            entry_results_list.sort(key=lambda x: (-x['composite_score'], x['ResultID']))
            for sort_idx, final_res_data in enumerate(entry_results_list, 1):
                update_tasks_payloads.append({"sql": f"UPDATE {IMAGE_SCRAPER_RESULT_TABLE_NAME} SET {IMAGE_SCRAPER_RESULT_SORT_ORDER_COLUMN} = :sort_val WHERE {IMAGE_SCRAPER_RESULT_PK_COLUMN} = :res_id", "params": {"sort_val": sort_idx, "res_id": final_res_data['ResultID']}, "task_type": f"update_sort_order_ai_res_{final_res_data['ResultID']}", "correlation_id": str(uuid.uuid4())})
        if update_tasks_payloads:
            logger.info(f"[{job_run_id}] Enqueuing {len(update_tasks_payloads)} SortOrder updates based on AI relevance.")
            for task_p in update_tasks_payloads: await enqueue_db_update(file_id=job_run_id, sql=task_p["sql"], params=task_p["params"], task_type=task_p["task_type"], correlation_id=task_p["correlation_id"], logger_param=logger)
        final_msg = f"AI relevance sorting initiated. {len(update_tasks_payloads)} SortOrder updates enqueued."
        logger.info(f"[{job_run_id}] {final_msg}")
        final_log_url = await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
        return {"status": "tasks_enqueued", "message": final_msg, "log_url": final_log_url}
    except ValueError: err_msg=f"Invalid FileID: {file_id}"; logger.error(f"[{job_run_id}] {err_msg}"); await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id); raise HTTPException(status_code=400, detail=err_msg)
    except Exception as e_main: logger.critical(f"[{job_run_id}] Critical error in AI relevance sort API: {e_main}", exc_info=True); crit_log_url = await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id); raise HTTPException(status_code=500, detail=f"Internal server error. Job Run ID: {job_run_id}. Log: {crit_log_url}")

@router.post("/reset-step1-status/{file_id}", tags=["Database"])
async def api_reset_step1_status_for_file(file_id: str):
    job_run_id = f"reset_step1_{file_id}_{uuid.uuid4().hex[:6]}"
    logger, log_file_path = setup_job_logger(job_id=job_run_id, console_output=True)
    logger.info(f"[{job_run_id}] API Call: Reset Step1 status for FileID: {file_id}")
    try:
        file_id_int = int(file_id) # FileID validation
        reset_sql = f"UPDATE {SCRAPER_RECORDS_TABLE_NAME} SET {SCRAPER_RECORDS_STEP1_COLUMN} = NULL WHERE {SCRAPER_RECORDS_FILE_ID_FK_COLUMN} = :fid"
        corr_id = str(uuid.uuid4())
        await enqueue_db_update(file_id=job_run_id, sql=reset_sql, params={"fid": file_id_int}, task_type=f"reset_step1_all_file_{file_id_int}", correlation_id=corr_id, logger_param=logger)
        msg = f"Task to reset Step1 status for FileID '{file_id}' enqueued."
        logger.info(f"[{job_run_id}] {msg} CorrelationID: {corr_id}")
        final_log_url = await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
        return {"status": "task_enqueued", "message": msg, "correlation_id": corr_id, "log_url": final_log_url}
    except ValueError: err_msg=f"Invalid FileID: {file_id}"; logger.error(f"[{job_run_id}] {err_msg}"); await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id); raise HTTPException(status_code=400, detail=err_msg)
    except Exception as e_main: logger.critical(f"[{job_run_id}] Critical error resetting Step1: {e_main}", exc_info=True); crit_log_url = await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id); raise HTTPException(status_code=500, detail=f"Internal server error. Log: {crit_log_url}")

@router.post("/reset-step1-for-no-results-entries/{file_id}", tags=["Database"])
async def api_reset_step1_for_no_result_entries(file_id: str, entry_ids_filter: Optional[List[int]] = Query(None)):
    job_run_id = f"reset_step1_no_res_{file_id}_{uuid.uuid4().hex[:6]}"
    logger, log_file_path = setup_job_logger(job_id=job_run_id, console_output=True)
    logger.info(f"[{job_run_id}] API Call: Reset Step1 for no-result entries in FileID: {file_id}. Filter: {entry_ids_filter or 'None'}")
    try:
        file_id_int = int(file_id) # FileID validation
        reset_sql = f"UPDATE r SET r.{SCRAPER_RECORDS_STEP1_COLUMN} = NULL FROM {SCRAPER_RECORDS_TABLE_NAME} r WHERE r.{SCRAPER_RECORDS_FILE_ID_FK_COLUMN} = :fid AND NOT EXISTS (SELECT 1 FROM {IMAGE_SCRAPER_RESULT_TABLE_NAME} res WHERE res.{IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN} = r.{SCRAPER_RECORDS_PK_COLUMN})"
        params_for_reset: Dict[str, Any] = {"fid": file_id_int}
        if entry_ids_filter: reset_sql += f" AND r.{SCRAPER_RECORDS_PK_COLUMN} IN :eids_filter_list"; params_for_reset["eids_filter_list"] = tuple(entry_ids_filter)
        corr_id = str(uuid.uuid4())
        await enqueue_db_update(file_id=job_run_id, sql=reset_sql, params=params_for_reset, task_type=f"reset_step1_no_res_file_{file_id_int}", correlation_id=corr_id, logger_param=logger)
        msg = f"Task to reset Step1 for no-result entries (FileID '{file_id}', Filter: {entry_ids_filter or 'All'}) enqueued."
        logger.info(f"[{job_run_id}] {msg} CorrelationID: {corr_id}")
        final_log_url = await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
        return {"status": "task_enqueued", "message": msg, "correlation_id": corr_id, "log_url": final_log_url}
    except ValueError: err_msg=f"Invalid FileID: {file_id}"; logger.error(f"[{job_run_id}] {err_msg}"); await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id); raise HTTPException(status_code=400, detail=err_msg)
    except Exception as e_main: logger.critical(f"[{job_run_id}] Critical error resetting Step1 for no-result entries: {e_main}", exc_info=True); crit_log_url = await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id); raise HTTPException(status_code=500, detail=f"Internal server error. Log: {crit_log_url}")

@router.post("/validate-result-images/{file_id}", tags=["Validation"])
async def api_validate_scraper_result_images(
    file_id: str, entry_ids_filter: Optional[List[int]] = Query(None),
    concurrency_limit: int = Query(10, ge=1, le=50)
):
    job_run_id = f"validate_images_{file_id}_{uuid.uuid4().hex[:6]}"
    logger, log_file_path = setup_job_logger(job_id=job_run_id, console_output=True)
    logger.info(f"[{job_run_id}] API Call: Validate images for FileID: {file_id}. Filter: {entry_ids_filter or 'None'}. Concurrency: {concurrency_limit}")
    try:
        file_id_int = int(file_id) # FileID validation
        fetch_results_sql = f"SELECT res.{IMAGE_SCRAPER_RESULT_PK_COLUMN}, res.{IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN}, res.{IMAGE_SCRAPER_RESULT_IMAGE_URL_COLUMN}, res.{IMAGE_SCRAPER_RESULT_IMAGE_URL_THUMBNAIL_COLUMN} FROM {IMAGE_SCRAPER_RESULT_TABLE_NAME} res JOIN {SCRAPER_RECORDS_TABLE_NAME} rec ON res.{IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN} = rec.{SCRAPER_RECORDS_PK_COLUMN} WHERE rec.{SCRAPER_RECORDS_FILE_ID_FK_COLUMN} = :fid AND res.{IMAGE_SCRAPER_RESULT_IMAGE_URL_COLUMN} IS NOT NULL AND (res.{IMAGE_SCRAPER_RESULT_SORT_ORDER_COLUMN} IS NULL OR res.{IMAGE_SCRAPER_RESULT_SORT_ORDER_COLUMN} >= 0)"
        params_fetch: Dict[str, Any] = {"fid": file_id_int}
        if entry_ids_filter: fetch_results_sql += f" AND res.{IMAGE_SCRAPER_RESULT_ENTRY_ID_FK_COLUMN} IN :eids_list"; params_fetch["eids_list"] = tuple(entry_ids_filter)
        images_to_validate_list: List[Dict] = []
        async with async_engine.connect() as conn:
            db_cursor = await conn.execute(text(fetch_results_sql), params_fetch)
            images_to_validate_list = [dict(row._mapping) for row in db_cursor.fetchall()]
        if not images_to_validate_list: msg = "No images found matching criteria for validation."; logger.info(f"[{job_run_id}] {msg}"); return {"status": "no_action_required", "message": msg}
        logger.info(f"[{job_run_id}] Found {len(images_to_validate_list)} images to validate.")
        validation_semaphore = asyncio.Semaphore(concurrency_limit)
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as http_client:
            async def _check_single_image(img_data: Dict) -> tuple[int, bool]:
                async with validation_semaphore:
                    res_id = img_data[IMAGE_SCRAPER_RESULT_PK_COLUMN]
                    urls_to_try = [img_data.get(IMAGE_SCRAPER_RESULT_IMAGE_URL_COLUMN), img_data.get(IMAGE_SCRAPER_RESULT_IMAGE_URL_THUMBNAIL_COLUMN)]
                    urls_to_try = [url for url in urls_to_try if url and isinstance(url, str) and url.startswith("http")]
                    for img_url_to_check in urls_to_try:
                        try:
                            response = await http_client.get(img_url_to_check)
                            if response.status_code == 200 and response.headers.get("content-type", "").lower().startswith("image/"):
                                logger.debug(f"[{job_run_id}] ResultID {res_id}: Valid image at {img_url_to_check}"); return res_id, True
                        except httpx.RequestError as e_req: logger.warning(f"[{job_run_id}] ResultID {res_id}: RequestError for {img_url_to_check}: {e_req}")
                        except Exception as e_gen: logger.warning(f"[{job_run_id}] ResultID {res_id}: Generic error for {img_url_to_check}: {e_gen}")
                    logger.warning(f"[{job_run_id}] ResultID {res_id}: All URLs failed validation."); return res_id, False
            validation_outcomes = await asyncio.gather(*[_check_single_image(img_item) for img_item in images_to_validate_list], return_exceptions=True)
        invalid_image_result_ids: List[int] = []; num_valid_checked, num_failed_checked = 0, 0
        for outcome in validation_outcomes:
            if isinstance(outcome, Exception): logger.error(f"[{job_run_id}] Exception during image validation task: {outcome}", exc_info=outcome)
            elif isinstance(outcome, tuple) and len(outcome) == 2:
                res_id_checked, is_valid_flag = outcome
                if is_valid_flag: num_valid_checked += 1
                else: num_failed_checked += 1; invalid_image_result_ids.append(res_id_checked)
        logger.info(f"[{job_run_id}] Validation check complete. Valid: {num_valid_checked}, Invalid: {num_failed_checked}.")
        if invalid_image_result_ids:
            logger.info(f"[{job_run_id}] Enqueuing SortOrder=-5 update for {len(invalid_image_result_ids)} invalid images.")
            update_invalid_sql = f"UPDATE {IMAGE_SCRAPER_RESULT_TABLE_NAME} SET {IMAGE_SCRAPER_RESULT_SORT_ORDER_COLUMN} = -5 WHERE {IMAGE_SCRAPER_RESULT_PK_COLUMN} IN :invalid_ids_list"
            corr_id_invalid = str(uuid.uuid4())
            await enqueue_db_update(file_id=job_run_id, sql=update_invalid_sql, params={"invalid_ids_list": invalid_image_result_ids}, task_type=f"mark_invalid_images_file_{file_id_int}", correlation_id=corr_id_invalid, logger_param=logger)
        final_msg = (f"Image validation for FileID '{file_id}' complete. Total checked: {len(images_to_validate_list)}. Valid: {num_valid_checked}. Invalid (marked SortOrder=-5): {num_failed_checked}.")
        logger.info(f"[{job_run_id}] {final_msg}")
        final_log_url = await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id)
        return {"status": "validation_complete", "message": final_msg, "counts": {"valid": num_valid_checked, "invalid": num_failed_checked}, "log_url": final_log_url}
    except ValueError: err_msg=f"Invalid FileID: {file_id}"; logger.error(f"[{job_run_id}] {err_msg}"); await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id); raise HTTPException(status_code=400, detail=err_msg)
    except Exception as e_main: logger.critical(f"[{job_run_id}] Critical error validating images: {e_main}", exc_info=True); crit_log_url = await upload_log_file(job_run_id, log_file_path, logger, db_record_file_id_to_update=file_id); raise HTTPException(status_code=500, detail=f"Internal server error. Log: {crit_log_url}")

app.include_router(router, prefix="/api/v6")