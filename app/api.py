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
from common import fetch_brand_rules, clean_string, generate_aliases
from database_config import conn_str, async_engine
from config import BRAND_RULES_URL, VERSION, SEARCH_PROXY_API_URL
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from email_utils import send_message_email
from app.legacy_utils import create_temp_dirs, cleanup_temp_dirs, process_and_tag_results
from urllib.parse import urlparse
from url_extract import extract_thumbnail_url
from openpyxl import Workbook
from openpyxl.styles import PatternFill
from openpyxl.utils import get_column_letter

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
    def __init__(self, endpoint: str, logger: logging.Logger, max_concurrency: int = 2):
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
                            if not results:
                                self.logger.warning(f"Worker PID {process.pid}: No results for term '{term}' in region {region}")
                                continue
                            results_html_bytes = results if isinstance(results, bytes) else results.encode("utf-8")
                            formatted_results = process_search_result(results_html_bytes, results_html_bytes, entry_id, self.logger)
                            if formatted_results:
                                self.logger.info(f"Worker PID {process.pid}: Found {len(formatted_results)} results for term '{term}' in region {region}")
                                return [
                                    {
                                        "EntryID": entry_id,  # Use provided entry_id
                                        "ImageUrl": res.get("image_url", "placeholder://no-image"),
                                        "ImageDesc": res.get("description", ""),
                                        "ImageSource": res.get("source", "N/A"),
                                        "ImageUrlThumbnail": res.get("thumbnail_url", res.get("image_url", "placeholder://no-thumbnail"))
                                    }
                                    for res in formatted_results
                                ]
                            self.logger.warning(f"Worker PID {process.pid}: Empty results for term '{term}' in region {region}")
                except (aiohttp.ClientError, json.JSONDecodeError) as e:
                    self.logger.warning(f"Worker PID {process.pid}: Failed for term '{term}' in region {region}: {e}")
                    continue
            self.logger.error(f"Worker PID {process.pid}: All regions failed for term '{term}'")
            return []
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
    search_terms_dict = generate_search_variations(search_string, brand, logger=logger)
    
    search_terms = []
    variation_types = [
        "default", "delimiter_variations", "color_variations", "brand_alias",
        "no_color", "model_alias", "category_specific"
    ]
    
    for variation_type in variation_types:
        if variation_type in search_terms_dict:
            search_terms.extend(search_terms_dict[variation_type])
            if not use_all_variations:
                break
    
    search_terms = list(set(search_terms))
    logger.info(f"Generated {len(search_terms)} search terms for EntryID {entry_id}")

    if not search_terms:
        logger.warning(f"No search terms for EntryID {entry_id}")
        return []

    client = SearchClient(endpoint, logger)
    try:
        tasks = [client.search(term, brand, entry_id) for term in search_terms]
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

        all_results = []
        for term, term_results in zip(search_terms, raw_results):
            if isinstance(term_results, Exception):
                logger.error(f"Error for term '{term}' in EntryID {entry_id}: {term_results}")
                continue
            if not term_results:
                continue
            results = await process_results(term_results, entry_id, brand, term, logger)
            all_results.extend(results)

        logger.info(f"Processed {len(all_results)} results for EntryID {entry_id}")
        return all_results
    finally:
        await client.close()

def generate_brand_aliases(brand: str) -> List[str]:
    """
    Generate a list of brand aliases for the given brand.
    Returns a list of alternate brand names or spellings.
    """
    brand = brand.lower().strip()
    aliases = [brand]
    
    brand_alias_map = {
        "nike": ["nike inc.", "nke", "nikes"],
        "adidas": ["adidas ag", "addidas", "adi"],
        "gucci": ["gucci group", "guchi"],
        "louis vuitton": ["lv", "louis v", "vuitton"],
    }
    
    if brand in brand_alias_map:
        aliases.extend(brand_alias_map[brand])
    
    aliases.append(brand.replace(" ", ""))
    if len(brand.split()) > 1:
        aliases.append("".join(word[0] for word in brand.split()))
    
    return list(set(aliases))

def generate_search_variations(
    search_string: str,
    brand: Optional[str] = None,
    model: Optional[str] = None,
    color: Optional[str] = None,
    category: Optional[str] = None,
    brand_rules: Optional[Dict] = None,
    logger: Optional[logging.Logger] = None
) -> Dict[str, List[str]]:
    logger = logger or default_logger
    variations = {
        "default": [],
        "delimiter_variations": [],
        "color_variations": [],
        "brand_alias": [],
        "no_color": [],
        "model_alias": [],
        "category_specific": []
    }

    if not search_string or not isinstance(search_string, str):
        logger.warning("Empty or invalid search string provided")
        return variations
    
    search_string = clean_string(search_string).lower()
    brand = clean_string(brand).lower() if brand else None
    model = clean_string(model).lower() if model else search_string
    color = clean_string(color).lower() if color else None
    category = clean_string(category).lower() if category else None

    variations["default"].append(search_string)
    logger.debug(f"Added default variation: '{search_string}'")

    delimiters = [' ', '-', '_', '/']
    delimiter_variations = []
    for delim in delimiters:
        if delim in search_string:
            for new_delim in delimiters:
                variation = search_string.replace(delim, new_delim)
                if variation != search_string:
                    delimiter_variations.append(variation)
    variations["delimiter_variations"] = list(set(delimiter_variations))
    logger.debug(f"Generated {len(delimiter_variations)} delimiter variations")

    if color:
        color_variations = [
            f"{search_string} {color}",
            f"{brand} {model} {color}" if brand and model else search_string,
            f"{model} {color}" if model else search_string
        ]
        variations["color_variations"] = list(set(color_variations))
        logger.debug(f"Generated {len(color_variations)} color variations")

    if brand:
        brand_aliases = generate_brand_aliases(brand) or generate_aliases(brand)
        brand_alias_variations = [f"{alias} {model}" for alias in brand_aliases if model]
        variations["brand_alias"] = list(set(brand_alias_variations))
        logger.debug(f"Generated {len(brand_alias_variations)} brand alias variations")

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
                    logger.debug(f"No color separator for brand {brand}, using full search string")
                    break

                if color_separator in search_string:
                    parts = search_string.split(color_separator)
                    base_part = parts[0]
                    if len(base_part) >= base_length and len(search_string) <= with_color_length:
                        no_color_string = base_part
                        logger.debug(f"Extracted no-color string: '{no_color_string}' using separator '{color_separator}'")
                        break
                elif len(search_string) <= base_length:
                    no_color_string = search_string
                    logger.debug(f"No color suffix detected, using: '{no_color_string}'")
                    break

    if no_color_string == search_string:
        for delim in ['_', '-', ' ']:
            if delim in search_string:
                no_color_string = search_string.rsplit(delim, 1)[0]
                logger.debug(f"Fallback no-color string: '{no_color_string}' using delimiter '{delim}'")
                break

    variations["no_color"].append(no_color_string)
    logger.debug(f"Added no-color variation: '{no_color_string}'")

    if model:
        model_aliases = generate_aliases(model)
        model_alias_variations = [f"{brand} {alias}" if brand else alias for alias in model_aliases]
        variations["model_alias"] = list(set(model_alias_variations))
        logger.debug(f"Generated {len(model_alias_variations)} model alias variations")

    if category and "apparel" in category.lower():
        apparel_terms = ["sneaker", "shoe", "hoodie", "shirt", "jacket", "pants", "clothing"]
        category_variations = [f"{search_string} {term}" for term in apparel_terms]
        if brand and model:
            category_variations.extend([f"{brand} {model} {term}" for term in apparel_terms])
        variations["category_specific"] = list(set(category_variations))
        logger.debug(f"Generated {len(category_variations)} category-specific variations")

    for key in variations:
        variations[key] = list(set(variations[key]))
    
    logger.info(f"Generated total of {sum(len(v) for v in variations.values())} unique variations for search string '{search_string}'")
    return variations

async def generate_download_file(file_id: int, background_tasks: BackgroundTasks, logger: Optional[logging.Logger] = None) -> Dict[str, str]:
    log_filename = f"job_logs/job_{file_id}.log"
    try:
        if logger is None:
            logger, log_filename = setup_job_logger(job_id=str(file_id), log_dir="job_logs", console_output=True)
        logger.setLevel(logging.DEBUG)
        process = psutil.Process()
        logger.debug(f"Logger initialized for generate_download_file")

        def log_memory_usage():
            mem_info = process.memory_info()
            logger.info(f"Memory: RSS={mem_info.rss / 1024**2:.2f} MB")
            if mem_info.rss / 1024**2 > 1000:
                logger.warning(f"High memory usage")
        logger.info(f"Generating download file for FileID: {file_id}")
        log_memory_usage()

        results_df = await get_images_excel_db(str(file_id), logger)
        if results_df.empty:
            logger.error(f"No data found for FileID {file_id}")
            background_tasks.add_task(monitor_and_resubmit_failed_jobs, str(file_id), logger)
            return {"error": f"No data found for FileID {file_id}", "log_filename": log_filename}

        temp_dir = f"temp_excel_{file_id}"
        os.makedirs(temp_dir, exist_ok=True)
        excel_filename = os.path.join(temp_dir, f"image_results_{file_id}.xlsx")

        wb = Workbook()
        ws = wb.active
        ws.title = "Image Results"

        headers = [
            "EntryID", "ProductBrand", "ProductModel", "ProductColor", "ProductCategory",
            "ImageUrl", "ImageDesc", "ImageSource", "ImageUrlThumbnail", "SortOrder"
        ]
        for col, header in enumerate(headers, 1):
            ws[f"{get_column_letter(col)}1"] = header
            ws[f"{get_column_letter(col)}1"].fill = PatternFill(start_color="D3D3D3", end_color="D3D3D3", fill_type="solid")

        for row_idx, row in results_df.iterrows():
            for col_idx, header in enumerate(headers):
                ws[f"{get_column_letter(col_idx + 1)}{row_idx + 2}"] = row.get(header, "")

        for col in ws.columns:
            max_length = 0
            column = col[0].column_letter
            for cell in col:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            ws.column_dimensions[column].width = adjusted_width

        wb.save(excel_filename)
        logger.info(f"Excel file generated: {excel_filename}")

        logger.debug(f"Checking if Excel file exists: {excel_filename}")
        if not os.path.exists(excel_filename):
            logger.error(f"Excel file {excel_filename} does not exist for upload for FileID {file_id}")
            return {"error": f"Excel file {excel_filename} not found", "log_filename": log_filename}
        logger.debug(f"Uploading to R2: file_src={excel_filename}, save_as=excel_results/image_results_{file_id}.xlsx, is_public=True")
        try:
            public_url = await upload_file_to_space(
                file_src=excel_filename,
                save_as=f"excel_results/image_results_{file_id}.xlsx",
                is_public=True,
                logger=logger,
                file_id=str(file_id)
            )
            logger.debug(f"Upload result: public_url={public_url}")
            if not public_url:
                logger.error(f"Failed to upload Excel file for FileID {file_id}")
                return {"error": "Failed to upload Excel file", "log_filename": log_filename}
        except Exception as upload_error:
            logger.error(f"Upload error for FileID {file_id}: {upload_error}", exc_info=True)
            return {"error": f"Upload failed: {str(upload_error)}", "log_filename": log_filename}

        await update_file_location_complete(str(file_id), public_url, logger)
        await update_file_generate_complete(str(file_id), logger)

        try:
            os.remove(excel_filename)
            os.rmdir(temp_dir)
            logger.debug(f"Cleaned up temporary directory: {temp_dir}")
        except Exception as e:
            logger.warning(f"Failed to clean up temporary directory {temp_dir}: {e}")

        logger.info(f"Download file generated and uploaded for FileID: {file_id}: {public_url}")
        return {
            "message": "Download file generated successfully",
            "file_id": str(file_id),
            "public_url": public_url,
            "log_filename": log_filename
        }
    except Exception as e:
        logger.error(f"Error generating download file for FileID {file_id}: {e}", exc_info=True)
        background_tasks.add_task(monitor_and_resubmit_failed_jobs, str(file_id), logger)
        return {"error": str(e), "log_filename": log_filename}
    finally:
        log_memory_usage()

 


async def process_restart_batch(
    file_id_db: int,
    entry_id: Optional[int] = None,
    use_all_variations: bool = False,
    logger: Optional[logging.Logger] = None
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
            logger.info(f"Memory: RSS={mem_info.rss / 1024**2:.2f} MB")
            if mem_info.rss / 1024**2 > 1000:
                logger.warning(f"High memory usage")

        logger.info(f"Starting processing for FileID: {file_id_db}, Use all variations: {use_all_variations}")
        log_memory_usage()

        file_id_db_int = file_id_db
        BATCH_SIZE = 1
        MAX_CONCURRENCY = 4
        MAX_ENTRY_RETRIES = 3

        async with async_engine.connect() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) FROM utb_ImageScraperFiles WHERE ID = :file_id"),
                {"file_id": file_id_db_int}
            )
            if result.fetchone()[0] == 0:
                logger.error(f"FileID {file_id_db} does not exist")
                return {"error": f"FileID {file_id_db} does not exist", "log_filename": log_filename, "log_public_url": "", "last_entry_id": str(entry_id or "")}
            result.close()

        if entry_id is None:
            entry_id = await fetch_last_valid_entry(str(file_id_db_int), logger)
            if entry_id is not None:
                async with async_engine.connect() as conn:
                    result = await conn.execute(
                        text("SELECT MIN(EntryID) FROM utb_ImageScraperRecords WHERE FileID = :file_id AND EntryID > :entry_id"),
                        {"file_id": file_id_db_int, "entry_id": entry_id}
                    )
                    next_entry = result.fetchone()
                    entry_id = next_entry[0] if next_entry and next_entry[0] else None
                    logger.info(f"Resuming from EntryID: {entry_id}")
                    result.close()

        brand_rules = await fetch_brand_rules(BRAND_RULES_URL, max_attempts=3, timeout=10, logger=logger)
        if not brand_rules:
            logger.warning(f"No brand rules fetched")
            return {"message": "Failed to fetch brand rules", "file_id": str(file_id_db), "log_filename": log_filename, "log_public_url": "", "last_entry_id": str(entry_id or "")}

        # Use the primary endpoint directly
        endpoint = SEARCH_PROXY_API_URL
        logger.info(f"Using endpoint: {endpoint} with API key authentication")

        async with async_engine.connect() as conn:
            query = text("""
                SELECT EntryID, ProductModel, ProductBrand, ProductColor, ProductCategory 
                FROM utb_ImageScraperRecords 
                WHERE FileID = :file_id AND (:entry_id IS NULL OR EntryID >= :entry_id) 
                ORDER BY EntryID
            """)
            result = await conn.execute(query, {"file_id": file_id_db_int, "entry_id": entry_id})
            entries = [(row[0], row[1], row[2], row[3], row[4]) for row in result.fetchall() if row[1] is not None]
            logger.info(f"Found {len(entries)} entries")
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

        semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
        async def process_entry(entry):
            entry_id, search_string, brand, color, category = entry
            async with semaphore:
                for attempt in range(1, MAX_ENTRY_RETRIES + 1):
                    try:
                        logger.info(f"Processing EntryID {entry_id}, Attempt {attempt}/{MAX_ENTRY_RETRIES}, Use all variations: {use_all_variations}")
                        results = await async_process_entry_search(
                            search_string=search_string,
                            brand=brand,
                            endpoint=endpoint,
                            entry_id=entry_id,
                            use_all_variations=use_all_variations,
                            file_id_db=file_id_db,
                            logger=logger
                        )
                        if not results:
                            logger.warning(f"No results for EntryID {entry_id} on attempt {attempt}")
                            continue

                        if not all(all(col in res for col in required_columns) for res in results):
                            logger.error(f"Missing columns for EntryID {entry_id} on attempt {attempt}")
                            continue

                        deduplicated_results = []
                        seen = set()
                        for res in results:
                            key = (res['EntryID'], res['ImageUrl'])
                            if key not in seen:
                                seen.add(key)
                                deduplicated_results.append(res)
                        logger.info(f"Deduplicated to {len(deduplicated_results)} rows for EntryID {entry_id}")

                        insert_success = await insert_search_results(deduplicated_results, logger=logger, file_id=str(file_id_db))
                        if not insert_success:
                            logger.error(f"Failed to insert results for EntryID {entry_id} on attempt {attempt}")
                            continue

                        update_result = await update_search_sort_order(
                            str(file_id_db), str(entry_id), brand, search_string, color, category, logger, brand_rules=brand_rules
                        )
                        if update_result is None or not update_result:
                            logger.error(f"SortOrder update failed for EntryID {entry_id} on attempt {attempt}")
                            continue

                        return entry_id, True
                    except Exception as e:
                        logger.error(f"Error processing EntryID {entry_id} on attempt {attempt}: {e}", exc_info=True)
                        if attempt < MAX_ENTRY_RETRIES:
                            await asyncio.sleep(2 ** attempt)
                        continue
                logger.error(f"Failed to process EntryID {entry_id} after {MAX_ENTRY_RETRIES} attempts")
                return entry_id, False

        for batch_idx, batch_entries in enumerate(entry_batches, 1):
            logger.info(f"Processing batch {batch_idx}/{len(entry_batches)}")
            start_time = datetime.datetime.now()

            results = await asyncio.gather(
                *(process_entry(entry) for entry in batch_entries),
                return_exceptions=True
            )

            for entry, result in zip(batch_entries, results):
                entry_id = entry[0]
                if isinstance(result, Exception):
                    logger.error(f"Error processing EntryID {entry_id}: {result}", exc_info=True)
                    failed_entries += 1
                    continue
                entry_id_result, success = result
                if success:
                    successful_entries += 1
                    last_entry_id_processed = entry_id
                else:
                    failed_entries += 1

            elapsed_time = (datetime.datetime.now() - start_time).total_seconds()
            logger.info(f"Completed batch {batch_idx} in {elapsed_time:.2f}s")
            log_memory_usage()

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
            if null_entries > 0:
                logger.warning(f"Found {null_entries} entries with NULL SortOrder")
            result.close()

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

        log_public_url = await upload_file_to_space(
            file_src=log_filename,
            save_as=f"job_logs/job_{file_id_db}.log",
            is_public=True,
            logger=logger,
            file_id=str(file_id_db)
        )
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
    except Exception as e:
        logger.error(f"Error processing FileID {file_id_db}: {e}", exc_info=True)
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




async def monitor_and_resubmit_failed_jobs(file_id: str, logger: logging.Logger):
    log_file = f"job_logs/job_{file_id}.log"
    max_attempts = 3
    attempt = 1

    while attempt <= max_attempts:
        if os.path.exists(log_file):
            with open(log_file, 'r') as f:
                log_content = f.read()
                if any(err in log_content for err in ["WORKER TIMEOUT", "SIGKILL", "placeholder://error", "No data found"]):
                    logger.warning(f"Detected failure in job for FileID: {file_id}, attempt {attempt}/{max_attempts}")
                    last_entry_id = await fetch_last_valid_entry(file_id, logger)
                    logger.info(f"Resubmitting job for FileID: {file_id} starting from EntryID: {last_entry_id or 'beginning'} with all variations")
                    
                    result = await process_restart_batch(
                        file_id_db=int(file_id),
                        logger=logger,
                        entry_id=last_entry_id,
                        use_all_variations=True
                    )
                    
                    if "error" not in result:
                        logger.info(f"Resubmission successful for FileID: {file_id}")
                        await run_generate_download_file(file_id, logger, log_file, BackgroundTasks())
                        await send_message_email(
                            to_emails=["nik@luxurymarket.com"],
                            subject=f"Success: Batch Resubmission for FileID {file_id}",
                            message=f"Resubmission succeeded for FileID {file_id} starting from EntryID {last_entry_id or 'beginning'} with all variations.\nLog: {log_file}",
                            logger=logger
                        )
                        return
                    else:
                        logger.error(f"Resubmission failed for FileID: {file_id}: {result['error']}")
                    attempt += 1
                    await asyncio.sleep(2 ** attempt)
                else:
                    logger.info(f"No failure detected in logs for FileID: {file_id}")
                    return
        else:
            logger.warning(f"Log file {log_file} does not exist for FileID: {file_id}")
            return
        await asyncio.sleep(60)

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
            result = await job_func(file_id, **kwargs)
        else:
            result = job_func(file_id, **kwargs)
        
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

async def run_generate_download_file(file_id: str, logger: logging.Logger, log_filename: str, background_tasks: BackgroundTasks):
    try:
        JOB_STATUS[file_id] = {
            "status": "running",
            "message": "Job is running",
            "timestamp": datetime.datetime.now().isoformat()
        }
        
        result = await generate_download_file(int(file_id), background_tasks, logger=logger)
        
        if "error" in result:
            JOB_STATUS[file_id] = {
                "status": "failed",
                "message": f"Error: {result['error']}",
                "log_url": result.get("log_filename") if os.path.exists(result.get("log_filename", "")) else None,
                "timestamp": datetime.datetime.now().isoformat()
            }
            logger.error(f"Job failed for FileID {file_id}: {result['error']}")
            background_tasks.add_task(monitor_and_resubmit_failed_jobs, file_id, logger)
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
        background_tasks.add_task(monitor_and_resubmit_failed_jobs, file_id, logger)

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

@router.post("/generate-download-file/{file_id}", tags=["Export"], response_model=JobStatusResponse)
async def api_generate_download_file(file_id: str, background_tasks: BackgroundTasks):
    logger, log_filename = setup_job_logger(job_id=file_id, console_output=True)
    logger.info(f"Received request to generate download file for FileID: {file_id}")
    
    try:
        async with async_engine.connect() as conn:
            result = await conn.execute(
                text("SELECT FileName FROM utb_ImageScraperFiles WHERE ID = :file_id"),
                {"file_id": int(file_id)}
            )
            if not result.fetchone():
                logger.error(f"Invalid FileID: {file_id}")
                raise HTTPException(status_code=404, detail=f"FileID {file_id} not found")
        
        JOB_STATUS[file_id] = {
            "status": "queued",
            "message": "Job queued for processing",
            "timestamp": datetime.datetime.now().isoformat()
        }
        
        background_tasks.add_task(run_generate_download_file, file_id, logger, log_filename, background_tasks)
        
        send_to_email = await get_send_to_email(int(file_id), logger=logger)
        if send_to_email:
            await send_message_email(
                to_emails=send_to_email,
                subject=f"Job Queued for FileID: {file_id}",
                message=f"Excel file generation for FileID {file_id} has been queued.",
                logger=logger
            )
        
        return JobStatusResponse(
            status="queued",
            message=f"Download file generation queued for FileID: {file_id}",
            timestamp=datetime.datetime.now().isoformat()
        )
    except SQLAlchemyError as e:
        logger.error(f"Database error for FileID {file_id}: {e}", exc_info=True)
        log_public_url = await upload_log_file(file_id, log_filename, logger)
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Error queuing download file for FileID {file_id}: {e}", exc_info=True)
        log_public_url = await upload_log_file(file_id, log_filename, logger)
        raise HTTPException(status_code=500, detail=f"Error queuing job: {str(e)}")

@router.get("/job-status/{file_id}", tags=["Export"], response_model=JobStatusResponse)
async def api_get_job_status(file_id: str):
    logger, log_filename = setup_job_logger(job_id=file_id, console_output=True)
    logger.info(f"Checking job status for FileID: {file_id}")
    
    job_status = JOB_STATUS.get(file_id)
    if not job_status:
        logger.warning(f"No job found for FileID: {file_id}")
        log_public_url = await upload_log_file(file_id, log_filename, logger)
        raise HTTPException(status_code=404, detail=f"No job found for FileID {file_id}")
    
    return JobStatusResponse(
        status=job_status["status"],
        message=job_status["message"],
        public_url=job_status.get("public_url"),
        log_url=job_status.get("log_url"),
        timestamp=job_status["timestamp"]
    )

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
        
        if background_tasks:
            background_tasks.add_task(monitor_and_resubmit_failed_jobs, file_id, logger)
        
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
        
        if background_tasks:
            background_tasks.add_task(monitor_and_resubmit_failed_jobs, file_id, logger)
        
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
            concurrency=concurrency,
            logger=logger
        )
        
        if result["status_code"] != 200:
            logger.error(f"Failed to process AI images for FileID {file_id}: {result['message']}")
            log_public_url = await upload_log_file(file_id, log_filename, logger)
            raise HTTPException(status_code=result["status_code"], detail=result["message"])
        
        if background_tasks:
            background_tasks.add_task(monitor_and_resubmit_failed_jobs, file_id, logger)
        
        logger.info(f"Completed AI image processing for FileID: {file_id}")
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

app.include_router(router, prefix="/api/v3")