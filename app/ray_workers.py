# ray_workers.py
import asyncio
import logging
import ray
import pyodbc
import requests
import json
import re
import pandas as pd
from typing import Optional, List, Dict
from config import conn_str
from database import (
    insert_search_results,
    update_sort_order,
    default_logger,
    process_search_row,
    get_endpoint,
    set_sort_order_negative_four_for_zero_match
)
from image_process import batch_process_images

BRAND_RULES_URL = "https://raw.githubusercontent.com/iconluxurygroup/legacy-icon-product-api/refs/heads/main/task_settings/brand_settings.json"

async def fetch_brand_rules(url: str, max_attempts: int = 3, timeout: int = 10, logger: Optional[logging.Logger] = None) -> Optional[dict]:
    logger = logger or default_logger
    for attempt in range(max_attempts):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            brand_rules = response.json()
            if "brand_rules" in brand_rules:
                logger.info(f"Successfully fetched brand rules from {url}")
                return brand_rules
            else:
                logger.error(f"Invalid brand rules format from {url}")
                return None
        except (requests.RequestException, ValueError) as e:
            logger.warning(f"Attempt {attempt + 1} failed to fetch brand rules from {url}: {e}")
            if attempt < max_attempts - 1:
                await asyncio.sleep(2)
            else:
                logger.error(f"Failed to fetch brand rules after {max_attempts} attempts")
                return None

@ray.remote
def search_variation(variation: str, endpoint: str, entry_id: int, search_type: str, logger: Optional[logging.Logger] = None) -> Dict:
    logger = logger or logging.getLogger(__name__)
    try:
        result = process_search_row(variation, endpoint, entry_id, logger=logger, search_type=search_type,max_retries=15)
        if isinstance(result, pd.DataFrame) and not result.empty:
            return {"variation": variation, "result": result, "status": "success", "result_count": len(result)}
        return {"variation": variation, "result": None, "status": "failed", "result_count": 0}
    except Exception as e:
        logger.error(f"Error searching variation '{variation}' for EntryID {entry_id}: {e}")
        return {"variation": variation, "result": None, "status": "failed", "result_count": 0, "error": str(e)}

@ray.remote
def process_db_row(entry_id: int, search_string: str, search_types: List[str], endpoint: str, brand_rules: Dict, logger: Optional[logging.Logger] = None) -> Dict:
    logger = logger or logging.getLogger(__name__)
    try:
        if not search_string or not isinstance(search_string, str) or search_string.strip() == "":
            logger.warning(f"Skipping EntryID {entry_id}: Empty or invalid search string '{search_string}'")
            return {"entry_id": entry_id, "status": "skipped", "error": "Empty or invalid search string", "result": None}

        product_brand = None
        try:
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT ProductBrand FROM utb_ImageScraperRecords WHERE EntryID = ?", (entry_id,))
                result = cursor.fetchone()
                product_brand = result[0].strip().lower() if result and result[0] else None
        except Exception as e:
            logger.warning(f"Could not fetch ProductBrand for EntryID {entry_id}: {e}")

        brand_rule = None
        if product_brand and brand_rules.get("brand_rules"):
            for rule in brand_rules["brand_rules"]:
                if rule["is_active"] and any(name.lower() == product_brand for name in rule["names"]):
                    brand_rule = rule
                    break
            if not brand_rule:
                logger.info(f"No active brand rule found for ProductBrand '{product_brand}' for EntryID {entry_id}")

        search_string = search_string.lower()
        all_variations = {}
        for search_type in search_types:
            variations = []
            if search_type == "default":
                variations = [search_string]
            elif search_type == "brand_name" and product_brand:
                brand_name = brand_rule["full_name"] if brand_rule else product_brand
                variations = [f"{search_string} {brand_name}".strip()]
            elif search_type == "brand_format" and product_brand and brand_rule:
                sku_format = brand_rule["sku_format"]
                expected_lengths = brand_rule["expected_length"]
                base_len = expected_lengths["base"][0]
                with_color_len = expected_lengths.get("with_color", [base_len])[0]
                article_len = int(sku_format["base"]["article"][0])
                model_len = int(sku_format["base"]["model"][0])
                color_len = int(sku_format["color_extension"][0]) if sku_format["color_extension"] else 0

                cleaned_search = re.sub(r'[\s\-_]+', '', search_string)
                search_len = len(cleaned_search)
                if search_len in [base_len, with_color_len]:
                    variations.append(search_string)
                if color_len > 0 and search_len == with_color_len:
                    article_part = cleaned_search[:article_len]
                    model_part = cleaned_search[article_len:article_len + model_len]
                    color_part = cleaned_search[-color_len:]
                    style_part = article_part + model_part
                    for delim in [' ', '-', '_']:
                        variations.append(f"{style_part}{delim}{color_part}")
            elif search_type == "retry_with_alternative":
                delimiters = r'[\s\-_ ]+'
                parts = re.split(delimiters, search_string)
                variations = [' '.join(parts[:i]) for i in range(len(parts), 0, -1)]
                if product_brand:
                    brand_name = brand_rule["full_name"] if brand_rule else product_brand
                    variations.append(f"{search_string}".strip())
            
            if variations:
                all_variations[search_type] = variations
                logger.info(f"Generated variations for EntryID {entry_id}, SearchType {search_type}: {variations}")

        if not all_variations:
            logger.info(f"No variations generated for EntryID {entry_id}")
            return {"entry_id": entry_id, "status": "skipped", "error": "No valid variations", "result": None}

        futures = []
        for search_type, variations in all_variations.items():
            for variation in variations:
                futures.append(search_variation.remote(variation, endpoint, entry_id, search_type, logger=logger))
        
        results = ray.get(futures)
        for res in results:
            logger.info(f"Variation '{res['variation']}' for EntryID {entry_id} returned {res['result_count']} results")
        successful_results = [res for res in results if res["status"] == "success"]
        
        if successful_results:
            combined_df = pd.concat([res["result"] for res in successful_results]).drop_duplicates()
            return {
                "entry_id": entry_id,
                "status": "success",
                "result_count": len(combined_df),
                "result": combined_df,
                "used_variation": "combined"
            }
        
        logger.warning(f"All variations failed for EntryID {entry_id}")
        return {"entry_id": entry_id, "status": "failed", "result_count": 0, "result": None, "error": "All search variations failed"}

    except Exception as e:
        logger.error(f"Error processing EntryID {entry_id}: {e}", exc_info=True)
        return {"entry_id": entry_id, "status": "failed", "error": str(e), "result": None}

@ray.remote
def process_batch(batch: List[Dict], brand_rules: Dict, logger: Optional[logging.Logger] = None) -> List[Dict]:
    logger = logger or logging.getLogger(__name__)
    try:
        if not batch:
            logger.warning("Empty batch received")
            return []

        endpoint = get_endpoint(logger=logger)
        if not endpoint:
            logger.error("No healthy endpoint found")
            return [{"entry_id": row['EntryID'], "status": "failed", "error": "No endpoint", "result": None} for row in batch]

        logger.info(f"⚙️ Processing batch of {len(batch)} search tasks with endpoint {endpoint}")
        search_types = ["default", "brand_name", "brand_format","retry_with_alternative"]
        futures = [process_db_row.remote(row['EntryID'], row['SearchString'], search_types, endpoint, brand_rules, logger=logger) 
                  for row in batch if row.get('SearchString')]
        results = ray.get(futures)

        success_count = sum(1 for r in results if r['status'] == 'success')
        logger.info(f"Batch completed: {success_count}/{len(results)} successful")
        return results
    except Exception as e:
        logger.error(f"🔴 Error processing batch: {e}", exc_info=True)
        return [{"entry_id": row['EntryID'], "status": "failed", "error": str(e), "result": None} for row in batch]

async def process_file_with_retries(file_id, max_retries: int = None, logger: Optional[logging.Logger] = None) -> None:
    logger = logger or default_logger
    try:
        brand_rules = await fetch_brand_rules(BRAND_RULES_URL, logger=logger)
        if not brand_rules:
            logger.warning("Failed to load brand rules; skipping brand-specific search types")
            brand_rules = {"brand_rules": []}

        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?", (file_id,))
            all_entry_ids = [row[0] for row in cursor.fetchall()]

        entries_to_process = set(all_entry_ids)
        search_types = ["default", "brand_name", "brand_format", "retry_with_alternative"]
        max_retries = max_retries if max_retries is not None else len(search_types)

        for retry in range(min(max_retries, len(search_types))):
            if not entries_to_process:
                logger.info("All entries have valid results")
                break
            search_type = search_types[retry]
            logger.info(f"Retry {retry+1}/{max_retries}: Processing {len(entries_to_process)} entries with search_type '{search_type}'")

            batch = []
            for entry_id in entries_to_process:
                cursor.execute("SELECT ProductModel FROM utb_ImageScraperRecords WHERE EntryID = ?", (entry_id,))
                search_string = cursor.fetchone()[0]
                batch.append({"EntryID": entry_id, "SearchString": search_string, "SearchType": search_type})

            results_ref = process_batch.remote(batch, brand_rules, logger=logger)
            results = ray.get(results_ref)

            for res in results:
                if res["status"] == "success":
                    insert_search_results(res["result"], logger=logger)

            await batch_process_images(file_id, logger=logger)
            update_sort_order(file_id, logger=logger)
            set_sort_order_negative_four_for_zero_match(file_id, logger=logger)

            cursor.execute("""
                SELECT EntryID
                FROM utb_ImageScraperRecords
                WHERE FileID = ?
                AND EntryID NOT IN (
                    SELECT EntryID
                    FROM utb_ImageScraperResult
                    WHERE EntryID IN (SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?)
                    AND SortOrder >= 0
                )
            """, (file_id, file_id))
            remaining_entries = [row[0] for row in cursor.fetchall()]
            entries_to_process = set(remaining_entries)
            logger.info(f"After retry {retry+1}, {len(entries_to_process)} entries still need processing")

        if entries_to_process:
            logger.warning(f"After {max_retries} retries, {len(entries_to_process)} entries still have no valid results")
        else:
            logger.info("All entries have valid results")
    except Exception as e:
        logger.error(f"Error in process_file_with_retries: {e}", exc_info=True)