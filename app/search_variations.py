import logging
import re
from typing import Dict, List, Optional
from common import clean_string, generate_aliases, generate_brand_aliases
from image_utils import process_restart_batch
from email_utils import send_message_email
from database_config import async_engine
import os
import asyncio
from sqlalchemy.sql import text
import traceback
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

import logging
from typing import Dict, List, Optional
from common import clean_string, generate_aliases, generate_brand_aliases

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

def generate_search_variations(
    search_string: str,
    brand: Optional[str] = None,
    model: Optional[str] = None,
    color: Optional[str] = None,
    category: Optional[str] = None,
    brand_rules: Optional[Dict] = None,
    logger: Optional[logging.Logger] = None
) -> Dict[str, List[str]]:
    """
    Generate all possible search variations for a given search string, brand, model, color, and category.
    Returns a dictionary with categorized variations for search optimization.
    """
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

async def monitor_and_resubmit_failed_jobs(file_id: str, logger: logging.Logger):
    """
    Monitor job logs for failures and resubmit failed jobs with all search variations.
    """
    log_file = f"job_logs/job_{file_id}.log"
    max_attempts = 3
    attempt = 1

    while attempt <= max_attempts:
        if os.path.exists(log_file):
            with open(log_file, 'r') as f:
                log_content = f.read()
                if any(err in log_content for err in ["WORKER TIMEOUT", "SIGKILL", "placeholder://error"]):
                    logger.warning(f"Detected failure in job for FileID: {file_id}, attempt {attempt}/{max_attempts}")
                    last_entry_id = await fetch_last_valid_entry(file_id, logger)
                    logger.info(f"Resubmitting job for FileID: {file_id} starting from EntryID: {last_entry_id or 'beginning'}")
                    
                    result = await process_restart_batch(
                        file_id_db=int(file_id),
                        logger=logger,
                        entry_id=last_entry_id,
                        use_all_variations=True  # Use all variations for retries
                    )
                    
                    if "error" not in result:
                        logger.info(f"Resubmission successful for FileID: {file_id}")
                        await send_message_email(
                            to_emails=["nik@luxurymarket.com"],
                            subject=f"Success: Batch Resubmission for FileID {file_id}",
                            message=f"Resubmission succeeded for FileID {file_id} starting from EntryID {last_entry_id or 'beginning'}.\nLog: {log_file}",
                            logger=logger
                        )
                        return
                    else:
                        logger.error(f"Resubmission failed for FileID: {file_id}: {result['error']}")
                    attempt += 1
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.info(f"No failure detected in logs for FileID: {file_id}")
                    return
        else:
            logger.warning(f"Log file {log_file} does not exist for FileID: {file_id}")
            return
        await asyncio.sleep(60)

async def fetch_last_valid_entry(file_id: str, logger: logging.Logger) -> Optional[int]:
    """
    Fetch the last valid EntryID for a given FileID.
    """
    try:
        async with async_engine.connect() as conn:
            result = await conn.execute(
                text("""
                    SELECT MAX(EntryID) 
                    FROM utb_ImageScraperRecords 
                    WHERE FileID = :file_id AND ProductModel IS NOT NULL
                """),
                {"file_id": int(file_id)}
            )
            entry_id = result.fetchone()[0]
            result.close()
            return entry_id
    except Exception as e:
        logger.error(f"Failed to fetch last valid entry for FileID {file_id}: {e}")
        return None

