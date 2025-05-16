import asyncio
import base64
import json
import logging
import re
import requests
import urllib.parse
import pyodbc
import pandas as pd
import aiohttp
from PIL import Image
from io import BytesIO
import traceback
from typing import Optional, List, Tuple, Dict, Set
from image_vision import detect_objects_with_computer_vision_async, analyze_image_with_gemini_async
from db_utils import (
    fetch_missing_images, set_sort_order_negative_four_for_zero_match,
    update_search_sort_order, insert_search_results, update_log_url_in_db,
    get_send_to_email, update_file_generate_complete, update_file_location_complete,
    sync_update_search_sort_order,
    get_records_to_search
)
from config import conn_str
from common import clean_string, generate_aliases, generate_brand_aliases

# Default logger setup
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# URLs and configuration variables
category_hierarchy_url = "https://iconluxury.group/static_settings/category_hierarchy.json"
category_mapping_url = "https://iconluxury.group/static_settings/category_mapping.json"
fashion_labels_url = "https://iconluxury.group/static_settings/fashion_labels.json"

# Fallback data
fashion_labels_example = [
    "t-shirt", "shirt", "trouser", "dress", "coat", "jacket", "sweater", "pullover",
    "sandal", "sneaker", "shoe", "bag", "backpack", "hat", "scarf", "glove", "belt",
    "skirt", "short", "suit", "tie", "sock", "boot", "running-shoe", "athletic-shoe", "trainer"
]

category_hierarchy_example = {
    "tops": ["t-shirt", "shirt", "sweater", "blouse", "tank-top", "hoodie"],
    "bottoms": ["trouser", "jean", "short", "skirt", "legging"],
    "outerwear": ["coat", "jacket", "vest"],
    "dress": ["maxi-dress", "midi-dress", "mini-dress", "kimono"],
    "footwear": ["shoe", "sandal", "boot"],
    "accessories": ["bag", "hat", "belt", "scarf", "glove", "sunglasses"]
}

category_mapping_example = {
    "pants": "trouser",
    "jeans": "trouser",
    "jacket": "coat",
    "sneakers": "sneaker",
    "running-shoe": "sneaker",
    "tshirt": "t-shirt",
    "shirt": "t-shirt",
    "sweatshirt": "sweater",
    "hoodie": "sweater"
}

# Initialize configuration variables
def load_config(url: str, fallback: any, logger: logging.Logger, config_name: str, expect_list: bool = False) -> any:
    try:
        for attempt in range(3):
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                config = response.json()
                if expect_list and not isinstance(config, list):
                    raise ValueError(f"{config_name} must be a list")
                logger.info(f"Loaded {config_name}")
                return config
            except Exception as e:
                logger.warning(f"Failed to load {config_name} (attempt {attempt + 1}): {e}")
                if attempt == 2:
                    logger.info(f"Using fallback {config_name}")
                    return fallback
    except Exception as e:
        logger.error(f"Critical failure loading {config_name}: {e}")
        logger.info(f"Using fallback {config_name} due to critical failure")
        return fallback

FASHION_LABELS = load_config(fashion_labels_url, fashion_labels_example, default_logger, "FASHION_LABELS", expect_list=True)
CATEGORY_MAPPING = load_config(category_mapping_url, category_mapping_example, default_logger, "CATEGORY_MAPPING")
category_hierarchy = load_config(category_hierarchy_url, category_hierarchy_example, default_logger, "category_hierarchy")

async def get_image_data_async(
    image_urls: List[str],
    session: aiohttp.ClientSession,
    logger: logging.Logger = None,
    retries: int = 3
) -> Tuple[Optional[bytes], Optional[str]]:
    logger = logger or default_logger
    for url in image_urls:
        if not url or not isinstance(url, str) or url.strip() == "":
            logger.warning(f"Invalid URL: {url}")
            continue
        if not url.startswith(("http://", "https://")):
            logger.warning(f"Skipping invalid URL: {url}")
            continue
        decoded_url = urllib.parse.unquote(url.replace("\\u003d", "=").replace("\\u0026", "&"))
        for attempt in range(1, retries + 1):
            try:
                async with session.head(decoded_url, timeout=aiohttp.ClientTimeout(total=5)) as head_response:
                    if head_response.status in [403, 404]:
                        logger.error(f"URL {decoded_url} returned {head_response.status}. Skipping after attempt {attempt}.")
                        break
                async with session.get(decoded_url, timeout=aiohttp.ClientTimeout(total=15)) as response:
                    response.raise_for_status()
                    image_data = await response.read()
                    logger.info(f"Downloaded image from {decoded_url} on attempt {attempt}")
                    return image_data, decoded_url
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.error(f"Attempt {attempt} failed for {decoded_url}: {e}")
                if attempt < retries:
                    await asyncio.sleep(2)
        else:
            logger.warning(f"All {retries} attempts failed for {decoded_url}")
    logger.warning(f"All URLs failed: {image_urls}")
    return None, None

def is_related_to_category(detected_label: str, expected_category: str) -> bool:
    logger = default_logger
    if not detected_label:
        return False

    detected_label = detected_label.lower().strip()
    if not expected_category:
        if detected_label in FASHION_LABELS:
            logger.info(f"No expected category provided; accepting fashion-related label '{detected_label}'")
            return True
        return False

    expected_category = expected_category.lower().strip()

    if detected_label in expected_category:
        return True

    mapped_label = CATEGORY_MAPPING.get(detected_label, detected_label)
    if mapped_label in expected_category:
        return True

    sneaker_synonyms = ["sneaker", "running-shoe", "athletic-shoe", "trainer"]
    if "sneaker" in expected_category and detected_label in sneaker_synonyms:
        logger.info(f"Matched '{detected_label}' to 'sneaker' in category '{expected_category}'")
        return True

    for parent, children in category_hierarchy.items():
        if parent in expected_category and mapped_label in children:
            return True

    category_words = expected_category.split()
    for word in category_words:
        if word in [detected_label, mapped_label] and word not in ["men", "women", "leather", "plakka"]:
            logger.info(f"Partial match: '{word}' from category '{expected_category}' matches '{detected_label}'")
            return True

    return False

async def process_image(row, session: aiohttp.ClientSession, logger: Optional[logging.Logger] = None):
    logger = logger or default_logger
    result_id = row.get("ResultID")
    default_result = (result_id, json.dumps({"error": "Unknown processing error", "result_id": result_id}), None, 1)

    try:
        logger.debug(f"FASHION_LABELS at start of process_image: {FASHION_LABELS}")
        if result_id is None:
            logger.error(f"Invalid row data: ResultID missing - row: {row}")
            return result_id, json.dumps({"error": "Invalid row data: ResultID missing", "result_id": result_id}), None, 1

        # Check current SortOrder
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT SortOrder FROM utb_ImageScraperResult WHERE ResultID = ?", (result_id,))
            result = cursor.fetchone()
            sort_order = result[0] if result else None
            logger.debug(f"SortOrder for ResultID {result_id}: {sort_order}")
            if isinstance(sort_order, (int, float)) and sort_order > 0:
                logger.info(f"Processing image with positive SortOrder for ResultID {result_id}: {sort_order}")

        logger.debug(f"Processing row for ResultID {result_id}: {row}")
        if isinstance(sort_order, (int, float)) and sort_order < 0:
            logger.info(f"Skipping ResultID {result_id} due to negative SortOrder: {sort_order}")
            return result_id, json.dumps({"error": f"Negative SortOrder: {sort_order}", "result_id": result_id}), None, 0

        image_urls = [row["ImageUrl"]]
        if pd.notna(row.get("ImageUrlThumbnail")):
            image_urls.append(row["ImageUrlThumbnail"])
        product_details = {
            "brand": str(row.get("ProductBrand") or ""),
            "category": str(row.get("ProductCategory") or ""),
            "color": str(row.get("ProductColor") or "")
        }

        image_data, downloaded_url = await get_image_data_async(image_urls, session, logger)
        if not image_data:
            logger.warning(f"Image download failed for ResultID {result_id}")
            return result_id, json.dumps({"error": f"Image download failed for URLs: {image_urls}", "result_id": result_id}), None, 1

        base64_image = base64.b64encode(image_data).decode("utf-8")
        Image.open(BytesIO(image_data)).convert("RGB")

        cv_success, cv_description, person_confidences = await detect_objects_with_computer_vision_async(base64_image, logger)
        if not cv_success and not cv_description:
            logger.warning(f"Computer vision detection failed for ResultID {result_id}: {cv_description}")
            return result_id, json.dumps({"error": cv_description, "result_id": result_id}), None, 1

        def extract_labels(description):
            cls_label = None
            seg_label = None
            if description.startswith("Classification:"):
                cls_match = re.search(r"Classification: (\w+(?:\s+\w+)*) \(confidence:", description)
                cls_label = cls_match.group(1) if cls_match else None
            if "Segmented objects:" in description:
                seg_match = re.search(r"(\w+(?:\s+\w+)*) \(confidence: [\d.]+, mask area:", description)
                seg_label = seg_match.group(1) if seg_match else None
            return cls_label, seg_label

        cls_label, seg_label = extract_labels(cv_description)

        detected_objects = cv_description.split("\n")[2:] if "Segmented objects:" in cv_description else []
        labels = [label for label in [cls_label, seg_label] if label]
        is_fashion = any(label.lower() in [fl.lower() for fl in FASHION_LABELS] for label in labels if label)
        non_fashion_labels = [label for label in labels if label.lower() not in [fl.lower() for fl in FASHION_LABELS]]
        if len(detected_objects) > 1 and not is_fashion:
            logger.info(f"Discarding image for ResultID {result_id} due to multiple non-fashion objects: {non_fashion_labels}")
            ai_json = json.dumps({
                "description": f"Image contains multiple objects: {non_fashion_labels}, none of which are fashion-related.",
                "extracted_features": {"brand": "Unknown", "category": "Multiple", "color": "Unknown"},
                "match_score": 0.0,
                "reasoning": f"Multiple non-fashion objects detected: {non_fashion_labels}.",
                "cv_detection": cv_description,
                "person_confidences": person_confidences,
                "result_id": result_id
            })
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("UPDATE utb_ImageScraperResult SET SortOrder = -6, AiJson = ? WHERE ResultID = ?", (ai_json, result_id))
                conn.commit()
                logger.info(f"Updated database with SortOrder=-6 for ResultID {result_id}")
            return result_id, ai_json, cv_description, 0

        person_detected = any(conf > 0.5 for conf in person_confidences)
        cls_conf = float(re.search(r"Classification: \w+(?:\s+\w+)* \(confidence: ([\d.]+)\)", cv_description).group(1)) if cls_label else 0.0
        seg_conf = float(re.search(r"confidence: ([\d.]+), mask area:", cv_description).group(1)) if seg_label else 0.0
        if not person_detected and not is_fashion and max(cls_conf, seg_conf) < 0.2:
            logger.info(f"Discarding image for ResultID {result_id} due to no person and low confidence")
            ai_json = json.dumps({
                "description": "Image lacks fashion items and persons with sufficient confidence.",
                "extracted_features": {"brand": "Unknown", "category": "Unknown", "color": "Unknown"},
                "match_score": 0.0,
                "reasoning": "No person detected and low confidence in fashion detection.",
                "cv_detection": cv_description,
                "person_confidences": person_confidences,
                "result_id": result_id
            })
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("UPDATE utb_ImageScraperResult SET SortOrder = -6, AiJson = ? WHERE ResultID = ?", (ai_json, result_id))
                conn.commit()
                logger.info(f"Updated database with SortOrder=-6 for ResultID {result_id}")
            return result_id, ai_json, cv_description, 0

        expected_category = product_details.get("category", "").lower()
        inferred_category = None
        if not expected_category and is_fashion:
            for label in labels:
                if label.lower() in [fl.lower() for fl in FASHION_LABELS]:
                    inferred_category = label
                    logger.info(f"Inferred category '{inferred_category}' for ResultID {result_id} from detected label")
                    break

        if cls_label and not is_related_to_category(cls_label, expected_category or inferred_category or ""):
            logger.warning(f"Detected label '{cls_label}' not related to category '{expected_category or inferred_category or 'None'}'")
            cv_description += f"\nWarning: Detected classification may be irrelevant to category '{expected_category or inferred_category or 'None'}'."
        if seg_label and not is_related_to_category(seg_label, expected_category or inferred_category or ""):
            logger.warning(f"Detected label '{seg_label}' not related to category '{expected_category or inferred_category or 'None'}'")
            cv_description += f"\nWarning: Detected segmentation may be irrelevant to category '{expected_category or inferred_category or 'None'}'."

        gemini_result = await analyze_image_with_gemini_async(base64_image, product_details, logger=logger, cv_description=cv_description)
        if not gemini_result["success"]:
            logger.warning(f"Gemini analysis failed for ResultID {result_id}: {gemini_result.get('features', {}).get('reasoning', 'No details')}")
        features = gemini_result.get("features", {
            "description": cv_description,
            "extracted_features": {"brand": "Unknown", "category": inferred_category or "Unknown", "color": "Unknown"},
            "match_score": 0.5,
            "reasoning": "Gemini analysis failed; using computer vision description with default score."
        })

        description = features.get("description", cv_description)
        extracted_features = features.get("extracted_features", {})
        try:
            match_score = float(features.get("match_score", 0.5))
        except (ValueError, TypeError) as e:
            logger.warning(f"Invalid match_score value '{features.get('match_score')}' for ResultID {result_id}: {e}. Using default 0.5")
            match_score = 0.5
        logger.debug(f"match_score type: {type(match_score)}, value: {match_score} for ResultID {result_id}")
        reasoning = features.get("reasoning", "No reasoning provided")

        if match_score < 0.1 and product_details:
            valid_details = sum(1 for v in product_details.values() if v != "None")
            matches = sum(
                1 for k, v in product_details.items()
                if v != "None" and v.lower() in str(extracted_features.get(k, "")).lower()
            )
            heuristic_score = matches / max(1, valid_details) if valid_details > 0 else 1.0
            match_score = max(match_score, heuristic_score)
            reasoning = reasoning + f" Adjusted with heuristic: {heuristic_score:.2f}"

        raw_category = (product_details.get("category") or inferred_category or extracted_features.get("category", "")).strip().lower()
        normalized_category = raw_category
        if raw_category:
            category_parts = raw_category.split()
            base_candidates = [part for part in category_parts if part]
            for candidate in reversed(base_candidates):
                singular = candidate[:-1] if candidate.endswith("s") else candidate
                plural = f"{candidate}s" if not candidate.endswith("s") else candidate
                for form in [candidate, singular, plural]:
                    if form in category_hierarchy or any(form in sublist for sublist in category_hierarchy.values()):
                        normalized_category = form
                        logger.info(f"Normalized '{raw_category}' to '{normalized_category}'")
                        break
                if normalized_category != raw_category:
                    break

        ai_json = json.dumps({
            "description": description,
            "extracted_features": {
                "brand": extracted_features.get("brand", "Unknown"),
                "color": extracted_features.get("color", "Unknown"),
                "category": normalized_category
            },
            "match_score": match_score,
            "reasoning": reasoning,
            "cv_detection": cv_description,
            "person_confidences": person_confidences,
            "result_id": result_id
        })
        ai_caption = description
        logger.info(f"Processed ResultID {result_id} successfully with SortOrder {sort_order}")
        return result_id, ai_json, ai_caption, 1 if is_fashion else 0

    except Exception as e:
        logger.error(f"Unexpected error in process_image for ResultID {result_id}: {str(e)}", exc_info=True)
        ai_json = json.dumps({"error": f"Processing error: {str(e)}", "result_id": result_id})
        return result_id, ai_json, None, 1

async def process_entry(
    file_id: int,
    entry_id: int,
    entry_df: pd.DataFrame,
    logger: logging.Logger
) -> List[Tuple[int, str, Optional[str], int]]:
    logger.info(f"Starting task for EntryID: {entry_id} with {len(entry_df)} rows for FileID: {file_id}")
    updates = []

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT ProductBrand, ProductModel, ProductColor, ProductCategory
                FROM utb_ImageScraperRecords
                WHERE FileID = ? AND EntryID = ?
            """, (file_id, entry_id))
            result = cursor.fetchone()
            product_brand = product_model = product_color = product_category = ''
            if result:
                product_brand, product_model, product_color, product_category = result
                logger.info(f"Fetched attributes for EntryID: {entry_id}")
            else:
                logger.warning(f"No attributes for FileID: {file_id}, EntryID: {entry_id}")

        sync_update_search_sort_order(
            file_id=str(file_id),
            entry_id=str(entry_id),
            brand=product_brand,
            model=product_model,
            color=product_color,
            category=product_category,
            logger=logger
        )
        logger.info(f"Updated sort order for EntryID: {entry_id}")

        async with aiohttp.ClientSession() as session:
            tasks = [process_image(row, session, logger) for _, row in entry_df.iterrows()]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Error in process_image: {result}")
                    continue
                if result is None:
                    logger.error(f"process_image returned None for row")
                    continue
                updates.append(result)

        logger.info(f"Completed task for EntryID: {entry_id} with {len(updates)} updates")
        return updates

    except Exception as e:
        logger.error(f"Error processing EntryID {entry_id}: {e}", exc_info=True)
        return [(row.get("ResultID"), json.dumps({"error": f"Entry processing error: {str(e)}", "result_id": row.get("ResultID")}), None, 1) for _, row in entry_df.iterrows()]