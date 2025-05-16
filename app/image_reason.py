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
from typing import Optional, List, Tuple, Dict, Set
from image_vision import detect_objects_with_computer_vision_async, analyze_image_with_gemini_async
from db_utils import (
    fetch_missing_images, set_sort_order_negative_four_for_zero_match,
    update_search_sort_order, insert_search_results, update_log_url_in_db,
    get_send_to_email, update_file_generate_complete, update_file_location_complete,
    sync_update_search_sort_order, get_records_to_search
)
from config import conn_str, BASE_CONFIG_URL
from common import clean_string, generate_aliases, generate_brand_aliases, load_config, CONFIG_FILES

# Default logger setup
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

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
    "pants": "trouser", "jeans": "trouser", "jacket": "coat", "sneakers": "sneaker",
    "running-shoe": "sneaker", "tshirt": "t-shirt", "shirt": "t-shirt",
    "sweatshirt": "sweater", "hoodie": "sweater"
}

# Load configurations
async def initialize_configs():
    global FASHION_LABELS, CATEGORY_MAPPING, category_hierarchy
    FASHION_LABELS = await load_config(
        "fashion_labels", fashion_labels_example, default_logger, "FASHION_LABELS", expect_list=True
    )
    CATEGORY_MAPPING = await load_config(
        "category_mapping", category_mapping_example, default_logger, "CATEGORY_MAPPING"
    )
    category_hierarchy = await load_config(
        "category_hierarchy", category_hierarchy_example, default_logger, "category_hierarchy"
    )

# Run initialization
asyncio.run(initialize_configs())

# ... (rest of image_reason.py remains unchanged)
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
async def process_image(row, session: aiohttp.ClientSession, logger: logging.Logger) -> Tuple[int, str, Optional[str], int]:
    """Process a single image row with Gemini API, ensuring valid JSON output."""
    result_id = row.get("ResultID")
    default_result = (
        result_id or 0,
        json.dumps({"error": "Unknown processing error", "result_id": result_id or 0, "scores": {"sentiment": 0.0, "relevance": 0.0}}),
        "Processing failed",
        1
    )

    try:
        if not result_id or not isinstance(result_id, int):
            logger.error(f"Invalid ResultID for row: {row}")
            return default_result

        image_urls = [row["ImageUrl"]]
        if pd.notna(row.get("ImageUrlThumbnail")):
            image_urls.append(row["ImageUrlThumbnail"])
        product_details = {
            "brand": str(row.get("ProductBrand") or "None"),
            "category": str(row.get("ProductCategory") or "None"),
            "color": str(row.get("ProductColor") or "None")
        }

        async def get_image_data_async(image_urls: List[str], session: aiohttp.ClientSession) -> Tuple[Optional[bytes], Optional[str]]:
            for url in image_urls:
                if not url or not isinstance(url, str) or url.strip() == "":
                    logger.warning(f"Invalid URL: {url}")
                    continue
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as response:
                        response.raise_for_status()
                        image_data = await response.read()
                        logger.info(f"Downloaded image from {url}")
                        return image_data, url
                except Exception as e:
                    logger.warning(f"Failed to download {url}: {e}")
            return None, None

        image_data, downloaded_url = await get_image_data_async(image_urls, session)
        if not image_data:
            logger.warning(f"Image download failed for ResultID {result_id}")
            return (
                result_id,
                json.dumps({"error": f"Image download failed for URLs: {image_urls}", "result_id": result_id, "scores": {"sentiment": 0.0, "relevance": 0.0}}),
                "Image download failed",
                1
            )

        base64_image = base64.b64encode(image_data).decode("utf-8")
        from PIL import Image
        from io import BytesIO
        Image.open(BytesIO(image_data)).convert("RGB")

        cv_success, cv_description, person_confidences = await detect_objects_with_computer_vision_async(base64_image, logger)
        if not cv_success or not cv_description:
            logger.warning(f"Computer vision detection failed for ResultID {result_id}: {cv_description}")
            return (
                result_id,
                json.dumps({"error": cv_description or "CV detection failed", "result_id": result_id, "scores": {"sentiment": 0.0, "relevance": 0.0}}),
                "CV detection failed",
                1
            )

        # Check for non-fashion or multiple objects
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
        fashion_labels = ["t-shirt", "shirt", "trouser", "dress", "coat", "jacket", "sweater", "sandal", "sneaker", "shoe", "bag", "hat", "scarf", "glove", "belt", "skirt", "short", "suit", "boot", "running_shoe"]
        labels = [label for label in [cls_label, seg_label] if label]
        is_fashion = any(label.lower() in [fl.lower() for fl in fashion_labels] for label in labels if label)
        non_fashion_labels = [label for label in labels if label and label.lower() not in [fl.lower() for fl in fashion_labels]]

        person_detected = any(conf > 0.5 for conf in person_confidences)
        cls_conf = float(re.search(r"Classification: \w+(?:\s+\w+)* \(confidence: ([\d.]+)\)", cv_description).group(1)) if cls_label else 0.0
        seg_conf = float(re.search(r"confidence: ([\d.]+), mask area:", cv_description).group(1)) if seg_label else 0.0

        if len(detected_objects) > 1 and not is_fashion:
            logger.info(f"Non-fashion objects detected for ResultID {result_id}: {non_fashion_labels}")
            ai_json = json.dumps({
                "description": f"Image contains multiple objects: {non_fashion_labels}, none of which are fashion-related.",
                "extracted_features": {"brand": "Unknown", "category": "Multiple", "color": "Unknown"},
                "scores": {"sentiment": 0.0, "relevance": 0.0},
                "reasoning": f"Multiple non-fashion objects detected: {non_fashion_labels}.",
                "cv_detection": cv_description,
                "person_confidences": person_confidences,
                "result_id": result_id
            })
            return result_id, ai_json, "Non-fashion objects detected", 0

        if not person_detected and not is_fashion and max(cls_conf, seg_conf) < 0.2:
            logger.info(f"No fashion items or persons for ResultID {result_id}")
            ai_json = json.dumps({
                "description": "Image lacks fashion items and persons with sufficient confidence.",
                "extracted_features": {"brand": "Unknown", "category": "Unknown", "color": "Unknown"},
                "scores": {"sentiment": 0.0, "relevance": 0.0},
                "reasoning": "No person detected and low confidence in fashion detection.",
                "cv_detection": cv_description,
                "person_confidences": person_confidences,
                "result_id": result_id
            })
            return result_id, ai_json, "No fashion items detected", 0

        gemini_result = await analyze_image_with_gemini_async(base64_image, product_details, logger=logger, cv_description=cv_description)
        logger.debug(f"Gemini raw response for ResultID {result_id}: {json.dumps(gemini_result, indent=2)}")

        if not gemini_result.get("success", False):
            logger.warning(f"Gemini analysis failed for ResultID {result_id}: {gemini_result.get('features', {}).get('reasoning', 'No details')}")
            ai_json = json.dumps({
                "error": gemini_result.get('features', {}).get('reasoning', 'Gemini analysis failed'),
                "result_id": result_id,
                "scores": {"sentiment": 0.0, "relevance": 0.0}
            })
            return result_id, ai_json, "Gemini analysis failed", 1

        features = gemini_result.get("features", {
            "description": cv_description,
            "extracted_features": {"brand": "Unknown", "category": "Unknown", "color": "Unknown"},
            "match_score": 0.5,
            "reasoning": "Gemini analysis failed; using CV description"
        })

        description = features.get("description", cv_description or "No description").encode('utf-8').decode('utf-8')
        extracted_features = features.get("extracted_features", {})
        try:
            match_score = float(features.get("match_score", 0.5))
        except (ValueError, TypeError):
            logger.warning(f"Invalid match_score for ResultID {result_id}: {features.get('match_score')}")
            match_score = 0.5
        reasoning = features.get("reasoning", "No reasoning provided").encode('utf-8').decode('utf-8')

        ai_json = json.dumps({
            "scores": {
                "sentiment": match_score,
                "relevance": match_score
            },
            "category": extracted_features.get("category", "unknown"),
            "keywords": [extracted_features.get("category", "unknown").lower()],
            "description": description,
            "extracted_features": extracted_features,
            "reasoning": reasoning,
            "cv_detection": cv_description,
            "person_confidences": person_confidences,
            "result_id": result_id
        })
        # Validate JSON before returning
        try:
            json.loads(ai_json)
        except json.JSONDecodeError as e:
            logger.error(f"Malformed JSON for ResultID {result_id}: {e}, AiJson: {ai_json}")
            return (
                result_id,
                json.dumps({"error": f"Malformed JSON: {e}", "result_id": result_id, "scores": {"sentiment": 0.0, "relevance": 0.0}}),
                "JSON generation failed",
                1
            )

        ai_caption = description if description.strip() else f"{product_details['brand']} {product_details['category']} item"
        is_fashion = extracted_features.get("category", "unknown").lower() != "unknown"

        logger.info(f"Processed ResultID {result_id} successfully")
        return result_id, ai_json, ai_caption, 1 if is_fashion else 0

    except Exception as e:
        logger.error(f"Unexpected error in process_image for ResultID {result_id}: {e}", exc_info=True)
        return (
            result_id or 0,
            json.dumps({"error": f"Processing error: {e}", "result_id": result_id or 0, "scores": {"sentiment": 0.0, "relevance": 0.0}}),
            "Processing failed",
            1
        )

async def process_entry(
    file_id: int,
    entry_id: int,
    entry_df: pd.DataFrame,
    logger: logging.Logger
) -> List[Tuple[str, bool, str, int]]:
    """Process image entries for an EntryID, ensuring valid updates."""
    logger.info(f"Starting task for EntryID: {entry_id} with {len(entry_df)} rows for FileID: {file_id}")
    updates = []

    try:
        # Validate entry_df
        if not all(col in entry_df.columns for col in ['ResultID', 'ImageUrl']):
            logger.error(f"Missing required columns in entry_df for EntryID {entry_id}: {entry_df.columns}")
            return []

        # Fetch product attributes
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

        # Process images
        async with aiohttp.ClientSession() as session:
            tasks = [process_image(row, session, logger) for _, row in entry_df.iterrows() if pd.notna(row.get('ResultID'))]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Error in process_image: {result}")
                    continue
                if not result or not isinstance(result, tuple) or len(result) != 4:
                    logger.error(f"Invalid result from process_image: {result}")
                    continue
                result_id, ai_json, ai_caption, is_fashion = result
                updates.append((ai_json, is_fashion, ai_caption, result_id))

        logger.info(f"Completed task for EntryID: {entry_id} with {len(updates)} updates")
        return updates

    except Exception as e:
        logger.error(f"Error processing EntryID {entry_id}: {e}", exc_info=True)
        return []
