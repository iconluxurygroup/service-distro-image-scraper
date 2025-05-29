import asyncio
import base64
import json
import logging
import re
import urllib.parse
import aiohttp
from PIL import Image
from io import BytesIO
from typing import Optional, List, Tuple, Dict
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from image_vision import detect_objects_with_computer_vision_async, analyze_image_with_gemini_async
from vision_utils import fetch_missing_images
from config import BASE_CONFIG_URL
from common import clean_string, generate_aliases, generate_brand_aliases, load_config, CONFIG_FILES
import pandas as pd
import pyodbc
from database_config import conn_str

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
    "sweatshirt": "sweater", "hoodie": "sweater", "purse": "bag", "handbag": "bag"
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

async def get_image_data_async(
    image_urls: List[str],
    session: aiohttp.ClientSession,
    logger: logging.Logger = None,
    retries: int = 3
) -> Tuple[Optional[bytes], Optional[str]]:
    logger = logger or default_logger
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    for url in image_urls:
        if not url or not isinstance(url, str) or url.strip() == "":
            logger.warning(f"Invalid URL: {url}")
            continue
        if not url.startswith(("http://", "https://")):
            logger.warning(f"Skipping invalid URL: {url}")
            continue
        decoded_url = urllib.parse.unquote(url.replace("\\u003d", "=").replace("\\u0026", "&"))
        max_retries = 5 if "google" in decoded_url.lower() else retries

        @retry(
            stop=stop_after_attempt(max_retries),
            wait=wait_fixed(2),
            retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
            before_sleep=lambda retry_state: logger.info(f"Retrying {decoded_url} (attempt {retry_state.attempt_number}/{max_retries})")
        )
        async def try_download():
            async with session.get(decoded_url, timeout=aiohttp.ClientTimeout(total=15), headers=headers) as response:
                response.raise_for_status()
                content = await response.read()
                try:
                    Image.open(BytesIO(content)).convert("RGB")
                    return content
                except Exception as e:
                    logger.error(f"Invalid image content from {decoded_url}: {e}")
                    raise ValueError("Invalid image content")

        try:
            image_data = await try_download()
            logger.info(f"Downloaded image from {decoded_url}")
            return image_data, decoded_url
        except Exception as e:
            logger.error(f"All retries failed for {decoded_url}: {e}")
            continue
    logger.warning(f"All URLs failed: {image_urls}")
    return None, None

async def generate_thumbnail(
    image_data: Optional[bytes],
    logger: logging.Logger = None,
    size: Tuple[int, int] = (100, 100)
) -> str:
    logger = logger or default_logger
    try:
        if image_data:
            image = Image.open(BytesIO(image_data)).convert("RGB")
        else:
            logger.warning("No image data provided; using fallback thumbnail")
            image = Image.new("RGB", size, color=(128, 128, 128))
        image.thumbnail(size, Image.Resampling.LANCZOS)
        buffered = BytesIO()
        image.save(buffered, format="JPEG")
        thumbnail_base64 = base64.b64encode(buffered.getvalue()).decode("utf-8")
        logger.info("Thumbnail generated successfully")
        return thumbnail_base64
    except Exception as e:
        logger.error(f"Thumbnail generation failed: {e}")
        image = Image.new("RGB", size, color=(128, 128, 128))
        image.thumbnail(size, Image.Resampling.LANCZOS)
        buffered = BytesIO()
        image.save(buffered, format="JPEG")
        thumbnail_base64 = base64.b64encode(buffered.getvalue()).decode("utf-8")
        logger.info("Generated fallback thumbnail")
        return thumbnail_base64

async def fetch_stored_thumbnail(
    result_id: int,
    session: aiohttp.ClientSession,
    logger: logging.Logger = None
) -> Optional[str]:
    logger = logger or default_logger
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT ImageUrlThumbnail FROM utb_ImageScraperResult WHERE ResultID = ?",
                (result_id,)
            )
            result = cursor.fetchone()
            if not result:
                logger.warning(f"No thumbnail found in database for ResultID {result_id}")
                return None

            thumbnail_url = result[0]
            if thumbnail_url:
                try:
                    base64.b64decode(thumbnail_url)
                    logger.info(f"Using stored ImageUrlThumbnail for ResultID {result_id}")
                    return thumbnail_url
                except Exception:
                    image_data, _ = await get_image_data_async([thumbnail_url], session, logger, retries=3)
                    if image_data:
                        thumbnail_base64 = await generate_thumbnail(image_data, logger)
                        logger.info(f"Generated thumbnail from stored ImageUrlThumbnail for ResultID {result_id}")
                        return thumbnail_base64
                    logger.warning(f"Failed to download stored ImageUrlThumbnail {thumbnail_url} for ResultID {result_id}")
            return None
    except pyodbc.Error as e:
        logger.error(f"Database error fetching thumbnail for ResultID {result_id}: {e}")
        return None

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

async def process_image(row, session: aiohttp.ClientSession, logger: logging.Logger) -> Tuple[int, str, Optional[str], int, Optional[str]]:
    """Process a single image row with Gemini API, ensuring valid JSON output and thumbnail."""
    result_id = row.get("ResultID")
    default_result = (
        result_id or 0,
        json.dumps({"error": "Unknown processing error", "result_id": result_id or 0, "scores": {"sentiment": 0.0, "relevance": 0.0, "category": 0.0, "color": 0.0, "brand": 0.0, "model": 0.0}}),
        "Processing failed",
        1,
        None
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
            "color": str(row.get("ProductColor") or "None"),
            "model": str(row.get("ProductModel") or "None")
        }

        thumbnail_base64 = await fetch_stored_thumbnail(result_id, session, logger)
        image_data, downloaded_url = await get_image_data_async(image_urls, session, logger)
        if not thumbnail_base64:
            thumbnail_base64 = await generate_thumbnail(image_data, logger)
            logger.info(f"Generated new thumbnail for ResultID {result_id}")

        if not image_data:
            logger.warning(f"Image download failed for ResultID {result_id}")
            ai_json = json.dumps({
                "error": f"Image download failed for URLs: {image_urls}",
                "result_id": result_id,
                "scores": {"sentiment": 0.0, "relevance": 0.0, "category": 0.0, "color": 0.0, "brand": 0.0, "model": 0.0},
                "thumbnail": thumbnail_base64
            })
            return result_id, ai_json, "Image download failed", 1, thumbnail_base64

        base64_image = base64.b64encode(image_data).decode("utf-8")
        Image.open(BytesIO(image_data)).convert("RGB")

        cv_success, cv_description, person_confidences = await detect_objects_with_computer_vision_async(base64_image, logger)
        if not cv_success or not cv_description:
            logger.warning(f"Computer vision detection failed for ResultID {result_id}: {cv_description}")
            ai_json = json.dumps({
                "error": cv_description or "CV detection failed",
                "result_id": result_id,
                "scores": {"sentiment": 0.0, "relevance": 0.0, "category": 0.0, "color": 0.0, "brand": 0.0, "model": 0.0},
                "thumbnail": thumbnail_base64
            })
            return result_id, ai_json, "CV detection failed", 1, thumbnail_base64

        cls_label, seg_label = extract_labels(cv_description)
        detected_objects = cv_description.split("\n")[2:] if "Segmented objects:" in cv_description else []
        labels = [label for label in [cls_label, seg_label] if label]
        is_fashion = any(is_related_to_category(label, product_details["category"]) for label in labels if label)
        non_fashion_labels = [label for label in labels if label and not is_related_to_category(label, product_details["category"])]

        person_detected = any(conf > 0.5 for conf in person_confidences)
        cls_conf = float(re.search(r"Classification: \w+(?:\s+\w+)* \(confidence: ([\d.]+)\)", cv_description).group(1)) if cls_label else 0.0
        seg_conf = float(re.search(r"confidence: ([\d.]+), mask area:", cv_description).group(1)) if seg_label else 0.0

        if len(detected_objects) > 1 and not is_fashion:
            logger.info(f"Non-fashion objects detected for ResultID {result_id}: {non_fashion_labels}")
            ai_json = json.dumps({
                "description": f"Image contains multiple objects: {non_fashion_labels}, none of which are fashion-related.",
                "extracted_features": {"brand": "Unknown", "category": "Multiple", "color": "Unknown", "model": "Unknown"},
                "scores": {"sentiment": 0.0, "relevance": 0.0, "category": 0.0, "color": 0.0, "brand": 0.0, "model": 0.0},
                "reasoning": f"Multiple non-fashion objects detected: {non_fashion_labels}.",
                "cv_detection": cv_description,
                "person_confidences": person_confidences,
                "result_id": result_id,
                "thumbnail": thumbnail_base64
            })
            return result_id, ai_json, "Non-fashion objects detected", 0, thumbnail_base64

        if not person_detected and not is_fashion and max(cls_conf, seg_conf) < 0.2:
            logger.info(f"No fashion items or persons for ResultID {result_id}")
            ai_json = json.dumps({
                "description": "Image lacks fashion items and persons with sufficient confidence.",
                "extracted_features": {"brand": "Unknown", "category": "Unknown", "color": "Unknown", "model": "Unknown"},
                "scores": {"sentiment": 0.0, "relevance": 0.0, "category": 0.0, "color": 0.0, "brand": 0.0, "model": 0.0},
                "reasoning": "No person detected and low confidence in fashion detection.",
                "cv_detection": cv_description,
                "person_confidences": person_confidences,
                "result_id": result_id,
                "thumbnail": thumbnail_base64
            })
            return result_id, ai_json, "No fashion items detected", 0, thumbnail_base64

        gemini_result = await analyze_image_with_gemini_async(base64_image, product_details, logger=logger, cv_description=cv_description)
        logger.debug(f"Gemini raw response for ResultID {result_id}: {json.dumps(gemini_result, indent=2)}")

        if not gemini_result.get("success", False):
            logger.warning(f"Gemini analysis failed for ResultID {result_id}: {gemini_result.get('features', {}).get('reasoning', 'No details')}")
            ai_json = json.dumps({
                "error": gemini_result.get('features', {}).get('reasoning', 'Gemini analysis failed'),
                "result_id": result_id,
                "scores": {"sentiment": 0.0, "relevance": 0.0, "category": 0.0, "color": 0.0, "brand": 0.0, "model": 0.0},
                "thumbnail": thumbnail_base64
            })
            return result_id, ai_json, "Gemini analysis failed", 1, thumbnail_base64

        features = gemini_result.get("features", {
            "description": cv_description,
            "extracted_features": {"brand": "Unknown", "category": "Unknown", "color": "Unknown", "model": "Unknown"},
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

        # Calculate individual scores
        detected_category = extracted_features.get("category", "unknown").lower()
        category_score = 1.0 if is_related_to_category(detected_category, product_details["category"]) else 0.5

        detected_color = extracted_features.get("color", "unknown").lower()
        color_score = 1.0 if detected_color == product_details["color"].lower() and detected_color != "unknown" else 0.5

        detected_brand = extracted_features.get("brand", "unknown").lower()
        brand_aliases = generate_brand_aliases(product_details["brand"]) if product_details["brand"].lower() != "none" else []
        brand_score = 1.0 if detected_brand in brand_aliases or detected_brand == product_details["brand"].lower() else 0.5

        detected_model = extracted_features.get("model", "unknown").lower()
        model_score = 1.0 if detected_model == product_details["model"].lower() and detected_model != "unknown" else 0.3

        ai_json = json.dumps({
            "scores": {
                "sentiment": match_score,
                "relevance": match_score,
                "category": category_score,
                "color": color_score,
                "brand": brand_score,
                "model": model_score
            },
            "category": extracted_features.get("category", "unknown"),
            "keywords": [extracted_features.get("category", "unknown").lower()],
            "description": description,
            "extracted_features": extracted_features,
            "reasoning": reasoning,
            "cv_detection": cv_description,
            "person_confidences": person_confidences,
            "result_id": result_id,
            "thumbnail": thumbnail_base64
        })
        try:
            json.loads(ai_json)
        except json.JSONDecodeError as e:
            logger.error(f"Malformed JSON for ResultID {result_id}: {e}, AiJson: {ai_json}")
            ai_json = json.dumps({
                "error": f"Malformed JSON: {e}",
                "result_id": result_id,
                "scores": {"sentiment": 0.0, "relevance": 0.0, "category": 0.0, "color": 0.0, "brand": 0.0, "model": 0.0},
                "thumbnail": thumbnail_base64
            })
            return result_id, ai_json, "JSON generation failed", 1, thumbnail_base64

        ai_caption = description if description.strip() else f"{product_details['brand']} {product_details['category']} item"
        is_fashion = extracted_features.get("category", "unknown").lower() != "unknown"

        logger.info(f"Processed ResultID {result_id} successfully")
        return result_id, ai_json, ai_caption, 1 if is_fashion else 0, thumbnail_base64

    except Exception as e:
        logger.error(f"Unexpected error in process_image for ResultID {result_id}: {e}", exc_info=True)
        thumbnail_base64 = thumbnail_base64 or await generate_thumbnail(None, logger)
        ai_json = json.dumps({
            "error": f"Processing error: {e}",
            "result_id": result_id or 0,
            "scores": {"sentiment": 0.0, "relevance": 0.0, "category": 0.0, "color": 0.0, "brand": 0.0, "model": 0.0},
            "thumbnail": thumbnail_base64
        })
        return result_id or 0, ai_json, "Processing failed", 1, thumbnail_base64

async def process_entry(
    file_id: int,
    entry_id: int,
    entry_df: pd.DataFrame,
    logger: logging.Logger
) -> List[Tuple[str, bool, str, int]]:
    logger.info(f"Starting task for EntryID: {entry_id} with {len(entry_df)} rows for FileID: {file_id}")
    updates = []

    try:
        if not all(col in entry_df.columns for col in ['ResultID', 'ImageUrl']):
            logger.error(f"Missing required columns in entry_df for EntryID {entry_id}: {entry_df.columns}")
            return []

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

        async with aiohttp.ClientSession() as session:
            tasks = [process_image(row, session, logger) for _, row in entry_df.iterrows() if pd.notna(row.get('ResultID'))]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Error in process_image: {result}")
                    continue
                if not result or not isinstance(result, tuple) or len(result) != 5:
                    logger.error(f"Invalid result from process_image: {result}")
                    continue
                result_id, ai_json, ai_caption, is_fashion, thumbnail_base64 = result
                updates.append((ai_json, is_fashion, ai_caption, result_id))

        logger.info(f"Completed task for EntryID: {entry_id} with {len(updates)} updates")
        return updates

    except Exception as e:
        logger.error(f"Error processing EntryID {entry_id}: {e}", exc_info=True)
        return []

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