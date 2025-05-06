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
import google.generativeai as genai
from ultralytics import YOLO
import numpy as np
from typing import Optional, List, Tuple, Dict
from config import GOOGLE_API_KEY, conn_str
import ray
import os

# Default logger setup
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Initialize YOLOv11 models for classification and segmentation
try:
    CLS_MODEL = YOLO("yolo11m-cls.pt", task="classify", verbose=True)
    SEG_MODEL = YOLO("yolo11m-seg.pt", task="segment", verbose=True)
    default_logger.info("Successfully initialized YOLOv11 models")
except Exception as e:
    CLS_MODEL = None
    SEG_MODEL = None
    default_logger.error(
        f"YOLOv11 initialization failed: {e}. Ensure ultralytics is installed (pip install ultralytics), "
        f"network access is available, and cache directory has write permissions. "
        f"Computer vision will use a fallback description.",
        exc_info=True
    )

FASHION_MNIST_CLASSES = [
    "T-shirt/top", "Trouser", "Pullover", "Dress", "Coat",
    "Sandal", "Shirt", "Sneaker", "Bag", "Ankle boot"
]

# Category hierarchy URL and fallback
category_hierarchy_url = "https://raw.githubusercontent.com/iconluxurygroup/settings-static-data/refs/heads/main/category_hierarchy.json"
category_hierarchy = None

# Expanded category mapping for similar labels
CATEGORY_MAPPING = {
    "jersey": "sweatshirt",
    "jacket": "coat",
    "wool": "sweater",
    "suit": "jacket",
    "trench_coat": "coat",
    "maillot": "tights",
    "velvet": "dress",
    "jean": "trouser",
    "kimono": "dress",
    "skim": "trouser",
    "skinny": "trouser",
    "yes": "dress"
}

# Non-fashion labels to filter out
NON_FASHION_LABELS = [
    "soap_dispenser", "parking meter", "spoon", "screw", "safety_pin", "tick",
    "ashcan", "loudspeaker", "joystick", "perfume", "car", "dog", "bench"
]

# Simplified hierarchical category relationships
category_hierarchy_example = {
    "coat": ["jacket", "trench_coat"],
    "trouser": ["jean", "skinny", "skim"],
    "dress": ["kimono", "velvet", "yes"],
    "sweatshirt": ["jersey"],
    "sweater": ["wool"],
    "tights": ["maillot"]
}

async def detect_objects_with_computer_vision_async(image_base64: str, logger: Optional[logging.Logger] = None, max_retries: int = 3) -> Tuple[bool, Optional[str], Optional[List[float]]]:
    logger = logger or default_logger
    if CLS_MODEL is None or SEG_MODEL is None:
        logger.error("YOLOv11 models not initialized. Using fallback description.")
        return True, "No computer vision detection available due to model initialization failure.", []

    person_confidences = []
    for attempt in range(1, max_retries + 1):
        try:
            image_bytes = base64.b64decode(image_base64)
            image = Image.open(BytesIO(image_bytes)).convert("RGB")

            # Classification with lowered threshold
            cls_results = CLS_MODEL(image)
            cls_probs = cls_results[0].probs
            cls_label = cls_results[0].names[cls_probs.top1]
            cls_conf = float(cls_probs.top1conf)
            if cls_conf < 0.2:
                logger.info(f"Classification confidence too low ({cls_conf:.2f}) for attempt {attempt}")
                cls_label = None
                cls_conf = 0.0
            elif cls_label in NON_FASHION_LABELS:
                logger.warning(f"Non-fashion label '{cls_label}' detected; discarding for attempt {attempt}")
                cls_label = None
                cls_conf = 0.0

            # Map similar labels
            if cls_label:
                cls_label = CATEGORY_MAPPING.get(cls_label, cls_label)

            # Segmentation with lowered threshold
            seg_results = SEG_MODEL(image)
            detected_objects = []
            if seg_results[0].masks is not None:
                masks = seg_results[0].masks.data.cpu().numpy()
                boxes = seg_results[0].boxes
                for i, (box, mask) in enumerate(zip(boxes, masks)):
                    if box.conf[0] < 0.2:
                        continue
                    label = seg_results[0].names[int(box.cls[0])]
                    if label in NON_FASHION_LABELS:
                        logger.warning(f"Non-fashion label '{label}' detected; skipping")
                        continue
                    label = CATEGORY_MAPPING.get(label, label)
                    score = float(box.conf[0])
                    mask_area = int(np.sum(mask))
                    box_coords = box.xyxy[0].cpu().numpy()
                    detected_objects.append(
                        f"{label} (confidence: {score:.2f}, mask area: {mask_area} pixels) at bounding box [xmin: {box_coords[0]:.1f}, ymin: {box_coords[1]:.1f}, xmax: {box_coords[2]:.1f}, ymax: {box_coords[3]:.1f}]"
                    )
                    if label == "person":
                        person_confidences.append(score)
                        logger.info(f"Person detected with confidence {score:.2f} for attempt {attempt}")

            if not detected_objects and not cls_label:
                logger.info(f"No high-confidence objects segmented in attempt {attempt}")
                description = "No objects detected in the image."
                return True, description, person_confidences

            description = f"Classification: {cls_label} (confidence: {cls_conf:.2f})" if cls_label else "Classification: None (confidence: 0.00)"
            if detected_objects:
                description += "\nSegmented objects:\n" + "\n".join(detected_objects)
            logger.info(f"YOLOv11 detection (attempt {attempt}): {description}")
            return True, description, person_confidences
        except Exception as e:
            logger.error(f"YOLOv11 detection failed (attempt {attempt}): {e}")
            if attempt == max_retries:
                logger.error("All retries failed. Using fallback description.")
                return True, "No computer vision detection available due to repeated failures.", person_confidences
            await asyncio.sleep(1)
    return False, None, person_confidences

async def analyze_image_with_gemini_async(
    image_base64: str,
    product_details: Optional[Dict[str, str]] = None,
    api_key: str = GOOGLE_API_KEY,
    model_name: str = "gemini-2.0-flash",
    mime_type: str = "image/jpeg",
    logger: Optional[logging.Logger] = None,
    cv_description: Optional[str] = None,
    max_retries: int = 3
) -> Dict[str, Optional[dict]]:
    logger = logger or default_logger
    if not api_key:
        logger.error("No API key provided for Gemini")
        return {"status_code": None, "success": False, "features": {"error": "No API key provided"}}

    try:
        image_bytes = base64.b64decode(image_base64)
    except Exception as e:
        logger.error(f"Invalid base64 string: {e}")
        return {"status_code": None, "success": False, "features": {"error": f"Invalid base64 string: {e}"}}

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(model_name=model_name)
    brand = product_details.get("brand", "None") if product_details else "None"
    category = product_details.get("category", "None") if product_details else "None"
    color = product_details.get("color", "None") if product_details else "None"
    cv_info = cv_description if cv_description else "No prior detection available."

    prompt = f"""
    Analyze this image and return a single JSON object with:
    {{
      "description": "One-sentence description of the image (brand, category, color).",
      "extracted_features": {{
        "brand": "Extracted brand or 'Unknown'.",
        "category": "Extracted category or 'Unknown'.",
        "color": "Primary color or 'Unknown'."
      }},
      "match_score": "Score (0–1) for match with provided details.",
      "reasoning": "Brief explanation of match score."
    }}
    Provided details:
    - Brand: {brand}
    - Category: {category}
    - Color: {color}
    Computer vision detection: {cv_info}
    If details are 'None', assume match for that field.
    Use computer vision results to guide analysis.
    Return valid JSON only as a single object, not a list.
    """

    contents = [{"mime_type": mime_type, "data": image_bytes}, prompt]
    for attempt in range(1, max_retries + 1):
        try:
            response = await model.generate_content_async(
                contents,
                generation_config=genai.types.GenerationConfig(response_mime_type="application/json")
            )
            response_text = None
            if isinstance(response, list):
                logger.warning(f"Received list response from Gemini (attempt {attempt}); extracting text")
                if response and hasattr(response[0], 'text'):
                    response_text = response[0].text
                elif response:
                    response_text = str(response[0])
            else:
                response_text = getattr(response, "text", "") or ""

            if response_text:
                try:
                    features = json.loads(response_text)
                    # Handle case where JSON is a list instead of a dictionary
                    if isinstance(features, list):
                        logger.info(f"Parsed JSON is a list; selecting first element")
                        features = features[0] if features else {}
                    # Ensure features is a dictionary
                    if not isinstance(features, dict):
                        logger.error(f"Unexpected features type: {type(features)}")
                        features = {
                            "description": "Unable to analyze image.",
                            "extracted_features": {"brand": "Unknown", "category": "Unknown", "color": "Unknown"},
                            "match_score": 0.0,
                            "reasoning": "Unexpected response structure from Gemini."
                        }
                    # Safely extract data from features dictionary
                    match_score = float(features.get("match_score", 0.0))
                    extracted = features.get("extracted_features", {})
                    if not isinstance(extracted, dict):
                        logger.warning(f"Invalid extracted_features format: {extracted}")
                        extracted = {"brand": "Unknown", "category": "Unknown", "color": "Unknown"}

                    # Adjust match_score if necessary
                    if match_score < 0.1 and product_details:
                        valid_details = sum(1 for v in product_details.values() if v != "None")
                        matches = sum(
                            1 for k, v in product_details.items()
                            if v != "None" and v.lower() in str(extracted.get(k, "")).lower()
                        )
                        heuristic_score = matches / max(1, valid_details) if valid_details > 0 else 1.0
                        features["match_score"] = max(match_score, heuristic_score)
                        features["reasoning"] = features.get("reasoning", "") + f" Adjusted with heuristic: {heuristic_score:.2f}"

                    logger.info(f"Gemini analysis succeeded (attempt {attempt})")
                    return {"status_code": 200, "success": True, "features": features}
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON from Gemini response: {e}")
                    features = {
                        "description": "Unable to analyze image.",
                        "extracted_features": {"brand": "Unknown", "category": "Unknown", "color": "Unknown"},
                        "match_score": 0.0,
                        "reasoning": f"JSON parsing failed: {e}"
                    }
                    return {"status_code": 200, "success": False, "features": features}
            logger.warning(f"Gemini returned no text (attempt {attempt})")
        except Exception as e:
            logger.error(f"Gemini analysis failed (attempt {attempt}): {e}")
            if attempt == max_retries:
                features = {
                    "description": cv_description or "Unable to analyze image.",
                    "extracted_features": {"brand": "Unknown", "category": "Unknown", "color": "Unknown"},
                    "match_score": 0.5,
                    "reasoning": f"Gemini failed after {max_retries} attempts; using fallback."
                }
                return {"status_code": 200, "success": False, "features": features}
        await asyncio.sleep(2)

    # Fallback if all retries fail
    features = {
        "description": "Analysis failed after retries.",
        "extracted_features": {"brand": "Unknown", "category": "Unknown", "color": "Unknown"},
        "match_score": 0.0,
        "reasoning": "Max retries exceeded."
    }
    return {"status_code": 200, "success": False, "features": features}

async def get_image_data_async(image_urls: List[str], session: aiohttp.ClientSession, logger: logging.Logger = None, retries: int = 3) -> Tuple[Optional[bytes], Optional[str]]:
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
    if not detected_label or not expected_category:
        return False

    # Normalize inputs
    detected_label = detected_label.lower().strip()
    expected_category = expected_category.lower().strip()

    # Direct match
    if detected_label in expected_category:
        return True

    # Mapped label match
    mapped_label = CATEGORY_MAPPING.get(detected_label, detected_label)
    if mapped_label in expected_category:
        return True

    # Synonym-based matching for sneakers
    sneaker_synonyms = [
        "sneaker", "running_shoe", "athletic_shoe", "trainer", "tennis_shoe",
        "sport_shoe", "kick", "gym_shoe", "footwear"
    ]
    if "sneaker" in expected_category and detected_label in sneaker_synonyms:
        logger.info(f"Matched '{detected_label}' to 'sneaker' in category '{expected_category}'")
        return True

    # Hierarchy-based matching
    for parent, children in category_hierarchy_example.items():
        if parent in expected_category and mapped_label in children:
            return True

    # Partial match for multi-word categories
    category_words = expected_category.split()
    for word in category_words:
        if word in [detected_label, mapped_label] and word not in ["men", "women", "leather", "plakka"]:
            logger.info(f"Partial match: '{word}' from category '{expected_category}' matches '{detected_label}'")
            return True

    return False

async def process_image(row, session: aiohttp.ClientSession, logger: Optional[logging.Logger] = None):
    logger = logger or default_logger
    result_id = row.get("ResultID")
    default_result = (result_id, json.dumps({"error": "Unknown processing error"}), None, 1)

    try:
        if result_id is None:
            logger.error(f"Invalid row data: ResultID missing - row: {row}")
            return result_id, json.dumps({"error": "Invalid row data: ResultID missing"}), None, 1

        logger.debug(f"Processing row for ResultID {result_id}: {row}")
        sort_order = row.get("SortOrder")
        if isinstance(sort_order, (int, float)) and sort_order < 0:
            logger.info(f"Skipping ResultID {result_id} due to negative SortOrder: {sort_order}")
            return result_id, json.dumps({"error": f"Negative SortOrder: {sort_order}"}), None, 0

        global category_hierarchy
        if category_hierarchy is None:
            for attempt in range(3):
                try:
                    response = requests.get(category_hierarchy_url, timeout=10)
                    response.raise_for_status()
                    category_hierarchy = response.json()
                    logger.info("Loaded category_hierarchy")
                    break
                except Exception as e:
                    logger.warning(f"Failed to load category_hierarchy (attempt {attempt + 1}): {e}")
                    if attempt == 2:
                        category_hierarchy = category_hierarchy_example
                        logger.info("Using example category_hierarchy")

        image_urls = [row["ImageUrl"]]
        if pd.notna(row.get("ImageUrlThumbnail")):
            image_urls.append(row["ImageUrlThumbnail"])
        product_details = {
            "brand": row.get("ProductBrand"),
            "category": row.get("ProductCategory"),
            "color": row.get("ProductColor")
        }

        image_data, downloaded_url = await get_image_data_async(image_urls, session, logger)
        if not image_data:
            logger.warning(f"Image download failed for ResultID {result_id}")
            return result_id, json.dumps({"error": f"Image download failed for URLs: {image_urls}"}), None, 1

        base64_image = base64.b64encode(image_data).decode("utf-8")
        image = Image.open(BytesIO(image_data)).convert("RGB")

        cv_success, cv_description, person_confidences = await detect_objects_with_computer_vision_async(base64_image, logger)
        if not cv_success and not cv_description:
            logger.warning(f"Computer vision detection failed for ResultID {result_id}: {cv_description}")
            return result_id, json.dumps({"error": cv_description}), None, 1

        fashion_label, fashion_conf = None, 0.0  # Placeholder during training

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

        FASHION_LABELS = [
            "t-shirt", "shirt", "trouser", "dress", "coat", "jacket", "sweater", "pullover",
            "sandal", "sneaker", "shoe", "bag", "backpack", "hat", "scarf", "gloves", "belt",
            "skirt", "shorts", "suit", "tie", "socks", "boots"
        ]

        detected_objects = cv_description.split("\n")[2:] if "Segmented objects:" in cv_description else []
        labels = [label for label in [cls_label, seg_label] if label]
        is_fashion = any(label.lower() in [fl.lower() for fl in FASHION_LABELS] for label in labels if label)
        if len(detected_objects) > 1 and not is_fashion:
            logger.info(f"Discarding image for ResultID {result_id} due to multiple non-fashion objects")
            ai_json = json.dumps({
                "description": "Image contains multiple objects, none of which are fashion-related.",
                "extracted_features": {"brand": "Unknown", "category": "Multiple", "color": "Unknown"},
                "match_score": 0.0,
                "reasoning": "Multiple objects detected, none in fashion categories.",
                "cv_detection": cv_description,
                "person_confidences": person_confidences
            })
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("UPDATE utb_ImageScraperResult SET SortOrder = -6, AiJson = ? WHERE ResultID = ?", (ai_json, result_id))
                conn.commit()
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
                "person_confidences": person_confidences
            })
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("UPDATE utb_ImageScraperResult SET SortOrder = -6, AiJson = ? WHERE ResultID = ?", (ai_json, result_id))
                conn.commit()
            return result_id, ai_json, cv_description, 0

        expected_category = product_details.get("category", "").lower()
        if cls_label and not is_related_to_category(cls_label, expected_category):
            logger.warning(f"Detected label '{cls_label}' not related to category '{expected_category}'")
            cv_description += f"\nWarning: Detected classification may be irrelevant to category '{expected_category}'."
        if seg_label and not is_related_to_category(seg_label, expected_category):
            logger.warning(f"Detected label '{seg_label}' not related to category '{expected_category}'")
            cv_description += f"\nWarning: Detected segmentation may be irrelevant to category '{expected_category}'."

        gemini_result = await analyze_image_with_gemini_async(base64_image, product_details, logger=logger, cv_description=cv_description)
        if not gemini_result["success"]:
            logger.warning(f"Gemini analysis failed for ResultID {result_id}: {gemini_result.get('features', {}).get('reasoning', 'No details')}")
        features = gemini_result.get("features", {
            "description": cv_description,
            "extracted_features": {"brand": "Unknown", "category": "Unknown", "color": "Unknown"},
            "match_score": 0.5,
            "reasoning": "Gemini analysis failed; using computer vision description with default score."
        })

        description = features.get("description", cv_description)
        extracted_features = features.get("extracted_features", {})
        match_score = features.get("match_score", 0.5)
        reasoning = features.get("reasoning", "No reasoning provided")

        raw_category = (product_details.get("category") or "").strip().lower() or extracted_features.get("category", "").lower()
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
            "person_confidences": person_confidences
        })
        ai_caption = description
        logger.info(f"Processed ResultID {result_id} successfully")
        return result_id, ai_json, ai_caption, 1

    except Exception as e:
        logger.error(f"Unexpected error in process_image for ResultID {result_id}: {str(e)}", exc_info=True)
        return default_result

async def process_entry(
    file_id: int,
    entry_id: int,
    entry_df: pd.DataFrame,
    logger: logging.Logger
) -> List[Tuple[int, str, Optional[str], int]]:
    """Process a single EntryID: update sort order and perform AI analysis."""
    from database import update_search_sort_order

    try:
        logger.info(f"Starting task for EntryID: {entry_id} with {len(entry_df)} rows")

        # Fetch attributes for this EntryID
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT ProductBrand, ProductModel, ProductColor, ProductCategory
                FROM utb_ImageScraperRecords
                WHERE FileID = ? AND EntryID = ?
            """, (file_id, entry_id))
            result = cursor.fetchone()
            if result:
                product_brand, product_model, product_color, product_category = result
                logger.info(f"Fetched attributes for EntryID: {entry_id} - "
                            f"Brand: {product_brand}, Model: {product_model}, "
                            f"Color: {product_color}, Category: {product_category}")
            else:
                logger.warning(f"No attributes found for FileID: {file_id}, EntryID: {entry_id}")
                product_brand = product_model = product_color = product_category = ''

        # Update sort order
        await asyncio.get_event_loop().run_in_executor(
            None,
            update_search_sort_order,
            file_id,
            str(entry_id),
            product_brand,
            product_model,
            product_color,
            product_category,
            logger
        )
        logger.info(f"Updated sort order for FileID: {file_id}, EntryID: {entry_id}")

        # Process images with AI analysis
        async with aiohttp.ClientSession() as session:
            logger.info(f"Created aiohttp.ClientSession for EntryID: {entry_id}")
            updates = []
            for _, row in entry_df.iterrows():
                result = await process_image(row, session, logger)
                if result is None:
                    logger.error(f"process_image returned None for row: {row}")
                    updates.append((row.get("ResultID"), json.dumps({"error": "process_image returned None"}), None, 1))
                else:
                    result_id, ai_json, ai_caption, image_is_fashion = result
                    updates.append((result_id, ai_json, ai_caption, image_is_fashion))

        logger.info(f"Completed task for EntryID: {entry_id}")
        return updates

    except Exception as e:
        logger.error(f"Error processing EntryID {entry_id}: {e}", exc_info=True)
        return [(row.get("ResultID"), json.dumps({"error": f"Entry processing error: {str(e)}"}), None, 1) for _, row in entry_df.iterrows()]

@ray.remote(num_cpus=1)
def process_entry_remote(
    file_id: int,
    entry_id: int,
    entry_df: pd.DataFrame,
    logger: logging.Logger
) -> List[Tuple[int, str, Optional[str], int]]:
    """Ray task to process a single EntryID."""
    return asyncio.run(process_entry(file_id, entry_id, entry_df, logger))

import asyncio
import logging
import os
import pyodbc
import pandas as pd
from typing import Optional, Dict, Set
from concurrent.futures import ThreadPoolExecutor
import ray

async def batch_process_images(
    file_id: str,
    entry_ids: Optional[List[int]] = None,
    step: int = 0,
    limit: int = 5000,
    concurrency: int = 10,
    logger: Optional[logging.Logger] = None
) -> None:
    """
    Process images for a given FileID and optional EntryIDs using Ray for parallelization, updating sort order per EntryID.

    Args:
        file_id: The ID of the file to process.
        entry_ids: Optional list of EntryIDs to process; if None, process all.
        step: The retry step (used for logging or retry logic).
        limit: Maximum number of images to fetch.
        concurrency: Maximum number of concurrent Ray tasks (default 10).
        logger: Optional logger instance.
    """
    logger = logger or default_logger
    from database import fetch_missing_images, set_sort_order_negative_four_for_zero_match, update_search_sort_order

    try:
        file_id = int(file_id)
        logger.info(f"📷 Starting batch image processing for FileID: {file_id}, Step: {step}, Limit: {limit}, Concurrency: {concurrency}, EntryIDs: {entry_ids or 'All'}")

        # Fetch missing images
        df = await asyncio.get_event_loop().run_in_executor(None, fetch_missing_images, file_id, limit, True, logger)
        if df.empty:
            logger.warning(f"⚠️ No missing images found for FileID: {file_id}")
            return

        # Filter by EntryIDs if provided
        if entry_ids is not None:
            df = df[df['EntryID'].isin(entry_ids)]
            if df.empty:
                logger.warning(f"⚠️ No missing images found for specified EntryIDs: {entry_ids}")
                return

        # Drop unnecessary columns
        columns_to_drop = ['Step1', 'Step2', 'Step3', 'Step4', 'CreateTime_1', 'CreateTime_2']
        df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
        logger.info(f"Dropped columns: {columns_to_drop}")
        logger.info(f"Retrieved {len(df)} image rows for FileID: {file_id}")
        entry_ids_to_process = list(df.groupby('EntryID').groups.keys())
        logger.info(f"Processing {len(entry_ids_to_process)} EntryIDs")

        # Process images with Ray, limiting to 'concurrency' tasks
        semaphore = asyncio.Semaphore(concurrency)
        futures = []

        async def submit_task(entry_id, entry_df):
            async with semaphore:
                logger.info(f"Submitting Ray task for EntryID: {entry_id}")
                future = process_entry_remote.remote(file_id, entry_id, entry_df, logger)
                futures.append(future)

        # Submit tasks
        tasks = [submit_task(entry_id, df[df['EntryID'] == entry_id]) for entry_id in entry_ids_to_process]
        logger.info(f"Waiting for {len(tasks)} Ray tasks to complete")
        await asyncio.gather(*tasks)

        # Collect results
        results = ray.get(futures)
        logger.info(f"Finished waiting for all Ray tasks")
        valid_updates = []
        for updates in results:
            valid_updates.extend(updates)

        # Update database
        if valid_updates:
            try:
                with pyodbc.connect(conn_str) as conn:
                    cursor = conn.cursor()
                    query = "UPDATE utb_ImageScraperResult SET AiJson = ?, ImageIsFashion = ?, AiCaption = ? WHERE ResultID = ?"
                    cursor.executemany(query, valid_updates)
                    conn.commit()
                    logger.info(f"Updated {len(valid_updates)} records in a single transaction")
            except pyodbc.Error as e:
                logger.error(f"Database error: {e}")

        # Update sort order for each EntryID
        for entry_id in entry_ids_to_process:
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT ProductBrand, ProductModel, ProductColor, ProductCategory
                    FROM utb_ImageScraperRecords
                    WHERE FileID = ? AND EntryID = ?
                """, (file_id, entry_id))
                result = cursor.fetchone()
                if result:
                    product_brand, product_model, product_color, product_category = result
                else:
                    logger.warning(f"No attributes found for FileID: {file_id}, EntryID: {entry_id}")
                    product_brand = product_model = product_color = product_category = ''
            
            await asyncio.get_event_loop().run_in_executor(
                None,
                update_search_sort_order,
                file_id,
                str(entry_id),
                product_brand,
                product_model,
                product_color,
                product_category,
                logger
            )
            logger.info(f"Updated sort order for FileID: {file_id}, EntryID: {entry_id}")

    except Exception as e:
        logger.error(f"🔴 Error in batch_process_images for FileID {file_id}: {e}", exc_info=True)
        raise

async def process_restart_batch(
    file_id_db: int,
    max_retries: int = 15,
    logger: Optional[logging.Logger] = None
) -> Dict[str, str]:
    logger = logger or default_logger
    log_filename = logger.handlers[0].baseFilename if logger.handlers else os.path.join(os.getcwd(), 'logs', f"file_{file_id_db}.log")

    async def validate_db_results(entry_ids: set, conn, logger: logging.Logger) -> set:
        """Validate which entries have fewer than 50 records with SortOrder > 0."""
        cursor = conn.cursor()
        unresolved_entries = set()
        for entry_id in entry_ids:
            try:
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM utb_ImageScraperResult 
                    WHERE EntryID = ? AND SortOrder > 0
                """, (entry_id,))
                count = cursor.fetchone()[0]
                logger.debug(f"EntryID {entry_id}: {count} records with SortOrder > 0")
                if count < 50:
                    unresolved_entries.add(entry_id)
                else:
                    logger.info(f"EntryID {entry_id} resolved with {count} valid records")
            except pyodbc.Error as e:
                logger.error(f"Database error validating EntryID {entry_id}: {e}", exc_info=True)
                unresolved_entries.add(entry_id)  # Conservatively assume unresolved on error
        logger.info(f"Validated database: {len(entry_ids) - len(unresolved_entries)} entries resolved, {len(unresolved_entries)} remain unresolved")
        if unresolved_entries:
            logger.debug(f"Unresolved EntryIDs: {unresolved_entries}")
        return unresolved_entries

    try:
        logger.info(f"🔁 Starting concurrent search processing for FileID: {file_id_db}")
        file_id_db = int(file_id_db)

        # Fetch brand rules
        brand_rules = await fetch_brand_rules(BRAND_RULES_URL, logger=logger)
        if not brand_rules:
            logger.warning("⚠️ Failed to load brand rules; using default logic")
            brand_rules = {"brand_rules": []}

        # Fetch entries
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT EntryID, ProductModel, ProductBrand, ProductColor, ProductCategory FROM utb_ImageScraperRecords WHERE FileID = ?", (file_id_db,))
            entries = [(row[0], row[1], row[2], row[3], row[4]) for row in cursor.fetchall()]
            logger.info(f"📋 Found {len(entries)} entries for FileID: {file_id_db}")

        if not entries:
            logger.warning(f"⚠️ No entries found for FileID: {file_id_db}")
            return {"error": f"No entries found for FileID {file_id_db}"}

        # Prepare batch
        batch = [
            {
                "EntryID": entry_id,
                "SearchString": search_string,
                "SearchTypes": ["default", "brand_format", "brand_name", "retry_with_alternative"],
                "ProductBrand": brand,
                "ProductColor": color,
                "ProductCategory": category
            }
            for entry_id, search_string, brand, color, category in entries if search_string
        ]
        logger.info(f"📦 Prepared batch with {len(batch)} valid entries")

        # Process entries with retries
        entries_to_process = set(entry_id for entry_id, _, _, _, _ in entries)
        search_types = ["default", "brand_format", "brand_name", "retry_with_alternative"]
        max_retries = min(max_retries, len(search_types))
        batch_size = 100

        with pyodbc.connect(conn_str) as conn:
            for retry in range(max_retries):
                if not entries_to_process:
                    logger.info("🎉 All entries have sufficient valid records")
                    break

                # Validate entries against database
                entries_to_process = await validate_db_results(entries_to_process, conn, logger)
                logger.info(f"🔄 Retry {retry + 1}/{max_retries}: Processing {len(entries_to_process)} entries with search type: {search_types[retry]}")

                # Filter batch to only include entries that still need processing
                current_batch = [
                    entry for entry in batch if entry["EntryID"] in entries_to_process
                ]
                if not current_batch:
                    logger.info("No entries left to process in this retry")
                    break

                # Update SearchTypes for this retry
                for entry in current_batch:
                    entry["SearchTypes"] = [search_types[retry]]

                # Process in smaller sub-batches
                for i in range(0, len(current_batch), batch_size):
                    sub_batch = current_batch[i:i + batch_size]
                    logger.info(f"Processing sub-batch {i // batch_size + 1} with {len(sub_batch)} entries")

                    # Submit batch to Ray
                    results_ref = process_batch.remote(sub_batch, brand_rules, logger=logger)
                    results = ray.get(results_ref)

                    # Process results per entry
                    processed_entry_ids = []
                    for res in results:
                        entry_id = res["entry_id"]
                        if res["status"] == "success" and res["result"] is not None:
                            result_df = res["result"]
                            if not result_df.empty:
                                max_insert_attempts = 3
                                for attempt in range(max_insert_attempts):
                                    logger.debug(f"Insertion attempt {attempt + 1}/{max_insert_attempts} for EntryID {entry_id}")
                                    insert_success = await asyncio.get_running_loop().run_in_executor(
                                        None, insert_search_results, result_df, logger
                                    )
                                    if insert_success:
                                        logger.info(f"✅ Inserted {len(result_df)} results for EntryID {entry_id}")
                                        processed_entry_ids.append(entry_id)
                                        break
                                    else:
                                        logger.error(f"❌ Insertion failed for EntryID {entry_id} on attempt {attempt + 1}")
                                        if attempt < max_insert_attempts - 1:
                                            await asyncio.sleep(1)
                                if not insert_success:
                                    logger.error(f"❌ All insertion attempts failed for EntryID {entry_id}")
                            else:
                                logger.debug(f"⚠️ Empty results for EntryID {entry_id}")
                        else:
                            logger.debug(f"⚠️ No valid results for EntryID {entry_id} in retry {retry + 1}")

                    # Commit after each sub-batch
                    conn.commit()
                    logger.debug(f"Committed database changes after sub-batch {i // batch_size + 1}")

                    # Perform AI analysis and sort order update for processed entries
                    if processed_entry_ids:
                        await batch_process_images(
                            file_id_db,
                            entry_ids=processed_entry_ids,
                            step=retry,
                            limit=5000,
                            concurrency=10,
                            logger=logger
                        )
                        logger.info(f"🏁 Completed AI analysis for {len(processed_entry_ids)} entries after search type: {search_types[retry]}")

                    # Re-validate entries to check if any are now resolved
                    entries_to_process = await validate_db_results(entries_to_process, conn, logger)

            # Fallback to GCloud for remaining entries
            if entries_to_process:
                logger.info(f"⚠️ Falling back to GCloud for {len(entries_to_process)} unresolved entries")
                gcloud_batch = [
                    entry for entry in batch if entry["EntryID"] in entries_to_process
                ]

                # Process GCloud batch in parallel
                for i in range(0, len(gcloud_batch), batch_size):
                    sub_batch = gcloud_batch[i:i + batch_size]
                    tasks = []
                    for entry in sub_batch:
                        entry_id = entry["EntryID"]
                        search_string = entry["SearchString"]
                        tasks.append(
                            asyncio.get_running_loop().run_in_executor(
                                None,
                                lambda: process_search_row_gcloud(
                                    search_string, entry_id, logger, remaining_retries=3
                                )
                            )
                        )

                    # Gather GCloud results
                    gcloud_results = await asyncio.gather(*tasks, return_exceptions=True)
                    processed_entry_ids = []
                    for entry_id, result in zip([entry["EntryID"] for entry in sub_batch], gcloud_results):
                        if isinstance(result, Exception):
                            logger.error(f"🔴 GCloud error for EntryID {entry_id}: {result}")
                            continue
                        if isinstance(result, pd.DataFrame) and not result.empty:
                            max_insert_attempts = 3
                            for attempt in range(max_insert_attempts):
                                insert_success = await asyncio.get_running_loop().run_in_executor(
                                    None, insert_search_results, result, logger
                                )
                                if insert_success:
                                    logger.info(f"✅ Inserted {len(result)} GCloud results for EntryID {entry_id}")
                                    processed_entry_ids.append(entry_id)
                                    break
                                else:
                                    logger.error(f"❌ GCloud insertion failed for EntryID {entry_id} on attempt {attempt + 1}")
                                    if attempt < max_insert_attempts - 1:
                                        await asyncio.sleep(1)
                            if not insert_success:
                                logger.error(f"❌ All GCloud insertion attempts failed for EntryID {entry_id}")
                        else:
                            logger.debug(f"⚠️ No valid GCloud results for EntryID {entry_id}")

                    # Commit after each GCloud sub-batch
                    conn.commit()
                    logger.debug(f"Committed database changes after GCloud sub-batch {i // batch_size + 1}")

                    # Perform AI analysis and sort order update for processed entries
                    if processed_entry_ids:
                        await batch_process_images(
                            file_id_db,
                            entry_ids=processed_entry_ids,
                            step=retry + 1,
                            limit=5000,
                            concurrency=10,
                            logger=logger
                        )
                        logger.info(f"🏁 Completed AI analysis for {len(processed_entry_ids)} entries after GCloud batch {i // batch_size + 1}")

                    # Re-validate entries
                    entries_to_process = await validate_db_results(entries_to_process, conn, logger)

            # Mark unresolved entries with SortOrder -1
            if entries_to_process:
                logger.warning(f"⚠️ {len(entries_to_process)} entries unresolved; setting SortOrder to -1")
                cursor = conn.cursor()
                unresolved_entries = [(entry["EntryID"], entry["SearchString"]) for entry in batch if entry["EntryID"] in entries_to_process]
                logger.warning(f"⚠️ Unresolved entries: {unresolved_entries}")
                for entry_id in entries_to_process:
                    cursor.execute("""
                        UPDATE utb_ImageScraperResult 
                        SET SortOrder = -1 
                        WHERE EntryID = ? AND (SortOrder IS NULL OR SortOrder <= 0)
                    """, (entry_id,))
                conn.commit()

        total_processed = len(entries)
        successful_entries = total_processed - len(entries_to_process)
        logger.info(f"✅ Completed processing for FileID: {file_id_db}. {successful_entries}/{total_processed} entries with sufficient valid records")
        return {"message": "Search processing completed successfully", "file_id": str(file_id_db)}

    except Exception as e:
        logger.error(f"🔴 Error processing FileID {file_id_db}: {e}", exc_info=True)
        loop = asyncio.get_running_loop()
        if os.path.exists(log_filename):
            upload_url = await loop.run_in_executor(
                ThreadPoolExecutor(),
                upload_file_to_space,
                log_filename,
                f"job_logs/job_{file_id_db}.log",
                True,
                logger,
                file_id_db
            )
            logger.info(f"📤 Log file uploaded to: {upload_url}")
            await loop.run_in_executor(None, update_log_url_in_db, file_id_db, upload_url, logger)

        send_to_email_addr = await loop.run_in_executor(None, get_send_to_email, file_id_db, logger)
        if send_to_email_addr:
            file_name = f"FileID: {file_id_db}"
            await loop.run_in_executor(
                None,
                lambda: send_message_email(
                    send_to_email_addr,
                    f"Error processing {file_name}",
                    f"An error occurred while processing your file: {str(e)}"
                )
            )
        return {"error": str(e)}