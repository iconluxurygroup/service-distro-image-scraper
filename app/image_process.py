import asyncio
import base64
import json
import time
import logging
import numpy as np
import pandas as pd
import requests
from io import BytesIO
from skimage import io as skio
from skimage.metrics import structural_similarity as ssim
from skimage.transform import resize
from typing import Dict, Optional, List, Tuple, Union
from database import fetch_missing_images, update_search_sort_order, update_sort_order_based_on_match_score  # Assume these are defined
import google.generativeai as genai
from config import GOOGLE_API_KEY, conn_str  # Assume these are defined
from logging_config import root_logger  # Assume logging setup is imported
from PIL import Image
import aiohttp
import urllib.parse
import re
import matplotlib.pyplot as plt
import asyncio
from typing import Optional, Tuple
import numpy as np
from PIL import Image
from transformers import pipeline
import logging
import base64
from io import BytesIO
import torch

# Set device based on availability
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
BATCH_SIZE = 2  # Adjust based on your hardware

async def classify_image_with_resnet50_async(image_base64: str, logger: Optional[logging.Logger] = None) -> Tuple[bool, Optional[str]]:
    logger = logger or logging.getLogger(__name__)
    classifier = pipeline("image-classification", model="microsoft/resnet-50", device=DEVICE)

    try:
        # Decode base64 image
        image_bytes = base64.b64decode(image_base64)
        image = Image.open(BytesIO(image_bytes)).convert("RGB")
    except Exception as e:
        logger.error(f"Invalid base64 string or image loading failed: {e}")
        return False, f"Error: Invalid base64 string or image loading failed: {e}"

    try:
        # Run ResNet-50 classification
        outputs = classifier(image, top_k=1, batch_size=BATCH_SIZE)
        top_result = outputs[0]
        label = top_result["label"].lower().replace("_", " ")
        score = top_result["score"]
        description = f"A {label} detected with confidence {score:.2f}."
        logger.info(f"ResNet-50 classification: {description}")
        return True, description
    except Exception as e:
        logger.error(f"ResNet-50 classification failed: {e}")
        return False, f"Error: ResNet-50 classification failed: {e}"
# Default logger setup
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Category hierarchy

category_hierarchy_url = "https://raw.githubusercontent.com/iconluxurygroup/settings-static-data/refs/heads/main/category_hierarchy.json"
json_url = "https://raw.githubusercontent.com/iconluxurygroup/settings-static-data/refs/heads/main/optimal-references.json"

# Fetch JSON data with retry logic
def fetch_json(url, max_attempts=3, timeout=10):
    for attempt in range(max_attempts):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as e:
            print(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
            if attempt < max_attempts - 1:
                time.sleep(2)
            else:
                print(f"Failed to fetch JSON after {max_attempts} attempts.")
                return None
loaded_references = {}
optimal_references = fetch_json(json_url)
logger = default_logger
category_hierarchy = fetch_json(category_hierarchy_url)

# Validate category_hierarchy
if category_hierarchy is None:
    logger.critical("category_hierarchy is None; using fallback empty hierarchy.")
    category_hierarchy = {}
elif not isinstance(category_hierarchy, dict):
    logger.critical(f"category_hierarchy is invalid: {type(category_hierarchy)}; using fallback empty hierarchy.")
    category_hierarchy = {}
else:
    logger.info("Successfully loaded category_hierarchy.")
if optimal_references:
    logger.info("Successfully fetched JSON data:")
    logger.info(json.dumps(optimal_references, indent=2))
    for category, url in optimal_references.items():
        for attempt in range(3):
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                image_data = BytesIO(response.content)
                img = skio.imread(image_data, as_gray=True)
                if img.size > 0:
                    loaded_references[category] = img
                    logger.info(f"Loaded image for '{category}' from {url}, shape={img.shape}")
                    plt.imsave(f"preloaded_{category}.png", img, cmap='gray')
                    logger.info(f"Saved preloaded image as 'preloaded_{category}.png'")
                    break
            except (requests.RequestException, ValueError) as e:
                logger.warning(f"Attempt {attempt + 1} failed for '{category}' ({url}): {str(e)}")
                if attempt == 2:
                    logger.error(f"Failed to load image for '{category}' after 3 attempts.")
else:
    logger.error("Could not fetch JSON data from the provided URL.")

# Category validation functions
def validate(category: str) -> bool:
    category = category.lower().strip()
    return category in category_hierarchy or any(category in sublist for sublist in category_hierarchy.values())

def are_categories_related(category1: str, category2: str) -> bool:
    category1 = category1.lower().strip()
    category2 = category2.lower().strip()
    if category1 == category2:
        return True
    for parent, children in category_hierarchy.items():
        if category1 in children and category2 in children:
            return True
        if (category1 == parent and category2 in children) or (category2 == parent and category1 in children):
            return True
    return False

# Asynchronous image downloading
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
                logger.info(f"Attempting to download image from {decoded_url} (attempt {attempt})")
                async with session.get(decoded_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    response.raise_for_status()
                    image_data = await response.read()
                    logger.info(f"Downloaded image from {decoded_url} on attempt {attempt}")
                    return image_data, decoded_url
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.error(f"Attempt {attempt} failed for {decoded_url}: {e}")
                if attempt < retries:
                    await asyncio.sleep(1)
        logger.warning(f"All {retries} attempts failed for {decoded_url}")
    logger.warning(f"All URLs failed: {image_urls}")
    return None, None

# Image preparation for SSIM
def prepare_image_for_ssim(image_data, target_shape=(512, 512), logger=None):
    logger = logger or logging.getLogger(__name__)
    try:
        with Image.open(BytesIO(image_data)) as img:
            img.verify()
        with Image.open(BytesIO(image_data)) as img:
            if img.mode != 'L':
                img = img.convert('L')
            img_array = np.array(img, dtype=np.float64) / 255.0

        if img_array.size == 0 or img_array.ndim != 2 or np.std(img_array) < 0.01:
            logger.error("Invalid or uniform image")
            return None, None

        orig_shape = img_array.shape
        target_h, target_w = target_shape
        orig_h, orig_w = orig_shape
        scale = min(target_h / orig_h, target_w / orig_w)
        new_h, new_w = int(orig_h * scale), int(orig_w * scale)
        img_array = resize(img_array, (new_h, new_w), anti_aliasing=True, preserve_range=True, order=1)

        pad_height = target_h - new_h
        pad_width = target_w - new_w
        pad_top = pad_height // 2
        pad_bottom = pad_height - pad_top
        pad_left = pad_width // 2
        pad_right = pad_width - pad_left
        padded = np.pad(img_array, ((pad_top, pad_bottom), (pad_left, pad_right)), mode='constant', constant_values=0)
        logger.info(f"Prepared image: {orig_shape} -> {padded.shape}")
        return padded, orig_shape
    except Exception as e:
        logger.error(f"Image preparation failed: {e}")
        return None, None

def prepare_reference_for_ssim(reference_image, target_shape=(512, 512), logger=None):
    logger = logger or logging.getLogger(__name__)
    try:
        ref_prepared = reference_image.astype(np.float64)
        if ref_prepared.size == 0 or ref_prepared.ndim < 2 or np.std(ref_prepared) < 0.01:
            logger.error("Invalid or uniform reference image")
            return None, None
        if ref_prepared.ndim > 2:
            ref_prepared = np.mean(ref_prepared, axis=2)
        if ref_prepared.max() > 1.0:
            ref_prepared /= 255.0

        orig_shape = ref_prepared.shape
        targ_h, targ_w = target_shape
        orig_h, orig_w = orig_shape
        scale = min(targ_h / orig_h, targ_w / orig_w)
        new_h, new_w = int(orig_h * scale), int(orig_w * scale)
        ref_prepared = resize(ref_prepared, (new_h, new_w), anti_aliasing=True, preserve_range=True, order=1)

        pad_height = targ_h - new_h
        pad_width = targ_w - new_w
        pad_top = pad_height // 2
        pad_bottom = pad_height - pad_top
        pad_left = pad_width // 2
        pad_right = pad_width - pad_left
        ref_prepared = np.pad(ref_prepared, ((pad_top, pad_bottom), (pad_left, pad_right)), mode='constant', constant_values=0)
        logger.info(f"Prepared reference: {orig_shape} -> {ref_prepared.shape}")
        return ref_prepared, orig_shape
    except Exception as e:
        logger.error(f"Reference preparation failed: {e}")
        return None, None

async def calculate_ssim_async(image_data: bytes, reference_image: np.ndarray, logger: Optional[logging.Logger] = None) -> Tuple[float, List[str], Dict[str, Union[str, float, np.ndarray]]]:
    logger = logger or logging.getLogger(__name__)
    debug_info = {"steps": []}
    target_shape = (512, 512)

    img_prepared, img_orig_shape = prepare_image_for_ssim(image_data, target_shape=target_shape, logger=logger)
    if img_prepared is None:
        logger.error("Input image preparation failed")
        debug_info["steps"].append("Input preparation failed")
        return -1, [], debug_info

    ref_prepared, ref_orig_shape = prepare_reference_for_ssim(reference_image, target_shape=target_shape, logger=logger)
    if ref_prepared is None:
        logger.error("Reference image preparation failed")
        debug_info["steps"].append("Reference preparation failed")
        return -1, [], debug_info

    if img_prepared.shape != ref_prepared.shape:
        logger.error(f"Shape mismatch: input={img_prepared.shape}, reference={ref_prepared.shape}")
        debug_info["steps"].append(f"Shape mismatch: input={img_prepared.shape}, reference={ref_prepared.shape}")
        return -1, [], debug_info

    used_references = ["original"]
    try:
        score = ssim(img_prepared, ref_prepared, data_range=1.0, gaussian_weights=True, sigma=1.5)
        logger.info(f"SSIM score: {score:.4f}")
        debug_info["steps"].append(f"SSIM: {score:.4f}")
    except Exception as e:
        logger.warning(f"SSIM calculation failed: {e}")
        debug_info["steps"].append(f"SSIM calculation failed: {e}")
        return -1, used_references, debug_info

    final_score = min(max(score, 0.0), 1.0)
    debug_info["best_score"] = final_score
    return final_score, used_references, debug_info

async def fetch_reference_image(category: str, session: aiohttp.ClientSession, logger: Optional[logging.Logger] = None) -> Optional[np.ndarray]:
    logger = logger or default_logger
    category = category.lower().strip()
    if category in loaded_references:
        logger.info(f"Using preloaded reference for '{category}', shape={loaded_references[category].shape}")
        return loaded_references[category]
    for parent, children in category_hierarchy.items():
        if category in children and parent in loaded_references:
            logger.info(f"Using preloaded parent reference '{parent}' for '{category}'")
            return loaded_references[parent]
    logger.info(f"No preloaded reference for '{category}', fetching from Google Images")
    search_url = f"https://www.google.com/search?q={category}+site:vitkac.com&tbm=isch"
    try:
        async with session.get(search_url) as response:
            html_bytes = await response.read()
        html_str = html_bytes.decode('utf-8', errors='replace')
        pattern = r'"(https://encrypted-tbn0.gstatic.com/images\?[^"]+)"'
        image_urls = re.findall(pattern, html_str)
        if not image_urls:
            logger.error(f"No image URLs found for '{category}'")
            return None
        for url in image_urls[:3]:
            image_data, downloaded_url = await get_image_data_async([url], session, logger)
            if image_data:
                try:
                    ref_img = skio.imread(BytesIO(image_data), as_gray=True)
                    if ref_img.size > 0 and min(ref_img.shape) >= 100:
                        ref_img_resized = resize(ref_img, (256, 256), anti_aliasing=True, preserve_range=True, mode='reflect', order=3)
                        if ref_img_resized.max() > 1.0:
                            ref_img_resized = ref_img_resized / 255.0
                        loaded_references[category] = ref_img_resized
                        logger.info(f"Fetched and cached reference for '{category}' from {downloaded_url}, shape={ref_img_resized.shape}")
                        plt.imsave(f"reference_{category}.png", ref_img_resized, cmap='gray')
                        logger.info(f"Saved fetched reference as 'reference_{category}.png'")
                        return ref_img_resized
                except Exception as e:
                    logger.warning(f"Failed to process image for '{category}' from {url}: {e}")
        logger.error(f"No suitable reference image found for '{category}'")
        return None
    except Exception as e:
        logger.error(f"Failed to fetch reference for '{category}': {e}")
        return None

async def analyze_image_with_gemini_async(image_base64: str, product_details: Optional[Dict[str, str]] = None, api_key: str = GOOGLE_API_KEY, model_name: str = "gemini-2.0-flash", mime_type: str = "image/jpeg", logger: Optional[logging.Logger] = None, resnet_description: Optional[str] = None) -> Dict[str, Optional[Union[str, int, bool, dict]]]:
    logger = logger or default_logger
    if not api_key:
        logger.error("No API key provided for Gemini")
        return {"status_code": None, "success": False, "text": "No API key provided"}
    try:
        image_bytes = base64.b64decode(image_base64)
    except Exception as e:
        logger.error(f"Invalid base64 string: {e}")
        return {"status_code": None, "success": False, "text": f"Invalid base64 string: {e}"}
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(model_name=model_name)
    brand = product_details.get("brand", "None") if product_details and product_details.get("brand") else "None"
    category = product_details.get("category", "None") if product_details and product_details.get("category") else "None"
    color = product_details.get("color", "None") if product_details and product_details.get("color") else "None"
    resnet_info = resnet_description if resnet_description else "No prior classification available."
    
    prompt = f"""
Analyze this image and provide the following information in JSON format:
{{
  "description": "A detailed description of the image in one sentence. Extract brand name, category, color, and composition.",
  "extracted_features": {{
    "brand": "Extracted brand name from the image, if any.",
    "category": "Extracted category of the product, if identifiable.",
    "color": "Primary color of the product.",
    "composition": "Any composition details visible in the image."
  }},
  "match_score": "A score between 0 and 1 indicating how well the image matches the provided details, considering semantic similarity.",
  "reasoning": "A brief explanation of the match score, mentioning which features match or mismatch."
}}
Provided details for matching:
- Brand: {brand}
- Category: {category}
- Color: {color}
Prior classification from ResNet-50: {resnet_info}
Use the ResNet-50 classification as a starting point to refine your analysis.
For each provided detail, compare it with the corresponding extracted feature.
Consider semantic similarity (e.g., 'shirt' and 't-shirt' are related, 'navy' and 'dark blue' are similar).
If a detail is 'None' or empty, do not consider it in the matching.
If all details are 'None' or empty, set match_score to 1.0 with reasoning "No user details provided, assuming match."
Ensure the response is a valid JSON object. Return only the JSON object, no additional text.
"""
    contents = [{"mime_type": mime_type, "data": image_bytes}, prompt]
    try:
        response = await model.generate_content_async(contents, generation_config=genai.types.GenerationConfig(response_mime_type="application/json"))
        response_text = getattr(response, "text", "") or ""
        if response_text:
            features = json.loads(response_text)
            logger.info("Image analysis and matching successful")
            return {"status_code": 200, "success": True, "features": features}
        logger.warning("Gemini returned no text")
        return {"status_code": 200, "success": False, "text": "No analysis text returned"}
    except Exception as e:
        logger.error(f"Gemini analysis failed: {e}")
        return {"status_code": None, "success": False, "text": f"Gemini analysis error: {e}"}

async def process_image(row, session: aiohttp.ClientSession, logger: Optional[logging.Logger] = None):
    logger = logger or default_logger
    result_id = row.get("ResultID")
    
    # Default return values in case of total failure
    default_result = (result_id, json.dumps({"error": "Unknown processing error"}), -1, None, 1)
    
    try:
        if result_id is None:
            logger.error(f"Invalid row data: ResultID missing - row: {row}")
            return result_id, json.dumps({"error": "Invalid row data: ResultID missing"}), -1, None, 1

        logger.debug(f"Processing row for ResultID {result_id}: {row}")
        sort_order = row.get("SortOrder")
        if isinstance(sort_order, (int, float)) and sort_order < 0:
            logger.info(f"Skipping ResultID {result_id} due to negative SortOrder: {sort_order}")
            return result_id, json.dumps({"error": f"Negative SortOrder: {sort_order}"}), -1, None, 0

        image_urls = [row["ImageUrl"]]
        if pd.notna(row.get("ImageUrlThumbnail")):
            image_urls.append(row["ImageUrlThumbnail"])
        product_details = {
            "brand": row.get("ProductBrand"),
            "category": row.get("ProductCategory"),
            "color": row.get("ProductColor")
        }

        logger.debug(f"Downloading image for ResultID {result_id} from URLs: {image_urls}")
        image_data, downloaded_url = await get_image_data_async(image_urls, session, logger)
        if not image_data:
            logger.warning(f"Image download failed for ResultID {result_id}")
            return result_id, json.dumps({"error": f"Image download failed for URLs: {image_urls}"}), -1, None, 1

        base64_image = base64.b64encode(image_data).decode("utf-8")

        # Step 1: Classify with ResNet-50
        logger.debug(f"Classifying image with ResNet-50 for ResultID {result_id}")
        resnet_success, resnet_description = await classify_image_with_resnet50_async(base64_image, logger)
        if not resnet_success:
            logger.warning(f"ResNet-50 classification failed for ResultID {result_id}: {resnet_description}")
            return result_id, json.dumps({"error": resnet_description}), -1, None, 1

        # Step 2: Pass ResNet-50 description to Gemini
        logger.debug(f"Analyzing image with Gemini for ResultID {result_id}")
        gemini_result = await analyze_image_with_gemini_async(base64_image, product_details=product_details, logger=logger, resnet_description=resnet_description)
        if not gemini_result["success"] or not isinstance(gemini_result.get("features"), dict):
            logger.warning(f"Gemini analysis failed for ResultID {result_id}: {gemini_result.get('text', 'No details')}")
            features = gemini_result.get("features", {
                "description": resnet_description,
                "extracted_features": {"brand": "Unknown", "category": "Unknown", "color": "Unknown", "composition": "Unknown"},
                "match_score": 0.0,
                "reasoning": "Gemini analysis failed; using ResNet-50 description."
            })
        else:
            features = gemini_result["features"]
            logger.info(f"Gemini analysis succeeded for ResultID {result_id}")

        description = features.get("description", resnet_description)
        extracted_features = features.get("extracted_features", {})
        match_score = features.get("match_score", 0.0)
        reasoning = features.get("reasoning", "No reasoning provided")

        raw_category = (product_details.get("category") or "").strip().lower() or extracted_features.get("category", "").lower()
        if not raw_category:
            logger.warning(f"No category provided or extracted for ResultID {result_id}, defaulting to 'unknown'")
            normalized_category = "unknown"
        else:
            category_parts = raw_category.split()
            base_candidates = [part for part in category_parts if part]
            normalized_category = None
            # Check if category_hierarchy is valid before iterating
            if not isinstance(category_hierarchy, dict):
                logger.warning(f"category_hierarchy is invalid ({type(category_hierarchy)}), skipping normalization for ResultID {result_id}")
                normalized_category = raw_category
            else:
                for candidate in reversed(base_candidates):
                    singular = candidate[:-1] if candidate.endswith("s") else candidate
                    plural = f"{candidate}s" if not candidate.endswith("s") else candidate
                    for form in [candidate, singular, plural]:
                        if form in loaded_references:
                            normalized_category = form
                            logger.info(f"Normalized '{raw_category}' to preloaded category '{normalized_category}'")
                            break
                        for parent, children in category_hierarchy.items():
                            if form in children or form == parent:
                                singular_parent = parent[:-1] if parent.endswith("s") else parent
                                plural_parent = f"{parent}s" if not parent.endswith("s") else parent
                                for parent_form in [parent, singular_parent, plural_parent]:
                                    if parent_form in loaded_references:
                                        normalized_category = parent_form
                                        logger.info(f"Mapped '{raw_category}' to preloaded parent '{normalized_category}' via '{form}'")
                                        break
                                if normalized_category:
                                    break
                        if normalized_category:
                            break
                    if normalized_category:
                        break
                if not normalized_category:
                    logger.warning(f"Category '{raw_category}' not mapped to hierarchy or preloaded, using as-is")
                    normalized_category = raw_category

        logger.info(f"Fetching reference image for normalized category '{normalized_category}'")
        reference_image = await fetch_reference_image(normalized_category, session, logger)
        if reference_image is not None:
            logger.debug(f"Calculating SSIM for ResultID {result_id}")
            ssim_score, used_references, debug_info = await calculate_ssim_async(image_data, reference_image, logger)
            logger.debug(f"SSIM Debug Info for ResultID {result_id}: {debug_info}")
        else:
            logger.warning(f"No reference image available for '{normalized_category}'")
            ssim_score, used_references = -1, []

        ai_json = json.dumps({
            "description": description,
            "extracted_features": {
                "brand": extracted_features.get("brand", "Unknown"),
                "color": extracted_features.get("color", "Unknown"),
                "category": normalized_category
            },
            "match_score": match_score,
            "reasoning": reasoning,
            "ssim_score": ssim_score,
            "used_references": used_references,
            "resnet_classification": resnet_description
        })
        ai_caption = description
        logger.info(f"Processed ResultID {result_id} successfully")
        return result_id, ai_json, ssim_score, ai_caption, 1

    except Exception as e:
        logger.error(f"Unexpected error in process_image for ResultID {result_id}: {str(e)}", exc_info=True)
        return default_result

async def batch_process_images(file_id: str, step: int = 0, limit: int = 5000, concurrency: int = 10, logger: Optional[logging.Logger] = None) -> None:
    logger = logger or default_logger
    update_search_sort_order(file_id)
    df = fetch_missing_images(file_id=file_id, limit=limit, logger=logger)
    if df.empty:
        logger.info(f"No images to process for FileID {file_id} at step {step}")
        return
    
    # Verify DataFrame integrity
    required_cols = ["ResultID", "ImageUrl"]
    if not all(col in df.columns for col in required_cols):
        logger.error(f"DataFrame missing required columns: {df.columns}")
        return
    invalid_rows = [i for i, row in df.iterrows() if not all(pd.notna(row.get(col)) for col in required_cols)]
    if invalid_rows:
        logger.error(f"Invalid rows detected at indices {invalid_rows}: {df.iloc[invalid_rows].to_dict('records')}")
        return

    async with aiohttp.ClientSession() as session:
        logger.info("Session opened for batch processing")
        semaphore = asyncio.Semaphore(concurrency)
        async def sem_process_image(row):
            async with semaphore:
                try:
                    result = await process_image(row, session, logger)
                    if result is None:
                        logger.error(f"process_image returned None for row: {row}")
                        return row.get("ResultID"), json.dumps({"error": "process_image returned None"}), -1, None, 1
                    return result
                except Exception as e:
                    logger.error(f"Exception in sem_process_image for row {row}: {str(e)}", exc_info=True)
                    return row.get("ResultID"), json.dumps({"error": f"Task exception: {str(e)}"}), -1, None, 1

        tasks = [sem_process_image(row) for _, row in df.iterrows()]
        results = await asyncio.gather(*tasks, return_exceptions=False)  # Set return_exceptions=False to raise exceptions
        valid_updates = []
        for i, result in enumerate(results):
            row_data = df.iloc[i].to_dict()
            if isinstance(result, tuple) and len(result) == 5:
                result_id, ai_json, ssim_score, ai_caption, image_is_fashion = result
                valid_updates.append((ai_json, image_is_fashion, ai_caption, result_id))
            else:
                logger.error(f"Task {i} returned invalid result for row {row_data}: {result}")
        
        if valid_updates:
            try:
                import pyodbc
                with pyodbc.connect(conn_str) as conn:
                    cursor = conn.cursor()
                    query = "UPDATE utb_ImageScraperResult SET AiJson = ?, ImageIsFashion = ?, AiCaption = ? WHERE ResultID = ?"
                    cursor.executemany(query, valid_updates)
                    conn.commit()
                    logger.info(f"Updated {len(valid_updates)} records")
            except pyodbc.Error as e:
                logger.error(f"Database error: {e}")
    
    update_sort_order_based_on_match_score(file_id)
    logger.info(f"Completed FileID {file_id} at step {step}")