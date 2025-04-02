import asyncio
import base64
import json
import logging
import numpy as np
import pandas as pd
import requests
from io import BytesIO
from skimage import io as skio
from skimage.metrics import structural_similarity as ssim
from skimage.transform import resize
from typing import Dict, Optional, List, Tuple, Union
from database import fetch_missing_images, update_search_sort_order  # Assume these are defined in a database module
import google.generativeai as genai
from config import GOOGLE_API_KEY, conn_str  # Assume these are defined in a config module
from logging_config import root_logger       # Assume logging setup is imported
from PIL import Image
import aiohttp
import urllib.parse
import re

# Default logger setup
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

# Sample category hierarchy for validation
category_hierarchy = {
    "clothing": ["shoes", "shirts"],
    "shoes": ["sneakers", "boots"],
    "shirts": ["t-shirts", "dress shirts"],
    "accessories": ["hats", "belts"]
}

# Optimal reference images for SSIM (example URLs)
optimal_references = {
    "shoe": "https://encrypted-tbn0.gstatic.com/images?q=tbn9GcQ7MGfepTaFjpQhcNFyetjseybIRxLUe58eag&s",
    "clothing": "https://encrypted-tbn0.gstatic.com/images?q=tbn9GcTyYe3Vgmztdh089e9IHLqdPPLuE2jUtV8IZg&s",
    "pant": "https://s3.eu-central-1.amazonaws.com/bilder.modehaus.de/8/7/1/9/0/2/7/8/6/6/6/2/0/8719027866620_fc0_medium.webp",
    "jeans": "https://encrypted-tbn0.gstatic.com/images?q=tbn9GcSjiKQ5mZWi6qnWCr6Yca5_AFinCDZXhXhiAg&s",
    "t-shirt": "https://encrypted-tbn0.gstatic.com/images?q=tbn9GcTyYe3Vgmztdh089e9IHLqdPPLuE2jUtV8IZg&s",
    "vest": "https://encrypted-tbn0.gstatic.com/images?q=tbn9GcTKoQAmadvbb5lASL9zuPuL56WNFFhdchwt-A&s",
    "boot": "https://encrypted-tbn0.gstatic.com/images?q=tbn9GcSbSeU_v1Y_4ZZRa8zEks1YiNecrUXtM0FfGQ&s",
    "dress": "https://s3.us-east-2.amazonaws.com/iconluxurygroup/images/9644db91-1872-4b92-965b-a1f924af5e1e/image_9644db91-1872-4b92-965b-a1f924af5e1e_B1224.png",
    "accessories": "https://example.com/optimal_accessories.jpg",
    "bag": "https://s3.us-east-2.amazonaws.com/iconluxurygroup/images/9644db91-1872-4b92-965b-a1f924af5e1e/image_9644db91-1872-4b92-965b-a1f924af5e1e_B990.png",
    "belt": "https://s3.us-east-2.amazonaws.com/iconluxurygroup/images/9644db91-1872-4b92-965b-a1f924af5e1e/image_9644db91-1872-4b92-965b-a1f924af5e1e_B948.png",
    "sunglasses": "https://encrypted-tbn0.gstatic.com/images?q=tbn9GcT5dovp7mjgUEXcPHICkZ-Iz1zxpNsKyToVDQ&s",
    "jacket": "https://encrypted-tbn0.gstatic.com/images?q=tbn9GcQPvOW6spTSJp8Ebda5w8yaFGxGuQmYLHUAoA&s",
    "hat": "https://encrypted-tbn0.gstatic.com/images?q=tbn9GcSkyqOpIB9aukpLuHYCIyLHyGUYkFVk0DBRHg&s",
    "cap": "https://s3.us-east-2.amazonaws.com/iconluxurygroup/images/9644db91-1872-4b92-965b-a1f924af5e1e/image_9644db91-1872-4b92-965b-a1f924af5e1e_B1096.png",
}

# Preload reference images with retry logic
loaded_references = {}
for category, url in optimal_references.items():
    for attempt in range(3):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            image_data = BytesIO(response.content)
            img = skio.imread(image_data, as_gray=True)
            if img.size > 0:
                loaded_references[category] = img
                default_logger.info(f"Loaded reference image for {category} on attempt {attempt + 1}")
                break
        except Exception as e:
            default_logger.error(f"Attempt {attempt + 1} failed for {category}: {e}")
            if attempt == 2:
                default_logger.warning(f"Failed to load reference image for {category} after retries")

# Gemini API prompt for feature extraction
gemini_prompt = """
Analyze this image and provide the following information in JSON format:
{
  "description": "A detailed description of the image in one sentence. Extract brand name, category, color, and composition.",
  "extracted_features": {
    "brand": "Extracted brand name from the image, if any.",
    "category": "Extracted category of the product, if identifiable.",
    "color": "Primary color of the product.",
    "composition": "Any composition details visible in the image."
  }
}
Ensure the response is a valid JSON object. Return only the JSON object, no additional text.
"""

# Category validation functions
def validate(category: str) -> bool:
    """Validate if a category exists in the hierarchy."""
    category = category.lower().strip()
    return category in category_hierarchy or any(category in sublist for sublist in category_hierarchy.values())

def are_categories_related(category1: str, category2: str) -> bool:
    """Check if two categories are related in the hierarchy."""
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
async def get_image_data_async(
    image_urls: List[str],
    session: aiohttp.ClientSession,
    logger: logging.Logger = None,
    retries: int = 3
) -> Tuple[Optional[bytes], Optional[str]]:
    """Download image data from a list of URLs with fallback asynchronously."""
    logger = logger or default_logger
    for url in image_urls:
        if not url or not isinstance(url, str) or url.strip() == "":
            logger.warning(f"Invalid URL: {url} (empty or not a string)")
            continue
        if not url.startswith(("http://", "https://")):
            logger.warning(f"Skipping invalid URL: {url} (no http:// or https://)")
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

# SSIM calculation
async def calculate_ssim_async(
    image_data: bytes,
    reference_image: np.ndarray,
    logger: Optional[logging.Logger] = None
) -> Tuple[float, List[str]]:
    """Calculate SSIM between an image and a reference image."""
    logger = logger or default_logger
    try:
        with Image.open(BytesIO(image_data)) as img:
            img.verify()
        with Image.open(BytesIO(image_data)) as img:
            if img.mode != 'L':
                img = img.convert('L')
            img_array = np.array(img, dtype=np.float64) / 255.0

        if img_array.size == 0 or img_array.ndim != 2:
            logger.error(f"Invalid input image: size={img_array.size}, ndim={img_array.ndim}")
            return -1, []
        if reference_image.size == 0 or reference_image.ndim < 2:
            logger.error(f"Invalid reference image: size={reference_image.size}, ndim={reference_image.ndim}")
            return -1, []
        if reference_image.ndim > 2:
            reference_image = np.mean(reference_image, axis=2)

        ref_normalized = reference_image.astype(np.float64)
        if ref_normalized.max() == ref_normalized.min():
            logger.error("Reference image has no variance")
            return -1, []
        if ref_normalized.max() > 1.0 or ref_normalized.min() < 0.0:
            ref_normalized = (ref_normalized - ref_normalized.min()) / (ref_normalized.max() - ref_normalized.min())

        if img_array.shape != ref_normalized.shape:
            img_resized = resize(
                img_array,
                ref_normalized.shape,
                anti_aliasing=True,
                preserve_range=True,
                mode='reflect',
                order=3
            )
        else:
            img_resized = img_array

        min_dim = min(img_resized.shape)
        if min_dim < 7:
            logger.warning(f"Image too small for SSIM (min_dim={min_dim})")
            return 0.0, ["preloaded"]
        win_size = min(11, max(7, min_dim // 2 * 2 + 1))
        if win_size > min_dim:
            win_size = min_dim if min_dim % 2 == 1 else min_dim - 1
        score = ssim(
            img_resized,
            ref_normalized,
            data_range=1.0,
            win_size=win_size,
            gaussian_weights=True,
            sigma=1.5,
            use_sample_covariance=False,
            K1=0.01,
            K2=0.03
        )
        if not isinstance(score, (int, float)) or np.isnan(score):
            logger.error(f"Invalid SSIM score: {score}")
            return -1, ["preloaded"]
        logger.info(f"SSIM score: {score}")
        return min(max(score, 0), 1), ["preloaded"]
    except Exception as e:
        logger.error(f"SSIM calculation failed: {e}")
        return -1, []

# Fetch reference image
async def fetch_reference_image(
    category: str,
    session: aiohttp.ClientSession,
    logger: Optional[logging.Logger] = None
) -> Optional[np.ndarray]:
    """Fetch a reference image for a category, using preloaded if available."""
    logger = logger or default_logger
    category = category.lower().strip()
    if category in loaded_references:
        logger.info(f"Using preloaded reference for '{category}'")
        return loaded_references[category]

    search_url = f"https://www.google.com/search?q={category}&tbm=isch"
    try:
        async with session.get(search_url) as response:
            html_bytes = await response.read()
        html_str = html_bytes.decode('utf-8', errors='replace')
        pattern = r'"(https://encrypted-tbn0.gstatic.com/images\?[^"]+)"'
        image_urls = re.findall(pattern, html_str)
        if image_urls:
            image_data, _ = await get_image_data_async([image_urls[0]], session, logger)
            if image_data:
                ref_img = skio.imread(BytesIO(image_data), as_gray=True)
                logger.info(f"Fetched reference image for '{category}'")
                return ref_img
    except Exception as e:
        logger.error(f"Failed to fetch reference for '{category}': {e}")
    return None

# Gemini image analysis
async def analyze_image_with_gemini_async(
    image_base64: str,
    api_key: str = GOOGLE_API_KEY,
    model_name: str = "gemini-2.0-flash",
    mime_type: str = "image/jpeg",
    logger: Optional[logging.Logger] = None,
    prompt: str = gemini_prompt
) -> Dict[str, Optional[Union[str, int, bool, dict]]]:
    """Analyze an image using Gemini API."""
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
    contents = [{"mime_type": mime_type, "data": image_bytes}, prompt]
    try:
        response = await model.generate_content_async(
            contents,
            generation_config=genai.types.GenerationConfig(response_mime_type="application/json")
        )
        response_text = getattr(response, "text", "") or ""
        if response_text:
            features = json.loads(response_text)
            logger.info("Image analysis successful")
            return {"status_code": 200, "success": True, "features": features}
        logger.warning("Gemini returned no text")
        return {"status_code": 200, "success": False, "text": "No analysis text returned"}
    except Exception as e:
        logger.error(f"Gemini analysis failed: {e}")
        return {"status_code": None, "success": False, "text": f"Gemini analysis error: {e}"}

# Process a single image
async def process_image(
    row,
    session: aiohttp.ClientSession,
    logger: Optional[logging.Logger] = None
):
    """Process an image with feature matching and SSIM."""
    logger = logger or default_logger
    result_id = row.get("ResultID")
    if result_id is None:
        logger.error(f"Invalid row data: ResultID missing - row: {row}")
        return None, json.dumps({"error": "Invalid row data: ResultID missing"}), -1, None, 1

    sort_order = row.get("SortOrder")
    if isinstance(sort_order, (int, float)) and sort_order < 0:
        logger.info(f"Skipping ResultID {result_id} due to negative SortOrder: {sort_order}")
        return result_id, json.dumps({"error": f"Negative SortOrder: {sort_order}"}), sort_order, None, 1

    image_urls = [row["ImageUrl"]]
    if pd.notna(row["ImageUrlThumbnail"]):
        image_urls.append(row["ImageUrlThumbnail"])
    product_details = {
        "brand": row["ProductBrand"],
        "category": row["ProductCategory"],
        "color": row["ProductColor"]
    }

    image_data, _ = await get_image_data_async(image_urls, session, logger)
    if not image_data:
        logger.warning(f"Image download failed for ResultID {result_id}")
        return result_id, json.dumps({"error": "Image download failed"}), -1, None, 1

    base64_image = base64.b64encode(image_data).decode("utf-8")
    gemini_result = await analyze_image_with_gemini_async(base64_image, logger=logger)
    if not gemini_result["success"] or not isinstance(gemini_result.get("features"), dict):
        logger.warning(f"Gemini analysis failed for ResultID {result_id}")
        return result_id, json.dumps({"error": "Gemini analysis failed"}), -1, None, 1
    initial_features = gemini_result["features"]

    provided_details = {k: v.strip().lower() for k, v in product_details.items() if v and v.strip()}
    if not provided_details:
        match_score = 1.0
        reasoning = "No user details provided, assuming match."
    else:
        extracted_features = {k: v.strip().lower() for k, v in initial_features.get("extracted_features", {}).items() if v}
        matches = {k: (extracted_features.get(k, "") == v) for k, v in provided_details.items()}
        all_match = all(matches.values())
        match_score = 1.0 if all_match else 0.0
        if all_match:
            reasoning = "All provided features match exactly."
        else:
            mismatched = [k for k, v in matches.items() if not v]
            reasoning = f"Mismatched features: {', '.join(mismatched)}"

    category = product_details.get("category", "").strip().lower() or initial_features.get("extracted_features", {}).get("category", "unknown").lower()
    reference_image = await fetch_reference_image(category, session, logger)
    if reference_image is not None:
        ssim_score, used_references = await calculate_ssim_async(image_data, reference_image, logger)
    else:
        ssim_score, used_references = -1, []

    features = {
        "description": initial_features.get("description", ""),
        "extracted_features": initial_features.get("extracted_features", {}),
        "match_score": match_score,
        "reasoning": reasoning
    }

    ai_json = json.dumps({
        "description": features["description"],
        "extracted_features": {
            "brand": features["extracted_features"].get("brand"),
            "color": features["extracted_features"].get("color"),
            "category": features["extracted_features"].get("category")
        },
        "match_score": features["match_score"],
        "reasoning": features["reasoning"],
        "ssim_score": ssim_score,
        "used_references": used_references
    })
    ai_caption = features["description"]
    logger.info(f"Processed ResultID {result_id}")
    return result_id, ai_json, ssim_score, ai_caption, 1

# Batch processing
async def batch_process_images(
    file_id: str,
    step: int = 0,
    limit: int = 5000,
    concurrency: int = 10,
    logger: Optional[logging.Logger] = None
) -> None:
    """Process images in batch and update database."""
    logger = logger or default_logger
    # Placeholder functions assumed to be defined elsewhere
    update_search_sort_order(file_id)
    df = fetch_missing_images(file_id=file_id, limit=limit, logger=logger)

    if df.empty:
        logger.info(f"No images to process for FileID {file_id} at step {step}")
        return
    async with aiohttp.ClientSession() as session:
        logger.info("Session opened for batch processing")
        semaphore = asyncio.Semaphore(concurrency)
        async def sem_process_image(row):
            async with semaphore:
                return await process_image(row, session, logger)
        tasks = [sem_process_image(row) for _, row in df.iterrows()]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Task failed: {result}")
        updates = [(result[1], result[4], result[3], result[0]) for result in results if isinstance(result, tuple) and result]
        if updates:
            try:
                import pyodbc
                with pyodbc.connect(conn_str) as conn:
                    cursor = conn.cursor()
                    query = """
                        UPDATE utb_ImageScraperResult 
                        SET AiJson = ?, ImageIsFashion = ?, AiCaption = ? 
                        WHERE ResultID = ?
                    """
                    cursor.executemany(query, updates)
                    conn.commit()
                    logger.info(f"Updated {len(updates)} records")
            except pyodbc.Error as e:
                logger.error(f"Database error: {e}")
    logger.info(f"Completed FileID {file_id} at step {step}")