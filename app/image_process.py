import asyncio
import base64
import json
import logging
import numpy as np
import pandas as pd
import pyodbc
import requests
from io import BytesIO
from skimage import io as skio
from skimage.metrics import structural_similarity as ssim
from skimage.transform import resize
from typing import Dict, Optional, List, Tuple, Union
import google.generativeai as genai
from config import GOOGLE_API_KEY, conn_str
from logging_config import root_logger
from PIL import Image
import aiohttp

# Default logger
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Optimal reference images for SSIM
optimal_references = {
    "shoe": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQ7MGfepTaFjpQhcNFyetjseybIRxLUe58eag&s",
    "clothing": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTyYe3Vgmztdh089e9IHLqdPPLuE2jUtV8IZg&s",
    "pant": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcStaRmmSmbuIuozGgVJa6GHuR59RuW3W8_8jA&s",
    "jeans": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSjiKQ5mZWi6qnWCr6Yca5_AFinCDZXhXhiAg&s",
    "t-shirt": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTyYe3Vgmztdh089e9IHLqdPPLuE2jUtV8IZg&s",
    "accessories": "path/to/optimal_accessories.jpg",
    "jacket": "path/to/optimal_jacket.jpg",
    "hat": "path/to/optimal_hat.jpg",
}
loaded_references = {}
for category, url in optimal_references.items():
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        image_data = BytesIO(response.content)
        loaded_references[category] = skio.imread(image_data, as_gray=True)
    except Exception as e:
        default_logger.error(f"Failed to load reference image for {category}: {e}")

# Diagnostic logging
if not loaded_references:
    default_logger.error("No reference images were loaded successfully.")
else:
    default_logger.info(f"Loaded reference images for categories: {list(loaded_references.keys())}")

# Initial analysis prompt
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

reasoning_prompt = """
Given the following extracted features from an image analysis and user-provided product details:
{{"image_features": {{"description": "{description}", "extracted_features": {{"brand": "{extracted_brand}", "category": "{extracted_category}", "color": "{extracted_color}", "composition": "{extracted_composition}"}}}}, "user_details": {{"brand": "{user_brand}", "category": "{user_category}", "color": "{user_color}"}}}}
Provide a JSON object with:
{{"match_score": "A numeric score from 0 to 1 indicating how well the image matches the user-provided product details. Use this rubric: Brand (weight 0.4): 1.0 if exact match, 0.5 if partial, 0.0 if no match; Category (weight 0.4): 1.0 if exact, 0.5 if partial, 0.0 if no match; Color (weight 0.2): 1.0 if exact, 0.5 if partial, 0.0 if no match. Redistribute weights equally if a user detail is empty. Round to 2 decimals.", "reasoning": "Short explanation of why you chose that match_score, specifying matches, partial matches, or mismatches for brand, category, and color."}}
Ensure the response is a valid JSON object. Return only the JSON object, no additional text.
"""
async def get_image_data_async(image_urls: List[str], session: aiohttp.ClientSession, logger: Optional[logging.Logger] = None, retries: int = 3) -> Tuple[Optional[bytes], Optional[str]]:
    """Download image data from a list of URLs with fallback asynchronously."""
    logger = logger or default_logger
    for url in image_urls:
        if not url or not isinstance(url, str):
            logger.warning(f"Invalid URL: {url}")
            continue
        for attempt in range(retries):
            try:
                logger.info(f"Attempting to download image from {url} (attempt {attempt + 1})")
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    response.raise_for_status()
                    image_data = await response.read()
                    logger.info(f"Successfully downloaded image from {url} on attempt {attempt + 1}")
                    return image_data, url
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.error(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(1)
        logger.warning(f"All {retries} attempts failed for {url}")
    logger.warning(f"All URLs failed for image: {image_urls}")
    return None, None

async def calculate_ssim_async(image_data: bytes, reference_image: np.ndarray, logger: Optional[logging.Logger] = None) -> float:
    """Calculate SSIM between an image and a reference image asynchronously."""
    logger = logger or default_logger
    try:
        img = skio.imread(BytesIO(image_data), as_gray=True)
        img_resized = resize(img, reference_image.shape, anti_aliasing=True)
        score = ssim(img_resized, reference_image, data_range=img_resized.max() - img_resized.min())
        return score
    except Exception as e:
        logger.error(f"Error calculating SSIM: {e}")
        return -1

async def analyze_image_with_gemini_async(
    image_base64: str,
    api_key: str = GOOGLE_API_KEY,
    model_name: str = "gemini-2.0-flash",
    mime_type: str = "image/jpeg",
    logger: Optional[logging.Logger] = None,
    prompt: str = gemini_prompt
) -> Dict[str, Optional[Union[str, int, bool]]]:
    """Analyze an image using the Gemini API asynchronously."""
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

def validate_aijson(
    aijson: Dict[str, Optional[Union[str, int, float]]],
    user_brand: str,
    user_color: str,
    user_category: str,
    logger: Optional[logging.Logger] = None
) -> Dict[str, Union[bool, str, Optional[dict]]]:
    """Validate the aijson response from Gemini analysis against user inputs."""
    logger = logger or default_logger

    # Normalize user inputs to lowercase for comparison
    user_brand = user_brand.strip().lower()
    user_color = user_color.strip().lower()
    user_category = user_category.strip().lower()

    # Check if aijson is a dictionary
    if not isinstance(aijson, dict):
        logger.error("Invalid aijson: not a dictionary")
        return {"success": False, "message": "Invalid aijson: not a dictionary", "features": None}

    # Validate and convert match_score
    match_score = aijson.get("match_score")
    logger.debug(f"match_score type: {type(match_score)}, value: {match_score}")  # Debug log
    if match_score is None:
        logger.error("match_score is missing in aijson")
        return {"success": False, "message": "match_score is missing", "features": None}
    
    # Convert string to float if necessary
    if isinstance(match_score, str):
        try:
            match_score = float(match_score)
            logger.debug(f"Converted match_score from string to float: {match_score}")
        except ValueError:
            logger.error(f"Invalid match_score: {match_score} (cannot convert to float)")
            return {"success": False, "message": f"Invalid match_score: {match_score}", "features": None}
    
    if not isinstance(match_score, (int, float)):
        logger.error(f"Invalid match_score type: {match_score} (must be an integer or float)")
        return {"success": False, "message": f"Invalid match_score type: {match_score}", "features": None}
    if not (0 <= match_score <= 1):
        logger.error(f"Invalid match_score value: {match_score} (must be between 0 and 1, inclusive)")
        return {"success": False, "message": f"Invalid match_score value: {match_score}", "features": None}

    # Round match_score to 1 decimal place, adjusting 0.9 or >=0.95 to 1.0
    if match_score >= 0.95:
        match_score = 1.0
        logger.debug(f"Rounded match_score from {aijson['match_score']} to 1.0 (threshold >= 0.95)")
    elif match_score == 0.9:
        match_score = 1.0
        logger.debug(f"Rounded match_score from 0.9 to 1.0 (exact match)")
    else:
        match_score = round(match_score, 1)
        logger.debug(f"Rounded match_score from {aijson['match_score']} to {match_score} (1 decimal place)")
    
    # Update aijson with the rounded match_score
    aijson["match_score"] = match_score

    # Validate reasoning
    reasoning = aijson.get("reasoning")
    if not isinstance(reasoning, str) or not reasoning.strip():
        logger.error("reasoning is missing or empty in aijson")
        return {"success": False, "message": "reasoning is missing or empty", "features": None}

    logger.info(f"Validation successful: match_score={match_score}")
    return {"success": True, "message": "Validation successful", "features": aijson}
async def process_image(row, session: aiohttp.ClientSession, logger: Optional[logging.Logger] = None):
    logger = logger or default_logger
    result_id = row["ResultID"]
    image_urls = [row["ImageUrl"]]
    if pd.notna(row["ImageUrlThumbnail"]):
        image_urls.append(row["ImageUrlThumbnail"])
    product_details = {
        "brand": row["ProductBrand"],
        "category": row["ProductCategory"],
        "color": row["ProductColor"]
    }

    # Download image asynchronously
    image_data, _ = await get_image_data_async(image_urls, session, logger)
    if not image_data:
        logger.warning(f"Failed to download image for ResultID {result_id}")
        return result_id, json.dumps({"error": "Image download failed"}), -1, None, 1

    # Calculate SSIM with proper reference image handling
    category = product_details["category"].lower()
    if category in loaded_references:
        reference_image = loaded_references[category]
    elif loaded_references:
        reference_image = next(iter(loaded_references.values()))
        logger.info(f"Using default reference image for category {category}")
    else:
        reference_image = None
        logger.warning(f"No reference images available for category {category}")

    ssim_score = await calculate_ssim_async(image_data, reference_image, logger) if reference_image is not None else -1

    # Initial analysis with Gemini (First Call)
    base64_image = base64.b64encode(image_data).decode("utf-8")
    gemini_result = await analyze_image_with_gemini_async(
        image_base64=base64_image,
        api_key=GOOGLE_API_KEY,
        model_name="gemini-2.0-flash",
        mime_type="image/jpeg",
        logger=logger
    )

    # Check if initial analysis succeeded
    if not gemini_result["success"] or not isinstance(gemini_result.get("features"), dict):
        logger.warning(f"Initial Gemini analysis failed for ResultID {result_id}: {gemini_result.get('text', 'Invalid features')}")
        return result_id, json.dumps({"error": "Initial Gemini analysis failed"}), ssim_score, None, 1

    initial_features = gemini_result["features"]

    # Second call to Gemini for reasoning (Always happens)
    try:
        reasoning_input = reasoning_prompt.format(
            description=initial_features.get("description", ""),
            extracted_brand=initial_features.get("extracted_features", {}).get("brand", ""),
            extracted_category=initial_features.get("extracted_features", {}).get("category", ""),
            extracted_color=initial_features.get("extracted_features", {}).get("color", ""),
            extracted_composition=initial_features.get("extracted_features", {}).get("composition", ""),
            user_brand=product_details.get("brand", ""),
            user_category=product_details.get("category", ""),
            user_color=product_details.get("color", "")
        )
    except KeyError as e:
        logger.error(f"Error formatting reasoning prompt for ResultID {result_id}: {e}")
        return result_id, json.dumps({"error": f"Error formatting reasoning prompt: {e}"}), ssim_score, None, 1

    reasoning_result = await analyze_image_with_gemini_async(
        image_base64=base64_image,
        api_key=GOOGLE_API_KEY,
        model_name="gemini-2.0-flash",
        mime_type="image/jpeg",
        logger=logger,
        prompt=reasoning_input
    )

    # Check if reasoning analysis succeeded
    if not reasoning_result["success"] or not isinstance(reasoning_result.get("features"), dict):
        logger.error(f"Reasoning analysis failed for ResultID {result_id}: {reasoning_result.get('text', 'Invalid features')}")
        return result_id, json.dumps({"error": "Reasoning analysis failed"}), ssim_score, None, 1

    reasoning_features = reasoning_result["features"]
    if "match_score" not in reasoning_features or reasoning_features["match_score"] is None:
        logger.error(f"match_score missing in reasoning analysis for ResultID {result_id}")
        return result_id, json.dumps({"error": "match_score missing in reasoning analysis"}), ssim_score, None, 1

    # Combine initial features with reasoning results
    features = {
        "description": initial_features.get("description", ""),
        "extracted_features": initial_features.get("extracted_features", {}),
        "match_score": reasoning_features["match_score"],
        "reasoning": reasoning_features["reasoning"]
    }

    # Validate combined features
    validation_result = validate_aijson(
        features,
        user_brand=product_details.get("brand", ""),
        user_color=product_details.get("color", ""),
        user_category=product_details.get("category", ""),
        logger=logger
    )
    if not validation_result["success"]:
        logger.warning(f"Validation failed for ResultID {result_id}: {validation_result['message']}")
        return result_id, json.dumps({"error": validation_result["message"]}), ssim_score, None, 1

    # If validation succeeds, proceed with features
    features = validation_result["features"]
    ai_json = json.dumps({
        "description": features["description"],
        "extracted_features": {
            "brand": features["extracted_features"].get("brand"),
            "color": features["extracted_features"].get("color"),
            "category": features["extracted_features"].get("category")
        },
        "match_score": features["match_score"],
        "reasoning": features["reasoning"],
        "ssim_score": ssim_score
    })
    ai_caption = features["description"]
    return result_id, ai_json, ssim_score, ai_caption, 1

async def batch_process_images(file_id: str, step: int = 0, limit: int = 1000, concurrency: int = 10, logger: Optional[logging.Logger] = None) -> None:
    """Process images in batches concurrently."""
    logger = logger or default_logger
    df = fetch_missing_images(file_id=file_id, step=step, limit=limit, logger=logger)
    if df.empty:
        logger.info(f"No images to process for FileID {file_id} at step {step}")
        return

    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(concurrency)
        async def sem_process_image(row):
            async with semaphore:
                return await process_image(row, session, logger)

        tasks = [sem_process_image(row) for _, row in df.iterrows()]
        results = await asyncio.gather(*tasks)

        updates = []
        for result in results:
            if result:
                result_id, ai_json, ssim_score, ai_caption, next_step = result
                updates.append((ai_json, next_step, ai_caption, result_id))

        if updates:
            try:
                with pyodbc.connect(conn_str) as conn:
                    cursor = conn.cursor()
                    query = """
                        UPDATE utb_ImageScraperResult 
                        SET AiJson = ?, ImageIsFashion = ?, AiCaption = ? 
                        WHERE ResultID = ?
                    """
                    cursor.executemany(query, updates)
                    conn.commit()
                    logger.info(f"Updated {len(updates)} records in the database")
            except pyodbc.Error as e:
                logger.error(f"Database error during batch update: {e}")

    logger.info(f"Completed processing for FileID {file_id} at step {step}")

def fetch_missing_images(file_id: str, step: int = 0, limit: int = 1000, logger=None) -> pd.DataFrame:
    """Fetch images at a specific processing step for a given file_id."""
    logger = logger or default_logger
    try:
        with pyodbc.connect(conn_str) as conn:
            query = """
                SELECT t.ResultID, t.EntryID, t.ImageUrl, t.ImageUrlThumbnail,
                       r.ProductBrand, r.ProductCategory, r.ProductColor
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                WHERE r.FileID = ?
                AND r.EntryStatus = ?
                AND t.ImageUrl IS NOT NULL AND t.ImageUrl <> ''
                AND (t.SortOrder > 0 OR t.SortOrder IS NULL)
                ORDER BY t.ResultID
                OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
            """
            params = (file_id, step, limit)
            df = pd.read_sql(query, conn, params=params)
            logger.info(f"Fetched {len(df)} images for FileID {file_id} at step {step}")
            return df
    except pyodbc.Error as e:
        logger.error(f"Database error fetching images for FileID {file_id} at step {step}: {e}", exc_info=True)
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Unexpected error fetching images for FileID {file_id} at step {step}: {e}", exc_info=True)
        return pd.DataFrame()