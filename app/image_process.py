import requests
import asyncio
import base64
import json
import logging
import pyodbc
import pandas as pd
from typing import Dict, Optional, List, Tuple
import google.generativeai as genai
from skimage import io as skio
from skimage.metrics import structural_similarity as ssim
from skimage.transform import resize
import numpy as np
from io import BytesIO
from config import GOOGLE_API_KEY, conn_str  # Assumes these are defined
from logging_config import root_logger  # Assumes this exists

# Fallback logger
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Image Loading
def load_image(url_or_path: str, logger: logging.Logger = None) -> Optional[np.ndarray]:
    logger = logger or default_logger
    """Load an image from a URL or local path as grayscale."""
    try:
        if url_or_path.startswith('http'):
            response = requests.get(url_or_path, timeout=10)
            response.raise_for_status()
            image_data = BytesIO(response.content)
            return skio.imread(image_data, as_gray=True)
        else:
            return skio.imread(url_or_path, as_gray=True)
    except Exception as e:
        logger.error(f"Error loading image from {url_or_path}: {e}")
        return None

# Optimal reference images for SSIM
optimal_references = {
    "shoe": load_image("https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQ7MGfepTaFjpQhcNFyetjseybIRxLUe58eag&s"),
    "clothing": load_image("https://encrypted-tbn0.gstatic.com/images?q=tbn9GcTyYe3Vgmztdh089e9IHLqdPPLuE2jUtV8IZg&s"),
    "pant": load_image("https://encrypted-tbn0.gstatic.com/images?q=tbn9GcStaRmmSmbuIuozGgVJa6GHuR59RuW3W8_8jA&s"),
    "jeans": load_image("https://encrypted-tbn0.gstatic.com/images?q=tbn9GcSjiKQ5mZWi6qnWCr6Yca5_AFinCDZXhXhiAg&s"),
    "t-shirt": load_image("https://encrypted-tbn0.gstatic.com/images?q=tbn9GcTyYe3Vgmztdh089e9IHLqdPPLuE2jUtV8IZg&s"),
}

for category, ref in optimal_references.items():
    if ref is None:
        default_logger.error(f"Reference image missing for {category}")

# SSIM Calculation
def calculate_ssim(image_data: bytes, reference_image: np.ndarray, logger: logging.Logger = None) -> float:
    logger = logger or default_logger
    """Calculate SSIM between an image and a reference image."""
    try:
        img = skio.imread(BytesIO(image_data), as_gray=True)
        img_resized = resize(img, reference_image.shape, anti_aliasing=True)
        score = ssim(img_resized, reference_image, data_range=img_resized.max() - img_resized.min())
        return score
    except Exception as e:
        logger.error(f"Error calculating SSIM: {e}")
        return -1

# Fetch Image Data with Fallback
def get_image_data(image_urls: List[str], logger: logging.Logger = None) -> Tuple[Optional[bytes], Optional[str]]:
    logger = logger or default_logger
    """Download image data from a list of URLs, with fallback to thumbnails."""
    for url in image_urls:
        if not url or not isinstance(url, str):
            logger.warning(f"Invalid URL: {url}")
            continue
        try:
            logger.info(f"Attempting to download image from {url}")
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            logger.info(f"Successfully downloaded image from {url}")
            return response.content, url
        except requests.RequestException as e:
            logger.error(f"Failed to download image from {url}: {e}")
            continue
    logger.warning(f"All URLs failed for image: {image_urls}")
    return None, None

# Gemini Analysis Prompt
gemini_prompt = """
Analyze this image and provide the following information in JSON format:
{
  "description": "A detailed description of the image in one sentence. Extract brand name, category, color, and composition.",
  "extracted_features": {
    "brand": "Extracted brand name from the image, if any.",
    "category": "Extracted category of the product, if identifiable.",
    "color": "Primary color of the product.",
    "composition": "Any composition details visible in the image."
  },
  "gemini_confidence_score": "A numeric score from 0 to 100 indicating your confidence.",
  "reasoning_confidence": "Short explanation of why you chose that gemini_confidence_score."
}
Ensure the response is a valid JSON object. Return only the JSON object, no additional text.
"""

async def analyze_image_with_gemini(
    image_base64: str,
    api_key: str = GOOGLE_API_KEY,
    model_name: str = "gemini-2.0-flash",
    mime_type: str = "image/jpeg",
    logger: logging.Logger = None
) -> Dict[str, Optional[str | int | bool]]:
    logger = logger or default_logger
    """Analyze an image using the Gemini API."""
    if not api_key:
        logger.error("No API key provided for Gemini")
        return {"status_code": None, "success": False, "text": "No API key provided"}

    try:
        image_bytes = base64.b64decode(image_base64)
    except Exception as e:
        logger.error(f"Invalid base64 string: {str(e)}")
        return {"status_code": None, "success": False, "text": f"Invalid base64 string: {str(e)}"}

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(model_name=model_name)
    contents = [{"mime_type": mime_type, "data": image_bytes}, gemini_prompt]
    loop = asyncio.get_running_loop()

    try:
        response = await loop.run_in_executor(
            None,
            lambda: model.generate_content(
                contents,
                generation_config=genai.types.GenerationConfig(response_mime_type="application/json")
            )
        )
        response_text = getattr(response, "text", "") or ""
        if response_text:
            features = json.loads(response_text)
            logger.info("Image analysis successful")
            return {"status_code": 200, "success": True, "features": features}
        else:
            logger.warning("Gemini returned no text")
            return {"status_code": 200, "success": False, "text": "No analysis text returned"}
    except Exception as e:
        logger.error(f"Gemini analysis failed: {str(e)}")
        return {"status_code": None, "success": False, "text": f"Gemini analysis error: {str(e)}"}

# Fetch Missing Images with Step Tracking
def fetch_missing_images(
    file_id: str,
    step: int = 0,
    limit: int = 1000,
    logger: logging.Logger = None
) -> pd.DataFrame:
    logger = logger or default_logger
    """Fetch images based on the current processing step using ImageIsFashion."""
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            query = """
                SELECT t.ResultID, t.EntryID, t.ImageURL, t.ImageURLThumbnail,
                       r.ProductBrand, r.ProductCategory, r.ProductColor
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                WHERE r.FileID = ?
                AND t.ImageIsFashion = ?
                AND t.ImageURL IS NOT NULL
                AND t.ImageURL <> ''
                ORDER BY t.ResultID
                OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
            """
            cursor.execute(query, (file_id, step, limit))
            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
            logger.info(f"Fetched {len(df)} images for FileID {file_id} at step {step}")
            return df
    except Exception as e:
        logger.error(f"Error fetching missing images for FileID {file_id} at step {step}: {e}")
        return pd.DataFrame()

# Database Update Function
def update_database(
    result_id: int,
    ai_json: str,
    ssim_score: float,
    ai_caption: Optional[str],
    next_step: int,
    logger: logging.Logger = None
) -> None:
    logger = logger or default_logger
    """Update the database with analysis results and the next step."""
    try:
        json.loads(ai_json)  # Validate JSON
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            update_query = """
                UPDATE utb_ImageScraperResult 
                SET AiJson = ?, ImageIsFashion = ?, AiCaption = ? 
                WHERE ResultID = ?
            """
            cursor.execute(update_query, (ai_json, next_step, ai_caption, result_id))
            conn.commit()
            logger.info(f"Updated ResultID {result_id} to step {next_step} with SSIM {ssim_score}")
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON for ResultID {result_id}: {e}")
    except Exception as e:
        logger.error(f"Error updating database for ResultID {result_id}: {e}")

# Batch Process Images with Step Tracking
async def batch_process_images(
    file_id: str,
    step: int = 0,
    limit: int = 1000,
    logger: logging.Logger = None
) -> None:
    logger = logger or default_logger
    """Process images in batches, updating steps via ImageIsFashion."""
    # Fetch images for the current step
    df = fetch_missing_images(file_id=file_id, step=step, limit=limit, logger=logger)
    if df.empty:
        logger.info(f"No images to process for FileID {file_id} at step {step}")
        return

    tasks = []
    for _, row in df.iterrows():
        result_id = row['ResultID']
        image_urls = [row['ImageURL']]
        if pd.notna(row['ImageURLThumbnail']):
            image_urls.append(row['ImageURLThumbnail'])
        product_details = {
            "brand": row['ProductBrand'],
            "category": row['ProductCategory'],
            "color": row['ProductColor']
        }

        # Download image data
        image_data, _ = await asyncio.to_thread(get_image_data, image_urls, logger)
        if not image_data:
            logger.warning(f"Failed to download image for ResultID {result_id}")
            update_database(result_id, json.dumps({"error": "Image download failed"}), -1, None, step + 1, logger)
            continue

        # Calculate SSIM
        category = product_details["category"].lower()
        reference_image = optimal_references.get(category, next(iter(optimal_references.values())))
        ssim_score = calculate_ssim(image_data, reference_image, logger) if reference_image else -1

        # Perform AI analysis
        base64_image = base64.b64encode(image_data).decode('utf-8')
        gemini_result = await analyze_image_with_gemini(base64_image, logger=logger)
        if not gemini_result['success']:
            logger.warning(f"Gemini analysis failed for ResultID {result_id}: {gemini_result['text']}")
            update_database(result_id, json.dumps({"error": gemini_result['text']}), ssim_score, None, step + 1, logger)
            continue

        features = gemini_result['features']
        ai_json = json.dumps({
            "description": features["description"],
            "extracted_features": features["extracted_features"],
            "ssim_score": ssim_score
        })
        ai_caption = features["description"]

        # Update database with results and move to next step
        update_database(result_id, ai_json, ssim_score, ai_caption, step + 1, logger)

    logger.info(f"Completed processing for FileID {file_id} at step {step}")

# Main Execution with Step-by-Step Processing
async def process_all_steps(file_id: str, logger: logging.Logger = None):
    logger = logger or default_logger
    """Execute all processing steps sequentially."""
    steps = [
        (1, "Search"),
        (2, "Initial Sort"),
        (3, "AI Analysis")
    ]
    for step_num, step_name in steps:
        logger.info(f"Starting step {step_num}: {step_name} for FileID {file_id}")
        await batch_process_images(file_id, step=step_num - 1, logger=logger)  # Step numbers are 0-based internally
