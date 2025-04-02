import asyncio
import base64
import json,time
import logging
import numpy as np
import pandas as pd
import requests
from io import BytesIO
from skimage import io as skio
from skimage.metrics import structural_similarity as ssim
from skimage.transform import resize
from typing import Dict, Optional, List, Tuple, Union
from database import fetch_missing_images, update_search_sort_order ,update_sort_order_based_on_match_score# Assume these are defined in a database module
import google.generativeai as genai
from config import GOOGLE_API_KEY, conn_str  # Assume these are defined in a config module
from logging_config import root_logger       # Assume logging setup is imported
from PIL import Image
import aiohttp
import urllib.parse
import re
# SSIM calculation
import matplotlib.pyplot as plt  # Add this to your imports at the top

import matplotlib.pyplot as plt  # Ensure this is in your imports
from skimage.transform import rotate  # Add this for rotation

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
    "clothing": ["tops", "bottoms", "outerwear", "dresses","rtw","ready to wear"],
    "tops": ["shirts", "vests"],
    "shirts": ["t-shirts", "dress shirts"],
    "vests": [],
    "bottoms": ["pants"],
    "pants": ["jeans", "trousers","chinos"],
    "outerwear": ["jackets"],
    "jackets": ["coats", "blazers"],
    "dresses": [],
    "footwear": ["shoes"],
    "shoes": ["boots", "sneakers"],
    "accessories": ["hats", "belts", "bags", "sunglasses"],
    "hats": ["caps", "beanies"],
    "belts": [],
    "bags": [],
    "sunglasses": []
}

json_url = "https://raw.githubusercontent.com/iconluxurygroup/settings-static-data/refs/heads/main/optimal-references.json"

# Function to fetch JSON data with retry logic
def fetch_json(url, max_attempts=3, timeout=10):
    for attempt in range(max_attempts):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()  # Raise an exception for bad status codes
            json_data = response.json()  # Parse the JSON response
            return json_data
        except (requests.RequestException, ValueError) as e:
            print(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
            if attempt < max_attempts - 1:
                time.sleep(2)  # Wait before retrying
            else:
                print(f"Failed to fetch JSON after {max_attempts} attempts.")
                return None

# Dictionary to store loaded images (if needed later)
loaded_references = {}

# Fetch the JSON data
optimal_references = fetch_json(json_url)
logger = default_logger
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
                    # Save for inspection
                    plt.imsave(f"preloaded_{category}.png", img, cmap='gray')
                    logger.info(f"Saved preloaded image as 'preloaded_{category}.png'")
                    break
            except (requests.RequestException, ValueError) as e:
                logger.warning(f"Attempt {attempt + 1} failed for '{category}' ({url}): {str(e)}")
                if attempt == 2:
                    logger.error(f"Failed to load image for '{category}' after 3 attempts.")
else:
    logger.error("Could not fetch JSON data from the provided URL.")

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

import numpy as np
from skimage import io as skio
from skimage.metrics import structural_similarity as ssim
from skimage.transform import rotate
from PIL import Image
from io import BytesIO
import matplotlib.pyplot as plt
import logging
import cv2

def compute_ssim(image1, image2):
    """Compute Structural Similarity Index (SSIM)."""
    if image1.shape != image2.shape:
        raise ValueError(f"Image shapes must match for SSIM: {image1.shape} vs {image2.shape}")
    return ssim(image1, image2)

def prepare_image_for_ssim(image_data, target_shape=None, logger=None):
    """Prepare an image for SSIM, padding to target shape if needed."""
    logger = logger or logging.getLogger(__name__)
    try:
        with Image.open(BytesIO(image_data)) as img:
            img.verify()
        with Image.open(BytesIO(image_data)) as img:
            if img.mode != 'L':
                img = img.convert('L')
            img_array = np.array(img, dtype=np.float64) / 255.0
        logger.info(f"Step 1: Loaded image with shape {img_array.shape}")
        plt.imsave("step1_image_loaded.png", img_array, cmap='gray')
        logger.info("Saved Step 1: Loaded image as 'step1_image_loaded.png'")

        if img_array.size == 0 or img_array.ndim != 2:
            logger.error(f"Invalid image: size={img_array.size}, ndim={img_array.ndim}")
            return None

        if img_array.max() == img_array.min():
            logger.error("Image has no variance")
            return None
        if img_array.max() > 1.0 or img_array.min() < 0.0:
            img_array = (img_array - img_array.min()) / (img_array.max() - img_array.min())
            logger.info("Step 2: Normalized image to [0, 1] range")
        plt.imsave("step2_image_normalized.png", img_array, cmap='gray')
        logger.info("Saved Step 2: Normalized image as 'step2_image_normalized.png'")

        orig_shape = img_array.shape

        if target_shape and img_array.shape != target_shape:
            targ_h, targ_w = target_shape
            orig_h, orig_w = img_array.shape
            if targ_h > orig_h or targ_w > orig_w:
                padded = np.pad(
                    img_array,
                    ((0, targ_h - orig_h), (0, targ_w - orig_w)),
                    mode='edge'
                )
                logger.info(f"Step 3: Padded image from {orig_shape} to {target_shape} with edge values")
                img_array = padded
                plt.imsave("step3_image_padded.png", img_array, cmap='gray')
                logger.info("Saved Step 3: Padded image as 'step3_image_padded.png'")
            # No cropping here, only padding if needed
        else:
            logger.info("Step 3: No adjustment needed, shape matches target or no target provided")

        logger.info(f"Final prepared image with shape {img_array.shape}")
        plt.imsave("prepared_image.png", img_array, cmap='gray')
        logger.info("Saved final prepared image as 'prepared_image.png'")
        return img_array, orig_shape
    except Exception as e:
        logger.error(f"Image preparation failed: {e}")
        return None, None

def prepare_reference_for_ssim(reference_image, target_shape=None, logger=None):
    """Prepare a reference image for SSIM, padding to target shape if needed."""
    logger = logger or logging.getLogger(__name__)
    try:
        ref_prepared = reference_image.astype(np.float64)
        if ref_prepared.size == 0 or ref_prepared.ndim < 2:
            logger.error(f"Invalid reference image: size={ref_prepared.size}, ndim={ref_prepared.ndim}")
            return None
        if ref_prepared.ndim > 2:
            ref_prepared = np.mean(ref_prepared, axis=2)
            logger.info("Step 1: Converted reference to grayscale using mean across channels")
        logger.info(f"Step 1: Loaded reference with shape {ref_prepared.shape}")
        plt.imsave("step1_reference_loaded.png", ref_prepared, cmap='gray')
        logger.info("Saved Step 1: Loaded reference as 'step1_reference_loaded.png'")

        if ref_prepared.max() == ref_prepared.min():
            logger.error("Reference image has no variance")
            return None
        if ref_prepared.max() > 1.0 or ref_prepared.min() < 0.0:
            ref_prepared = (ref_prepared - ref_prepared.min()) / (ref_prepared.max() - ref_prepared.min())
            logger.info("Step 2: Normalized reference to [0, 1] range")
        plt.imsave("step2_reference_normalized.png", ref_prepared, cmap='gray')
        logger.info("Saved Step 2: Normalized reference as 'step2_reference_normalized.png'")

        orig_shape = ref_prepared.shape

        if target_shape and ref_prepared.shape != target_shape:
            targ_h, targ_w = target_shape
            orig_h, orig_w = ref_prepared.shape
            if targ_h > orig_h or targ_w > orig_w:
                padded = np.pad(
                    ref_prepared,
                    ((0, targ_h - orig_h), (0, targ_w - orig_w)),
                    mode='edge'
                )
                logger.info(f"Step 3: Padded reference from {orig_shape} to {target_shape} with edge values")
                ref_prepared = padded
                plt.imsave("step3_reference_padded.png", ref_prepared, cmap='gray')
                logger.info("Saved Step 3: Padded reference as 'step3_reference_padded.png'")
            # No cropping here, only padding if needed
        else:
            logger.info("Step 3: No adjustment needed, shape matches target or no target provided")

        logger.info(f"Final prepared reference with shape {ref_prepared.shape}")
        plt.imsave("ssim_reference_prepared.png", ref_prepared, cmap='gray')
        logger.info("Saved final prepared reference as 'ssim_reference_prepared.png'")
        return ref_prepared, orig_shape
    except Exception as e:
        logger.error(f"Reference preparation failed: {e}")
        return None, None

async def calculate_ssim_async(
    image_data: bytes,
    reference_image: np.ndarray,
    logger: Optional[logging.Logger] = None
) -> Tuple[float, List[str]]:
    """Calculate SSIM using full larger image shape, padding smaller image."""
    logger = logger or logging.getLogger(__name__)

    # Step 1: Prepare full images without padding
    img_prepared_full, img_orig_shape = prepare_image_for_ssim(image_data, logger=logger)
    if img_prepared_full is None:
        return -1, []

    ref_prepared_full, ref_orig_shape = prepare_reference_for_ssim(reference_image, logger=logger)
    if ref_prepared_full is None:
        return -1, []

    # Log prepared shapes
    logger.debug(f"Step 1: img_prepared_full shape: {img_prepared_full.shape}, original: {img_orig_shape}")
    logger.debug(f"Step 1: ref_prepared_full shape: {ref_prepared_full.shape}, original: {ref_orig_shape}")

    # Step 2: Determine the target shape as the larger image
    img_h, img_w = img_prepared_full.shape
    ref_h, ref_w = ref_prepared_full.shape
    if img_h * img_w >= ref_h * ref_w:
        target_shape = (img_h, img_w)
        logger.info(f"Step 2: Using full image shape {target_shape} as target (image is larger or equal)")
    else:
        target_shape = (ref_h, ref_w)
        logger.info(f"Step 2: Using full reference shape {target_shape} as target (reference is larger)")

    # Step 3: Pad both to the target shape (larger image)
    img_region, _ = prepare_image_for_ssim(image_data, target_shape=target_shape, logger=logger)
    ref_region, _ = prepare_reference_for_ssim(reference_image, target_shape=target_shape, logger=logger)

    # Log SSIM region shapes
    logger.debug(f"Step 3: img_region shape for SSIM: {img_region.shape}")
    logger.debug(f"Step 3: ref_region shape for SSIM: {ref_region.shape}")

    # Step 4: Save full unpadded images with outlines
    img_8bit = (img_prepared_full * 255).astype(np.uint8)
    ref_8bit = (ref_prepared_full * 255).astype(np.uint8)

    # Unpack original shapes
    img_orig_h, img_orig_w = img_orig_shape
    ref_orig_h, ref_orig_w = ref_orig_shape

    # Outline full original image
    cv2.rectangle(
        img_8bit,
        (0, 0),
        (img_orig_w, img_orig_h),
        (255, 0, 0),  # Red in BGR
        2
    )

    # Outline full original reference
    cv2.rectangle(
        ref_8bit,
        (0, 0),
        (ref_orig_w, ref_orig_h),
        (255, 0, 0),  # Red in BGR
        2
    )

    # Save full unpadded images with outlines
    plt.imsave("ssim_image_region.png", cv2.cvtColor(img_8bit, cv2.COLOR_GRAY2RGB))
    logger.info("Step 4: Saved full unpadded image region with outline as 'ssim_image_region.png'")
    plt.imsave("ssim_reference_region.png", cv2.cvtColor(ref_8bit, cv2.COLOR_GRAY2RGB))
    logger.info("Step 4: Saved full unpadded reference region with outline as 'ssim_reference_region.png'")

    # Step 5: Flipped reference
    ref_flipped = np.fliplr(ref_region)
    plt.imsave("ssim_reference_flipped.png", ref_flipped, cmap='gray')
    logger.info("Step 5: Saved flipped padded reference region as 'ssim_reference_flipped.png'")

    # Step 6: Calculate SSIM for original and flipped
    try:
        min_dim = min(target_shape)
        if min_dim < 7:
            logger.warning(f"Step 6: Dimension too small for SSIM (min_dim={min_dim})")
            return 0.0, ["preloaded"]
        win_size = min(11, max(7, min_dim // 2 * 2 + 1))
        logger.info(f"Step 6: Using win_size={win_size} for SSIM calculation on shape {target_shape}")

        score_original = ssim(
            img_region,
            ref_region,
            data_range=1.0,
            win_size=win_size,
            gaussian_weights=True,
            sigma=1.5,
            use_sample_covariance=False,
            K1=0.01,
            K2=0.03
        )
        logger.info(f"Step 6: SSIM score (original): {score_original}")

        score_flipped = ssim(
            img_region,
            ref_flipped,
            data_range=1.0,
            win_size=win_size,
            gaussian_weights=True,
            sigma=1.5,
            use_sample_covariance=False,
            K1=0.01,
            K2=0.03
        )
        logger.info(f"Step 6: SSIM score (flipped): {score_flipped}")

        final_score = max(score_original, score_flipped)
        used_references = ["preloaded"]
        if final_score == score_flipped:
            logger.info("Step 6: Selected flipped reference for higher SSIM score")
            used_references.append("flipped")
        else:
            logger.info("Step 6: Selected original reference for higher SSIM score")

        if not isinstance(final_score, (int, float)) or np.isnan(final_score):
            logger.error(f"Step 6: Invalid SSIM score: {final_score}")
            return -1, used_references

        logger.info(f"Step 6: Final SSIM score: {final_score}")
    except Exception as e:
        logger.error(f"Step 6: SSIM calculation failed: {e}")
        return -1, []

    # Step 7: Side-by-side comparison at SSIM scale (full larger shape)
    from PIL import Image as PILImage
    img_pil = PILImage.fromarray((img_region * 255).astype(np.uint8)).convert('RGB')
    ref_pil = PILImage.fromarray((ref_region * 255).astype(np.uint8)).convert('RGB')
    flip_pil = PILImage.fromarray((ref_flipped * 255).astype(np.uint8)).convert('RGB')

    total_width = target_shape[1] * 3
    total_height = target_shape[0]
    combined = PILImage.new('RGB', (total_width, total_height))

    combined.paste(img_pil, (0, 0))
    combined.paste(ref_pil, (target_shape[1], 0))
    combined.paste(flip_pil, (target_shape[1] * 2, 0))
    combined.save("side_by_side_comparison.png")
    logger.info(f"Step 7: Saved side-by-side comparison at full scale ({target_shape}) as 'side_by_side_comparison.png'")

    return min(max(final_score, 0), 1), used_references

# Fetch reference image
async def fetch_reference_image(
    category: str,
    session: aiohttp.ClientSession,
    logger: Optional[logging.Logger] = None
) -> Optional[np.ndarray]:
    """Fetch a consistent reference image, prioritizing preloaded references."""
    logger = logger or default_logger
    category = category.lower().strip()

    # Use preloaded reference if available
    if category in loaded_references:
        logger.info(f"Using preloaded reference for '{category}', shape={loaded_references[category].shape}")
        return loaded_references[category]

    # Fallback to parent category if subcategory isn’t preloaded
    for parent, children in category_hierarchy.items():
        if category in children and parent in loaded_references:
            logger.info(f"Using preloaded parent reference '{parent}' for '{category}'")
            return loaded_references[parent]

    # Fetch from Google Images if no preloaded match
    logger.info(f"No preloaded reference for '{category}', fetching from Google Images")
    search_url = f"https://www.google.com/search?q={category}&tbm=isch"
    try:
        async with session.get(search_url) as response:
            html_bytes = await response.read()
        html_str = html_bytes.decode('utf-8', errors='replace')
        pattern = r'"(https://encrypted-tbn0.gstatic.com/images\?[^"]+)"'
        image_urls = re.findall(pattern, html_str)
        
        if not image_urls:
            logger.error(f"No image URLs found for '{category}'")
            return None
        
        # Try up to 3 URLs for a valid image
        for url in image_urls[:3]:
            image_data, downloaded_url = await get_image_data_async([url], session, logger)
            if image_data:
                try:
                    ref_img = skio.imread(BytesIO(image_data), as_gray=True)
                    if ref_img.size > 0 and min(ref_img.shape) >= 100:  # Ensure decent quality
                        # Standardize size and range
                        ref_img_resized = resize(
                            ref_img,
                            (256, 256),
                            anti_aliasing=True,
                            preserve_range=True,
                            mode='reflect',
                            order=3
                        )
                        if ref_img_resized.max() > 1.0:
                            ref_img_resized = ref_img_resized / 255.0
                        # Cache the fetched image
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
    """Process an image with feature matching and SSIM, normalizing and mapping categories."""
    logger = logger or default_logger
    result_id = row.get("ResultID")
    if result_id is None:
        logger.error(f"Invalid row data: ResultID missing - row: {row}")
        return None, json.dumps({"error": "Invalid row data: ResultID missing"}), -1, None, 1

    logger.debug(f"Processing row for ResultID {result_id}: {row}")

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
        logger.warning(f"Gemini analysis failed for ResultID {result_id}: {gemini_result.get('text', 'No details')}")
        return result_id, json.dumps({"error": "Gemini analysis failed"}), -1, None, 1
    initial_features = gemini_result.get("features", {"description": "", "extracted_features": {}})

    # Handle None values safely in product_details
    provided_details = {
        k: v.strip().lower() for k, v in product_details.items() 
        if v is not None and isinstance(v, str) and v.strip()
    }
    if not provided_details:
        match_score = 1.0
        reasoning = "No user details provided, assuming match."
    else:
        extracted_features = {
            k: v.strip().lower() for k, v in initial_features.get("extracted_features", {}).items() 
            if v is not None and isinstance(v, str) and v.strip()
        }
        matches = {k: (extracted_features.get(k, "") == v) for k, v in provided_details.items()}
        num_features = len(provided_details)
        feature_weight = 1.0 / num_features if num_features > 0 else 0.0
        match_score = sum(feature_weight for match in matches.values() if match)
        if match_score == 1.0:
            reasoning = "All provided features match exactly."
        elif match_score == 0.0:
            reasoning = "No provided features match."
        else:
            mismatched = [k for k, v in matches.items() if not v]
            matched = [k for k, v in matches.items() if v]
            reasoning = f"Match score: {match_score:.3f}; Matched features: {', '.join(matched) if matched else 'None'}; Mismatched features: {', '.join(mismatched) if mismatched else 'None'}"

    # Step 1: Extract and normalize category
    raw_category = (product_details.get("category") or "").strip().lower() or initial_features.get("extracted_features", {}).get("category", "").lower()
    if not raw_category:
        logger.warning(f"No category provided or extracted for ResultID {result_id}, defaulting to 'unknown'")
        normalized_category = "unknown"
    else:
        # Split compound category and extract base term
        category_parts = raw_category.split()
        base_candidates = [part for part in category_parts if part]  # Remove empty parts
        normalized_category = None

        # Step 2: Normalize and map to preloaded or hierarchy
        for candidate in reversed(base_candidates):  # Start from the end (e.g., "chino" in "twill chino")
            singular = candidate[:-1] if candidate.endswith("s") else candidate
            plural = f"{candidate}s" if not candidate.endswith("s") else candidate

            # Check exact match in loaded_references first
            for form in [candidate, singular, plural]:
                if form in loaded_references:
                    normalized_category = form
                    logger.info(f"Normalized '{raw_category}' to preloaded category '{normalized_category}'")
                    break
                # Map to hierarchy and check preloaded parent
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

    # Step 3: Fetch reference image with normalized category
    logger.info(f"Fetching reference image for normalized category '{normalized_category}'")
    reference_image = await fetch_reference_image(normalized_category, session, logger)
    if reference_image is not None:
        ssim_score, used_references = await calculate_ssim_async(image_data, reference_image, logger)
    else:
        logger.warning(f"No reference image available for '{normalized_category}'")
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
            "category": normalized_category
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
    update_sort_order_based_on_match_score(file_id)
    logger.info(f"Completed FileID {file_id} at step {step}")