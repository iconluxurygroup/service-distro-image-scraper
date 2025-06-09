import asyncio
import base64
import json
import logging
from collections import defaultdict
from io import BytesIO
from typing import Any, Dict, List, Tuple

import aiohttp
import google.generativeai as genai
import imagehash
import numpy as np
from PIL import Image, ImageFilter, ImageFile
from ultralytics import YOLO

# --- Assumed Project Structure & Configs ---
from config import GOOGLE_API_KEY
from common import load_config


ImageFile.LOAD_TRUNCATED_IMAGES = True

# --- Default Logger Setup ---
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# --- Global Models & Configs (Initialized ONCE at startup) ---
YOLO_MODEL = None
FASHION_LABELS = []
CATEGORY_MAPPING = {}
IMAGE_HASH_THRESHOLD = 5 # Hamming distance; lower is more strict.
CR_HASH_SIMILARITY_THRESHOLD = 0.75

async def initialize_models_and_config():
    """Initializes models and loads configurations. Run once at startup."""
    global YOLO_MODEL, FASHION_LABELS, CATEGORY_MAPPING
    if YOLO_MODEL: return
        
    default_logger.info("Initializing YOLOv8 model (yolov8s-seg.pt)...")
    try:
        YOLO_MODEL = YOLO("yolov8s-seg.pt")
        default_logger.info("Fusing YOLOv8 model for performance optimization...")
        YOLO_MODEL.model.fuse()
        default_logger.info("YOLOv8 model initialized and fused successfully.")
    except Exception as e:
        default_logger.error(f"Fatal: YOLO model initialization failed: {e}", exc_info=True)
        raise SystemExit("Could not initialize the primary vision model.") from e

    FALLBACK_FASHION_LABELS = ["shirt", "pant", "dress", "shoe", "bag", "hat", "jacket", "jean", "hoodie"]
    FALLBACK_MAPPING = {"t-shirt/top": "t-shirt", "trouser": "pants", "pullover": "sweater"}
    FASHION_LABELS = await load_config("fashion_labels", FALLBACK_FASHION_LABELS, default_logger, expect_list=True)
    CATEGORY_MAPPING = await load_config("category_mapping", FALLBACK_MAPPING, default_logger)

# Run initialization when module is imported in a server context
if __name__ != "__main__":
    try:
        # This check ensures it runs only once per worker process.
        if not YOLO_MODEL:
            asyncio.run(initialize_models_and_config())
    except Exception as e:
        default_logger.error(f"Could not run async initialization in module scope: {e}")


# --- Internal Helper Functions ---
def _calculate_image_metrics(image_bytes: bytes) -> Dict[str, Any]:
    """Calculates crop-resistant hash and sharpness for an image."""
    try:
        img = Image.open(BytesIO(image_bytes))
        if 'A' in img.getbands() or 'a' in img.getbands():
            img = img.convert('RGBA').convert('RGB')
        else:
            img = img.convert('RGB')

        # >>> CHANGE 2: Calculate crop_resistant_hash instead of phash
        # These parameters are a good starting point. Higher segmentation_image_size
        # can find smaller details but is slower.
        cr_hash = imagehash.crop_resistant_hash(
            img, min_segment_size=500, segmentation_image_size=1000
        )

        # Sharpness calculation remains the same
        gray_img = img.convert('L')
        laplacian_kernel = ImageFilter.Kernel((3, 3), [0, 1, 0, 1, -4, 1, 0, 1, 0], scale=1, offset=0)
        laplacian_filtered_img = gray_img.filter(laplacian_kernel)
        sharpness = np.array(laplacian_filtered_img).var()
        
        return {"cr_hash": cr_hash, "sharpness": sharpness, "success": True}
    except Exception as e:
        default_logger.warning(f"Could not calculate image metrics: {e}", exc_info=True)
        return {"cr_hash": None, "sharpness": 0.0, "success": False}

async def _preprocess_image(record: Dict, session: aiohttp.ClientSession, logger: logging.Logger) -> Dict:
    """Downloads, hashes, and calculates sharpness for a single image record."""
    result_id = record.get("ResultID")
    image_url = record.get("ImageUrl")
    if not image_url:
        return {"record": record, "status": "missing_url"}
    try:
        timeout = aiohttp.ClientTimeout(total=45)
        async with session.get(image_url, timeout=timeout) as response:
            response.raise_for_status()
            image_data = await response.read()
    except Exception as e:
        logger.warning(f"ResultID {result_id}: Failed to download image from {image_url}: {type(e).__name__} - {e}")
        return {"record": record, "status": "download_failed"}

    metrics = await asyncio.to_thread(_calculate_image_metrics, image_data)
    if not metrics["success"]:
        return {"record": record, "status": "processing_failed"}
        
    return {
        "record": record,
        "status": "success",
        "image_data": image_data,
        "cr_hash": metrics["cr_hash"], # Changed from "phash"
        "sharpness": metrics["sharpness"],
    }
    
def _are_cr_hashes_similar(hash1_str: str, hash2_str: str, threshold: float) -> bool:
    """Compares two crop-resistant hashes stored as strings."""
    if not hash1_str or not hash2_str:
        return False
    try:
        # Recreate the multihash objects from their string representations
        h1 = imagehash.hex_to_multihash(hash1_str)
        h2 = imagehash.hex_to_multihash(hash2_str)

        # .hash_diff returns (matches, total_segments_in_h1, total_segments_in_h2)
        matches, seg1, seg2 = h1.hash_diff(h2)

        # Don't compare empty hashes
        if seg1 == 0 or seg2 == 0:
            return False

        # Calculate similarity as the ratio of matches to the number of segments in the SMALLER image.
        # This correctly identifies if a small image is contained within a larger one.
        similarity_score = matches / min(seg1, seg2)
        
        return similarity_score >= threshold
    except Exception as e:
        default_logger.error(f"Error comparing crop-resistant hashes: {e}")
        return False

def _run_yolo_analysis(image_bytes: bytes) -> Dict[str, Any]:
    """Processes an image with YOLOv8 to detect objects."""
    if not YOLO_MODEL: raise RuntimeError("YOLO model not initialized.")
    image = Image.open(BytesIO(image_bytes)).convert("RGB")
    # The model is already fused, so this call is now safe in a multi-process environment.
    results = YOLO_MODEL(image, verbose=False)[0]
    
    detected_objects, person_detected = [], False
    if results.boxes:
        for box in results.boxes:
            if float(box.conf[0]) < 0.35: continue
            label = results.names[int(box.cls[0])]
            if label == "person": person_detected = True
            processed_label = CATEGORY_MAPPING.get(label, label)
            detected_objects.append({"label": processed_label, "confidence": round(float(box.conf[0]), 3)})
            
    detected_objects.sort(key=lambda x: x["confidence"], reverse=True)
    return {"person_detected": person_detected, "detected_objects": detected_objects}

async def _perform_full_ai_analysis(image_data: bytes, record: Dict, logger: logging.Logger) -> Dict:
    """Runs the full YOLO + Gemini pipeline on a single image. Returns a structured result."""
    try:
        yolo_analysis = await asyncio.to_thread(_run_yolo_analysis, image_data)
        product_context = {"expected_category": record.get("ProductCategory", "N/A"), "brand": record.get("ProductBrand", "N/A")}
        image_base64 = base64.b64encode(image_data).decode("utf-8")
        
        gemini_prompt = f"""
        You are an expert e-commerce analyst specializing in fashion. Your task is to analyze the provided image and contextual data to produce a structured JSON output.

        **CONTEXTUAL INFORMATION:**
        - **Expected Product:** My database says this should be a '{product_context['expected_category']}' from brand '{product_context['brand']}'.
        - **Local Vision Analysis (YOLOv8):** My preliminary vision model detected the following objects in the image: {json.dumps(yolo_analysis["detected_objects"])}.
        - **Person Detected in Image:** {yolo_analysis["person_detected"]}

        **YOUR REASONING TASKS & JSON OUTPUT DEFINITION:**
        Based on ALL the information above, return a single, valid JSON object with the following structure. Do NOT include markdown formatting or any text outside the JSON.

        {{
          "final_category": "Your final, most specific category for the main product shown. E.g., 'quilted leather jacket', 'high-top canvas sneakers', 'denim jeans'.",
          "is_match": "Boolean (true/false). Is the main item in the image a match for the 'Expected Product' category? Critically evaluate this, considering synonyms and hierarchies.",
          "relevance_score": "Float (0.0-1.0). How relevant is the image to the 'Expected Product'? Score high for a clear shot of the correct item. Score lower for wrong items, blurriness, or clutter.",
          "shot_quality_score": "Float (0.0-1.0). A score for image quality as a product shot. High scores for clean, studio shots. Mid scores for lifestyle shots. Low scores for blurry, dark, busy images.",
          "description": "A compelling, one-sentence marketing description for the item shown.",
          "reasoning": "A brief but clear justification for your scores and decisions."
        }}
        """

        genai.configure(api_key=GOOGLE_API_KEY)
        model = genai.GenerativeModel("gemini-1.5-flash")
        image_part = {"mime_type": "image/jpeg", "data": base64.b64decode(image_base64)}

        for attempt in range(3):
            try:
                response = await model.generate_content_async(
                    [image_part, gemini_prompt],
                    generation_config={"response_mime_type": "application/json"},
                )
                analysis = json.loads(response.text.strip().lstrip("```json").rstrip("```").strip())
                
                final_scores = {
                    "relevance": round(float(analysis.get("relevance_score", 0.0)), 3),
                    "shot_quality": round(float(analysis.get("shot_quality_score", 0.0)), 3),
                    "is_match": bool(analysis.get("is_match", False)),
                }
                return {
                    "success": True, "scores": final_scores, "category": analysis.get("final_category", "unknown"),
                    "description": analysis.get("description", ""), "reasoning": analysis.get("reasoning", ""),
                    "preliminary_cv_analysis": yolo_analysis,
                }
            except Exception as e:
                logger.warning(f"ResultID {record.get('ResultID')}: Gemini API attempt {attempt+1} failed: {e}")
                if attempt < 2: await asyncio.sleep(2**attempt)
    except Exception as e:
        logger.error(f"ResultID {record.get('ResultID')}: Unhandled error in full AI analysis: {e}", exc_info=True)

    return {"success": False, "error": "AI analysis pipeline failed."}


# --- PRIMARY PUBLIC FUNCTION ---

async def process_batch_for_relevance(records: List[Dict], logger: logging.Logger) -> List[Tuple[str, bool, str]]:
    logger.info(f"Starting batch process for {len(records)} records.")
    all_final_results = []
    
    # --- Stage 1: Pre-process all images (Download, Hash, Sharpness) ---
    logger.info("Stage 1: Downloading and pre-processing all images...")
    async with aiohttp.ClientSession() as session:
        tasks = [_preprocess_image(rec, session, logger) for rec in records]
        processed_items = await asyncio.gather(*tasks)

    successful_items = [item for item in processed_items if item["status"] == "success"]
    failed_items = [item for item in processed_items if item["status"] != "success"]
    logger.info(f"Successfully pre-processed {len(successful_items)} images. {len(failed_items)} failed.")

    for item in failed_items:
        error_json = json.dumps({"error": f"Pre-processing failed: {item['status']}", "result_id": item['record']['ResultID']})
        all_final_results.append({"output": (error_json, False, "Error: Image could not be processed."), "sort_score": (-1.0, -1.0)})

    if not successful_items:
        logger.warning("No images were successfully pre-processed. Aborting further analysis.")
        if all_final_results:
            return [res["output"] for res in sorted(all_final_results, key=lambda x: x["sort_score"], reverse=True)]
        return []

    # --- Stage 2: Group images by hash similarity ---
    logger.info("Stage 2: Grouping images by visual similarity using crop-resistant hash...")
    groups = []
    processed_ids = set()
    
    # Convert hash objects to strings once for efficient comparison
    for item in successful_items:
        item['cr_hash_str'] = str(item['cr_hash'])

    for item in successful_items:
        item_id = item['record']['ResultID']
        if item_id in processed_ids: continue
        
        current_group = [item]
        processed_ids.add(item_id)
        
        # >>> CHANGE 4: Use the new comparison logic
        for other_item in successful_items:
            other_id = other_item['record']['ResultID']
            if other_id in processed_ids: continue
            
            # Use the dedicated comparison function
            if _are_cr_hashes_similar(item['cr_hash_str'], other_item['cr_hash_str'], CR_HASH_SIMILARITY_THRESHOLD):
                current_group.append(other_item)
                processed_ids.add(other_id)
        groups.append(current_group)
    logger.info(f"Identified {len(groups)} unique image groups from {len(successful_items)} images.")
    # --- Stage 3 & 4: Select Representatives and Analyze Them ---
    logger.info("Stage 3 & 4: Analyzing sharpest representative from each group...")
    representatives = [max(group, key=lambda x: x['sharpness']) for group in groups]
    analysis_tasks = [_perform_full_ai_analysis(rep['image_data'], rep['record'], logger) for rep in representatives]
    analysis_results = await asyncio.gather(*analysis_tasks)
    
    rep_analysis_map = {rep['record']['ResultID']: result for rep, result in zip(representatives, analysis_results)}
    logger.info("Finished analysis for all group representatives.")

    # --- Stage 5 & 6: Propagate Results and Build Rich AiJson for All ---
    logger.info("Stage 5 & 6: Propagating results and building final JSON for all items...")
    for group in groups:
        representative = max(group, key=lambda x: x['sharpness'])
        rep_id = representative['record']['ResultID']
        analysis_core = rep_analysis_map.get(rep_id, {"success": False, "error": "Representative analysis failed."})

        sort_score = (-1.0, -1.0)
        if analysis_core.get("success"):
            scores = analysis_core.get("scores", {})
            sort_score = (scores.get("relevance", 0.0), scores.get("shot_quality", 0.0))

        for member_item in group:
            final_json = analysis_core.copy()
            final_json["result_id"] = member_item['record']['ResultID'] # Ensure correct ID
            
            final_json["group_info"] = {
                "phash": str(member_item['phash']),
                "is_representative": member_item['record']['ResultID'] == rep_id,
                "analysis_source_id": rep_id,
                "hamming_distance_from_source": int(member_item['phash'] - representative['phash']),
            }

            is_fashion = False
            if final_json.get("success"):
                category_lower = final_json.get("category", "").lower()
                is_fashion = any(label in category_lower for label in FASHION_LABELS)

            caption = final_json.get("description", "AI-generated description.") if final_json.get("success") else "Error: Analysis failed."
            output_tuple = (json.dumps(final_json), is_fashion, caption)
            all_final_results.append({"output": output_tuple, "sort_score": sort_score})

    # --- Stage 7: Rank All Results and Return ---
    logger.info("Stage 7: Ranking all results based on group representative's score.")
    all_final_results.sort(key=lambda x: x["sort_score"], reverse=True)
    
    return [res["output"] for res in all_final_results]