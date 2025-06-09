import asyncio
import base64
import json
import logging
from io import BytesIO
from typing import Dict, List, Tuple

import aiohttp
import google.generativeai as genai
from PIL import Image, ImageFile
from ultralytics import YOLO

from config import GOOGLE_API_KEY # Assumes you have this in your config
from common import load_config # Assumes you have this helper in common.py

# Allows PIL to load truncated images, which are common on the web
ImageFile.LOAD_TRUNCATED_IMAGES = True

# --- Default Logger ---
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# --- Global Models and Configurations (Initialized once) ---
YOLO_CLS_MODEL = None
YOLO_SEG_MODEL = None
CATEGORY_MAPPING = {}
NON_FASHION_LABELS = []
FASHION_LABELS = []

async def initialize_models_and_config():
    """
    Initializes AI models and loads necessary configurations asynchronously.
    This function is called once when the module is first imported.
    """
    global YOLO_CLS_MODEL, YOLO_SEG_MODEL, CATEGORY_MAPPING, NON_FASHION_LABELS, FASHION_LABELS
    
    # --- Initialize YOLO Models ---
    try:
        # Using a smaller, faster model is often sufficient and better for production.
        default_logger.info("Initializing YOLO models (yolov8s-cls.pt, yolov8s-seg.pt)...")
        YOLO_CLS_MODEL = YOLO("yolov8s-cls.pt")
        YOLO_SEG_MODEL = YOLO("yolov8s-seg.pt")
        default_logger.info("YOLO models initialized successfully.")
    except Exception as e:
        default_logger.error(
            f"Fatal: YOLO model initialization failed: {e}. "
            "Computer vision steps will be skipped. Ensure model files are accessible.",
            exc_info=True
        )

    # --- Load Configuration from remote/local files ---
    # Define fallbacks in case remote loading fails.
    FALLBACK_CATEGORY_MAPPING = {"t-shirt/top": "t-shirt", "trouser": "pants", "pullover": "sweater"}
    FALLBACK_NON_FASHION_LABELS = ["car", "dog", "chair", "table", "person"]
    FALLBACK_FASHION_LABELS = ["shirt", "pants", "dress", "shoe", "bag", "hat", "jacket", "coat"]

    CATEGORY_MAPPING = await load_config("category_mapping", FALLBACK_CATEGORY_MAPPING, default_logger)
    NON_FASHION_LABELS = await load_config("non_fashion_labels", FALLBACK_NON_FASHION_LABELS, default_logger, expect_list=True)
    FASHION_LABELS = await load_config("fashion_labels", FALLBACK_FASHION_LABELS, default_logger, expect_list=True)


# Initialize everything when the module is loaded.
asyncio.run(initialize_models_and_config())


async def _download_image(url: str, session: aiohttp.ClientSession, logger: logging.Logger) -> bytes:
    """Downloads image data from a URL."""
    async with session.get(url, timeout=15) as response:
        response.raise_for_status()
        return await response.read()

async def _run_yolo_detection(image_bytes: bytes, logger: logging.Logger) -> Tuple[str, List[float]]:
    """Runs YOLO classification and segmentation on image bytes."""
    if not YOLO_CLS_MODEL or not YOLO_SEG_MODEL:
        return "Computer vision models not available.", []

    try:
        image = Image.open(BytesIO(image_bytes)).convert("RGB")
        
        # Run synchronous, CPU-bound YOLO models in a separate thread to not block the event loop.
        cls_results, seg_results = await asyncio.to_thread(
            lambda: (YOLO_CLS_MODEL(image, verbose=False), YOLO_SEG_MODEL(image, verbose=False))
        )

        # Process Classification Result
        cls_probs = cls_results[0].probs
        cls_label = cls_results[0].names[cls_probs.top1]
        cls_conf = float(cls_probs.top1conf)
        cls_label_processed = CATEGORY_MAPPING.get(cls_label, cls_label) if cls_conf > 0.3 and cls_label not in NON_FASHION_LABELS else "unknown"

        # Process Segmentation Result
        detected_objects, person_confidences = [], []
        if seg_results and seg_results[0].masks:
            for box in seg_results[0].boxes:
                label = seg_results[0].names[int(box.cls[0])]
                score = float(box.conf[0])
                if score < 0.3: continue
                
                if label == "person":
                    person_confidences.append(score)
                elif label not in NON_FASHION_LABELS:
                    processed_label = CATEGORY_MAPPING.get(label, label)
                    detected_objects.append(f"{processed_label} ({score:.2f})")
        
        description = f"Primary object: {cls_label_processed}. Other items: {', '.join(detected_objects) if detected_objects else 'None'}."
        return description, person_confidences

    except Exception as e:
        logger.error(f"Error during YOLO processing: {e}", exc_info=True)
        return "CV processing error.", []


async def _call_gemini_api(image_base64: str, prompt: str, logger: logging.Logger) -> Dict:
    """Calls the Google Gemini API with built-in retries and error handling."""
    if not GOOGLE_API_KEY:
        return {"success": False, "features": {"error": "Google API Key not configured."}}

    genai.configure(api_key=GOOGLE_API_KEY)
    model = genai.GenerativeModel("gemini-1.5-flash")
    image_part = {"mime_type": "image/jpeg", "data": base64.b64decode(image_base64)}

    for attempt in range(3):
        try:
            response = await model.generate_content_async(
                [image_part, prompt],
                generation_config={"response_mime_type": "application/json"}
            )
            # Clean the response which might be wrapped in markdown
            cleaned_text = response.text.strip().replace("```json", "").replace("```", "").strip()
            return {"success": True, "features": json.loads(cleaned_text)}
        except Exception as e:
            logger.warning(f"Gemini API call attempt {attempt + 1} failed: {e}")
            if attempt < 2: await asyncio.sleep(2)
    
    return {"success": False, "features": {"error": "Gemini API failed after multiple retries."}}


# --- PRIMARY PUBLIC FUNCTION ---
async def analyze_single_image_record(record: Dict, logger: logging.Logger) -> Tuple[str, bool, str]:
    """
    Analyzes a single image record from the database. This is the main entry point.

    Orchestrates downloading, computer vision (YOLO), and generative AI (Gemini)
    to produce a comprehensive analysis encapsulated in a JSON object.

    Args:
        record: A dictionary representing a single row from `utb_ImageScraperResult`
                joined with `utb_ImageScraperRecords`.
        logger: The logger instance for the job.

    Returns:
        A tuple containing (ai_json_string, is_fashion_boolean, ai_caption_string).
        On failure, returns a JSON with an error and a descriptive failure caption.
    """
    result_id = record.get("ResultID")
    image_url = record.get("ImageUrl")
    
    failure_json = json.dumps({"error": "Processing failed due to an unexpected error.", "scores": {}})
    failure_response = (failure_json, False, "Error: AI processing failed.")

    if not result_id or not image_url:
        logger.error(f"Record is missing 'ResultID' or 'ImageUrl'. Record: {record}")
        return failure_response

    try:
        # Step 1: Download the image
        async with aiohttp.ClientSession() as session:
            image_data = await _download_image(image_url, session, logger)
        
        image_base64 = base64.b64encode(image_data).decode("utf-8")

        # Step 2: Run local Computer Vision (YOLO)
        cv_description, person_confidences = await _run_yolo_detection(image_data, logger)

        # Step 3: Prepare context and prompt for Generative AI (Gemini)
        product_details = {
            "brand": record.get("ProductBrand", "N/A"),
            "category": record.get("ProductCategory", "N/A"),
            "model": record.get("ProductModel", "N/A"),
            "color": record.get("ProductColor", "N/A"),
        }

        gemini_prompt = f"""
        Analyze the provided image based on the following context and return a single, valid JSON object.
        Context:
        - Expected Product: Category='{product_details['category']}', Brand='{product_details['brand']}', Model='{product_details['model']}'.
        - My initial computer vision scan found: "{cv_description}".

        Task: Based on all available information, provide your analysis in the following JSON format ONLY:
        {{
          "description": "A crisp, one-sentence marketing description of the product shown.",
          "extracted_category": "Your best guess for the item's specific category (e.g., 'leather jacket', 'running shoes').",
          "relevance_score": "A float from 0.0 to 1.0 on how well the image matches the 'Expected Product' context.",
          "product_shot_quality_score": "A float from 0.0 to 1.0 evaluating if this is a good product photo (high score for clean, solid backgrounds; low score for lifestyle shots or images with people).",
          "reasoning": "A brief justification for your scores."
        }}
        """

        # Step 4: Call Generative AI (Gemini)
        gemini_result = await _call_gemini_api(image_base64, gemini_prompt, logger)

        if not gemini_result["success"]:
            # If Gemini fails, we still have the CV results.
            failure_reason = gemini_result["features"].get("error", "Gemini analysis failed.")
            ai_json = json.dumps({"error": failure_reason, "cv_description": cv_description, "scores": {}})
            return (ai_json, False, f"Error: {failure_reason}")

        features = gemini_result["features"]

        # Step 5: Consolidate results and calculate final scores
        is_fashion = features.get("extracted_category", "unknown").lower() in FASHION_LABELS

        final_scores = {
            "relevance": round(float(features.get("relevance_score", 0.0)), 2),
            "sentiment": round(float(features.get("product_shot_quality_score", 0.0)), 2),
            "category_match": 1.0 if is_fashion else 0.1,
        }

        # Step 6: Assemble the final AiJson payload
        final_ai_json = {
            "scores": final_scores,
            "category": features.get("extracted_category", "unknown"),
            "description": features.get("description", "No description available."),
            "reasoning": features.get("reasoning", "No reasoning available."),
            "cv_detection": cv_description,
            "person_detected": bool(person_confidences),
            "result_id": result_id,
        }

        final_caption = features.get("description", "AI-generated description.")
        
        logger.info(f"Successfully processed ResultID {result_id}. Scores: {final_scores}")
        return json.dumps(final_ai_json), is_fashion, final_caption

    except Exception as e:
        logger.error(f"Unhandled exception in analyze_single_image_record for ResultID {result_id}: {e}", exc_info=True)
        return failure_response