import asyncio
import base64
import json
import logging
from io import BytesIO
from typing import Any, Dict, List, Tuple

import aiohttp
import google.generativeai as genai
from PIL import Image, ImageFile
from ultralytics import YOLO

# --- Assumed Project Structure ---
# It is assumed you have these two files in your project.
# config.py should contain: GOOGLE_API_KEY = "your_google_api_key"
# common.py should contain the load_config function.
from config import GOOGLE_API_KEY
from common import load_config

# Allows PIL to load truncated or malformed images, a common issue with web images.
ImageFile.LOAD_TRUNCATED_IMAGES = True

# --- Default Logger Setup ---
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

# --- Global Models & Configs (Initialized ONCE at startup) ---
YOLO_MODEL = None
FASHION_LABELS = []
NON_FASHION_LABELS = []
CATEGORY_MAPPING = {}


async def initialize_models_and_config():
    """
    Initializes the YOLOv8 model and loads all necessary configurations.
    This is a critical step for performance, ensuring models are not reloaded on every call.
    """
    global YOLO_MODEL, FASHION_LABELS, NON_FASHION_LABELS, CATEGORY_MAPPING

    # 1. Initialize YOLOv8 Object Detection Model
    try:
        # yolov8s-seg.pt is a good balance of speed and accuracy. It can detect and segment objects.
        default_logger.info("Initializing YOLOv8 model (yolov8s-seg.pt)...")
        YOLO_MODEL = YOLO("yolov8s-seg.pt")
        default_logger.info("YOLOv8 model initialized successfully.")
    except Exception as e:
        default_logger.error(
            f"Fatal: YOLO model initialization failed: {e}. "
            "Computer vision steps will be skipped. Ensure 'yolov8s-seg.pt' is accessible.",
            exc_info=True,
        )
        # If the core CV model fails, the service cannot function.
        raise SystemExit("Could not initialize the primary vision model.") from e

    # 2. Load Application-Specific Labels and Mappings from Configuration
    # These configurations can be stored in a JSON file and fetched by `load_config`.
    FALLBACK_FASHION_LABELS = [
        "shirt", "pant", "trouser", "dress", "shoe", "boot", "sneaker",
        "bag", "hat", "jacket", "coat", "sweater", "jean", "hoodie", "footwear",
    ]
    FALLBACK_NON_FASHION = ["person", "car", "dog", "chair", "table", "bed", "sofa", "plant"]
    FALLBACK_MAPPING = {"t-shirt/top": "t-shirt", "trouser": "pants", "pullover": "sweater"}

    FASHION_LABELS = await load_config(
        "fashion_labels", FALLBACK_FASHION_LABELS, default_logger, expect_list=True
    )
    NON_FASHION_LABELS = await load_config(
        "non_fashion_labels", FALLBACK_NON_FASHION, default_logger, expect_list=True
    )
    CATEGORY_MAPPING = await load_config(
        "category_mapping", FALLBACK_MAPPING, default_logger
    )


# Run the one-time initialization when the module is first imported.
asyncio.run(initialize_models_and_config())


# --- Internal Helper Functions ---

async def _download_image(url: str, session: aiohttp.ClientSession) -> bytes:
    """Asynchronously downloads image data from a URL with a timeout."""
    async with session.get(url, timeout=20) as response:
        response.raise_for_status()
        return await response.read()


def _run_yolo_analysis(image_bytes: bytes) -> Dict[str, Any]:
    """
    Processes an image with YOLOv8 to detect objects.
    This is a CPU/GPU-bound function and must be run in a thread to avoid blocking asyncio.
    """
    if not YOLO_MODEL:
        raise RuntimeError("YOLOv8 model is not available for analysis.")

    image = Image.open(BytesIO(image_bytes)).convert("RGB")
    # verbose=False keeps the console logs clean during batch processing
    results = YOLO_MODEL(image, verbose=False)

    detected_objects = []
    person_detected = False

    result = results[0]  # Get results for the first (and only) image
    if result.boxes:
        for box in result.boxes:
            label = result.names[int(box.cls[0])]
            confidence = float(box.conf[0])

            # Filter out low-confidence detections to reduce noise
            if confidence < 0.35:
                continue

            # Check if a person is detected, which influences shot quality
            if label == "person":
                person_detected = True

            # Normalize and map category names using our config
            processed_label = CATEGORY_MAPPING.get(label, label)

            detected_objects.append(
                {"label": processed_label, "confidence": round(confidence, 3)}
            )

    # Sort detections by confidence so the most likely objects are listed first
    detected_objects.sort(key=lambda x: x["confidence"], reverse=True)

    return {"person_detected": person_detected, "detected_objects": detected_objects}


async def _call_gemini_api(
    image_base64: str, prompt: str, logger: logging.Logger
) -> Dict:
    """Calls the Google Gemini API with robust error handling and retries."""
    if not GOOGLE_API_KEY:
        return {"success": False, "analysis": {"error": "Google API Key not configured."}}

    genai.configure(api_key=GOOGLE_API_KEY)
    model = genai.GenerativeModel("gemini-1.5-flash")
    image_part = {"mime_type": "image/jpeg", "data": base64.b64decode(image_base64)}

    for attempt in range(3):  # Retry logic
        try:
            response = await model.generate_content_async(
                [image_part, prompt],
                generation_config={"response_mime_type": "application/json"},
            )
            # Clean up potential markdown wrappers from the response
            cleaned_text = response.text.strip().lstrip("```json").rstrip("```").strip()
            return {"success": True, "analysis": json.loads(cleaned_text)}
        except Exception as e:
            logger.warning(f"Gemini API call attempt {attempt + 1} failed: {e}")
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s

    return {
        "success": False,
        "analysis": {"error": "Gemini API failed after multiple retries."},
    }


# --- PRIMARY PUBLIC FUNCTION ---


async def analyze_single_image_record(
    record: Dict, logger: logging.Logger
) -> Tuple[str, bool, str]:
    """
    Analyzes a single image record using a powerful multi-stage AI pipeline.
    This is the main entry point to be called by a batch processor.

    The pipeline is as follows:
    1.  **Download Image**: Fetches the image from its URL.
    2.  **Local CV Analysis (YOLOv8)**: Detects all objects in the image, noting if a person is present.
    3.  **LLM Reasoning (Gemini)**: A sophisticated prompt sends the image, the YOLO findings,
        and the expected product context to Gemini. It's tasked with:
        -   Performing semantic/multilingual matching (the "BabelNet" concept).
        -   Determining the final, specific product category.
        -   Scoring relevance and image quality.
        -   Generating a marketing description.
    4.  **Consolidate Results**: The final analysis from Gemini is structured into a JSON payload.

    Args:
        record: A dictionary containing at least `ResultID` and `ImageUrl`, plus product context.
        logger: A logger instance for logging progress and errors.

    Returns:
        A tuple containing: (final_ai_json_string, is_fashion_boolean, final_caption_string).
        On failure, returns a JSON with an error message and a default failure caption.
    """
    result_id = record.get("ResultID")
    image_url = record.get("ImageUrl")

    # Standardized failure response
    failure_json = json.dumps(
        {"error": "Processing failed due to an unexpected error.", "scores": {}}
    )
    failure_response = (failure_json, False, "Error: AI processing failed.")

    if not result_id or not image_url:
        logger.error(f"Record is missing 'ResultID' or 'ImageUrl'. Record: {record}")
        return failure_response

    try:
        # Step 1: Download Image
        async with aiohttp.ClientSession() as session:
            image_data = await _download_image(image_url, session)
        image_base64 = base64.b64encode(image_data).decode("utf-8")

        # Step 2: Local Computer Vision - Run YOLOv8 in a separate thread
        yolo_analysis = await asyncio.to_thread(_run_yolo_analysis, image_data)

        # Step 3: Prepare a rich, structured prompt for the Gemini Reasoning Engine
        product_context = {
            "expected_category": record.get("ProductCategory", "N/A"),
            "brand": record.get("ProductBrand", "N/A"),
            "model": record.get("ProductModel", "N/A"),
        }
        detected_objects_str = json.dumps(yolo_analysis["detected_objects"])

        # This is the "secret sauce": a detailed prompt that guides the LLM's reasoning
        gemini_prompt = f"""
        You are an expert e-commerce analyst specializing in fashion. Your task is to analyze the provided image and contextual data to produce a structured JSON output.

        **CONTEXTUAL INFORMATION:**
        - **Expected Product:** My database says this should be a '{product_context['expected_category']}' from brand '{product_context['brand']}'.
        - **Local Vision Analysis (YOLOv8):** My preliminary vision model detected the following objects in the image: {detected_objects_str}.
        - **Person Detected in Image:** {yolo_analysis['person_detected']}

        **YOUR REASONING TASKS & JSON OUTPUT DEFINITION:**
        Based on ALL the information above, return a single, valid JSON object with the following structure. Do NOT include markdown formatting or any text outside the JSON.

        {{
          "final_category": "Your final, most specific category for the main product shown. E.g., 'quilted leather jacket', 'high-top canvas sneakers', 'denim jeans'.",
          "is_match": "Boolean (true/false). Is the main item in the image a match for the 'Expected Product' category? Critically evaluate this, considering synonyms, translations (e.g., 'camisa' is 'shirt'), and hierarchies (e.g., a 'sneaker' is a 'shoe').",
          "relevance_score": "Float (0.0-1.0). How relevant is the image to the 'Expected Product'? Score high if it's a clear shot of the correct item. Score lower if it's the wrong item, blurry, or cluttered.",
          "shot_quality_score": "Float (0.0-1.0). A score for the image quality as a product shot. High scores (0.8-1.0) for clean, studio shots on plain backgrounds. Mid scores for clean lifestyle shots. Low scores (0.0-0.4) for blurry, dark, busy images, or if a person's face is the main focus.",
          "description": "A compelling, one-sentence marketing description for the item shown.",
          "reasoning": "A brief but clear justification for your scores and decisions, referencing the vision analysis and context."
        }}
        """

        # Step 4: Call Gemini for the final reasoning and analysis step
        gemini_result = await _call_gemini_api(image_base64, gemini_prompt, logger)

        if not gemini_result["success"]:
            failure_reason = gemini_result["analysis"].get(
                "error", "Gemini reasoning failed."
            )
            ai_json = json.dumps(
                {
                    "error": failure_reason,
                    "yolo_analysis": yolo_analysis,
                    "scores": {},
                }
            )
            return (ai_json, False, f"Error: {failure_reason}")

        analysis = gemini_result["analysis"]

        # Step 5: Consolidate results into the final AiJson payload
        # Check if the final category determined by the AI is considered a fashion item
        final_category_lower = analysis.get("final_category", "").lower()
        is_fashion = any(label in final_category_lower for label in FASHION_LABELS)

        # Create the final scores dictionary
        final_scores = {
            "relevance": round(float(analysis.get("relevance_score", 0.0)), 3),
            "shot_quality": round(float(analysis.get("shot_quality_score", 0.0)), 3),
            "is_match": bool(analysis.get("is_match", False)),
        }

        # Assemble the comprehensive JSON to be stored in the database
        final_ai_json = {
            "scores": final_scores,
            "category": analysis.get("final_category", "unknown"),
            "description": analysis.get("description", "No description available."),
            "reasoning": analysis.get("reasoning", "No reasoning available."),
            "preliminary_cv_analysis": yolo_analysis,  # Store raw CV output for data analysis
            "result_id": result_id,
        }

        final_caption = analysis.get("description", "AI-generated description.")

        logger.info(
            f"Successfully processed ResultID {result_id}. Category: '{final_ai_json['category']}'. Scores: {final_scores}"
        )
        return json.dumps(final_ai_json), is_fashion, final_caption

    except Exception as e:
        logger.error(
            f"Unhandled exception in analyze_single_image_record for ResultID {result_id}: {e}",
            exc_info=True,
        )
        return failure_response