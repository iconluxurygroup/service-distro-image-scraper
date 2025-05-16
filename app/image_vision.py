import asyncio
import base64
import json
import logging
import re
import requests
from PIL import Image
from io import BytesIO
import google.generativeai as genai
from ultralytics import YOLO
import numpy as np
from typing import Optional, List, Tuple, Dict
from config import GOOGLE_API_KEY

# Default logger setup
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Initialize YOLOv11 models
try:
    CLS_MODEL = YOLO("yolo11m-cls.pt", task="classify", verbose=True)
    SEG_MODEL = YOLO("yolo11m-seg.pt", task="segment", verbose=True)
    default_logger.info("Successfully initialized YOLOv11 models")
except Exception as e:
    CLS_MODEL = None
    SEG_MODEL = None
    default_logger.error(
        f"YOLOv11 initialization failed: {e}. Ensure ultralytics is installed, "
        f"network access is available, and cache directory has write permissions.",
        exc_info=True
    )

FASHION_MNIST_CLASSES = [
    "T-shirt/top", "Trouser", "Pullover", "Dress", "Coat",
    "Sandal", "Shirt", "Sneaker", "Bag", "Ankle boot"
]

# Load CATEGORY_MAPPING
CATEGORY_MAPPING = None
try:
    for attempt in range(3):
        try:
            response = requests.get("https://iconluxury.group/static_settings/category_mapping.json", timeout=10)
            response.raise_for_status()
            CATEGORY_MAPPING = response.json()
            default_logger.info("Loaded CATEGORY_MAPPING")
            break
        except Exception as e:
            default_logger.warning(f"Failed to load CATEGORY_MAPPING (attempt {attempt + 1}): {e}")
            if attempt == 2:
                CATEGORY_MAPPING = {
                    "pants": "trouser", "slacks": "trouser", "jeans": "trouser", "chino": "trouser",
                    "tshirt": "t-shirt", "tee": "t-shirt", "shirt": "t-shirt", "jacket": "coat",
                    "parka": "coat", "overcoat": "coat", "sneakers": "sneaker", "running-shoe": "sneaker",
                    "athletic-shoe": "sneaker", "trainer": "sneaker", "sweatshirt": "sweater",
                    "hoodie": "sweater", "cardigan": "sweater", "jersey": "sweatshirt", "wool": "sweater",
                    "suit": "jacket", "trench-coat": "coat", "maillot": "legging", "velvet": "dress",
                    "kimono": "dress", "skinny": "trouser", "cargo-pant": "trouser", "blouse": "shirt",
                    "tank": "tank-top", "shorts": "short", "sandals": "sandal", "boots": "boot",
                    "heels": "heel", "hat": "cap", "gloves": "glove", "scarves": "scarf", "bags": "bag",
                    "T-shirt/top": "t-shirt", "Trouser": "trouser", "Pullover": "sweater", "Dress": "dress",
                    "Coat": "coat", "Sandal": "sandal", "Shirt": "t-shirt", "Sneaker": "sneaker",
                    "Bag": "bag", "Ankle boot": "boot"
                }
                default_logger.info("Using fallback CATEGORY_MAPPING")
except Exception as e:
    default_logger.error(f"Critical failure loading CATEGORY_MAPPING: {e}")
    raise

NON_FASHION_LABELS = [
    "soap_dispenser", "parking_meter", "spoon", "screw", "safety_pin", "tick",
    "ashcan", "loudspeaker", "joystick", "perfume", "car", "dog", "bench",
    "chair", "table", "sofa", "bed", "tv", "laptop", "phone", "book", "clock",
    "vase", "scissors", "teddy_bear", "hair_drier", "toothbrush", "bottle",
    "wine_glass", "cup", "fork", "knife", "bowl", "banana", "apple", "sandwich",
    "orange", "broccoli", "carrot", "hot_dog", "pizza", "donut", "cake",
    "refrigerator", "oven", "microwave", "sink", "toaster", "bus", "train",
    "truck", "boat", "traffic_light", "fire_hydrant", "stop_sign", "umbrella",
    "bird", "cat", "horse", "sheep", "cow", "elephant", "bear", "zebra", "giraffe",
    "kite", "baseball_bat", "baseball_glove", "skateboard", "surfboard", "tennis_racket"
]

async def detect_objects_with_computer_vision_async(
    image_base64: str,
    logger: Optional[logging.Logger] = None,
    max_retries: int = 3
) -> Tuple[bool, Optional[str], Optional[List[float]]]:
    logger = logger or default_logger
    if CLS_MODEL is None or SEG_MODEL is None:
        logger.error("YOLOv11 models not initialized. Using fallback description.")
        return True, "No computer vision detection available due to model initialization failure.", []

    person_confidences = []
    for attempt in range(1, max_retries + 1):
        try:
            image_bytes = base64.b64decode(image_base64)
            image = Image.open(BytesIO(image_bytes)).convert("RGB")

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

            if cls_label:
                cls_label = CATEGORY_MAPPING.get(cls_label, cls_label)

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
                        f"{label} (confidence: {score:.2f}, mask area: {mask_area} pixels) at "
                        f"bounding box [xmin: {box_coords[0]:.1f}, ymin: {box_coords[1]:.1f}, "
                        f"xmax: {box_coords[2]:.1f}, ymax: {box_coords[3]:.1f}]"
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
    Return valid JSON only as a single object.
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
                    if isinstance(features, list):
                        logger.info(f"Parsed JSON is a list; selecting first element")
                        features = features[0] if features else {}
                    if not isinstance(features, dict):
                        logger.error(f"Unexpected features type: {type(features)}")
                        features = {
                            "description": "Unable to analyze image.",
                            "extracted_features": {"brand": "Unknown", "category": "Unknown", "color": "Unknown"},
                            "match_score": 0.0,
                            "reasoning": "Unexpected response structure from Gemini."
                        }
                    match_score = float(features.get("match_score", 0.0))
                    extracted = features.get("extracted_features", {})
                    if not isinstance(extracted, dict):
                        logger.warning(f"Invalid extracted_features format: {extracted}")
                        extracted = {"brand": "Unknown", "category": "Unknown", "color": "Unknown"}

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

    features = {
        "description": "Analysis failed after retries.",
        "extracted_features": {"brand": "Unknown", "category": "Unknown", "color": "Unknown"},
        "match_score": 0.0,
        "reasoning": "Max retries exceeded."
    }
    return {"status_code": 200, "success": False, "features": features}