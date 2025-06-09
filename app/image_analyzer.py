import asyncio
import base64
import json
import logging
from io import BytesIO
from typing import Dict, List, Tuple

import aiohttp
import google.generativeai as genai
import spacy
import torch
from PIL import Image, ImageFile
from torchvision import models, transforms

from config import GOOGLE_API_KEY
from common import load_config

# Allows PIL to load truncated or malformed images, a common issue with web images.
ImageFile.LOAD_TRUNCATED_IMAGES = True

# --- Default Logger ---
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# --- Global Models and Configurations (Initialized ONCE at startup) ---
RESNET_MODEL = None
NLP_MODEL = None
IMAGENET_LABELS = {}
FASHION_LABELS = []

async def initialize_models_and_config():
    """
    Initializes all necessary AI models and configurations. This is a critical
    step for performance, ensuring models are not reloaded on every API call.
    """
    global RESNET_MODEL, NLP_MODEL, IMAGENET_LABELS, FASHION_LABELS
    
    # 1. Initialize ResNet-50 Model Pre-trained on ImageNet
    try:
        default_logger.info("Initializing pre-trained ResNet-50 model...")
        weights = models.ResNet50_Weights.IMAGENET1K_V2
        RESNET_MODEL = models.resnet50(weights=weights)
        RESNET_MODEL.eval()  # Set model to evaluation mode for inference
        # Load the class labels that ImageNet was trained on
        IMAGENET_LABELS = {i: label.replace("_", " ") for i, label in enumerate(weights.meta["categories"])}
        default_logger.info("ResNet-50 model initialized successfully.")
    except Exception as e:
        default_logger.error(f"Fatal: ResNet-50 initialization failed: {e}", exc_info=True)

    # 2. Initialize spaCy NLP Model for Semantic Similarity
    try:
        default_logger.info("Initializing spaCy NLP model (en_core_web_md)...")
        NLP_MODEL = spacy.load("en_core_web_md")
        default_logger.info("spaCy NLP model initialized successfully.")
    except OSError:
        default_logger.error("spaCy model 'en_core_web_md' not found. Please run: python -m spacy download en_core_web_md")
    except Exception as e:
        default_logger.error(f"Fatal: spaCy initialization failed: {e}. Semantic analysis will be skipped.", exc_info=True)

    # 3. Load Application-Specific Fashion Labels
    FALLBACK_FASHION_LABELS = ["shirt", "pants", "dress", "shoe", "bag", "hat", "jacket", "coat", "sneaker", "boot"]
    FASHION_LABELS = await load_config("fashion_labels", FALLBACK_FASHION_LABELS, default_logger, expect_list=True)

# Run the initialization when the module is first imported.
asyncio.run(initialize_models_and_config())


# --- Internal Helper Functions ---

async def _download_image(url: str, session: aiohttp.ClientSession) -> bytes:
    """Downloads image data from a URL."""
    async with session.get(url, timeout=20) as response:
        response.raise_for_status()
        return await response.read()

def _run_resnet_classification(image_bytes: bytes) -> List[Tuple[str, float]]:
    """Processes an image with ResNet-50 and returns its top predictions."""
    if not RESNET_MODEL:
        raise RuntimeError("ResNet-50 model is not available for classification.")
    
    # --- THIS IS THE CORRECTED, COMPLETE IMPLEMENTATION ---
    # Define the same transformations ImageNet was trained with
    preprocess = transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
    ])
    
    image = Image.open(BytesIO(image_bytes)).convert("RGB")
    image_tensor = preprocess(image).unsqueeze(0)  # Add batch dimension

    with torch.no_grad():
        outputs = RESNET_MODEL(image_tensor)
        probabilities = torch.nn.functional.softmax(outputs[0], dim=0)

    # Get the top 5 predictions
    top5_prob, top5_catid = torch.topk(probabilities, 5)
    
    results = []
    for i in range(top5_prob.size(0)):
        label = IMAGENET_LABELS[top5_catid[i].item()]
        confidence = top5_prob[i].item()
        results.append((label, confidence))
    return results
    # --- END OF CORRECTION ---

def _get_semantic_similarity(text1: str, text2: str) -> float:
    """Calculates semantic similarity between two texts using spaCy."""
    if not NLP_MODEL or not text1 or not text2:
        return 0.0
    
    # Process text for better comparison (e.g., 't-shirt' -> 't shirt')
    doc1 = NLP_MODEL(text1.lower().replace("-", " "))
    doc2 = NLP_MODEL(text2.lower().replace("-", " "))
    
    # Ensure vectors are available for comparison
    if not doc1.has_vector or not doc2.has_vector or doc1.vector_norm == 0 or doc2.vector_norm == 0:
        return 0.0
        
    return doc1.similarity(doc2)


async def _call_gemini_api(image_base64: str, prompt: str, logger: logging.Logger) -> Dict:
    """Calls the Google Gemini API with robust error handling and retries."""
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
            cleaned_text = response.text.strip().lstrip("```json").rstrip("```").strip()
            return {"success": True, "features": json.loads(cleaned_text)}
        except Exception as e:
            logger.warning(f"Gemini API call attempt {attempt + 1} failed: {e}")
            if attempt < 2: await asyncio.sleep(2)
    
    return {"success": False, "features": {"error": "Gemini API failed after multiple retries."}}


# --- PRIMARY PUBLIC FUNCTION ---
async def analyze_single_image_record(record: Dict, logger: logging.Logger) -> Tuple[str, bool, str]:
    """
    Analyzes a single image record using an advanced multi-modal AI pipeline.
    This is the main entry point to be called by the batch processor.
    """
    result_id = record.get("ResultID")
    image_url = record.get("ImageUrl")
    
    failure_json = json.dumps({"error": "Processing failed due to an unexpected error.", "scores": {}})
    failure_response = (failure_json, False, "Error: AI processing failed.")

    if not result_id or not image_url:
        logger.error(f"Record is missing 'ResultID' or 'ImageUrl'. Record: {record}")
        return failure_response

    try:
        # Step 1: Download Image
        async with aiohttp.ClientSession() as session:
            image_data = await _download_image(image_url, session)
        image_base64 = base64.b64encode(image_data).decode("utf-8")

        # Step 2: Computer Vision - ResNet-50 Classification
        # Run in a separate thread to avoid blocking the async event loop
        detected_objects = await asyncio.to_thread(_run_resnet_classification, image_data)
        top_detection = detected_objects[0] if detected_objects else ("unknown", 0.0)
        
        # Step 3: Semantic Analysis
        product_details = {key: record.get(key, "N/A") for key in ["ProductBrand", "ProductCategory", "ProductModel"]}
        expected_category = product_details["ProductCategory"]
        category_similarity_score = _get_semantic_similarity(top_detection[0], expected_category)

        # Step 4: Prepare a rich, structured prompt for the Gemini Reasoning Engine
        gemini_prompt = f"""
        You are an expert fashion product analyst. Synthesize the following structured data into a final analysis.
        
        **CONTEXT:**
        - **Expected Product:** Category='{expected_category}', Brand='{product_details['ProductBrand']}'.
        - **Image Analysis (ResNet-50):** My vision model detected the primary object as a '{top_detection[0]}' with {top_detection[1]:.2%} confidence. Other detections: {detected_objects[1:]}.
        - **Semantic Analysis (NLP Concept):** The detected term '{top_detection[0]}' has a semantic similarity score of {category_similarity_score:.2f} with the expected category '{expected_category}'. A score > 0.65 is a good match.

        **YOUR TASK:**
        Return a single, valid JSON object with the following structure. Do NOT include markdown or any other text.
        {{
          "description": "A compelling, one-sentence marketing description for the item shown.",
          "extracted_category": "Your final, most specific category for the item (e.g., 'quilted leather jacket', 'high-top canvas sneakers').",
          "relevance_score": "A final relevance score (float 0.0-1.0) based on ALL context, especially the semantic score.",
          "product_shot_quality_score": "A score (float 0.0-1.0) for the image quality. High scores for clean, studio shots on plain backgrounds. Low scores for blurry, dark, or busy lifestyle photos.",
          "reasoning": "A brief justification for your scores, referencing the provided context."
        }}
        """

        # Step 5: Call Gemini for the final reasoning step
        gemini_result = await _call_gemini_api(image_base64, gemini_prompt, logger)

        if not gemini_result["success"]:
            failure_reason = gemini_result["features"].get("error", "Gemini reasoning failed.")
            ai_json = json.dumps({"error": failure_reason, "resnet_detection": top_detection[0], "scores": {}})
            return (ai_json, False, f"Error: {failure_reason}")

        features = gemini_result["features"]
        
        # Step 6: Consolidate results into the final AiJson payload
        is_fashion = any(label in features.get("extracted_category", "").lower() for label in FASHION_LABELS)

        final_scores = {
            "relevance": round(float(features.get("relevance_score", 0.0)), 2),
            "sentiment": round(float(features.get("product_shot_quality_score", 0.0)), 2),
            "category_match": round(category_similarity_score, 2),
        }

        final_ai_json = {
            "scores": final_scores,
            "category": features.get("extracted_category", "unknown"),
            "description": features.get("description", "No description available."),
            "reasoning": features.get("reasoning", "No reasoning available."),
            "cv_detection_summary": f"ResNet-50 Top Match: {top_detection[0]} ({top_detection[1]:.2%})",
            "result_id": result_id,
        }

        final_caption = features.get("description", "AI-generated description.")
        
        logger.info(f"Successfully processed ResultID {result_id}. Scores: {final_scores}")
        return json.dumps(final_ai_json), is_fashion, final_caption

    except Exception as e:
        logger.error(f"Unhandled exception in analyze_single_image_record for ResultID {result_id}: {e}", exc_info=True)
        return failure_response