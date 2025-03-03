# image_processing.py
import asyncio
import base64
import json
import logging
import requests
import random
from typing import Dict, Optional
import google.generativeai as genai
from urllib.parse import urlparse
from config import GROK_API_KEY, GOOGLE_API_KEY
from openai import OpenAI
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# Gemini prompt
prompt = """
Analyze this image and provide the following information in JSON format:
{
  "description": "A detailed description of the image in one sentence. Extract brand name, category, color, and composition.",
  "extracted_features": {
    "brand": "Extracted brand name from the image, if any.",
    "category": "Extracted category of the product, if identifiable.",
    "color": "Primary color of the product.",
    "composition": "Any composition details visible in the image as well as angle and scale, background."
  }
}
Ensure the response is a valid JSON object. Return only the JSON object, no additional text.
"""

# Pydantic schema for Grok response
class GrokScore(BaseModel):
    match_score: Optional[int] = Field(description="A score from 0-100 indicating how well the extracted features match the user-provided details", ge=0, le=100)
    linesheet_score: Optional[int] = Field(description="A score from 0-100 indicating suitability for a linesheet", ge=0, le=100)
    reasoning_match: str = Field(description="Explanation for the match score")
    reasoning_linesheet: str = Field(description="Explanation for the linesheet score")

async def analyze_image_with_gemini(
    image_base64: str,
    prompt: str = prompt,
    api_key: str = None,
    model_name: str = "gemini-2.0-pro-exp-02-05",
    mime_type: str = "image/jpeg",
    max_retries: int = 5,
    initial_delay: float = 4.0
) -> Dict[str, Optional[str | int | bool]]:
    api_key = api_key or GOOGLE_API_KEY
    if not api_key:
        logger.error("No API key provided for Gemini")
        return {"status_code": None, "success": False, "text": "No API key provided for Gemini"}

    logger.info(f"Analyzing base64 image with prompt: '{prompt}'")

    # Validate base64 input
    if not image_base64 or not isinstance(image_base64, str):
        logger.error(f"Invalid base64 input: {image_base64}")
        return {"status_code": None, "success": False, "text": "Invalid base64 input"}

    if image_base64.startswith("data:"):
        try:
            mime_type, base64_data = image_base64.split(",", 1)
            mime_type = mime_type.split(";")[0].replace("data:", "")
            image_base64 = base64_data
        except ValueError as e:
            logger.error(f"Invalid data URI format: {str(e)}")
            return {"status_code": None, "success": False, "text": f"Invalid base64 data URI: {str(e)}"}

    try:
        image_bytes = base64.b64decode(image_base64)
        if len(image_bytes) < 100:
            logger.error("Image data too small to process")
            return {"status_code": None, "success": False, "text": "Image data too small"}
        logger.debug(f"Base64 input validated (length: {len(image_base64)})")
    except Exception as e:
        logger.error(f"Invalid base64 string: {str(e)}")
        return {"status_code": None, "success": False, "text": f"Invalid base64 string: {str(e)}"}

    # Configure Gemini
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(model_name=model_name)
    except Exception as e:
        logger.error(f"Failed to configure Gemini with {model_name}: {str(e)}")
        try:
            model = genai.GenerativeModel(model_name="gemini-1.5-pro")
            logger.info("Falling back to gemini-1.5-pro")
        except Exception as fallback_e:
            logger.error(f"Failed to configure fallback model: {str(fallback_e)}")
            return {"status_code": None, "success": False, "text": f"Failed to configure Gemini: {str(fallback_e)}"}

    # Prepare content
    contents = [
        {
            "mime_type": mime_type,
            "data": image_bytes
        },
        prompt
    ]

    # Retry logic
    loop = asyncio.get_running_loop()
    for attempt in range(max_retries + 1):
        try:
            response = await loop.run_in_executor(
                None,
                lambda: model.generate_content(
                    contents,
                    generation_config=genai.types.GenerationConfig(
                        response_mime_type="application/json"
                    )
                )
            )
            response_text = response.text if hasattr(response, "text") else ""
            logger.info(f"Gemini raw response: '{response_text[:500]}'")

            if response_text:
                try:
                    features = json.loads(response_text)
                    if isinstance(features, dict) and "description" in features and "extracted_features" in features:
                        logger.info("Image analysis successful")
                        return {"status_code": 200, "success": True, "features": features}
                    else:
                        logger.warning(f"Invalid JSON structure from Gemini: {response_text}")
                        return {"status_code": 200, "success": False, "text": "Invalid JSON structure"}
                except json.JSONDecodeError as e:
                    logger.warning(f"JSON decode error from Gemini: {e}")
                    return {"status_code": 200, "success": False, "text": f"JSON decode error: {str(e)}"}
            else:
                logger.warning("Gemini returned no text")
                return {"status_code": 200, "success": False, "text": "No analysis text returned"}

        except Exception as e:
            error_str = str(e)
            if "429" in error_str or "Resource has been exhausted" in error_str:
                if attempt < max_retries:
                    delay = initial_delay * (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(f"Rate limit hit, retrying in {delay:.2f}s (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(delay)
                    continue
                return {"status_code": 429, "success": False, "text": f"Rate limit exhausted: {error_str}"}
            logger.error(f"Gemini analysis failed: {error_str}")
            return {"status_code": None, "success": False, "text": f"Gemini analysis error: {error_str}"}
async def evaluate_with_grok_text(features: dict, product_details: dict) -> dict:
    client = OpenAI(
        api_key=GROK_API_KEY,
        base_url="https://api.x.ai/v1",
    )
    prompt = (
        "Evaluate the match between the extracted image features and user-provided product details.\n"
        f"Extracted features: {json.dumps(features)}\n"
        f"User-provided details: {json.dumps(product_details)}\n"
        "First, calculate a match_score from 0 to 100 indicating how well the extracted features match the user-provided details based on fields like brand, category, color, and any other overlapping fields. If no elements align match_score is 0\n"
        "Then, if the match_score is at least 50, calculate a linesheet_score from 0 to 100 indicating the suitability of the image for a linesheet (a clean product shot with no models, no extra items, and a solid white background. Full visability of product, no zoom, straight on angle shot of front or side of product), using the composition field to assess the image quality.\n"
        "If the match_score is less than 50, set linesheet_score to null and explain that it’s not applicable due to insufficient matching.\n"
        "Provide reasoning for both scores.\n"
        "Return a JSON object with the following structure:\n"
        "{\n"
        "  \"match_score\": \"An integer from 0-100\",\n"
        "  \"linesheet_score\": \"An integer from 0-100 or null\",\n"
        "  \"reasoning_match\": \"Explanation for the match score\",\n"
        "  \"reasoning_linesheet\": \"Explanation for the linesheet score or why it’s not applicable\"\n"
        "}\n"
        "Ensure the response is a valid JSON object. Return only the JSON object, no additional text."
    )
    try:
        loop = asyncio.get_running_loop()
        completion = await loop.run_in_executor(
            None,
            lambda: client.beta.chat.completions.parse(
                model="grok-2-latest",
                messages=[
                    {"role": "system", "content": "You are an expert at analyzing data and returning structured JSON responses."},
                    {"role": "user", "content": prompt},
                ],
                response_format=GrokScore,
            )
        )
        parsed_response = completion.choices[0].message.parsed
        logger.info(f"Grok parsed response: {parsed_response.model_dump_json()}")
        return parsed_response.dict()  # Convert Pydantic model to dict
    except Exception as e:
        logger.error(f"Grok analysis failed: {str(e)}")
        return {
            "match_score": None,
            "linesheet_score": None,
            "reasoning_match": f"Error: {str(e)}",
            "reasoning_linesheet": f"Error: {str(e)}"
        }

def get_image_data(image_path_or_url: str) -> bytes:
    parsed_url = urlparse(image_path_or_url)
    is_url = bool(parsed_url.scheme and parsed_url.netloc)
    if is_url:
        try:
            logger.debug(f"Fetching image from URL: {image_path_or_url}")
            response = requests.get(image_path_or_url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            response.raise_for_status()
            content = response.content
            logger.debug(f"Image downloaded, size: {len(content)} bytes, content-type: {response.headers.get('Content-Type', 'unknown')}")
            if len(content) < 100:
                raise ValueError("Image data too small")
            return content
        except requests.RequestException as e:
            logger.error(f"Failed to download image from URL: {e}")
            raise
    else:
        try:
            with open(image_path_or_url, 'rb') as img_file:
                content = img_file.read()
                logger.debug(f"Local image read, size: {len(content)} bytes")
                if len(content) < 100:
                    raise ValueError("Image data too small")
                return content
        except IOError as e:
            logger.error(f"Failed to read local image file: {e}")
            raise