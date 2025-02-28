import aiohttp
import base64
import json
import logging
import asyncio
from config import GROK_API_KEY, GROK_ENDPOINT
import requests
from urllib.parse import urlparse

logging.getLogger(__name__)

async def analyze_image_with_grok_vision(image_data: bytes) -> dict:
    headers = {
        "Authorization": f"Bearer {GROK_API_KEY}",
        "Content-Type": "application/json"
    }
    base64_img = base64.b64encode(image_data).decode('utf-8')
    messages = [
        {
            "role": "user",
            "content": [
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_img}"}},
                {"type": "text", "text": "Extract the brand, category, and color from this image in JSON format."}
            ]
        }
    ]
    payload = {
        "model": "grok-2-vision-latest",
        "messages": messages,
        "temperature": 0.01,
        "max_tokens": 500
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(GROK_ENDPOINT, json=payload, headers=headers) as response:
            if response.status == 200:
                result = await response.json()
                try:
                    content = result["choices"][0]["message"]["content"]
                    return json.loads(content)
                except (KeyError, json.JSONDecodeError) as e:
                    logging.error(f"Failed to parse Grok Vision response: {e}")
                    return {"extraction_failed": True}
            else:
                logging.error(f"Grok Vision API failed: {response.status}")
                return {"extraction_failed": True}

async def evaluate_with_grok_text(features: dict, product_details: dict) -> dict:
    headers = {
        "Authorization": f"Bearer {GROK_API_KEY}",
        "Content-Type": "application/json"
    }
    prompt = f"""
    Evaluate the match between the extracted image features and user-provided product details.
    Extracted: {features}
    User-provided: {product_details}
    Provide match_score (0-100), linesheet_score (0-100), and reasoning for both in JSON format.
    """
    messages = [{"role": "user", "content": prompt}]
    payload = {
        "model": "grok-2-text-latest",
        "messages": messages,
        "temperature": 0.01,
        "max_tokens": 500
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(GROK_ENDPOINT, json=payload, headers=headers) as response:
            if response.status == 200:
                result = await response.json()
                try:
                    content = result["choices"][0]["message"]["content"]
                    return json.loads(content)
                except (KeyError, json.JSONDecodeError) as e:
                    logging.error(f"Failed to parse Grok Text response: {e}")
                    return {"match_score": None, "linesheet_score": None, "reasoning_match": "", "reasoning_linesheet": ""}
            else:
                logging.error(f"Grok Text API failed: {response.status}")
                return {"match_score": None, "linesheet_score": None, "reasoning_match": "", "reasoning_linesheet": ""}

def get_image_data(image_path_or_url: str) -> bytes:
    parsed_url = urlparse(image_path_or_url)
    is_url = bool(parsed_url.scheme and parsed_url.netloc)
    if is_url:
        try:
            response = requests.get(image_path_or_url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            response.raise_for_status()
            return response.content
        except requests.RequestException as e:
            logging.error(f"Failed to download image from URL: {e}")
            raise
    else:
        try:
            with open(image_path_or_url, 'rb') as img_file:
                return img_file.read()
        except IOError as e:
            logging.error(f"Failed to read local image file: {e}")
            raise