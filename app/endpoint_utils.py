import logging
import httpx
from typing import Optional
import psutil
import asyncio  
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

async def check_endpoint_health(endpoint: str, client: httpx.AsyncClient, timeout: int = 5, logger: Optional[logging.Logger] = None) -> bool:
    """Check if an endpoint is healthy by querying its health check URL."""
    logger = logger or default_logger
    health_url = f"{endpoint}/health"  # Adjust based on dataproxy requirements
    headers = {
        "accept": "application/json",
        "x-api-key": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiMGRkZTIwZjAtNjlmZS00ODc2LWE0MmItMTY1YzM1YTk4MzMyIiwiaWF0IjoxNzQ2NDcyMDQzLjc0Njk3NiwiZXhwIjoxNzc4MDA4MDQzLjc0Njk4MX0.MduHUL3BeVw9k6Tk3mbOVHuZyT7k49d01ddeFqnmU8k"
    }
    try:
        response = await client.get(health_url, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response.status_code == 200  # Adjust if specific response is expected
    except httpx.RequestException as e:
        logger.warning(f"Endpoint {endpoint} health check failed: {e}")
        return False