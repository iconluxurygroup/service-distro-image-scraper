import logging
import asyncio
from typing import List, Dict, Optional
from urllib.parse import urlparse
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from utils import generate_search_variations, process_and_tag_results
from url_extract import extract_thumbnail_url

class SearchClient:
    def __init__(self, endpoint: str, logger: logging.Logger, max_concurrency: int = 2):
        self.endpoint = endpoint
        self.logger = logger
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.client = httpx.AsyncClient(timeout=10.0)

    async def close(self):
        await self.client.aclose()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.RequestError))
    )
    async def search(self, term: str, brand: str) -> List[Dict]:
        async with self.semaphore:
            try:
                response = await self.client.get(
                    self.endpoint,
                    params={"q": term, "brand": brand}
                )
                response.raise_for_status()
                return response.json().get("results", [])
            except httpx.HTTPStatusError as e:
                self.logger.error(f"HTTP error for term '{term}': {e}")
                raise
            except httpx.RequestError as e:
                self.logger.error(f"Request error for term '{term}': {e}")
                raise

async def process_results(
    raw_results: List[Dict],
    entry_id: int,
    brand: str,
    search_term: str,
    logger: logging.Logger
) -> List[Dict]:
    results = []
    required_columns = ["EntryID", "ImageUrl", "ImageDesc", "ImageSource", "ImageUrlThumbnail"]

    tagged_results = await process_and_tag_results(raw_results, brand=brand, search_term=search_term, logger=logger)
    for result in tagged_results:
        image_url = result.get("image_url")
        if not image_url:
            continue

        thumbnail_url = await extract_thumbnail_url(image_url, logger=logger) or image_url
        parsed_url = urlparse(image_url)
        image_source = parsed_url.netloc or "unknown"

        formatted_result = {
            "EntryID": entry_id,
            "ImageUrl": image_url,
            "ImageDesc": result.get("description", ""),
            "ImageSource": image_source,
            "ImageUrlThumbnail": thumbnail_url
        }

        if all(col in formatted_result for col in required_columns):
            results.append(formatted_result)
        else:
            logger.warning(f"Result missing required columns for EntryID {entry_id}: {formatted_result}")

    return results

async def async_process_entry_search(
    search_string: str,
    brand: str,
    endpoint: str,
    entry_id: int,
    use_all_variations: bool,
    file_id_db: int,
    logger: logging.Logger
) -> List[Dict]:
    logger.debug(f"Processing search for EntryID {entry_id}, FileID {file_id_db}")
    search_terms = generate_search_variations(search_string, brand, use_all_variations=use_all_variations)
    if not search_terms:
        logger.warning(f"No search terms for EntryID {entry_id}")
        return []

    client = SearchClient(endpoint, logger)
    try:
        tasks = [client.search(term, brand) for term in search_terms]
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

        all_results = []
        for term, term_results in zip(search_terms, raw_results):
            if isinstance(term_results, Exception):
                logger.error(f"Error for term '{term}' in EntryID {entry_id}: {term_results}")
                continue
            if not term_results:
                continue
            results = await process_results(term_results, entry_id, brand, term, logger)
            all_results.extend(results)

        logger.info(f"Processed {len(all_results)} results for EntryID {entry_id}")
        return all_results
    finally:
        await client.close()