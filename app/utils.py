import logging
import pandas as pd
import requests
import json
import re
import base64
import zlib
import urllib.parse
from typing import List, Optional, Dict, Tuple,Any
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from urllib3.util.retry import Retry
from requests import Session
from icon_image_lib.google_parser import process_search_result
import asyncio
from unidecode import unidecode
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

BRAND_RULES_URL = "https://raw.githubusercontent.com/iconluxurygroup/legacy-icon-product-api/refs/heads/main/task_settings/brand_settings.json"

async def fetch_brand_rules(url: str = BRAND_RULES_URL, max_attempts: int = 3, timeout: int = 10, logger: Optional[logging.Logger] = None) -> Optional[Dict]:
    """Fetch brand rules from a remote URL with retry logic."""
    logger = logger or default_logger
    for attempt in range(max_attempts):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            brand_rules = response.json()
            if "brand_rules" not in brand_rules:
                logger.error(f"Invalid brand rules format from {url}")
                return {"brand_rules": []}
            brand_rules["brand_rules"].append({
                "names": ["Scotch & Soda", "Scotch and Soda", "Scotch Soda", "ScotchSoda"],
                "full_name": "Scotch & Soda",
                "version": "1.0",
                "last_updated": "2025-05-06",
                "is_active": True,
                "sku_format": {
                    "base": {"article": ["6"], "model": ["0"]},
                    "base_separator": "",
                    "color_extension": ["3"],
                    "color_separator": "_"
                },
                "expected_length": {"base": [6], "with_color": [10]},
                "fallback_format": "base",
                "render": False,
                "delay": False,
                "domain_hierarchy": ["scotch-soda.com", "scotchandsoda.com"],
                "comments": "SKU format: 6-digit base + 3-digit color",
                "example_sku": "179177_260"
            })
            logger.info(f"Successfully fetched brand rules from {url} with Scotch & Soda fallback")
            return brand_rules
        except (RequestException, ValueError) as e:
            logger.warning(f"Attempt {attempt + 1} failed to fetch brand rules from {url}: {e}")
            if attempt < max_attempts - 1:
                await asyncio.sleep(2)
            else:
                logger.error(f"Failed to fetch brand rules after {max_attempts} attempts")
                return {"brand_rules": []}

def unpack_content(encoded_content: str, logger: Optional[logging.Logger] = None) -> Optional[bytes]:
    """Unpack base64-encoded and zlib-compressed content."""
    logger = logger or default_logger
    try:
        if encoded_content:
            compressed_content = base64.b64decode(encoded_content)
            return zlib.decompress(compressed_content)
        return None
    except Exception as e:
        logger.error(f"Error unpacking content: {e}")
        return None

def check_endpoint_health(endpoint: str, timeout: int = 5, logger: Optional[logging.Logger] = None) -> bool:
    """Check if an endpoint is healthy by querying its health check URL."""
    logger = logger or default_logger
    health_url = f"{endpoint}/health/google"
    try:
        response = requests.get(health_url, timeout=timeout)
        response.raise_for_status()
        return "Google is reachable" in response.json().get("status", "")
    except RequestException as e:
        logger.warning(f"Endpoint {endpoint} health check failed: {e}")
        return False

def get_healthy_endpoint(endpoints: List[str], logger: Optional[logging.Logger] = None) -> Optional[str]:
    """Find and return a healthy endpoint from a list."""
    logger = logger or default_logger
    for endpoint in endpoints:
        if check_endpoint_health(endpoint, logger=logger):
            logger.info(f"Selected healthy endpoint: {endpoint}")
            return endpoint
    logger.error("No healthy endpoints found")
    return None

def process_search_row(
    search_string: str,
    endpoint: str,
    entry_id: int,
    logger: Optional[logging.Logger] = None,
    search_type: str = "default",
    max_retries: int = 15,
    brand: Optional[str] = None,
    category: Optional[str] = None
) -> pd.DataFrame:
    """Process a search query and return filtered image results."""
    logger = logger or default_logger
    if not search_string or not endpoint:
        logger.warning(f"Invalid input for EntryID {entry_id}: search_string={search_string}, endpoint={endpoint}")
        return pd.DataFrame()

    total_attempts = [0]
    session = Session()
    retry_strategy = Retry(total=3, status_forcelist=[500, 502, 503, 504], backoff_factor=1)
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))

    def log_retry_status(attempt_type: str, attempt_num: int) -> bool:
        total_attempts[0] += 1
        if total_attempts[0] > max_retries:
            logger.error(f"Exceeded max retries ({max_retries}) for EntryID {entry_id} after {attempt_type} attempt {attempt_num}")
            return False
        logger.info(f"{attempt_type} attempt {attempt_num} (Total attempts: {total_attempts[0]}/{max_retries}) for EntryID {entry_id}")
        return True

    try:
        if search_type == "retry_with_alternative":
            delimiters = r'[\s\-_,]+'
            words = re.split(delimiters, search_string.strip())
            for i in range(len(words), 0, -1):
                partial_search = ' '.join(words[:i])
                query = partial_search
                if brand:
                    query += f" {brand}"
                if category:
                    query += f" {category}"
                search_url = f"https://www.google.com/search?q={urllib.parse.quote(query)}&tbm=isch"
                fetch_endpoint = f"{endpoint}/fetch"
                attempt_num = 1
                while attempt_num <= retry_strategy.total + 1:
                    if not log_retry_status("Primary", attempt_num):
                        return pd.DataFrame()
                    try:
                        logger.info(f"Fetching URLs via {fetch_endpoint} with query: {query}")
                        response = session.post(fetch_endpoint, json={"url": search_url}, timeout=60)
                        response.raise_for_status()
                        result = response.json().get("result")
                        if result:
                            results_html_bytes = result if isinstance(result, bytes) else result.encode("utf-8")
                            df = process_search_result(results_html_bytes, results_html_bytes, entry_id, logger)
                            if not df.empty:
                                irrelevant_keywords = ['wallpaper', 'furniture', 'decor', 'stock photo', 'pistol', 'mattress', 'trunk', 'clutch', 'solenoid', 'card', 'pokemon']
                                df = df[df['ImageDesc'].str.lower().str.contains('scotch|soda|sneaker|shoe|hoodie|shirt|jacket|pants|apparel|clothing', na=False) &
                                       ~df['ImageDesc'].str.lower().str.contains('|'.join(irrelevant_keywords), na=False)]
                                logger.info(f"Filtered out irrelevant results, kept {len(df)} rows for EntryID {entry_id}")
                                return df
                        logger.warning(f"No results for query: {query}, moving to next attempt or split")
                        break
                    except (RequestException, json.JSONDecodeError) as e:
                        logger.warning(f"Primary attempt {attempt_num} failed for {fetch_endpoint} with query '{query}': {e}")
                        attempt_num += 1
                        if attempt_num > retry_strategy.total + 1:
                            break
                if total_attempts[0] < max_retries:
                    gcloud_df = process_search_row_gcloud(partial_search, entry_id, logger, max_retries - total_attempts[0], total_attempts)
                    if not gcloud_df.empty:
                        logger.info(f"GCloud fallback succeeded for EntryID {entry_id} with {len(gcloud_df)} images using query: {partial_search}")
                        return gcloud_df
                    logger.warning(f"GCloud fallback failed for query: {partial_search}, trying next split")
            logger.warning(f"No valid data for EntryID {entry_id} after all splits and retries")
            return pd.DataFrame()
        else:
            query = search_string
            if brand:
                query += f" {brand}"
            if category:
                query += f" {category}"
            search_url = f"https://www.google.com/search?q={urllib.parse.quote(query + ' images')}&tbm=isch"
            attempt_num = 1
            while attempt_num <= retry_strategy.total + 1:
                if not log_retry_status("Primary", attempt_num):
                    return pd.DataFrame()
                try:
                    logger.info(f"Searching {search_url}")
                    response = session.get(search_url, timeout=60)
                    response.raise_for_status()
                    result = response.json().get("body")
                    if not result:
                        logger.warning(f"No body in response for {search_url}")
                        raise ValueError("Empty response body")
                    unpacked_html = unpack_content(result, logger)
                    if not unpacked_html or len(unpacked_html) < 100:
                        logger.warning(f"Invalid HTML for {search_url}")
                        raise ValueError("Invalid HTML content")
                    df = process_search_result(unpacked_html, unpacked_html, entry_id, logger)
                    if not df.empty:
                        irrelevant_keywords = ['wallpaper', 'furniture', 'decor', 'stock photo', 'pistol', 'mattress', 'trunk', 'clutch', 'solenoid', 'card', 'pokemon']
                        df = df[df['ImageDesc'].str.lower().str.contains('scotch|soda|sneaker|shoe|hoodie|shirt|jacket|pants|apparel|clothing', na=False) &
                               ~df['ImageDesc'].str.lower().str.contains('|'.join(irrelevant_keywords), na=False)]
                        logger.info(f"Filtered out irrelevant results, kept {len(df)} rows for EntryID {entry_id}")
                        return df
                    logger.warning(f"No valid data for EntryID {entry_id}")
                    return df
                except (RequestException, json.JSONDecodeError, ValueError) as e:
                    logger.warning(f"Primary attempt {attempt_num} failed for {search_url}: {e}")
                    attempt_num += 1
                    if attempt_num > retry_strategy.total + 1:
                        break
            if total_attempts[0] < max_retries:
                gcloud_df = process_search_row_gcloud(search_string, entry_id, logger, max_retries - total_attempts[0], total_attempts)
                if not gcloud_df.empty:
                    logger.info(f"GCloud fallback succeeded for EntryID {entry_id} with {len(gcloud_df)} images")
                    return gcloud_df
                logger.error(f"GCloud fallback also failed for EntryID {entry_id} after {total_attempts[0]} total attempts")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Unexpected error processing EntryID {entry_id} after {total_attempts[0]} attempts: {e}", exc_info=True)
        return pd.DataFrame()

def process_search_row_gcloud(search_string: str, entry_id: int, logger: Optional[logging.Logger] = None, remaining_retries: int = 5, total_attempts: Optional[List[int]] = None) -> pd.DataFrame:
    """Process a search query using GCloud proxy with regional retries."""
    logger = logger or default_logger
    if not search_string or len(search_string.strip()) < 3:
        logger.warning(f"Invalid search string for EntryID {entry_id}: '{search_string}'")
        return pd.DataFrame()

    total_attempts = total_attempts or [0]
    base_url = "https://api.thedataproxy.com/v2/proxy/fetch"
    api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiMGRkZTIwZjAtNjlmZS00ODc2LWE0MmItMTY1YzM1YTk4MzMyIiwiaWF0IjoxNzQ2NDcyMDQzLjc0Njk3NiwiZXhwIjoxNzc4MDA4MDQzLjc0Njk4MX0.MduHUL3BeVw9k6Tk3mbOVHuZyT7k49d01ddeFqnmU8k"
    regions = ['northamerica-northeast', 'southamerica', 'us-central', 'us-east', 'us-west', 'europe', 'australia']
    headers = {
        "accept": "application/json",
        "x-api-key": api_key,
        "Content-Type": "application/json"
    }

    session = Session()
    retry_strategy = Retry(total=3, status_forcelist=[500, 502, 503, 504], backoff_factor=1)
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
    session.headers.update(headers)

    def log_gcloud_retry(attempt: int) -> bool:
        total_attempts[0] += 1
        if total_attempts[0] > remaining_retries:
            logger.error(f"Exceeded remaining retries ({remaining_retries}) for EntryID {entry_id} at GCloud attempt {attempt}")
            return False
        logger.info(f"GCloud attempt {attempt} (Total attempts: {total_attempts[0]}/{remaining_retries}) for EntryID {entry_id}")
        return True

    search_url = f"https://www.google.com/search?q={urllib.parse.quote(search_string)}&tbm=isch"
    for attempt, region in enumerate(regions, 1):
        if not log_gcloud_retry(attempt):
            break
        fetch_endpoint = f"{base_url}?region={region}"
        try:
            logger.info(f"Attempt {attempt}: Fetching {search_url} via {fetch_endpoint} with region {region}")
            response = session.post(fetch_endpoint, json={"url": search_url}, timeout=30)
            response.raise_for_status()
            result = response.json().get("result")
            if not result:
                logger.warning(f"No result returned for EntryID {entry_id} in region {region}")
                continue
            results_html_bytes = result if isinstance(result, bytes) else result.encode("utf-8")
            df = process_search_result(results_html_bytes, results_html_bytes, entry_id, logger)
            if not df.empty:
                irrelevant_keywords = ['wallpaper', 'sofa', 'furniture', 'decor', 'stock photo', 'card', 'pokemon']
                df = df[~df['ImageDesc'].str.lower().str.contains('|'.join(irrelevant_keywords), na=False)]
                logger.info(f"Filtered out irrelevant results, kept {len(df)} rows for EntryID {entry_id}")
                return df
        except (RequestException, json.JSONDecodeError) as e:
            logger.warning(f"Attempt {attempt} failed for {fetch_endpoint} in region {region}: {e}")
            continue
    logger.error(f"All GCloud attempts failed for EntryID {entry_id} after {total_attempts[0]} total attempts")
    return pd.DataFrame()
def generate_search_variations(
    search_string: str,
    brand: Optional[str] = None,
    model: Optional[str] = None,
    brand_rules: Optional[Dict] = None,
    logger: Optional[logging.Logger] = None
) -> Dict[str, List[str]]:
    """
    Generate variations of a search string for different search types.
    
    Args:
        search_string: The base search string (e.g., product model or name).
        brand: Optional brand name to include in variations.
        model: Optional model name to include in variations.
        brand_rules: Dictionary containing brand-specific rules (e.g., from fetch_brand_rules).
        logger: Optional logger for debugging.
    
    Returns:
        Dictionary mapping search types to lists of search variations.
    """
    logger = logger or default_logger
    variations = {
        "default": [],
        "delimiter_variations": [],
        "brand_delimiter": [],
        "color_delimiter": [],
        "brand_alias": [],
        "brand_name": [],
        "no_color": [],
        "brand_settings": [],
        "retry_with_alternative": []
    }
    
    if not search_string:
        logger.warning("Empty search string provided")
        return variations
    
    # Clean and normalize inputs
    search_string = clean_string(search_string)
    brand = clean_string(brand) if brand else None
    model = clean_string(model) if model else search_string
    
    # Default variation
    variations["default"].append(search_string)
    
    # Delimiter variations (e.g., replace spaces, hyphens, underscores)
    delimiters = [' ', '-', '_', '/']
    for delim in delimiters:
        if delim in search_string:
            variations["delimiter_variations"].append(search_string.replace(delim, ' '))
            variations["delimiter_variations"].append(search_string.replace(delim, '-'))
            variations["delimiter_variations"].append(search_string.replace(delim, '_'))
    
    # Brand delimiter (include brand in search)
    if brand:
        variations["brand_delimiter"].append(f"{brand} {search_string}")
        variations["brand_delimiter"].append(f"{brand}-{search_string}")
        variations["brand_delimiter"].append(f"{brand}_{search_string}")
    
    # Color delimiter (assume color is part of search_string or model)
    variations["color_delimiter"].append(search_string)  # Placeholder; add logic if color is provided
    
    # Brand alias (use aliases from brand_rules or generate simple ones)
    if brand:
        brand_aliases = generate_aliases(brand) if brand else [brand]
        variations["brand_alias"].extend([f"{alias} {search_string}" for alias in brand_aliases])
    
    # Brand name (search with brand name only)
    if brand:
        variations["brand_name"].append(brand)
    
    # No color (remove color-related terms if applicable)
    variations["no_color"].append(search_string) 
    
    # Brand settings (use brand_rules for specific formats)
    if brand_rules and "brand_rules" in brand_rules:
        for rule in brand_rules["brand_rules"]:
            if brand and any(brand.lower() in name.lower() for name in rule.get("names", [])):
                variations["brand_settings"].append(f"{rule['full_name']} {search_string}")
                for name in rule.get("names", []):
                    variations["brand_settings"].append(f"{name} {search_string}")
    
    # Retry with alternative (split search string into partial terms)
    words = search_string.split()
    for i in range(1, len(words) + 1):
        variations["retry_with_alternative"].append(' '.join(words[:i]))
    
    # Remove duplicates and empty strings
    for key in variations:
        variations[key] = list(set([v for v in variations[key] if v.strip()]))
        if variations[key]:
            logger.debug(f"Generated {len(variations[key])} variations for {key}: {variations[key]}")
    
    return variations
def clean_string(text):
    """Clean and normalize a string for better matching."""
    if pd.isna(text) or text is None:
        return ''
    try:
        text = unidecode(str(text).encode().decode('unicode_escape'))
        return re.sub(r'[^\w\d\-_]', '', text.upper()).strip()
    except:
        if not isinstance(text, str):
            return ''
        return re.sub(r'[^\w\d\-_]', '', str(text).upper()).strip()

def normalize_model(model: Any) -> str:
    """Normalize a model string to lowercase."""
    if not isinstance(model, str):
        return str(model).strip().lower()
    return model.strip().lower()

def generate_aliases(model: Any) -> List[str]:
    """Generate a list of possible aliases for a model string."""
    if not isinstance(model, str):
        model = str(model)
    if not model or model.strip() == '':
        return []
    aliases = {model, model.lower(), model.upper()}
    aliases.add(model.replace("_", "-"))
    aliases.add(model.replace("_", ""))
    aliases.add(model.replace("_", " "))
    aliases.add(model.replace("_", "/"))
    aliases.add(model.replace("_", "."))
    digits_only = re.sub(r'[_/.-]', '', model)
    if digits_only.isdigit():
        aliases.add(digits_only)
    base = model.split('_')[0] if '_' in model else model
    aliases.add(base)
    return [a for a in aliases if a and len(a) >= len(model) - 3]

def validate_model(row, expected_models, result_id, logger=None):
    """
    Validate if the model in the row matches any expected models using exact substring matching.
    Args:
        row (pd.Series): Row data with 'ProductModel', 'ImageDesc', 'ImageSource', 'ImageUrl'.
        expected_models (list): List of expected model names or aliases.
        result_id (str): ResultID for logging.
        logger (logging.Logger): Logger instance.
    Returns:
        bool: True if model matches, False otherwise.
    """
    logger = logger or logging.getLogger(__name__)
    input_model = clean_string(row.get('ProductModel', ''))
    if not input_model:
        logger.warning(f"ResultID {result_id}: No ProductModel provided in row")
        return False

    # Clean fields for matching
    fields = [
        clean_string(row.get('ImageDesc', '')),
        clean_string(row.get('ImageSource', '')),
        clean_string(row.get('ImageUrl', ''))
    ]

    for expected_model in expected_models:
        expected_model = clean_string(expected_model)
        if not expected_model:
            continue

        # Check for substring match in each field
        for field in fields:
            if not field:
                continue
            if expected_model.lower() in field.lower():
                logger.info(f"ResultID {result_id}: Model match: '{expected_model}' in field")
                return True

    logger.warning(f"ResultID {result_id}: Model match failed: Input model='{input_model}', Expected models={expected_models}")
    return False
async def filter_model_results(df: pd.DataFrame, debug: bool = True, logger: Optional[logging.Logger] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Filter DataFrame to keep rows with model matches and discard others."""
    logger = logger or default_logger
    try:
        if debug:
            logger.debug("\nDebugging and Filtering Model Results:")

        required_columns = ['ProductModel', 'ImageSource', 'ImageUrl', 'ImageDesc', 'ResultID']
        if not all(col in df.columns for col in required_columns):
            missing_cols = [col for col in required_columns if col not in df.columns]
            raise ValueError(f"DataFrame missing required columns: {missing_cols}")

        keep_indices = []
        discarded_indices = []

        if df.empty:
            logger.warning("Empty DataFrame provided to filter_model_results")
            return df.copy(), df.copy()

        # Fetch brand rules asynchronously
        brand_rules = await fetch_brand_rules(BRAND_RULES_URL, logger=logger)
        brand_names = []
        domain_hierarchy = []
        for rule in brand_rules.get("brand_rules", []):
            if rule["is_active"]:
                brand_names.extend(rule["names"])
                domain_hierarchy.extend(rule.get("domain_hierarchy", []))
        brand_pattern = '|'.join(re.escape(name.lower()) for name in brand_names)
        domain_pattern = '|'.join(re.escape(domain.lower()) for domain in domain_hierarchy)

        for idx, row in df.iterrows():
            if pd.isna(row['ProductModel']) or not str(row['ProductModel']).strip():
                logger.warning(f"ResultID {row['ResultID']}: Skipping row due to missing or empty ProductModel")
                discarded_indices.append(idx)
                continue

            model = str(row['ProductModel'])
            aliases = generate_aliases(model)
            result_id = row['ResultID']

            # Use validate_model to check for model matches
            has_model_match = validate_model(row, aliases, result_id, logger)
            has_brand_match = False

            # Check for brand or domain match
            source = clean_string(row.get('ImageSource', ''))
            url = clean_string(row.get('ImageUrl', ''))
            desc = clean_string(row.get('ImageDesc', ''))
            combined_text = f"{source} {desc} {url}".lower()
            if re.search(brand_pattern, combined_text) or re.search(domain_pattern, combined_text):
                has_brand_match = True
                if debug:
                    logger.debug(f"ResultID {result_id}: Brand or domain match found")

            if has_model_match or has_brand_match:
                keep_indices.append(idx)
                if debug:
                    logger.debug(f"ResultID {result_id}: Keeping row (model_match={has_model_match}, brand_match={has_brand_match})")
            else:
                discarded_indices.append(idx)
                if debug:
                    logger.debug(f"ResultID {result_id}: Discarding row (no model or brand match)")

        exact_df = df.loc[keep_indices].copy() if keep_indices else pd.DataFrame(columns=df.columns)
        discarded_df = df.loc[discarded_indices].copy() if discarded_indices else pd.DataFrame(columns=df.columns)

        logger.info(f"Filtered {len(exact_df)} rows with matches and {len(discarded_df)} rows discarded")
        return exact_df, discarded_df
    except Exception as e:
        logger.error(f"Error in filter_model_results: {e}", exc_info=True)
        return pd.DataFrame(columns=df.columns), df.copy()
def calculate_priority(row: pd.Series, exact_df: pd.DataFrame, model_clean: str, model_aliases: List[str], brand_clean: str, brand_aliases: List[str], logger: Optional[logging.Logger] = None) -> int:
    """Calculate priority for a search result based on model and brand matches."""
    logger = logger or default_logger
    try:
        model_matched = row.name in exact_df.index
        brand_matched = False

        desc_clean = row['ImageDesc_clean'].lower() if 'ImageDesc_clean' in row else ''
        source_clean = row['ImageSource_clean'].lower() if 'ImageSource_clean' in row else ''
        url_clean = row['ImageUrl_clean'].lower() if 'ImageUrl_clean' in row else ''
        
        if brand_clean:
            for alias in brand_aliases:
                if alias and (
                    alias.lower() in desc_clean or
                    alias.lower() in source_clean or
                    alias.lower() in url_clean or
                    ('ProductBrand_clean' in row and alias.lower() in row['ProductBrand_clean'].lower())
                ):
                    brand_matched = True
                    logger.debug(f"ResultID {row.get('ResultID', 'unknown')}: Brand match found for alias '{alias}'")
                    break

        logger.debug(f"ResultID {row.get('ResultID', 'unknown')}: model_matched={model_matched}, brand_matched={brand_matched}")

        if model_matched and brand_matched:
            return 1
        if model_matched:
            return 2
        if brand_matched:
            return 3
        return 4
    except Exception as e:
        logger.error(f"Error in calculate_priority for ResultID {row.get('ResultID', 'unknown')}: {e}")
        return 4