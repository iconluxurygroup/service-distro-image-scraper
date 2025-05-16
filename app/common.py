# common.py
# Contains shared utility functions to avoid circular imports

import logging
import pandas as pd
import re
import requests
import unicodedata
from unidecode import unidecode
from fuzzywuzzy import fuzz
from typing import List, Optional, Dict, Any, Tuple
from requests.exceptions import RequestException
import asyncio
import os
import requests
import aiohttp
import asyncio
import logging
from typing import Any, Optional
from functools import lru_cache
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

BRAND_RULES_URL = "https://raw.githubusercontent.com/iconluxurygroup/legacy-icon-product-api/refs/heads/main/task_settings/brand_settings.json"
# Configuration file paths
CONFIG_FILES = {
    "category_hierarchy": "category_hierarchy.json",
    "category_mapping": "category_mapping.json",
    "fashion_labels": "fashion_labels.json",
    "non_fashion_labels": "non_fashion_labels.json"
}

# Cache synchronous loads
@lru_cache(maxsize=32)
def sync_load_config(file_key: str, url: str, config_name: str, expect_list: bool = False) -> Any:
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        config = response.json()
        if expect_list and not isinstance(config, list):
            raise ValueError(f"{config_name} must be a list")
        return config
    except Exception as e:
        raise e

async def load_config(
    file_key: str,
    fallback: Any,
    logger: Optional[logging.Logger] = None,
    config_name: str = "",
    expect_list: bool = False,
    retries: int = 3,
    backoff_factor: float = 2.0
) -> Any:
    logger = logger or logging.getLogger(__name__)
    url = f"{BASE_CONFIG_URL}{CONFIG_FILES[file_key]}"
    
    # Try cached synchronous load first
    try:
        config = sync_load_config(file_key, url, config_name, expect_list)
        logger.info(f"Loaded {config_name} from cache for {url}")
        return config
    except Exception:
        logger.debug(f"Cache miss for {config_name}, attempting async load")

    # Async load with retries
    for attempt in range(1, retries + 1):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    response.raise_for_status()
                    config = await response.json()
                    if expect_list and not isinstance(config, list):
                        raise ValueError(f"{config_name} must be a list")
                    logger.info(f"Loaded {config_name} from {url} on attempt {attempt}")
                    sync_load_config.cache_clear()
                    sync_load_config(file_key, url, config_name, expect_list)
                    return config
        except (aiohttp.ClientError, ValueError, asyncio.TimeoutError) as e:
            logger.warning(f"Failed to load {config_name} from {url} (attempt {attempt}/{retries}): {e}")
            if attempt < retries:
                await asyncio.sleep(backoff_factor * attempt)
            else:
                logger.info(f"Exhausted retries for {config_name}, using fallback")
                return fallback
    logger.error(f"Critical failure loading {config_name} from {url}, using fallback")
    return fallback
def clean_string(s: str, preserve_url: bool = False) -> str:
    """Clean a string, optionally preserving URL structures."""
    if not isinstance(s, str):
        return ''
    s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode('ascii')
    s = s.replace('\\u0026', '&')
    if preserve_url:
        s = re.sub(r'\s+', ' ', s.strip().lower())
    else:
        s = re.sub(r'[^a-z0-9\s&]', '', s.strip().lower())
        s = re.sub(r'\s+', ' ', s)
    return s

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

import httpx
import logging
import asyncio
from typing import Optional, Dict

async def fetch_brand_rules(
    url: str = BRAND_RULES_URL,
    max_attempts: int = 3,
    timeout: int = 10,
    logger: Optional[logging.Logger] = None
) -> Optional[Dict]:
    """Fetch brand rules from a remote URL with retry logic."""
    logger = logger or default_logger
    async with httpx.AsyncClient() as client:
        for attempt in range(max_attempts):
            try:
                response = await client.get(url, timeout=timeout)
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
            except (httpx.RequestError, httpx.HTTPStatusError, ValueError) as e:
                logger.warning(f"Attempt {attempt + 1} failed to fetch brand rules from {url}: {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(2)
                else:
                    logger.error(f"Failed to fetch brand rules after {max_attempts} attempts")
                    return {"brand_rules": []}

def normalize_model(model: Any) -> str:
    """Normalize a model string to lowercase."""
    if not isinstance(model, str):
        return str(model).strip().lower()
    return model.strip().lower()

def validate_model(row, expected_models, result_id, logger=None) -> bool:
    """
    Validate if the model in the row matches any expected models using exact substring matching.
    """
    logger = logger or logging.getLogger(__name__)
    input_model = clean_string(row.get('ProductModel', ''))
    if not input_model:
        logger.warning(f"ResultID {result_id}: No ProductModel provided in row")
        return False

    fields = [
        clean_string(row.get('ImageDesc', '')),
        clean_string(row.get('ImageSource', '')),
        clean_string(row.get('ImageUrl', ''))
    ]

    for expected_model in expected_models:
        expected_model = clean_string(expected_model)
        if not expected_model:
            continue
        for field in fields:
            if not field:
                continue
            if expected_model.lower() in field.lower():
                logger.info(f"ResultID {result_id}: Model match: '{expected_model}' in field")
                return True

    logger.warning(f"ResultID {result_id}: Model match failed: Input model='{input_model}', Expected models={expected_models}")
    return False

async def generate_brand_aliases(brand: str, predefined_aliases: Dict[str, List[str]]) -> List[str]:
    brand_clean = clean_string(brand).lower()
    if not brand_clean:
        return []

    aliases = [brand_clean]
    for key, alias_list in predefined_aliases.items():
        if clean_string(key).lower() == brand_clean:
            aliases.extend(clean_string(alias).lower() for alias in alias_list)

    base_brand = brand_clean.replace('&', 'and').replace('  ', ' ')
    variations = [
        base_brand.replace(' ', ''),
        base_brand.replace(' ', '-'),
        re.sub(r'[^a-z0-9]', '', base_brand),
    ]

    words = base_brand.split()
    if len(words) > 1:
        abbreviation = ''.join(word[0] for word in words if word)
        if len(abbreviation) >= 4:
            variations.append(abbreviation)
        variations.append(words[0])
        variations.append(words[-1])

    aliases.extend(variations)
    aliases = [alias for alias in aliases if len(alias) >= 4 and alias.lower() not in ["sas", "soda"]]
    return list(set(aliases))

def validate_brand(
    row: pd.Series,
    brand_aliases: List[str],
    result_id: str,
    domain_hierarchy: Optional[List[str]] = None,
    logger: Optional[logging.Logger] = None
) -> bool:
    logger = logger or logging.getLogger(__name__)
    if domain_hierarchy is not None and not isinstance(domain_hierarchy, (list, tuple)):
        logger.error(f"Invalid domain_hierarchy type: {type(domain_hierarchy)}. Expected list or None.")
        raise TypeError("domain_hierarchy must be a list or None")

    fields = [
        clean_string(row.get('ImageDesc', ''), preserve_url=False),
        clean_string(row.get('ImageSource', ''), preserve_url=True),
        clean_string(row.get('ImageUrl', ''), preserve_url=True)
    ]

    for alias in brand_aliases:
        alias_lower = clean_string(alias).lower()
        pattern = rf'\b{re.escape(alias_lower)}\b'
        for field in fields:
            if not field:
                continue
            if re.search(pattern, field.lower()):
                logger.debug(f"ResultID {result_id}: Exact brand match: '{alias_lower}' in field")
                return True
            if fuzz.partial_ratio(alias_lower, field.lower()) > 85:
                logger.debug(f"ResultID {result_id}: Fuzzy brand match: '{alias_lower}' in field")
                return True

    if domain_hierarchy is not None:
        domain_pattern = '|'.join(re.escape(domain.lower()) for domain in domain_hierarchy)
        for field in fields:
            if field and re.search(domain_pattern, field.lower()):
                logger.debug(f"ResultID {result_id}: Domain match: '{domain_pattern}' in field")
                return True

    logger.warning(
        f"ResultID {result_id}: Brand validation failed for aliases {brand_aliases}, "
        f"ImageDesc: {fields[0][:100]}, ImageSource: {fields[1][:100]}, ImageUrl: {fields[2][:100]}"
    )
    return False

async def filter_model_results(df: pd.DataFrame, debug: bool = True, logger: Optional[logging.Logger] = None, brand_aliases: Optional[List[str]] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
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

            has_model_match = validate_model(row, aliases, result_id, logger)
            has_brand_match = False

            source = clean_string(row.get('ImageSource', ''), preserve_url=True)
            url = clean_string(row.get('ImageUrl', ''), preserve_url=True)
            desc = clean_string(row.get('ImageDesc', ''))
            combined_text = f"{source} {desc} {url}".lower()
            if re.search(brand_pattern, combined_text) or re.search(domain_pattern, combined_text) or (brand_aliases and any(alias.lower() in combined_text for alias in brand_aliases)):
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