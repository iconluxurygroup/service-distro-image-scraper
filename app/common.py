import logging
import re
import unicodedata
import requests
import httpx
import aiohttp
import os
import shutil
import asyncio
from typing import List, Optional, Dict, Any, Tuple
from fuzzywuzzy import fuzz
from functools import lru_cache
from config import BASE_CONFIG_URL

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

CONFIG_FILES = {
    "category_hierarchy": "category_hierarchy.json",
    "category_mapping": "category_mapping.json",
    "fashion_labels": "fashion_labels.json",
    "non_fashion_labels": "non_fashion_labels.json",
    "brand_rules": "brand_rules.json"
}

async def create_temp_dirs(file_id: int, logger: Optional[logging.Logger] = None) -> Tuple[str, str]:
    logger = logger or default_logger
    temp_images_dir = f"temp_images_{file_id}"
    temp_excel_dir = f"temp_excel_{file_id}"
    
    os.makedirs(temp_images_dir, exist_ok=True)
    os.makedirs(temp_excel_dir, exist_ok=True)
    
    logger.debug(f"Created temp directories: {temp_images_dir}, {temp_excel_dir}")
    return temp_images_dir, temp_excel_dir

async def cleanup_temp_dirs(dirs: List[str], logger: Optional[logging.Logger] = None) -> None:
    logger = logger or default_logger
    for dir_path in dirs:
        if os.path.exists(dir_path):
            try:
                shutil.rmtree(dir_path)
                logger.debug(f"Removed temp directory: {dir_path}")
            except Exception as e:
                logger.error(f"Failed to remove temp directory {dir_path}: {e}", exc_info=True)

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
    logger = logger or default_logger
    url = f"{BASE_CONFIG_URL}{CONFIG_FILES[file_key]}"
    
    try:
        config = sync_load_config(file_key, url, config_name, expect_list)
        logger.info(f"Loaded {config_name} from cache for {url}")
        return config
    except Exception:
        logger.debug(f"Cache miss for {config_name}, attempting async load")

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
    if not isinstance(model, str):
        model = str(model)
    if not model or model.strip() == '':
        return []
    
    aliases = {model, model.lower(), model.upper()}
    separators = ['_', '-', ' ', '/', '.']
    base_model = model
    
    for sep in separators:
        base_model = base_model.replace(sep, '')
        aliases.add(base_model)
    
    for sep in separators:
        if 's69' in base_model.lower():
            idx = base_model.lower().index('s69')
            alias_with_sep = base_model[:idx] + sep + base_model[idx:]
            aliases.add(alias_with_sep)
            aliases.add(alias_with_sep.lower())
            aliases.add(alias_with_sep.upper())
    
    digits_only = re.sub(r'[^0-9]', '', base_model)
    if digits_only and digits_only.isdigit():
        aliases.add(digits_only)
    
    for sep in separators:
        if sep in model:
            base = model.split(sep)[0]
            aliases.add(base)
            break
    
    return [a for a in aliases if a and len(a) >= len(model) - 3]

async def fetch_brand_rules(
    file_key: str = "brand_rules",
    max_attempts: int = 3,
    timeout: int = 10,
    logger: Optional[logging.Logger] = None
) -> Optional[Dict]:
    logger = logger or default_logger
    url = f"{BASE_CONFIG_URL}{CONFIG_FILES.get(file_key, 'brand_rules.json')}"
    
    async with httpx.AsyncClient() as client:
        for attempt in range(1, max_attempts + 1):
            try:
                response = await client.get(url, timeout=timeout)
                response.raise_for_status()
                brand_rules = response.json()
                
                if not isinstance(brand_rules, dict) or "brand_rules" not in brand_rules:
                    logger.error(f"Invalid brand rules format from {url}")
                    return {"brand_rules": []}
                
                logger.info(f"Successfully fetched brand rules from {url} on attempt {attempt}")
                return brand_rules
            
            except (httpx.RequestError, httpx.HTTPStatusError, ValueError) as e:
                logger.warning(f"Attempt {attempt}/{max_attempts} failed to fetch brand rules from {url}: {e}")
                if attempt < max_attempts:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Failed to fetch brand rules after {max_attempts} attempts")
                    return {"brand_rules": []}  # Fallback to empty rules

    logger.error(f"Critical failure fetching brand rules from {url}")
    return {"brand_rules": []}

def normalize_model(model: Any) -> str:
    if not isinstance(model, str):
        return str(model).strip().lower()
    return model.strip().lower()

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
    seen = set()
    filtered_aliases = []
    for alias in aliases:
        alias_lower = alias.lower()
        if len(alias_lower) >= 4 and alias_lower not in ["sas", "soda","s&s","scotch"] and alias_lower not in seen:
            seen.add(alias_lower)
            filtered_aliases.append(alias_lower)

    return filtered_aliases

def validate_model(row: Dict, expected_models: List[str], result_id: str, logger: Optional[logging.Logger] = None) -> bool:
    logger = logger or default_logger
    input_model = clean_string(row.get('ProductModel', ''))
    if not input_model:
        logger.warning(f"ResultID {result_id}: No ProductModel provided in row")
        return False

    fields = [
        clean_string(row.get('ImageDesc', '')),
        clean_string(row.get('ImageSource', '')),
        clean_string(row.get('ImageUrl', ''))
    ]

    def normalize_separators(text):
        for sep in ['_', '-', ' ', '/', '.']:
            text = text.replace(sep, '')
        return text.lower()

    normalized_fields = [normalize_separators(field) for field in fields]

    for expected_model in expected_models:
        expected_model_clean = clean_string(expected_model)
        if not expected_model_clean:
            continue
        normalized_expected = normalize_separators(expected_model_clean)
        for field, norm_field in zip(fields, normalized_fields):
            if not field:
                continue
            if (expected_model_clean.lower() in field.lower() or
                normalized_expected in norm_field):
                logger.info(f"ResultID {result_id}: Model match: '{expected_model}' in field")
                return True

    logger.warning(f"ResultID {result_id}: Model match failed: Input model='{input_model}', Expected models={expected_models}")
    return False

def validate_brand(
    row: Dict,
    brand_aliases: List[str],
    result_id: str,
    domain_hierarchy: Optional[List[str]] = None,
    logger: Optional[logging.Logger] = None
) -> bool:
    logger = logger or default_logger
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

async def filter_model_results(
    results: List[Dict],
    debug: bool = True,
    logger: Optional[logging.Logger] = None,
    brand_aliases: Optional[List[str]] = None
) -> Tuple[List[Dict], List[Dict]]:
    logger = logger or default_logger
    try:
        if debug:
            logger.debug("\nDebugging and Filtering Model Results:")

        required_keys = ['ProductModel', 'ImageSource', 'ImageUrl', 'ImageDesc', 'ResultID']
        for res in results:
            if not all(key in res for key in required_keys):
                missing_keys = [key for key in required_keys if key not in res]
                raise ValueError(f"Result missing required keys: {missing_keys}")

        keep_results = []
        discarded_results = []

        if not results:
            logger.warning("Empty result list provided to filter_model_results")
            return [], []

        brand_rules = await fetch_brand_rules(logger=logger)
        brand_names = []
        domain_hierarchy = []
        for rule in brand_rules.get("brand_rules", []):
            if rule["is_active"]:
                brand_names.extend(rule["names"])
                domain_hierarchy.extend(rule.get("domain_hierarchy", []))
        brand_pattern = '|'.join(re.escape(name.lower()) for name in brand_names)
        domain_pattern = '|'.join(re.escape(domain.lower()) for domain in domain_hierarchy)

        for res in results:
            result_id = res['ResultID']
            model = str(res['ProductModel'])
            if not model.strip():
                logger.warning(f"ResultID {result_id}: Skipping row due to missing or empty ProductModel")
                discarded_results.append(res)
                continue

            aliases = generate_aliases(model)
            has_model_match = validate_model(res, aliases, result_id, logger)
            has_brand_match = False

            source = clean_string(res.get('ImageSource', ''), preserve_url=True)
            url = clean_string(res.get('ImageUrl', ''), preserve_url=True)
            desc = clean_string(res.get('ImageDesc', ''))
            combined_text = f"{source} {desc} {url}".lower()
            if re.search(brand_pattern, combined_text) or re.search(domain_pattern, combined_text) or (brand_aliases and any(alias.lower() in combined_text for alias in brand_aliases)):
                has_brand_match = True
                if debug:
                    logger.debug(f"ResultID {result_id}: Brand or domain match found")

            if has_model_match or has_brand_match:
                keep_results.append(res)
                if debug:
                    logger.debug(f"ResultID {result_id}: Keeping row (model_match={has_model_match}, brand_match={has_brand_match})")
            else:
                discarded_results.append(res)
                if debug:
                    logger.debug(f"ResultID {result_id}: Discarding row (no model or brand match)")

        logger.info(f"Filtered {len(keep_results)} rows with matches and {len(discarded_results)} rows discarded")
        return keep_results, discarded_results
    except Exception as e:
        logger.error(f"Error in filter_model_results: {e}", exc_info=True)
        return [], results

def calculate_priority(
    row: Dict,
    exact_results: List[Dict],
    model_clean: str,
    model_aliases: List[str],
    brand_clean: str,
    brand_aliases: List[str],
    logger: Optional[logging.Logger] = None
) -> int:
    logger = logger or default_logger
    try:
        model_matched = any(res['ResultID'] == row['ResultID'] for res in exact_results)
        brand_matched = False

        desc_clean = row.get('ImageDesc_clean', '').lower()
        source_clean = row.get('ImageSource_clean', '').lower()
        url_clean = row.get('ImageUrl_clean', '').lower()
        
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