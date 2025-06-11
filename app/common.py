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
import urllib.parse
from datetime import datetime, timedelta

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
def validate_thumbnail_url(url: Optional[str], logger: Optional[logging.Logger] = None) -> bool:
    """
    Validates a thumbnail URL by checking for HTTP/HTTPS scheme and absence of placeholder values.
    """
    logger = logger or default_logger
    if not url or not isinstance(url, str) or re.search(r'placeholder', url, re.IGNORECASE):
        logger.debug(f"Invalid thumbnail URL: {url}")
        return False
    if not url.startswith(('http://', 'https://')):
        logger.debug(f"Non-HTTP thumbnail URL: {url}")
        return False
    return True

def clean_url_string(value: Optional[str], is_url: bool = True, logger: Optional[logging.Logger] = None) -> str:
    """
    Cleans a string, particularly URLs, by removing invalid characters and normalizing structure.
    """
    logger = logger or default_logger
    if not value:
        return ""
    cleaned = str(value).replace('\\', '').replace('%5C', '').replace('%5c', '')
    cleaned = re.sub(r'[\x00-\x1F\x7F]+', '', cleaned).strip()
    if is_url:
        cleaned = urllib.parse.unquote(cleaned)
        try:
            parsed = urllib.parse.urlparse(cleaned)
            if not parsed.scheme or not parsed.netloc:
                logger.debug(f"Invalid URL format: {cleaned[:100]}")
                return ""
            path = re.sub(r'/+', '/', parsed.path)
            cleaned = f"{parsed.scheme}://{parsed.netloc}{path}"
            if parsed.query:
                cleaned += f"?{parsed.query}"
            if parsed.fragment:
                cleaned += f"#{parsed.fragment}"
        except ValueError as e:
            logger.debug(f"Invalid URL format: {cleaned[:100]}, error: {e}")
            return ""
    return cleaned
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

class ConfigCache:
    def __init__(self, ttl_seconds=300):
        self.ttl = ttl_seconds
        self.cache = {}
        self.last_updated = {}

    async def load_config(self, file_key: str, url: str, config_name: str, expect_list: bool = False, max_attempts: int = 3, timeout: float = 10, logger=None):
        logger = logger or default_logger
        cache_key = (file_key, url)
        now = datetime.now()
        if cache_key in self.cache and self.last_updated.get(cache_key) and (now - self.last_updated[cache_key]).total_seconds() < self.ttl:
            logger.debug(f"Returning cached {config_name} from {url}")
            return self.cache[cache_key]

        for attempt in range(1, max_attempts + 1):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=timeout) as response:
                        response.raise_for_status()
                        config = await response.json()
                        if expect_list and not isinstance(config, list):
                            raise ValueError(f"{config_name} must be a list")
                        self.cache[cache_key] = config
                        self.last_updated[cache_key] = now
                        logger.info(f"Loaded {config_name} from {url} on attempt {attempt}")
                        return config
            except (aiohttp.ClientError, ValueError) as e:
                logger.warning(f"Attempt {attempt}/{max_attempts} failed to load {config_name}: {e}")
                if attempt < max_attempts:
                    await asyncio.sleep(2 * attempt)
                else:
                    logger.error(f"Failed to load {config_name} after {max_attempts} attempts")
                    return [] if expect_list else {}
        return [] if expect_list else {}

config_cache = ConfigCache()

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
    # Note: timeout is derived from backoff_factor here, not passed directly
    timeout = backoff_factor * retries
    config = await config_cache.load_config(file_key, url, config_name, expect_list, retries, timeout, logger)
    return config if config else fallback

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

class BrandRulesCache:
    def __init__(self, ttl_seconds=300):
        self.ttl = ttl_seconds
        self.cache = None
        self.last_updated = None

    async def fetch_brand_rules(self, url: str, max_attempts: int = 3, timeout: float = 10, logger=None):
        logger = logger or default_logger
        now = datetime.now()
        if self.cache and self.last_updated and (now - self.last_updated).total_seconds() < self.ttl:
            logger.debug(f"Returning cached brand rules from {url}")
            return self.cache
        
        for attempt in range(1, max_attempts + 1):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=timeout) as response:
                        response.raise_for_status()
                        self.cache = await response.json()
                        self.last_updated = now
                        logger.info(f"Successfully fetched and cached brand rules from {url} on attempt {attempt}")
                        return self.cache
            except (aiohttp.ClientError, ValueError) as e:
                logger.warning(f"Attempt {attempt}/{max_attempts} failed to fetch brand rules: {e}")
                if attempt < max_attempts:
                    await asyncio.sleep(2 ** attempt)
                else:
                    logger.error(f"Failed to fetch brand rules after {max_attempts} attempts")
                    return {"brand_rules": []}
        return {"brand_rules": []}

brand_rules_cache = BrandRulesCache()

async def fetch_brand_rules(
    file_key: str = "brand_rules",
    max_attempts: int = 3,
    timeout: int = 10,
    logger: Optional[logging.Logger] = None
) -> Dict[str, Any]:
    logger = logger or default_logger
    url = f"{BASE_CONFIG_URL}{CONFIG_FILES.get(file_key, 'brand_rules.json')}"
    return await brand_rules_cache.fetch_brand_rules(url, max_attempts, timeout, logger)

async def preprocess_sku(
    search_string: str,
    known_brand: Optional[str] = None,
    brand_rules: Optional[Dict] = None,
    logger: Optional[logging.Logger] = None
) -> Tuple[str, Optional[str], Optional[str], Optional[str]]:
    """
    Preprocesses a search string to identify brand, model, and color based on brand rules.

    - If a `known_brand` is provided, it exclusively attempts to process the SKU against that brand's rules.
    - If the SKU format matches the rule, it's parsed into model/color.
    - If the format does not match, the brand is "forced," and the original SKU is used as the model.
    - If the `known_brand` is not in the rules, it is still returned, preventing incorrect matching with other brands.
    - If no `known_brand` is provided, it iterates through all rules to find a potential match.
    """
    logger = logger or logging.getLogger(__name__)
    brand_rules_data = brand_rules or await fetch_brand_rules(logger=logger)

    if not search_string or not isinstance(search_string, str):
        logger.warning(f"Invalid search string: {search_string}")
        return search_string, known_brand, search_string, None

    search_string_clean = clean_string(search_string).lower()

    # --- Internal function to safely process a rule ---
    def process_rule(rule, current_sku):
        sku_format = rule.get("sku_format", {})
        expected_length = rule.get("expected_length", {})

        base_lengths = expected_length.get("base", [])
        base_length = int(base_lengths[0]) if base_lengths else 0

        with_color_lengths = expected_length.get("with_color", [])
        with_color_length = int(with_color_lengths[0]) if with_color_lengths else 0

        color_ext_lengths = sku_format.get("color_extension", [])
        color_extension_length = int(color_ext_lengths[0]) if color_ext_lengths else 0

        color_separator = sku_format.get("color_separator")

        # Try splitting on color_separator if it exists
        if color_separator and color_separator in current_sku:
            parts = current_sku.rsplit(color_separator, 1)
            if len(parts) > 1:
                base_part, color_part = parts[0].strip(), parts[1].strip()
                # Check if lengths are plausible
                if (base_length == 0 or abs(len(base_part) - base_length) <= 2) and \
                   (color_extension_length == 0 or len(color_part) <= color_extension_length + 2):
                    return rule.get("full_name"), base_part, color_part

        # Try splitting based on expected lengths if lengths are defined
        if with_color_length > 0 and color_extension_length > 0 and len(current_sku) >= with_color_length:
            base_part = current_sku[:-color_extension_length]
            color_part = current_sku[-color_extension_length:]
            return rule.get("full_name"), base_part, color_part

        # Check if it matches the base length
        if base_length > 0 and abs(len(current_sku) - base_length) <= 2:
            return rule.get("full_name"), current_sku, None

        return None, None, None

    # --- Main logic ---

    # 1. If a known_brand is provided, handle it exclusively and then exit.
    if known_brand:
        known_brand_clean = clean_string(known_brand).lower()
        for rule in brand_rules_data.get("brand_rules", []):
            if not rule.get("is_active", False):
                continue

            rule_name_clean = clean_string(rule.get("full_name", "")).lower()
            aliases_clean = [clean_string(name).lower() for name in rule.get("names", [])]

            if known_brand_clean == rule_name_clean or known_brand_clean in aliases_clean:
                # Found the rule for the known brand.
                brand, model, color = process_rule(rule, search_string_clean)
                if brand: # Rule successfully parsed the SKU
                    logger.debug(f"Preprocessed SKU '{search_string}' with known brand '{brand}', model '{model}', color '{color}'")
                    return search_string, brand, model, color

                # Rule did not parse SKU, but we matched the brand. Force it and return.
                logger.debug(f"Forced brand match for '{known_brand}'. SKU did not match rule structure.")
                return search_string, rule.get("full_name"), search_string_clean, None

        # If the loop finished, the known_brand was provided but NOT found in the rules.
        # Log it, and return the original brand to prevent fall-through.
        logger.warning(f"Known brand '{known_brand}' not found in active brand rules. Treating as unmatched and returning original brand.")
        return search_string, known_brand, search_string_clean, None

    # 2. This part is ONLY executed if known_brand was NOT provided.
    for rule in brand_rules_data.get("brand_rules", []):
        if not rule.get("is_active", False):
            continue
        brand, model, color = process_rule(rule, search_string_clean)
        if brand:
            logger.debug(f"Preprocessed SKU '{search_string}' to brand '{brand}', model '{model}', color '{color}'")
            return search_string, brand, model, color

    # 3. Fallback: No brand was provided, and no rule matched.
    logger.warning(f"No brand matched for SKU '{search_string}'")
    return search_string, None, search_string_clean, None

def generate_aliases(model: Any, article_length: int = 8, model_length: int = 7, color: Optional[str] = None) -> List[str]:
    logger = logging.getLogger(__name__)
    if not isinstance(model, str):
        model = str(model)
    if not model or model.strip() == '':
        return []

    model = clean_string(model).lower()
    aliases = set()

    # Base model
    aliases.add(model)

    # Digits-only
    digits_only = re.sub(r'[^0-9]', '', model)
    if digits_only and digits_only.isdigit():
        aliases.add(digits_only)

    # Delimiter variations
    delimiters = ['-', '_', ' ', '.']
    if len(model) >= article_length + model_length:
        article = model[:article_length]
        model_part = model[article_length:article_length + model_length]
        for delim in delimiters:
            alias = f"{article}{delim}{model_part}"
            aliases.add(alias)
            if color:
                for delim2 in delimiters:
                    aliases.add(f"{article}{delim}{model_part}{delim2}{color}")

    logger.debug(f"Generated aliases for model '{model}' with article_length={article_length}, color='{color}': {aliases}")
    return [a for a in aliases if a and len(a) >= 4]

async def generate_search_variations(
    search_string: str,
    brand: Optional[str] = None,
    model: Optional[str] = None,
    color: Optional[str] = None,
    category: Optional[str] = None,
    brand_rules: Optional[Dict] = None,
    logger: Optional[logging.Logger] = None
) -> Dict[str, List[str]]:
    logger = logger or logging.getLogger(__name__)
    variations = {
        "default": [],
        "delimiter_variations": [],
        "brand_alias": [],
        "no_color": [],
        "model_alias": []
    }
    all_variations = set()

    if not search_string or not isinstance(search_string, str):
        logger.warning("Empty or invalid search string provided")
        return variations

    raw_search_string = search_string
    search_string = clean_string(search_string).lower()
    brand = clean_string(brand).lower() if brand else None
    model = clean_string(model).lower() if model else search_string
    color = clean_string(color).lower() if color else None
    brand_rules_data = brand_rules or await fetch_brand_rules(logger=logger)
    logger.debug(f"Input: search_string='{search_string}', brand='{brand}', model='{model}', color='{color}'")

    variations["default"].append(raw_search_string)
    all_variations.add(search_string)
    logger.debug(f"Added default variation: '{raw_search_string}'")

    matched_rule = None
    if brand:
        for rule in brand_rules_data.get("brand_rules", []):
            if rule.get("is_active", False) and clean_string(rule.get("full_name", "")).lower() == brand:
                matched_rule = rule
                break

    delimiters = [' ', '-', '_']
    if matched_rule:
        # --- Start of SAFE dictionary access ---
        sku_format = matched_rule.get("sku_format", {})
        base_format = sku_format.get("base", {})
        article_lengths = base_format.get("article", ["8"])
        model_lengths = base_format.get("model", ["7"])
        color_lengths = sku_format.get("color_extension", ["0"])
        
        article_length = int(article_lengths[0]) if article_lengths else 8
        model_length = int(model_lengths[0]) if model_lengths else 7
        color_length = int(color_lengths[0]) if color_lengths else 0
        
        expected_lengths = matched_rule.get("expected_length", {})
        with_color_lengths = expected_lengths.get("with_color", [])
        # --- End of SAFE dictionary access ---
        
        base = model if model else search_string
        color_part = color if color else ""

        # Handle missing separator based on expected length
        if not color_part and with_color_lengths and len(search_string) == int(with_color_lengths[0]):
            base = search_string[:-color_length]
            color_part = search_string[-color_length:]
            logger.debug(f"Assumed color: '{color_part}', base: '{base}'")

        # No Color: Base only
        no_color_var = base[:article_length + model_length]
        if no_color_var not in all_variations:
            variations["no_color"].append(no_color_var)
            all_variations.add(no_color_var)
        logger.debug(f"Added no_color variation: '{no_color_var}'")

        # Delimiter Variations
        if color_part:
            for delim in delimiters:
                delim_var = f"{base}{delim}{color_part}"
                if delim_var not in all_variations:
                    variations["delimiter_variations"].append(delim_var)
                    all_variations.add(delim_var)
            logger.debug(f"Generated delimiter variations: {variations['delimiter_variations']}")

        # Brand Alias
        brand_alias_var = f"{matched_rule.get('full_name', brand).lower()} {search_string}"
        if brand_alias_var not in all_variations:
            variations["brand_alias"].append(brand_alias_var)
            all_variations.add(brand_alias_var)
        logger.debug(f"Added brand_alias: '{brand_alias_var}'")

        # Model Alias
        article = base[:article_length]
        model_part = base[article_length:article_length + model_length]
        for delim1 in delimiters:
            base_alias = f"{article}{delim1}{model_part}"
            if base_alias not in all_variations:
                variations["model_alias"].append(base_alias)
                all_variations.add(base_alias)
            if color_part:
                for delim2 in delimiters:
                    full_alias = f"{base_alias}{delim2}{color_part}"
                    if full_alias not in all_variations:
                        variations["model_alias"].append(full_alias)
                        all_variations.add(full_alias)
        logger.debug(f"Generated model_alias variations: {variations['model_alias']}")

    else:
        logger.warning(f"No brand matched for SKU: {search_string}. Using fallback (default only).")

    variations = {k: [item.upper() for item in v] for k, v in variations.items() if v}
    total_variations = sum(len(v) for v in variations.values())
    logger.info(f"Generated total of {total_variations} unique variations for search string '{raw_search_string}': {variations}")
    return variations

async def create_predefined_aliases(logger: Optional[logging.Logger] = None) -> Dict[str, List[str]]:
    logger = logger or default_logger
    # Load brand_rules using load_config
    brand_rules = await load_config(
        file_key="brand_rules",
        fallback={"brand_rules": []},
        logger=logger,
        config_name="brand_rules"
    )
    
    predefined_aliases = {}
    for rule in brand_rules.get("brand_rules", []):
        if rule.get("is_active", False):
            full_name = rule.get("full_name", "")
            aliases = rule.get("names", [])
            if full_name:
                predefined_aliases[full_name] = aliases
    
    logger.debug(f"Created predefined_aliases: {predefined_aliases}")
    return predefined_aliases

async def generate_brand_aliases(brand: str, predefined_aliases: Dict[str, List[str]]) -> List[str]:
    brand_clean = clean_string(brand).lower()
    if not brand_clean:
        return []

    aliases = []
    for key, alias_list in predefined_aliases.items():
        key_clean = clean_string(key).lower()
        if key_clean == brand_clean:
            aliases.append(key)  # Add full name with original case
            aliases.extend(clean_string(alias) for alias in alias_list if clean_string(alias).lower() != key_clean)
            break

    seen = set()
    filtered_aliases = []
    for alias in aliases:
        alias_lower = alias.lower()
        if len(alias_lower) >= 2 and alias_lower not in seen:
            seen.add(alias_lower)
            filtered_aliases.append(alias)  # Preserve original case

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