import logging
import asyncio
import json
from typing import Optional, List, Dict
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from database_config import conn_str, async_engine
from common import clean_string, normalize_model, validate_model, generate_aliases, generate_brand_aliases, calculate_priority, filter_model_results
import psutil
import re
import unicodedata

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

def validate_thumbnail_url(url: Optional[str], logger: Optional[logging.Logger] = None) -> bool:
    logger = logger or logging.getLogger(__name__)
    if not url or url == '' or 'placeholder' in str(url).lower():
        logger.debug(f"Invalid thumbnail URL: {url}")
        return False
    if not str(url).startswith(('http://', 'https://')):
        logger.debug(f"Non-HTTP thumbnail URL: {url}")
        return False
    return True

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type(SQLAlchemyError),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying insert_search_results for FileID {retry_state.kwargs.get('file_id', 'unknown')} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def insert_search_results(
    results: List[Dict],
    logger: Optional[logging.Logger] = None,
    file_id: str = None
) -> bool:
    logger = logger or logging.getLogger(__name__)
    process = psutil.Process()

    if not results:
        logger.warning(f"Worker PID {process.pid}: Empty results provided for insert_search_results")
        return False

    required_columns = ["EntryID", "ImageUrl", "ImageDesc", "ImageSource", "ImageUrlThumbnail"]
    for res in results:
        if not all(col in res for col in required_columns):
            missing_cols = set(required_columns) - set(res.keys())
            logger.error(f"Worker PID {process.pid}: Missing required columns: {missing_cols}")
            return False

    try:
        parameters = []
        # Check for duplicates
        async with async_engine.connect() as conn:
            existing_urls = set()
            for res in results:
                result = await conn.execute(
                    text("SELECT ImageUrl FROM utb_ImageScraperResult WHERE EntryID = :entry_id"),
                    {"entry_id": res["EntryID"]}
                )
                existing_urls.update(row[0] for row in result.fetchall())
                result.close()
            await conn.commit()

        for res in results:
            try:
                entry_id = int(res["EntryID"])
            except (ValueError, TypeError):
                logger.error(f"Worker PID {process.pid}: Invalid EntryID value: {res.get('EntryID')}")
                return False

            image_url = str(res.get("ImageUrl", "")) if res.get("ImageUrl") else ""
            if image_url in existing_urls:
                logger.debug(f"Worker PID {process.pid}: Skipping duplicate ImageUrl: {image_url}")
                continue

            category = res.get("ProductCategory", "")
            if category.lower() == "footwear" and any(keyword in image_url.lower() for keyword in ["appliance", "whirlpool", "parts"]):
                logger.debug(f"Worker PID {process.pid}: Filtered out irrelevant URL: {image_url}")
                continue

            parameters.append({
                "entry_id": entry_id,
                "image_url": image_url,
                "image_desc": str(res.get("ImageDesc", "")) if res.get("ImageDesc") else "",
                "image_source": str(res.get("ImageSource", "")) if res.get("ImageSource") else "",
                "image_url_thumbnail": str(res.get("ImageUrlThumbnail", "")) if res.get("ImageUrlThumbnail") else ""
            })

        logger.debug(f"Worker PID {process.pid}: Inserting {len(parameters)} rows into utb_ImageScraperResult")
        if not parameters:
            logger.warning(f"Worker PID {process.pid}: No valid rows to insert for FileID {file_id}")
            return False

        async with async_engine.connect() as conn:
            try:
                await conn.execute(text("SELECT 1"))
                await conn.commit()
            except Exception as e:
                logger.warning(f"Worker PID {process.pid}: Failed to clear connection state: {e}")

            await conn.execute(
                text("""
                    INSERT INTO utb_ImageScraperResult (EntryID, ImageUrl, ImageDesc, ImageSource, ImageUrlThumbnail)
                    VALUES (:entry_id, :image_url, :image_desc, :image_source, :image_url_thumbnail)
                """),
                parameters
            )
            await conn.commit()
            logger.info(f"Worker PID {process.pid}: Successfully inserted {len(parameters)} rows for FileID {file_id}")
            return True
    except SQLAlchemyError as e:
        logger.error(f"Worker PID {process.pid}: Database error inserting results for FileID {file_id}: {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Worker PID {process.pid}: Unexpected error inserting results for FileID {file_id}: {e}", exc_info=True)
        return False

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type((SQLAlchemyError, ValueError)),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying update_search_sort_order for EntryID {retry_state.kwargs['entry_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def update_search_sort_order(
    file_id: str,
    entry_id: str,
    brand: Optional[str] = None,
    model: Optional[str] = None,
    color: Optional[str] = None,
    category: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
    brand_rules: Optional[Dict] = None
) -> Optional[bool]:
    logger = logger or logging.getLogger(__name__)
    process = psutil.Process()

    def normalize_text(text: str) -> str:
        if not text:
            return ""
        # Decode Unicode escapes (e.g., \u0026 to &), normalize to NFC, and lowercase
        text = unicodedata.normalize('NFC', text.encode().decode('unicode_escape')).lower().strip()
        return text

    try:
        file_id = int(file_id)
        entry_id = int(entry_id)
        logger.info(f"Worker PID {process.pid}: Starting SortOrder update for FileID: {file_id}, EntryID: {entry_id}")

        # Fetch attributes if not provided
        async with async_engine.connect() as conn:
            if not all([brand, model]):
                result = await conn.execute(
                    text("""
                        SELECT ProductBrand, ProductModel, ProductColor, ProductCategory
                        FROM utb_ImageScraperRecords
                        WHERE FileID = :file_id AND EntryID = :entry_id
                    """),
                    {"file_id": file_id, "entry_id": entry_id}
                )
                row = result.fetchone()
                result.close()
                await conn.commit()
                if row:
                    brand, model, color, category = row
                    logger.info(f"Worker PID {process.pid}: Fetched attributes - Brand: {brand}, Model: {model}, Color: {color}, Category: {category}")
                else:
                    logger.warning(f"Worker PID {process.pid}: No attributes found for FileID: {file_id}, EntryID: {entry_id}")
                    brand = brand or ''
                    model = model or ''
                    color = color or ''
                    category = category or ''

        # Generate brand and model aliases
        brand_clean = normalize_text(brand)
        brand_aliases_dict = {
            "Scotch & Soda": [
                "Scotch and Soda", "Scotch Soda", "Scotch&Soda", "ScotchAndSoda", "Scotch"
            ],
            "Adidas": ["Adidas AG", "Adidas Originals", "Addidas", "Adiddas"],
            "BAPE": ["A Bathing Ape", "BATHING APE", "Bape Japan", "ABathingApe", "Bape"]
        }
        brand_aliases = [normalize_text(alias) for alias in brand_aliases_dict.get(brand, [brand_clean])]
        if not brand_aliases and brand_clean:
            brand_aliases = [brand_clean]
        logger.debug(f"Worker PID {process.pid}: Using brand aliases: {brand_aliases}")

        model_clean = normalize_text(model) if model else ''
        model_aliases = generate_aliases(model_clean) if model_clean else []
        logger.debug(f"Worker PID {process.pid}: Using model aliases: {model_aliases}")

        # Reset SortOrder to NULL
        async with async_engine.connect() as conn:
            await conn.execute(
                text("""
                    UPDATE utb_ImageScraperResult
                    SET SortOrder = NULL
                    WHERE EntryID = :entry_id
                """),
                {"entry_id": entry_id}
            )
            await conn.commit()
            logger.debug(f"Worker PID {process.pid}: Reset SortOrder to NULL for EntryID {entry_id}")

        # Fetch results
        async with async_engine.connect() as conn:
            query = text("""
                SELECT 
                    t.ResultID, 
                    t.EntryID,
                    ISNULL(CAST(JSON_VALUE(t.AiJson, '$.match_score') AS FLOAT), 0) AS match_score,
                    t.SortOrder,
                    t.ImageDesc, 
                    t.ImageSource, 
                    t.ImageUrl,
                    r.ProductBrand, 
                    r.ProductModel
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                WHERE r.FileID = :file_id AND t.EntryID = :entry_id
            """)
            result = await conn.execute(query, {"file_id": file_id, "entry_id": entry_id})
            rows = result.fetchall()
            columns = result.keys()
            result.close()
            await conn.commit()

        if not rows:
            logger.warning(f"Worker PID {process.pid}: No eligible data found for FileID: {file_id}, EntryID: {entry_id}")
            return False

        # Convert rows to list of dicts
        results = [dict(zip(columns, row)) for row in rows]
        logger.info(f"Worker PID {process.pid}: Fetched {len(results)} rows for EntryID {entry_id}")

        # Clean and normalize result fields
        for res in results:
            for col in ["ImageDesc", "ImageSource", "ImageUrl", "ProductBrand", "ProductModel"]:
                res[f"{col}_clean"] = normalize_text(res.get(col, '')) if res.get(col) else ''
            res["priority"] = None
            res["new_sort_order"] = -2  # Default sort order

        # Simplified brand validation
        match_results = []
        for res in results:
            image_desc = res["ImageDesc_clean"]
            if not image_desc and not res["ImageSource_clean"] and not res["ImageUrl_clean"]:
                logger.debug(f"Worker PID {process.pid}: ResultID {res['ResultID']}: Empty fields, setting SortOrder=-1")
                res["new_sort_order"] = -1
                continue

            brand_found = False
            for alias in brand_aliases:
                if alias and alias in image_desc:
                    brand_found = True
                    res["new_sort_order"] = 1  # Positive SortOrder for brand match
                    res["priority"] = 3
                    match_results.append(res)
                    logger.debug(f"Worker PID {process.pid}: ResultID {res['ResultID']}: Matched brand alias '{alias}' in ImageDesc: {image_desc}")
                    break
            if not brand_found:
                logger.debug(f"Worker PID {process.pid}: ResultID {res['ResultID']}: No brand match in ImageDesc: {image_desc}")

        # Update SortOrder
        async with async_engine.connect() as conn:
            update_count = 0
            success_count = 0
            for res in results:
                try:
                    await conn.execute(
                        text("""
                            UPDATE utb_ImageScraperResult 
                            SET SortOrder = :sort_order 
                            WHERE ResultID = :result_id
                        """),
                        {"sort_order": int(res["new_sort_order"]), "result_id": int(res["ResultID"])}
                    )
                    update_count += 1
                    success_count += 1
                    logger.debug(f"Worker PID {process.pid}: Updated SortOrder to {res['new_sort_order']} for ResultID {res['ResultID']}")
                except SQLAlchemyError as e:
                    logger.error(f"Worker PID {process.pid}: Failed to update ResultID {res['ResultID']}: {e}")
                    continue
            await conn.commit()

            logger.info(f"Worker PID {process.pid}: Updated {success_count}/{update_count} rows for EntryID {entry_id}")

        # Verify updates
        async with async_engine.connect() as conn:
            verify_query = text("""
                SELECT ResultID, EntryID, SortOrder, ImageDesc, ImageSource, ImageUrl
                FROM utb_ImageScraperResult
                WHERE EntryID = :entry_id
                ORDER BY SortOrder DESC
            """)
            result = await conn.execute(verify_query, {"entry_id": entry_id})
            verified_results = [
                {
                    "ResultID": r[0],
                    "EntryID": r[1],
                    "SortOrder": r[2],
                    "ImageDesc": r[3],
                    "ImageSource": r[4],
                    "ImageUrl": r[5]
                }
                for r in result.fetchall()
            ]
            result.close()
            await conn.commit()

            positive_count = sum(1 for r in verified_results if r['SortOrder'] is not None and r['SortOrder'] > 0)
            zero_count = sum(1 for r in verified_results if r['SortOrder'] == 0)
            negative_count = sum(1 for r in verified_results if r['SortOrder'] is not None and r['SortOrder'] < 0)
            null_count = sum(1 for r in verified_results if r['SortOrder'] is None)
            logger.info(
                f"Worker PID {process.pid}: Verification for EntryID {entry_id}: "
                f"{positive_count} rows with positive SortOrder (model or brand matches), "
                f"{zero_count} rows with SortOrder=0 (legacy brand matches), "
                f"{negative_count} rows with negative SortOrder, "
                f"{null_count} rows with NULL SortOrder"
            )
            if null_count > 0:
                logger.warning(f"Worker PID {process.pid}: Found {null_count} rows with NULL SortOrder for EntryID {entry_id}")
                for r in verified_results:
                    if r['SortOrder'] is None:
                        logger.debug(f"Worker PID {process.pid}: NULL SortOrder for ResultID {r['ResultID']}, ImageDesc: {r['ImageDesc']}")

            for r in verified_results[:3]:
                logger.info(f"Worker PID {process.pid}: Sample - ResultID: {r['ResultID']}, EntryID: {r['EntryID']}, SortOrder: {r['SortOrder']}, ImageDesc: {r['ImageDesc']}")

        return success_count > 0

    except SQLAlchemyError as e:
        logger.error(f"Worker PID {process.pid}: Database error in update_search_sort_order for EntryID {entry_id}: {e}", exc_info=True)
        return False
    except ValueError as e:
        logger.error(f"Worker PID {process.pid}: ValueError in update_search_sort_order for EntryID {entry_id}: {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Worker PID {process.pid}: Unexpected error in update_search_sort_order for EntryID {entry_id}: {e}", exc_info=True)
        return False

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type(SQLAlchemyError),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying update_sort_order for FileID {retry_state.kwargs['file_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def update_sort_order(file_id: str, logger: Optional[logging.Logger] = None) -> Optional[List[Dict]]:
    logger = logger or logging.getLogger(__name__)
    try:
        file_id = int(file_id)
        logger.info(f"Starting batch SortOrder update for FileID: {file_id}")
        
        async with async_engine.connect() as conn:
            query = text("""
                SELECT EntryID, ProductBrand, ProductModel, ProductColor, ProductCategory 
                FROM utb_ImageScraperRecords 
                WHERE FileID = :file_id
            """)
            logger.debug(f"Executing query: {query} with FileID: {file_id}")
            result = await conn.execute(query, {"file_id": file_id})
            entries = result.fetchall()
            result.close()
            await conn.commit()
        
        if not entries:
            logger.warning(f"No entries found for FileID: {file_id}")
            return []
            
        results = []
        success_count = 0
        failure_count = 0
        
        for entry in entries:
            entry_id, brand, model, color, category = entry
            logger.debug(f"Worker PID {process.pid}: Processing EntryID {entry_id}, Brand: {brand}, Model: {model}")
            try:
                entry_results = await update_search_sort_order(
                    file_id=str(file_id),
                    entry_id=str(entry_id),
                    brand=brand,
                    model=model,
                    color=color,
                    category=category,
                    logger=logger
                )
                
                if entry_results:
                    results.append({"EntryID": entry_id, "Success": True})
                    success_count += 1
                else:
                    results.append({"EntryID": entry_id, "Success": False})
                    failure_count += 1
                    logger.warning(f"No results for EntryID {entry_id}")
            except Exception as e:
                results.append({"EntryID": entry_id, "Success": False, "Error": str(e)})
                failure_count += 1
                logger.error(f"Error processing EntryID {entry_id}: {e}", exc_info=True)
        
        logger.info(f"Completed batch SortOrder update for FileID {file_id}: {success_count} entries successful, {failure_count} failed")
        
        async with async_engine.connect() as conn:
            verification = {}
            queries = [
                ("PositiveSortOrderEntries", "t.SortOrder > 0"),
                ("BrandMatchEntries", "t.SortOrder = 0"),
                ("NoMatchEntries", "t.SortOrder < 0"),
                ("NullSortOrderEntries", "t.SortOrder IS NULL"),
                ("UnexpectedSortOrderEntries", "t.SortOrder = -1")
            ]
            for key, condition in queries:
                query = text(f"""
                    SELECT COUNT(DISTINCT t.EntryID)
                    FROM utb_ImageScraperResult t
                    INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                    WHERE r.FileID = :file_id AND {condition}
                """)
                result = await conn.execute(query, {"file_id": file_id})
                verification[key] = result.scalar()
                result.close()
            
            query = text("""
                SELECT t.EntryID, t.SortOrder, t.ImageUrl
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                WHERE r.FileID = :file_id
            """)
            result = await conn.execute(query, {"file_id": file_id})
            sort_orders = result.fetchall()
            logger.info(f"SortOrder values for FileID {file_id}: {[(row[0], row[1], row[2][:50]) for row in sort_orders]}")
            
            logger.info(f"Verification for FileID {file_id}: "
                       f"{verification['PositiveSortOrderEntries']} entries with model matches, "
                       f"{verification['BrandMatchEntries']} entries with brand matches only, "
                       f"{verification['NoMatchEntries']} entries with no matches, "
                       f"{verification['NullSortOrderEntries']} entries with NULL SortOrder, "
                       f"{verification['UnexpectedSortOrderEntries']} entries with unexpected SortOrder")
        
        return results
    except SQLAlchemyError as e:
        logger.error(f"Database error in batch SortOrder update for FileID {file_id}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Error in batch SortOrder update for FileID {file_id}: {e}", exc_info=True)
        return None

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type(SQLAlchemyError),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying update_sort_no_image_entry for FileID {retry_state.kwargs['file_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def update_sort_no_image_entry(file_id: str, logger: Optional[logging.Logger] = None) -> Optional[Dict]:
    logger = logger or logging.getLogger(__name__)
    try:
        file_id = int(file_id)
        logger.info(f"Starting per-entry SortOrder update for FileID: {file_id}")
        
        async with async_engine.begin() as conn:
            result = await conn.execute(
                text("""
                    SELECT COUNT(*) 
                    FROM utb_ImageScraperResult 
                    WHERE FileID = :file_id AND SortOrder IS NULL
                """),
                {"file_id": file_id}
            )
            null_count = result.scalar()
            logger.debug(f"Worker PID {process.pid}: {null_count} entries with NULL SortOrder for FileID {file_id}")
            result.close()
            await conn.commit()

            result = await conn.execute(
                text("""
                    DELETE FROM utb_ImageScraperResult
                    WHERE EntryID IN (
                        SELECT r.EntryID
                        FROM utb_ImageScraperRecords r
                        WHERE r.FileID = :file_id
                        AND utb_ImageScraperResult.ImageUrl = 'placeholder://no-results'
                    )
                """),
                {"file_id": file_id}
            )
            rows_deleted = result.rowcount
            logger.info(f"Deleted {rows_deleted} placeholder entries for FileID {file_id}")

            result = await conn.execute(
                text("""
                    UPDATE utb_ImageScraperResult
                    SET SortOrder = -2
                    WHERE EntryID IN (
                        SELECT r.EntryID
                        FROM utb_ImageScraperRecords r
                        WHERE r.FileID = :file_id
                    ) AND SortOrder IS NULL
                """),
                {"file_id": file_id}
            )
            rows_updated = result.rowcount
            logger.info(f"Updated {rows_updated} NULL SortOrder entries to -2 for FileID {file_id}")
            await conn.commit()
            
            return {"file_id": file_id, "rows_deleted": rows_deleted, "rows_updated": rows_updated}
    
    except SQLAlchemyError as e:
        logger.error(f"Database error updating entries for FileID {file_id}: {e}", exc_info=True)
        raise
    except ValueError as ve:
        logger.error(f"Invalid file_id format: {file_id}, error: {str(ve)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error updating entries for FileID {file_id}: {e}", exc_info=True)
        return None

def generate_search_variations(
    search_string: str,
    brand: Optional[str] = None,
    model: Optional[str] = None,
    brand_rules: Optional[Dict] = None,
    logger: Optional[logging.Logger] = None
) -> Dict[str, List[str]]:
    logger = logger or default_logger
    process = psutil.Process()
    variations = {
        "default": [],
        "delimiter_variations": [],
        "color_delimiter": [],
        "brand_alias": [],
        "no_color": []
    }
    
    if not search_string:
        logger.warning(f"Worker PID {process.pid}: Empty search string provided")
        return variations
    
    # Normalize all inputs to lowercase to suppress case sensitivity
    search_string = search_string.lower()
    brand = clean_string(brand).lower() if brand else None
    model = clean_string(model).lower() if model else search_string
    
    variations["default"].append(search_string)
    
    delimiters = [' ', '-', '_', '/']
    delimiter_variations = []
    for delim in delimiters:
        if delim in search_string:
            delimiter_variations.append(search_string.replace(delim, ' '))
            delimiter_variations.append(search_string.replace(delim, '-'))
            delimiter_variations.append(search_string.replace(delim, '_'))
    variations["delimiter_variations"] = list(set(delimiter_variations))
    
    variations["color_delimiter"].append(search_string)
    
    if brand:
        brand_aliases = generate_aliases(brand)
        variations["brand_alias"] = [f"{alias} {search_string}" for alias in brand_aliases]
    
    no_color_string = search_string
    if brand and brand_rules and "brand_rules" in brand_rules:
        for rule in brand_rules["brand_rules"]:
            if any(brand in name.lower() for name in rule.get("names", [])):
                sku_format = rule.get("sku_format", {})
                color_separator = sku_format.get("color_separator", "_")
                expected_length = rule.get("expected_length", {})
                base_length = expected_length.get("base", [6])[0]
                with_color_length = expected_length.get("with_color", [10])[0]
                
                if not color_separator:
                    logger.warning(f"Worker PID {process.pid}: Empty color_separator for brand {brand}, skipping color split")
                    no_color_string = search_string
                    logger.debug(f"Worker PID {process.pid}: Brand rule applied for {brand}: No color split, no_color='{no_color_string}'")
                    break
                
                if color_separator in search_string:
                    logger.debug(f"Worker PID {process.pid}: Applying color_separator '{color_separator}' to search_string '{search_string}'")
                    parts = search_string.split(color_separator)
                    base_part = parts[0]
                    if len(base_part) == base_length and len(search_string) <= with_color_length:
                        no_color_string = base_part
                        logger.debug(f"Worker PID {process.pid}: Brand rule applied for {brand}: Extracted no_color='{no_color_string}' from '{search_string}'")
                        break
                elif len(search_string) == base_length:
                    no_color_string = search_string
                    logger.debug(f"Worker PID {process.pid}: Brand rule applied for {brand}: No color suffix, no_color='{no_color_string}'")
                    break
    
    if no_color_string == search_string:
        for delim in ['_', '-', ' ']:
            if delim in search_string:
                no_color_string = search_string.rsplit(delim, 1)[0]
                logger.debug(f"Worker PID {process.pid}: Delimiter fallback: Extracted no_color='{no_color_string}' from '{search_string}' using delimiter '{delim}'")
                break
    
    variations["no_color"].append(no_color_string if no_color_string else search_string)
    if no_color_string != search_string:
        logger.info(f"Worker PID {process.pid}: Generated no_color variation: '{no_color_string}' from original '{search_string}'")
    else:
        logger.debug(f"Worker PID {process.pid}: No color suffix detected, no_color variation same as original: '{search_string}'")
    
    # Ensure all variations are lowercase and unique
    for key in variations:
        variations[key] = list(set(v.lower() for v in variations[key]))
    
    logger.debug(f"Worker PID {process.pid}: Generated variations: {variations}")
    return variations