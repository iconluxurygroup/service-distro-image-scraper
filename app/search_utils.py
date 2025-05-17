import logging
from typing import Optional, List, Dict
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from database_config import async_engine
from common import clean_string, normalize_model, generate_aliases
import psutil
import pyodbc

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

def validate_thumbnail_url(url: Optional[str], logger: Optional[logging.Logger] = None) -> bool:
    logger = logger or default_logger
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
    logger = logger or default_logger
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
        for res in results:
            try:
                entry_id = int(res["EntryID"])
            except (ValueError, TypeError):
                logger.error(f"Worker PID {process.pid}: Invalid EntryID value: {res.get('EntryID')}")
                return False

            category = res.get("ProductCategory", "")
            image_url = str(res.get("ImageUrl", "")) if res.get("ImageUrl") else ""
            if category.lower() == "footwear" and any(keyword in image_url.lower() for keyword in ["appliance", "whirlpool", "parts"]):
                logger.debug(f"Filtered out irrelevant URL: {image_url}")
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
    retry=retry_if_exception_type((pyodbc.Error, ValueError)),
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
    logger = logger or default_logger
    process = psutil.Process()
    
    try:
        # Use a separate connection for SELECT
        async with async_engine.connect() as conn:
            query = text("""
                SELECT r.ResultID, r.ImageUrl, r.ImageDesc, r.ImageSource, r.ImageUrlThumbnail
                FROM utb_ImageScraperResult r
                INNER JOIN utb_ImageScraperRecords rec ON r.EntryID = rec.EntryID
                WHERE r.EntryID = :entry_id AND rec.FileID = :file_id
            """)
            result = await conn.execute(query, {"entry_id": entry_id, "file_id": file_id})
            rows = result.fetchall()
            columns = result.keys()
            result.close()
            await conn.commit()

        if not rows:
            logger.warning(f"Worker PID {process.pid}: No results found for FileID {file_id}, EntryID {entry_id}")
            return False

        results = [dict(zip(columns, row)) for row in rows]
        logger.debug(f"Worker PID {process.pid}: Fetched {len(results)} rows for EntryID {entry_id}")

        brand_clean = clean_string(brand).lower() if brand else ""
        model_clean = normalize_model(model) if model else ""
        logger.debug(f"Worker PID {process.pid}: Cleaned brand: {brand_clean}, Cleaned model: {model_clean}")

        brand_aliases = []
        if brand and brand_rules and "brand_rules" in brand_rules:
            for rule in brand_rules["brand_rules"]:
                if any(brand.lower() in name.lower() for name in rule.get("names", [])):
                    brand_aliases = rule.get("names", [])
                    break
        if not brand_aliases and brand_clean:
            brand_aliases = [brand_clean, brand_clean.replace(" & ", " and "), brand_clean.replace(" ", "")]
        brand_aliases = [clean_string(alias).lower() for alias in brand_aliases]
        model_aliases = generate_aliases(model_clean) if model_clean else []
        if model_clean and not model_aliases:
            model_aliases = [model_clean, model_clean.replace("-", ""), model_clean.replace(" ", "")]
        logger.debug(f"Worker PID {process.pid}: Brand aliases: {brand_aliases}, Model aliases: {model_aliases}")

        for res in results:
            image_desc = clean_string(res.get("ImageDesc", ""), preserve_url=False).lower()
            image_source = clean_string(res.get("ImageSource", ""), preserve_url=True).lower()
            image_url = clean_string(res.get("ImageUrl", ""), preserve_url=True).lower()
            logger.debug(f"Worker PID {process.pid}: ImageDesc: {image_desc[:100]}, ImageSource: {image_source[:100]}, ImageUrl: {image_url[:100]}")

            model_matched = any(alias in image_desc or alias in image_source or alias in image_url for alias in model_aliases)
            brand_matched = any(alias in image_desc or alias in image_source or alias in image_url for alias in brand_aliases)
            logger.debug(f"Worker PID {process.pid}: Model matched: {model_matched}, Brand matched: {brand_matched}")

            if model_matched and brand_matched:
                res["priority"] = 1
            elif model_matched:
                res["priority"] = 2
            elif brand_matched:
                res["priority"] = 3
            else:
                res["priority"] = 4
            logger.debug(f"Worker PID {process.pid}: Assigned priority {res['priority']} to ResultID {res['ResultID']}")

        sorted_results = sorted(results, key=lambda x: x["priority"])
        logger.debug(f"Worker PID {process.pid}: Sorted {len(sorted_results)} results for EntryID {entry_id}")

        # Use a new connection for UPDATE operations
        async with async_engine.begin() as conn:
            for index, res in enumerate(sorted_results, 1):
                try:
                    # Assign SortOrder = 1 to only the top result, others get 2, 3, etc.
                    sort_order = 1 if index == 1 else index
                    await conn.execute(
                        text("""
                            UPDATE utb_ImageScraperResult
                            SET SortOrder = :sort_order
                            WHERE ResultID = :result_id AND EntryID = :entry_id
                        """),
                        {"sort_order": sort_order, "result_id": res["ResultID"], "entry_id": entry_id}
                    )
                    logger.debug(f"Worker PID {process.pid}: Updated SortOrder to {sort_order} for ResultID {res['ResultID']}")
                except SQLAlchemyError as e:
                    logger.error(f"Worker PID {process.pid}: Failed to update SortOrder for ResultID {res['ResultID']}, EntryID {entry_id}: {e}")
                    return False
            logger.info(f"Worker PID {process.pid}: Updated SortOrder for {len(sorted_results)} rows for EntryID {entry_id}")

        return True

    except SQLAlchemyError as e:
        logger.error(f"Worker PID {process.pid}: Database error in update_search_sort_order for EntryID {entry_id}: {e}", exc_info=True)
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
    logger = logger or default_logger
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
        
        if not entries:
            logger.warning(f"No entries found for FileID: {file_id}")
            return []
            
        results = []
        success_count = 0
        failure_count = 0
        
        for entry in entries:
            entry_id, brand, model, color, category = entry
            logger.debug(f"Worker PID {psutil.Process().pid}: Processing EntryID {entry_id}, Brand: {brand}, Model: {model}")
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
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        logger.info(f"Starting per-entry SortOrder update for FileID: {file_id}")
        
        async with async_engine.begin() as conn:
            result = await conn.execute(
                text("""
                    SELECT COUNT(*) 
                    FROM utb_ImageScraperResult 
                    WHERE EntryID IN (
                        SELECT EntryID 
                        FROM utb_ImageScraperRecords 
                        WHERE FileID = :file_id
                    ) AND SortOrder IS NULL
                """),
                {"file_id": file_id}
            )
            null_count = result.scalar()
            logger.debug(f"Worker PID {psutil.Process().pid}: {null_count} entries with NULL SortOrder for FileID {file_id}")

            result = await conn.execute(
                text("""
                    DELETE FROM utb_ImageScraperResult
                    WHERE EntryID IN (
                        SELECT r.EntryID
                        FROM utb_ImageScraperRecords r
                        WHERE r.FileID = :file_id
                    ) AND ImageUrl = 'placeholder://no-results'
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
    
async def process_and_tag_results(
    search_string: str,
    brand: Optional[str] = None,
    model: Optional[str] = None,
    endpoint: str = None,
    entry_id: int = None,
    logger: Optional[logging.Logger] = None,
    use_all_variations: bool = False,
    file_id_db: Optional[int] = None
) -> List[Dict]:
    """
    Process search results for a given search string and tag them with metadata.
    
    Args:
        search_string: The search query string.
        brand: The brand name, if available.
        model: The model name, if available.
        endpoint: The search endpoint URL.
        entry_id: The EntryID from utb_ImageScraperRecords.
        logger: Logger instance for logging.
        use_all_variations: Whether to use all search variations or stop after first successful type.
        file_id_db: The FileID from utb_ImageScraperFiles.
    
    Returns:
        List of dictionaries containing tagged search results with required fields.
    """
    logger = logger or default_logger
    process = psutil.Process()
    
    try:
        logger.debug(f"Worker PID {process.pid}: Processing results for EntryID {entry_id}, Search: {search_string}")
        
        # Generate search variations
        variations = generate_search_variations(
            search_string=search_string,
            brand=brand,
            model=model,
            logger=logger
        )
        
        all_results = []
        search_types = [
            "default", "delimiter_variations", "color_delimiter",
            "brand_alias", "no_color"
        ]
        
        required_columns = ["EntryID", "ImageUrl", "ImageDesc", "ImageSource", "ImageUrlThumbnail"]
        
        for search_type in search_types:
            if search_type not in variations:
                logger.warning(f"Worker PID {process.pid}: Search type '{search_type}' not found for EntryID {entry_id}")
                continue
            
            logger.info(f"Worker PID {process.pid}: Processing search type '{search_type}' for EntryID {entry_id}")
            for variation in variations[search_type]:
                logger.debug(f"Worker PID {process.pid}: Searching variation '{variation}' for EntryID {entry_id}")
                search_result = await search_variation(
                    variation=variation,
                    endpoint=endpoint,
                    entry_id=entry_id,
                    search_type=search_type,
                    brand=brand,
                    logger=logger
                )
                
                if search_result["status"] == "success" and search_result["result"]:
                    logger.info(f"Worker PID {process.pid}: Found {search_result['result_count']} results for variation '{variation}'")
                    tagged_results = []
                    for res in search_result["result"]:
                        tagged_result = {
                            "EntryID": entry_id,
                            "ImageUrl": res.get("ImageUrl", "placeholder://no-image"),
                            "ImageDesc": res.get("ImageDesc", ""),
                            "ImageSource": res.get("ImageSource", "N/A"),
                            "ImageUrlThumbnail": res.get("ImageUrlThumbnail", res.get("ImageUrl", "placeholder://no-thumbnail")),
                            "ProductCategory": res.get("ProductCategory", "")
                        }
                        if all(col in tagged_result for col in required_columns):
                            tagged_results.append(tagged_result)
                        else:
                            logger.warning(f"Worker PID {process.pid}: Skipping result with missing columns for EntryID {entry_id}")
                    
                    all_results.extend(tagged_results)
                    logger.info(f"Worker PID {process.pid}: Added {len(tagged_results)} valid results for variation '{variation}'")
                
                else:
                    logger.warning(f"Worker PID {process.pid}: No valid results for variation '{variation}' in search type '{search_type}'")
            
            if all_results and not use_all_variations:
                logger.info(f"Worker PID {process.pid}: Stopping after {len(all_results)} results from '{search_type}' for EntryID {entry_id}")
                break
        
        if not all_results:
            logger.error(f"Worker PID {process.pid}: No valid results found across all search types for EntryID {entry_id}")
            return [{
                "EntryID": entry_id,
                "ImageUrl": "placeholder://no-results",
                "ImageDesc": f"No results found for {search_string}",
                "ImageSource": "N/A",
                "ImageUrlThumbnail": "placeholder://no-results",
                "ProductCategory": ""
            }]
        
        # Deduplicate results based on ImageUrl
        deduplicated_results = []
        seen_urls = set()
        for res in all_results:
            image_url = res["ImageUrl"]
            if image_url not in seen_urls and image_url != "placeholder://no-results":
                seen_urls.add(image_url)
                deduplicated_results.append(res)
        
        logger.info(f"Worker PID {process.pid}: Deduplicated to {len(deduplicated_results)} results for EntryID {entry_id}")
        
        # Filter out irrelevant results
        irrelevant_keywords = ['wallpaper', 'sofa', 'furniture', 'decor', 'stock photo', 'card', 'pokemon']
        filtered_results = [
            res for res in deduplicated_results
            if not any(kw.lower() in res.get("ImageDesc", "").lower() for kw in irrelevant_keywords)
        ]
        
        logger.info(f"Worker PID {process.pid}: Filtered to {len(filtered_results)} results after removing irrelevant items for EntryID {entry_id}")
        
        return filtered_results
    
    except Exception as e:
        logger.error(f"Worker PID {process.pid}: Error processing results for EntryID {entry_id}: {e}", exc_info=True)
        return [{
            "EntryID": entry_id,
            "ImageUrl": "placeholder://error",
            "ImageDesc": f"Error processing: {str(e)}",
            "ImageSource": "N/A",
            "ImageUrlThumbnail": "placeholder://error",
            "ProductCategory": ""
        }]