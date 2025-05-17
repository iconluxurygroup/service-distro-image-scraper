import logging
import pandas as pd
import re
import asyncio
import json
from typing import Optional, List, Dict
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from database_config import conn_str, async_engine
from common import clean_string, validate_model, validate_brand, calculate_priority, generate_aliases, generate_brand_aliases
import psutil

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
async def insert_search_results(df: pd.DataFrame, logger: Optional[logging.Logger] = None, file_id: Optional[str] = None) -> bool:
    logger = logger or logging.getLogger(__name__)
    try:
        if df.empty:
            logger.warning("Empty DataFrame provided for insertion")
            return False
        required_columns = ["EntryID", "ImageUrl", "ImageDesc", "ImageSource", "ImageUrlThumbnail"]
        if not all(col in df.columns for col in required_columns):
            missing_cols = set(required_columns) - set(df.columns)
            logger.error(f"Missing required columns: {missing_cols}")
            return False
        original_len = len(df)
        # Fallback to ImageUrl if ImageUrlThumbnail is invalid
        df['ImageUrlThumbnail'] = df.apply(
            lambda row: row['ImageUrl'] if not validate_thumbnail_url(row['ImageUrlThumbnail'], logger) else row['ImageUrlThumbnail'], axis=1
        )
        df = df[df['ImageUrlThumbnail'].apply(lambda x: validate_thumbnail_url(x, logger)) & 
                ~df['ImageUrl'].str.contains('placeholder://', na=False)]
        logger.debug(f"Filtered to {len(df)} rows with valid thumbnail URLs from {original_len} rows")
        if df.empty:
            logger.warning(f"No rows with valid thumbnail URLs after filtering {original_len} rows")
            return False
        async with async_engine.connect() as conn:
            for _, row in df.iterrows():
                await conn.execute(
                    text("""
                        INSERT INTO utb_ImageScraperResult (EntryID, ImageUrl, ImageDesc, ImageSource, ImageUrlThumbnail)
                        VALUES (:entry_id, :image_url, :image_desc, :image_source, :image_url_thumbnail)
                    """),
                    {
                        "entry_id": row['EntryID'],
                        "image_url": row['ImageUrl'],
                        "image_desc": row['ImageDesc'],
                        "image_source": row['ImageSource'],
                        "image_url_thumbnail": row['ImageUrlThumbnail']
                    }
                )
            await conn.commit()
        logger.info(f"Inserted {len(df)} rows into utb_ImageScraperResult for FileID {file_id}")
        return True
    except SQLAlchemyError as e:
        logger.error(f"Database error during insertion for FileID {file_id or 'unknown'}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error during insertion for FileID {file_id or 'unknown'}: {e}", exc_info=True)
        return False

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type(SQLAlchemyError),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying update_search_sort_order for EntryID {retry_state.kwargs['entry_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def update_search_sort_order(
    file_id: str,
    entry_id: str,
    brand: str,
    model: str,
    color: str,
    category: str,
    logger: logging.Logger,
    brand_rules: Optional[Dict] = None
) -> Optional[bool]:
    process = psutil.Process()
    try:
        logger.debug(f"Worker PID {process.pid}: Updating SortOrder for FileID: {file_id}, EntryID: {entry_id}")
        async with async_engine.connect() as conn:
            query = text("""
                SELECT t.ResultID, t.EntryID, t.ImageDesc, t.ImageSource, t.ImageUrl,
                       r.ProductBrand, r.ProductModel, t.AiJson
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                WHERE t.EntryID = :entry_id AND r.FileID = :file_id
            """)
            result = await conn.execute(query, {"entry_id": entry_id, "file_id": file_id})
            rows = result.fetchall()
            columns = result.keys()
            result.close()
        
        logger.debug(f"Worker PID {process.pid}: Fetched {len(rows)} rows with columns: {columns}")
        if not rows:
            logger.warning(f"Worker PID {process.pid}: No results found for FileID {file_id}, EntryID {entry_id}")
            async with async_engine.connect() as conn:
                await conn.execute(
                    text("UPDATE utb_ImageScraperResult SET SortOrder = -2 WHERE EntryID = :entry_id"),
                    {"entry_id": entry_id}
                )
                await conn.commit()
            return False
        
        try:
            df = pd.DataFrame(rows, columns=columns)
            df['match_score'] = df['AiJson'].apply(
                lambda x: float(json.loads(x)['match_score']) if x and json.loads(x).get('match_score') else 0.0
            )
        except (ValueError, json.JSONDecodeError) as e:
            logger.error(f"Worker PID {process.pid}: DataFrame creation or JSON parsing failed for EntryID {entry_id}: {e}")
            async with async_engine.connect() as conn:
                await conn.execute(
                    text("UPDATE utb_ImageScraperResult SET SortOrder = -2 WHERE EntryID = :entry_id"),
                    {"entry_id": entry_id}
                )
                await conn.commit()
            return False
        
        async with async_engine.connect() as conn:
            for _, row in df.iterrows():
                match_score = row['match_score']
                ai_json = row['AiJson']
                if pd.isna(match_score) or not isinstance(match_score, (int, float)):
                    logger.warning(f"Invalid match_score for ResultID {row['ResultID']}: {match_score}")
                    sort_order = -2
                elif ai_json:
                    try:
                        json.loads(ai_json)
                        sort_order = 1.0 if match_score > 0 else 0.0
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid AiJson for ResultID {row['ResultID']}: {ai_json[:100]}...")
                        sort_order = -2
                else:
                    logger.warning(f"Missing AiJson for ResultID {row['ResultID']}")
                    sort_order = -2
                await conn.execute(
                    text("UPDATE utb_ImageScraperResult SET SortOrder = :sort_order WHERE ResultID = :result_id"),
                    {"sort_order": sort_order, "result_id": row['ResultID']}
                )
            await conn.commit()
        
        logger.info(f"Worker PID {process.pid}: Updated SortOrder for {len(df)} results in EntryID {entry_id}")
        return True
    except SQLAlchemyError as e:
        logger.error(f"Worker PID {process.pid}: Database error for FileID {file_id}, EntryID {entry_id}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Worker PID {process.pid}: Unexpected error for FileID {file_id}, EntryID {entry_id}: {e}", exc_info=True)
        async with async_engine.connect() as conn:
            await conn.execute(
                text("UPDATE utb_ImageScraperResult SET SortOrder = -2 WHERE EntryID = :entry_id"),
                {"entry_id": entry_id}
            )
            await conn.commit()
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
        
        if not entries:
            logger.warning(f"No entries found for FileID: {file_id}")
            return []
            
        results = []
        success_count = 0
        failure_count = 0
        
        for entry in entries:
            entry_id, brand, model, color, category = entry
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
            logger.info(f"Deleted {rows_deleted} placeholder entries for FileID: {file_id}")

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
            logger.info(f"Updated {rows_updated} NULL SortOrder entries to -2 for FileID: {file_id}")
            
            return {"file_id": file_id, "rows_deleted": rows_deleted, "rows_updated": rows_updated}
    
    except SQLAlchemyError as e:
        logger.error(f"Database error updating entries for FileID: {file_id}: {e}", exc_info=True)
        raise
    except ValueError as ve:
        logger.error(f"Invalid file_id format: {file_id}, error: {str(ve)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error updating entries for FileID: {file_id}: {e}", exc_info=True)
        return None