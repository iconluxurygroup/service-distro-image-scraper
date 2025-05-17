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
import pyodbc
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
                SELECT t.ResultID, t.EntryID, t.match_score, t.ImageDesc, t.ImageSource, t.ImageUrl,
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
        except ValueError as e:
            logger.error(f"Worker PID {process.pid}: DataFrame creation failed for EntryID {entry_id}: {e}")
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
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(pyodbc.Error),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying sync_update_search_sort_order for EntryID {retry_state.kwargs['entry_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
def sync_update_search_sort_order(
    file_id: str,
    entry_id: str,
    brand: Optional[str] = None,
    model: Optional[str] = None,
    color: Optional[str] = None,
    category: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
    brand_rules: Optional[Dict] = None,
    brand_aliases: Optional[List[str]] = None
) -> Optional[List[Dict]]:
    logger = logger or logging.getLogger(__name__)
    try:
        file_id = int(file_id)
        entry_id = int(entry_id)
        logger.debug(f"Updating SortOrder for FileID: {file_id}, EntryID: {entry_id}")

        with pyodbc.connect(conn_str, autocommit=False) as conn:
            cursor = conn.cursor()
            if not all([brand, model]):
                cursor.execute(
                    """
                    SELECT ProductBrand, ProductModel, ProductColor, ProductCategory
                    FROM utb_ImageScraperRecords
                    WHERE FileID = ? AND EntryID = ?
                    """,
                    (file_id, entry_id)
                )
                row = cursor.fetchone()
                if row:
                    brand, model, color, category = row
                    logger.debug(f"Fetched attributes - Brand: {brand}, Model: {model}")
                else:
                    logger.warning(f"No attributes found for FileID: {file_id}, EntryID: {entry_id}")
                    brand, model, color, category = brand or '', model or '', color or '', category or ''

            cursor.execute(
                """
                SELECT 
                    t.ResultID, 
                    t.EntryID,
                    CASE WHEN ISJSON(t.AiJson) = 1 
                         THEN ISNULL(TRY_CAST(JSON_VALUE(t.AiJson, '$.match_score') AS FLOAT), 0)
                         ELSE 0 END AS match_score,
                    t.ImageDesc, 
                    t.ImageSource, 
                    t.ImageUrl,
                    r.ProductBrand, 
                    r.ProductModel,
                    t.AiJson
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                WHERE r.FileID = ? AND t.EntryID = ?
                """,
                (file_id, entry_id)
            )
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            logger.debug(f"Fetched {len(rows)} rows with columns: {columns}")
            if not rows:
                logger.warning(f"No data found for FileID: {file_id}, EntryID: {entry_id}")
                cursor.execute(
                    "UPDATE utb_ImageScraperResult SET SortOrder = -2 WHERE EntryID = ?",
                    (entry_id,)
                )
                conn.commit()
                return []

            df = pd.DataFrame(rows, columns=columns)
            if df.empty:
                logger.warning(f"Empty DataFrame for FileID: {file_id}, EntryID: {entry_id}")
                cursor.execute(
                    "UPDATE utb_ImageScraperResult SET SortOrder = -2 WHERE EntryID = ?",
                    (entry_id,)
                )
                conn.commit()
                return []

            invalid_json_rows = df[df['AiJson'].notnull() & (df['AiJson'].str.strip() != '') & (df['match_score'] == 0)]
            for _, row in invalid_json_rows.iterrows():
                logger.warning(f"Invalid JSON in ResultID {row['ResultID']}: AiJson={row['AiJson'][:100]}...")
                df.loc[df['ResultID'] == row['ResultID'], 'match_score'] = 0

            if brand_aliases is None:
                brand_aliases_dict = {
                    "Scotch & Soda": ["Scotch and Soda", "Scotch Soda", "Scotch&Soda", "ScotchAndSoda", "Scotch"],
                    "Adidas": ["Adidas AG", "Adidas Originals", "Addidas", "Adiddas"],
                    "BAPE": ["A Bathing Ape", "BATHING APE", "Bape Japan", "ABathingApe", "Bape"]
                }
                brand_clean = clean_string(brand).lower() if brand else ''
                brand_aliases = brand_aliases_dict.get(brand_clean, [brand_clean] if brand_clean else [])
                logger.debug(f"Brand aliases: {brand_aliases}")

            model_clean = clean_string(model) if model else ''
            model_aliases = generate_aliases(model_clean) if model_clean else []
            logger.debug(f"Model aliases: {model_aliases}")

            required_cols = ["ImageDesc", "ImageSource", "ImageUrl", "ProductBrand", "ProductModel"]
            for col in required_cols:
                if col not in df.columns:
                    logger.error(f"Missing column {col}")
                    cursor.execute(
                        "UPDATE utb_ImageScraperResult SET SortOrder = -2 WHERE EntryID = ?",
                        (entry_id,)
                    )
                    conn.commit()
                    return []
                df[f"{col}_clean"] = df[col].fillna('').apply(clean_string)

            df['is_model_match'] = df.apply(lambda row: validate_model(row, model_aliases, row['ResultID'], logger), axis=1)
            df['is_brand_match'] = df.apply(lambda row: validate_brand(row, brand_aliases, row['ResultID'], None, logger), axis=1)
            df['priority'] = df.apply(
                lambda row: calculate_priority(
                    row, df[df['is_model_match']], model_clean, model_aliases, brand_clean, brand_aliases, logger
                ),
                axis=1
            )

            df['new_sort_order'] = -2
            match_df = df[df['priority'].isin([1, 2, 3])].sort_values(['priority', 'match_score'], ascending=[True, False])
            if not match_df.empty:
                valid_indices = match_df.index[match_df.apply(
                    lambda row: row['is_model_match'] if row['priority'] in [1, 2] else row['is_brand_match'], axis=1
                )]
                if not valid_indices.empty:
                    df.loc[valid_indices, 'new_sort_order'] = range(1, len(valid_indices) + 1)

            cursor.execute(
                "UPDATE utb_ImageScraperResult SET SortOrder = NULL WHERE EntryID = ?",
                (entry_id,)
            )
            updates = df[['ResultID', 'new_sort_order']].to_dict('records')
            if updates:
                values_clause = ", ".join(
                    f"({row['ResultID']}, {row['new_sort_order']})"
                    for row in updates
                )
                cursor.execute(
                    f"""
                    UPDATE utb_ImageScraperResult
                    SET SortOrder = v.sort_order
                    FROM (VALUES {values_clause}) AS v(result_id, sort_order)
                    WHERE utb_ImageScraperResult.ResultID = v.result_id
                    """
                )
                logger.info(f"Updated {len(updates)} rows for EntryID {entry_id}")
            else:
                logger.warning(f"No valid matches for EntryID {entry_id}, setting SortOrder to -2")
                cursor.execute(
                    "UPDATE utb_ImageScraperResult SET SortOrder = -2 WHERE EntryID = ?",
                    (entry_id,)
                )

            cursor.execute(
                """
                SELECT ResultID, EntryID, SortOrder, ImageDesc, ImageSource, ImageUrl
                FROM utb_ImageScraperResult
                WHERE EntryID = ?
                ORDER BY SortOrder DESC
                """,
                (entry_id,)
            )
            results = [
                {
                    "ResultID": r[0],
                    "EntryID": r[1],
                    "SortOrder": r[2],
                    "ImageDesc": r[3],
                    "ImageSource": r[4],
                    "ImageUrl": r[5]
                }
                for r in cursor.fetchall()
            ]

            null_count = sum(1 for r in results if r['SortOrder'] is None)
            if null_count > 0:
                logger.warning(f"Found {null_count} rows with NULL SortOrder for EntryID {entry_id}")
                cursor.execute(
                    "UPDATE utb_ImageScraperResult SET SortOrder = -2 WHERE EntryID = ? AND SortOrder IS NULL",
                    (entry_id,)
                )
                logger.info(f"Set {null_count} NULL SortOrder rows to -2 for EntryID {entry_id}")

            conn.commit()
            return results

    except pyodbc.Error as e:
        logger.error(f"Database error for FileID {file_id}, EntryID {entry_id}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error for FileID {file_id}, EntryID {entry_id}: {e}", exc_info=True)
        cursor.execute(
            "UPDATE utb_ImageScraperResult SET SortOrder = -2 WHERE EntryID = ?",
            (entry_id,)
        )
        conn.commit()
        return None

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
            # Delete placeholder entries
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

            # Set NULL SortOrder to -2
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
    
    except ValueError as ve:
        logger.error(f"Invalid file_id format: {file_id}, error: {str(ve)}")
        return None
    except Exception as e:
        logger.error(f"Error updating entries for FileID: {file_id}, error: {str(e)}")
        return None

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type(SQLAlchemyError),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying update_sort_order_per_entry for FileID {retry_state.kwargs['file_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def update_sort_order_per_entry(file_id: str, logger: Optional[logging.Logger] = None) -> Optional[Dict]:
    logger = logger or logging.getLogger(__name__)
    try:
        file_id = int(file_id)
        logger.info(f"Starting per-entry SortOrder update for FileID: {file_id}")

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
                return {
                    "FileID": file_id,
                    "TotalEntries": 0,
                    "SuccessfulEntries": 0,
                    "FailedEntries": 0,
                    "BrandMismatchEntries": 0,
                    "ModelMismatchEntries": 0,
                    "UnexpectedSortOrderEntries": 0,
                    "EntryResults": [],
                    "Verification": {
                        "PositiveSortOrderEntries": 0,
                        "BrandMatchEntries": 0,
                        "NoMatchEntries": 0,
                        "NullSortOrderEntries": 0,
                        "UnexpectedSortOrderEntries": 0
                    }
                }

            entry_results = []
            success_count = 0
            failure_count = 0
            brand_mismatch_count = 0
            model_mismatch_count = 0
            unexpected_sort_order_count = 0

            brand_aliases_dict = {
                "Scotch & Soda": ["Scotch and Soda", "Scotch Soda", "Scotch&Soda", "ScotchAndSoda", "Scotch"],
                "Adidas": ["Adidas AG", "Adidas Originals", "Addidas", "Adiddas"],
                "BAPE": ["A Bathing Ape", "BATHING APE", "Bape Japan", "ABathingApe", "Bape"]
            }

            for entry in entries:
                entry_id, brand, model, color, category = entry
                entry_result = {
                    "EntryID": entry_id,
                    "ProductModel": model,
                    "ProductBrand": brand,
                    "Status": "Success",
                    "Details": {},
                    "Error": None,
                    "BrandMatches": [],
                    "ModelMatches": []
                }

                try:
                    brand_aliases = await generate_brand_aliases(brand or '', brand_aliases_dict)
                    logger.debug(f"Brand aliases for EntryID {entry_id}, Brand '{brand}': {brand_aliases}")

                    model_aliases = generate_aliases(model) if model else []
                    if model and '_' in model:
                        model_base = model.split('_')[0]
                        model_aliases.append(model_base)
                    model_aliases = list(set(model_aliases))
                    logger.debug(f"Model aliases for EntryID {entry_id}, Model '{model}': {model_aliases}")

                    updates = sync_update_search_sort_order(
                        file_id=str(file_id),
                        entry_id=str(entry_id),
                        brand=brand,
                        model=model,
                        color=color,
                        category=category,
                        logger=logger,
                        brand_rules=None,
                        brand_aliases=brand_aliases
                    )

                    if updates is not None:
                        positive_count = sum(1 for r in updates if r['SortOrder'] is not None and r['SortOrder'] > 0)
                        zero_count = sum(1 for r in updates if r['SortOrder'] == 0)
                        negative_count = sum(1 for r in updates if r['SortOrder'] is not None and r['SortOrder'] < 0)
                        null_count = sum(1 for r in updates if r['SortOrder'] is None)
                        unexpected_count = sum(1 for r in updates if r['SortOrder'] == -1)

                        brand_matches = []
                        model_matches = []
                        for result in updates:
                            image_desc = clean_string(result.get('ImageDesc', ''), preserve_url=False)
                            image_source = clean_string(result.get('ImageSource', ''), preserve_url=True)
                            image_url = clean_string(result.get('ImageUrl', ''), preserve_url=True)

                            matched_brand_aliases = [
                                alias for alias in brand_aliases
                                if re.search(rf'\b{re.escape(alias.lower())}\b', 
                                            (image_desc + ' ' + image_source + ' ' + image_url).lower())
                            ]
                            if matched_brand_aliases:
                                brand_matches.append({
                                    "ResultID": result['ResultID'],
                                    "SortOrder": result['SortOrder'],
                                    "MatchedAliases": matched_brand_aliases,
                                    "ImageUrl": image_url,
                                    "ImageDesc": image_desc,
                                    "ImageSource": image_source
                                })

                            matched_model_aliases = [
                                alias for alias in model_aliases
                                if re.search(rf'\b{re.escape(alias.lower())}\b', 
                                            (image_desc + ' ' + image_source + ' ' + image_url).lower())
                            ]
                            if matched_model_aliases:
                                model_matches.append({
                                    "ResultID": result['ResultID'],
                                    "SortOrder": result['SortOrder'],
                                    "MatchedAliases": matched_model_aliases,
                                    "ImageUrl": image_url,
                                    "ImageDesc": image_desc,
                                    "ImageSource": image_source
                                })

                            if result['SortOrder'] == -1:
                                logger.warning(
                                    f"Unexpected SortOrder=-1 for ResultID {result['ResultID']}, "
                                    f"EntryID {entry_id}, Model '{model}', Brand '{brand}'"
                                )
                                unexpected_count += 1

                        if unexpected_count > 0:
                            brand_pattern = '|'.join(re.escape(alias.lower()) for alias in brand_aliases)
                            async with async_engine.connect() as conn:
                                await conn.execute(
                                    text("""
                                        UPDATE utb_ImageScraperResult
                                        SET SortOrder = CASE
                                            WHEN (
                                                LOWER(ImageDesc) REGEXP :brand_pattern OR
                                                LOWER(ImageSource) REGEXP :brand_pattern OR
                                                LOWER(ImageUrl) REGEXP :brand_pattern
                                            ) THEN (
                                                SELECT COALESCE(MAX(SortOrder), 0) + 1 
                                                FROM utb_ImageScraperResult 
                                                WHERE EntryID = :entry_id AND SortOrder > 0
                                            )
                                            ELSE -2
                                        END
                                        WHERE EntryID = :entry_id AND SortOrder = -1
                                    """),
                                    {"brand_pattern": brand_pattern, "entry_id": entry_id}
                                )
                                await conn.commit()
                                corrected_count = unexpected_count
                                unexpected_sort_order_count += unexpected_count - corrected_count
                                logger.info(f"Corrected {corrected_count} SortOrder=-1 entries for EntryID {entry_id}")

                        entry_result["Details"] = {
                            "UpdatedRows": len(updates),
                            "PositiveSortOrder": positive_count,
                            "ZeroSortOrder": zero_count,
                            "NegativeSortOrder": negative_count,
                            "NullSortOrder": null_count,
                            "UnexpectedSortOrder": unexpected_count
                        }
                        entry_result["BrandMatches"] = brand_matches
                        entry_result["ModelMatches"] = model_matches

                        if not brand_matches and brand:
                            logger.warning(f"No brand aliases matched for EntryID {entry_id}, Brand '{brand}'")
                            brand_mismatch_count += 1
                        if not model_matches and model:
                            logger.warning(f"No model aliases matched for EntryID {entry_id}, Model '{model}'")
                            model_mismatch_count += 1

                        success_count += 1
                    else:
                        failure_count += 1
                        entry_result["Status"] = "Failed"
                        entry_result["Error"] = "sync_update_search_sort_order returned None"
                        logger.warning(f"No results for EntryID {entry_id}")

                    entry_results.append(entry_result)
                except Exception as e:
                    failure_count += 1
                    entry_result["Status"] = "Failed"
                    entry_result["Error"] = str(e)
                    logger.error(f"Error processing EntryID {entry_id}: {e}", exc_info=True)
                    entry_results.append(entry_result)

            verification = {}
            queries = [
                ("PositiveSortOrderEntries", "t.SortOrder > 0"),
                ("BrandMatchEntries", "t.SortOrder = 0"),
                ("NoMatchEntries", "t.SortOrder < 0"),
                ("NullSortOrderEntries", "t.SortOrder IS NULL"),
                ("UnexpectedSortOrderEntries", "t.SortOrder = -1")
            ]
            async with async_engine.connect() as conn:
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

            logger.info(
                f"Verification for FileID {file_id}: "
                f"{verification['PositiveSortOrderEntries']} model/brand matches, "
                f"{verification['BrandMatchEntries']} legacy brand matches, "
                f"{verification['NoMatchEntries']} no matches, "
                f"{verification['NullSortOrderEntries']} NULL, "
                f"{verification['UnexpectedSortOrderEntries']} unexpected"
            )

            if brand_mismatch_count > 0:
                logger.warning(f"{brand_mismatch_count} entries had no brand alias matches")
            if model_mismatch_count > 0:
                logger.warning(f"{model_mismatch_count} entries had no model alias matches")
            if verification['UnexpectedSortOrderEntries'] > 0:
                logger.warning(f"{verification['UnexpectedSortOrderEntries']} entries retained SortOrder=-1")

            return {
                "FileID": file_id,
                "TotalEntries": len(entries),
                "SuccessfulEntries": success_count,
                "FailedEntries": failure_count,
                "BrandMismatchEntries": brand_mismatch_count,
                "ModelMismatchEntries": model_mismatch_count,
                "UnexpectedSortOrderEntries": verification['UnexpectedSortOrderEntries'],
                "EntryResults": entry_results,
                "Verification": verification
            }

    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")
        return None
    except Exception as e:
        logger.error(f"Error in per-entry SortOrder update for FileID {file_id}: {e}", exc_info=True)
        return None