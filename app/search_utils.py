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
async def insert_search_results(
    df: pd.DataFrame,
    logger: Optional[logging.Logger] = None,
    file_id: str = None
) -> bool:
    logger = logger or logging.getLogger(__name__)
    process = psutil.Process()

    if df.empty:
        logger.warning(f"Worker PID {process.pid}: Empty DataFrame provided for insert_search_results")
        return False

    required_columns = ["EntryID", "ImageUrl", "ImageDesc", "ImageSource", "ImageUrlThumbnail"]
    if not all(col in df.columns for col in required_columns):
        missing_cols = set(required_columns) - set(df.columns)
        logger.error(f"Worker PID {process.pid}: Missing required columns: {missing_cols}")
        return False

    try:
        async with async_engine.connect() as conn:
            # Ensure no pending results on the connection
            await conn.execute(text("SELECT 1"))  # Dummy query to clear connection state
            await conn.commit()  # Commit to ensure connection is clean

            parameters = [
                (
                    row["EntryID"],
                    row["ImageUrl"],
                    row["ImageDesc"],
                    row["ImageSource"],
                    row["ImageUrlThumbnail"]
                )
                for _, row in df.iterrows()
            ]
            logger.debug(f"Worker PID {process.pid}: Inserting {len(parameters)} rows into utb_ImageScraperResult")
            await conn.execute(
                text("""
                    INSERT INTO utb_ImageScraperResult (EntryID, ImageUrl, ImageDesc, ImageSource, ImageUrlThumbnail)
                    VALUES (?, ?, ?, ?, ?)
                """),
                parameters
            )
            await conn.commit()
            logger.info(f"Worker PID {process.pid}: Successfully inserted {len(parameters)} rows for FileID {file_id}")
            return True
    except Exception as e:
        logger.error(f"Worker PID {process.pid}: Failed to insert results for FileID {file_id}: {e}", exc_info=True)
        return False
import logging
import pandas as pd
import re
import aioodbc
import pyodbc
import asyncio
from typing import Optional, List, Dict, Any
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from common import clean_string, validate_model, validate_brand, calculate_priority, generate_aliases
from database_config import conn_str

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
    brand_rules: Optional[Dict] = None,
    brand_aliases: Optional[List[str]] = None
) -> Optional[List[Dict]]:
    logger = logger or logging.getLogger(__name__)
    try:
        file_id = int(file_id)
        entry_id = int(entry_id)
        logger.debug(f"Updating SortOrder for FileID: {file_id}, EntryID: {entry_id}")

        async with aioodbc.connect(dsn=conn_str) as conn:
            async with conn.cursor() as cursor:
                # Fetch attributes if not provided
                if not all([brand, model]):
                    await cursor.execute(
                        """
                        SELECT ProductBrand, ProductModel, ProductColor, ProductCategory
                        FROM utb_ImageScraperRecords
                        WHERE FileID = ? AND EntryID = ?
                        """,
                        (file_id, entry_id)
                    )
                    row = await cursor.fetchone()
                    if row:
                        brand, model, color, category = row
                        logger.debug(f"Fetched attributes - Brand: {brand}, Model: {model}")
                    else:
                        logger.warning(f"No attributes found for FileID: {file_id}, EntryID: {entry_id}")
                        brand, model, color, category = brand or '', model or '', color or '', category or ''

                # Fetch search results
                query = """
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
                    INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                    WHERE r.FileID = ? AND t.EntryID = ?
                """
                await cursor.execute(query, (file_id, entry_id))
                columns = [column[0] for column in cursor.description]
                rows = await cursor.fetchall()
                logger.debug(f"Raw rows (first 2): {rows[:2]}")
                logger.debug(f"Columns: {columns}")
                if not rows:
                    logger.warning(f"No data found for FileID {file_id}, EntryID {entry_id}")
                    return []

                # Validate all rows
                expected_columns = len(columns)
                malformed_rows = [(i, len(row)) for i, row in enumerate(rows) if len(row) != expected_columns]
                if malformed_rows:
                    logger.error(f"Malformed rows detected: {[(i, count) for i, count in malformed_rows[:10]]}")
                    # Fallback to pyodbc
                    logger.info("Falling back to pyodbc for query execution")
                    loop = asyncio.get_event_loop()
                    def sync_fetch():
                        with pyodbc.connect(conn_str) as conn:
                            cursor = conn.cursor()
                            cursor.execute(query, (file_id, entry_id))
                            return cursor.fetchall()
                    rows = await loop.run_in_executor(None, sync_fetch)
                    logger.debug(f"pyodbc rows (first 2): {rows[:2]}")
                    malformed_rows = [(i, len(row)) for i, row in enumerate(rows) if len(row) != expected_columns]
                    if malformed_rows:
                        logger.error(f"pyodbc also returned malformed rows: {[(i, count) for i, count in malformed_rows[:10]]}")
                        return []

                df = pd.DataFrame(rows, columns=columns)
                if df.empty:
                    logger.warning(f"No data found for FileID {file_id}, EntryID {entry_id}")
                    return []

                # Skip placeholder/error results
                df = df[~df['ImageUrl'].str.contains('placeholder://', na=False)]
                if df.empty:
                    logger.warning(f"All results for EntryID {entry_id} are placeholders, skipping sort order update")
                    return []

                # Handle invalid JSON
                invalid_json_rows = df[df['AiJson'].notnull() & (df['AiJson'].str.strip() != '') & (df['match_score'] == 0)]
                for _, row in invalid_json_rows.iterrows():
                    logger.warning(f"Invalid JSON in ResultID {row['ResultID']}: AiJson={row['AiJson'][:100]}...")
                    df.loc[df['ResultID'] == row['ResultID'], 'match_score'] = 0

                # Generate aliases
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

                # Clean columns
                required_cols = ["ImageDesc", "ImageSource", "ImageUrl", "ProductBrand", "ProductModel"]
                for col in required_cols:
                    if col not in df.columns:
                        logger.error(f"Missing column {col}")
                        return []
                    df[f"{col}_clean"] = df[col].fillna('').apply(clean_string)

                # Validate matches and calculate priority
                df['is_model_match'] = df.apply(lambda row: validate_model(row, model_aliases, row['ResultID'], logger), axis=1)
                df['is_brand_match'] = df.apply(lambda row: validate_brand(row, brand_aliases, row['ResultID'], None, logger), axis=1)
                df['priority'] = df.apply(
                    lambda row: calculate_priority(
                        row, df[df['is_model_match']], model_clean, model_aliases, brand_clean, brand_aliases, logger
                    ),
                    axis=1
                )

                # Assign sort order
                df['new_sort_order'] = -2
                match_df = df[df['priority'].isin([1, 2, 3])].sort_values(['priority', 'match_score'], ascending=[True, False])
                if not match_df.empty:
                    valid_indices = match_df.index[match_df.apply(
                        lambda row: row['is_model_match'] if row['priority'] in [1, 2] else row['is_brand_match'], axis=1
                    )]
                    if not valid_indices.empty:
                        df.loc[valid_indices, 'new_sort_order'] = range(1, len(valid_indices) + 1)

                # Update database
                await cursor.execute(
                    "UPDATE utb_ImageScraperResult SET SortOrder = NULL WHERE EntryID = ?",
                    (entry_id,)
                )
                updates = df[['ResultID', 'new_sort_order']].to_dict('records')
                if updates:
                    values_clause = ", ".join(
                        f"({row['ResultID']}, {row['new_sort_order']})"
                        for row in updates
                    )
                    bulk_update_query = f"""
                        UPDATE utb_ImageScraperResult
                        SET SortOrder = v.sort_order
                        FROM (VALUES {values_clause}) AS v(result_id, sort_order)
                        WHERE utb_ImageScraperResult.ResultID = v.result_id
                    """
                    await cursor.execute(bulk_update_query)
                    logger.info(f"Updated {len(updates)} rows for EntryID {entry_id}")

                # Verify and fix NULL SortOrder
                await cursor.execute(
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
                    for r in await cursor.fetchall()
                ]

                null_count = sum(1 for r in results if r['SortOrder'] is None)
                if null_count > 0:
                    logger.warning(f"Found {null_count} rows with NULL SortOrder for EntryID {entry_id}")
                    await cursor.execute(
                        "UPDATE utb_ImageScraperResult SET SortOrder = -2 WHERE EntryID = ? AND SortOrder IS NULL",
                        (entry_id,)
                    )
                    logger.info(f"Set {null_count} NULL SortOrder rows to -2 for EntryID {entry_id}")

                await conn.commit()
                return results

    except pyodbc.Error as e:
        logger.error(f"Database error for FileID {file_id}, EntryID {entry_id}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error for FileID {file_id}, EntryID {entry_id}: {e}", exc_info=True)
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