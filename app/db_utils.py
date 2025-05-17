
import logging
import pandas as pd
import json
import os
import aiofiles
import re
import pyodbc
import aioodbc
import asyncio
import datetime
from typing import Optional, List, Dict, Any
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError, DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession
from database_config import conn_str, async_engine, engine
from aws_s3 import upload_file_to_space
from common import clean_string, validate_model, validate_brand, generate_aliases, calculate_priority, generate_brand_aliases
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import logging
import pandas as pd
import re
import aioodbc
import asyncio
from typing import Optional, List, Dict, Any
from sqlalchemy.exc import SQLAlchemyError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from database_config import conn_str
from common import clean_string, validate_model, validate_brand, generate_aliases, calculate_priority, generate_brand_aliases

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

async def get_endpoint(logger: Optional[logging.Logger] = None) -> str:
    logger = logger or default_logger
    try:
        async with aioodbc.connect(dsn=conn_str) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT TOP 1 EndpointURL FROM utb_Endpoints WHERE EndpointIsBlocked = 0 ORDER BY NEWID()")
                result = await cursor.fetchone()
                endpoint = result[0] if result else "No EndpointURL"
                logger.info(f"Retrieved endpoint: {endpoint}")
                return endpoint
    except Exception as e:
        logger.error(f"Database error getting endpoint: {e}")
        return "No EndpointURL"

def sync_get_endpoint(logger: Optional[logging.Logger] = None) -> Optional[str]:
    logger = logger or default_logger
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT TOP 1 EndpointURL FROM utb_Endpoints WHERE EndpointIsBlocked = 0 ORDER BY NEWID()")
            result = cursor.fetchone()
            endpoint = result[0] if result else None
            cursor.close()
            if endpoint:
                logger.info(f"Retrieved endpoint: {endpoint}")
            else:
                logger.error("No endpoint found")
            return endpoint
    except pyodbc.Error as e:
        logger.error(f"Database error getting endpoint: {e}")
        return None
import logging
import pandas as pd
import re
import aioodbc
import asyncio
import aiofiles
from typing import Optional, List, Dict, Any
from sqlalchemy.exc import SQLAlchemyError, DBAPIError
from sqlalchemy.sql import text
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from database_config import conn_str, async_engine, engine
from common import clean_string, validate_model, validate_brand, generate_aliases, calculate_priority, generate_brand_aliases
from aiobotocore.session import get_session
from aiobotocore.config import AioConfig
from config import S3_CONFIG
import mimetypes
import os
import urllib.parse

# Initialize mimetypes for .log files
mimetypes.add_type('text/plain', '.log')
import logging
import pandas as pd
import aioodbc
from typing import Optional, List, Dict
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from common import clean_string, validate_model, validate_brand, calculate_priority, generate_aliases, generate_brand_aliases
from database_config import conn_str  # Import conn_str from database_config
from sqlalchemy.sql import text

import logging
import pandas as pd
import aioodbc
import pyodbc  # Added for pyodbc.Error
from typing import Optional, List, Dict
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from common import clean_string, validate_model, validate_brand, calculate_priority, generate_aliases, generate_brand_aliases
from database_config import conn_str  # Import conn_str from database_config

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type(pyodbc.Error),  # Changed from aioodbc.Error to pyodbc.Error
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
                await cursor.execute(
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
                    INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                    WHERE r.FileID = ? AND t.EntryID = ?
                    """,
                    (file_id, entry_id)
                )
                columns = [column[0] for column in cursor.description]
                logger.debug(f"Raw rows: {rows[:2]}")  # Log first two rows
                logger.debug(f"Columns: {columns}")
                if rows and len(rows[0]) != len(columns):
                    logger.error(f"Row length mismatch: got {len(rows[0])}, expected {len(columns)}")
                    return []
                df = pd.DataFrame(rows, columns=columns)
                if df.empty:
                    logger.warning(f"No data found for FileID: {file_id}, EntryID: {entry_id}")
                    return []
                rows = await cursor.fetchall()
                if not rows:
                    logger.warning(f"No data returned for FileID {file_id}, EntryID {entry_id}")
                    return []
                df = pd.DataFrame(rows, columns=columns)
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

async def fetch_missing_images(file_id: str, limit: int = 1000, ai_analysis_only: bool = True, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    logger = logger or logging.getLogger(__name__)
    try:
        file_id = int(file_id)
        logger.info(f"Starting fetch_missing_images for FileID {file_id}, ai_analysis_only={ai_analysis_only}, limit={limit}")

        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with async_engine.connect() as conn:
                    if ai_analysis_only:
                        query = text("""
                            SELECT t.ResultID, t.EntryID, t.ImageUrl, t.ImageUrlThumbnail,
                                   r.ProductBrand, r.ProductCategory, r.ProductColor
                            FROM utb_ImageScraperResult t
                            INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                            WHERE r.FileID = :file_id
                            AND (t.AiJson IS NULL OR t.AiJson = '' OR ISJSON(t.AiJson) = 0)
                            AND t.ImageUrl IS NOT NULL AND t.ImageUrl <> ''
                            AND t.SortOrder > 0
                            ORDER BY t.ResultID
                            OFFSET 0 ROWS FETCH NEXT :limit ROWS ONLY
                        """)
                        params = {"file_id": file_id, "limit": limit}
                    else:
                        query = text("""
                            SELECT r.EntryID, r.FileID, r.ProductBrand, r.ProductCategory, r.ProductColor,
                                   t.ResultID, t.ImageUrl, t.ImageUrlThumbnail
                            FROM utb_ImageScraperRecords r
                            LEFT JOIN utb_ImageScraperResult t ON r.EntryID = t.EntryID AND t.SortOrder >= 0
                            WHERE r.FileID = :file_id AND t.ResultID IS NULL
                            ORDER BY r.EntryID
                            OFFSET 0 ROWS FETCH NEXT :limit ROWS ONLY
                        """)
                        params = {"file_id": file_id, "limit": limit}

                    logger.debug(f"Attempt {attempt + 1}: Executing query: {query} with params: {params}")
                    result = await conn.execute(query, params)
                    df = pd.DataFrame(result.fetchall(), columns=result.keys())
                    result.close()

                    logger.info(f"Fetched {len(df)} images for FileID {file_id}, ai_analysis_only={ai_analysis_only}")
                    if not df.empty:
                        logger.debug(f"Sample results: {df.head(2).to_dict()}")
                    else:
                        logger.info(f"No missing images found for FileID {file_id}")
                    return df

            except SQLAlchemyError as e:
                logger.error(f"Database error on attempt {attempt + 1}/{max_retries} for FileID {file_id}: {e}")
                if attempt < max_retries - 1:
                    delay = 5 * (2 ** attempt)
                    logger.info(f"Retrying after {delay} seconds...")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"Max retries reached for FileID {file_id}")
                    raise

    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Unexpected error fetching missing images for FileID {file_id}: {e}", exc_info=True)
        return pd.DataFrame()

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type(pyodbc.Error),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying insert_search_results for FileID {retry_state.kwargs.get('file_id', 'unknown')} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def insert_search_results(df: pd.DataFrame, logger: Optional[logging.Logger] = None, file_id: Optional[str] = None) -> bool:
    """
    Insert DataFrame rows into utb_ImageScraperResult table.
    Returns True on success, False on failure.
    """
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

        async with aioodbc.connect(dsn=conn_str) as conn:
            async with conn.cursor() as cursor:
                insert_query = """
                    INSERT INTO utb_ImageScraperResult (EntryID, ImageUrl, ImageDesc, ImageSource, ImageUrlThumbnail)
                    VALUES (?, ?, ?, ?, ?)
                """
                for _, row in df.iterrows():
                    await cursor.execute(
                        insert_query,
                        (
                            row['EntryID'],
                            row['ImageUrl'],
                            row['ImageDesc'],
                            row['ImageSource'],
                            row['ImageUrlThumbnail']
                        )
                    )
                await conn.commit()
                logger.info(f"Inserted {len(df)} rows into utb_ImageScraperResult")
                return True

    except pyodbc.Error as e:
        logger.error(f"Database error during insertion for FileID {file_id or 'unknown'}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error during insertion for FileID {file_id or 'unknown'}: {e}", exc_info=True)
        return False
async def update_sort_order(file_id: str, logger: Optional[logging.Logger] = None) -> Optional[List[Dict]]:
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        logger.info(f"Starting batch SortOrder update for FileID: {file_id}")
        
        async with aioodbc.connect(dsn=conn_str) as conn:
            async with conn.cursor() as cursor:
                query = """
                    SELECT EntryID, ProductBrand, ProductModel, ProductColor, ProductCategory 
                    FROM utb_ImageScraperRecords 
                    WHERE FileID = ?
                """
                logger.debug(f"Executing query: {query} with FileID: {file_id}")
                await cursor.execute(query, (file_id,))
                entries = await cursor.fetchall()
        
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
                    results.extend(entry_results)
                    success_count += 1
                else:
                    failure_count += 1
                    logger.warning(f"No results for EntryID {entry_id}")
            except Exception as e:
                failure_count += 1
                logger.error(f"Error processing EntryID {entry_id}: {e}", exc_info=True)
        
        logger.info(f"Completed batch SortOrder update for FileID {file_id}: {success_count} entries successful, {failure_count} failed")
        
        async with aioodbc.connect(dsn=conn_str) as conn:
            async with conn.cursor() as cursor:
                query_positive = """
                    SELECT COUNT(DISTINCT t.EntryID)
                    FROM utb_ImageScraperResult t
                    INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                    WHERE r.FileID = ? AND t.SortOrder > 0
                """
                logger.debug(f"Executing query: {query_positive} with FileID: {file_id}")
                await cursor.execute(query_positive, (file_id,))
                positive_entries = (await cursor.fetchone())[0]
                
                query_zero = """
                    SELECT COUNT(DISTINCT t.EntryID)
                    FROM utb_ImageScraperResult t
                    INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                    WHERE r.FileID = ? AND t.SortOrder = 0
                """
                await cursor.execute(query_zero, (file_id,))
                brand_match_entries = (await cursor.fetchone())[0]
                
                query_negative = """
                    SELECT COUNT(DISTINCT t.EntryID)
                    FROM utb_ImageScraperResult t
                    INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                    WHERE r.FileID = ? AND t.SortOrder < 0
                """
                await cursor.execute(query_negative, (file_id,))
                no_match_entries = (await cursor.fetchone())[0]
            
            logger.info(f"Verification for FileID {file_id}: "
                       f"{positive_entries} entries with model matches, "
                       f"{brand_match_entries} entries with brand matches only, "
                       f"{no_match_entries} entries with no matches")
        
        return results
    except Exception as e:
        logger.error(f"Error in batch SortOrder update for FileID {file_id}: {e}", exc_info=True)
        return None

async def update_sort_no_image_entry(file_id: str, logger: Optional[logging.Logger] = None) -> Optional[Dict]:
    logger = logger or default_logger
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
            rows_affected = result.rowcount
            logger.info(f"Deleted {rows_affected} placeholder entries for FileID: {file_id}")
            
            return {"file_id": file_id, "rows_affected": rows_affected}
    
    except ValueError as ve:
        logger.error(f"Invalid file_id format: {file_id}, error: {str(ve)}")
        return None
    except Exception as e:
        logger.error(f"Error updating entries for FileID: {file_id}, error: {str(e)}")
        return None
import logging
import pyodbc
from typing import Optional, List, Dict
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from database_config import conn_str  # Import conn_str from database_config

# Existing database functions (e.g., update_sort_order_per_entry, get_send_to_email) would be here
# For brevity, only adding update_initial_sort_order

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(pyodbc.Error),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying update_initial_sort_order for FileID {retry_state.kwargs['file_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def update_initial_sort_order(file_id: str, logger: Optional[logging.Logger] = None) -> Optional[List[Dict]]:
    """
    Perform an initial sort order update for all entries under a given file_id.
    Sets SortOrder based on match_score from AiJson, with fallback to ResultID.
    Returns a list of updated records for verification.
    """
    logger = logger or logging.getLogger(__name__)
    try:
        file_id = int(file_id)
        logger.info(f"Starting initial SortOrder update for FileID: {file_id}")

        with pyodbc.connect(conn_str, autocommit=False) as conn:
            cursor = conn.cursor()

            # Fetch all results for the file_id
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
                    t.SortOrder
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                WHERE r.FileID = ?
                """,
                (file_id,)
            )
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            if not rows:
                logger.warning(f"No data found for FileID: {file_id}")
                return []

            # Process results
            results = [dict(zip(columns, row)) for row in rows]
            updates = []
            for result in results:
                result_id = result['ResultID']
                match_score = result['match_score']
                # Assign SortOrder: positive for valid match_score (>0), -2 for invalid or zero
                new_sort_order = int(match_score * 100) if match_score > 0 else -2
                updates.append((result_id, new_sort_order))

            # Reset existing SortOrder
            cursor.execute(
                "UPDATE utb_ImageScraperResult SET SortOrder = NULL WHERE EntryID IN "
                "(SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?)",
                (file_id,)
            )

            # Apply updates
            if updates:
                values_clause = ", ".join(f"({result_id}, {sort_order})" for result_id, sort_order in updates)
                cursor.execute(
                    f"""
                    UPDATE utb_ImageScraperResult
                    SET SortOrder = v.sort_order
                    FROM (VALUES {values_clause}) AS v(result_id, sort_order)
                    WHERE utb_ImageScraperResult.ResultID = v.result_id
                    """
                )
                logger.info(f"Updated {len(updates)} rows for FileID: {file_id}")

            # Verify and fix NULL SortOrder
            cursor.execute(
                """
                SELECT 
                    COUNT(*) AS total_count,
                    SUM(CASE WHEN SortOrder > 0 THEN 1 ELSE 0 END) AS positive_count,
                    SUM(CASE WHEN SortOrder IS NULL THEN 1 ELSE 0 END) AS null_count
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                WHERE r.FileID = ?
                """,
                (file_id,)
            )
            row = cursor.fetchone()
            total_count, positive_count, null_count = row
            logger.info(f"Verification for FileID {file_id}: {total_count} total rows, "
                       f"{positive_count} positive SortOrder, {null_count} NULL SortOrder")
            if null_count > 0:
                logger.warning(f"Found {null_count} rows with NULL SortOrder for FileID {file_id}")
                cursor.execute(
                    "UPDATE utb_ImageScraperResult SET SortOrder = -2 WHERE EntryID IN "
                    "(SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?) AND SortOrder IS NULL",
                    (file_id,)
                )
                logger.info(f"Set {null_count} NULL SortOrder rows to -2 for FileID: {file_id}")

            # Fetch updated results
            cursor.execute(
                """
                SELECT ResultID, EntryID, SortOrder, ImageDesc, ImageSource, ImageUrl
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                WHERE r.FileID = ?
                ORDER BY SortOrder DESC
                """,
                (file_id,)
            )
            final_results = [
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

            conn.commit()
            logger.info(f"Completed initial SortOrder update for FileID: {file_id} with {len(final_results)} results")
            return final_results

    except pyodbc.Error as e:
        logger.error(f"Database error for FileID {file_id}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error for FileID {file_id}: {e}", exc_info=True)
        return None

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

            # Fetch attributes if not provided
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

            # Fetch search results
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
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                WHERE r.FileID = ? AND t.EntryID = ?
                """,
                (file_id, entry_id)
            )
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            df = pd.DataFrame(rows, columns=columns)
            if df.empty:
                logger.warning(f"No data found for FileID: {file_id}, EntryID: {entry_id}")
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

            # Fetch updated results
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

            # Verify and fix NULL SortOrder
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
        return None
async def update_sort_order_per_entry(file_id: str, logger: Optional[logging.Logger] = None) -> Optional[Dict]:
    logger = logger or logging.getLogger(__name__)
    try:
        file_id = int(file_id)
        logger.info(f"Starting per-entry SortOrder update for FileID: {file_id}")

        # Single connection for all operations
        async with aioodbc.connect(dsn=conn_str) as conn:
            async with conn.cursor() as cursor:
                # Fetch entries
                query = """
                    SELECT EntryID, ProductBrand, ProductModel, ProductColor, ProductCategory 
                    FROM utb_ImageScraperRecords 
                    WHERE FileID = ?
                """
                logger.debug(f"Executing query: {query} with FileID: {file_id}")
                await cursor.execute(query, (file_id,))
                entries = await cursor.fetchall()

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
                        # Generate aliases
                        brand_aliases = await generate_brand_aliases(brand or '', brand_aliases_dict)
                        logger.debug(f"Brand aliases for EntryID {entry_id}, Brand '{brand}': {brand_aliases}")

                        model_aliases = generate_aliases(model) if model else []
                        if model and '_' in model:
                            model_base = model.split('_')[0]
                            model_aliases.append(model_base)
                        model_aliases = list(set(model_aliases))
                        logger.debug(f"Model aliases for EntryID {entry_id}, Model '{model}': {model_aliases}")

                        # Update sort order
                        updates = await update_search_sort_order(
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

                                # Simplified alias matching with word boundaries
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

                            # Consolidated correction for SortOrder=-1
                            if unexpected_count > 0:
                                brand_pattern = '|'.join(re.escape(alias.lower()) for alias in brand_aliases)
                                model_pattern = '|'.join(re.escape(alias.lower()) for alias in model_aliases)
                                await cursor.execute(
                                    """
                                    UPDATE utb_ImageScraperResult
                                    SET SortOrder = CASE
                                        WHEN (
                                            LOWER(ImageDesc) REGEXP ? OR
                                            LOWER(ImageSource) REGEXP ? OR
                                            LOWER(ImageUrl) REGEXP ?
                                        ) THEN (
                                            SELECT COALESCE(MAX(SortOrder), 0) + 1 
                                            FROM utb_ImageScraperResult 
                                            WHERE EntryID = ? AND SortOrder > 0
                                        )
                                        ELSE -2
                                    END
                                    WHERE EntryID = ? AND SortOrder = -1
                                    """,
                                    (brand_pattern, brand_pattern, brand_pattern, entry_id, entry_id)
                                )
                                corrected_count = cursor.rowcount
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
                            entry_result["Error"] = "update_search_sort_order returned None"
                            logger.warning(f"No results for EntryID {entry_id}")

                        entry_results.append(entry_result)
                    except Exception as e:
                        failure_count += 1
                        entry_result["Status"] = "Failed"
                        entry_result["Error"] = str(e)
                        logger.error(f"Error processing EntryID {entry_id}: {e}", exc_info=True)
                        entry_results.append(entry_result)

                # Optimized verification
                verification_queries = [
                    ("PositiveSortOrderEntries", "t.SortOrder > 0"),
                    ("BrandMatchEntries", "t.SortOrder = 0"),
                    ("NoMatchEntries", "t.SortOrder < 0"),
                    ("NullSortOrderEntries", "t.SortOrder IS NULL"),
                    ("UnexpectedSortOrderEntries", "t.SortOrder = -1")
                ]
                verification = {}
                for key, condition in verification_queries:
                    await cursor.execute(
                        """
                        SELECT COUNT(DISTINCT t.EntryID)
                        FROM utb_ImageScraperResult t
                        INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                        WHERE r.FileID = ? AND {}
                        """.format(condition),
                        (file_id,)
                    )
                    verification[key] = (await cursor.fetchone())[0]

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


async def get_records_to_search(file_id: str, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        async with async_engine.connect() as conn:
            query = text("""
                SELECT EntryID, ProductModel AS SearchString, 'model_only' AS SearchType, FileID
                FROM utb_ImageScraperRecords 
                WHERE FileID = :file_id AND Step1 IS NULL
                ORDER BY EntryID, SearchType
            """)
            logger.debug(f"Executing query: {query} with FileID: {file_id}")
            result = await conn.execute(query, {"file_id": file_id})
            df = pd.DataFrame(await result.fetchall(), columns=result.keys())
            result.close()
            if not df.empty and (df["FileID"] != file_id).any():
                logger.error(f"Found rows with incorrect FileID for {file_id}")
                df = df[df["FileID"] == file_id]
            logger.info(f"Got {len(df)} search records for FileID: {file_id}")
            return df[["EntryID", "SearchString", "SearchType"]]
    except Exception as e:
        logger.error(f"Error getting records for FileID {file_id}: {e}", exc_info=True)
        return pd.DataFrame()

async def get_images_excel_db(file_id: str, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    logger = logger or default_logger
    expected_columns = ["ExcelRowID", "ImageUrl", "ImageUrlThumbnail", "Brand", "Style", "Color", "Category"]
    
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str, timeout=10) as conn:
            cursor = conn.cursor()
            query = """
                SELECT 
                    CAST(s.ExcelRowID AS INT) AS ExcelRowID,
                    ISNULL(r.ImageUrl, '') AS ImageUrl,
                    ISNULL(r.ImageUrlThumbnail, '') AS ImageUrlThumbnail,
                    ISNULL(s.ProductBrand, '') AS Brand,
                    ISNULL(s.ProductModel, '') AS Style,
                    ISNULL(s.ProductColor, '') AS Color,
                    ISNULL(s.ProductCategory, '') AS Category
                FROM utb_ImageScraperFiles f
                INNER JOIN utb_ImageScraperRecords s ON s.FileID = f.ID
                LEFT JOIN utb_ImageScraperResult r ON r.EntryID = s.EntryID 
                    AND r.SortOrder >= 0
                    AND r.ImageUrl IS NOT NULL AND r.ImageUrl <> ''
                WHERE f.ID = ?
                ORDER BY s.ExcelRowID
            """
            logger.debug(f"Executing query: {query} with ID: {file_id}")
            cursor.execute(query, (file_id,))
            
            columns = [desc[0] for desc in cursor.description]
            if columns != expected_columns:
                logger.error(f"Invalid columns returned for ID {file_id}. Got: {columns}, Expected: {expected_columns}")
                cursor.close()
                return pd.DataFrame(columns=expected_columns)
            
            rows = cursor.fetchall()
            logger.debug(f"Query result: rows={len(rows)}, columns={columns}")
            if rows:
                logger.debug(f"Sample row: {rows[0]}")
            
            valid_rows = []
            for i, row in enumerate(rows):
                if len(row) != len(columns):
                    logger.error(f"Row {i} has incorrect number of columns: got {len(row)}, expected {len(columns)}, row: {row}")
                    continue
                try:
                    row = list(row)
                    row[0] = int(row[0])  # ExcelRowID
                    valid_rows.append(row)
                except (ValueError, TypeError) as e:
                    logger.error(f"Invalid ExcelRowID in row {i}: {row[0]}, error: {e}")
                    continue
            
            cursor.close()
            if not valid_rows:
                logger.warning(f"No valid rows returned for ID {file_id}. Check database records.")
                return pd.DataFrame(columns=expected_columns)
            
            df = pd.DataFrame(valid_rows, columns=columns)
            df['ExcelRowID'] = pd.to_numeric(df['ExcelRowID'], errors='coerce').astype('Int64')
            logger.info(f"Fetched {len(df)} rows for Excel export for ID {file_id}")
            df = df[df['ImageUrl'].notnull() & (df['ImageUrl'] != '')]
            logger.debug(f"Filtered to {len(df)} rows with non-empty ImageUrl")
            
            if df.empty:
                logger.warning(f"No valid images found for ID {file_id} after filtering")
            return df
    except pyodbc.Error as e:
        logger.error(f"Database error in get_images_excel_db for ID {file_id}: {e}", exc_info=True)
        return pd.DataFrame(columns=expected_columns)
    except ValueError as e:
        logger.error(f"ValueError in get_images_excel_db for ID {file_id}: {e}", exc_info=True)
        return pd.DataFrame(columns=expected_columns)
    except Exception as e:
        logger.error(f"Unexpected error in get_images_excel_db for ID {file_id}: {e}", exc_info=True)
        return pd.DataFrame(columns=expected_columns)

async def fetch_last_valid_entry(file_id: str, logger: Optional[logging.Logger] = None) -> Optional[int]:
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        async with aioodbc.connect(dsn=conn_str) as conn:
            async with conn.cursor() as cursor:
                query = """
                    SELECT MAX(t.EntryID)
                    FROM utb_ImageScraperResult t
                    INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                    WHERE r.FileID = ? AND t.SortOrder IS NOT NULL
                """
                logger.debug(f"Executing query: {query} with FileID: {file_id}")
                await cursor.execute(query, (file_id,))
                result = await cursor.fetchone()
                if result and result[0]:
                    logger.info(f"Last valid EntryID for FileID {file_id}: {result[0]}")
                    return result[0]
                logger.info(f"No valid EntryIDs found for FileID {file_id}")
                return None
    except Exception as e:
        logger.error(f"Error fetching last valid EntryID for FileID {file_id}: {e}", exc_info=True)
        return None

async def update_file_location_complete(file_id: str, file_location: str, logger: Optional[logging.Logger] = None) -> None:
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE utb_ImageScraperFiles SET FileLocationURLComplete = ? WHERE ID = ?", (file_location, file_id))
            conn.commit()
            cursor.close()
            logger.info(f"Updated file location for FileID: {file_id}")
    except pyodbc.Error as e:
        logger.error(f"Database error in update_file_location_complete: {e}")
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")

async def update_file_generate_complete(file_id: str, logger: Optional[logging.Logger] = None) -> None:
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE utb_ImageScraperFiles SET CreateFileCompleteTime = GETDATE() WHERE ID = ?", (file_id,))
            conn.commit()
            cursor.close()
            logger.info(f"Marked file generation complete for FileID: {file_id}")
    except pyodbc.Error as e:
        logger.error(f"Database error in update_file_generate_complete: {e}")
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")

async def get_send_to_email(file_id: int, logger: Optional[logging.Logger] = None) -> str:
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT UserEmail FROM utb_ImageScraperFiles WHERE ID = ?", (file_id,))
            result = cursor.fetchone()
            cursor.close()
            if result and result[0]:
                return result[0]
            logger.warning(f"No email found for FileID {file_id}")
            return "nik@accessx.com"
    except pyodbc.Error as e:
        logger.error(f"Database error fetching email for FileID {file_id}: {e}")
        return "nik@accessx.com"
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")
        return "nik@accessx.com"

async def update_log_url_in_db(file_id: str, log_url: str, logger: Optional[logging.Logger] = None) -> bool:
    logger = logger or logging.getLogger(__name__)
    try:
        file_id = int(file_id)
        async with aioodbc.connect(dsn=conn_str) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    IF NOT EXISTS (
                        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_NAME = 'utb_ImageScraperFiles' 
                        AND COLUMN_NAME = 'LogFileUrl'
                    )
                    BEGIN
                        ALTER TABLE utb_ImageScraperFiles 
                        ADD LogFileUrl NVARCHAR(MAX)
                    END
                    """
                )
                await cursor.execute(
                    "UPDATE utb_ImageScraperFiles SET LogFileUrl = ? WHERE ID = ?",
                    (log_url, file_id)
                )
                await conn.commit()
                logger.info(f"Updated log URL '{log_url}' for FileID {file_id}")
                return True
    except aioodbc.Error as e:
        logger.error(f"Database error updating log URL for FileID {file_id}: {e}", exc_info=True)
        return False
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error updating log URL for FileID {file_id}: {e}", exc_info=True)
        return False

# Other database functions (e.g., update_initial_sort_order, get_send_to_email) would be here
# For brevity, only showing update_log_url_in_db

async def export_dai_json(file_id: int, entry_ids: Optional[List[int]], logger: logging.Logger) -> str:
    try:
        json_urls = []
        async with aioodbc.connect(dsn=conn_str) as conn:
            async with conn.cursor() as cursor:
                query = """
                    SELECT t.ResultID, t.EntryID, t.AiJson, t.AiCaption, t.ImageIsFashion
                    FROM utb_ImageScraperResult t
                    INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                    WHERE r.FileID = ? AND t.AiJson IS NOT NULL AND t.AiCaption IS NOT NULL
                """
                params = [file_id]
                if entry_ids:
                    query += " AND t.EntryID IN ({})".format(','.join('?' * len(entry_ids)))
                    params.extend(entry_ids)
                
                logger.debug(f"Executing query: {query} with params: {params}")
                await cursor.execute(query, params)
                entry_results = {}
                for row in await cursor.fetchall():
                    entry_id = row[1]
                    result = {
                        "ResultID": row[0],
                        "EntryID": row[1],
                        "AiJson": json.loads(row[2]) if row[2] else {},
                        "AiCaption": row[3],
                        "ImageIsFashion": bool(row[4])
                    }
                    if entry_id not in entry_results:
                        entry_results[entry_id] = []
                    entry_results[entry_id].append(result)

        if not entry_results:
            logger.warning(f"No valid AI results to export for FileID {file_id}")
            return ""

        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        temp_json_dir = f"temp_json_{file_id}"
        os.makedirs(temp_json_dir, exist_ok=True)

        for entry_id, results in entry_results.items():
            json_filename = f"result_{entry_id}_{timestamp}.json"
            local_json_path = os.path.join(temp_json_dir, json_filename)

            async with aiofiles.open(local_json_path, 'w') as f:
                await f.write(json.dumps(results, indent=2))
            
            logger.debug(f"Saved JSON to {local_json_path}, size: {os.path.getsize(local_json_path)} bytes")
            logger.debug(f"JSON content sample for EntryID {entry_id}: {json.dumps(results[:2], indent=2)}")

            s3_key = f"super_scraper/jobs/{file_id}/{json_filename}"
            public_url = await upload_file_to_space(
                local_json_path, s3_key, is_public=True, logger=logger, file_id=file_id
            )
            
            if public_url:
                logger.info(f"Exported JSON for EntryID {entry_id} to {public_url}")
                json_urls.append(public_url)
            else:
                logger.error(f"Failed to upload JSON for EntryID {entry_id}")
            
            os.remove(local_json_path)

        os.rmdir(temp_json_dir)
        return json_urls[0] if json_urls else ""

    except Exception as e:
        logger.error(f"Error exporting DAI JSON for FileID {file_id}: {e}", exc_info=True)
        return ""

async def remove_endpoint(endpoint: str, logger: Optional[logging.Logger] = None) -> None:
    logger = logger or default_logger
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE utb_Endpoints SET EndpointIsBlocked = 1 WHERE EndpointURL = ?", (endpoint,))
            conn.commit()
            cursor.close()
            logger.info(f"Marked endpoint as blocked: {endpoint}")
    except pyodbc.Error as e:
        logger.error(f"Error marking endpoint as blocked: {e}")