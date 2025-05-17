import logging
import pandas as pd
import re
import asyncio
import pyodbc
from typing import Optional, List, Dict
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from database_config import conn_str
from common import clean_string, validate_model, validate_brand, calculate_priority, generate_aliases

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
    retry=retry_if_exception_type(pyodbc.Error),
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

        # Filter rows with invalid thumbnail URLs or placeholders
        original_len = len(df)
        df = df[df['ImageUrlThumbnail'].apply(lambda x: validate_thumbnail_url(x, logger)) & 
                ~df['ImageUrl'].str.contains('placeholder://', na=False)]
        if df.empty:
            logger.warning(f"No rows with valid thumbnail URLs after filtering {original_len} rows")
            return False
        logger.debug(f"Filtered to {len(df)} rows with valid thumbnail URLs from {original_len}")

        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            insert_query = """
                INSERT INTO utb_ImageScraperResult (EntryID, ImageUrl, ImageDesc, ImageSource, ImageUrlThumbnail)
                VALUES (?, ?, ?, ?, ?)
            """
            for _, row in df.iterrows():
                cursor.execute(
                    insert_query,
                    (
                        row['EntryID'],
                        row['ImageUrl'],
                        row['ImageDesc'],
                        row['ImageSource'],
                        row['ImageUrlThumbnail']
                    )
                )
            conn.commit()
            logger.info(f"Inserted {len(df)} rows into utb_ImageScraperResult for FileID {file_id}")
            return True

    except pyodbc.Error as e:
        logger.error(f"Database error during insertion for FileID {file_id or 'unknown'}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error during insertion for FileID {file_id or 'unknown'}: {e}", exc_info=True)
        return False

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type(pyodbc.Error),
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
        with pyodbc.connect(conn_str, timeout=30) as conn:
            cursor = conn.cursor()
            query = """
                SELECT ResultID, EntryID, match_score, ImageDesc, ImageSource, ImageUrl,
                       ProductBrand, ProductModel, AiJson
                FROM utb_ImageScraperResult
                WHERE EntryID = ? AND FileID = ?
            """
            cursor.execute(query, (entry_id, file_id))
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            cursor.close()
        
        logger.debug(f"Worker PID {process.pid}: Fetched {len(rows)} rows with columns: {columns}")
        if not rows:
            logger.warning(f"Worker PID {process.pid}: No results found for FileID {file_id}, EntryID {entry_id}")
            return None
        
        if not all(len(row) == len(columns) for row in rows):
            logger.error(f"Worker PID {process.pid}: Row data mismatch for EntryID {entry_id}: expected {len(columns)} columns, got {len(rows[0])}")
            return None
        
        try:
            df = pd.DataFrame(rows, columns=columns)
        except ValueError as e:
            logger.error(f"Worker PID {process.pid}: DataFrame creation failed for EntryID {entry_id}: {e}")
            return None
        
        with pyodbc.connect(conn_str, timeout=30) as conn:
            cursor = conn.cursor()
            for _, row in df.iterrows():
                sort_order = 1.0 if row['match_score'] > 0 else 0.0
                cursor.execute(
                    "UPDATE utb_ImageScraperResult SET SortOrder = ? WHERE ResultID = ?",
                    (sort_order, row['ResultID'])
                )
            conn.commit()
            cursor.close()
        
        logger.info(f"Worker PID {process.pid}: Updated SortOrder for {len(df)} results in EntryID {entry_id}")
        return True
    except Exception as e:
        logger.error(f"Worker PID {process.pid}: Unexpected error for FileID {file_id}, EntryID {entry_id}: {e}", exc_info=True)
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
                    logger.warning(f"No attributes found for FileID: {file_id}, EntryID {entry_id}")
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
                logger.warning(f"No data found for FileID: {file_id}, EntryID {entry_id}")
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