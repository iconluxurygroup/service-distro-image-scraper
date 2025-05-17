
import logging
import pandas as pd
import json
import os
import aiofiles
import re
import pyodbc
import aioodbc
import asyncio
from typing import Optional, List, Dict, Any
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError, DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession
from database_config import conn_str, async_engine, engine
from aws_s3 import upload_file_to_space
from common import clean_string, validate_model, validate_brand, generate_aliases, calculate_priority, generate_brand_aliases
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type((SQLAlchemyError, DBAPIError, pyodbc.Error)),
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
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        entry_id = int(entry_id)
        logger.debug(f"Updating SortOrder for FileID: {file_id}, EntryID: {entry_id}")

        async with async_engine.connect() as conn:
            async with conn.begin():
                if not all([brand, model]):
                    result = await conn.execute(
                        text("""
                            SELECT ProductBrand, ProductModel, ProductColor, ProductCategory
                            FROM utb_ImageScraperRecords
                            WHERE FileID = :file_id AND EntryID = :entry_id
                        """),
                        {"file_id": file_id, "entry_id": entry_id}
                    )
                    row = await result.fetchone()
                    result.close()
                    if row:
                        brand, model, color, category = row
                        logger.debug(f"Fetched attributes - Brand: {brand}, Model: {model}")
                    else:
                        logger.warning(f"No attributes found for FileID: {file_id}, EntryID: {entry_id}")
                        brand, model, color, category = brand or '', model or '', color or '', category or ''

                result = await conn.execute(
                    text("""
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
                            r.ProductModel
                        FROM utb_ImageScraperResult t
                        INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                        WHERE r.FileID = :file_id AND t.EntryID = :entry_id
                    """),
                    {"file_id": file_id, "entry_id": entry_id}
                )
                df = pd.DataFrame(await result.fetchall(), columns=result.keys())
                result.close()
                if df.empty:
                    logger.warning(f"No data found for FileID: {file_id}, EntryID: {entry_id}")
                    return []

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

                updates = df[['ResultID', 'new_sort_order']].to_dict('records')
                if updates:
                    await conn.execute(
                        text("UPDATE utb_ImageScraperResult SET SortOrder = NULL WHERE EntryID = :entry_id"),
                        {"entry_id": entry_id}
                    )
                    values_clause = ", ".join(
                        f"({row['ResultID']}, {row['new_sort_order']})"
                        for row in updates
                    )
                    bulk_update_query = text(f"""
                        UPDATE utb_ImageScraperResult
                        SET SortOrder = v.sort_order
                        FROM (VALUES {values_clause}) AS v(result_id, sort_order)
                        WHERE utb_ImageScraperResult.ResultID = v.result_id
                    """)
                    await conn.execute(bulk_update_query)
                    logger.info(f"Updated {len(updates)} rows for EntryID {entry_id}")

            async with conn.begin():
                result = await conn.execute(
                    text("""
                        SELECT ResultID, EntryID, SortOrder, ImageDesc, ImageSource, ImageUrl
                        FROM utb_ImageScraperResult
                        WHERE EntryID = :entry_id
                        ORDER BY SortOrder DESC
                    """),
                    {"entry_id": entry_id}
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
                    for r in await result.fetchall()
                ]
                result.close()

                positive_count = sum(1 for r in results if r['SortOrder'] is not None and r['SortOrder'] > 0)
                negative_count = sum(1 for r in results if r['SortOrder'] is not None and r['SortOrder'] < 0)
                null_count = sum(1 for r in results if r['SortOrder'] is None)
                logger.info(
                    f"Verification for EntryID {entry_id}: "
                    f"{positive_count} positive, {negative_count} negative, {null_count} NULL SortOrder"
                )
                if null_count > 0:
                    logger.warning(f"Found {null_count} rows with NULL SortOrder for EntryID {entry_id}")
                    await conn.execute(
                        text("UPDATE utb_ImageScraperResult SET SortOrder = -2 WHERE EntryID = :entry_id AND SortOrder IS NULL"),
                        {"entry_id": entry_id}
                    )
                    logger.info(f"Set {null_count} NULL SortOrder rows to -2 for EntryID {entry_id}")

                return results

    except (SQLAlchemyError, DBAPIError, pyodbc.Error) as e:
        logger.error(f"Database error for FileID {file_id}, EntryID {entry_id}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error for FileID {file_id}, EntryID {entry_id}: {e}", exc_info=True)
        return None
    finally:
        async with async_engine.connect() as conn:
            await conn.execute(
                text("UPDATE utb_ImageScraperResult SET SortOrder = -2 WHERE EntryID = :entry_id AND SortOrder IS NULL"),
                {"entry_id": entry_id}
            )
            await conn.commit()
            logger.debug(f"Applied fallback: Set NULL SortOrder to -2 for EntryID {entry_id}")

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
