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
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from config import conn_str, engine, async_engine
from aws_s3 import upload_file_to_space
from common import clean_string, validate_model, generate_aliases, filter_model_results, calculate_priority, generate_brand_aliases, validate_brand

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

async def get_endpoint(logger: Optional[logging.Logger] = None) -> str:
    """Retrieve a random non-blocked endpoint from the database."""
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
    """Synchronously retrieve a random non-blocked endpoint from the database."""
    logger = logger or default_logger
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT TOP 1 EndpointURL FROM utb_Endpoints WHERE EndpointIsBlocked = 0 ORDER BY NEWID()")
            result = cursor.fetchone()
            endpoint = result[0] if result else None
            if endpoint:
                logger.info(f"Retrieved endpoint: {endpoint}")
            else:
                logger.error("No endpoint found")
            return endpoint
    except pyodbc.Error as e:
        logger.error(f"Database error getting endpoint: {e}")
        return None
def insert_search_results(df: pd.DataFrame, logger: Optional[logging.Logger] = None) -> bool:
    logger = logger or logging.getLogger(__name__)
    if df.empty:
        logger.info("No rows to insert: DataFrame is empty")
        return False

    required = ['EntryID', 'ImageUrl', 'ImageDesc', 'ImageSource', 'ImageUrlThumbnail']
    if not all(col in df.columns for col in required):
        missing_cols = [col for col in required if col not in df.columns]
        logger.error(f"Missing columns: {missing_cols}")
        return False

    try:
        df = df.copy()
        df['EntryID'] = df['EntryID'].astype(int)
        for col in required[1:]:
            df.loc[:, col] = df[col].astype(str).replace('', None)
            if df[col].isnull().any():
                logger.warning(f"Null values in {col}: {df[df[col].isnull()].to_dict()}")
    except Exception as e:
        logger.error(f"Data validation failed: {e}")
        return False

    df = df.drop_duplicates(subset=['EntryID', 'ImageUrl'], keep='first')
    logger.info(f"Deduplicated to {len(df)} rows")

    try:
        with engine.begin() as conn:
            inserted_count = 0
            updated_count = 0
            for index, row in df.iterrows():
                try:
                    update_query = text("""
                        UPDATE utb_ImageScraperResult
                        SET ImageDesc = :image_desc, ImageSource = :image_source, 
                            ImageUrlThumbnail = :image_url_thumbnail, CreateTime = GETDATE()
                        WHERE EntryID = :entry_id AND ImageUrl = :image_url
                    """)
                    result = conn.execute(
                        update_query,
                        {
                            'image_desc': row['ImageDesc'],
                            'image_source': row['ImageSource'],
                            'image_url_thumbnail': row['ImageUrlThumbnail'],
                            'entry_id': row['EntryID'],
                            'image_url': row['ImageUrl']
                        }
                    )
                    updated_count += result.rowcount

                    if result.rowcount == 0:
                        insert_query = text("""
                            INSERT INTO utb_ImageScraperResult (EntryID, ImageUrl, ImageDesc, ImageSource, ImageUrlThumbnail, CreateTime)
                            SELECT :entry_id, :image_url, :image_desc, :image_source, :image_url_thumbnail, GETDATE()
                            WHERE NOT EXISTS (
                                SELECT 1 FROM utb_ImageScraperResult 
                                WHERE EntryID = :entry_id AND ImageUrl = :image_url
                            )
                        """)
                        result = conn.execute(
                            insert_query,
                            {
                                'entry_id': row['EntryID'],
                                'image_url': row['ImageUrl'],
                                'image_desc': row['ImageDesc'],
                                'image_source': row['ImageSource'],
                                'image_url_thumbnail': row['ImageUrlThumbnail']
                            }
                        )
                        inserted_count += result.rowcount
                except SQLAlchemyError as e:
                    logger.error(f"Failed to process row {index} for EntryID {row['EntryID']}: {e}", exc_info=True)
                    logger.debug(f"Row data: {row.to_dict()}")
                    continue
            logger.info(f"Inserted {inserted_count} and updated {updated_count} of {len(df)} rows")
            if inserted_count == 0 and updated_count == 0:
                logger.warning("No rows inserted or updated; likely all rows are duplicates")
            return inserted_count > 0 or updated_count > 0
    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}", exc_info=True)
        return False

        logger.error(f"Error in batch SortOrder update for FileID {file_id}: {e}", exc_info=True)
        return None

async def update_sort_order(file_id: str, logger: Optional[logging.Logger] = None) -> Optional[List[Dict]]:
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        logger.info(f"🔄 Starting batch SortOrder update for FileID: {file_id}")
        
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

async def update_log_url_in_db(file_id: str, log_url: str, logger: Optional[logging.Logger] = None) -> bool:
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                IF NOT EXISTS (
                    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = 'utb_ImageScraperFiles' 
                    AND COLUMN_NAME = 'LogFileUrl'
                )
                BEGIN
                    ALTER TABLE utb_ImageScraperFiles 
                    ADD LogFileUrl NVARCHAR(MAX)
                END
            """)
            cursor.execute("UPDATE utb_ImageScraperFiles SET LogFileUrl = ? WHERE ID = ?", (log_url, file_id))
            conn.commit()
            logger.info(f"✅ Updated log URL '{log_url}' for FileID {file_id}")
            return True
    except pyodbc.Error as e:
        logger.error(f"🔴 Database error updating log URL for FileID {file_id}: {e}", exc_info=True)
        return False
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")
        return False
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
            df = await conn.execute(query, {"file_id": file_id})
            df = pd.DataFrame(df.fetchall(), columns=df.keys())
            if not df.empty and (df["FileID"] != file_id).any():
                logger.error(f"Found rows with incorrect FileID for {file_id}")
                df = df[df["FileID"] == file_id]
            logger.info(f"Got {len(df)} search records for FileID: {file_id}")
            return df[["EntryID", "SearchString", "SearchType"]]
    except Exception as e:
        logger.error(f"Error getting records for FileID {file_id}: {e}", exc_info=True)
        return pd.DataFrame()
 

async def fetch_last_valid_entry(file_id: str, logger: Optional[logging.Logger] = None) -> Optional[int]:
    """Retrieve the last valid EntryID processed for a given FileID."""
    logger = logger or logging.getLogger(__name__)
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
import logging
import pandas as pd
import json
import os
import aiofiles
import asyncio
from typing import Optional, List, Dict, Any
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from config import conn_str, engine, async_engine
from aws_s3 import upload_file_to_space
from common import clean_string, validate_model, generate_aliases, filter_model_results, calculate_priority, generate_brand_aliases, validate_brand

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Other functions (unchanged) are omitted for brevity
def sync_update_search_sort_order(
    file_id: str,
    entry_id: str,
    brand: Optional[str] = None,
    model: Optional[str] = None,
    color: Optional[str] = None,
    category: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
    brand_rules: Optional[Dict] = None
) -> Optional[bool]:
    from utils import normalize_model, generate_aliases, clean_string
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        entry_id = int(entry_id)
        logger.info(f"🔄 Starting SortOrder update for FileID: {file_id}, EntryID: {entry_id}")

        with engine.begin() as conn:
            conn.execute(
                text("UPDATE utb_ImageScraperResult SET SortOrder = NULL WHERE EntryID = :entry_id"),
                {"entry_id": entry_id}
            )
            logger.debug(f"Reset SortOrder to NULL for EntryID {entry_id}")

            if not all([brand, model]):
                query_attrs = text("""
                    SELECT ProductBrand, ProductModel, ProductColor, ProductCategory
                    FROM utb_ImageScraperRecords
                    WHERE FileID = :file_id AND EntryID = :entry_id
                """)
                logger.debug(f"Executing query: {query_attrs} with FileID: {file_id}, EntryID: {entry_id}")
                result = conn.execute(query_attrs, {"file_id": file_id, "entry_id": entry_id}).fetchone()
                if result:
                    brand, model, color, category = result
                    logger.info(f"Fetched attributes - Brand: {brand}, Model: {model}, Color: {color}, Category: {category}")
                else:
                    logger.warning(f"No attributes found for FileID: {file_id}, EntryID: {entry_id}")
                    brand = brand or ''
                    model = model or ''
                    color = color or ''
                    category = category or ''

            query = text("""
                SELECT 
                    t.ResultID, 
                    t.EntryID,
                    CASE 
                        WHEN ISJSON(t.AiJson) = 1 
                        THEN ISNULL(TRY_CAST(JSON_VALUE(t.AiJson, '$.match_score') AS FLOAT), 0)
                        ELSE 0
                    END AS match_score,
                    t.SortOrder,
                    t.ImageDesc, 
                    t.ImageSource, 
                    t.ImageUrl,
                    r.ProductBrand, 
                    r.ProductModel,
                    t.AiJson
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                WHERE r.FileID = :file_id AND t.EntryID = :entry_id
            """)
            logger.debug(f"Executing query: {query} with FileID: {file_id}, EntryID: {entry_id}")
            df = pd.read_sql_query(query, conn, params={"file_id": file_id, "entry_id": entry_id})
            # ... (rest of the function unchanged)
            if df.empty:
                logger.warning(f"No eligible data found for FileID: {file_id}, EntryID: {entry_id}")
                return False

            # Log invalid JSON
            invalid_json_rows = df[df['AiJson'].notnull() & (df['AiJson'].str.strip() != '') & (df['match_score'] == 0)]
            for _, row in invalid_json_rows.iterrows():
                logger.warning(f"Invalid JSON in ResultID {row['ResultID']}: AiJson={row['AiJson'][:100]}...")

            logger.info(f"Fetched {len(df)} rows for EntryID {entry_id}")

            # Extract brand aliases from brand_rules
            brand_aliases = []
            if brand_rules and "brand_rules" in brand_rules:
                for rule in brand_rules["brand_rules"]:
                    if "names" in rule:
                        brand_clean = clean_string(brand).lower() if brand else ''
                        if any(clean_string(name).lower() == brand_clean for name in rule["names"]):
                            brand_aliases = [clean_string(name).lower() for name in rule["names"]]
                            break
            if not brand_aliases:
                # Fallback to default aliases
                brand_aliases_dict = {
                    "Scotch & Soda": [
                        "Scotch and Soda", "Scotch Soda", "Scotch&Soda", "ScotchAndSoda", "Scotch"
                    ],
                    "Adidas": ["Adidas AG", "Adidas Originals", "Addidas", "Adiddas"],
                    "BAPE": ["A Bathing Ape", "BATHING APE", "Bape Japan", "ABathingApe", "Bape"]
                }
                brand_clean = clean_string(brand).lower() if brand else ''
                for b, aliases in brand_aliases_dict.items():
                    if clean_string(b).lower() == brand_clean:
                        brand_aliases = [clean_string(b).lower()] + [clean_string(a).lower() for a in aliases]
                        break
                if not brand_aliases and brand_clean:
                    brand_aliases = [brand_clean]
            logger.debug(f"Using brand aliases: {brand_aliases}")

            # Normalize model and prepare aliases
            model_clean = normalize_model(model) if model else ''
            model_aliases = generate_aliases(model_clean) if model_clean else []

            # Clean DataFrame columns
            required_cols = ["ImageDesc", "ImageSource", "ImageUrl", "ProductBrand", "ProductModel"]
            for col in required_cols:
                if col not in df.columns:
                    logger.warning(f"Column {col} missing; adding empty column")
                    df[col] = ''
                df[f"{col}_clean"] = df[col].fillna('').apply(clean_string)

            # Filter model results (synchronous version)
            exact_df = df[df.apply(lambda row: validate_model(row, model_aliases, row['ResultID'], logger), axis=1)]
            discarded_df = df[~df.index.isin(exact_df.index)]
            logger.info(f"Filtered {len(exact_df)} exact matches and {len(discarded_df)} discarded rows for EntryID {entry_id}")

            # Calculate priorities
            df['priority'] = [calculate_priority(
                row, exact_df, model_clean, model_aliases, clean_string(brand) if brand else '', brand_aliases, logger
            ) for _, row in df.iterrows()]
            logger.info(f"Priority distribution: {df['priority'].value_counts().to_dict()}")

            # Assign default sort order
            df['new_sort_order'] = -2

            # Process matches
            match_rows = df[df['priority'].isin([1, 2, 3])]
            if not match_rows.empty:
                match_df = match_rows.sort_values(['priority', 'match_score'], ascending=[True, False])
                valid_indices = []
                for idx, row in match_df.iterrows():
                    result_id = row['ResultID']
                    if row['priority'] in [1, 2] and validate_model(row, model_aliases, result_id, logger):
                        valid_indices.append(idx)
                    elif row['priority'] == 3:
                        brand_found = validate_brand(row, brand_aliases, result_id, None, logger)
                        if brand_found:
                            valid_indices.append(idx)
                        else:
                            logger.warning(
                                f"ResultID {result_id}: Brand validation failed for aliases {brand_aliases}, "
                                f"ImageDesc: {row['ImageDesc']}, ImageSource: {row['ImageSource']}, ImageUrl: {row['ImageUrl']}"
                            )
                            df.at[idx, 'new_sort_order'] = -2
                            df.at[idx, 'priority'] = 4
                    else:
                        logger.warning(f"Model validation failed for ResultID {result_id}, checking brand")
                        brand_found = validate_brand(row, brand_aliases, result_id, None, logger)
                        df.at[idx, 'new_sort_order'] = -2 if not brand_found else None
                        df.at[idx, 'priority'] = 4 if not brand_found else 3
                        if brand_found:
                            valid_indices.append(idx)
                            logger.warning(
                                f"ResultID {result_id}: Brand validation failed for aliases {brand_aliases}, "
                                f"ImageDesc: {row['ImageDesc']}, ImageSource: {row['ImageSource']}, ImageUrl: {row['ImageUrl']}"
                            )

                valid_match_df = match_df.loc[valid_indices]
                if not valid_match_df.empty:
                    df.loc[valid_match_df.index, 'new_sort_order'] = range(1, len(valid_match_df) + 1)

            # Update SortOrder
            update_count = 0
            success_count = 0
            for _, row in df[['ResultID', 'new_sort_order']].iterrows():
                if row['new_sort_order'] == row.get('SortOrder', None) or pd.isna(row['new_sort_order']):
                    continue
                try:
                    result_id = int(row['ResultID'])
                    new_sort_order = int(row['new_sort_order'])
                    conn.execute(
                        text("""
                            UPDATE utb_ImageScraperResult 
                            SET SortOrder = :sort_order 
                            WHERE ResultID = :result_id
                        """),
                        {"sort_order": new_sort_order, "result_id": result_id}
                    )
                    update_count += 1
                    success_count += 1
                    logger.debug(f"Updated SortOrder to {new_sort_order} for ResultID {result_id}")
                except SQLAlchemyError as e:
                    logger.error(f"Failed to update ResultID {row['ResultID']}: {e}")
                    continue

            logger.info(f"Updated {success_count}/{update_count} rows for EntryID {entry_id}")

            # Verify updates
            temp_verify = conn.execute(
                text("SELECT COUNT(*) FROM utb_ImageScraperResult WHERE EntryID = :entry_id AND SortOrder IS NOT NULL"),
                {"entry_id": entry_id}
            ).scalar()
            logger.debug(f"Rows with non-NULL SortOrder after update: {temp_verify}")

            verify_query = text("""
                SELECT ResultID, EntryID, SortOrder, ImageDesc, ImageSource, ImageUrl
                FROM utb_ImageScraperResult
                WHERE EntryID = :entry_id
                ORDER BY SortOrder DESC
            """)
            results = [
                {
                    "ResultID": r[0], 
                    "EntryID": r[1], 
                    "SortOrder": r[2], 
                    "ImageDesc": r[3], 
                    "ImageSource": r[4], 
                    "ImageUrl": r[5]
                }
                for r in conn.execute(verify_query, {"entry_id": entry_id}).fetchall()
            ]

            positive_count = sum(1 for r in results if r['SortOrder'] is not None and r['SortOrder'] > 0)
            zero_count = sum(1 for r in results if r['SortOrder'] == 0)
            negative_count = sum(1 for r in results if r['SortOrder'] is not None and r['SortOrder'] < 0)
            null_count = sum(1 for r in results if r['SortOrder'] is None)
            logger.info(
                f"Verification for EntryID {entry_id}: "
                f"{positive_count} rows with positive SortOrder (model or brand matches), "
                f"{zero_count} rows with SortOrder=0 (legacy brand matches), "
                f"{negative_count} rows with negative SortOrder, "
                f"{null_count} rows with NULL SortOrder"
            )
            if null_count > 0:
                logger.warning(f"Found {null_count} rows with NULL SortOrder for EntryID {entry_id}")

            for r in results[:3]:
                logger.info(f"Sample - ResultID: {r['ResultID']}, EntryID: {r['EntryID']}, SortOrder: {r['SortOrder']}, ImageDesc: {r['ImageDesc']}")

            return success_count > 0

    except SQLAlchemyError as e:
        logger.error(f"Database error updating SortOrder for FileID {file_id}, EntryID {entry_id}: {e}", exc_info=True)
        return None
    except ValueError as e:
        logger.error(f"ValueError updating SortOrder for FileID {file_id}, EntryID {entry_id}: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Unexpected error updating SortOrder for FileID {file_id}, EntryID {entry_id}: {e}", exc_info=True)
        return None
import datetime
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
async def fetch_missing_images(file_id: str, limit: int = 1000, ai_analysis_only: bool = True, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    logger = logger or default_logger
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
                    df = await conn.execute(query, params)
                    df = pd.DataFrame(df.fetchall(), columns=df.keys())

                    logger.info(f"Fetched {len(df)} images for FileID {file_id}, ai_analysis_only={ai_analysis_only}")
                    if not df.empty:
                        logger.debug(f"Sample results: {df.head(2).to_dict()}")
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

async def remove_endpoint(endpoint: str, logger: Optional[logging.Logger] = None) -> None:
    logger = logger or default_logger
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE utb_Endpoints SET EndpointIsBlocked = 1 WHERE EndpointURL = ?", (endpoint,))
            conn.commit()
            logger.info(f"Marked endpoint as blocked: {endpoint}")
    except pyodbc.Error as e:
        logger.error(f"Error marking endpoint as blocked: {e}")

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
        logger.info(f"🔄 Starting SortOrder update for FileID: {file_id}, EntryID: {entry_id}")
        
        async with async_engine.begin() as conn:
            await conn.execute(
                text("""
                    UPDATE utb_ImageScraperResult
                    SET SortOrder = NULL
                    WHERE EntryID = :entry_id
                """),
                {"entry_id": entry_id}
            )
            logger.debug(f"Reset SortOrder to NULL for EntryID {entry_id}")

            if not all([brand, model]):
                query_attrs = text("""
                    SELECT ProductBrand, ProductModel, ProductColor, ProductCategory
                    FROM utb_ImageScraperRecords
                    WHERE FileID = :file_id AND EntryID = :entry_id
                """)
                logger.debug(f"Executing query: {query_attrs} with FileID: {file_id}, EntryID: {entry_id}")
                result = (await conn.execute(query_attrs, {"file_id": file_id, "entry_id": entry_id})).fetchone()
                if result:
                    brand, model, color, category = result
                    logger.info(f"Fetched attributes - Brand: {brand}, Model: {model}, Color: {color}, Category: {category}")
                else:
                    logger.warning(f"No attributes found for FileID: {file_id}, EntryID: {entry_id}")
                    brand = brand or ''
                    model = model or ''
                    color = color or ''
                    category = category or ''

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
            logger.debug(f"Executing query: {query} with FileID: {file_id}, EntryID: {entry_id}")
            df = await conn.execute(query, {"file_id": file_id, "entry_id": entry_id})
            df = pd.DataFrame(df.fetchall(), columns=df.keys())
            # ... (rest of the function unchanged)
            if df.empty:
                logger.warning(f"No eligible data found for FileID: {file_id}, EntryID: {entry_id}")
                return []

            logger.info(f"Fetched {len(df)} rows for EntryID {entry_id}")
            logger.debug(f"DataFrame columns after fetch: {df.columns.tolist()}")

            if brand_aliases is None:
                brand_aliases_dict = {
                    "Scotch & Soda": [
                        "Scotch and Soda", "Scotch Soda", "Scotch&Soda", "ScotchAndSoda", "Scotch"
                    ],
                    "Adidas": ["Adidas AG", "Adidas Originals", "Addidas", "Adiddas"],
                    "BAPE": ["A Bathing Ape", "BATHING APE", "Bape Japan", "ABathingApe", "Bape"]
                }
                brand_clean = clean_string(brand) if brand else ''
                brand_aliases = await generate_brand_aliases(brand or '', brand_aliases_dict)
                for b, aliases in brand_aliases_dict.items():
                    if clean_string(b).lower() == brand_clean.lower():
                        brand_aliases = [clean_string(b).lower()] + [clean_string(a).lower() for a in aliases]
                        break
                if not brand_aliases and brand_clean:
                    brand_aliases = [brand_clean]
            logger.debug(f"Using brand aliases: {brand_aliases}")

            model_clean = clean_string(model) if model else ''
            model_aliases = generate_aliases(model) if model else []

            required_cols = ["ImageDesc", "ImageSource", "ImageUrl", "ProductBrand", "ProductModel"]
            for col in required_cols:
                if col not in df.columns:
                    logger.warning(f"Column {col} missing; adding empty column")
                    df[col] = ''
                df[f"{col}_clean"] = df[col].fillna('').apply(clean_string)

            exact_df, discarded_df = await filter_model_results(df, debug=True, logger=logger, brand_aliases=brand_aliases)
            logger.info(f"Filtered {len(exact_df)} exact matches and {len(discarded_df)} discarded rows for EntryID {entry_id}")

            df['priority'] = [calculate_priority(
                row, exact_df, model_clean, model_aliases, clean_string(brand) if brand else '', brand_aliases, logger
            ) for _, row in df.iterrows()]
            logger.info(f"Priority distribution: {df['priority'].value_counts().to_dict()}")

            df['new_sort_order'] = -2
            logger.debug(f"Assigned default new_sort_order: {df[['ResultID', 'new_sort_order']].to_dict(orient='records')}")

            match_rows = df[df['priority'].isin([1, 2, 3])]
            if not match_rows.empty:
                match_df = match_rows.sort_values(['priority', 'match_score'], ascending=[True, False])
                valid_indices = []
                for idx, row in match_df.iterrows():
                    result_id = row['ResultID']
                    if row['priority'] in [1, 2] and validate_model(row, model_aliases, result_id, logger):
                        valid_indices.append(idx)
                    elif row['priority'] == 3:
                        brand_found = validate_brand(row, brand_aliases, result_id, None, logger)  # Pass None
                        if brand_found:
                            valid_indices.append(idx)
                        else:
                            logger.warning(f"Brand validation failed for ResultID {result_id}, downgrading to no match")
                            df.at[idx, 'new_sort_order'] = -2
                            df.at[idx, 'priority'] = 4
                    else:
                        logger.warning(f"Model validation failed for ResultID {result_id}, checking brand")
                        brand_found = validate_brand(row, brand_aliases, result_id, None, logger)  # Pass None
                        df.at[idx, 'new_sort_order'] = -2 if not brand_found else None
                        df.at[idx, 'priority'] = 4 if not brand_found else 3
                        if brand_found:
                            valid_indices.append(idx)
                            logger.debug(f"Downgraded ResultID {result_id} to new_sort_order={df.at[idx, 'new_sort_order']}")
                # ... (rest unchanged)

                valid_match_df = match_df.loc[valid_indices]
                if not valid_match_df.empty:
                    df.loc[valid_match_df.index, 'new_sort_order'] = range(1, len(valid_match_df) + 1)
                    logger.debug(f"Set new_sort_order for matches: {valid_match_df[['ResultID', 'new_sort_order']].to_dict(orient='records')}")

            update_count = 0
            success_count = 0
            for _, row in df[['ResultID', 'new_sort_order']].iterrows():
                if row['new_sort_order'] == row.get('SortOrder', None) or pd.isna(row['new_sort_order']):
                    continue
                try:
                    result_id = int(row['ResultID'])
                    new_sort_order = int(row['new_sort_order'])
                    result = conn.execute(
                        text("""
                            UPDATE utb_ImageScraperResult 
                            SET SortOrder = :sort_order 
                            WHERE ResultID = :result_id
                        """),
                        {"sort_order": new_sort_order, "result_id": result_id}
                    )
                    update_count += 1
                    success_count += result.rowcount
                    logger.debug(f"Updated SortOrder to {new_sort_order} for ResultID {result_id}")
                except SQLAlchemyError as e:
                    logger.error(f"Failed to update ResultID {row['ResultID']}: {e}")
                    raise

            logger.info(f"Updated {success_count}/{update_count} rows for EntryID {entry_id}")

            temp_verify = conn.execute(
                text("SELECT COUNT(*) FROM utb_ImageScraperResult WHERE EntryID = :entry_id AND SortOrder IS NOT NULL"),
                {"entry_id": entry_id}
            ).scalar()
            logger.debug(f"Rows with non-NULL SortOrder after update: {temp_verify}")

            verify_query = text("""
                SELECT ResultID, EntryID, SortOrder, ImageDesc, ImageSource, ImageUrl
                FROM utb_ImageScraperResult
                WHERE EntryID = :entry_id
                ORDER BY SortOrder DESC
            """)
            results = [
                {
                    "ResultID": r[0], 
                    "EntryID": r[1], 
                    "SortOrder": r[2], 
                    "ImageDesc": r[3], 
                    "ImageSource": r[4], 
                    "ImageUrl": r[5]
                }
                for r in conn.execute(verify_query, {"entry_id": entry_id}).fetchall()
            ]

            positive_count = sum(1 for r in results if r['SortOrder'] is not None and r['SortOrder'] > 0)
            zero_count = sum(1 for r in results if r['SortOrder'] == 0)
            negative_count = sum(1 for r in results if r['SortOrder'] is not None and r['SortOrder'] < 0)
            null_count = sum(1 for r in results if r['SortOrder'] is None)
            logger.info(
                f"Verification for EntryID {entry_id}: "
                f"{positive_count} rows with positive SortOrder (model or brand matches), "
                f"{zero_count} rows with SortOrder=0 (legacy brand matches), "
                f"{negative_count} rows with negative SortOrder, "
                f"{null_count} rows with NULL SortOrder"
            )
            if null_count > 0:
                logger.warning(f"Found {null_count} rows with NULL SortOrder for EntryID {entry_id}")

            for r in results[:3]:
                logger.info(f"Sample - ResultID: {r['ResultID']}, EntryID: {r['EntryID']}, SortOrder: {r['SortOrder']}, ImageDesc: {r['ImageDesc']}")

            return results

    except SQLAlchemyError as e:
        logger.error(f"Database error updating SortOrder for FileID {file_id}, EntryID {entry_id}: {e}", exc_info=True)
        return None
    except ValueError as e:
        logger.error(f"ValueError updating SortOrder for FileID {file_id}, EntryID {entry_id}: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Unexpected error updating SortOrder for FileID {file_id}, EntryID {entry_id}: {e}", exc_info=True)
        return None
def sync_update_search_sort_order(
    file_id: str,
    entry_id: str,
    brand: Optional[str] = None,
    model: Optional[str] = None,
    color: Optional[str] = None,
    category: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
    brand_rules: Optional[Dict] = None
) -> Optional[bool]:
    """Synchronously update the sort order for search results using SQLAlchemy."""
    from utils import normalize_model, generate_aliases, clean_string
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        entry_id = int(entry_id)
        logger.info(f"🔄 Starting SortOrder update for FileID: {file_id}, EntryID: {entry_id}")

        with engine.begin() as conn:
            # Reset SortOrder to NULL
            conn.execute(
                text("UPDATE utb_ImageScraperResult SET SortOrder = NULL WHERE EntryID = :entry_id"),
                {"entry_id": entry_id}
            )
            logger.debug(f"Reset SortOrder to NULL for EntryID {entry_id}")

            # Fetch attributes if not provided
            if not all([brand, model]):
                result = conn.execute(
                    text("""
                        SELECT ProductBrand, ProductModel, ProductColor, ProductCategory
                        FROM utb_ImageScraperRecords
                        WHERE FileID = :file_id AND EntryID = :entry_id
                    """),
                    {"file_id": file_id, "entry_id": entry_id}
                ).fetchone()
                if result:
                    brand, model, color, category = result
                    logger.info(f"Fetched attributes - Brand: {brand}, Model: {model}, Color: {color}, Category: {category}")
                else:
                    logger.warning(f"No attributes found for FileID: {file_id}, EntryID: {entry_id}")
                    brand = brand or ''
                    model = model or ''
                    color = color or ''
                    category = category or ''

            # Fetch results with JSON validation
            query = text("""
                SELECT 
                    t.ResultID, 
                    t.EntryID,
                    CASE 
                        WHEN ISJSON(t.AiJson) = 1 
                        THEN ISNULL(TRY_CAST(JSON_VALUE(t.AiJson, '$.match_score') AS FLOAT), 0)
                        ELSE 0
                    END AS match_score,
                    t.SortOrder,
                    t.ImageDesc, 
                    t.ImageSource, 
                    t.ImageUrl,
                    r.ProductBrand, 
                    r.ProductModel,
                    t.AiJson
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                WHERE r.FileID = :file_id AND t.EntryID = :entry_id
            """)
            df = pd.read_sql_query(query, conn, params={"file_id": file_id, "entry_id": entry_id})
            if df.empty:
                logger.warning(f"No eligible data found for FileID: {file_id}, EntryID: {entry_id}")
                return False

            # Log invalid JSON
            invalid_json_rows = df[df['AiJson'].notnull() & (df['AiJson'].str.strip() != '') & (df['match_score'] == 0)]
            for _, row in invalid_json_rows.iterrows():
                logger.warning(f"Invalid JSON in ResultID {row['ResultID']}: AiJson={row['AiJson'][:100]}...")

            logger.info(f"Fetched {len(df)} rows for EntryID {entry_id}")

            # Extract brand aliases from brand_rules
            brand_aliases = []
            if brand_rules and "brand_rules" in brand_rules:
                for rule in brand_rules["brand_rules"]:
                    if "names" in rule:
                        brand_clean = clean_string(brand).lower() if brand else ''
                        if any(clean_string(name).lower() == brand_clean for name in rule["names"]):
                            brand_aliases = [clean_string(name).lower() for name in rule["names"]]
                            break
            if not brand_aliases:
                # Fallback to default aliases
                brand_aliases_dict = {
                    "Scotch & Soda": [
                        "Scotch and Soda", "Scotch Soda", "Scotch&Soda", "ScotchAndSoda", "Scotch"
                    ],
                    "Adidas": ["Adidas AG", "Adidas Originals", "Addidas", "Adiddas"],
                    "BAPE": ["A Bathing Ape", "BATHING APE", "Bape Japan", "ABathingApe", "Bape"]
                }
                brand_clean = clean_string(brand).lower() if brand else ''
                for b, aliases in brand_aliases_dict.items():
                    if clean_string(b).lower() == brand_clean:
                        brand_aliases = [clean_string(b).lower()] + [clean_string(a).lower() for a in aliases]
                        break
                if not brand_aliases and brand_clean:
                    brand_aliases = [brand_clean]
            logger.debug(f"Using brand aliases: {brand_aliases}")

            # Normalize model and prepare aliases
            model_clean = normalize_model(model) if model else ''
            model_aliases = generate_aliases(model_clean) if model_clean else []

            # Clean DataFrame columns
            required_cols = ["ImageDesc", "ImageSource", "ImageUrl", "ProductBrand", "ProductModel"]
            for col in required_cols:
                if col not in df.columns:
                    logger.warning(f"Column {col} missing; adding empty column")
                    df[col] = ''
                df[f"{col}_clean"] = df[col].fillna('').apply(clean_string)

            # Filter model results (synchronous version)
            exact_df = df[df.apply(lambda row: validate_model(row, model_aliases, row['ResultID'], logger), axis=1)]
            discarded_df = df[~df.index.isin(exact_df.index)]
            logger.info(f"Filtered {len(exact_df)} exact matches and {len(discarded_df)} discarded rows for EntryID {entry_id}")

            # Calculate priorities
            df['priority'] = [calculate_priority(
                row, exact_df, model_clean, model_aliases, clean_string(brand) if brand else '', brand_aliases, logger
            ) for _, row in df.iterrows()]
            logger.info(f"Priority distribution: {df['priority'].value_counts().to_dict()}")

            # Assign default sort order
            df['new_sort_order'] = -2

            # Process matches
            match_rows = df[df['priority'].isin([1, 2, 3])]
            if not match_rows.empty:
                match_df = match_rows.sort_values(['priority', 'match_score'], ascending=[True, False])
                valid_indices = []
                for idx, row in match_df.iterrows():
                    result_id = row['ResultID']
                    if row['priority'] in [1, 2] and validate_model(row, model_aliases, result_id, logger):
                        valid_indices.append(idx)
                    elif row['priority'] == 3:
                        brand_found = validate_brand(row, brand_aliases, result_id, None, logger)
                        if brand_found:
                            valid_indices.append(idx)
                        else:
                            logger.warning(
                                f"ResultID {result_id}: Brand validation failed for aliases {brand_aliases}, "
                                f"ImageDesc: {row['ImageDesc']}, ImageSource: {row['ImageSource']}, ImageUrl: {row['ImageUrl']}"
                            )
                            df.at[idx, 'new_sort_order'] = -2
                            df.at[idx, 'priority'] = 4
                    else:
                        logger.warning(f"Model validation failed for ResultID {result_id}, checking brand")
                        brand_found = validate_brand(row, brand_aliases, result_id, None, logger)
                        df.at[idx, 'new_sort_order'] = -2 if not brand_found else None
                        df.at[idx, 'priority'] = 4 if not brand_found else 3
                        if brand_found:
                            valid_indices.append(idx)
                            logger.warning(
                                f"ResultID {result_id}: Brand validation failed for aliases {brand_aliases}, "
                                f"ImageDesc: {row['ImageDesc']}, ImageSource: {row['ImageSource']}, ImageUrl: {row['ImageUrl']}"
                            )

                valid_match_df = match_df.loc[valid_indices]
                if not valid_match_df.empty:
                    df.loc[valid_match_df.index, 'new_sort_order'] = range(1, len(valid_match_df) + 1)

            # Update SortOrder
            update_count = 0
            success_count = 0
            for _, row in df[['ResultID', 'new_sort_order']].iterrows():
                if row['new_sort_order'] == row.get('SortOrder', None) or pd.isna(row['new_sort_order']):
                    continue
                try:
                    result_id = int(row['ResultID'])
                    new_sort_order = int(row['new_sort_order'])
                    conn.execute(
                        text("""
                            UPDATE utb_ImageScraperResult 
                            SET SortOrder = :sort_order 
                            WHERE ResultID = :result_id
                        """),
                        {"sort_order": new_sort_order, "result_id": result_id}
                    )
                    update_count += 1
                    success_count += 1
                    logger.debug(f"Updated SortOrder to {new_sort_order} for ResultID {result_id}")
                except SQLAlchemyError as e:
                    logger.error(f"Failed to update ResultID {row['ResultID']}: {e}")
                    continue

            logger.info(f"Updated {success_count}/{update_count} rows for EntryID {entry_id}")

            # Verify updates
            temp_verify = conn.execute(
                text("SELECT COUNT(*) FROM utb_ImageScraperResult WHERE EntryID = :entry_id AND SortOrder IS NOT NULL"),
                {"entry_id": entry_id}
            ).scalar()
            logger.debug(f"Rows with non-NULL SortOrder after update: {temp_verify}")

            verify_query = text("""
                SELECT ResultID, EntryID, SortOrder, ImageDesc, ImageSource, ImageUrl
                FROM utb_ImageScraperResult
                WHERE EntryID = :entry_id
                ORDER BY SortOrder DESC
            """)
            results = [
                {
                    "ResultID": r[0], 
                    "EntryID": r[1], 
                    "SortOrder": r[2], 
                    "ImageDesc": r[3], 
                    "ImageSource": r[4], 
                    "ImageUrl": r[5]
                }
                for r in conn.execute(verify_query, {"entry_id": entry_id}).fetchall()
            ]

            positive_count = sum(1 for r in results if r['SortOrder'] is not None and r['SortOrder'] > 0)
            zero_count = sum(1 for r in results if r['SortOrder'] == 0)
            negative_count = sum(1 for r in results if r['SortOrder'] is not None and r['SortOrder'] < 0)
            null_count = sum(1 for r in results if r['SortOrder'] is None)
            logger.info(
                f"Verification for EntryID {entry_id}: "
                f"{positive_count} rows with positive SortOrder (model or brand matches), "
                f"{zero_count} rows with SortOrder=0 (legacy brand matches), "
                f"{negative_count} rows with negative SortOrder, "
                f"{null_count} rows with NULL SortOrder"
            )
            if null_count > 0:
                logger.warning(f"Found {null_count} rows with NULL SortOrder for EntryID {entry_id}")

            for r in results[:3]:
                logger.info(f"Sample - ResultID: {r['ResultID']}, EntryID: {r['EntryID']}, SortOrder: {r['SortOrder']}, ImageDesc: {r['ImageDesc']}")

            return success_count > 0

    except SQLAlchemyError as e:
        logger.error(f"Database error updating SortOrder for FileID {file_id}, EntryID {entry_id}: {e}", exc_info=True)
        return None
    except ValueError as e:
        logger.error(f"ValueError updating SortOrder for FileID {file_id}, EntryID {entry_id}: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Unexpected error updating SortOrder for FileID {file_id}, EntryID {entry_id}: {e}", exc_info=True)
        return None
async def update_sort_no_image_entry(file_id: str, logger: Optional[logging.Logger] = None) -> Optional[Dict]:
    logger = logger or logging.getLogger(__name__)
    try:
        file_id = int(file_id)
        logger.info(f"Starting per-entry SortOrder update for FileID: {file_id}")
        # Use async engine with async context manager
        async with async_engine.begin() as conn:  
            result = await conn.execute(
                text(
                    """
                    DELETE FROM utb_ImageScraperResult
                    WHERE EntryID IN (
                        SELECT r.EntryID
                        FROM utb_ImageScraperRecords r
                        WHERE r.FileID = :file_id
                        AND utb_ImageScraperResult.ImageUrl = 'placeholder://no-results'
                    )
                    """
                ),
                {"file_id": file_id}
            )
            rows_affected = result.rowcount
            logger.info(f"Deleted {rows_affected} entries for FileID: {file_id}")
            
            return {"file_id": file_id, "rows_affected": rows_affected}
    
    except ValueError as ve:
        logger.error(f"Invalid file_id format: {file_id}, error: {str(ve)}")
        return None
    except Exception as e:
        logger.error(f"Error updating entries for FileID: {file_id}, error: {str(e)}")
        return None


async def update_sort_order_per_entry(file_id: str, logger: Optional[logging.Logger] = None) -> Optional[Dict]:
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        logger.info(f"Starting per-entry SortOrder update for FileID: {file_id}")
        
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
                        brand_aliases_dict = {
                            "Scotch & Soda": [
                                "Scotch and Soda", "Scotch Soda", "Scotch&Soda", "ScotchAndSoda", "Scotch"
                            ],
                            "Adidas": ["Adidas AG", "Adidas Originals", "Addidas", "Adiddas"],
                            "BAPE": ["A Bathing Ape", "BATHING APE", "Bape Japan", "ABathingApe", "Bape"]
                        }
                        brand_aliases = await generate_brand_aliases(brand or '', brand_aliases_dict)
                        logger.debug(f"Brand aliases for EntryID {entry_id}, Brand '{brand}': {brand_aliases}")
                        
                        model_aliases = generate_aliases(model) if model else []
                        if model and '_' in model:
                            model_base = model.split('_')[0]
                            model_aliases.append(model_base)
                            model_aliases = list(set(model_aliases))
                        logger.debug(f"Model aliases for EntryID {entry_id}, Model '{model}': {model_aliases}")
                        
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
                        # ... (rest of the function unchanged)
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
                                    if (alias.lower() in image_desc.lower() or
                                        alias.lower() in image_source.lower() or
                                        alias.lower() in image_url.lower() or
                                        re.search(rf'{re.escape(alias)}', image_desc, re.IGNORECASE) or
                                        re.search(rf'{re.escape(alias)}', image_source, re.IGNORECASE) or
                                        re.search(rf'{re.escape(alias)}', image_url, re.IGNORECASE))
                                ]
                                if matched_brand_aliases:
                                    brand_matches.append({
                                        "ResultID": result['ResultID'],
                                        "SortOrder": result['SortOrder'],
                                        "MatchedAliases": matched_brand_aliases,
                                        "ImageUrl": result.get('ImageUrl', ''),
                                        "ImageDesc": image_desc,
                                        "ImageSource": image_source
                                    })
                                
                                matched_model_aliases = [
                                    alias for alias in model_aliases
                                    if (alias.lower() in image_desc.lower() or
                                        alias.lower() in image_source.lower() or
                                        alias.lower() in image_url.lower() or
                                        re.search(rf'\b{re.escape(alias)}\b', image_desc, re.IGNORECASE) or
                                        re.search(rf'\b{re.escape(alias)}\b', image_source, re.IGNORECASE) or
                                        re.search(rf'\b{re.escape(alias)}\b', image_url, re.IGNORECASE))
                                ]
                                if matched_model_aliases:
                                    model_matches.append({
                                        "ResultID": result['ResultID'],
                                        "SortOrder": result['SortOrder'],
                                        "MatchedAliases": matched_model_aliases,
                                        "ImageUrl": result.get('ImageUrl', ''),
                                        "ImageDesc": image_desc,
                                        "ImageSource": image_source
                                    })
                                
                                if result['SortOrder'] == -1:
                                    logger.warning(
                                        f"Unexpected SortOrder=-1 for ResultID {result['ResultID']}, "
                                        f"EntryID {entry_id}, Model '{model}', Brand '{brand}', "
                                        f"ImageDesc: {image_desc}, ImageSource: {image_source}, ImageUrl: {image_url}"
                                    )
                                    unexpected_count += 1
                            
                            if unexpected_count > 0:
                                brand_pattern = '|'.join(re.escape(alias.lower()) for alias in brand_aliases)
                                model_pattern = '|'.join(re.escape(alias.lower()) for alias in model_aliases)
                                cursor.execute(
                                    """
                                    UPDATE utb_ImageScraperResult
                                    SET SortOrder = CASE
                                        WHEN (
                                            LOWER(ImageDesc) LIKE '%' + ? + '%' OR
                                            LOWER(ImageSource) LIKE '%' + ? + '%' OR
                                            LOWER(ImageUrl) LIKE '%' + ? + '%' OR
                                            LOWER(ImageDesc) LIKE '%' + ? + '%' OR
                                            LOWER(ImageSource) LIKE '%' + ? + '%' OR
                                            LOWER(ImageUrl) LIKE '%' + ? + '%' OR
                                            LOWER(ImageDesc) REGEXP ? OR
                                            LOWER(ImageSource) REGEXP ? OR
                                            LOWER(ImageUrl) REGEXP ? OR
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
                                    (
                                        brand, brand, brand,
                                        model, model, model,
                                        brand_pattern, brand_pattern, brand_pattern,
                                        model_pattern, model_pattern, model_pattern,
                                        entry_id, entry_id
                                    )
                                )
                                corrected_count = cursor.rowcount
                                if corrected_count > 0:
                                    logger.info(f"Corrected {corrected_count} SortOrder=-1 entries for EntryID {entry_id} to positive or -2")
                                unexpected_count -= corrected_count
                                unexpected_sort_order_count += unexpected_count
                            
                            if unexpected_count > 0:
                                cursor.execute(
                                    """
                                    UPDATE utb_ImageScraperResult
                                    SET SortOrder = -2
                                    WHERE EntryID = ? AND SortOrder = -1
                                    """,
                                    (entry_id,)
                                )
                                fallback_count = cursor.rowcount
                                if fallback_count > 0:
                                    logger.info(f"Fallback corrected {fallback_count} SortOrder=-1 entries to -2 for EntryID {entry_id}")
                                unexpected_count -= fallback_count
                                unexpected_sort_order_count += unexpected_count
                            
                            cursor.execute(
                                """
                                SELECT ResultID, ImageDesc, ImageSource, ImageUrl
                                FROM utb_ImageScraperResult
                                WHERE EntryID = ? AND SortOrder = -1
                                """,
                                (entry_id,)
                            )
                            remaining_minus_one = cursor.fetchall()
                            for res in remaining_minus_one:
                                result_id, image_desc, image_source, image_url = res
                                logger.warning(
                                    f"Remaining SortOrder=-1 for ResultID {result_id}, EntryID {entry_id}, "
                                    f"Model '{model}', Brand '{brand}', "
                                    f"ImageDesc: {image_desc}, ImageSource: {image_source}, ImageUrl: {image_url}"
                                )
                                unexpected_count += 1
                                unexpected_sort_order_count += 1
                            
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
                                logger.warning(
                                    f"No brand aliases matched for EntryID {entry_id}, Brand '{brand}', "
                                    f"URLs checked: {[r.get('ImageUrl', '') for r in updates]}, "
                                    f"Descriptions checked: {[r.get('ImageDesc', '') for r in updates]}, "
                                    f"Sources checked: {[r.get('ImageSource', '') for r in updates]}"
                                )
                                brand_mismatch_count += 1
                            if not model_matches and model:
                                logger.warning(
                                    f"No model aliases matched for EntryID {entry_id}, Model '{model}', "
                                    f"URLs checked: {[r.get('ImageUrl', '') for r in updates]}, "
                                    f"Descriptions checked: {[r.get('ImageDesc', '') for r in updates]}, "
                                    f"Sources checked: {[r.get('ImageSource', '') for r in updates]}"
                                )
                                model_mismatch_count += 1
                            
                            success_count += 1
                        else:
                            failure_count += 1
                            entry_result["Status"] = "Failed"
                            entry_result["Error"] = "update_search_sort_order returned None"
                            logger.warning(f"No results for EntryID {entry_id}: update_search_sort_order returned None")
                        
                        conn.connection.commit()
                    
                    except Exception as e:
                        conn.connection.rollback()
                        logger.error(f"Transaction error for EntryID {entry_id}: {e}", exc_info=True)
                        entry_result["Status"] = "Failed"
                        entry_result["Error"] = str(e)
                        failure_count += 1
                    
                    entry_results.append(entry_result)
                
                cursor.execute(
                    """
                    SELECT COUNT(DISTINCT t.EntryID)
                    FROM utb_ImageScraperResult t
                    INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                    WHERE r.FileID = ? AND t.SortOrder > 0
                    """,
                    (file_id,)
                )
                positive_entries = cursor.fetchone()[0]
                cursor.execute(
                    """
                    SELECT COUNT(DISTINCT t.EntryID)
                    FROM utb_ImageScraperResult t
                    INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                    WHERE r.FileID = ? AND t.SortOrder = 0
                    """,
                    (file_id,)
                )
                brand_match_entries = cursor.fetchone()[0]
                cursor.execute(
                    """
                    SELECT COUNT(DISTINCT t.EntryID)
                    FROM utb_ImageScraperResult t
                    INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                    WHERE r.FileID = ? AND t.SortOrder < 0
                    """,
                    (file_id,)
                )
                no_match_entries = cursor.fetchone()[0]
                cursor.execute(
                    """
                    SELECT COUNT(DISTINCT t.EntryID)
                    FROM utb_ImageScraperResult t
                    INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                    WHERE r.FileID = ? AND t.SortOrder IS NULL
                    """,
                    (file_id,)
                )
                null_entries = cursor.fetchone()[0]
                cursor.execute(
                    """
                    SELECT COUNT(DISTINCT t.EntryID)
                    FROM utb_ImageScraperResult t
                    INNER JOIN utb_ImageScraperRecords r ON t.EntryID = r.EntryID
                    WHERE r.FileID = ? AND t.SortOrder = -1
                    """,
                    (file_id,)
                )
                unexpected_entries = cursor.fetchone()[0]
                
                logger.info(
                    f"Verification for FileID {file_id}: "
                    f"{positive_entries} entries with model or brand matches, "
                    f"{brand_match_entries} entries with legacy brand matches (SortOrder=0), "
                    f"{no_match_entries} entries with no matches, "
                    f"{null_entries} entries with NULL SortOrder, "
                    f"{unexpected_entries} entries with unexpected SortOrder=-1"
                )
                if brand_mismatch_count > 0:
                    logger.warning(f"{brand_mismatch_count} entries had no brand alias matches")
                if model_mismatch_count > 0:
                    logger.warning(f"{model_mismatch_count} entries had no model alias matches")
                if unexpected_entries > 0:
                    logger.warning(f"{unexpected_entries} entries retained SortOrder=-1")
                
                conn.connection.commit()
                
                return {
                    "FileID": file_id,
                    "TotalEntries": len(entries),
                    "SuccessfulEntries": success_count,
                    "FailedEntries": failure_count,
                    "BrandMismatchEntries": brand_mismatch_count,
                    "ModelMismatchEntries": model_mismatch_count,
                    "UnexpectedSortOrderEntries": unexpected_entries,
                    "EntryResults": entry_results,
                    "Verification": {
                        "PositiveSortOrderEntries": positive_entries,
                        "BrandMatchEntries": brand_match_entries,
                        "NoMatchEntries": no_match_entries,
                        "NullSortOrderEntries": null_entries,
                        "UnexpectedSortOrderEntries": unexpected_entries
                    }
                }
            
    except Exception as e:
            logger.error(f"Error in per-entry SortOrder update for FileID {file_id}: {e}", exc_info=True)
            return None

async def get_images_excel_db(file_id: str, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        async with aioodbc.connect(dsn=conn_str) as conn:
            async with conn.cursor() as cursor:
                query = """
                    SELECT 
                        s.ExcelRowID, 
                        r.ImageUrl, 
                        r.ImageUrlThumbnail, 
                        s.ProductBrand AS Brand, 
                        s.ProductModel AS Style, 
                        s.ProductColor AS Color, 
                        s.ProductCategory AS Category
                    FROM utb_ImageScraperFiles f
                    INNER JOIN utb_ImageScraperRecords s ON s.FileID = f.ID
                    LEFT JOIN utb_ImageScraperResult r ON r.EntryID = s.EntryID AND r.SortOrder = 1
                    WHERE f.ID = ?
                    GROUP BY 
                        s.ExcelRowID, 
                        r.ImageUrl, 
                        r.ImageUrlThumbnail, 
                        s.ProductBrand, 
                        s.ProductModel, 
                        s.ProductColor, 
                        s.ProductCategory
                    ORDER BY s.ExcelRowID
                """
                logger.debug(f"Executing query: {query} with FileID: {file_id}")
                await cursor.execute(query, (file_id,))
                rows = await cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(rows, columns=columns)
                logger.info(f"Fetched {len(df)} rows for Excel export for FileID {file_id}")
                return df
    except pyodbc.Error as e:
        logger.error(f"Database error in get_images_excel_db: {e}", exc_info=True)
        return pd.DataFrame()
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")
        return pd.DataFrame()

async def get_send_to_email(file_id: int, logger: Optional[logging.Logger] = None) -> str:
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            query = "SELECT UserEmail FROM utb_ImageScraperFiles WHERE ID = ?"
            cursor.execute(query, (file_id,))
            result = cursor.fetchone()
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

async def update_file_generate_complete(file_id: str, logger: Optional[logging.Logger] = None) -> None:
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            query = "UPDATE utb_ImageScraperFiles SET CreateFileCompleteTime = GETDATE() WHERE ID = ?"
            cursor.execute(query, (file_id,))
            conn.commit()
            logger.info(f"Marked file generation complete for FileID: {file_id}")
    except pyodbc.Error as e:
        logger.error(f"Database error in update_file_generate_complete: {e}")
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")

async def update_file_location_complete(file_id: str, file_location: str, logger: Optional[logging.Logger] = None) -> None:
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            query = "UPDATE utb_ImageScraperFiles SET FileLocationURLComplete = ? WHERE ID = ?"
            cursor.execute(query, (file_location, file_id))
            conn.commit()
            logger.info(f"Updated file location for FileID: {file_id}")
    except pyodbc.Error as e:
        logger.error(f"Database error in update_file_location_complete: {e}")
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")

async def update_initial_sort_order(file_id: str, logger: Optional[logging.Logger] = None) -> bool:
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE utb_ImageScraperResult
                SET SortOrder = 1
                WHERE EntryID IN (
                    SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?
                )
                AND SortOrder IS NULL
                """,
                (file_id,)
            )
            updated_count = cursor.rowcount
            conn.commit()
            logger.info(f"Updated initial SortOrder to 1 for {updated_count} rows for FileID: {file_id}")
            return updated_count > 0
    except pyodbc.Error as e:
        logger.error(f"Database error in update_initial_sort_order for FileID {file_id}: {e}")
        return False
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")
        return False

async def call_fetch_missing_images(file_id: str, limit: int = 1000, ai_analysis_only: bool = True, logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
    logger = logger or default_logger
    try:
        logger.info(f"Testing fetch_missing_images with FileID: {file_id}, limit: {limit}, ai_analysis_only: {ai_analysis_only}")
        result = await fetch_missing_images(file_id, limit, ai_analysis_only, logger)
        if result.empty:
            logger.info("No missing images found")
            return {"success": True, "output": [], "message": "No missing images found"}
        return {"success": True, "output": result.to_dict(orient='records'), "message": "Fetched missing images successfully"}
    except Exception as e:
        logger.error(f"Test failed for fetch_missing_images: {e}", exc_info=True)
        return {"success": False, "error": str(e), "message": "Failed to fetch missing images"}

async def call_get_images_excel_db(file_id: str, logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
    logger = logger or default_logger
    try:
        logger.info(f"Testing get_images_excel_db with FileID: {file_id}")
        result = await get_images_excel_db(file_id, logger)
        if result.empty:
            logger.info("No images found for Excel export")
            return {"success": True, "output": [], "message": "No images found for Excel export"}
        return {"success": True, "output": result.to_dict(orient='records'), "message": "Fetched Excel images successfully"}
    except Exception as e:
        logger.error(f"Test failed for get_images_excel_db: {e}", exc_info=True)
        return {"success": False, "error": str(e), "message": "Failed to fetch Excel images"}

async def call_get_send_to_email(file_id: int, logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
    logger = logger or default_logger
    try:
        logger.info(f"Testing get_send_to_email with FileID: {file_id}")
        result = await get_send_to_email(file_id, logger)
        return {"success": True, "output": result, "message": "Retrieved email successfully"}
    except Exception as e:
        logger.error(f"Test failed for get_send_to_email: {e}", exc_info=True)
        return {"success": False, "error": str(e), "message": "Failed to retrieve email"}

async def call_update_file_generate_complete(file_id: str, logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
    logger = logger or default_logger
    try:
        logger.info(f"Testing update_file_generate_complete with FileID: {file_id}")
        await update_file_generate_complete(file_id, logger)
        return {"success": True, "output": None, "message": "Updated file generate complete successfully"}
    except Exception as e:
        logger.error(f"Test failed for update_file_generate_complete: {e}", exc_info=True)
        return {"success": False, "error": str(e), "message": "Failed to update file generate complete"}

async def call_update_file_location_complete(file_id: str, file_location: str, logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
    logger = logger or default_logger
    try:
        logger.info(f"Testing update_file_location_complete with FileID: {file_id}, file_location: {file_location}")
        await update_file_location_complete(file_id, file_location, logger)
        return {"success": True, "output": None, "message": "Updated file location successfully"}
    except Exception as e:
        logger.error(f"Test failed for update_file_location_complete: {e}", exc_info=True)
        return {"success": False, "error": str(e), "message": "Failed to update file location"}