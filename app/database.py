import logging,re
import pandas as pd
import pyodbc
from typing import List, Optional, Dict, Any
from config import conn_str,BRAND_RULES_URL,engine
from utils import clean_string, validate_model, generate_aliases, filter_model_results, calculate_priority

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

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

    # Validate data
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
        with engine.begin() as conn:  # Use SQLAlchemy transaction
            inserted_count = 0
            updated_count = 0
            for index, row in df.iterrows():
                try:
                    # Try updating existing row
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

                    # If not updated, insert new row
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


async def update_sort_order(file_id: str, logger: Optional[logging.Logger] = None) -> Optional[List[Dict]]:
    """Update sort order for all entries in a file using update_search_sort_order."""
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        logger.info(f"🔄 Starting batch SortOrder update for FileID: {file_id}")
        
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT EntryID, ProductBrand, ProductModel, ProductColor, ProductCategory 
                FROM utb_ImageScraperRecords 
                WHERE FileID = ?
                """, 
                (file_id,)
            )
            entries = cursor.fetchall()
        
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
        
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
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
            
            logger.info(f"Verification for FileID {file_id}: "
                       f"{positive_entries} entries with model matches, "
                       f"{brand_match_entries} entries with brand matches only, "
                       f"{no_match_entries} entries with no matches")
        
        return results
    except Exception as e:
        logger.error(f"Error in batch SortOrder update for FileID {file_id}: {e}", exc_info=True)
        return None

async def update_log_url_in_db(file_id: str, log_url: str, logger: Optional[logging.Logger] = None) -> bool:
    """Update the log URL for a file in the database."""
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
    """Retrieve records to search for a given file ID."""
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        query = """
            SELECT EntryID, ProductModel AS SearchString, 'model_only' AS SearchType, FileID
            FROM utb_ImageScraperRecords 
            WHERE FileID = ? AND Step1 IS NULL
            ORDER BY EntryID, SearchType
        """
        with pyodbc.connect(conn_str) as conn:
            df = pd.read_sql_query(query, conn, params=[file_id])
            if not df.empty and (df["FileID"] != file_id).any():
                logger.error(f"Found rows with incorrect FileID for {file_id}")
                df = df[df["FileID"] == file_id]
            logger.info(f"Got {len(df)} search records for FileID: {file_id}")
            return df[["EntryID", "SearchString", "SearchType"]]
    except pyodbc.Error as e:
        logger.error(f"Database error getting records for FileID {file_id}: {e}")
        return pd.DataFrame()
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")
        return pd.DataFrame()

async def fetch_missing_images(file_id: str, limit: int = 1000, ai_analysis_only: bool = True, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """Fetch images missing AI analysis or not yet scraped for a file."""
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        logger.info(f"Starting fetch_missing_images for FileID {file_id}, ai_analysis_only={ai_analysis_only}, limit={limit}")
        with pyodbc.connect(conn_str) as conn:
            if ai_analysis_only:
                query = """
                    SELECT t.ResultID, t.EntryID, t.ImageUrl, t.ImageUrlThumbnail,
                           r.ProductBrand, r.ProductCategory, r.ProductColor
                    FROM utb_ImageScraperResult t
                    INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                    WHERE r.FileID = ?
                    AND (t.AiJson IS NULL OR t.AiJson = '' OR ISJSON(t.AiJson) = 0)
                    AND t.ImageUrl IS NOT NULL AND t.ImageUrl <> ''
                    AND t.SortOrder > 0
                    ORDER BY t.ResultID
                    OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
                """
                params = (file_id, limit)
            else:
                query = """
                    SELECT r.EntryID, r.FileID, r.ProductBrand, r.ProductCategory, r.ProductColor,
                           gradio t.ResultID, t.ImageUrl, t.ImageUrlThumbnail
                    FROM utb_ImageScraperRecords r
                    LEFT JOIN utb_ImageScraperResult t ON r.EntryID = t.EntryID AND t.SortOrder >= 0
                    WHERE r.FileID = ? AND t.ResultID IS NULL
                    ORDER BY r.EntryID
                    OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
                """
                params = (file_id, limit)

            df = pd.read_sql_query(query, conn, params=params)
            logger.info(f"Fetched {len(df)} images for FileID {file_id}, ai_analysis_only={ai_analysis_only}")
            if not df.empty:
                logger.debug(f"Sample results: {df.head(2).to_dict()}")
            return df
    except pyodbc.Error as e:
        logger.error(f"Database error fetching missing images for FileID {file_id}: {e}", exc_info=True)
        return pd.DataFrame()
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")
        return pd.DataFrame()

async def get_endpoint(logger: Optional[logging.Logger] = None) -> str:
    """Retrieve a random non-blocked endpoint from the database."""
    logger = logger or default_logger
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT TOP 1 EndpointURL FROM utb_Endpoints WHERE EndpointIsBlocked = 0 ORDER BY NEWID()")
            result = cursor.fetchone()
            endpoint = result[0] if result else "No EndpointURL"
            logger.info(f"Retrieved endpoint: {endpoint}")
            return endpoint
    except pyodbc.Error as e:
        logger.error(f"Database error getting endpoint: {e}")
        return "No EndpointURL"

async def remove_endpoint(endpoint: str, logger: Optional[logging.Logger] = None) -> None:
    """Mark an endpoint as blocked in the database."""
    logger = logger or default_logger
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE utb_Endpoints SET EndpointIsBlocked = 1 WHERE EndpointURL = ?", (endpoint,))
            conn.commit()
            logger.info(f"Marked endpoint as blocked: {endpoint}")
    except pyodbc.Error as e:
        logger.error(f"Error marking endpoint as blocked: {e}")

async def update_sort_order_per_entry(file_id: str, logger: Optional[logging.Logger] = None) -> Optional[Dict]:
    """
    Update SortOrder for each entry associated with the given FileID individually.
    
    Args:
        file_id (str): The ID of the file to process.
        logger (Optional[logging.Logger]): Logger instance for logging messages.
    
    Returns:
        dict: Results including per-entry status, updated SortOrder counts, and verification details.
        None: If a critical error occurs.
    """
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        logger.info(f"Starting per-entry SortOrder update for FileID: {file_id}")
        
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT EntryID, ProductBrand, ProductModel, ProductColor, ProductCategory 
                FROM utb_ImageScraperRecords 
                WHERE FileID = ?
                """,
                (file_id,)
            )
            entries = cursor.fetchall()
        
        if not entries:
            logger.warning(f"No entries found for FileID: {file_id}")
            return {
                "FileID": file_id,
                "TotalEntries": 0,
                "SuccessfulEntries": 0,
                "FailedEntries": 0,
                "EntryResults": [],
                "Verification": {
                    "PositiveSortOrderEntries": 0,
                    "BrandMatchEntries": 0,
                    "NoMatchEntries": 0,
                    "NullSortOrderEntries": 0
                }
            }
        
        entry_results = []
        success_count = 0
        failure_count = 0
        
        for entry in entries:
            entry_id, brand, model, color, category = entry
            entry_result = {
                "EntryID": entry_id,
                "ProductModel": model,
                "Status": "Success",
                "Details": {},
                "Error": None
            }
            try:
                updates = await update_search_sort_order(
                    file_id=str(file_id),
                    entry_id=str(entry_id),
                    brand=brand,
                    model=model,
                    color=color,
                    category=category,
                    logger=logger
                )
                
                if updates is not None:
                    positive_count = sum(1 for r in updates if r['SortOrder'] is not None and r['SortOrder'] > 0)
                    zero_count = sum(1 for r in updates if r['SortOrder'] == 0)
                    negative_count = sum(1 for r in updates if r['SortOrder'] is not None and r['SortOrder'] < 0)
                    null_count = sum(1 for r in updates if r['SortOrder'] is None)
                    entry_result["Details"] = {
                        "UpdatedRows": len(updates),
                        "PositiveSortOrder": positive_count,
                        "ZeroSortOrder": zero_count,
                        "NegativeSortOrder": negative_count,
                        "NullSortOrder": null_count,
                        "Records": updates
                    }
                    success_count += 1
                else:
                    failure_count += 1
                    entry_result["Status"] = "Failed"
                    entry_result["Error"] = "update_search_sort_order returned None"
                    logger.warning(f"No results for EntryID {entry_id}: update_search_sort_order returned None")
            except Exception as e:
                failure_count += 1
                entry_result["Status"] = "Failed"
                entry_result["Error"] = str(e)
                logger.error(f"Error processing EntryID {entry_id}: {e}", exc_info=True)
            
            entry_results.append(entry_result)
        
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
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
            
            logger.info(
                f"Verification for FileID {file_id}: "
                f"{positive_entries} entries with model matches, "
                f"{brand_match_entries} entries with brand matches only, "
                f"{no_match_entries} entries with no matches, "
                f"{null_entries} entries with NULL SortOrder"
            )
        
        return {
            "FileID": file_id,
            "TotalEntries": len(entries),
            "SuccessfulEntries": success_count,
            "FailedEntries": failure_count,
            "EntryResults": entry_results,
            "Verification": {
                "PositiveSortOrderEntries": positive_entries,
                "BrandMatchEntries": brand_match_entries,
                "NoMatchEntries": no_match_entries,
                "NullSortOrderEntries": null_entries
            }
        }
    
    except Exception as e:
        logger.error(f"Error in per-entry SortOrder update for FileID {file_id}: {e}", exc_info=True)
        return None
async def update_search_sort_order(
    file_id: str,
    entry_id: str,
    brand: Optional[str] = None,
    model: Optional[str] = None,
    color: Optional[str] = None,
    category: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
    brand_rules: Optional[Dict] = None
) -> Optional[List[Dict]]:
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        entry_id = int(entry_id)
        logger.info(f"🔄 Starting SortOrder update for FileID: {file_id}, EntryID: {entry_id}")
        
        with engine.begin() as conn:
            conn.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))

            # Reset SortOrder to NULL for all rows
            conn.execute(
                text("""
                    UPDATE utb_ImageScraperResult
                    SET SortOrder = NULL
                    WHERE EntryID = :entry_id
                """),
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

            # Fetch results
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
            df = pd.read_sql_query(query, conn, params={"file_id": file_id, "entry_id": entry_id})
            if df.empty:
                logger.warning(f"No eligible data found for FileID: {file_id}, EntryID: {entry_id}")
                return []

            logger.info(f"Fetched {len(df)} rows for EntryID {entry_id}")
            logger.debug(f"DataFrame columns after fetch: {df.columns.tolist()}")

            # Define brand aliases
            brand_aliases_dict = {
                "Scotch & Soda": ["Scotch and Soda", "Scotch Soda", "Scotch&Soda", "ScotchAndSoda", "Scotch", "S&S"],
                "Adidas": ["Adidas AG", "Adidas Originals"],
                "BAPE": ["A Bathing Ape", "BATHING APE"]
            }
            
            # Clean data
            brand_clean = clean_string(brand) if brand else ''
            model_clean = clean_string(model) if model else ''
            model_aliases = generate_aliases(model) if model else []
            brand_aliases = []
            for b, aliases in brand_aliases_dict.items():
                if clean_string(b) == brand_clean:
                    brand_aliases = [clean_string(b)] + [clean_string(a) for a in aliases]
                    break
            if not brand_aliases and brand_clean:
                brand_aliases = [brand_clean]

            # Process column cleaning
            required_cols = ["ImageDesc", "ImageSource", "ImageUrl", "ProductBrand", "ProductModel"]
            for col in required_cols:
                if col not in df.columns:
                    logger.warning(f"Column {col} missing; adding empty column")
                    df[col] = ''
                df[f"{col}_clean"] = df[col].fillna('').apply(clean_string)

            # Filter exact matches
            exact_df, discarded_df = await filter_model_results(df, debug=True, logger=logger)
            logger.info(f"Filtered {len(exact_df)} exact matches and {len(discarded_df)} discarded rows for EntryID {entry_id}")

            # Apply priority
            df['priority'] = df.apply(
                lambda row: calculate_priority(
                    row, exact_df, model_clean, model_aliases, brand_clean, brand_aliases, logger
                ), axis=1
            )
            logger.info(f"Priority distribution: {df['priority'].value_counts().to_dict()}")

            # Assign SortOrder
            df['new_sort_order'] = -2  # Default for no match (priority 4)
            logger.debug(f"Assigned default new_sort_order: {df[['ResultID', 'new_sort_order']].to_dict(orient='records')}")

            # Set brand matches to 0 (priority 3)
            brand_match_rows = df[df['priority'] == 3]
            if not brand_match_rows.empty:
                df.loc[brand_match_rows.index, 'new_sort_order'] = 0
                logger.debug(f"Set new_sort_order=0 for brand matches: {brand_match_rows[['ResultID', 'new_sort_order']].to_dict(orient='records')}")

            # Set model matches to positive, sequential values (priority 1 or 2)
            model_match_rows = df[df['priority'].isin([1, 2])]
            if not model_match_rows.empty:
                model_match_df = model_match_rows.sort_values('match_score', ascending=False)
                # Validate model matches before assigning SortOrder
                valid_model_indices = []
                for idx, row in model_match_df.iterrows():
                    result_id = row['ResultID']
                    if validate_model(row, model_aliases, result_id, logger):
                        valid_model_indices.append(idx)
                    else:
                        logger.warning(f"Validation failed for ResultID {result_id}: Model match not verified, checking brand")
                        brand_found = False
                        for alias in brand_aliases:
                            alias_lower = clean_string(alias).lower()
                            if (alias_lower in clean_string(row.get('ImageDesc', '')).lower() or
                                alias_lower in clean_string(row.get('ImageSource', '')).lower() or
                                alias_lower in clean_string(row.get('ImageUrl', '')).lower()):
                                brand_found = True
                                break
                        df.at[idx, 'new_sort_order'] = 0 if brand_found else -2
                        df.at[idx, 'priority'] = 3 if brand_found else 4
                        logger.debug(f"Downgraded ResultID {result_id} to new_sort_order={df.at[idx, 'new_sort_order']}")

                # Assign sequential SortOrder to valid model matches
                valid_model_df = model_match_df.loc[valid_model_indices]
                if not valid_model_df.empty:
                    df.loc[valid_model_df.index, 'new_sort_order'] = range(1, len(valid_model_df) + 1)
                    logger.debug(f"Set new_sort_order for model matches: {valid_model_df[['ResultID', 'new_sort_order']].to_dict(orient='records')}")

            # Update database
            update_count = 0
            success_count = 0
            for _, row in df[['ResultID', 'new_sort_order']].iterrows():
                if row['new_sort_order'] == row.get('SortOrder', None):
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

            # Verify updates within transaction
            temp_verify = conn.execute(
                text("SELECT COUNT(*) FROM utb_ImageScraperResult WHERE EntryID = :entry_id AND SortOrder IS NOT NULL"),
                {"entry_id": entry_id}
            ).scalar()
            logger.debug(f"Rows with non-NULL SortOrder after update: {temp_verify}")

            # Final verification
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
                f"{positive_count} rows with positive SortOrder, "
                f"{zero_count} rows with SortOrder=0, "
                f"{negative_count} rows with negative SortOrder, "
                f"{null_count} rows with NULL SortOrder"
            )
            if null_count > 0:
                logger.warning(f"Found {null_count} rows with NULL SortOrder for EntryID {entry_id}")

            # Log sample results for debugging
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
async def set_sort_order_negative_four_for_zero_match(file_id: str, logger: Optional[logging.Logger] = None) -> Optional[List[Dict]]:
    """Set SortOrder to -4 for records with match_score = 0."""
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            logger.info(f"🔄 Setting SortOrder to -4 for match_score = 0 for FileID: {file_id}")
            
            query = """
                SELECT 
                    t.ResultID, 
                    t.EntryID,
                    CASE 
                        WHEN ISJSON(t.AiJson) = 1 
                        THEN ISNULL(TRY_CAST(JSON_VALUE(t.AiJson, '$.match_score') AS FLOAT), 0)
                        ELSE 0 
                    END AS match_score,
                    t.SortOrder,
                    t.AiJson
                FROM utb_ImageScraperResult t
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID
                WHERE r.FileID = ?
            """
            df = pd.read_sql_query(query, conn, params=(file_id,))
            
            if df.empty:
                logger.info(f"No records found for FileID: {file_id}")
                return []

            zero_match_df = df[df['match_score'] == 0].copy()
            invalid_json_df = df[df['AiJson'].notnull() & (df['AiJson'].str.strip() != '') & (df['match_score'] == 0)]
            for _, row in invalid_json_df.iterrows():
                if row['AiJson'] and not pd.isna(row['AiJson']):
                    logger.warning(f"Record with potentially malformed JSON - ResultID: {row['ResultID']}, AiJson: {row['AiJson'][:100]}...")

            if zero_match_df.empty:
                logger.info(f"No records found with match_score = 0 for FileID: {file_id}")
                return []

            updates = []
            for _, row in zero_match_df.iterrows():
                if row["SortOrder"] != -4:
                    updates.append((-4, int(row["ResultID"])))

            if updates:
                cursor.executemany(
                    "UPDATE utb_ImageScraperResult SET SortOrder = ? WHERE ResultID = ?",
                    updates
                )
                conn.commit()
                logger.info(f"Updated SortOrder to -4 for {len(updates)} records with match_score = 0 for FileID: {file_id}")
            else:
                logger.info(f"No SortOrder updates needed for FileID {file_id} (all already -4)")

            cursor.execute(
                """
                SELECT ResultID, EntryID, SortOrder
                FROM utb_ImageScraperResult
                WHERE EntryID IN (SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?)
                AND (
                    (ISJSON(AiJson) = 1 AND ISNULL(TRY_CAST(JSON_VALUE(AiJson, '$.match_score') AS FLOAT), 0) = 0)
                    OR (ISJSON(AiJson) = 0 AND AiJson IS NOT NULL)
                )
                """, (file_id,)
            )
            results = [{"ResultID": r[0], "EntryID": r[1], "SortOrder": r[2]} for r in cursor.fetchall()]
            return results if results else []

    except pyodbc.Error as e:
        logger.error(f"Critical error setting SortOrder to -4 for match_score = 0 for FileID {file_id}: {e}", exc_info=True)
        return None
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")
        return None

async def get_images_excel_db(file_id: str, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """Fetch images for Excel export for a given file ID."""
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with pyodbc.connect(conn_str) as conn:
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
                INNER JOIN utb_ImageScraperResult r ON r.EntryID = s.EntryID
                WHERE f.ID = ? AND r.SortOrder = 1
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
            df = pd.read_sql_query(query, conn, params=(file_id,))
            logger.info(f"Fetched {len(df)} images for Excel export for FileID {file_id}")
            return df
    except pyodbc.Error as e:
        logger.error(f"Database error in get_images_excel_db: {e}")
        return pd.DataFrame()
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")
        return pd.DataFrame()

async def get_send_to_email(file_id: int, logger: Optional[logging.Logger] = None) -> str:
    """Retrieve the email address associated with a file ID."""
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
    """Mark file generation as complete in the database."""
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
    """Update the file location URL in the database."""
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
    """Set initial SortOrder to 1 for new results."""
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
    """Wrapper to test fetch_missing_images."""
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
    """Wrapper to test get_images_excel_db."""
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
    """Wrapper to test get_send_to_email."""
    logger = logger or default_logger
    try:
        logger.info(f"Testing get_send_to_email with FileID: {file_id}")
        result = await get_send_to_email(file_id, logger)
        return {"success": True, "output": result, "message": "Retrieved email successfully"}
    except Exception as e:
        logger.error(f"Test failed for get_send_to_email: {e}", exc_info=True)
        return {"success": False, "error": str(e), "message": "Failed to retrieve email"}

async def call_update_file_generate_complete(file_id: str, logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
    """Wrapper to test update_file_generate_complete."""
    logger = logger or default_logger
    try:
        logger.info(f"Testing update_file_generate_complete with FileID: {file_id}")
        await update_file_generate_complete(file_id, logger)
        return {"success": True, "output": None, "message": "Updated file generate complete successfully"}
    except Exception as e:
        logger.error(f"Test failed for update_file_generate_complete: {e}", exc_info=True)
        return {"success": False, "error": str(e), "message": "Failed to update file generate complete"}

async def call_update_file_location_complete(file_id: str, file_location: str, logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
    """Wrapper to test update_file_location_complete."""
    logger = logger or default_logger
    try:
        logger.info(f"Testing update_file_location_complete with FileID: {file_id}, file_location: {file_location}")
        await update_file_location_complete(file_id, file_location, logger)
        return {"success": True, "output": None, "message": "Updated file location successfully"}
    except Exception as e:
        logger.error(f"Test failed for update_file_location_complete: {e}", exc_info=True)
        return {"success": False, "error": str(e), "message": "Failed to update file location"}