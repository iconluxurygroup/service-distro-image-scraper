import logging
import pandas as pd
import asyncio
from typing import Optional
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from database_config import async_engine

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