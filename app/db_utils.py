import logging
import pandas as pd
from typing import Optional, List
from sqlalchemy.sql import text
from database_config import async_engine
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from sqlalchemy.exc import SQLAlchemyError

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type(SQLAlchemyError),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying get_images_excel_db for FileID {retry_state.kwargs['file_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def get_images_excel_db(file_id: str, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    logger = logger or default_logger
    try:
        async with async_engine.connect() as conn:
            query = text("""
                SELECT 
                    r.ExcelRowID,
                    res.ImageUrl,
                    res.ImageUrlThumbnail,
                    r.ProductBrand AS Brand,
                    r.ProductModel AS Style,
                    r.ProductColor AS Color,
                    r.ProductCategory AS Category
                FROM utb_ImageScraperRecords r
                INNER JOIN utb_ImageScraperResult res ON r.EntryID = res.EntryID
                WHERE r.FileID = :file_id AND res.SortOrder = 1
            """)
            result = await conn.execute(query, {"file_id": file_id})
            rows = result.fetchall()
            columns = result.keys()
            result.close()

        if not rows:
            logger.warning(f"No images found for FileID {file_id} with SortOrder = 1")
            return pd.DataFrame(columns=["ExcelRowID", "ImageUrl", "ImageUrlThumbnail", "Brand", "Style", "Color", "Category"])

        df = pd.DataFrame(rows, columns=columns)
        logger.info(f"Retrieved {len(df)} image records for FileID {file_id}")
        return df
    except SQLAlchemyError as e:
        logger.error(f"Database error fetching images for FileID {file_id}: {e}", exc_info=True)
        return pd.DataFrame(columns=["ExcelRowID", "ImageUrl", "ImageUrlThumbnail", "Brand", "Style", "Color", "Category"])
    except Exception as e:
        logger.error(f"Unexpected error fetching images for FileID {file_id}: {e}", exc_info=True)
        return pd.DataFrame(columns=["ExcelRowID", "ImageUrl", "ImageUrlThumbnail", "Brand", "Style", "Color", "Category"])

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type(SQLAlchemyError),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying get_send_to_email for FileID {retry_state.kwargs['file_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def get_send_to_email(file_id: int, logger: Optional[logging.Logger] = None) -> List[str]:
    logger = logger or default_logger
    try:
        async with async_engine.connect() as conn:
            result = await conn.execute(
                text("SELECT UserEmail FROM utb_ImageScraperFiles WHERE ID = :file_id"),
                {"file_id": file_id}
            )
            row = result.fetchone()
            result.close()
            if row and row[0]:
                emails = [email.strip() for email in row[0].split(',') if email.strip()]
                logger.info(f"Retrieved email(s) for FileID {file_id}: {emails}")
                return emails
            logger.warning(f"No email found for FileID {file_id}")
            return []
    except SQLAlchemyError as e:
        logger.error(f"Database error retrieving email for FileID {file_id}: {e}", exc_info=True)
        return []
    except Exception as e:
        logger.error(f"Unexpected error retrieving email for FileID {file_id}: {e}", exc_info=True)
        return []

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type(SQLAlchemyError),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying update_file_location_complete for FileID {retry_state.kwargs['file_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def update_file_location_complete(file_id: str, file_location: str, logger: Optional[logging.Logger] = None):
    logger = logger or default_logger
    try:
        async with async_engine.begin() as conn:
            await conn.execute(
                text("""
                    UPDATE utb_ImageScraperFiles
                    SET FileLocationURLComplete = :file_location, CreateFileCompleteTime = GETDATE()
                    WHERE ID = :file_id
                """),
                {"file_id": file_id, "file_location": file_location}
            )
        logger.info(f"Updated file location for FileID: {file_id}")
    except SQLAlchemyError as e:
        logger.error(f"Database error updating file location for FileID {file_id}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error updating file location for FileID {file_id}: {e}", exc_info=True)
        raise

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type(SQLAlchemyError),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying update_file_generate_complete for FileID {retry_state.kwargs['file_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def update_file_generate_complete(file_id: str, logger: Optional[logging.Logger] = None):
    logger = logger or default_logger
    try:
        async with async_engine.begin() as conn:
            await conn.execute(
                text("""
                    UPDATE utb_ImageScraperFiles
                    SET CreateFileCompleteTime = GETDATE()
                    WHERE ID = :file_id
                """),
                {"file_id": file_id}
            )
        logger.info(f"Marked file generation complete for FileID: {file_id}")
    except SQLAlchemyError as e:
        logger.error(f"Database error marking file generation complete for FileID {file_id}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error marking file generation complete for FileID {file_id}: {e}", exc_info=True)
        raise

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type(SQLAlchemyError),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying fetch_last_valid_entry for FileID {retry_state.kwargs['file_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def fetch_last_valid_entry(file_id: str, logger: Optional[logging.Logger] = None) -> Optional[int]:
    logger = logger or default_logger
    try:
        async with async_engine.connect() as conn:
            result = await conn.execute(
                text("""
                    SELECT MAX(EntryID)
                    FROM utb_ImageScraperRecords
                    WHERE FileID = :file_id AND CompleteTime IS NOT NULL
                """),
                {"file_id": file_id}
            )
            row = result.fetchone()
            result.close()
            if row and row[0]:
                logger.info(f"Last valid EntryID for FileID {file_id}: {row[0]}")
                return row[0]
            logger.warning(f"No valid entries found for FileID {file_id}")
            return None
    except SQLAlchemyError as e:
        logger.error(f"Database error fetching last valid entry for FileID {file_id}: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching last valid entry for FileID {file_id}: {e}", exc_info=True)
        return None

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type(SQLAlchemyError),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying update_log_url_in_db for FileID {retry_state.kwargs['file_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def update_log_url_in_db(file_id: str, log_url: str, logger: Optional[logging.Logger] = None):
    logger = logger or default_logger
    try:
        async with async_engine.begin() as conn:
            await conn.execute(
                text("""
                    UPDATE utb_ImageScraperFiles
                    SET LogFileUrl = :log_url
                    WHERE ID = :file_id
                """),
                {"file_id": file_id, "log_url": log_url}
            )
        logger.info(f"Updated log URL '{log_url}' for FileID {file_id}")
    except SQLAlchemyError as e:
        logger.error(f"Database error updating log URL for FileID {file_id}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error updating log URL for FileID {file_id}: {e}", exc_info=True)
        raise