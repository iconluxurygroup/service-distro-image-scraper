import logging
import os
import asyncio
import urllib.parse
import mimetypes
import secrets
import datetime
from typing import Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from sqlalchemy.exc import SQLAlchemyError
import aiobotocore.session
from aiobotocore.config import AioConfig
from config import S3_CONFIG
from sqlalchemy.sql import text
from database_config import async_engine, conn_str
import pyodbc

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

async def get_s3_client(service='s3', logger=None, file_id=None):
    logger = logger or default_logger
    try:
        logger.info(f"Creating {service.upper()} client")
        session = aiobotocore.session.get_session()
        config = AioConfig(signature_version='s3v4')
        if service == 'r2':
            logger.debug(f"R2 config: endpoint={S3_CONFIG['r2_endpoint']}, access_key={S3_CONFIG['r2_access_key'][:4]}...")
            return session.create_client(
                "s3",
                region_name='auto',
                endpoint_url=S3_CONFIG['r2_endpoint'],
                aws_access_key_id=S3_CONFIG['r2_access_key'],
                aws_secret_access_key=S3_CONFIG['r2_secret_key'],
                config=config
            )
        else:
            return session.create_client(
                "s3",
                region_name=S3_CONFIG['region'],
                endpoint_url=S3_CONFIG['endpoint'],
                aws_access_key_id=S3_CONFIG['access_key'],
                aws_secret_access_key=S3_CONFIG['secret_key'],
                config=config
            )
    except Exception as e:
        logger.error(f"Error creating {service.upper()} client: {e}", exc_info=True)
        raise

def double_encode_plus(filename, logger=None):
    logger = logger or default_logger
    logger.debug(f"Encoding filename: {filename}")
    first_pass = filename.replace('+', '%2B')
    second_pass = urllib.parse.quote(first_pass)
    logger.debug(f"Double-encoded filename: {second_pass}")
    return second_pass

def generate_unique_filename(original_filename, logger=None):
    logger = logger or default_logger
    code = secrets.token_hex(4).lower()[:8]
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    _, ext = os.path.splitext(original_filename)
    new_filename = f"{code}_{timestamp}{ext}"
    logger.debug(f"Generated unique filename: {new_filename} from {original_filename}")
    return new_filename

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(SQLAlchemyError),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying database update for FileID {retry_state.kwargs['file_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def update_file_location_complete_async(file_id: str, file_location: str, logger: Optional[logging.Logger] = None) -> None:
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        async with async_engine.connect() as conn:
            await conn.execute(
                text("UPDATE utb_ImageScraperFiles SET FileLocationURLComplete = :url WHERE ID = :file_id"),
                {"url": file_location, "file_id": file_id}
            )
            await conn.commit()
            logger.info(f"Updated FileLocationURLComplete for FileID: {file_id} with result file URL: {file_location}")
    except SQLAlchemyError as e:
        logger.error(f"Database error in update_file_location_complete_async: {e}", exc_info=True)
        raise
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")
        raise

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(pyodbc.Error),
    before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
        f"Retrying update_file_generate_complete for FileID {retry_state.kwargs['file_id']} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
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
        logger.error(f"Database error in update_file_generate_complete: {e}", exc_info=True)
        raise
    except ValueError as e:
        logger.error(f"Invalid file_id format: {e}")
        raise

async def upload_file_to_space(file_src, save_as, is_public=True, public=None, logger=None, file_id=None):
    logger = logger or default_logger
    if public is not None:
        is_public = public
        logger.warning("Deprecated 'public' parameter used; use 'is_public' instead")

    if not os.path.exists(file_src):
        logger.error(f"Local file does not exist: {file_src}")
        raise FileNotFoundError(f"Local file does not exist: {file_src}")

    # Warn if save_as suggests a log file path
    if 'job_logs' in save_as.lower():
        logger.warning(f"Potential log file path detected in save_as: {save_as}. Ensure this is not a result file.")

    # Determine file type for logging
    file_type = 'result'
    if file_src.endswith(('.xlsx', '.json')):
        file_type = 'result (Excel/JSON)'
    logger.info(f"Processing {file_type} file: {file_src}")

    # Handle Excel files: generate unique filename and set save_as
    if file_src.endswith('.xlsx'):
        unique_filename = generate_unique_filename(os.path.basename(file_src), logger)
        save_as = f"excel_files/{file_id}/{unique_filename}"
        logger.info(f"Excel {file_type} detected. Setting save_as to: {save_as}")

    content_type, _ = mimetypes.guess_type(file_src)
    if not content_type:
        content_type = 'application/octet-stream'
        logger.debug(f"Set Content-Type to {content_type} for {file_src}")

    file_size = os.path.getsize(file_src)
    logger.info(f"Uploading {file_type} file: {file_src}, size: {file_size / 1024:.2f} KB, save_as: {save_as}")

    result_urls = {}
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            async with await get_s3_client(service='r2', logger=logger, file_id=file_id) as r2_client:
                logger.info(f"Uploading {file_type} file {file_src} to R2: {S3_CONFIG['r2_bucket_name']}/{save_as}")
                with open(file_src, 'rb') as file:
                    await r2_client.put_object(
                        Bucket=S3_CONFIG['r2_bucket_name'],
                        Key=save_as,
                        Body=file,
                        ACL='public-read' if is_public else 'private',
                        ContentType=content_type
                    )
                double_encoded_key = double_encode_plus(save_as, logger=logger)
                r2_url = f"{S3_CONFIG['r2_custom_domain']}/{double_encoded_key}"
                if len(r2_url) > 255:
                    logger.error(f"R2 URL length exceeds 255 characters for FileID {file_id}: {len(r2_url)}")
                    return None
                logger.info(f"Uploaded {file_type} file {file_src} to R2: {r2_url} with Content-Type: {content_type}")
                result_urls['r2'] = r2_url
                break
        except Exception as e:
            if attempt < max_attempts - 1 and 'SignatureDoesNotMatch' in str(e):
                logger.warning(f"SignatureDoesNotMatch on attempt {attempt + 1} for {file_type} file, retrying...")
                await asyncio.sleep(2 ** attempt)
                continue
            logger.error(f"Failed to upload {file_type} file {file_src} to R2: {e}", exc_info=True)
            return None

    if is_public and result_urls.get('r2'):
        if file_id:
            try:
                await update_file_location_complete_async(file_id, result_urls['r2'], logger)
                await update_file_generate_complete(file_id, logger)
                logger.info(f"Updated database for {file_type} file with FileID: {file_id}")
                return result_urls['r2']
            except Exception as e:
                logger.error(f"Failed to update database for {file_type} file with FileID {file_id}: {e}", exc_info=True)
                return None
        else:
            logger.error(f"No file_id provided for database update of {file_type} file")
            return result_urls['r2']
    return None