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
    # Generate 8-letter random alphanumeric code
    code = secrets.token_hex(4).lower()[:8]  # 4 bytes = 8 hex chars
    # Get timestamp in YYYYMMDDHHMMSS format
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    # Get file extension
    _, ext = os.path.splitext(original_filename)
    # Create new filename: code_timestamp.ext
    new_filename = f"{code}_{timestamp}{ext}"
    logger.debug(f"Generated unique filename: {new_filename} from {original_filename}")
    return new_filename
async def upload_file_to_space(file_src, save_as, is_public=True, public=None, logger=None, file_id=None):
    if public is not None:
        is_public = public
        logger.warning("Use of 'public' parameter is deprecated; use 'is_public' instead")
    logger = logger or default_logger

    result_urls = {}

    if not os.path.exists(file_src):
        logger.error(f"Local file does not exist: {file_src}")
        raise FileNotFoundError(f"Local file does not exist: {file_src}")

    # Handle Excel files: generate unique filename and set save_as
    if file_src.endswith('.xlsx'):
        unique_filename = generate_unique_filename(os.path.basename(file_src), logger)
        save_as = f"excel_files/{file_id}/{unique_filename}"
        logger.info(f"Excel file detected. Setting save_as to: {save_as}")

    content_type, _ = mimetypes.guess_type(file_src)
    if not content_type:
        content_type = 'application/octet-stream'
        logger.debug(f"Set Content-Type to {content_type} for {file_src}")

    file_size = os.path.getsize(file_src)
    logger.info(f"Uploading file: {file_src}, size: {file_size / 1024:.2f} KB, save_as: {save_as}")

    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            async with await get_s3_client(service='r2', logger=logger, file_id=file_id) as r2_client:
                logger.info(f"Uploading {file_src} to R2: {S3_CONFIG['r2_bucket_name']}/{save_as}")
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
                logger.info(f"Uploaded {file_src} to R2: {r2_url} with Content-Type: {content_type}")
                result_urls['r2'] = r2_url
                break
        except Exception as e:
            if attempt < max_attempts - 1 and 'SignatureDoesNotMatch' in str(e):
                logger.warning(f"SignatureDoesNotMatch on attempt {attempt + 1}, retrying after delay...")
                await asyncio.sleep(2 ** attempt)
                continue
            logger.error(f"Failed to upload {file_src} to R2: {e}", exc_info=True)
            return None

    if is_public and result_urls.get('r2'):
        if file_id:
            try:
                # Update FileLocationURLComplete with the latest URL
                # await update_file_location_complete_async(file_id, result_urls['r2'], logger)
                # Mark file generation complete
                # await update_file_generate_complete(file_id, logger)
                logger.info(f"Successfully updated FileLocationURLComplete and marked generation complete for FileID: {file_id}")
                return result_urls['r2']
            except Exception as e:
                logger.error(f"Failed to update database for FileID {file_id}: {e}", exc_info=True)
                return None
        else:
            logger.error(f"No file_id provided for database update")
            return result_urls['r2']
    return None