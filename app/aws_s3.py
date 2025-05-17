from fastapi import APIRouter, BackgroundTasks
from fastapi.responses import JSONResponse
import logging
import asyncio
import aiobotocore.session
from aiobotocore.config import AioConfig
from typing import Callable, Any, Union
import traceback
import os
import urllib.parse
import mimetypes
from logging_config import setup_job_logger
import aioodbc 
# Import database configuration
from config import VERSION,S3_CONFIG
from database_config import async_engine, engine
# Initialize default logger
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)

def get_s3_client(service='s3', logger=None, file_id=None):
    logger = logger or default_logger
    if logger == default_logger and file_id:
        logger, _ = setup_job_logger(job_id=file_id, console_output=True)
        logger.info(f"Setup logger for get_s3_client, FileID: {file_id}")
    try:
        logger.info(f"Creating {service.upper()} client")
        session = aiobotocore.session.get_session()
        config = AioConfig(signature_version='s3v4')  # Explicitly use Signature Version 4
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
import boto3
from botocore.config import Config

def get_s3_client_sync(service='s3', logger=None, file_id=None):
    logger = logger or default_logger
    if logger == default_logger and file_id:
        logger, _ = setup_job_logger(job_id=file_id, console_output=True)
        logger.info(f"Setup logger for get_s3_client_sync, FileID: {file_id}")
    try:
        logger.info(f"Creating {service.upper()} client (sync)")
        if service == 'r2':
            return boto3.client(
                "s3",
                region_name='auto',
                endpoint_url=S3_CONFIG['r2_endpoint'],
                aws_access_key_id=S3_CONFIG['r2_access_key'],
                aws_secret_access_key=S3_CONFIG['r2_secret_key'],
                config=Config()
            )
        else:
            return boto3.client(
                "s3",
                region_name=S3_CONFIG['region'],
                endpoint_url=S3_CONFIG['endpoint'],
                aws_access_key_id=S3_CONFIG['access_key'],
                aws_secret_access_key=S3_CONFIG['secret_key'],
                config=Config()
            )
    except Exception as e:
        logger.error(f"Error creating {service.upper()} client: {e}", exc_info=True)
        raise

def upload_file_to_space_sync(file_src, save_as, is_public=True, logger=None, file_id=None):
    logger = logger or default_logger
    if logger == default_logger and file_id:
        logger, _ = setup_job_logger(job_id=file_id, console_output=True)
        logger.info(f"Setup logger for upload_file_to_space_sync, FileID: {file_id}")

    result_urls = {}

    if not os.path.exists(file_src):
        logger.error(f"Local file does not exist: {file_src}")
        raise FileNotFoundError(f"Local file does not exist: {file_src}")

    content_type, _ = mimetypes.guess_type(file_src)
    if not content_type:
        content_type = 'text/plain' if file_src.endswith('.log') else 'application/octet-stream'
        logger.warning(f"Could not determine Content-Type for {file_src}, using {content_type}")

    try:
        s3_client = get_s3_client_sync(service='s3', logger=logger, file_id=file_id)
        logger.info(f"Uploading {file_src} to S3: {S3_CONFIG['bucket_name']}/{save_as}")
        with open(file_src, 'rb') as file:
            s3_client.put_object(
                Bucket=S3_CONFIG['bucket_name'],
                Key=save_as,
                Body=file,
                ACL='public-read' if is_public else 'private',
                ContentType=content_type
            )
        double_encoded_key = double_encode_plus(save_as, logger=logger)
        s3_url = f"https://{S3_CONFIG['bucket_name']}.s3.{S3_CONFIG['region']}.amazonaws.com/{double_encoded_key}"
        logger.info(f"Uploaded {file_src} to S3: {s3_url} with Content-Type: {content_type}")
        result_urls['s3'] = s3_url
    except Exception as e:
        logger.error(f"Failed to upload {file_src} to S3: {e}", exc_info=True)
        raise
    finally:
        if 's3_client' in locals():
            s3_client.close()

    try:
        r2_client = get_s3_client_sync(service='r2', logger=logger, file_id=file_id)
        logger.info(f"Uploading {file_src} to R2: {S3_CONFIG['r2_bucket_name']}/{save_as}")
        with open(file_src, 'rb') as file:
            r2_client.put_object(
                Bucket=S3_CONFIG['r2_bucket_name'],
                Key=save_as,
                Body=file,
                ACL='public-read' if is_public else 'private',
                ContentType=content_type
            )
        double_encoded_key = double_encode_plus(save_as, logger=logger)
        r2_url = f"{S3_CONFIG['r2_custom_domain']}/{double_encoded_key}"
        logger.info(f"Uploaded {file_src} to R2: {r2_url} with Content-Type: {content_type}")
        result_urls['r2'] = r2_url
    except Exception as e:
        logger.error(f"Failed to upload {file_src} to R2: {e}", exc_info=True)
        pass
    finally:
        if 'r2_client' in locals():
            r2_client.close()

    if is_public and result_urls.get('s3'):
        return result_urls['s3']
    return None
def double_encode_plus(filename, logger=None):
    logger = logger or default_logger
    logger.debug(f"Encoding filename: {filename}")
    first_pass = filename.replace('+', '%2B')
    second_pass = urllib.parse.quote(first_pass)
    logger.debug(f"Double-encoded filename: {second_pass}")
    return second_pass

async def upload_file_to_space(file_src, save_as, is_public=True, public=None, logger=None, file_id=None):
    if public is not None:
        is_public = public
        logger.warning("Use of 'public' parameter is deprecated; use 'is_public' instead")
    logger = logger or logging.getLogger(__name__)
    if logger == logging.getLogger(__name__) and file_id:
        logger, _ = setup_job_logger(job_id=file_id, console_output=True)
        logger.info(f"Setup logger for upload_file_to_space, FileID: {file_id}")

    result_urls = {}

    if not os.path.exists(file_src):
        logger.error(f"Local file does not exist: {file_src}")
        raise FileNotFoundError(f"Local file does not exist: {file_src}")

    # Check if log URL already exists
    async with aioodbc.connect(dsn=conn_str) as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT LogFileUrl FROM utb_ImageScraperFiles WHERE ID = ?", (file_id,))
            existing_url = await cursor.fetchone()
            if existing_url and existing_url[0]:
                logger.info(f"Log already uploaded for FileID {file_id}: {existing_url[0]}")
                return existing_url[0]

    content_type, _ = mimetypes.guess_type(file_src)
    if not content_type:
        content_type = 'text/plain'
        logger.debug(f"Set Content-Type to {content_type} for {file_src}")

    file_size = os.path.getsize(file_src)
    logger.info(f"Uploading file: {file_src}, size: {file_size / 1024:.2f} KB, save_as: {save_as}")

    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            async with get_session().create_client(
                "s3",
                region_name=S3_CONFIG['region'],
                endpoint_url=S3_CONFIG['endpoint'],
                aws_access_key_id=S3_CONFIG['access_key'],
                aws_secret_access_key=S3_CONFIG['secret_key'],
                config=AioConfig(signature_version='s3v4')
            ) as s3_client:
                logger.info(f"Uploading {file_src} to S3: {S3_CONFIG['bucket_name']}/{save_as}")
                with open(file_src, 'rb') as file:
                    await s3_client.put_object(
                        Bucket=S3_CONFIG['bucket_name'],
                        Key=save_as,
                        Body=file,
                        ACL='public-read' if is_public else 'private',
                        ContentType=content_type
                    )
                double_encoded_key = urllib.parse.quote(save_as.replace('+', '%2B'))
                s3_url = f"https://{S3_CONFIG['bucket_name']}.s3.{S3_CONFIG['region']}.amazonaws.com/{double_encoded_key}"
                logger.info(f"Uploaded {file_src} to S3: {s3_url} with Content-Type: {content_type}")
                result_urls['s3'] = s3_url
                break
        except Exception as e:
            if attempt < max_attempts - 1 and 'InternalError' in str(e):
                logger.warning(f"InternalError on attempt {attempt + 1}, retrying after delay...")
                await asyncio.sleep(2 ** attempt)
                continue
            logger.error(f"Failed to upload {file_src} to S3: {e}", exc_info=True)
            raise

    for attempt in range(max_attempts):
        try:
            async with get_session().create_client(
                "s3",
                region_name='auto',
                endpoint_url=S3_CONFIG['r2_endpoint'],
                aws_access_key_id=S3_CONFIG['r2_access_key'],
                aws_secret_access_key=S3_CONFIG['r2_secret_key'],
                config=AioConfig(signature_version='s3v4')
            ) as r2_client:
                logger.info(f"Uploading {file_src} to R2: {S3_CONFIG['r2_bucket_name']}/{save_as}")
                with open(file_src, 'rb') as file:
                    await r2_client.put_object(
                        Bucket=S3_CONFIG['r2_bucket_name'],
                        Key=save_as,
                        Body=file,
                        ACL='public-read' if is_public else 'private',
                        ContentType=content_type
                    )
                double_encoded_key = urllib.parse.quote(save_as.replace('+', '%2B'))
                r2_url = f"{S3_CONFIG['r2_custom_domain']}/{double_encoded_key}"
                logger.info(f"Uploaded {file_src} to R2: {r2_url} with Content-Type: {content_type}")
                result_urls['r2'] = r2_url
                break
        except Exception as e:
            if attempt < max_attempts - 1 and 'SignatureDoesNotMatch' in str(e):
                logger.warning(f"SignatureDoesNotMatch on attempt {attempt + 1}, retrying after delay...")
                await asyncio.sleep(2 ** attempt)
                continue
            logger.error(f"Failed to upload {file_src} to R2: {e}", exc_info=True)
            break

    if is_public and result_urls.get('r2'):
        await update_log_url_in_db(file_id, result_urls['r2'], logger)
        return result_urls['r2']
    return None