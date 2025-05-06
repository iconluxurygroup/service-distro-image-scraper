import boto3
import logging
import os
import urllib.parse
import mimetypes
from logging_config import setup_job_logger
from config import S3_CONFIG
# S3 and R2 configuration


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
        if service == 'r2':
            client = boto3.client(
                "s3",
                region_name='auto',
                endpoint_url=S3_CONFIG['r2_endpoint'],
                aws_access_key_id=S3_CONFIG['r2_access_key'],
                aws_secret_access_key=S3_CONFIG['r2_secret_key']
            )
        else:
            client = boto3.client(
                "s3",
                region_name=S3_CONFIG['region'],
                endpoint_url=S3_CONFIG['endpoint'],
                aws_access_key_id=S3_CONFIG['access_key'],
                aws_secret_access_key=S3_CONFIG['secret_key']
            )
        logger.info(f"{service.upper()} client created successfully")
        return client
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

def upload_file_to_space(file_src, save_as, is_public=True, logger=None, file_id=None):
    logger = logger or default_logger
    if logger == default_logger and file_id:
        logger, _ = setup_job_logger(job_id=file_id, console_output=True)
        logger.info(f"Setup logger for upload_file_to_space, FileID: {file_id}")
    
    result_urls = {}
    
    # Validate file existence
    if not os.path.exists(file_src):
        logger.error(f"Local file does not exist: {file_src}")
        raise FileNotFoundError(f"Local file does not exist: {file_src}")
    
    # Determine Content-Type
    content_type, _ = mimetypes.guess_type(file_src)
    if not content_type:
        content_type = 'application/octet-stream'
        logger.warning(f"Could not determine Content-Type for {file_src}")
    
    # Upload to AWS S3
    try:
        s3_client = get_s3_client(service='s3', logger=logger, file_id=file_id)
        logger.info(f"Uploading {file_src} to S3: {S3_CONFIG['bucket_name']}/{save_as}")
        s3_client.upload_file(
            file_src,
            S3_CONFIG['bucket_name'],
            save_as,
            ExtraArgs={
                'ACL': 'public-read' if is_public else 'private',
                'ContentType': content_type
            }
        )
        double_encoded_key = double_encode_plus(save_as, logger=logger)
        s3_url = f"https://{S3_CONFIG['bucket_name']}.s3.{S3_CONFIG['region']}.amazonaws.com/{double_encoded_key}"
        logger.info(f"Uploaded {file_src} to S3: {s3_url} with Content-Type: {content_type}")
        result_urls['s3'] = s3_url
    except Exception as e:
        logger.error(f"Failed to upload {file_src} to S3: {e}", exc_info=True)
        raise
    
    # Upload to Cloudflare R2
    try:
        r2_client = get_s3_client(service='r2', logger=logger, file_id=file_id)
        logger.info(f"Uploading {file_src} to R2: {S3_CONFIG['r2_bucket_name']}/{save_as}")
        r2_client.upload_file(
            file_src,
            S3_CONFIG['r2_bucket_name'],
            save_as,
            ExtraArgs={
                'ACL': 'public-read' if is_public else 'private',
                'ContentType': content_type
            }
        )
        double_encoded_key = double_encode_plus(save_as, logger=logger)
        r2_url = f"{S3_CONFIG['r2_custom_domain']}/{double_encoded_key}"
        logger.info(f"Uploaded {file_src} to R2: {r2_url} with Content-Type: {content_type}")
        result_urls['r2'] = r2_url
    except Exception as e:
        logger.error(f"Failed to upload {file_src} to R2: {e}", exc_info=True)
        # Continue to return S3 URL if R2 fails
        pass
    
    if is_public and result_urls.get('s3'):
        return result_urls['s3']
    return None