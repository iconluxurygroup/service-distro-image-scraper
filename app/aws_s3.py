# aws_s3.py
import boto3
import logging
import uuid
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, REGION
import urllib.parse
from logging_config import setup_job_logger

# Define a default logger for standalone use
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)

def get_spaces_client(logger=None, file_id=None):
    logger = logger or default_logger
    if logger == default_logger and file_id:
        logger, _ = setup_job_logger(job_id=file_id, console_output=True)
        logger.info(f"Setup logger for get_spaces_client, FileID: {file_id}")
    
    try:
        logger.info("Creating S3 client")
        client = boto3.client(
            "s3",
            region_name=REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        logger.info("S3 client created successfully")
        return client
    except Exception as e:
        logger.error(f"Error creating S3 client: {e}", exc_info=True)
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
    
    try:
        spaces_client = get_spaces_client(logger=logger, file_id=file_id)
        space_name = 'iconluxurygroup-s3'
        
        logger.info(f"Uploading {file_src} to {space_name}/{save_as}")
        spaces_client.upload_file(
            file_src, 
            space_name,
            save_as,
            ExtraArgs={'ACL': 'public-read'} if is_public else {}
        )
        
        logger.info(f"File uploaded successfully to {space_name}/{save_as}")
        double_encoded_filename = double_encode_plus(save_as, logger=logger)
        
        if is_public:
            upload_url = f"https://iconluxurygroup-s3.s3.us-east-2.amazonaws.com/{double_encoded_filename}"
            logger.info(f"Public URL (double-encoded): {upload_url}")
            return upload_url
        
        return None
    except Exception as e:
        logger.error(f"Error uploading file to S3: {e}")
        raise