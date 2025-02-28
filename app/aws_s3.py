import boto3
import logging
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, REGION

logger = logging.getLogger(__name__)

def get_spaces_client():
    try:
        client = boto3.client(
            service_name='s3',
            region_name=REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        logging.info("S3 client created successfully")
        return client
    except Exception as e:
        logging.error(f"Error creating S3 client: {e}")
        raise

def upload_file_to_space(file_src, save_as, is_public):
    try:
        client = get_spaces_client()
        space_name = 'iconluxurygroup-s3'
        client.upload_file(
            file_src, 
            space_name,
            save_as,
            ExtraArgs={'ACL': 'public-read'} if is_public else {}
        )
        logging.info(f"File uploaded successfully to {space_name}/{save_as}")
        if is_public:
            upload_url = f"https://{space_name}.s3.{REGION}.amazonaws.com/{save_as}"
            logging.info(f"Public URL: {upload_url}")
            return upload_url
        return None
    except Exception as e:
        logging.error(f"Error uploading file to S3: {e}")
        raise