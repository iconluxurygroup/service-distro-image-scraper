import boto3
import logging
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, REGION
import urllib.parse  # For URL encoding/decoding
logging.getLogger(__name__)
def get_spaces_client():
    """
    Create an AWS S3 client for file storage.
    
    Returns:
        boto3.client: AWS S3 client
    """
    try:
        logging.info("Creating S3 client")
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
def double_encode_plus(filename):
    """
    Explicitly double-encode the '+' character while preserving other characters.
    
    Args:
        filename (str): Filename to encode
    
    Returns:
        str: Double-encoded filename
    """
    # First, replace '+' with its percent-encoded representation
    first_pass = filename.replace('+', '%2B')
    
    # Then percent-encode the result again
    second_pass = urllib.parse.quote(first_pass)
    
    return second_pass

def upload_file_to_space(file_src, save_as, is_public=True):
    """
    Upload a file to AWS S3.
    
    Args:
        file_src (str): Path to the file to upload
        save_as (str): Name to save the file as in S3
        is_public (bool): Whether the file should be publicly accessible
        
    Returns:
        str: Public URL of the uploaded file if is_public is True
    """
    try:
        spaces_client = get_spaces_client()
        space_name = 'iconluxurygroup-s3'
        
        spaces_client.upload_file(
            file_src, 
            space_name,
            save_as,
            ExtraArgs={'ACL': 'public-read'} if is_public else {}
        )
        
        logging.info(f"File uploaded successfully to {space_name}/{save_as}")
        double_encoded_filename = double_encode_plus(save_as)
        # Generate and return the public URL if the file is public
        if is_public:
            upload_url = f"https://iconluxurygroup-s3.s3.us-east-2.amazonaws.com/{double_encoded_filename}"
            logging.info(f"Public URL (double-encoded): {upload_url}")
            return upload_url
        
        return None
    except Exception as e:
        logging.error(f"Error uploading file to S3: {e}")
        raise
