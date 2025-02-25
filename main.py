from fastapi import FastAPI, BackgroundTasks
import asyncio, os, threading, uuid, requests, openpyxl, uvicorn, shutil, mimetypes, time
from openpyxl import load_workbook
from PIL import Image as IMG2
from PIL import UnidentifiedImageError
from openpyxl.drawing.image import Image
from openpyxl.styles import PatternFill
import datetime, re
import boto3
import logging
from io import BytesIO
from openpyxl.utils import get_column_letter
from icon_image_lib.google_parser import get_original_images as GP
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib3.util.retry import Retry
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition, Personalization, Cc, To
from base64 import b64encode
import aiohttp
from aiohttp import ClientTimeout
from aiohttp_retry import RetryClient, ExponentialRetry
import pandas as pd
import pyodbc
from dotenv import load_dotenv
import base64, zlib
import json
import ray
import tldextract
from collections import Counter
from sqlalchemy import create_engine
import urllib.parse  # For URL encoding/decoding
import base64  # For base64 encoding/decoding
import zlib  # Fo
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment settings
AWS_ACCESS_KEY_ID = 'AKIAZQ3DSIQ5BGLY355N'
AWS_SECRET_ACCESS_KEY = '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'
REGION = 'us-east-2'
MSSQLS_PWD = "Ftu5675FDG54hjhiuu$"

# Database connection strings
pwd_str = f"Pwd={MSSQLS_PWD};"
conn_str = "DRIVER={ODBC Driver 17 for SQL Server};Server=35.172.243.170;Database=luxurymarket_p4;Uid=luxurysitescraper;" + pwd_str
conn = conn_str
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % conn)

# Initialize FastAPI app
app = FastAPI()

#################################################
# AWS S3 FUNCTIONS
#################################################

def get_spaces_client():
    """
    Create an AWS S3 client for file storage.
    
    Returns:
        boto3.client: AWS S3 client
    """
    try:
        logger.info("Creating S3 client")
        client = boto3.client(
            service_name='s3',
            region_name=REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        logger.info("S3 client created successfully")
        return client
    except Exception as e:
        logger.error(f"Error creating S3 client: {e}")
        raise

def upload_file_to_space(file_src, save_as, is_public):
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
        
        logger.info(f"File uploaded successfully to {space_name}/{save_as}")
        
        # Generate and return the public URL if the file is public
        if is_public:
            upload_url = f"https://iconluxurygroup-s3.s3.us-east-2.amazonaws.com/{save_as}"
            logger.info(f"Public URL: {upload_url}")
            return upload_url
        
        return None
    except Exception as e:
        logger.error(f"Error uploading file to S3: {e}")
        raise

#################################################
# EMAIL FUNCTIONS
#################################################

def send_email(to_emails, subject, download_url, jobId):
    """
    Send an email notification with file download link.
    
    Args:
        to_emails (str): Email address to send to
        subject (str): Email subject
        download_url (str): URL to download the file
        jobId (str): Job ID for editing the file
    """
    try:
        html_content = f"""
        <html>
        <body>
        <div class="container">
            <p>Your file is ready for download.</p>
             <a href="{download_url}" class="download-button">Download File</a>

            <p><br>Please use the link below to modify the file<br></p
            <a href="https://cms.rtsplusdev.com/webadmin/ImageScraperForm.asp?Action=Edit&ID={str(jobId)}" class="download-button">Edit / View</a> 
            <br>  
            
            <p>--</p>
            <p>CMS:v1.1</p>
        </div>
        </body>
        </html>
        """
        
        message = Mail(
            from_email='nik@iconluxurygroup.com',
            subject=subject,
            html_content=html_content
        )
        
        cc_recipient = 'nik@iconluxurygroup.com'
        if to_emails == cc_recipient:
            cc_recipient = 'notifications@popovtech.com'
        
        personalization = Personalization()
        personalization.add_cc(Cc(cc_recipient))
        personalization.add_to(To(to_emails))
        message.add_personalization(personalization)
        
        logger.info(f"Sending email to: {to_emails}, CC: {cc_recipient}, Subject: {subject}")
        
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        response = sg.send(message)
        
        logger.info(f"Email sent successfully: {response.status_code}")
    except Exception as e:
        logger.error(f"Error sending email: {e}")

def send_message_email(to_emails, subject, message):
    """
    Send a simple message email.
    
    Args:
        to_emails (str): Email address to send to
        subject (str): Email subject
        message (str): Email message content
    """
    try:
        message_with_breaks = message.replace("\n", "<br>")

        html_content = f"""
        <html>
        <body>
        <div class="container">
            <p>Message details:<br>{message_with_breaks}</p>
            <p>CMS:v1</p>
        </div>
        </body>
        </html>
        """
        
        message_obj = Mail(
            from_email='distrotool@iconluxurygroup.com',
            subject=subject,
            html_content=html_content
        )
        
        cc_recipient = 'notifications@popovtech.com'
        personalization = Personalization()
        personalization.add_cc(Cc(cc_recipient))
        personalization.add_to(To(to_emails))
        message_obj.add_personalization(personalization)
        
        logger.info(f"Sending message email to: {to_emails}, CC: {cc_recipient}, Subject: {subject}")
        
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        response = sg.send(message_obj)
        
        logger.info(f"Message email sent successfully: {response.status_code}")
    except Exception as e:
        logger.error(f"Error sending message email: {e}")

#################################################
# DATABASE FUNCTIONS
#################################################

def fetch_pending_images(limit=10):
    """
    Fetch pending images from the database that need AI processing.
    
    Args:
        limit (int): Maximum number of records to fetch
        
    Returns:
        pd.DataFrame: DataFrame containing pending image records
    """
    query = """
        SELECT rr.ResultID, rr.EntryID, rr.ImageURL, 
               r.ProductBrand, r.ProductCategory, r.ProductColor
        FROM utb_ImageScraperRecords r
        INNER JOIN utb_ImageScraperResult rr ON r.EntryID = rr.EntryID
        WHERE rr.aijson IS NULL
        ORDER BY rr.ResultID
        LIMIT ?
    """
    try:
        with pyodbc.connect(conn_str) as conn:
            df = pd.read_sql(query, conn, params=[limit])
            logging.info(f"Fetched {len(df)} pending images for processing")
            return df
    except Exception as e:
        logging.error(f"Error fetching pending images: {e}")
        return pd.DataFrame()

def fetch_images_by_file_id(file_id):
    """
    Fetch image data along with brand, category, and color from the database by FileID.
    
    Args:
        file_id (int): The FileID to fetch records for
        
    Returns:
        pd.DataFrame: DataFrame containing image records for the given FileID
    """
    query = """
        SELECT rr.ResultID, rr.EntryID, rr.ImageURL, 
               r.ProductBrand, r.ProductCategory, r.ProductColor
        FROM utb_ImageScraperRecords r
        INNER JOIN utb_ImageScraperResult rr ON r.EntryID = rr.EntryID
        WHERE r.FileID = ?
    """
    try:
        with pyodbc.connect(conn_str) as conn:
            df = pd.read_sql(query, conn, params=[file_id])
            logging.info(f"Fetched {len(df)} images for FileID {file_id}")
            return df
    except Exception as e:
        logging.error(f"Error fetching images for FileID {file_id}: {e}")
        return pd.DataFrame()

def update_database(result_id, aijson, aicaption):
    """
    Update the database with the AI JSON and AI-generated caption.
    
    Args:
        result_id (int): The ResultID to update
        aijson (str): JSON string containing AI analysis results
        aicaption (str): AI-generated caption for the image
        
    Returns:
        bool: True if update successful, False otherwise
    """
    query = """
        UPDATE utb_ImageScraperResult
        SET aijson = ?, aicaption = ?
        WHERE ResultID = ?
    """
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute(query, (aijson, aicaption, result_id))
            conn.commit()
            logging.info(f"Database updated for ResultID={result_id}, rows affected: {cursor.rowcount}")
            return cursor.rowcount > 0
    except Exception as e:
        logging.error(f"Error updating database for ResultID {result_id}: {e}")
        return False

def insert_file_db(file_name, file_source, send_to_email="nik@iconluxurygroup.com"):
    """
    Insert a new file record into the database.
    
    Args:
        file_name (str): The name of the file
        file_source (str): The URL of the file
        send_to_email (str): The email to send notifications to
        
    Returns:
        int: The ID of the newly inserted file
    """
    try:
        connection = pyodbc.connect(conn)
        cursor = connection.cursor()
        insert_query = "INSERT INTO utb_ImageScraperFiles (FileName, FileLocationUrl, UserEmail) OUTPUT INSERTED.Id VALUES (?, ?, ?)"
        values = (file_name, file_source, send_to_email)

        cursor.execute(insert_query, values)
        file_id = cursor.fetchval()
        connection.commit()
        cursor.close()
        connection.close()
        logging.info(f"Inserted new file record with ID: {file_id}")
        return file_id
    except Exception as e:
        logging.error(f"Error inserting file record: {e}")
        raise

def load_payload_db(rows, file_id):
    """
    Load payload data into the database.
    
    Args:
        rows (list): List of dictionaries containing row data
        file_id (int): The FileID to associate with the rows
        
    Returns:
        pd.DataFrame: DataFrame containing the loaded rows
    """
    try:
        # Create DataFrame from list of dictionaries (rows)
        df = pd.DataFrame(rows)

        # Rename columns
        df = df.rename(columns={
            'absoluteRowIndex': 'ExcelRowID',
            'searchValue': 'ProductModel',
            'brandValue': 'ProductBrand',
            'colorValue': 'ProductColor',
            'CategoryValue': 'ProductCategory'
        })

        # Insert new column 'FileID' at the beginning with all values set to file_id
        df.insert(0, 'FileID', file_id)
        if 'imageValue' in df.columns:
            df = df.drop(columns=['imageValue'], axis=1)
        
        # Load DataFrame into SQL database using pyodbc instead of to_sql
        connection = pyodbc.connect(conn)
        cursor = connection.cursor()
        
        for _, row in df.iterrows():
            columns = ', '.join(df.columns)
            placeholders = ', '.join(['?'] * len(df.columns))
            insert_query = f"INSERT INTO utb_ImageScraperRecords ({columns}) VALUES ({placeholders})"
            cursor.execute(insert_query, tuple(row))
        
        connection.commit()
        cursor.close()
        connection.close()
        
        logging.info(f"Loaded {len(df)} rows into utb_ImageScraperRecords for FileID: {file_id}")
        return df
    except Exception as e:
        logging.error(f"Error loading payload data: {e}")
        raise

def get_records_to_search(file_id):
    """
    Get records that need to be searched for images.
    
    Args:
        file_id (int): The FileID to get records for
        
    Returns:
        pd.DataFrame: DataFrame containing records to search
    """
    try:
        sql_query = f"""
            SELECT EntryID, ProductModel as SearchString 
            FROM utb_ImageScraperRecords 
            WHERE FileID = {file_id} AND Step1 is null 
            UNION ALL 
            SELECT EntryID, ProductModel + ' ' + ProductBrand as SearchString 
            FROM utb_ImageScraperRecords 
            WHERE FileID = {file_id} AND Step1 is null 
            ORDER BY 1
        """
        
        with pyodbc.connect(conn) as connection:
            df = pd.read_sql_query(sql_query, connection)
        
        logging.info(f"Got {len(df)} records to search for FileID: {file_id}")
        return df
    except Exception as e:
        logging.error(f"Error getting records to search: {e}")
        return pd.DataFrame()

def update_sort_order(file_id):
    """
    Update the sort order of image results.
    
    Args:
        file_id (int): The FileID to update sort order for
    """
    try:
        query = """
            WITH toupdate AS (
                SELECT t.*,
                ROW_NUMBER() OVER (PARTITION BY t.EntryID ORDER BY t.ResultID) AS seqnum
                FROM utb_ImageScraperResult t 
                INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID 
                WHERE r.FileID = ?
            ) 
            UPDATE toupdate SET SortOrder = seqnum
        """
        
        connection = pyodbc.connect(conn)
        cursor = connection.cursor()
        cursor.execute(query, (file_id,))
        connection.commit()
        
        # Mark image processing as complete
        complete_query = "UPDATE utb_ImageScraperFiles SET ImageCompleteTime = GETDATE() WHERE ID = ?"
        cursor.execute(complete_query, (file_id,))
        connection.commit()
        
        cursor.close()
        connection.close()
        
        logging.info(f"Updated sort order for FileID: {file_id}")
    except Exception as e:
        logging.error(f"Error updating sort order: {e}")
        raise

def update_file_generate_complete(file_id):
    """
    Update file generation completion time.
    
    Args:
        file_id (int): The FileID to update
    """
    try:
        query = "UPDATE utb_ImageScraperFiles SET CreateFileCompleteTime = GETDATE() WHERE ID = ?"
        
        connection = pyodbc.connect(conn)
        cursor = connection.cursor()
        cursor.execute(query, (file_id,))
        connection.commit()
        cursor.close()
        connection.close()
        
        logging.info(f"Marked file generation as complete for FileID: {file_id}")
    except Exception as e:
        logging.error(f"Error updating file generation completion time: {e}")

def update_file_location_complete(file_id, file_location):
    """
    Update file location URL after processing.
    
    Args:
        file_id (int): The FileID to update
        file_location (str): The URL of the processed file
    """
    try:
        query = "UPDATE utb_ImageScraperFiles SET FileLocationURLComplete = ? WHERE ID = ?"
        
        connection = pyodbc.connect(conn)
        cursor = connection.cursor()
        cursor.execute(query, (file_location, file_id))
        connection.commit()
        cursor.close()
        connection.close()
        
        logging.info(f"Updated file location URL for FileID: {file_id}")
    except Exception as e:
        logging.error(f"Error updating file location URL: {e}")

def get_file_location(file_id):
    """
    Get the file location URL for a file.
    
    Args:
        file_id (int): The FileID to get the location for
        
    Returns:
        str: The file location URL
    """
    try:
        query = "SELECT FileLocationUrl FROM utb_ImageScraperFiles WHERE ID = ?"
        
        connection = pyodbc.connect(conn)
        cursor = connection.cursor()
        cursor.execute(query, (file_id,))
        file_location_url = cursor.fetchone()
        connection.close()
        
        if file_location_url:
            file_location_url = file_location_url[0]
            logging.info(f"Got file location URL for FileID: {file_id}: {file_location_url}")
            return file_location_url
        else:
            logging.warning(f"No file location URL found for FileID: {file_id}")
            return "No File Found"
    except Exception as e:
        logging.error(f"Error getting file location URL: {e}")
        return "Error retrieving file location"

def get_send_to_email(file_id):
    """
    Get the email address to send notifications to.
    
    Args:
        file_id (int): The FileID to get the email for
        
    Returns:
        str: The email address
    """
    try:
        query = "SELECT UserEmail FROM utb_ImageScraperFiles WHERE ID = ?"
        
        connection = pyodbc.connect(conn)
        cursor = connection.cursor()
        cursor.execute(query, (file_id,))
        send_to_email = cursor.fetchone()
        connection.close()
        
        if send_to_email:
            send_to_email = send_to_email[0]
            logging.info(f"Got email address for FileID: {file_id}: {send_to_email}")
            return send_to_email
        else:
            logging.warning(f"No email address found for FileID: {file_id}")
            return "No Email Found"
    except Exception as e:
        logging.error(f"Error getting email address: {e}")
        return "nik@iconluxurygroup.com"  # Default fallback

def get_images_excel_db(file_id):
    """
    Get images for Excel export from the database.
    
    Args:
        file_id (int): The FileID to get images for
        
    Returns:
        pd.DataFrame: DataFrame containing images for Excel export
    """
    try:
        # Update file start time
        update_file_start_query = "UPDATE utb_ImageScraperFiles SET CreateFileStartTime = GETDATE() WHERE ID = ?"
        
        connection = pyodbc.connect(conn)
        cursor = connection.cursor()
        cursor.execute(update_file_start_query, (file_id,))
        connection.commit()
        connection.close()
        
        # Get images for Excel
        query = """
            SELECT s.ExcelRowID, r.ImageUrl, r.ImageUrlThumbnail 
            FROM utb_ImageScraperFiles f
            INNER JOIN utb_ImageScraperRecords s ON s.FileID = f.ID 
            INNER JOIN utb_ImageScraperResult r ON r.EntryID = s.EntryID 
            WHERE f.ID = ? AND r.SortOrder = 1
            ORDER BY s.ExcelRowID
        """
        
        with pyodbc.connect(conn) as connection:
            df = pd.read_sql_query(query, connection, params=[file_id])
        
        logging.info(f"Got {len(df)} images for Excel export for FileID: {file_id}")
        return df
    except Exception as e:
        logging.error(f"Error getting images for Excel export: {e}")
        return pd.DataFrame()

def get_lm_products(file_id):
    """
    Execute stored procedure to match products from retail.
    
    Args:
        file_id (int): The FileID to match products for
    """
    try:
        connection = pyodbc.connect(conn)
        cursor = connection.cursor()
        query = f"EXEC usp_ImageScrapergetMatchFromRetail {file_id}"
        cursor.execute(query)
        connection.commit()
        connection.close()
        
        logging.info(f"Executed stored procedure to match products for FileID: {file_id}")
    except Exception as e:
        logging.error(f"Error executing stored procedure to match products: {e}")

def get_endpoint():
    """
    Get a random endpoint URL from the database.
    
    Returns:
        str: The endpoint URL
    """
    try:
        connection = pyodbc.connect(conn)
        cursor = connection.cursor()
        sql_query = "SELECT TOP 1 EndpointURL FROM utb_Endpoints WHERE EndpointIsBlocked = 0 ORDER BY NEWID()"
        cursor.execute(sql_query)
        endpoint_url = cursor.fetchone()
        connection.close()
        
        if endpoint_url:
            endpoint = endpoint_url[0]
            logging.info(f"Got endpoint URL: {endpoint}")
            return endpoint
        else:
            logging.warning("No endpoint URL found")
            return "No EndpointURL"
    except Exception as e:
        logging.error(f"Error getting endpoint URL: {e}")
        return "No EndpointURL"

def remove_endpoint(endpoint):
    """
    Mark an endpoint as blocked in the database.
    
    Args:
        endpoint (str): The endpoint URL to block
    """
    try:
        connection = pyodbc.connect(conn)
        cursor = connection.cursor()
        sql_query = f"UPDATE utb_Endpoints SET EndpointIsBlocked = 1 WHERE EndpointURL = '{endpoint}'"
        cursor.execute(sql_query)
        connection.commit()
        connection.close()
        logging.info(f"Marked endpoint as blocked: {endpoint}")
    except Exception as e:
        logging.error(f"Error marking endpoint as blocked: {e}")

#################################################
# IMAGE PROCESSING FUNCTIONS
#################################################

def unpack_content(encoded_content):
    """
    Unpack base64 encoded and compressed content.
    
    Args:
        encoded_content (str): Base64 encoded and compressed content
        
    Returns:
        bytes: Unpacked content
    """
    try:
        if encoded_content:
            compressed_content = base64.b64decode(encoded_content)
            original_content = zlib.decompress(compressed_content)
            return original_content  # Return as binary data
        return None
    except Exception as e:
        logging.error(f"Error unpacking content: {e}")
        return None
import requests
import json
import re
from collections import OrderedDict
import math
import base64
from PIL import Image
import io
from typing import Dict, Any, Optional, Union, Tuple
from urllib.parse import urlparse
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Expected JSON schemas
match_schema = {
    "type": "object",
    "properties": {
        "description": {"type": "string"},
        "user_provided": {
            "type": "object",
            "properties": {
                "brand": {"type": "string"},
                "category": {"type": "string"},
                "color": {"type": "string"}
            },
            "required": ["brand", "category", "color"]
        },
        "extracted_features": {
            "type": "object",
            "properties": {
                "brand": {"type": "string"},
                "category": {"type": "string"},
                "color": {"type": "string"}
            },
            "required": ["brand", "category", "color"]
        },
        "match_score": {"type": "number"},
        "reasoning_match": {"type": "string"}
    },
    "required": ["description", "user_provided", "extracted_features", "match_score", "reasoning_match"]
}

linesheet_schema = {
    "type": "object",
    "properties": {
        "linesheet_score": {"type": "number"},
        "reasoning_linesheet": {"type": "string"}
    },
    "required": ["linesheet_score", "reasoning_linesheet"]
}


def get_image_data(image_path_or_url: str) -> bytes:
    """
    Gets image data from either a local path or a URL.
    Returns the image data as bytes.
    """
    # Check if the input is a URL
    parsed_url = urlparse(image_path_or_url)
    is_url = bool(parsed_url.scheme and parsed_url.netloc)
    
    if is_url:
        # Handle URL
        try:
            response = requests.get(image_path_or_url, timeout=30, 
                                    headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"})
            response.raise_for_status()
            return response.content
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download image from URL: {e}")
            raise
    else:
        # Handle local file path
        try:
            with open(image_path_or_url, 'rb') as img_file:
                return img_file.read()
        except IOError as e:
            logger.error(f"Failed to read local image file: {e}")
            raise


def create_default_result(product_brand: str = "", product_category: str = "", product_color: str = "") -> Dict[str, Any]:
    """
    Creates a default result dictionary with NaN values for numerical fields and 
    provided product details for user_provided fields.
    """
    return {
        "description": "",
        "user_provided": {
            "brand": product_brand,
            "category": product_category,
            "color": product_color
        },
        "extracted_features": {
            "brand": "",
            "category": "",
            "color": ""
        },
        "match_score": float('nan'),
        "reasoning_match": "",
        "linesheet_score": float('nan'),
        "reasoning_linesheet": ""
    }


def extract_json(text: str, schema_type: str = 'generic') -> Dict[str, Any]:
    """
    Extracts a valid JSON object from raw text using multiple fallback methods.
    
    Args:
        text (str): The text to extract JSON from
        schema_type (str): Either 'match' or 'linesheet' to enable schema-specific extraction
        
    Returns:
        Dict[str, Any]: Extracted data or a dictionary with extraction_failed=True
    """
    if not text or not text.strip():
        return {"extraction_failed": True}
    
    # First attempt: direct JSON parsing
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        logger.warning("Direct JSON parsing failed, trying regex")
        
        # Second attempt: regex to find JSON between outermost curly braces
        try:
            match = re.search(r'\{(?:[^{}]|(?:\{(?:[^{}]|(?:\{[^{}]*\}))*\}))*\}', text, re.DOTALL)
            if match:
                return json.loads(match.group(0))
        except (json.JSONDecodeError, AttributeError):
            logger.warning("JSON extraction using regex failed, trying line-by-line approach")
            
            # Third attempt: reconstruct JSON from lines
            try:
                json_lines = []
                capture = False
                for line in text.split('\n'):
                    if '{' in line and not capture:
                        capture = True
                    if capture:
                        json_lines.append(line)
                    if '}' in line and capture:
                        break
                
                reconstructed = ' '.join(json_lines)
                # Clean up any markdown or extra text
                reconstructed = re.sub(r'```json|```', '', reconstructed)
                return json.loads(reconstructed)
            except Exception as e:
                logger.warning(f"JSON reconstruction failed: {str(e)}, trying format-specific parsing")
                
                # Fourth attempt: schema-specific parsing
                if schema_type == 'linesheet':
                    return extract_linesheet_data(text)
                elif schema_type == 'match':
                    return extract_match_data(text)
                else:
                    logger.error("All JSON extraction methods failed")
                    return {"extraction_failed": True}
    
    return {"extraction_failed": True}


def extract_linesheet_data(text: str) -> Dict[str, Any]:
    """
    Extract linesheet data from non-JSON text formats
    """
    try:
        # First look for a direct score in the format "linesheet_score": X
        score_match = re.search(r'"linesheet_score":\s*(\d+)', text)
        if score_match:
            score = int(score_match.group(1))
            
            # Look for reasoning text
            reasoning_match = re.search(r'"reasoning_linesheet":\s*"([^"]+)"', text)
            reasoning = reasoning_match.group(1) if reasoning_match else ""
            
            return {
                "linesheet_score": score,
                "reasoning_linesheet": reasoning
            }
        
        # Look for score patterns in the document format
        total_score_match = re.search(r'\*\*Total Linesheet Score.*?\*\*Value:\*\*\s*(\d+)', text, re.DOTALL)
        
        if not total_score_match:
            total_score_match = re.search(r'Total Linesheet Score.*?Value:.*?(\d+)', text, re.DOTALL)
            
        if not total_score_match:
            # Try another pattern that might appear in the output
            total_score_match = re.search(r'linesheet_score.*?(\d+)', text, re.DOTALL)
    
        # Extract individual component scores
        angle_match = re.search(r'angle.*?score.*?Value:.*?(\d+)', text, re.DOTALL)
        background_match = re.search(r'background.*?score.*?Value:.*?(\d+)', text, re.DOTALL)
        composition_match = re.search(r'composition.*?score.*?Value:.*?(\d+)', text, re.DOTALL)
        quality_match = re.search(r'Image Quality.*?score.*?Value:.*?(\d+)', text, re.DOTALL)
        
        # If we found a total score, use it
        if total_score_match:
            linesheet_score = int(total_score_match.group(1))
        else:
            # Look for scores in a more general format
            components = [
                re.search(r'angle.*?(\d+).*?points', text, re.DOTALL),
                re.search(r'background.*?(\d+).*?points', text, re.DOTALL),
                re.search(r'composition.*?(\d+).*?points', text, re.DOTALL),
                re.search(r'(image|quality).*?(\d+).*?points', text, re.DOTALL)
            ]
            
            scores = []
            for comp in components:
                if comp:
                    # Some patterns might have the score in group 1, others in group 2
                    group_to_use = 1 if len(comp.groups()) == 1 else 2
                    try:
                        scores.append(int(comp.group(group_to_use)))
                    except (ValueError, IndexError):
                        pass
            
            if scores:
                linesheet_score = sum(scores)
            else:
                # If we can't find component scores, look for a numeric value followed by "points" or "score"
                all_scores = re.findall(r'(\d+)\s*(?:points|score)', text, re.IGNORECASE)
                if all_scores:
                    try:
                        linesheet_score = sum(int(s) for s in all_scores)
                    except ValueError:
                        return {"extraction_failed": True}
                else:
                    return {"extraction_failed": True}
        
        # Try to find a reasoning section
        reasoning_section = re.search(r'reasoning_linesheet[\":\s]+([^}\"]+)', text, re.DOTALL | re.IGNORECASE)
        
        if reasoning_section:
            reasoning = reasoning_section.group(1).strip()
        else:
            # Build reasoning text from component descriptions
            reasoning_parts = []
            
            if angle_match:
                angle_reasoning = re.search(r'angle.*?Reasoning:.*?([^*]+)', text, re.DOTALL)
                if angle_reasoning:
                    reasoning_parts.append(f"Angle ({angle_match.group(1)} points): {angle_reasoning.group(1).strip()}")
            
            if background_match:
                bg_reasoning = re.search(r'background.*?Reasoning:.*?([^*]+)', text, re.DOTALL)
                if bg_reasoning:
                    reasoning_parts.append(f"Background ({background_match.group(1)} points): {bg_reasoning.group(1).strip()}")
            
            if composition_match:
                comp_reasoning = re.search(r'composition.*?Reasoning:.*?([^*]+)', text, re.DOTALL)
                if comp_reasoning:
                    reasoning_parts.append(f"Composition ({composition_match.group(1)} points): {comp_reasoning.group(1).strip()}")
            
            if quality_match:
                quality_reasoning = re.search(r'Image Quality.*?Reasoning:.*?([^*]+)', text, re.DOTALL)
                if quality_reasoning:
                    reasoning_parts.append(f"Image Quality ({quality_match.group(1)} points): {quality_reasoning.group(1).strip()}")
            
            reasoning = " ".join(reasoning_parts) if reasoning_parts else "Scores extracted from document format."
        
        return {
            "linesheet_score": linesheet_score,
            "reasoning_linesheet": reasoning
        }
                
    except Exception as e:
        logger.error(f"Linesheet extraction failed: {str(e)}")
        return {"extraction_failed": True}


def extract_match_data(text: str) -> Dict[str, Any]:
    """
    Extract match analysis data from non-JSON text formats
    """
    try:
        # Create default structure
        result = {
            "description": "",
            "user_provided": {
                "brand": "",
                "category": "",
                "color": ""
            },
            "extracted_features": {
                "brand": "",
                "category": "",
                "color": ""
            },
            "match_score": float('nan'),
            "reasoning_match": ""
        }
        
        # Look for description
        description_match = re.search(r'(The\s+image\s+shows.*?\.)', text, re.DOTALL)
        if description_match:
            result["description"] = description_match.group(1).strip()
        else:
            # Try generic description extraction
            sentences = re.findall(r'([^.!?]+[.!?])', text)
            if sentences:
                # Use the first sentence as description
                result["description"] = sentences[0].strip()
        
        # Extract brand information
        brand_match = re.search(r'brand[:\s]+"?([^"\n,]+)"?', text, re.IGNORECASE)
        if brand_match:
            brand = brand_match.group(1).strip()
            # Check if brand is mentioned elsewhere
            brand_mention = re.search(r'\b(adidas|nike|puma|reebok|new\s+balance)\b', text, re.IGNORECASE)
            if not brand and brand_mention:
                brand = brand_mention.group(1)
            result["extracted_features"]["brand"] = brand
            
        # Extract category information
        category_match = re.search(r'category[:\s]+"?([^"\n,]+)"?', text, re.IGNORECASE)
        if category_match:
            result["extracted_features"]["category"] = category_match.group(1).strip()
        else:
            # Look for category terms
            category_terms = ["shoe", "sneaker", "footwear", "apparel", "clothing", "accessory"]
            for term in category_terms:
                if re.search(r'\b' + term + r'\b', text, re.IGNORECASE):
                    result["extracted_features"]["category"] = term
                    break
        
        # Extract color information
        color_match = re.search(r'color[:\s]+"?([^"\n,]+)"?', text, re.IGNORECASE)
        if color_match:
            result["extracted_features"]["color"] = color_match.group(1).strip()
        else:
            # Look for color terms
            color_list = ["red", "blue", "green", "yellow", "black", "white", "gray", "grey", "purple", "orange", "brown", "pink"]
            found_colors = []
            for color in color_list:
                if re.search(r'\b' + color + r'\b', text, re.IGNORECASE):
                    found_colors.append(color)
            if found_colors:
                result["extracted_features"]["color"] = ", ".join(found_colors)
        
        # Look for user provided info
        user_brand_match = re.search(r'user[\s-]*provided.*?brand[:\s]+"?([^"\n,]+)"?', text, re.IGNORECASE | re.DOTALL)
        if user_brand_match:
            result["user_provided"]["brand"] = user_brand_match.group(1).strip()
            
        user_category_match = re.search(r'user[\s-]*provided.*?category[:\s]+"?([^"\n,]+)"?', text, re.IGNORECASE | re.DOTALL)
        if user_category_match:
            result["user_provided"]["category"] = user_category_match.group(1).strip()
            
        user_color_match = re.search(r'user[\s-]*provided.*?color[:\s]+"?([^"\n,]+)"?', text, re.IGNORECASE | re.DOTALL)
        if user_color_match:
            result["user_provided"]["color"] = user_color_match.group(1).strip()
        
        # Extract match score
        score_match = re.search(r'match_score"?\s*:\s*(\d+)', text)
        if score_match:
            try:
                result["match_score"] = float(score_match.group(1))
            except ValueError:
                pass
        else:
            # Try to infer score from text
            if "all three features match" in text.lower():
                result["match_score"] = 100
            elif "two features match" in text.lower():
                result["match_score"] = 67
            elif "one feature match" in text.lower():
                result["match_score"] = 33
            elif "no features match" in text.lower():
                result["match_score"] = 0
        
        # Extract reasoning
        reasoning_match = re.search(r'reasoning_match"?\s*:\s*"?([^}"]+)"?', text, re.IGNORECASE)
        if reasoning_match:
            result["reasoning_match"] = reasoning_match.group(1).strip()
        else:
            # Try to construct reasoning based on what we found
            features_matched = []
            features_mismatched = []
            
            # Compare extracted to user provided
            if result["extracted_features"]["brand"].lower() == result["user_provided"]["brand"].lower():
                features_matched.append("brand")
            else:
                features_mismatched.append("brand")
                
            if result["extracted_features"]["category"].lower() == result["user_provided"]["category"].lower():
                features_matched.append("category")
            else:
                features_mismatched.append("category")
                
            if result["extracted_features"]["color"].lower() == result["user_provided"]["color"].lower():
                features_matched.append("color")
            else:
                features_mismatched.append("color")
                
            if features_matched and features_mismatched:
                result["reasoning_match"] = f"The {', '.join(features_matched)} match, but the {', '.join(features_mismatched)} don't match."
            elif features_matched:
                result["reasoning_match"] = f"All features match: {', '.join(features_matched)}."
            elif features_mismatched:
                result["reasoning_match"] = f"No features match: {', '.join(features_mismatched)}."
            else:
                result["reasoning_match"] = "Could not determine reasoning."
        
        return result
                
    except Exception as e:
        logger.error(f"Match extraction failed: {str(e)}")
        return {"extraction_failed": True}


def send_request_llama(image_path_or_url: str, prompt: str, headers: Dict[str, str], schema_type: str = 'generic') -> Dict[str, Any]:
    """
    Sends a request to the Llama 3.2 API with the image and prompt.
    Works with both local file paths and URLs.
    Returns the parsed response or a dictionary with extraction_failed=True if the request fails.
    
    Args:
        image_path_or_url: Path to image file or URL
        prompt: The prompt text to send
        headers: API request headers
        schema_type: Either 'match' or 'linesheet' to enable specific extraction
    """
    try:
        # Get image data from either path or URL
        img_data = get_image_data(image_path_or_url)
        
        # Convert to base64
        base64_img = base64.b64encode(img_data).decode('utf-8')
        
        # Using Llama 3.2 compatible format
        payload = {
            "model": "meta-llama/Llama-3.2-70b-chat-hf",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{base64_img}"
                            }
                        },
                        {
                            "type": "text",
                            "text": prompt
                        }
                    ]
                }
            ],
            "max_tokens": 1024,
            "temperature": 0.2,  # Lower temperature for more deterministic responses
            "response_format": {"type": "json_object"}  # Request JSON format
        }

        logger.info("Sending request to Llama 3.2 API")
        response = requests.post(
            "https://api-inference.huggingface.co/models/meta-llama/Llama-3.2-70b-chat-hf",
            headers=headers,
            json=payload
        )
        response.raise_for_status()
        
        response_data = response.json()
        
        # Extract the content from Llama 3.2 response format
        if "choices" in response_data and len(response_data["choices"]) > 0:
            content = response_data["choices"][0]["message"]["content"]
            logger.info("Successfully received response from Llama 3.2 API")
            return extract_json(content, schema_type)
        else:
            logger.error(f"Unexpected response format: {response_data}")
            return {"extraction_failed": True}
            
    except Exception as e:
        logger.error(f"Request to Llama 3.2 API failed: {e}")
        return {"extraction_failed": True}


def process_image(image_path_or_url: str, product_details: Dict[str, str], headers: Dict[str, str]) -> Dict[str, Any]:
    """
    Processes an image to perform match and linesheet analysis using Llama 3.2.
    Works with both local file paths and URLs.
    Returns a combined result dictionary with NaN values for failed numerical fields.
    """
    # Extract product details
    product_brand = product_details.get("brand", "")
    product_category = product_details.get("category", "")
    product_color = product_details.get("color", "")
    
    # Create default result
    default_result = create_default_result(product_brand, product_category, product_color)
    
    # Create prompts
    match_analysis_prompt = f"""
    Analyze this product image and extract key features.

    Compare these features to the user-provided values:
    - Brand: {product_brand}
    - Category: {product_category}
    - Color: {product_color}

    IMPORTANT: Your response MUST be a valid JSON object with exactly this structure:
    {{
      "description": "Brief description of what you see in the image",
      "user_provided": {{
        "brand": "{product_brand}",
        "category": "{product_category}",
        "color": "{product_color}"
      }},
      "extracted_features": {{
        "brand": "The brand you see in the image",
        "category": "The product category you see",
        "color": "The main colors you see"
      }},
      "match_score": 0,
      "reasoning_match": "Explanation of your score"
    }}

    Calculate the match_score as follows:
    - 100: All three features match
    - 67: Two features match
    - 33: One feature matches
    - 0: No features match

    Only return the JSON object, nothing else.
    """

    linesheet_analysis_prompt = """
    Evaluate the visual composition of this product image.

    Score the image on these 4 criteria (provide a score for EACH):

    1. Angle (max 50 points):
       - 50: Perfect straight-on side view
       - 25: Front view or 3/4 angle
       - 5: Rotated or tilted view

    2. Background (max 50 points):
       - 50: Clean white background
       - 25: Neutral grey background
       - 5: Complex or colorful background

    3. Composition (max 50 points):
       - 50: Clear product-only shot
       - 25: Model wearing product
       - 5: Product partially obstructed

    4. Image Quality (max 50 points):
       - 50: High-resolution, sharp image
       - 25: Acceptable quality with minor issues
       - 5: Low quality, blurry or pixelated

    IMPORTANT: Your response MUST be a valid JSON object with exactly this structure:
    {
      "linesheet_score": 0,
      "reasoning_linesheet": "Detailed explanation with individual scores for each criterion"
    }

    The linesheet_score should be the sum of all four individual criteria scores.

    Only return the JSON object, nothing else.
    """

    try:
        # Get match analysis - note the 'match' schema type
        logger.info("Requesting match analysis from Llama 3.2")
        match_result = send_request_llama(image_path_or_url, match_analysis_prompt, headers, 'match')
        
        # Check if extraction failed
        if match_result.get("extraction_failed", False):
            logger.warning("Match analysis extraction failed, using default values")
            match_data = default_result
        else:
            # Update default result with match data
            match_data = default_result.copy()
            match_data["description"] = match_result.get("description", "")
            
            # Extract nested objects safely
            if isinstance(match_result.get("extracted_features"), dict):
                match_data["extracted_features"] = match_result["extracted_features"]
            
            if isinstance(match_result.get("user_provided"), dict):
                match_data["user_provided"] = match_result["user_provided"]
                
            # Handle match score
            try:
                match_score = float(match_result.get("match_score", float('nan')))
                match_data["match_score"] = match_score if not math.isnan(match_score) else float('nan')
            except (ValueError, TypeError):
                match_data["match_score"] = float('nan')
                
            match_data["reasoning_match"] = match_result.get("reasoning_match", "")

        # Get linesheet analysis - note the 'linesheet' schema type
        logger.info("Requesting linesheet analysis from Llama 3.2")
        linesheet_result = send_request_llama(image_path_or_url, linesheet_analysis_prompt, headers, 'linesheet')
        
        # Process linesheet result
        if linesheet_result.get("extraction_failed", False):
            logger.warning("Linesheet analysis extraction failed, using default values")
            linesheet_score = float('nan')
            reasoning_linesheet = ""
        else:
            # Handle linesheet score
            try:
                linesheet_score = float(linesheet_result.get("linesheet_score", float('nan')))
                linesheet_score = linesheet_score if not math.isnan(linesheet_score) else float('nan')
            except (ValueError, TypeError):
                linesheet_score = float('nan')
                
            reasoning_linesheet = linesheet_result.get("reasoning_linesheet", "")

        # Create final OrderedDict with both results
        final_result = OrderedDict([
            ("description", match_data["description"]),
            ("user_provided", match_data["user_provided"]),
            ("extracted_features", match_data["extracted_features"]),
            ("match_score", match_data["match_score"]),
            ("reasoning_match", match_data["reasoning_match"]),
            ("linesheet_score", linesheet_score),
            ("reasoning_linesheet", reasoning_linesheet),
        ])

        return final_result
    
    except Exception as e:
        logger.error(f"Processing failed: {str(e)}")
        return default_result


def batch_process_images(headers, file_id=None, limit=10):
    """
    Process multiple images in a batch, either by file_id or by fetching pending images.
    
    Args:
        headers (dict): API request headers
        file_id (int, optional): FileID to process images for
        limit (int): Maximum number of records to process if file_id is None
        
    Returns:
        int: Number of successfully processed images
    """
    # Fetch images either by file_id or pending status
    if file_id:
        df = fetch_images_by_file_id(file_id)
    else:
        df = fetch_pending_images(limit)
    
    if df.empty:
        logging.info("No images to process")
        return 0
    
    success_count = 0
    
    # Process each image
    for _, row in df.iterrows():
        result_id = row['ResultID']
        image_url = row['ImageURL']
        
        # Create product details dictionary
        product_details = {
            "brand": row['ProductBrand'],
            "category": row['ProductCategory'],
            "color": row['ProductColor']
        }
        
        try:
            # Process the image
            result = process_image(image_url, product_details, headers)
            
            # Serialize the JSON result
            json_result = json.dumps(result)
            caption = result.get('caption', '')
            
            # Update the database
            success = update_database(result_id, json_result, caption)
            
            if success:
                success_count += 1
                logging.info(f"Successfully processed and updated image {result_id}")
            else:
                logging.warning(f"Database update failed for image {result_id}")
                
        except Exception as e:
            logging.error(f"Error processing image {result_id}: {e}")
    
    logging.info(f"Batch processing complete. Processed {success_count} out of {len(df)} images.")
    return success_count

def process_images(file_id):
    """
    Process images for a specific file.
    
    Args:
        file_id (int): The FileID to process images for
        
    Returns:
        int: Number of successfully processed images
    """
    try:
        # API headers
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer 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"
        }
        
        logger.info(f"Processing images for FileID {file_id}")
        count = batch_process_images(headers, file_id=file_id)
        logger.info(f"Successfully processed {count} images for FileID {file_id}")
        
        return count
    except Exception as e:
        logger.error(f"Error processing images: {e}")
        return 0

def process_search_row(search_string, endpoint, entry_id):
    """
    Process a search row to find images.
    
    Args:
        search_string (str): String to search for
        endpoint (str): API endpoint to use
        entry_id (int): Entry ID in the database
        
    Returns:
        bool: True if processing was successful, False otherwise
    """
    try:
        search_url = f"{endpoint}?query={search_string}"
        logging.info(f"Searching URL: {search_url}")

        response = requests.get(search_url, timeout=60)
        logging.info(f"Got response with status code: {response.status_code}")
        
  # First check if the response status is OK
        if response.status_code != 200:
            logging.warning(f'Response status code not OK: {response.status_code}, trying again with new endpoint')
            remove_endpoint(endpoint)
            n_endpoint = get_endpoint()
            return process_search_row(search_string, n_endpoint, entry_id)
        
        # Then try to parse JSON, handling the case where response is not JSON
        try:
            response_json = response.json()
            result = response_json.get('body', None)
        except json.JSONDecodeError:
            logging.warning(f'Response is not valid JSON: {response.text[:100]}..., trying again with new endpoint')
            remove_endpoint(endpoint)
            n_endpoint = get_endpoint()
            return process_search_row(search_string, n_endpoint, entry_id)
        
        # Check if 'body' field exists and is not None
        if not result:
            logging.warning('No result body, trying again with new endpoint')
            remove_endpoint(endpoint)
            n_endpoint = get_endpoint()
            return process_search_row(search_string, n_endpoint, entry_id)
        
        unpacked_html = unpack_content(result)
        
        if not unpacked_html or len(unpacked_html) < 100:
            logging.warning('Unpacked HTML invalid or too small, trying again with new endpoint')
            remove_endpoint(endpoint)
            n_endpoint = get_endpoint()
            return process_search_row(search_string, n_endpoint, entry_id)
        
        parsed_data = GP(unpacked_html)
        
        if parsed_data is None:
            logging.warning('Parsed data is None, trying again with new endpoint')
            remove_endpoint(endpoint)
            n_endpoint = get_endpoint()
            return process_search_row(search_string, n_endpoint, entry_id)
        
        if isinstance(parsed_data, list) and parsed_data[0][0] == 'No start_tag or end_tag':
            logging.warning('Invalid parsed data structure, trying again with new endpoint')
            remove_endpoint(endpoint)
            n_endpoint = get_endpoint()
            return process_search_row(search_string, n_endpoint, entry_id)
        
        # Process valid parsed data
        image_url = parsed_data[0]
        image_desc = parsed_data[1]
        image_source = parsed_data[2]
        image_thumb = parsed_data[3]
        
        logging.info(f'Got image data for entry ID {entry_id}')
        
        if image_url:
            # Create DataFrame with image data
            df = pd.DataFrame({
                'ImageUrl': image_url,
                'ImageDesc': image_desc,
                'ImageSource': image_source,
                'ImageUrlThumbnail': image_thumb,
            })
            
            if not df.empty:
                # Insert EntryId column
                df.insert(0, 'EntryId', entry_id)
                
                # Insert data into database
                df.to_sql(name='utb_ImageScraperResult', con=engine, index=False, if_exists='append')
                
                # Update record status
                connection = pyodbc.connect(conn)
                cursor = connection.cursor()
                sql_query = f"UPDATE utb_ImageScraperRecords SET Step1 = GETDATE() WHERE EntryID = {entry_id}"
                cursor.execute(sql_query)
                connection.commit()
                connection.close()
                
                logging.info(f'Successfully processed and updated entry ID {entry_id}')
                return True
        
        # If we get here, there was no valid image URL
        logging.warning('No valid image URL, trying again with new endpoint')
        remove_endpoint(endpoint)
        n_endpoint = get_endpoint()
        return process_search_row(search_string, n_endpoint, entry_id)
    
    except requests.RequestException as e:
        logging.error(f"Request error: {e}")
        remove_endpoint(endpoint)
        n_endpoint = get_endpoint()
        logging.info(f"Trying again with new endpoint: {n_endpoint}")
        return process_search_row(search_string, n_endpoint, entry_id)
    
    except Exception as e:
        logging.error(f"Error processing search row: {e}")
        remove_endpoint(endpoint)
        n_endpoint = get_endpoint()
        logging.info(f"Trying again with new endpoint: {n_endpoint}")
        return process_search_row(search_string, n_endpoint, entry_id)

#################################################
# IMAGE DOWNLOAD AND PROCESSING
#################################################

def extract_domains_and_counts(data):
    """Extract domains from URLs and count their occurrences."""
    domains = [tldextract.extract(url).registered_domain for _, url, thumb in data]
    domain_counts = Counter(domains)
    return domain_counts

def analyze_data(data):
    """
    Analyze image data to determine optimal connection pool size.
    
    Args:
        data (list): List of image data to analyze
        
    Returns:
        int: Optimal connection pool size
    """
    domain_counts = extract_domains_and_counts(data)
    logger.info("Domain counts: %s", domain_counts)
    unique_domains = len(domain_counts)
    logger.info(f"Unique Domain Count: {unique_domains}")
    
    # Adjust pool size based on unique domains
    pool_size = min(500, max(10, unique_domains * 2))
    logger.info(f"Using connection pool size: {pool_size}")
    
    return pool_size

def build_headers(url):
    """
    Build request headers based on URL domain.
    
    Args:
        url (str): URL to build headers for
        
    Returns:
        dict: Headers for the request
    """
    domain_info = tldextract.extract(url)
    domain = f"{domain_info.domain}.{domain_info.suffix}"
    
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    }
    
    return headers

async def image_download(semaphore, url, thumbnail, image_name, save_path, session, fallback_formats=None):
    """
    Download an image from a URL.
    
    Args:
        semaphore (asyncio.Semaphore): Semaphore to limit concurrent downloads
        url (str): URL to download the image from
        thumbnail (str): Thumbnail URL to fall back to if main URL fails
        image_name (str): Name to save the image as
        save_path (str): Path to save the image to
        session (aiohttp.ClientSession): HTTP session to use for the request
        fallback_formats (list): List of image formats to try if the default format fails
        
    Returns:
        bool: True if download was successful, False otherwise
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9",
    }
    async with semaphore:
        if fallback_formats is None:
            fallback_formats = ['png', 'jpeg', 'gif', 'bmp', 'webp', 'avif', 'tiff', 'ico']

        logger.info(f"Initiating download for URL: {url} Img: {image_name}")
        try:
            async with session.get(url, headers=headers) as response:
                logger.info(f"Received response: {response.status} for URL: {url}")

                if response.status == 200:
                    logger.info(f"Processing content from URL: {url}")
                    data = await response.read()
                    image_data = BytesIO(data)
                    try:
                        logger.info(f"Attempting to open image stream and save as PNG for {image_name}")
                        with IMG2.open(image_data) as img:
                            final_image_path = os.path.join(save_path, f"{image_name}.png")
                            img.save(final_image_path)
                            logger.info(f"Successfully saved: {final_image_path}")
                            return True
                    except UnidentifiedImageError as e:
                        logger.error(f"Image file type unidentified, trying fallback formats for {image_name}: {e}")
                        for fmt in fallback_formats:
                            image_data.seek(0)  # Reset stream position
                            try:
                                logger.info(f"Trying to save image with fallback format {fmt} for {image_name}")
                                with IMG2.open(image_data) as img:
                                    final_image_path = os.path.join(save_path, f"{image_name}.{fmt}")
                                    img.save(final_image_path)
                                    logger.info(f"Successfully saved with fallback format {fmt}: {final_image_path}")
                                    return True
                            except Exception as fallback_exc:
                                logger.error(f"Failed with fallback format {fmt} for {image_name}: {fallback_exc}")
                        return False
                else:
                    logger.error(f"Download failed with status code {response.status} for URL: {url}")
                    await thumbnail_download(semaphore, thumbnail, image_name, save_path, session)
                    return False

        except TimeoutError as exc:
            # Handle the timeout specifically
            logger.error(f"Timeout occurred while downloading {url} Image: {image_name}")
            print('timeout error inside the download function')
            print(exc)
            return False

        except Exception as exc:
            logger.error(f"Exception occurred during download or processing for URL: {url}: {exc}", exc_info=True)
            print(exc)
            return False

async def thumbnail_download(semaphore, url, image_name, save_path, session, fallback_formats=None):
    """
    Download a thumbnail image as a fallback.
    
    Args:
        semaphore (asyncio.Semaphore): Semaphore to limit concurrent downloads
        url (str): URL to download the thumbnail from
        image_name (str): Name to save the image as
        save_path (str): Path to save the image to
        session (aiohttp.ClientSession): HTTP session to use for the request
        fallback_formats (list): List of image formats to try if the default format fails
        
    Returns:
        bool: True if download was successful, False otherwise
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9",
    }
    async with semaphore:
        if fallback_formats is None:
            fallback_formats = ['png', 'jpeg', 'gif', 'bmp', 'webp', 'avif', 'tiff', 'ico']

        logger.info(f"Initiating thumbnail download for URL: {url} Img: {image_name}")
        try:
            async with session.get(url, headers=headers) as response:
                logger.info(f"Received response: {response.status} for URL: {url}")

                if response.status == 200:
                    logger.info(f"Processing content from URL: {url}")
                    data = await response.read()
                    image_data = BytesIO(data)
                    try:
                        logger.info(f"Attempting to open image stream and save as PNG for {image_name}")
                        with IMG2.open(image_data) as img:
                            final_image_path = os.path.join(save_path, f"{image_name}.png")
                            img.save(final_image_path)
                            logger.info(f"Successfully saved: {final_image_path}")
                            return True
                    except UnidentifiedImageError as e:
                        logger.error(f"Image file type unidentified, trying fallback formats for {image_name}: {e}")
                        for fmt in fallback_formats:
                            image_data.seek(0)  # Reset stream position
                            try:
                                logger.info(f"Trying to save image with fallback format {fmt} for {image_name}")
                                with IMG2.open(image_data) as img:
                                    final_image_path = os.path.join(save_path, f"{image_name}.{fmt}")
                                    img.save(final_image_path)
                                    logger.info(f"Successfully saved with fallback format {fmt}: {final_image_path}")
                                    return True
                            except Exception as fallback_exc:
                                logger.error(f"Failed with fallback format {fmt} for {image_name}: {fallback_exc}")
                    return False
                else:
                    logger.error(f"Thumbnail download failed with status code {response.status} for URL: {url}")

        except TimeoutError:
            # Handle the timeout specifically
            logger.error(f"Timeout occurred while downloading thumbnail {url} Image: {image_name}")
            return False
        except Exception as exc:
            logger.error(f"Exception occurred during thumbnail download or processing for URL: {url}: {exc}", exc_info=True)
            return False

async def download_all_images(data, save_path):
    """
    Download all images in the data list.
    
    Args:
        data (list): List of image data to download
        save_path (str): Path to save downloaded images to
        
    Returns:
        list: List of failed downloads (URL, row ID pairs)
    """
    failed_downloads = []
    pool_size = analyze_data(data)  # Get optimal pool size based on data analysis

    logger.info(f"Setting up session with pool size: {pool_size}")

    # Setup async session with retry policy
    timeout = ClientTimeout(total=60)
    retry_options = ExponentialRetry(attempts=3, start_timeout=3)
    connector = aiohttp.TCPConnector(ssl=False, limit=pool_size)

    async with RetryClient(raise_for_status=False, retry_options=retry_options, 
                          timeout=timeout, connector=connector) as session:
        semaphore = asyncio.Semaphore(pool_size)

        logger.info("Scheduling image downloads")
        tasks = [
            image_download(semaphore, str(item[1]), str(item[2]), str(item[0]), save_path, session)
            for index, item in enumerate(data, start=1)
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        logger.info("Processing download results")
        for index, result in enumerate(results):
            if isinstance(result, Exception):
                # Try thumbnail download on failure
                logger.error(f"Download task generated an exception: {result}")
                logger.error(f"Trying again with thumbnail: {str(data[index][2])}")
                await thumbnail_download(semaphore, str(data[index][2]), str(data[index][0]), save_path, session)
                failed_downloads.append((data[index][1], data[index][0]))  # Append the image URL and row ID
            else:
                logger.info(f"Download task completed with result: {result}")
                if result is False:
                    failed_downloads.append((data[index][1], data[index][0]))  # Append the image URL and row ID

    return failed_downloads

def verify_png_image_single(image_path):
    """
    Verify that an image is a valid PNG.
    
    Args:
        image_path (str): Path to the image to verify
        
    Returns:
        bool: True if the image is valid, False otherwise
    """
    try:
        img = IMG2.open(image_path)
        img.verify()  # Verify it's a valid image
        logging.info(f"Image verified successfully: {image_path}")
    except Exception as e:
        logging.error(f"IMAGE verify ERROR: {e}, for image: {image_path}")
        return False

    imageSize = os.path.getsize(image_path)
    logging.debug(f"Image size: {imageSize} bytes")

    if imageSize < 3000:
        logging.warning(f"File may be corrupted or too small: {image_path}")
        return False

    try:
        resize_image(image_path)
    except Exception as e:
        logging.error(f"Error resizing image: {e}, for image: {image_path}")
        return False
    return True

def resize_image(image_path):
    """
    Resize an image to a maximum size.
    
    Args:
        image_path (str): Path to the image to resize
        
    Returns:
        bool: True if resizing was successful, False otherwise
    """
    try:
        img = IMG2.open(image_path)
        MAXSIZE = 145  # Maximum size in pixels
        if img:
            h, w = img.height, img.width  # original size
            logging.debug(f"Original size: height={h}, width={w}")
            if h > MAXSIZE or w > MAXSIZE:
                if h > w:
                    w = int(w * MAXSIZE / h)
                    h = MAXSIZE
                else:
                    h = int(h * MAXSIZE / w)
                    w = MAXSIZE
            logging.debug(f"Resized to: height={h}, width={w}")
            newImg = img.resize((w, h))
            newImg.save(image_path)
            logging.info(f"Image resized and saved: {image_path}")
            return True
    except Exception as e:
        logging.error(f"Error resizing image: {e}, for image: {image_path}")
        return False

def highlight_cell(excel_file, cell_reference):
    """
    Highlight a cell in an Excel file.
    
    Args:
        excel_file (str): Path to the Excel file
        cell_reference (str): Cell reference to highlight (e.g., "A1")
    """
    try:
        workbook = load_workbook(excel_file)
        sheet = workbook.active
        sheet[cell_reference].fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
        workbook.save(excel_file)
        logging.info(f"Highlighted cell {cell_reference} in {excel_file}")
    except Exception as e:
        logging.error(f"Error highlighting cell: {e}")

def write_failed_downloads_to_excel(failed_downloads, excel_file):
    """
    Write failed downloads to an Excel file.
    
    Args:
        failed_downloads (list): List of failed downloads (URL, row ID pairs)
        excel_file (str): Path to the Excel file
    """
    if failed_downloads:
        try:
            workbook = load_workbook(excel_file)
            worksheet = workbook.active
            
            for row in failed_downloads:
                url = row[0]
                row_id = row[1]
                if url and url != 'None found in this filter':
                    # Write the URL to column A of the failed row
                    cell_reference = f"{get_column_letter(1)}{row_id}"  # Column A, row number
                    worksheet[cell_reference] = str(url)
                    highlight_cell(excel_file, cell_reference)
            
            workbook.save(excel_file)
            logger.info(f"Failed downloads written to Excel file: {excel_file}")
            return True
        except Exception as e:
            logger.error(f"Error writing failed downloads to Excel: {e}")
            return False
    else:
        logger.info("No failed downloads to write to Excel.")
        return True

def write_excel_image(local_filename, temp_dir, preferred_image_method):
    """
    Write images to an Excel file.
    
    Args:
        local_filename (str): Path to the Excel file
        temp_dir (str): Path to the directory containing images
        preferred_image_method (str): Preferred method for inserting images ('append', 'overwrite', 'NewColumn')
        
    Returns:
        list: List of row numbers that failed
    """
    failed_rows = []
    try:
        # Load the workbook and select the active worksheet
        wb = load_workbook(local_filename)
        ws = wb.active
        logger.info(f"Processing images in {temp_dir} for Excel file {local_filename}")
        
        # Iterate through each file in the temporary directory
        for image_file in os.listdir(temp_dir):
            image_path = os.path.join(temp_dir, image_file)
            # Extract row number from the image file name
            try:
                # Assuming the file name can be directly converted to an integer row number
                row_number = int(image_file.split('.')[0])
                logging.info(f"Processing row {row_number}, image path: {image_path}")
            except ValueError:
                logging.warning(f"Skipping file {image_file}: does not match expected naming convention")
                continue  # Skip files that do not match the expected naming convention
            
            # Verify the image meets criteria to be added
            verify_image = verify_png_image_single(image_path)    
            if verify_image:
                logging.info('Inserting image')
                img = Image(image_path)
                # Determine the anchor point based on the preferred image method
                if preferred_image_method in ["overwrite", "append"]:
                    anchor = "A" + str(row_number)
                    logging.info('Anchor assigned')
                elif preferred_image_method == "NewColumn":
                    anchor = "B" + str(row_number)  # Example adjustment for a different method
                else:
                    logging.error(f'Unrecognized preferred image method: {preferred_image_method}')
                    continue  # Skip if the method is not recognized
                    
                img.anchor = anchor
                ws.add_image(img)
                logging.info(f'Image added at {anchor}')
            else:
                failed_rows.append(row_number)
                logging.warning('Inserting image skipped due to verify_png_image_single failure.')   
        
        # Save the workbook
        logging.info('Finished processing all images.')
        wb.save(local_filename)
        return failed_rows
    except Exception as e:
        logging.error(f"Error writing images to Excel: {e}")
        return failed_rows

def write_failed_img_urls(excel_file_path, clean_results, failed_rows):
    """
    Write failed image URLs to an Excel file.
    
    Args:
        excel_file_path (str): Path to the Excel file
        clean_results (list): List of (row, URL) pairs
        failed_rows (list): List of row numbers that failed
        
    Returns:
        list: List of row numbers that were added
    """
    added_rows = [] 
    try:
        # Load the workbook
        workbook = load_workbook(excel_file_path)
        
        # Select the active worksheet
        worksheet = workbook.active  
        
        # Convert clean_results to a dictionary for easier lookup
        clean_results_dict = {row: url for row, url in clean_results}
        
        # Iterate over the failed rows
        for row in failed_rows:
            # Look up the URL in the clean_results_dict using the row as a key
            url = clean_results_dict.get(row)
            
            if url:
                # Write the URL to column A of the failed row
                cell_reference = f"{get_column_letter(1)}{row}"  # Column A, row number
                worksheet[cell_reference] = str(url)
                highlight_cell(excel_file_path, cell_reference)
                added_rows.append(row)
        
        # Save the workbook
        workbook.save(excel_file_path)
        return added_rows
    except Exception as e:
        logging.error(f"Error writing failed image URLs to Excel: {e}")
        return added_rows

def prepare_images_for_download_dataframe(df):
    """
    Prepare images for download from a DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame containing image data
        
    Returns:
        list: List of image tuples (row, URL, thumbnail) for download
    """
    images_to_download = []
    try:
        for row in df.itertuples(index=False, name=None):
            if row[1] != 'No google image results found':
                images_to_download.append(row)
        
        logging.info(f"Prepared {len(images_to_download)} images for download")
        return images_to_download
    except Exception as e:
        logging.error(f"Error preparing images for download: {e}")
        return []

#################################################
# RAY REMOTE FUNCTIONS
#################################################

@ray.remote
def process_db_row(row):
    """
    Process a database row for image searching.
    
    Args:
        row (dict): Dictionary containing row data
        
    Returns:
        dict: Result of processing the row
    """
    try:
        entry_id = row['EntryID']
        searchString = row['SearchString']
        logger.info(f"Processing entry ID: {entry_id}, search string: {searchString}")
        
        endpoint = get_endpoint()
        result = process_search_row(searchString, endpoint, entry_id)
        
        return {"entry_id": entry_id, "status": "success" if result else "error"}
    except Exception as e:
        logger.error(f"Error processing row: {e}")
        return {"entry_id": row.get('EntryID', 'unknown'), "status": "error", "error": str(e)}

@ray.remote
def process_batch(batch):
    """
    Process a batch of database rows.
    
    Args:
        batch (list): List of dictionaries containing row data
        
    Returns:
        list: Results of processing the batch
    """
    try:
        # Process each item in the batch in parallel
        futures = [process_db_row.remote(data) for data in batch]
        logger.info(f"Submitted {len(futures)} row processing tasks")
        results = ray.get(futures)
        return results
    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        return [{"status": "error", "error": str(e)}]

#################################################
# ASYNC WORKFLOW FUNCTIONS
#################################################

async def create_temp_dirs(unique_id):
    """
    Create temporary directories for file processing.
    
    Args:
        unique_id (str): Unique identifier for the directories
        
    Returns:
        tuple: Tuple containing paths to the temporary image and Excel directories
    """
    try:
        loop = asyncio.get_running_loop()
        base_dir = os.path.join(os.getcwd(), 'temp_files')
        temp_images_dir = os.path.join(base_dir, 'images', str(unique_id))
        temp_excel_dir = os.path.join(base_dir, 'excel', str(unique_id))

        await loop.run_in_executor(None, lambda: os.makedirs(temp_images_dir, exist_ok=True))
        await loop.run_in_executor(None, lambda: os.makedirs(temp_excel_dir, exist_ok=True))

        logger.info(f"Created temporary directories for ID: {unique_id}")
        return temp_images_dir, temp_excel_dir
    except Exception as e:
        logger.error(f"Error creating temporary directories: {e}")
        raise

async def cleanup_temp_dirs(directories):
    """
    Clean up temporary directories after processing.
    
    Args:
        directories (list): List of directory paths to clean up
    """
    try:
        loop = asyncio.get_running_loop()
        for dir_path in directories:
            await loop.run_in_executor(None, lambda dp=dir_path: shutil.rmtree(dp, ignore_errors=True))
        logger.info(f"Cleaned up temporary directories: {directories}")
    except Exception as e:
        logger.error(f"Error cleaning up temporary directories: {e}")

async def generate_download_file(file_id):
    """
    Generate a download file for a processed file.
    
    Args:
        file_id (str): The FileID to generate a download file for
        
    Returns:
        dict: Result of generating the download file
    """
    try:
        preferred_image_method = 'append'
        start_time = time.time()
        loop = asyncio.get_running_loop()
        
        # Get images for Excel
        selected_images_df = await loop.run_in_executor(ThreadPoolExecutor(), get_images_excel_db, file_id)
        selected_image_list = await loop.run_in_executor(ThreadPoolExecutor(), prepare_images_for_download_dataframe, selected_images_df)
        
        logger.info(f"Selected {len(selected_image_list)} images for download")
        
        # Get file location
        provided_file_path = await loop.run_in_executor(ThreadPoolExecutor(), get_file_location, file_id)
        decoded_string = urllib.parse.unquote(provided_file_path)
        file_name = provided_file_path.split('/')[-1]
        
        # Create temporary directories
        temp_images_dir, temp_excel_dir = await create_temp_dirs(file_id)
        local_filename = os.path.join(temp_excel_dir, file_name)
        
        # Download images
        failed_img_urls = await download_all_images(selected_image_list, temp_images_dir)
        
        # Download Excel file
        response = await loop.run_in_executor(None, requests.get, provided_file_path, {'allow_redirects': True, 'timeout': 60})
        if response.status_code != 200:
            logger.error(f"Failed to download file: {response.status_code}")
            return {"error": "Failed to download the provided file."}
        
        with open(local_filename, "wb") as file:
            file.write(response.content)
        
        # Write images to Excel
        logger.info("Writing images to Excel")
        failed_rows = await loop.run_in_executor(ThreadPoolExecutor(), write_excel_image, local_filename, temp_images_dir, preferred_image_method)
        
        # Write failed downloads to Excel
        if failed_img_urls:
            await loop.run_in_executor(ThreadPoolExecutor(), write_failed_downloads_to_excel, failed_img_urls, local_filename)
            logger.warning(f"Failed to download {len(failed_img_urls)} images")
        
        # Upload file to S3
        logger.info("Uploading file to S3")
        public_url = await loop.run_in_executor(ThreadPoolExecutor(), upload_file_to_space, local_filename, file_name, True)
        
        # Update database
        await loop.run_in_executor(ThreadPoolExecutor(), update_file_location_complete, file_id, public_url)
        await loop.run_in_executor(ThreadPoolExecutor(), update_file_generate_complete, file_id)
        
        # Send email notification
        subject_line = f"{file_name} Job Notification"
        send_to_email = await loop.run_in_executor(ThreadPoolExecutor(), get_send_to_email, file_id)
        await loop.run_in_executor(ThreadPoolExecutor(), send_email, send_to_email, subject_line, public_url, file_id)
        
        # Clean up temporary directories
        logger.info("Cleaning up temporary directories")
        await cleanup_temp_dirs([temp_images_dir, temp_excel_dir])
        
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(f"Processing completed in {execution_time:.2f} seconds")
        
        return {
            "message": "Processing completed successfully.",
            "public_url": public_url
        }
    except Exception as e:
        logger.error(f"Error generating download file: {e}")
        return {"error": f"An error occurred: {str(e)}"}
async def process_restart_batch(file_id_db):
    """
    Restart processing for a file.
    
    Args:
        file_id_db (str): The FileID to restart processing for
    """
    try:
        logger.info(f"Restarting processing for FileID: {file_id_db}")
        
        # Get records to search
        search_df = get_records_to_search(file_id_db)
        
        # If there are records to search, process them
        if len(search_df) > 0:
            logger.info(f"Found {len(search_df)} records to search for FileID: {file_id_db}")
            search_list = search_df.to_dict('records')
            
            # Process in batches
            BATCH_SIZE = 100
            batches = [search_list[i:i+BATCH_SIZE] for i in range(0, len(search_list), BATCH_SIZE)]
            
            logger.info(f"Processing {len(search_list)} records in {len(batches)} batches")
            
            futures = [process_batch.remote(batch) for batch in batches]
            ray.get(futures)
        else:
            logger.info(f"No records to search for FileID: {file_id_db}, continuing with next steps")
        
        # Continue with remaining steps regardless of whether records were found
        logger.info(f"Updating sort order for FileID: {file_id_db}")
        update_sort_order(file_id_db)
        
        logger.info(f"Processing images for FileID: {file_id_db}")
        process_images(file_id_db)
        
        logger.info(f"Generating download file for FileID: {file_id_db}")
        await generate_download_file(file_id_db)
        
        logger.info(f"Restart processing completed for FileID: {file_id_db}")
    except Exception as e:
        logger.error(f"Error restarting processing for FileID {file_id_db}: {e}")

def process_image_batch(payload):
    """
    Process a batch of images from payload data.
    
    Args:
        payload (dict): Dictionary containing payload data
    """
    try:
        logger.info(f"Processing started for payload")
        
        rows = payload.get('rowData', [])
        provided_file_path = payload.get('filePath')
        file_name = provided_file_path.split('/')[-1]
        send_to_email = payload.get('sendToEmail', 'nik@iconluxurygroup.com')
        
        logger.info(f"Processing file: {file_name}, send to: {send_to_email}")
        
        # Insert file record
        file_id_db = insert_file_db(file_name, provided_file_path, send_to_email)
        
        # Load payload data
        load_payload_db(rows, file_id_db)
        
        # Match products from retail
        get_lm_products(file_id_db)
        
        # Get records to search
        search_df = get_records_to_search(file_id_db)
        
        if search_df.empty:
            logger.warning(f"No records to search for FileID: {file_id_db}")
            return
        
        search_list = search_df.to_dict('records')
        
        # Process in batches
        BATCH_SIZE = 100
        batches = [search_list[i:i+BATCH_SIZE] for i in range(0, len(search_list), BATCH_SIZE)]
        
        logger.info(f"Processing {len(search_list)} records in {len(batches)} batches")
        
        start = datetime.datetime.now()
        futures = [process_batch.remote(batch) for batch in batches]
        ray.get(futures)
        end = datetime.datetime.now()
        
        logger.info(f"Batch processing completed in {end - start}")
        
        # Update sort order and process images
        update_sort_order(file_id_db)
        process_images(file_id_db)
        
        # Generate download file
        asyncio.run(generate_download_file(file_id_db))
        
        logger.info(f"Processing completed for FileID: {file_id_db}")
    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        try:
            # Send failure notification
            send_message_email(
                send_to_email, 
                f"Error processing {file_name}", 
                f"An error occurred while processing your file: {str(e)}"
            )
        except Exception as email_error:
            logger.error(f"Failed to send error notification: {email_error}")

#################################################
# API ROUTES
#################################################

@app.post("/restart-failed-batch/")
async def api_process_restart(background_tasks: BackgroundTasks, file_id_db: str):
    """
    API route to restart processing for a file.
    
    Args:
        background_tasks: FastAPI background tasks
        file_id_db (str): The FileID to restart processing for
        
    Returns:
        dict: Result of restarting processing
    """
    try:
        logger.info(f"Received request to restart processing for FileID: {file_id_db}")
        background_tasks.add_task(process_restart_batch, file_id_db)
        return {"message": "Processing restart initiated successfully. You will be notified upon completion."}
    except Exception as e:
        logger.error(f"Error restarting processing: {e}")
        return {"error": f"An error occurred: {str(e)}"}

@app.post("/process-image-batch/")
async def api_process_payload(background_tasks: BackgroundTasks, payload: dict):
    """
    API route to process a batch of images.
    
    Args:
        background_tasks: FastAPI background tasks
        payload (dict): Dictionary containing payload data
        
    Returns:
        dict: Result of processing the batch
    """
    try:
        logger.info("Received request to process image batch")
        background_tasks.add_task(process_image_batch, payload)
        return {"message": "Processing started successfully. You will be notified upon completion."}
    except Exception as e:
        logger.error(f"Error processing payload: {e}")
        return {"error": f"An error occurred: {str(e)}"}

@app.post("/generate-download-file/")
async def api_process_file(background_tasks: BackgroundTasks, file_id: int):
    """
    API route to generate a download file.
    
    Args:
        background_tasks: FastAPI background tasks
        file_id (int): The FileID to generate a download file for
        
    Returns:
        dict: Result of generating the download file
    """
    try:
        logger.info(f"Received request to generate download file for FileID: {file_id}")
        background_tasks.add_task(generate_download_file, str(file_id))
        return {"message": "Processing started successfully. You will be notified upon completion."}
    except Exception as e:
        logger.error(f"Error processing file: {e}")
        return {"error": f"An error occurred: {str(e)}"}

#################################################
# MAIN ENTRY POINT
#################################################

if __name__ == "__main__":
    logger.info("Starting Uvicorn server")
    
    # Initialize Ray if not already initialized
    if ray.is_initialized():
        ray.shutdown()
    ray.init(address='auto')
    
    # Start the FastAPI server
    import uvicorn
    uvicorn.run("main:app", port=8080, host='0.0.0.0')
    # API headers
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer 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"
    }
  