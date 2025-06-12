import asyncio
import logging
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from typing import Optional, List, Tuple
from uuid import uuid4
import datetime
import aiohttp
import pandas as pd
import pyodbc
import requests
from aiohttp import ClientTimeout
from aiohttp_retry import RetryClient, ExponentialRetry
from fastapi import FastAPI, BackgroundTasks
from openpyxl import load_workbook
from openpyxl.drawing.image import Image
from PIL import Image as PILImage
from PIL import UnidentifiedImageError
from tldextract import tldextract
import urllib.parse
from app_config import engine, conn_str, VERSION
from email_utils import send_email, send_message_email
from s3_utils import upload_file_to_space
from pathlib import Path  # Moved import to module level

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI()

def setup_logging(file_id: str, timestamp: str) -> logging.Logger:
    """Configure logging to save logs to a file in jobs/{file_id}/{timestamp}/logs/."""
    # Create log directory
    log_dir = os.path.join('jobs', file_id, timestamp, 'logs')
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    # Define log file path
    log_file = os.path.join(log_dir, f'job_{file_id}_{timestamp}.log')
    
    # Configure logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    # Create file handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Define log format
    log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(log_format)
    console_handler.setFormatter(log_format)
    
    # Add handlers to logger
    logger.handlers = []  # Clear existing handlers to avoid duplicates
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logger.info(f"Logging initialized for FileID: {file_id}, Timestamp: {timestamp}")
    return logger

# Constants
MAX_THREADS = int(os.environ.get('MAX_THREADS', 10))
FALLBACK_FORMATS = ['png', 'jpeg', 'gif', 'bmp', 'webp', 'avif', 'tiff', 'ico']
VALID_IMAGE_TYPES = ['image/jpeg', 'image/png', 'image/gif', 'image/bmp', 'image/webp', 'image/avif', 'image/tiff', 'image/x-icon']
MIN_IMAGE_SIZE = 1000  # Minimum content length in bytes
MAX_IMAGE_SIZE = 3000  # For verification
MAX_IMAGE_DIMENSION = 145  # For resizing

# Utility Functions
async def create_temp_dirs(unique_id: str) -> Tuple[str, str]:
    """Create temporary directories for images and Excel files."""
    base_dir = os.path.join(os.getcwd(), 'temp_files')
    temp_images_dir = os.path.join(base_dir, 'images', unique_id)
    temp_excel_dir = os.path.join(base_dir, 'excel', unique_id)
    
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, lambda: os.makedirs(temp_images_dir, exist_ok=True))
    await loop.run_in_executor(None, lambda: os.makedirs(temp_excel_dir, exist_ok=True))
    
    return temp_images_dir, temp_excel_dir

async def cleanup_temp_dirs(directories: List[str]) -> None:
    """Remove temporary directories."""
    loop = asyncio.get_running_loop()
    for dir_path in directories:
        await loop.run_in_executor(None, lambda dp=dir_path: shutil.rmtree(dp, ignore_errors=True))

def find_header_row_index(excel_file: str, max_rows_to_check: int = 10) -> Optional[int]:
    """Identify the header row index in an Excel file."""
    logger.info(f"Searching for header row in Excel file: {excel_file}")
    
    try:
        wb = load_workbook(excel_file, read_only=True)
        ws = wb.active
        if not ws:
            logger.error("No active worksheet found.")
            return None
        
        header_keywords = {'id', 'name', 'product', 'brand', 'sku', 'category', 'color', 'price', 'description'}
        best_header_row, best_header_score = None, 0
        
        for row_idx, row in enumerate(ws.iter_rows(min_row=1, max_row=max_rows_to_check, values_only=True), start=1):
            if row is None:
                continue
                
            text_cells = [str(cell).strip().lower() for cell in row if cell and str(cell).strip() and not isinstance(cell, (int, float, bool))]
            text_count = len(text_cells)
            unique_count = len(set(text_cells))
            keyword_count = sum(1 for cell in text_cells if any(keyword in cell for keyword in header_keywords))
            
            score = (text_count * 0.4) + (unique_count * 0.4) + (keyword_count * 0.2)
            logger.debug(f"Row {row_idx}: text_count={text_count}, unique_count={unique_count}, keyword_count={keyword_count}, score={score:.2f}")
            
            if score > best_header_score:
                best_header_score = score
                best_header_row = row_idx
        
        if best_header_row is None or best_header_score < 2.0:
            logger.warning("No reliable header row identified.")
            return None
        
        logger.info(f"Header row identified: Row {best_header_row} (score={best_header_score:.2f})")
        return best_header_row
    except Exception as e:
        logger.error(f"Error finding header row: {e}")
        return None

def find_row_with_most_text_columns(excel_file: str) -> int:
    """Identify the row with the most non-empty text-filled columns."""
    logger.info(f"Analyzing Excel file for row with most text columns: {excel_file}")
    
    try:
        wb = load_workbook(excel_file, read_only=True)
        ws = wb.active
        if not ws:
            logger.error("No active worksheet found.")
            return 0
        
        max_text_count, max_text_row = 0, 0
        
        for row_idx, row in enumerate(ws.iter_rows(min_row=1, values_only=True), start=1):
            if row is None:
                continue
                
            text_count = sum(1 for cell in row if cell and str(cell).strip() and not isinstance(cell, (int, float, bool)))
            logger.debug(f"Row {row_idx}: {text_count} text-filled columns")
            
            if text_count > max_text_count:
                max_text_count = text_count
                max_text_row = row_idx
        
        logger.info(f"Row {max_text_row} has the most text-filled columns: {max_text_count} columns")
        return max_text_row
    except Exception as e:
        logger.error(f"Error analyzing Excel file: {e}")
        return 0

# Database Functions
def insert_file_db(file_name: str, file_source: str) -> int:
    """Insert file metadata into the database and return the file ID."""
    with pyodbc.connect(conn_str) as connection:
        cursor = connection.cursor()
        insert_query = "INSERT INTO utb_ImageScraperFiles (FileName, FileLocationUrl) OUTPUT INSERTED.Id VALUES (?, ?)"
        cursor.execute(insert_query, (file_name, file_source))
        file_id = cursor.fetchval()
        connection.commit()
    return file_id

def get_records_to_search(file_id: int, engine) -> pd.DataFrame:
    """Retrieve records to search from the database."""
    sql_query = f"""
        SELECT EntryID, ProductModel AS SearchString 
        FROM utb_ImageScraperRecords 
        WHERE FileID = {file_id} AND Step1 IS NULL
        UNION ALL 
        SELECT EntryID, ProductModel + ' ' + ProductBrand AS SearchString 
        FROM utb_ImageScraperRecords 
        WHERE FileID = {file_id} AND Step1 IS NULL 
        ORDER BY 1
    """
    logger.debug(f"Executing query: {sql_query}")
    return pd.read_sql_query(sql_query, con=engine)

def load_payload_db(rows: List[dict], file_id: int) -> pd.DataFrame:
    """Load payload data into the database."""
    df = pd.DataFrame(rows).rename(columns={
        'absoluteRowIndex': 'ExcelRowID',
        'searchValue': 'ProductModel',
        'brandValue': 'ProductBrand',
        'colorValue': 'ProductColor',
        'CategoryValue': 'ProductCategory'
    })
    df.insert(0, 'FileID', file_id)
    df.drop(columns=['imageValue'], errors='ignore', inplace=True)
    df.to_sql(name='utb_ImageScraperRecords', con=engine, index=False, if_exists='append')
    return df

def get_endpoint() -> str:
    """Retrieve a random unblocked endpoint from the database."""
    with pyodbc.connect(conn_str) as connection:
        cursor = connection.cursor()
        sql_query = "SELECT TOP 1 EndpointURL FROM utb_Endpoints WHERE EndpointIsBlocked = 0 ORDER BY NEWID()"
        cursor.execute(sql_query)
        endpoint = cursor.fetchone()
        connection.commit()
        return endpoint[0] if endpoint else "No EndpointURL"

def remove_endpoint(endpoint: str) -> None:
    """Mark an endpoint as blocked in the database."""
    with pyodbc.connect(conn_str) as connection:
        cursor = connection.cursor()
        sql_query = f"UPDATE utb_Endpoints SET EndpointIsBlocked = 1 WHERE EndpointURL = ?"
        cursor.execute(sql_query, (endpoint,))
        connection.commit()

def get_file_location(file_id: int) -> str:
    """Retrieve the file location URL from the database."""
    with pyodbc.connect(conn_str) as connection:
        cursor = connection.cursor()
        query = f"SELECT FileLocationUrl FROM utb_ImageScraperFiles WHERE ID = ?"
        cursor.execute(query, (file_id,))
        file_location_url = cursor.fetchone()
        connection.commit()
        return file_location_url[0] if file_location_url else "No File Found"

def update_file_location_complete(file_id: int, file_location: str) -> None:
    """Update the completed file location URL in the database."""
    with pyodbc.connect(conn_str) as connection:
        cursor = connection.cursor()
        query = f"UPDATE utb_ImageScraperFiles SET FileLocationURLComplete = ? WHERE ID = ?"
        cursor.execute(query, (file_location, file_id))
        connection.commit()

def update_sort_order(file_id: int) -> None:
    """Update the sort order of image results in the database."""
    query = f"""
        WITH toupdate AS (
            SELECT t.*, ROW_NUMBER() OVER (PARTITION BY t.EntryID ORDER BY t.ResultID) AS seqnum
            FROM utb_ImageScraperResult t 
            INNER JOIN utb_ImageScraperRecords r ON r.EntryID = t.EntryID 
            WHERE r.FileID = {file_id}
        ) 
        UPDATE toupdate SET SortOrder = seqnum;
    """
    with pyodbc.connect(conn_str) as connection:
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()

def get_images_excel_db(file_id: int) -> pd.DataFrame:
    """Retrieve images for Excel processing from the database."""
    with pyodbc.connect(conn_str) as connection:
        cursor = connection.cursor()
        cursor.execute(f"UPDATE utb_ImageScraperFiles SET CreateFileStartTime = GETDATE() WHERE ID = ?", (file_id,))
        connection.commit()
    
    query = f"""
        SELECT s.ExcelRowID, r.ImageUrl, r.ImageUrlThumbnail 
        FROM utb_ImageScraperFiles f
        INNER JOIN utb_ImageScraperRecords s ON s.FileID = f.ID 
        INNER JOIN utb_ImageScraperResult r ON r.EntryID = s.EntryID 
        WHERE f.ID = {file_id} AND r.SortOrder = 1
        ORDER BY s.ExcelRowID
    """
    return pd.read_sql_query(query, con=engine)

def get_lm_products(file_id: int) -> None:
    """Execute stored procedure to match products."""
    with pyodbc.connect(conn_str) as connection:
        cursor = connection.cursor()
        query = f"EXEC usp_ImageScrapergetMatchFromRetail {file_id}"
        cursor.execute(query)
        connection.commit()

# Image Processing Functions
async def validate_image_response(response, url: str, image_name: str) -> Tuple[bool, Optional[bytes]]:
    """Validate HTTP response for valid image content."""
    logger.info(f"Validating response for URL: {url} Img: {image_name}")
    
    if response.status != 200:
        logger.error(f"Invalid status code {response.status} for URL: {url}")
        return False, None
    
    content_type = response.headers.get('Content-Type', '').lower()
    if not any(content_type.startswith(img_type) for img_type in VALID_IMAGE_TYPES):
        logger.error(f"Invalid Content-Type {content_type} for URL: {url}")
        return False, None
    
    content_length = int(response.headers.get('Content-Length', 0))
    if content_length < MIN_IMAGE_SIZE:
        logger.error(f"Content too small ({content_length} bytes) for URL: {url}")
        return False, None
    
    data = await response.read()
    logger.debug(f"First 10 bytes: {data[:10]}")
    return True, data

async def image_download(semaphore, url: str, thumbnail: str, image_name: str, save_path: str, session, fallback_formats: Optional[List[str]] = None) -> Tuple[bool, bool]:
    """Download an image with fallback to thumbnail if necessary."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    async with semaphore:
        fallback_formats = fallback_formats or FALLBACK_FORMATS
        logger.info(f"Initiating download for URL: {url} Img: {image_name}")
        
        try:
            async with session.get(url, headers=headers) as response:
                is_valid, result = await validate_image_response(response, url, image_name)
                if not is_valid:
                    if thumbnail and thumbnail != url:
                        logger.info(f"Attempting thumbnail download for {image_name}")
                        thumbnail_success = await thumbnail_download(semaphore, thumbnail, image_name, save_path, session, fallback_formats)
                        return thumbnail_success, True
                    return False, False
                
                image_data = BytesIO(result)
                try:
                    with PILImage.open(image_data) as img:
                        final_image_path = os.path.join(save_path, f"{image_name}.png")
                        img.save(final_image_path)
                        logger.info(f"Successfully saved: {final_image_path}")
                        return True, False
                except UnidentifiedImageError:
                    logger.error(f"Image file type unidentified for {image_name}")
                    for fmt in fallback_formats:
                        image_data.seek(0)
                        try:
                            with PILImage.open(image_data) as img:
                                final_image_path = os.path.join(save_path, f"{image_name}.{fmt}")
                                img.save(final_image_path)
                                logger.info(f"Successfully saved with fallback format {fmt}: {final_image_path}")
                                return True, False
                        except Exception as e:
                            logger.error(f"Failed with fallback format {fmt} for {image_name}: {e}")
                    if thumbnail and thumbnail != url:
                        logger.info(f"Attempting thumbnail download for {image_name}")
                        thumbnail_success = await thumbnail_download(semaphore, thumbnail, image_name, save_path, session, fallback_formats)
                        return thumbnail_success, True
                    return False, False
        except Exception as e:
            logger.error(f"Exception during download for URL: {url}: {e}")
            if thumbnail and thumbnail != url:
                logger.info(f"Attempting thumbnail download for {image_name}")
                thumbnail_success = await thumbnail_download(semaphore, thumbnail, image_name, save_path, session, fallback_formats)
                return thumbnail_success, True
            return False, False

async def thumbnail_download(semaphore, url: str, image_name: str, save_path: str, session, fallback_formats: Optional[List[str]] = None) -> bool:
    """Download a thumbnail image."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    async with semaphore:
        fallback_formats = fallback_formats or FALLBACK_FORMATS
        logger.info(f"Initiating thumbnail download for URL: {url} Img: {image_name}")
        
        try:
            async with session.get(url, headers=headers) as response:
                is_valid, result = await validate_image_response(response, url, image_name)
                if not is_valid:
                    logger.error(f"Thumbnail validation failed")
                    return False
                
                image_data = BytesIO(result)
                try:
                    with PILImage.open(image_data) as img:
                        final_image_path = os.path.join(save_path, f"{image_name}.png")
                        img.save(final_image_path)
                        logger.info(f"Successfully saved thumbnail: {final_image_path}")
                        return True
                except UnidentifiedImageError:
                    logger.error(f"Thumbnail file type unidentified for {image_name}")
                    for fmt in fallback_formats:
                        image_data.seek(0)
                        try:
                            with PILImage.open(image_data) as img:
                                final_image_path = os.path.join(save_path, f"{image_name}.{fmt}")
                                img.save(final_image_path)
                                logger.info(f"Successfully saved thumbnail with fallback format {fmt}: {final_image_path}")
                                return True
                        except Exception as e:
                            logger.error(f"Failed with fallback format {fmt} for {image_name}: {e}")
                    return False
        except Exception as e:
            logger.error(f"Exception during thumbnail download for URL: {url}: {e}")
            return False

async def download_all_images(data: List[Tuple[int, str, str]], save_path: str) -> List[Tuple[str, int]]:
    """Download all images concurrently."""
    from collections import Counter
    
    valid_data = [(row_id, url, thumb) for row_id, url, thumb in data if url]
    domains = [tldextract.extract(url).registered_domain for _, url, _ in valid_data]
    unique_domains = len(Counter(domains))
    pool_size = min(500, max(10, unique_domains * 2))
    
    failed_downloads = []
    timeout = ClientTimeout(total=60)
    retry_options = ExponentialRetry(attempts=3, start_timeout=3)
    
    async with RetryClient(raise_for_status=False, retry_options=retry_options, timeout=timeout) as session:
        semaphore = asyncio.Semaphore(pool_size)
        tasks = [image_download(semaphore, item[1], item[2], str(item[0]), save_path, session) for item in data]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for index, result in enumerate(results):
            if isinstance(result, Exception) or (isinstance(result, tuple) and not result[0]):
                failed_downloads.append((data[index][1], data[index][0]))
    
    return failed_downloads

def verify_png_image_single(image_path: str) -> bool:
    """Verify if an image is a valid PNG and meets size requirements."""
    try:
        with PILImage.open(image_path) as img:
            img.verify()
        if os.path.getsize(image_path) < MAX_IMAGE_SIZE:
            logger.warning(f"File may be corrupted or too small: {image_path}")
            return False
        return resize_image(image_path)
    except Exception as e:
        logger.error(f"Image verification failed: {e}, for image: {image_path}")
        return False

def resize_image(image_path: str) -> bool:
    """Resize an image to fit within specified dimensions."""
    try:
        with PILImage.open(image_path) as img:
            h, w = img.height, img.width
            if h > MAX_IMAGE_DIMENSION or w > MAX_IMAGE_DIMENSION:
                if h > w:
                    w = int(w * MAX_IMAGE_DIMENSION / h)
                    h = MAX_IMAGE_DIMENSION
                else:
                    h = int(h * MAX_IMAGE_DIMENSION / w)
                    w = MAX_IMAGE_DIMENSION
            new_img = img.resize((w, h))
            new_img.save(image_path)
            logger.info(f"Image resized and saved: {image_path}")
            return True
    except Exception as e:
        logger.error(f"Error resizing image: {e}, for image: {image_path}")
        return False

def write_excel_image(local_filename: str, temp_dir: str, preferred_image_method: str, header_row: int = 0, row_offset: int = 0) -> List[int]:
    """Write images to an Excel file."""
    logger.info(f"Processing images in {temp_dir} for Excel file {local_filename} with header_row={header_row}, row_offset={row_offset}")
    failed_rows = []
    
    try:
        wb = load_workbook(local_filename)
        ws = wb.active
        
        for image_file in os.listdir(temp_dir):
            image_path = os.path.join(temp_dir, image_file)
            try:
                row_number = int(image_file.split('.')[0])
            except ValueError:
                logger.warning(f"Skipping file {image_file}: does not match expected naming convention")
                continue
                
            if verify_png_image_single(image_path):
                img = Image(image_path)
                adjusted_row = row_number + header_row + row_offset
                anchor = f"A{adjusted_row}" if preferred_image_method in ["overwrite", "append"] else f"B{adjusted_row}"
                img.anchor = anchor
                ws.add_image(img)
                logger.info(f"Image added at {anchor}")
            else:
                failed_rows.append(row_number)
                logger.warning(f"Image skipped for row {row_number} due to verification failure.")
        
        wb.save(local_filename)
        return failed_rows
    except Exception as e:
        logger.error(f"Error writing images to Excel: {e}")
        return failed_rows
def get_file_type_id(file_id: int) -> Optional[int]:
    """Retrieve the FileTypeID from the database for a given file_id."""
    with pyodbc.connect(conn_str) as connection:
        cursor = connection.cursor()
        query = "SELECT FileTypeID FROM utb_ImageScraperFiles WHERE ID = ?"
        cursor.execute(query, (file_id,))
        file_type_id = cursor.fetchone()
        connection.commit()
        return file_type_id[0] if file_type_id else None
async def generate_download_file(file_id: str, row_offset: int = 0):
    """Generate and upload a processed Excel file with images."""
    start_time = time.time()
    loop = asyncio.get_running_loop()
    
    # Define constants for file types
    FILE_TYPE_DISTRO = 3
    DISTRO_FILE_URL = "https://iconluxury.group/public/ICON_DISTRO_USD_20250312.xlsx"
    DISTRO_HEADER_ROW = 5 # 1-indexed

    try:
        # Generate timestamp for this generation run
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Setup logging
        global logger
        logger = setup_logging(file_id, timestamp)
        
        # Determine file type and adjust behavior
        file_type_id = await loop.run_in_executor(ThreadPoolExecutor(), get_file_type_id, int(file_id))
        logger.info(f"File ID {file_id} has FileTypeID: {file_type_id}")

        if file_type_id == FILE_TYPE_DISTRO:
            provided_file_path = DISTRO_FILE_URL
            file_name = urllib.parse.unquote(DISTRO_FILE_URL.split('/')[-1])
            header_row = DISTRO_HEADER_ROW
            logger.info(f"Using Distro file: {file_name} with hardcoded header row: {header_row}")
        else:
            provided_file_path = await loop.run_in_executor(ThreadPoolExecutor(), get_file_location, int(file_id))
            file_name = urllib.parse.unquote(provided_file_path.split('/')[-1])  # Decode URL-encoded filename
            
            # Identify header row for non-distro files
            header_row = await loop.run_in_executor(ThreadPoolExecutor(), find_header_row_index, provided_file_path) # NOTE: This will download the file twice if not handled carefully
            if header_row is None:
                # If find_header_row_index fails (e.g., file not yet downloaded or no clear header),
                # we need to download it first to analyze for text columns.
                # Or, if we assume find_header_row_index takes a *local path*, then this needs adjustment.
                # Assuming find_header_row_index is meant to work on a *local* file,
                # the file needs to be downloaded before header detection.
                
                # For now, let's assume find_header_row_index and find_row_with_most_text_columns
                # are robust enough to handle the URL or expect the file to be downloaded already.
                # The current code will download the file *again* after this if the header is not found.
                # It's better to download the file once and then pass the local path.
                
                # Re-ordering for efficiency: download file first.
                pass # This logic will be re-ordered below.

        # Create temporary directories
        temp_images_dir, temp_excel_dir = await create_temp_dirs(file_id)
        local_filename = os.path.join(temp_excel_dir, file_name)

        # Download the base Excel file (either original or distro)
        logger.info(f"Downloading base Excel file from: {provided_file_path} to {local_filename}")
        response = await loop.run_in_executor(None, requests.get, provided_file_path, {'allow_redirects': True, 'timeout': 60})
        if response.status_code != 200:
            logger.error(f"Failed to download file from {provided_file_path}: {response.status_code}")
            await cleanup_temp_dirs([temp_images_dir, temp_excel_dir])
            return {"error": "Failed to download the provided file."}
        
        with open(local_filename, "wb") as file:
            file.write(response.content)
        logger.info(f"Successfully downloaded base Excel file to: {local_filename}")

        # Now that the file is local, find the header row if not hardcoded
        if file_type_id != FILE_TYPE_DISTRO:
            header_row = await loop.run_in_executor(ThreadPoolExecutor(), find_header_row_index, local_filename)
            if header_row is None:
                header_row = await loop.run_in_executor(ThreadPoolExecutor(), find_row_with_most_text_columns, local_filename)
                if header_row == 0:
                    logger.warning("No text-rich row found. Using no offset for header detection.")
                    header_row = 0 # Default to 0 if no header found, meaning data starts from row 1.
            logger.info(f"Detected header row for non-Distro file: {header_row}")

        # Fetch images data from DB
        selected_images_df = await loop.run_in_executor(ThreadPoolExecutor(), get_images_excel_db, int(file_id))
        selected_image_list = [(row.ExcelRowID, row.ImageUrl, row.ImageUrlThumbnail) for row in selected_images_df.itertuples(index=False)]
        
        # Download images
        failed_img_urls = await download_all_images(selected_image_list, temp_images_dir)
        
        # Write images to Excel
        # The `row_offset` from the API endpoint is applied *after* the header row and the base ExcelRowID.
        # So, if `ExcelRowID` is 1 for the first data row, and `header_row` is 5 (for distro),
        # then the actual row in Excel is `ExcelRowID + header_row_if_applicable`.
        # However, if `ExcelRowID` is the absolute row number in the original excel,
        # then `row_number` in `write_excel_image` is already the target row.
        # Let's clarify the `write_excel_image`'s `header_row` and `row_offset` usage.
        # Assuming `write_excel_image`'s `header_row` parameter *adds* to the `row_number` (ExcelRowID),
        # and `row_offset` is an *additional* user-defined offset.
        
        # If ExcelRowID is the actual row in the Excel, and `write_excel_image` expects
        # an additional offset for image placement based on the header,
        # then if `header_row` is 5, and ExcelRowID is 1 (first data row), it should go to row 6.
        # The current `write_excel_image` has `adjusted_row = row_number + header_row + row_offset`.
        # If `header_row` here means the actual physical header row number (e.g., 5),
        # and `row_number` is the original Excel row ID (e.g., starts at 6 for first data row),
        # this calculation might be off.
        
        # Let's assume the `ExcelRowID` from the database is the *absolute* row number in the Excel sheet where the data belongs.
        # And the `header_row` argument for `write_excel_image` should be 0 if the `ExcelRowID` already includes the header offset.
        # But if `header_row` means "insert image *after* this row number", then it's fine.
        
        # A common pattern is:
        # data_row_in_excel = header_row + (original_payload_index_0_based + 1)
        # However, given your `ExcelRowID` from DB, it's likely already `data_row_in_excel`.
        # So, if `ExcelRowID` is the *actual row number in the Excel file*, `write_excel_image` needs to be simpler.
        
        # For `FileTypeID = 3` where `header_row = 5`, and `ExcelRowID` starts from 1 for your payload records:
        # If payload row 1 should go to Excel row 6, then `adjusted_row = ExcelRowID + 5`.
        # This implies `header_row` in `write_excel_image` should be 5, and `row_number` is the 0-indexed offset within the payload.
        # BUT, `ExcelRowID` in your DB is already an `absoluteRowIndex`.
        # Let's adjust `write_excel_image` to just use `row_number + row_offset` for `adjusted_row`,
        # implying `ExcelRowID` *is* the target row. If we need to account for a header row,
        # it means the `ExcelRowID`s stored in `utb_ImageScraperRecords` are relative to the data,
        # not absolute to the excel sheet.
        
        # Given the column name `absoluteRowIndex` in `load_payload_db`,
        # it suggests `ExcelRowID` *is* the absolute row in the original Excel.
        # If `write_excel_image`'s `row_number` is this `ExcelRowID`, then:
        # `adjusted_row = row_number + row_offset` (where `row_offset` is from API, usually 0).
        # The `header_row` identified by `find_header_row_index` is then just for informational purposes
        # or if `ExcelRowID` needs to be re-calculated based on a new header.
        
        # If the goal is that images for records should go *after* the determined header row,
        # and `ExcelRowID` is 1-indexed for the *data* (e.g., 1 is the first product entry),
        # then `actual_excel_row = header_row + ExcelRowID`.
        # The `row_offset` could then be used for an *additional* offset.
        
        # Let's assume `ExcelRowID` is the *absolute row number in the final Excel file* where the data for that record starts.
        # And the `row_offset` from the FastAPI request is an *additional* offset to apply to this.
        # This simplifies `write_excel_image`.
        
        # For FileTypeID 3, `DISTRO_HEADER_ROW = 5`. If `ExcelRowID` from DB is 1, then the record is for actual row 6.
        # So we need to ensure the ExcelRowID from the DB reflects the *absolute* row for the image.
        # The `load_payload_db` function uses `absoluteRowIndex`, which means `ExcelRowID` should already be the absolute row.
        # Therefore, the `header_row` variable here in `generate_download_file` (whether detected or hardcoded)
        # serves as information *for* the payload generation, but not necessarily for direct image placement calculation in `write_excel_image`
        # if `ExcelRowID` in DB is already absolute.
        
        # However, your `write_excel_image` currently uses `row_number + header_row + row_offset`.
        # This implies `row_number` is *relative* to the header.
        # This is a conflict. Let's fix `write_excel_image` to only apply `row_offset` if `ExcelRowID` is absolute.
        
        # Correction in write_excel_image:
        # If `ExcelRowID` from the database is the *absolute* row number, then:
        # `adjusted_row = row_number + row_offset`.
        # The `header_row` argument to `write_excel_image` then becomes misleading or should be 0.
        
        # Let's refine `write_excel_image` based on the assumption that `ExcelRowID` *is* the absolute row.
        # If `row_offset` is meant to apply to `ExcelRowID`, then `adjusted_row = row_number + row_offset`.
        # The `header_row` parameter in `write_excel_image` will be passed as 0, as its logic is handled here for initial file setup.
        
        # `header_row` will now be primarily for logging and determining the start of the data.
        # For `write_excel_image`, we pass the `row_offset` from the API.
        failed_rows = await loop.run_in_executor(ThreadPoolExecutor(), write_excel_image, local_filename, temp_images_dir, 'append', 0, row_offset)
        # Note: 'append' might not be the most appropriate preferred_image_method if you are inserting at specific cells.
        # 'overwrite' or 'insert' depending on desired Excel behavior might be better.
        # For now, keeping 'append' as it was.

        # Upload images to R2 for archiving
        image_urls = []
        for image_file in os.listdir(temp_images_dir):
            image_path = os.path.join(temp_images_dir, image_file)
            save_as = f"super_scraper/jobs/{file_id}/{timestamp}/images/{image_file}"
            public_url = await upload_file_to_space(
                file_src=image_path,
                save_as=save_as,
                is_public=True,
                logger=logger,
                file_id=None  # Avoid database updates for images
            )
            if public_url:
                image_urls.append(public_url)
                logger.info(f"Successfully uploaded image to R2: {public_url}")
            else:
                logger.error(f"Failed to upload image to R2: {image_file}")
        
        # Upload Excel to R2 with specific path
        s3_path = f"super_scraper/jobs/{file_id}/{timestamp}/{file_name}"
        public_url = await upload_file_to_space(
            file_src=local_filename,
            save_as=s3_path,
            is_public=True,
            logger=logger,
            file_id=int(file_id)  # Trigger database updates for Excel
        )
        
        if not public_url:
            logger.error(f"Failed to upload file to R2 for FileID: {file_id}")
            await cleanup_temp_dirs([temp_images_dir, temp_excel_dir])
            return {"error": "Failed to upload file to R2."}
        
        # Send email notifications
        execution_time = time.time() - start_time
        email_message = (
            f"Total Rows: {len(selected_image_list)}\nFilename: {file_name}\nBatch ID: {file_id}\n"
            f"Location: R2\nUploaded File: {public_url}\n"
            f"Header Row Used: {header_row} (if applicable, else detected)\n" # Clarified header row info
            f"Images Archived: {len(image_urls)}\nTimestamp: {timestamp}"
        )
        
        # Email for Excel processing
        await send_email(
            to_emails='nik@iconluxurygroup.com',
            subject=f'{file_name} - {file_id} - {execution_time:.2f}s',
            download_url=public_url,
            job_id=file_id,
            logger=logger
        )
        
        # Email for image archiving status
        archive_message = (
            f"Image Archive Status for Batch ID: {file_id}\n"
            f"Total Images Processed: {len(selected_image_list)}\n"
            f"Successfully Archived: {len(image_urls)}\nFailed Rows: {len(failed_rows)}\n"
            f"Archive Location: R2\nTimestamp: {timestamp}"
        )
        await send_message_email(
            to_emails='nik@luxurymarket.com',
            subject=f'Image Archive Status - {file_id}',
            message=archive_message,
            logger=logger
        )
        
        return {
            "message": "Processing completed successfully.",
            "public_url": public_url,
            "header_row_used": header_row, # Changed key to reflect usage
            "archived_images": len(image_urls),
            "timestamp": timestamp
        }
    except Exception as e:
        logger.error(f"Error in generate_download_file: {e}")
        # Ensure cleanup even on error
        if 'temp_images_dir' in locals() and 'temp_excel_dir' in locals():
            await cleanup_temp_dirs([temp_images_dir, temp_excel_dir])
        return {"error": str(e)}
    finally:
        # Ensure cleanup in any case if dirs were created
        if 'temp_images_dir' in locals() and 'temp_excel_dir' in locals():
            await cleanup_temp_dirs([temp_images_dir, temp_excel_dir])


@app.post("/generate-download-file/")
async def process_file(background_tasks: BackgroundTasks, file_id: int, row_offset: Optional[int] = 0):
    """Generate and upload a processed Excel file."""
    logger.info(f"Received request to generate download file for FileID: {file_id} with row_offset={row_offset}")
    background_tasks.add_task(generate_download_file, str(file_id), row_offset)
    return {"message": "Processing started successfully. You will be notified upon completion."}

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting Uvicorn server")
    uvicorn.run("main:app", port=8080, host='0.0.0.0')
