import asyncio
import logging
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from typing import Optional, List, Tuple, Dict
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
from pathlib import Path
import numpy as np
from collections import Counter

# --- Production Imports ---
# These are assumed to be in your project structure
from app_config import engine, conn_str
from email_utils import send_email, send_message_email
from s3_utils import upload_file_to_space

# --- Global Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
app = FastAPI()

# --- Constants ---
MAX_THREADS = int(os.environ.get('MAX_THREADS', 10))
VALID_IMAGE_TYPES = ['image/jpeg', 'image/png', 'image/gif', 'image/bmp', 'image/webp', 'image/avif', 'image/tiff', 'image/x-icon']
MIN_IMAGE_SIZE = 1000  # Minimum content length in bytes
MAX_IMAGE_VERIFY_SIZE = 3000 # For verification before processing
MAX_IMAGE_DIMENSION = 130   # For resizing

# --- General Utility Functions ---
def setup_logging(file_id: str, timestamp: str) -> logging.Logger:
    """Configure logging to save logs to a file for a specific job run."""
    log_dir = os.path.join('jobs', file_id, timestamp, 'logs')
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_file = os.path.join(log_dir, f'job_{file_id}_{timestamp}.log')
    
    logger_instance = logging.getLogger(f"job_{file_id}_{timestamp}")
    logger_instance.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    logger_instance.handlers = [file_handler, console_handler]
    logger_instance.propagate = False
    
    logger_instance.info(f"Logging initialized for FileID: {file_id}, Timestamp: {timestamp}")
    return logger_instance

async def create_temp_dirs(unique_id: str) -> Tuple[str, str]:
    """Create temporary directories for a job."""
    base_dir = Path.cwd() / 'temp_files'
    temp_images_dir = base_dir / 'images' / unique_id
    temp_excel_dir = base_dir / 'excel' / unique_id
    
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, temp_images_dir.mkdir, True, True)
    await loop.run_in_executor(None, temp_excel_dir.mkdir, True, True)
    
    return str(temp_images_dir), str(temp_excel_dir)

async def cleanup_temp_dirs(directories: List[str]):
    """Remove temporary directories."""
    loop = asyncio.get_running_loop()
    for dir_path in directories:
        await loop.run_in_executor(None, lambda dp=dir_path: shutil.rmtree(dp, ignore_errors=True))

# --- Database Functions ---
def get_file_type_id(file_id: int, logger_instance: logging.Logger) -> Optional[int]:
    """Retrieve the FileTypeID from the database."""
    with pyodbc.connect(conn_str) as connection:
        cursor = connection.cursor()
        query = "SELECT FileTypeID FROM utb_ImageScraperFiles WHERE ID = ?"
        cursor.execute(query, (file_id,))
        result = cursor.fetchone()
        logger_instance.info(f"FileTypeID for {file_id} is {result[0] if result else 'Not Found'}")
        return result[0] if result else None

def get_file_location(file_id: int, logger_instance: logging.Logger) -> str:
    """Retrieve the source file location URL from the database."""
    with pyodbc.connect(conn_str) as connection:
        cursor = connection.cursor()
        query = "SELECT FileLocationUrl FROM utb_ImageScraperFiles WHERE ID = ?"
        cursor.execute(query, (file_id,))
        result = cursor.fetchone()
        if not result:
            raise FileNotFoundError(f"No file location found in DB for FileID {file_id}")
        return result[0]
    
def get_images_excel_db(file_id: int, logger_instance: logging.Logger) -> pd.DataFrame:
    """Retrieve detailed image and product data for Excel processing."""
    # This block remains the same
    with pyodbc.connect(conn_str) as connection:
        cursor = connection.cursor()
        cursor.execute("UPDATE utb_ImageScraperFiles SET CreateFileStartTime = GETDATE() WHERE ID = ?", (file_id,))
        connection.commit()
    
    # 1. SQL QUERY: Reverted to using the '?' positional placeholder for pyodbc.
    query = """
        SELECT
            s.ExcelRowID, r.ImageUrl, r.ImageUrlThumbnail,
            s.ProductBrand AS Brand, s.ProductModel AS Style,
            s.ProductColor AS Color, s.ProductCategory AS Category
        FROM utb_ImageScraperFiles f
        INNER JOIN utb_ImageScraperRecords s ON s.FileID = f.ID 
        INNER JOIN utb_ImageScraperResult r ON r.EntryID = s.EntryID 
        WHERE f.ID = ? AND r.SortOrder = 1
        ORDER BY s.ExcelRowID
    """
    
    # 2. PANDAS CALL: Pass the parameters as a simple list.
    # Pandas is smart enough to handle this correctly with the '?' placeholder.
    return pd.read_sql_query(query, engine, params=[file_id])

def update_file_location_complete(file_id: int, file_location: str, logger_instance: logging.Logger):
    """Update the completed file location URL in the database."""
    with pyodbc.connect(conn_str) as connection:
        cursor = connection.cursor()
        query = "UPDATE utb_ImageScraperFiles SET FileLocationURLComplete = ? WHERE ID = ?"
        cursor.execute(query, (file_location, file_id))
        connection.commit()
        logger_instance.info(f"Updated completed file location for FileID {file_id}")

# --- Image Downloading Functions ---
async def validate_image_response(response, url: str, logger_instance: logging.Logger) -> Tuple[bool, Optional[bytes]]:
    if response.status != 200:
        logger_instance.warning(f"Invalid status code {response.status} for URL: {url}")
        return False, None
    if not any(response.headers.get('Content-Type', '').lower().startswith(it) for it in VALID_IMAGE_TYPES):
        logger_instance.warning(f"Invalid Content-Type for URL: {url}")
        return False, None
    if int(response.headers.get('Content-Length', 0)) < MIN_IMAGE_SIZE:
        logger_instance.warning(f"Content too small for URL: {url}")
        return False, None
    return True, await response.read()

async def image_download(semaphore, item: Dict, save_path: str, session, logger_instance: logging.Logger) -> bool:
    url, thumbnail = item.get('ImageUrl'), item.get('ImageUrlThumbnail')
    image_name = f"{item['ExcelRowID']}_{item.get('Style', 'style').replace(' ', '_')}"

    async def attempt_download(download_url: str, is_thumb: bool) -> bool:
        if not download_url: return False
        try:
            async with session.get(download_url) as response:
                is_valid, data = await validate_image_response(response, download_url, logger_instance)
                if not is_valid: return False
                
                final_path = os.path.join(save_path, f"{image_name}.png")
                with PILImage.open(BytesIO(data)) as img:
                    img.save(final_path, 'PNG')
                return True
        except Exception as e:
            logger_instance.error(f"Download failed for {'thumbnail' if is_thumb else 'image'} {download_url}: {e}")
            return False

    async with semaphore:
        if await attempt_download(url, False) or await attempt_download(thumbnail, True):
            return True
        logger_instance.error(f"All download attempts failed for Row {item['ExcelRowID']}")
        return False

async def download_all_images(data: List[Dict], save_path: str, logger_instance: logging.Logger):
    domains = [tldextract.extract(item['ImageUrl']).registered_domain for item in data if item.get('ImageUrl')]
    pool_size = min(500, max(10, len(Counter(domains)) * 2))
    
    async with RetryClient(raise_for_status=False, retry_options=ExponentialRetry(attempts=3), client_timeout=ClientTimeout(total=60)) as session:
        semaphore = asyncio.Semaphore(pool_size)
        tasks = [image_download(semaphore, item, save_path, session, logger_instance) for item in data]
        results = await asyncio.gather(*tasks)
        failed_count = len([res for res in results if not res])
        if failed_count > 0:
            logger_instance.warning(f"{failed_count} images failed to download.")

# --- Image & Excel Processing Functions ---
def resize_image(image_path: str, logger_instance: logging.Logger) -> bool:
    try:
        img = PILImage.open(image_path)
        if img.mode == 'RGBA':
            background = PILImage.new('RGB', img.size, (255, 255, 255))
            background.paste(img, mask=img.split()[3]); img = background
        elif img.mode != 'RGB':
            img = img.convert('RGB')
        
        # Replace non-white background
        pixels = np.array(img)
        border_pixels = np.concatenate([pixels[0, :], pixels[-1, :], pixels[:, 0], pixels[:, -1]])
        colors, counts = np.unique(border_pixels, axis=0, return_counts=True)
        most_common = colors[counts.argmax()]
        if np.sum(most_common) < 700: # Is the background dark?
            mask = np.all(np.abs(pixels - most_common) <= 15, axis=2)
            pixels[mask] = [255, 255, 255]
            img = PILImage.fromarray(pixels)

        h, w = img.size
        if h > MAX_IMAGE_DIMENSION or w > MAX_IMAGE_DIMENSION:
            img.thumbnail((MAX_IMAGE_DIMENSION, MAX_IMAGE_DIMENSION))

        img.save(image_path, 'PNG')
        return True
    except Exception as e:
        logger_instance.error(f"Error resizing image {image_path}: {e}", exc_info=True)
        return False

def verify_and_process_image(image_path: str, logger_instance: logging.Logger) -> bool:
    try:
        with PILImage.open(image_path) as img: img.verify()
        if os.path.getsize(image_path) < MAX_IMAGE_VERIFY_SIZE:
            logger_instance.warning(f"File may be too small: {image_path}"); return False
        return resize_image(image_path, logger_instance)
    except Exception:
        logger_instance.error(f"Image verification failed for {image_path}", exc_info=True); return False

def write_excel_distro(local_filename: str, temp_dir: str, image_data: List[Dict], header_row: int, logger_instance: logging.Logger):
    wb = load_workbook(local_filename); ws = wb.active
    image_map = {int(f.split('_')[0]): f for f in os.listdir(temp_dir) if '_' in f and f.split('_')[0].isdigit()}

    for item in image_data:
        row_id, row_num = item['ExcelRowID'], item['ExcelRowID'] + header_row
        if row_id in image_map:
            image_path = os.path.join(temp_dir, image_map[row_id])
            if verify_and_process_image(image_path, logger_instance):
                img = Image(image_path); img.anchor = f"A{row_num}"; ws.add_image(img)
        ws[f"B{row_num}"] = item.get('Brand', ''); ws[f"D{row_num}"] = item.get('Style', '')
        ws[f"E{row_num}"] = item.get('Color', ''); ws[f"H{row_num}"] = item.get('Category', '')

    if image_data:
        max_data_row = max(item['ExcelRowID'] for item in image_data) + header_row
        if ws.max_row > max_data_row: ws.delete_rows(max_data_row + 1, ws.max_row - max_data_row)
    wb.save(local_filename)

def write_excel_generic(local_filename: str, temp_dir: str, header_row: int, row_offset: int, logger_instance: logging.Logger):
    wb = load_workbook(local_filename); ws = wb.active
    image_map = {int(f.split('_')[0]): f for f in os.listdir(temp_dir) if '_' in f and f.split('_')[0].isdigit()}

    for row_id, image_file in image_map.items():
        image_path = os.path.join(temp_dir, image_file)
        if verify_and_process_image(image_path, logger_instance):
            img = Image(image_path)
            # Generic files use absolute ExcelRowID. Header_row is informational. API offset is applied.
            adjusted_row = row_id + row_offset
            img.anchor = f"A{adjusted_row}"; ws.add_image(img)
    wb.save(local_filename)

def find_header_row_index(excel_file: str, logger_instance: logging.Logger) -> Optional[int]:
    try:
        wb = load_workbook(excel_file, read_only=True); ws = wb.active
        keywords = {'id', 'name', 'product', 'brand', 'sku', 'category', 'color', 'price', 'description'}
        best_row, best_score = None, 0
        for i, row in enumerate(ws.iter_rows(min_row=1, max_row=10, values_only=True), 1):
            if not row: continue
            cells = [str(c).strip().lower() for c in row if isinstance(c, str)]
            score = len(cells) + len(set(cells)) + sum(any(k in c for k in keywords) for c in cells)
            if score > best_score: best_score, best_row = score, i
        if best_row and best_score > 3: return best_row
        return None
    except Exception as e:
        logger_instance.error(f"Could not find header row: {e}"); return None

# --- Main Background Task ---
async def generate_download_file(file_id: str, row_offset: int = 0):
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    logger_instance = setup_logging(file_id, timestamp)
    temp_images_dir, temp_excel_dir = None, None
    file_id_int = int(file_id)

    try:
        temp_images_dir, temp_excel_dir = await create_temp_dirs(file_id)
        logger_instance.info("Fetching data and downloading images...")
        images_df = get_images_excel_db(file_id_int, logger_instance)
        if images_df.empty: raise ValueError("No image data found in database.")
        
        await download_all_images(images_df.to_dict('records'), temp_images_dir, logger_instance)
        
        file_type_id = get_file_type_id(file_id_int, logger_instance)
        FILE_TYPE_DISTRO = 3

        if file_type_id == FILE_TYPE_DISTRO:
            logger_instance.info("Starting DISTRO file generation.")
            template_url = "https://iconluxury.group/public/ICON_DISTRO_USD_20250312.xlsx"
            file_name = os.path.basename(urllib.parse.unquote(template_url))
            local_filename = os.path.join(temp_excel_dir, file_name)
            header_row = 5 # Data starts after this many rows

            res = requests.get(template_url, timeout=60); res.raise_for_status()
            with open(local_filename, "wb") as f: f.write(res.content)
            
            write_excel_distro(local_filename, temp_images_dir, images_df.to_dict('records'), header_row, logger_instance)
        else:
            logger_instance.info("Starting GENERIC file generation.")
            file_url = get_file_location(file_id_int, logger_instance)
            file_name = os.path.basename(urllib.parse.unquote(file_url))
            local_filename = os.path.join(temp_excel_dir, file_name)

            res = requests.get(file_url, timeout=60); res.raise_for_status()
            with open(local_filename, "wb") as f: f.write(res.content)
            
            # This header is informational; row_offset from API is the key adjuster
            header_row = find_header_row_index(local_filename, logger_instance) or 0
            write_excel_generic(local_filename, temp_images_dir, header_row, row_offset, logger_instance)

        # Upload final file and send notifications
        processed_file_name = f"{Path(file_name).stem}_processed_{timestamp}.xlsx"
        public_url = await upload_file_to_space(local_filename, save_as=f"processed_files/{processed_file_name}", file_id=file_id_int, is_public=True)
        update_file_location_complete(file_id_int, public_url, logger_instance)
        await send_email(to_emails='nik@iconluxurygroup.com', subject=f'File Processed: {file_name}', download_url=public_url, job_id=file_id)
        
        logger_instance.info(f"Successfully completed job for FileID {file_id}.")

    except Exception as e:
        logger_instance.error(f"FATAL ERROR for FileID {file_id}: {e}", exc_info=True)
    finally:
        if temp_images_dir: await cleanup_temp_dirs([temp_images_dir, temp_excel_dir])

# --- FastAPI Endpoints ---
@app.post("/generate-download-file/")
async def process_file(background_tasks: BackgroundTasks, file_id: int, row_offset: Optional[int] = 0):
    logger.info(f"Received request for FileID: {file_id} with row_offset={row_offset}")
    background_tasks.add_task(generate_download_file, str(file_id), row_offset)
    return {"message": "Processing started. You will be notified upon completion."}

@app.get("/")
def read_root():
    return {"message": "Image Scraper Service is running."}

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting Uvicorn server on http://0.0.0.0:8080")
    uvicorn.run(app, host="0.0.0.0", port=8080)
