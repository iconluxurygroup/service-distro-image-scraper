import logging
import pandas as pd
import pyodbc
import asyncio
import json
import datetime
import os
import time
import hashlib
import psutil
import httpx
from fastapi import BackgroundTasks
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from database_config import conn_str, async_engine
from s3_utils import upload_file_to_space
from db_utils import (
    get_send_to_email,
    get_images_excel_db,
    update_file_location_complete,
    update_file_generate_complete,
    update_log_url_in_db,
)
from logging_config import setup_job_logger
from email_utils import send_message_email
import aiofiles
import aiohttp
from typing import Optional, List, Dict, Tuple
from urllib.parse import urlparse

from url_extract import extract_thumbnail_url
import re
from PIL import Image as IMG2
from io import BytesIO
import numpy as np
from collections import Counter
from openpyxl import Workbook, load_workbook
from openpyxl.drawing.image import Image as OpenpyxlImage
from openpyxl.styles import PatternFill


default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Image Processing Functions (From Old Logic)
async def download_all_images(
    image_list: List[Dict],
    temp_dir: str,
    logger: Optional[logging.Logger] = None,
    batch_size: int = 10
) -> List[Tuple[str, int]]:
    logger = logger or default_logger
    failed_downloads = []
    logger.info(f"📥 Starting download of {len(image_list)} images to {temp_dir}")

    os.makedirs(temp_dir, exist_ok=True)
    logger.info(f"📁 Ensured directory exists: {temp_dir}")

    def clean_url(url: str, attempt: int = 1) -> str:
        try:
            if attempt == 1:
                url = re.sub(r'\\+|%5[Cc]', '', url)
                parsed = urlparse(url)
                path = parsed.path.replace('%2F', '/').replace('%2f', '/')
                cleaned_url = f"{parsed.scheme}://{parsed.netloc}{path}"
                if parsed.query:
                    query = parsed.query.replace('%5C', '').replace('%5c', '')
                    cleaned_url += f"?{query}"
                if parsed.fragment:
                    cleaned_url += f"#{parsed.fragment}"
                return cleaned_url
            elif attempt == 2:
                url = re.sub(r'\\+|%5[Cc]|%2[Ff]', '', url)
                parsed = urlparse(url)
                path = parsed.path
                cleaned_url = f"{parsed.scheme}://{parsed.netloc}{path}"
                if parsed.query:
                    cleaned_url += f"?{parsed.query}"
                if parsed.fragment:
                    cleaned_url += f"#{parsed.fragment}"
                return cleaned_url
            elif attempt == 3:
                url = re.sub(r'[\x00-\x1F\x7F]', '', url)
                parsed = urlparse(url)
                return f"{parsed.scheme}://{parsed.netloc}{parsed.path}" + \
                       (f"?{parsed.query}" if parsed.query else "") + \
                       (f"#{parsed.fragment}" if parsed.fragment else "")
            return url
        except Exception as e:
            logger.warning(f"Error cleaning URL {url} on attempt {attempt}: {e}")
            return url

    async def validate_url(url: str, session: aiohttp.ClientSession, logger: logging.Logger) -> bool:
        try:
            if not re.match(r'^https?://', url):
                logger.warning(f"Invalid URL format: {url}")
                return False
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124',
                'Accept': 'image/*,*/*;q=0.8',
                'Referer': 'https://www.google.com/'
            }
            async with session.head(url, timeout=5, headers=headers) as response:
                if response.status == 200:
                    logger.debug(f"URL {url} is accessible")
                    return True
                elif response.status == 404:
                    logger.warning(f"URL {url} is permanently unavailable (404)")
                    return False
                logger.warning(f"URL {url} returned status {response.status}")
                return False
        except aiohttp.ClientError as e:
            logger.warning(f"URL {url} is not accessible: {e}")
            return False

    @retry(
        stop=stop_after_attempt(lambda attempt, kwargs: 4 if kwargs.get('entry_index', 0) >= 2 else 3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((aiohttp.ClientResponseError, asyncio.TimeoutError)),
        before_sleep=lambda retry_state: retry_state.kwargs['logger'].info(
            f"Retrying download for URL {retry_state.kwargs['url']} (attempt {retry_state.attempt_number}/{4 if retry_state.kwargs.get('entry_index', 0) >= 2 else 3}) after {retry_state.next_action.sleep}s"
        )
    )
    async def download_image(
        url: str,
        filename: str,
        session: aiohttp.ClientSession,
        logger: logging.Logger,
        entry_index: int = 0,
        timeout: int = 30,
        max_clean_attempts: int = 3
    ) -> bool:
        extracted_url = extract_thumbnail_url(url, logger)
        logger.debug(f"Extracted URL: {extracted_url}")
        for attempt in range(1, max_clean_attempts + 1):
            try:
                logger.debug(f"Attempt {attempt} - Raw URL: {url}")
                logger.debug(f"Attempt {attempt} - Extracted URL: {extracted_url}")
                if not await validate_url(extracted_url, session, logger):
                    logger.warning(f"Attempt {attempt} - Skipping inaccessible URL: {extracted_url}")
                    continue
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124',
                    'Accept': 'image/*,*/*;q=0.8',
                    'Referer': 'https://www.google.com/'
                }
                async with session.get(extracted_url, timeout=timeout, headers=headers) as response:
                    if response.status != 200:
                        logger.warning(f"Attempt {attempt} - HTTP error for image {extracted_url}: {response.status} {response.reason}")
                        if response.status == 404:
                            return False
                        continue
                    async with aiofiles.open(filename, 'wb') as f:
                        await f.write(await response.read())
                    logger.debug(f"Attempt {attempt} - Successfully downloaded {extracted_url} to {filename}")
                    return True
            except aiohttp.ClientResponseError as e:
                if e.status == 404:
                    logger.error(f"Attempt {attempt} - Permanent failure for {url}: 404 Not Found")
                    return False
                logger.error(f"Attempt {attempt} - HTTP error for image {url}: {str(e)}")
                raise
            except asyncio.TimeoutError:
                logger.error(f"Attempt {attempt} - Timeout downloading image {url}")
                raise
            except Exception as e:
                logger.error(f"Attempt {attempt} - Error downloading image {url}: {str(e)}", exc_info=True)
                continue
        logger.error(f"All {max_clean_attempts} attempts failed for URL {url}")
        return False

    sort_strategies = [
        lambda lst: sorted(lst, key=lambda x: 1 if x.get('ImageUrlThumbnail') else 0, reverse=True),
        lambda lst: sorted(lst, key=lambda x: len(x.get('ImageUrl', '')), reverse=False),
        lambda lst: sorted(lst, key=lambda x: urlparse(x.get('ImageUrl', '')).netloc or ''),
        lambda lst: sorted(lst, key=lambda x: x.get('ExcelRowID', 0)),
    ]

    async def process_image(image: Dict, index: int) -> None:
        excel_row_id = image['ExcelRowID']
        main_url = image['ImageUrl']
        thumb_url = image.get('ImageUrlThumbnail', '')
        filename = os.path.join(temp_dir, f"{excel_row_id}_image.jpg")

        logger.debug(f"Processing ExcelRowID {excel_row_id} at index {index}: Main URL = {main_url}, Thumbnail URL = {thumb_url}")

        async with aiohttp.ClientSession() as session:
            success = await download_image(main_url, filename, session, logger, entry_index=index)
            if not success and thumb_url:
                logger.debug(f"Falling back to thumbnail {thumb_url} for ExcelRowID {excel_row_id}")
                success = await download_image(thumb_url, filename, session, logger, entry_index=index)
            
            if not success:
                logger.error(f"Failed to download both main and thumbnail for ExcelRowID {excel_row_id}")
                failed_downloads.append((main_url or thumb_url or "No valid URL", excel_row_id))
                if not main_url and not thumb_url:
                    logger.critical(f"No valid URLs for ExcelRowID {excel_row_id}. Check database for FileID {image.get('FileID', 'unknown')}.")

    best_failed_downloads = []
    min_failures = float('inf')
    original_tail = image_list[2:] if len(image_list) > 2 else []
    fixed_prefix = image_list[:2] if len(image_list) >= 2 else image_list

    for strategy_idx, sort_func in enumerate(sort_strategies, 1):
        failed_downloads.clear()
        logger.info(f"Trying sort strategy {strategy_idx}")
        sorted_tail = sort_func(original_tail)
        current_list = fixed_prefix + sorted_tail
        logger.debug(f"Current list order: {[item['ExcelRowID'] for item in current_list]}")

        batches = [current_list[i:i + batch_size] for i in range(0, len(current_list), batch_size)]
        for batch_idx, batch in enumerate(batches, 1):
            logger.info(f"Processing batch {batch_idx} with {len(batch)} images")
            await asyncio.gather(*(process_image(image, idx) for idx, image in enumerate(current_list)))
            logger.info(f"Batch {batch_idx} completed, failures: {len(failed_downloads)}")

        logger.info(f"Sort strategy {strategy_idx} completed with {len(failed_downloads)} failures")
        if len(failed_downloads) < min_failures:
            min_failures = len(failed_downloads)
            best_failed_downloads = failed_downloads.copy()
        if min_failures == 0:
            break

    if best_failed_downloads:
        logger.info(f"Retrying {len(best_failed_downloads)} failed downloads with fallback strategy")
        retry_failed = []
        for url, excel_row_id in best_failed_downloads:
            image = next((img for img in image_list if img['ExcelRowID'] == excel_row_id), None)
            if not image:
                continue
            filename = os.path.join(temp_dir, f"{excel_row_id}_image.jpg")
            async with aiohttp.ClientSession() as session:
                main_url = clean_url(image['ImageUrl'], attempt=3)
                success = await download_image(main_url, filename, session, logger, entry_index=image_list.index(image))
                if not success and image.get('ImageUrlThumbnail'):
                    thumb_url = clean_url(image['ImageUrlThumbnail'], attempt=3)
                    success = await download_image(thumb_url, filename, session, logger, entry_index=image_list.index(image))
                if not success:
                    retry_failed.append((url, excel_row_id))
        best_failed_downloads = retry_failed

    logger.info(f"📸 Completed image downloads. Total failed: {len(best_failed_downloads)}/{len(image_list)}")
    return best_failed_downloads

async def verify_png_image_single(image_path: str, logger: Optional[logging.Logger] = None) -> bool:
    logger = logger or default_logger
    try:
        logger.debug(f"🔎 Verifying image: {image_path}")
        async with aiofiles.open(image_path, 'rb') as f:
            img_data = await f.read()
        img = await asyncio.to_thread(IMG2.open, BytesIO(img_data))
        if img is None:
            logger.error(f"❌ Failed to open image: {image_path}")
            return False
        await asyncio.to_thread(img.verify)
        logger.info(f"✅ Image verified successfully: {image_path}")

        image_size = (await aiofiles.os.stat(image_path)).st_size
        logger.debug(f"📏 Image size: {image_size} bytes")

        if image_size < 3000:
            logger.warning(f"⚠️ File may be corrupted or too small: {image_path}")
            return False

        if not await resize_image(image_path, logger=logger):
            logger.warning(f"⚠️ Resize failed for: {image_path}")
            return False

        async with aiofiles.open(image_path, 'rb') as f:
            img_data = await f.read()
        img = await asyncio.to_thread(IMG2.open, BytesIO(img_data))
        if img is None:
            logger.error(f"❌ Failed to open image after resize: {image_path}")
            return False
        await asyncio.to_thread(img.verify)
        logger.info(f"✅ Post-resize verification successful: {image_path}")
        return True
    except Exception as e:
        logger.error(f"❌ Image verify failed: {e}, for image: {image_path}", exc_info=True)
        return False

async def resize_image(image_path: str, logger: Optional[logging.Logger] = None) -> bool:
    logger = logger or default_logger
    try:
        logger.debug(f"📂 Attempting to open image: {image_path}")
        async with aiofiles.open(image_path, 'rb') as f:
            img_data = await f.read()
        img = await asyncio.to_thread(IMG2.open, BytesIO(img_data))
        logger.debug(f"After opening: img={img}, type={type(img)}, has_size={hasattr(img, 'size')}")
        if img is None or not hasattr(img, 'size'):
            logger.error(f"❌ Invalid image object after opening: {image_path}")
            return False
        
        MAXSIZE = 130

        if img.mode == 'RGBA':
            logger.info(f"🌈 Converting RGBA image to RGB with white background: {image_path}")
            background = await asyncio.to_thread(IMG2.new, 'RGB', img.size, (255, 255, 255))
            await asyncio.to_thread(background.paste, img, mask=img.split()[3])
            img = background
        elif img.mode != 'RGB':
            logger.info(f"🌈 Converting {img.mode} image to RGB: {image_path}")
            img = await asyncio.to_thread(img.convert, 'RGB')
        logger.debug(f"After mode conversion: img={img}, type={type(img)}, has_size={hasattr(img, 'size')}")
        if not hasattr(img, 'size'):
            logger.error(f"❌ Image object is invalid after mode conversion: {image_path}")
            return False

        def get_background_color(img):
            logger.debug(f"Inside get_background_color: img={img}, type={type(img)}")
            width, height = img.size
            pixels = np.array(img)
            top = pixels[0, :]
            bottom = pixels[height-1, :]
            left = pixels[1:height-1, 0]
            right = pixels[1:height-1, width-1]
            border_pixels = np.concatenate((top, bottom, left, right))
            color_counts = Counter(map(tuple, border_pixels))
            most_common_color, count = color_counts.most_common(1)[0]
            total_border_pixels = border_pixels.shape[0]
            if count / total_border_pixels >= 0.9:
                return most_common_color
            return None

        def is_white(color, threshold=240):
            r, g, b = color
            return r >= threshold and g >= threshold and b >= threshold

        def replace_background(img, bg_color):
            pixels = np.array(img)
            bg_color = np.array(bg_color)
            white = np.array([255, 255, 255])
            diff = np.abs(pixels - bg_color)
            mask = np.all(diff <= 5, axis=2)
            pixels[mask] = white
            return IMG2.fromarray(pixels)

        logger.debug(f"Before get_background_color: img={img}, type={type(img)}")
        background_color = await asyncio.to_thread(get_background_color, img)
        if background_color and not await asyncio.to_thread(is_white, background_color):
            logger.info(f"🖌️ Replacing background color {background_color} with white for {image_path}")
            img = await asyncio.to_thread(replace_background, img, background_color)

        h, w = img.height, img.width
        logger.debug(f"📐 Original size: height={h}, width={w}")
        if h > MAXSIZE or w > MAXSIZE:
            if h > w:
                w = int(w * MAXSIZE / h)
                h = MAXSIZE
            else:
                h = int(h * MAXSIZE / w)
                w = MAXSIZE
            logger.debug(f"🔍 Resizing to: height={h}, width={w}")
            new_img = await asyncio.to_thread(img.resize, (w, h))
        else:
            new_img = img

        buffer = BytesIO()
        await asyncio.to_thread(new_img.save, buffer, format='PNG')
        async with aiofiles.open(image_path, 'wb') as f:
            await f.write(buffer.getvalue())
        logger.info(f"✅ Image processed and saved: {image_path}")

        if await aiofiles.os.path.exists(image_path):
            logger.debug(f"📏 File size after save: {(await aiofiles.os.stat(image_path)).st_size} bytes")
        else:
            logger.error(f"❌ File not found after save: {image_path}")
            return False
        return True
    except Exception as e:
        logger.error(f"❌ Error resizing image: {e}, for image: {image_path}", exc_info=True)
        return False

async def anchor_images_to_excel(
    excel_file: str,
    temp_dir: str,
    image_data: List[Dict],
    column: str = "A",
    row_offset: int = 5,
    preferred_image_method: str = "append",
    logger: Optional[logging.Logger] = None
) -> List[int]:
    logger = logger or default_logger
    failed_rows = []

    try:
        logger.debug(f"📂 Loading or creating workbook at {excel_file}")
        if os.path.exists(excel_file):
            wb = await asyncio.to_thread(load_workbook, excel_file)
            ws = wb.active
        else:
            wb = Workbook()
            ws = wb.active
            ws.title = "Images"
            headers = ["Image", "Brand", "Style", "Color", "Category", "Status", "Width", "Height", "Format"]
            for col, header in enumerate(headers, start=1):
                ws.cell(row=1, column=col).value = header

        if ws.max_row < row_offset:
            for row in range(2, row_offset + 1):
                ws.cell(row=row, column=1).value = ""

        logger.info(f"🖼️ Processing images for {excel_file} with method {preferred_image_method}")
        if not await aiofiles.os.path.exists(temp_dir):
            logger.error(f"❌ Temp directory does not exist: {temp_dir}")
            return failed_rows

        image_files = await asyncio.to_thread(os.listdir, temp_dir)
        image_map = {}
        for f in image_files:
            if '_' in f and f.split('_')[0].isdigit():
                row_id = int(f.split('_')[0])
                image_map[row_id] = f

        for item in image_data:
            row_id = item['ExcelRowID']
            try:
                row_id_int = int(row_id)
            except (ValueError, TypeError) as e:
                logger.error(f"Invalid row_id type for {row_id}: expected int, got {type(row_id)}, error: {e}")
                failed_rows.append(row_id)
                continue
            row_number = row_id_int + row_offset
            logger.debug(f"Processing row_id={row_id_int}, row_number={row_number}")

            image_path = os.path.join(temp_dir, image_map.get(row_id_int, ""))
            status = "Failed"
            width, height, format_ = 0, 0, "Unknown"

            if row_id_int in image_map and await verify_png_image_single(image_path, logger):
                img = await asyncio.to_thread(OpenpyxlImage, image_path)
                img.width, img.height = 80, 80
                if preferred_image_method in ["append", "overwrite"]:
                    anchor = f"{column}{row_number}"
                elif preferred_image_method == "NewColumn":
                    anchor = f"B{row_number}"
                else:
                    logger.error(f"Unrecognized preferred image method: {preferred_image_method}")
                    failed_rows.append(row_id_int)
                    continue

                await asyncio.to_thread(ws.add_image, img, anchor)
                ws.row_dimensions[row_number].height = 60
                logger.info(f"✅ Image added at {anchor}")

                async with aiofiles.open(image_path, 'rb') as f:
                    img_data = await f.read()
                pil_img = await asyncio.to_thread(IMG2.open, BytesIO(img_data))
                width, height, format_ = pil_img.width, pil_img.height, pil_img.format
                status = "Success"
            else:
                logger.warning(f"⚠️ Image verification failed or no image for row {row_id_int}")
                failed_rows.append(row_id_int)

            ws[f"B{row_number}"] = item.get('Brand', '')
            ws[f"C{row_number}"] = item.get('Style', '')
            ws[f"D{row_number}"] = item.get('Color', '')
            ws[f"E{row_number}"] = item.get('Category', '')
            ws[f"F{row_number}"] = status
            ws[f"G{row_number}"] = width
            ws[f"H{row_number}"] = height
            ws[f"I{row_number}"] = format_

            if not ws[f"B{row_number}"].value:
                logger.warning(f"⚠️ Missing Brand in B{row_number}")
            if not ws[f"C{row_number}"].value:
                logger.warning(f"⚠️ Missing Style in C{row_number}")

        max_data_row = max(int(item['ExcelRowID']) for item in image_data) + row_offset
        if ws.max_row > max_data_row:
            logger.info(f"🗑️ Removing rows {max_data_row + 1} to {ws.max_row}")
            await asyncio.to_thread(ws.delete_rows, max_data_row + 1, ws.max_row - max_data_row)

        await asyncio.to_thread(wb.save, excel_file)
        logger.info(f"🏁 Excel file saved with anchored images: {excel_file}")
        return failed_rows
    except Exception as e:
        logger.error(f"❌ Error anchoring images to Excel: {e}", exc_info=True)
        return failed_rows

async def write_failed_downloads_to_excel(
    failed_downloads: List[Tuple[str, int]],
    excel_file: str,
    logger: Optional[logging.Logger] = None
) -> bool:
    logger = logger or default_logger
    if failed_downloads:
        try:
            logger.debug(f"📂 Loading workbook from {excel_file}")
            wb = await asyncio.to_thread(load_workbook, excel_file)
            ws = wb.create_sheet("FailedDownloads") if "FailedDownloads" not in wb else wb["FailedDownloads"]

            max_row = ws.max_row or 1
            max_failed_row = max(row_id for _, row_id in failed_downloads)
            if max_failed_row > max_row:
                logger.debug(f"Extending worksheet from {max_row} to {max_failed_row} rows")
                for i in range(max_row + 1, max_failed_row + 1):
                    ws[f"A{i}"] = ""

            for url, row_id in failed_downloads:
                cell_reference = f"A{row_id}"
                logger.debug(f"✍️ Writing URL {url} to cell {cell_reference}")
                ws[cell_reference] = str(url)
                ws[cell_reference].fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")

            await asyncio.to_thread(wb.save, excel_file)
            logger.info(f"✅ Failed downloads written to Excel file: {excel_file}")
            return True
        except Exception as e:
            logger.error(f"❌ Error writing failed downloads to Excel: {e}", exc_info=True)
            return False
    logger.info("ℹ️ No failed downloads to write to Excel.")
    return True

async def process_images_and_anchor(
    image_list: List[Dict],
    temp_dir: str,
    excel_file: str,
    preferred_image_method: str = "append",
    logger: Optional[logging.Logger] = None
) -> bool:
    logger = logger or default_logger
    logger.info(f"🚀 Starting image processing pipeline with method {preferred_image_method}")

    failed_downloads = await download_all_images(image_list, temp_dir, logger)
    
    failed_rows = await anchor_images_to_excel(excel_file, temp_dir, image_list, "A", 5, preferred_image_method, logger)
    
    success = await write_failed_downloads_to_excel(failed_downloads, excel_file, logger)
    
    logger.info(f"🎉 Pipeline completed. Failed downloads: {len(failed_downloads)}, Failed rows: {len(failed_rows)}")
    return success and not failed_rows

# New Code Utilities
def highlight_cell(excel_file, cell_reference, logger: Optional[logging.Logger] = None):
    logger = logger or default_logger
    try:
        workbook = load_workbook(excel_file)
        sheet = workbook.active
        sheet[cell_reference].fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
        workbook.save(excel_file)
        logger.info(f"Cell {cell_reference} highlighted in {excel_file}")
    except Exception as e:
        logger.error(f"Failed to highlight cell {cell_reference} in {excel_file}: {e}", exc_info=True)

# Main FastAPI Endpoint (Updated to Use process_images_and_anchor)
async def generate_download_file(
    file_id: int,
    background_tasks: BackgroundTasks = None,
    logger: Optional[logging.Logger] = None,
    file_id_param: Optional[int] = None,
    preferred_image_method: str = "append"
) -> Dict[str, str]:
    logger, log_filename = setup_job_logger(job_id=str(file_id), log_dir="job_logs", console_output=True)
    process = psutil.Process()
    temp_images_dir, temp_excel_dir = None, None
    successful_entries = 0
    failed_entries = 0
    entries = []

    try:
        file_id = int(file_id)
        async with async_engine.connect() as conn:
            result = await conn.execute(
                text("SELECT FileName FROM utb_ImageScraperFiles WHERE ID = :file_id"),
                {"file_id": file_id}
            )
            row = result.fetchone()
            result.close()
            if not row:
                logger.error(f"No file found for ID {file_id}")
                return {"error": f"No file found for ID {file_id}", "log_filename": log_filename}
            original_filename = row[0]

        logger.info(f"Fetching images for ID: {file_id}")
        selected_images_df = await get_images_excel_db(str(file_id), logger=logger)
        logger.info(f"Fetched DataFrame for ID {file_id}, shape: {selected_images_df.shape}")

        expected_columns = ["ExcelRowID", "ImageUrl", "ImageUrlThumbnail", "Brand", "Style", "Color", "Category"]
        if list(selected_images_df.columns) != expected_columns:
            logger.error(f"Invalid columns in DataFrame for ID {file_id}. Got: {list(selected_images_df.columns)}")
            return {"error": f"Invalid DataFrame columns", "log_filename": log_filename}

        entries = selected_images_df['ExcelRowID'].unique().tolist()
        logger.info(f"Processing {len(entries)} unique entries for FileID {file_id}")

        template_file_path = "https://iconluxurygroup.s3.us-east-2.amazonaws.com/ICON_DISTRO_USD_20250312.xlsx"
        base_name, extension = os.path.splitext(original_filename)
        processed_file_name = f"excel_files/{file_id}/{base_name}{extension}"

        temp_images_dir, temp_excel_dir = await create_temp_dirs(file_id, logger=logger)
        local_filename = os.path.join(temp_excel_dir, original_filename)

        async with httpx.AsyncClient() as client:
            response = await client.get(template_file_path, timeout=httpx.Timeout(30, connect=10))
            response.raise_for_status()
            async with aiofiles.open(local_filename, 'wb') as f:
                await f.write(response.content)

        if not os.path.exists(local_filename):
            logger.error(f"Template file not found at {local_filename}")
            return {"error": f"Failed to download template file", "log_filename": log_filename}
        logger.debug(f"Template file saved: {local_filename}, size: {os.path.getsize(local_filename)} bytes")

        has_valid_images = not selected_images_df.empty and any(pd.notna(row['ImageUrl']) and row['ImageUrl'] for _, row in selected_images_df.iterrows())
        
        if not has_valid_images:
            logger.warning(f"No valid images found for ID {file_id}")
            with pd.ExcelWriter(local_filename, engine='openpyxl', mode='a') as writer:
                pd.DataFrame({"Message": [f"No valid images found for FileID {file_id}."]}).to_excel(writer, sheet_name="NoImages", index=False)
            failed_entries = len(entries)
        else:
            selected_image_list = [
                {
                    'ExcelRowID': int(row['ExcelRowID']),
                    'ImageUrl': row['ImageUrl'],
                    'ImageUrlThumbnail': row.get('ImageUrlThumbnail', ''),
                    'Brand': row.get('Brand', ''),
                    'Style': row.get('Style', ''),
                    'Color': row.get('Color', ''),
                    'Category': row.get('Category', ''),
                    'FileID': file_id
                }
                for _, row in selected_images_df.iterrows()
                if pd.notna(row['ImageUrl']) and row['ImageUrl']
            ]
            logger.info(f"Selected {len(selected_image_list)} valid images")

            success = await process_images_and_anchor(
                selected_image_list,
                temp_images_dir,
                local_filename,
                preferred_image_method=preferred_image_method,
                logger=logger
            )
            
            successful_entries = len(entries) - sum(1 for item in selected_image_list if not os.path.exists(os.path.join(temp_images_dir, f"{item['ExcelRowID']}_image.jpg")))
            failed_entries = len(entries) - successful_entries
            logger.info(f"Processed {successful_entries} successful entries, {failed_entries} failed entries")

        if not os.path.exists(local_filename):
            logger.error(f"Excel file not found at {local_filename}")
            return {"error": f"Excel file not found", "log_filename": log_filename}
        logger.debug(f"Excel file exists: {local_filename}, size: {os.path.getsize(local_filename)} bytes")

        logger.info(f"Uploading Excel file to R2: {processed_file_name}")
        public_url = await upload_file_to_space(
            file_src=local_filename,
            save_as=processed_file_name,
            is_public=True,
            logger=logger,
            file_id=file_id
        )

        if not public_url:
            logger.error(f"Upload failed for ID {file_id}")
            return {"error": "Failed to upload processed file", "log_filename": log_filename}
        logger.info(f"Excel file uploaded to R2, public_url: {public_url}")

        await update_file_location_complete(str(file_id), public_url, logger=logger)
        await update_file_generate_complete(str(file_id), logger=logger)

        log_public_url = await upload_log_file(str(file_id), log_filename, logger)

        send_to_email_addr = await get_send_to_email(file_id, logger=logger)
        if not send_to_email_addr:
            logger.error(f"No email address for ID {file_id}")
            return {"error": "Failed to retrieve email address", "log_filename": log_filename}

        subject_line = f"{original_filename} Job Notification{' - No Images' if not has_valid_images else ''}"
        user_message = (
            f"Excel file generation for FileID {file_id} {'completed successfully' if successful_entries > 0 else 'completed with no valid images'}.\n"
            f"Download Excel file: {public_url}\n"
            f"Log file: {log_public_url or log_filename}"
        )
        logger.debug(f"Scheduling user email to {send_to_email_addr} with public_url: {public_url}, message: {user_message}")
        if background_tasks:
            background_tasks.add_task(
                send_message_email,
                to_emails=send_to_email_addr,
                subject=subject_line,
                message=user_message,
                logger=logger
            )

        admin_email = "nik@luxurymarket.com"
        admin_subject = f"Admin Log: Job Completed for FileID {file_id}"
        admin_message = (
            f"Processing for FileID {file_id} completed.\n"
            f"Successful entries: {successful_entries}/{len(entries)}\n"
            f"Failed entries: {failed_entries}\n"
            f"Last EntryID: {max(entries) if entries else 0}\n"
            f"Excel file: {public_url}\n"
            f"Log file: {log_public_url or log_filename}"
        )
        logger.debug(f"Scheduling admin email to {admin_email} with message: {admin_message}")
        if background_tasks:
            background_tasks.add_task(
                send_message_email,
                to_emails=[admin_email],
                subject=admin_subject,
                message=admin_message,
                logger=logger
            )

        logger.info(f"Completed ID {file_id}")
        return {
            "message": "Processing completed successfully" if successful_entries > 0 else "No valid images found, empty Excel file generated",
            "public_url": public_url,
            "log_filename": log_filename,
            "log_public_url": log_public_url or ""
        }
    except Exception as e:
        logger.error(f"Error for ID {file_id}: {e}", exc_info=True)
        log_public_url = await upload_log_file(str(file_id), log_filename, logger)
        send_to_email_addr = await get_send_to_email(file_id, logger=logger)
        if send_to_email_addr and background_tasks:
            error_message = f"Excel file generation for FileID {file_id} failed.\nError: {str(e)}\nLog file: {log_public_url or log_filename}"
            logger.debug(f"Scheduling error email to {send_to_email_addr} with message: {error_message}")
            background_tasks.add_task(
                send_message_email,
                to_emails=send_to_email_addr,
                subject=f"Error: Job Failed for FileID {file_id}",
                message=error_message,
                logger=logger
            )
        return {"error": f"An error occurred: {str(e)}", "log_filename": log_filename, "log_public_url": log_public_url or ""}
    finally:
        if temp_images_dir and temp_excel_dir:
            await cleanup_temp_dirs([temp_images_dir, temp_excel_dir], logger=logger)
        await async_engine.dispose()
        logger.info(f"Cleaned up resources for ID {file_id}")

# Log Upload
async def upload_log_file(file_id: str, log_filename: str, logger: logging.Logger) -> Optional[str]:
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(Exception),
        before_sleep=lambda retry_state: logger.info(
            f"Retrying log upload for FileID {file_id} (attempt {retry_state.attempt_number}/3)"
        )
    )
    async def try_upload():
        if not os.path.exists(log_filename):
            logger.warning(f"Log file {log_filename} does not exist, skipping upload")
            return None

        file_hash = hashlib.md5(open(log_filename, "rb").read()).hexdigest()
        current_time = time.time()
        key = (log_filename, file_id)

        LAST_UPLOAD = {}
        if key in LAST_UPLOAD and LAST_UPLOAD[key]["hash"] == file_hash and current_time - LAST_UPLOAD[key]["time"] < 60:
            logger.info(f"Skipping redundant upload for {log_filename}")
            return LAST_UPLOAD[key]["url"]

        try:
            upload_url = await upload_file_to_space(
                file_src=log_filename,
                save_as=f"job_logs/job_{file_id}.log",
                is_public=True,
                logger=logger,
                file_id=file_id
            )
            await update_log_url_in_db(file_id, upload_url, logger)
            LAST_UPLOAD[key] = {"hash": file_hash, "time": current_time, "url": upload_url}
            logger.info(f"Log uploaded to R2: {upload_url}")
            return upload_url
        except Exception as e:
            logger.error(f"Failed to upload log for FileID {file_id}: {e}", exc_info=True)
            raise

    try:
        return await try_upload()
    except Exception as e:
        logger.error(f"Failed to upload log for FileID {file_id} after retries: {e}", exc_info=True)
        return None