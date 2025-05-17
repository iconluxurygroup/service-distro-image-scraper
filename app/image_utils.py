import os
import asyncio
import logging
import aiohttp
import aiofiles
from typing import List, Dict, Tuple, Optional
from urllib.parse import urlparse
from url_extract import extract_thumbnail_url
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import re
from operator import itemgetter
from PIL import Image as IMG2
from io import BytesIO
import numpy as np
from collections import Counter
from openpyxl import Workbook, load_workbook
from openpyxl.drawing.image import Image as OpenpyxlImage
from openpyxl.styles import PatternFill
from openpyxl.utils import get_column_letter

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# URL Cleaning and Validation (New Flow)
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
        default_logger.warning(f"Error cleaning URL {url} on attempt {attempt}: {e}")
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

# Image Verification and Resizing (Current Flow)
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

# Excel Operations (Merged Old and Current Flows)
async def anchor_images_to_excel(
    image_data: List[Dict],
    temp_dir: str,
    excel_file: str,
    column: str = "A",
    row_offset: int = 5,
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

        logger.info(f"🖼️ Processing images for {excel_file}")
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
                anchor = f"{column}{row_number}"
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

# Main Flow (Merged Old, Current, and New)
async def process_images_and_anchor(
    image_list: List[Dict],
    temp_dir: str,
    excel_file: str,
    logger: Optional[logging.Logger] = None
) -> bool:
    logger = logger or default_logger
    logger.info(f"🚀 Starting image processing pipeline")

    # Step 1: Download images
    failed_downloads = await download_all_images(image_list, temp_dir, logger)
    
    # Step 2: Anchor images to Excel
    failed_rows = await anchor_images_to_excel(image_list, temp_dir, excel_file, logger=logger)
    
    # Step 3: Write failed downloads to Excel
    success = await write_failed_downloads_to_excel(failed_downloads, excel_file, logger)
    
    logger.info(f"🎉 Pipeline completed. Failed downloads: {len(failed_downloads)}, Failed rows: {len(failed_rows)}")
    return success and not failed_rows