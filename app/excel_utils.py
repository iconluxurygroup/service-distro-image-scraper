import os
import logging
import asyncio
import aiohttp
from openpyxl import load_workbook, Workbook
from openpyxl.drawing.image import Image
from openpyxl.styles import PatternFill
from openpyxl.utils import get_column_letter
from typing import List, Dict, Optional, Tuple
from PIL import Image as IMG2
from io import BytesIO
import numpy as np
from collections import Counter
import aiofiles
import aiofiles.os

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

async def download_image(url: str, dest_path: str, logger: Optional[logging.Logger] = None) -> bool:
    logger = logger or default_logger
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    logger.error(f"❌ Failed to download {url}: HTTP {response.status}")
                    return False
                data = await response.read()
        async with aiofiles.open(dest_path, 'wb') as f:
            await f.write(data)
        logger.info(f"✅ Downloaded {url} to {dest_path}")
        return True
    except Exception as e:
        logger.error(f"❌ Error downloading {url}: {e}", exc_info=True)
        return False

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

        if image_size < 1000:  # Relaxed for thumbnails
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

        def get_background_color(img):
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

        background_color = await asyncio.to_thread(get_background_color, img)
        if background_color and not await asyncio.to_thread(is_white, background_color):
            logger.info(f"🖌️ Replacing background color {background_color} with white for {image_path}")
            img = await asyncio.to_thread(replace_background, img, background_color)

        h, w = img.height, img.width
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
        if await aiofiles.os.path.exists(image_path):
            logger.debug(f"📏 File size after save: {(await aiofiles.os.stat(image_path)).st_size} bytes")
        else:
            logger.error(f"❌ File not found after save: {image_path}")
            return False
        return True
    except Exception as e:
        logger.error(f"❌ Error resizing image: {e}, for image: {image_path}", exc_info=True)
        return False

async def write_excel_image(
    local_filename: str,
    temp_dir: str,
    image_data: List[Dict],
    column: str = "A",
    row_offset: int = 5,
    logger: Optional[logging.Logger] = None
) -> List[int]:
    logger = logger or default_logger
    failed_rows = []

    try:
        logger.debug(f"📂 Loading workbook from {local_filename}")
        wb = await asyncio.to_thread(load_workbook, local_filename)
        ws = wb.active

        if ws.max_row < 5:
            logger.error(f"❌ Excel file must have at least 5 rows for header")
            return failed_rows

        logger.info(f"🖼️ Processing images for {local_filename}")
        if not await aiofiles.os.path.exists(temp_dir):
            logger.debug(f"📁 Creating temp directory: {temp_dir}")
            await aiofiles.os.makedirs(temp_dir)

        for item in image_data:
            row_id = item.get('ExcelRowID')
            try:
                row_id_int = int(row_id)
            except (ValueError, TypeError) as e:
                logger.error(f"❌ Invalid row_id type for {row_id}: expected int, got {type(row_id)}, error: {e}")
                failed_rows.append(row_id)
                continue
            row_number = row_id_int + row_offset
            logger.debug(f"Processing row_id={row_id_int}, row_number={row_number}")

            # Try full image first, then thumbnail
            full_image_path = os.path.join(temp_dir, f"{row_id_int}_full.png")
            thumb_image_path = os.path.join(temp_dir, f"{row_id_int}_thumb.png")
            image_added = False

            # Download full image if not present
            if not await aiofiles.os.path.exists(full_image_path) and item.get('ImageUrl'):
                logger.debug(f"⬇️ Downloading full image for row {row_id_int}: {item['ImageUrl']}")
                if not await download_image(item['ImageUrl'], full_image_path, logger):
                    logger.warning(f"⚠️ Failed to download full image for row {row_id_int}")

            # Verify and add full image
            if await aiofiles.os.path.exists(full_image_path):
                if await verify_png_image_single(full_image_path, logger):
                    img = await asyncio.to_thread(Image, full_image_path)
                    anchor = f"{column}{row_number}"
                    await asyncio.to_thread(setattr, img, 'anchor', anchor)
                    await asyncio.to_thread(ws.add_image, img)
                    logger.info(f"✅ Full image added at {anchor}")
                    image_added = True
                else:
                    logger.warning(f"⚠️ Full image verification failed for row {row_id_int}")

            # Try thumbnail if full image fails or is missing
            if not image_added and item.get('ImageUrlThumbnail'):
                if not await aiofiles.os.path.exists(thumb_image_path):
                    logger.debug(f"⬇️ Downloading thumbnail for row {row_id_int}: {item['ImageUrlThumbnail']}")
                    if not await download_image(item['ImageUrlThumbnail'], thumb_image_path, logger):
                        logger.warning(f"⚠️ Failed to download thumbnail for row {row_id_int}")
                if await aiofiles.os.path.exists(thumb_image_path):
                    if await verify_png_image_single(thumb_image_path, logger):
                        img = await asyncio.to_thread(Image, thumb_image_path)
                        anchor = f"{column}{row_number}"
                        await asyncio.to_thread(setattr, img, 'anchor', anchor)
                        await asyncio.to_thread(ws.add_image, img)
                        logger.info(f"✅ Thumbnail added at {anchor}")
                        image_added = True
                    else:
                        logger.warning(f"⚠️ Thumbnail verification failed for row {row_id_int}")

            if not image_added:
                logger.warning(f"⚠️ No valid image or thumbnail for row {row_id_int}")
                failed_rows.append(row_id_int)

            # Write metadata
            ws[f"B{row_number}"] = item.get('Brand', '')
            ws[f"D{row_number}"] = item.get('Style', '')
            if item.get('Color'):
                ws[f"E{row_number}"] = item['Color']
            if item.get('Category'):
                ws[f"H{row_number}"] = item['Category']

            if not ws[f"B{row_number}"].value:
                logger.warning(f"⚠️ Missing Brand in B{row_number}")
            if not ws[f"D{row_number}"].value:
                logger.warning(f"⚠️ Missing Style in D{row_number}")

        max_data_row = max(int(item['ExcelRowID']) for item in image_data) + row_offset
        if ws.max_row > max_data_row:
            logger.info(f"🗑️ Removing rows {max_data_row + 1} to {ws.max_row}")
            await asyncio.to_thread(ws.delete_rows, max_data_row + 1, ws.max_row - max_data_row)

        await asyncio.to_thread(wb.save, local_filename)
        logger.info("🏁 Finished processing images")
        return failed_rows
    except Exception as e:
        logger.error(f"❌ Error writing images: {e}", exc_info=True)
        return failed_rows