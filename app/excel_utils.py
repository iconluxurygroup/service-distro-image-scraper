import os
import logging
import asyncio
from openpyxl import load_workbook
from openpyxl.drawing.image import Image
from openpyxl.styles import PatternFill
from openpyxl.utils import get_column_letter
from typing import List, Dict, Optional
from PIL import Image as IMG2
from io import BytesIO
import numpy as np
from collections import Counter
import aiofiles
import aiofiles.os

# Module-level logger
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

async def verify_png_image_single(image_path: str, logger: Optional[logging.Logger] = None) -> bool:
    """Verify and validate a PNG image asynchronously."""
    logger = logger or default_logger
    try:
        logger.debug(f"🔎 Verifying image: {image_path}")
        # Run PIL image verification in a thread to avoid blocking
        async with aiofiles.open(image_path, 'rb') as f:
            img_data = await f.read()
        img = await asyncio.to_thread(IMG2.open, BytesIO(img_data))
        await asyncio.to_thread(img.verify)  # Verify it's a valid image
        logger.info(f"✅ Image verified successfully: {image_path}")

        # Get file size asynchronously
        image_size = (await aiofiles.os.stat(image_path)).st_size
        logger.debug(f"📏 Image size: {image_size} bytes")

        if image_size < 3000:
            logger.warning(f"⚠️ File may be corrupted or too small: {image_path}")
            return False

        # Resize image asynchronously
        if not await resize_image(image_path, logger=logger):
            logger.warning(f"⚠️ Resize failed for: {image_path}")
            return False

        # Re-verify after resizing
        async with aiofiles.open(image_path, 'rb') as f:
            img_data = await f.read()
        img = await asyncio.to_thread(IMG2.open, BytesIO(img_data))
        await asyncio.to_thread(img.verify)
        logger.info(f"✅ Post-resize verification successful: {image_path}")
        return True

    except Exception as e:
        logger.error(f"❌ Image verify failed: {e}, for image: {image_path}", exc_info=True)
        return False

async def resize_image(image_path: str, logger: Optional[logging.Logger] = None) -> bool:
    """Resize an image asynchronously, handling transparency and background."""
    logger = logger or default_logger
    try:
        logger.debug(f"📂 Attempting to open image: {image_path}")
        # Read image data asynchronously
        async with aiofiles.open(image_path, 'rb') as f:
            img_data = await f.read()
        img = await asyncio.to_thread(IMG2.open, BytesIO(img_data))
        MAXSIZE = 130  # Maximum size in pixels

        # Step 1: Handle transparency
        if img.mode == 'RGBA':
            logger.info(f"🌈 Converting RGBA image to RGB with white background: {image_path}")
            background = await asyncio.to_thread(IMG2.new, 'RGB', img.size, (255, 255, 255))
            background = await asyncio.to_thread(background.paste, img, mask=img.split()[3])
            img = background
        elif img.mode != 'RGB':
            logger.info(f"🌈 Converting {img.mode} image to RGB: {image_path}")
            img = await asyncio.to_thread(img.convert, 'RGB')

        # Step 2: Define helper functions for background processing (synchronous, run in thread)
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

        # Step 3 & 4: Detect and replace non-white background
        background_color = await asyncio.to_thread(get_background_color, img)
        if background_color and not await asyncio.to_thread(is_white, background_color):
            logger.info(f"🖌️ Replacing background color {background_color} with white for {image_path}")
            img = await asyncio.to_thread(replace_background, img, background_color)

        # Step 5: Resize if necessary
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

        # Save image asynchronously
        buffer = BytesIO()
        await asyncio.to_thread(new_img.save, buffer, format='PNG')
        async with aiofiles.open(image_path, 'wb') as f:
            await f.write(buffer.getvalue())
        logger.info(f"✅ Image processed and saved: {image_path}")

        # Verify file existence
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
    """Write one image per entry to an Excel file asynchronously, removing unneeded rows."""
    logger = logger or default_logger
    failed_rows = []

    try:
        logger.debug(f"📂 Loading workbook from {local_filename}")
        # Load workbook in a thread
        wb = await asyncio.to_thread(load_workbook, local_filename)
        ws = wb.active

        if ws.max_row < 5:
            logger.error(f"❌ Excel file must have at least 5 rows for header")
            return failed_rows

        logger.info(f"🖼️ Processing images for {local_filename}")
        if not await aiofiles.os.path.exists(temp_dir):
            logger.error(f"❌ Temp directory does not exist: {temp_dir}")
            return failed_rows

        # List directory asynchronously
        image_files = await asyncio.to_thread(os.listdir, temp_dir)
        if not image_files:
            logger.warning(f"⚠️ No images found in {temp_dir}")
            return failed_rows

        # Create a map of ExcelRowID to filename
        image_map = {}
        for f in image_files:
            if '_' in f and f.split('_')[0].isdigit():
                row_id = int(f.split('_')[0])
                image_map[row_id] = f

        # Process each entry
        for item in image_data:
            row_id = item['ExcelRowID']
            row_number = row_id + row_offset  # e.g., ExcelRowID=1 -> row 6

            if row_id in image_map:
                image_file = image_map[row_id]
                image_path = os.path.join(temp_dir, image_file)
                if await verify_png_image_single(image_path, logger=logger):
                    img = await asyncio.to_thread(Image, image_path)
                    anchor = f"{column}{row_number}"
                    await asyncio.to_thread(setattr, img, 'anchor', anchor)
                    await asyncio.to_thread(ws.add_image, img)
                    logger.info(f"✅ Image added at {anchor}")
                else:
                    failed_rows.append(row_id)
                    logger.warning(f"⚠️ Image verification failed for {image_file}")
            else:
                logger.warning(f"⚠️ No image found for row {row_id}")
                failed_rows.append(row_id)

            # Populate data
            ws[f"B{row_number}"] = item.get('Brand', '')
            ws[f"D{row_number}"] = item.get('Style', '')
            if item.get('Color'):
                ws[f"E{row_number}"] = item['Color']
            if item.get('Category'):
                ws[f"H{row_number}"] = item['Category']

            # Validate
            if not ws[f"B{row_number}"].value:
                logger.warning(f"⚠️ Missing Brand in B{row_number}")
            if not ws[f"D{row_number}"].value:
                logger.warning(f"⚠️ Missing Style in D{row_number}")

        # Remove unneeded rows
        max_data_row = max(item['ExcelRowID'] for item in image_data) + row_offset
        if ws.max_row > max_data_row:
            logger.info(f"🗑️ Removing rows {max_data_row + 1} to {ws.max_row}")
            await asyncio.to_thread(ws.delete_rows, max_data_row + 1, ws.max_row - max_data_row)

        # Save workbook asynchronously
        await asyncio.to_thread(wb.save, local_filename)
        logger.info("🏁 Finished processing images")
        return failed_rows

    except Exception as e:
        logger.error(f"❌ Error writing images: {e}")
        return failed_rows

async def highlight_cell(
    excel_file: str,
    cell_reference: str,
    logger: Optional[logging.Logger] = None
) -> None:
    """Highlight a cell in an Excel file with a yellow fill asynchronously."""
    logger = logger or default_logger
    try:
        if not await aiofiles.os.path.exists(excel_file):
            logger.error(f"❌ Excel file does not exist: {excel_file}")
            raise FileNotFoundError(f"Excel file not found: {excel_file}")

        logger.debug(f"📂 Loading workbook from {excel_file}")
        wb = await asyncio.to_thread(load_workbook, excel_file)
        ws = wb.active

        if cell_reference not in ws:
            logger.error(f"❌ Invalid cell reference: {cell_reference} not in worksheet")
            raise ValueError(f"Invalid cell reference: {cell_reference}")

        logger.debug(f"🖌️ Applying yellow fill to cell {cell_reference}")
        await asyncio.to_thread(
            setattr,
            ws[cell_reference],
            'fill',
            PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
        )

        logger.debug(f"💾 Saving workbook to {excel_file}")
        await asyncio.to_thread(wb.save, excel_file)

        # Verify highlight
        wb_verify = await asyncio.to_thread(load_workbook, excel_file)
        ws_verify = wb_verify.active
        fill_color = ws_verify[cell_reference].fill.start_color.index if ws_verify[cell_reference].fill else None
        if fill_color != "FFFF00":
            logger.warning(f"⚠️ Highlight not applied to {cell_reference} after save, found color: {fill_color}")
        else:
            logger.info(f"✅ Successfully highlighted cell {cell_reference} in {excel_file} with yellow fill")

    except FileNotFoundError as e:
        logger.error(f"❌ File error: {e}", exc_info=True)
        raise
    except ValueError as e:
        logger.error(f"❌ Value error: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"❌ Error highlighting cell {cell_reference} in {excel_file}: {e}", exc_info=True)
        raise

async def write_failed_downloads_to_excel(
    failed_downloads: List[tuple],
    excel_file: str,
    logger: Optional[logging.Logger] = None
) -> bool:
    """Write failed downloads to an Excel file and highlight them asynchronously."""
    logger = logger or default_logger
    if failed_downloads:
        try:
            logger.debug(f"📂 Loading workbook from {excel_file}")
            wb = await asyncio.to_thread(load_workbook, excel_file)
            ws = wb.active

            # Ensure worksheet has enough rows
            max_row = ws.max_row
            max_failed_row = max(row_id for _, row_id in failed_downloads if row_id)
            if max_failed_row > max_row:
                logger.debug(f"Extending worksheet from {max_row} to {max_failed_row} rows")
                for i in range(max_row + 1, max_failed_row + 1):
                    ws[f"A{i}"] = ""  # Add empty cells to extend the sheet

            for url, row_id in failed_downloads:
                if url and url != 'None found in this filter':
                    cell_reference = f"{get_column_letter(1)}{row_id}"  # Column A
                    logger.debug(f"✍️ Writing URL {url} to cell {cell_reference}")
                    ws[cell_reference] = str(url)
                    try:
                        await highlight_cell(excel_file, cell_reference, logger=logger)
                    except ValueError as e:
                        logger.warning(f"⚠️ Skipping highlight for {cell_reference}: {e}")
                        continue  # Skip highlighting if cell isn’t valid, but still write URL

            logger.debug(f"💾 Saving workbook to {excel_file}")
            await asyncio.to_thread(wb.save, excel_file)
            logger.info(f"✅ Failed downloads written to Excel file: {excel_file}")
            return True
        except Exception as e:
            logger.error(f"❌ Error writing failed downloads to Excel: {e}", exc_info=True)
            return False
    else:
        logger.info("ℹ️ No failed downloads to write to Excel.")
        return True