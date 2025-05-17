import os
import logging
import asyncio
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

        # Convert image modes if necessary
        if img.mode == 'RGBA':
            logger.info(f"🌈 Converting RGBA image to RGB with white background: {image_path}")
            background = await asyncio.to_thread(IMG2.new, 'RGB', img.size, (255, 255, 255))
            await asyncio.to_thread(background.paste, img, mask=img.split()[3])  # Fixed: Do not reassign background
            img = background
        elif img.mode != 'RGB':
            logger.info(f"🌈 Converting {img.mode} image to RGB: {image_path}")
            img = await asyncio.to_thread(img.convert, 'RGB')
        logger.debug(f"After mode conversion: img={img}, type={type(img)}, has_size={hasattr(img, 'size')}")
        if not hasattr(img, 'size'):
            logger.error(f"❌ Image object is invalid after mode conversion: {image_path}")
            return False

        # Background color detection and replacement functions
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

        # Replace background color if needed
        logger.debug(f"Before get_background_color: img={img}, type={type(img)}")
        background_color = await asyncio.to_thread(get_background_color, img)
        if background_color and not await asyncio.to_thread(is_white, background_color):
            logger.info(f"🖌️ Replacing background color {background_color} with white for {image_path}")
            img = await asyncio.to_thread(replace_background, img, background_color)

        # Resize image if necessary
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

        # Save the processed image
        buffer = BytesIO()
        await asyncio.to_thread(new_img.save, buffer, format='PNG')
        async with aiofiles.open(image_path, 'wb') as f:
            await f.write(buffer.getvalue())
        logger.info(f"✅ Image processed and saved: {image_path}")

        # Verify the file was saved
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
            logger.error(f"❌ Temp directory does not exist: {temp_dir}")
            return failed_rows

        image_files = await asyncio.to_thread(os.listdir, temp_dir)
        if not image_files:
            logger.warning(f"⚠️ No images found in {temp_dir}")
            return failed_rows

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

            if row_id_int in image_map:
                image_file = image_map[row_id_int]
                image_path = os.path.join(temp_dir, image_file)
                if await verify_png_image_single(image_path, logger=logger):
                    img = await asyncio.to_thread(Image, image_path)
                    anchor = f"{column}{row_number}"
                    await asyncio.to_thread(setattr, img, 'anchor', anchor)
                    await asyncio.to_thread(ws.add_image, img)
                    logger.info(f"✅ Image added at {anchor}")
                else:
                    failed_rows.append(row_id_int)
                    logger.warning(f"⚠️ Image verification failed for {image_file}")
            else:
                logger.warning(f"⚠️ No image found for row {row_id_int}")
                failed_rows.append(row_id_int)

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

async def highlight_cell(
    excel_file: str,
    cell_reference: str,
    logger: Optional[logging.Logger] = None
) -> None:
    logger = logger or default_logger
    try:
        if not await aiofiles.os.path.exists(excel_file):
            logger.error(f"❌ Excel file does not exist: {excel_file}")
            raise FileNotFoundError(f"Excel file not found: {excel_file}")

        logger.debug(f"📂 Loading workbook from {excel_file}")
        wb = await asyncio.to_thread(load_workbook, excel_file)
        ws = wb.active

        if ws.max_row < 1 or ws.max_column < 1:
            logger.warning(f"Worksheet is empty, initializing with at least one cell")
            ws['A1'] = ""

        try:
            col_letter = ''.join(c for c in cell_reference if c.isalpha())
            row_num = int(''.join(c for c in cell_reference if c.isdigit()))
            if not col_letter or row_num < 1:
                raise ValueError(f"Invalid cell reference format: {cell_reference}")
        except ValueError:
            logger.error(f"❌ Invalid cell reference format: {cell_reference}")
            raise ValueError(f"Invalid cell reference format: {cell_reference}")

        # Extend worksheet dimensions if necessary
        if row_num > ws.max_row:
            for r in range(ws.max_row + 1, row_num + 1):
                for c in range(1, ws.max_column + 1):
                    ws[f"{get_column_letter(c)}{r}"] = ""
        if ord(col_letter.upper()) - ord('A') + 1 > ws.max_column:
            for c in range(ws.max_column + 1, ord(col_letter.upper()) - ord('A') + 2):
                for r in range(1, ws.max_row + 1):
                    ws[f"{get_column_letter(c)}{r}"] = ""

        logger.debug(f"🖌️ Applying yellow fill to cell {cell_reference}")
        ws[cell_reference].fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")

        logger.debug(f"💾 Saving workbook to {excel_file}")
        await asyncio.to_thread(wb.save, excel_file)

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
            max_failed_row = 0
            for _, row_id in failed_downloads:
                try:
                    row_id_int = int(row_id)
                    if row_id_int > max_failed_row:
                        max_failed_row = row_id_int
                except (ValueError, TypeError) as e:
                    logger.error(f"Invalid row_id {row_id}: {e}")
                    continue

            logger.debug(f"Max row: {max_row}, Max failed row: {max_failed_row}")
            if max_failed_row > max_row:
                logger.debug(f"Extending worksheet from {max_row} to {max_failed_row} rows")
                for i in range(max_row + 1, max_failed_row + 1):
                    ws[f"A{i}"] = ""

            for url, row_id in failed_downloads:
                try:
                    row_id_int = int(row_id)
                    if url and url != 'None found in this filter':
                        cell_reference = f"{get_column_letter(1)}{row_id_int}"
                        logger.debug(f"✍️ Writing URL {url} to cell {cell_reference}")
                        ws[cell_reference] = str(url)
                        try:
                            await highlight_cell(excel_file, cell_reference, logger=logger)
                        except ValueError as e:
                            logger.warning(f"⚠️ Skipping highlight for {cell_reference}: {e}")
                            continue
                except (ValueError, TypeError) as e:
                    logger.error(f"Invalid row_id {row_id}: {e}")
                    continue

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