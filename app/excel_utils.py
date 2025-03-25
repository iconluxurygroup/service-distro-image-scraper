# excel_utils.py
import os
import logging
from openpyxl import load_workbook
from openpyxl.drawing.image import Image
from openpyxl.styles import PatternFill
from openpyxl.utils import get_column_letter
from PIL import Image as IMG2
from io import BytesIO
import numpy as np
from collections import Counter
# Module-level logger
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def verify_png_image_single(image_path, logger=None):
    logger = logger or default_logger
    try:
        logger.debug(f"🔎 Verifying image: {image_path}")
        img = IMG2.open(image_path)
        img.verify()  # Verify it's a valid image
        logger.info(f"✅ Image verified successfully: {image_path}")
    except Exception as e:
        logger.error(f"❌ Image verify failed: {e}, for image: {image_path}", exc_info=True)
        return False

    imageSize = os.path.getsize(image_path)
    logger.debug(f"📏 Image size: {imageSize} bytes")

    if imageSize < 3000:
        logger.warning(f"⚠️ File may be corrupted or too small: {image_path}")
        return False

    try:
        if not resize_image(image_path, logger=logger):
            logger.warning(f"⚠️ Resize failed for: {image_path}")
            return False
        # Re-verify after resizing
        img = IMG2.open(image_path)
        img.verify()
        logger.info(f"✅ Post-resize verification successful: {image_path}")
        return True
    except Exception as e:
        logger.error(f"❌ Error during verification or resizing: {e}, for image: {image_path}", exc_info=True)
        return False

def resize_image(image_path, logger=None):
    logger = logger or default_logger
    try:
        logger.debug(f"📂 Attempting to open image: {image_path}")
        img = IMG2.open(image_path)
        MAXSIZE = 130  # Maximum size in pixels

        # Step 1: Handle transparency
        if img.mode == 'RGBA':
            logger.info(f"🌈 Converting RGBA image to RGB with white background: {image_path}")
            background = IMG2.new('RGB', img.size, (255, 255, 255))
            background.paste(img, mask=img.split()[3])
            img = background
        elif img.mode != 'RGB':
            logger.info(f"🌈 Converting {img.mode} image to RGB: {image_path}")
            img = img.convert('RGB')

        # Step 2: Define helper functions for background processing
        def get_background_color(img):
            """Detect if the image has a uniform background color based on border pixels."""
            width, height = img.size
            pixels = np.array(img)
            # Extract border pixels
            top = pixels[0, :]
            bottom = pixels[height-1, :]
            left = pixels[1:height-1, 0]
            right = pixels[1:height-1, width-1]
            border_pixels = np.concatenate((top, bottom, left, right))
            # Count colors
            color_counts = Counter(map(tuple, border_pixels))
            most_common_color, count = color_counts.most_common(1)[0]
            total_border_pixels = border_pixels.shape[0]
            # Consider it uniform if 90% of border pixels match
            if count / total_border_pixels >= 0.9:
                return most_common_color
            return None

        def is_white(color, threshold=240):
            """Check if a color is white, allowing for slight variations."""
            r, g, b = color
            return r >= threshold and g >= threshold and b >= threshold

        def replace_background(img, bg_color):
            """Replace all pixels matching the background color with white."""
            pixels = np.array(img)
            bg_color = np.array(bg_color)
            white = np.array([255, 255, 255])
            # Allow a small tolerance for color matching
            diff = np.abs(pixels - bg_color)
            mask = np.all(diff <= 5, axis=2)
            pixels[mask] = white
            return IMG2.fromarray(pixels)

        # Step 3 & 4: Detect and replace non-white background
        background_color = get_background_color(img)
        if background_color and not is_white(background_color):
            logger.info(f"🖌️ Replacing background color {background_color} with white for {image_path}")
            img = replace_background(img, background_color)

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
            newImg = img.resize((w, h))
        else:
            newImg = img

        newImg.save(image_path, 'PNG')
        logger.info(f"✅ Image processed and saved: {image_path}")
        if os.path.exists(image_path):
            logger.debug(f"📏 File size after save: {os.path.getsize(image_path)} bytes")
        else:
            logger.error(f"❌ File not found after save: {image_path}")
            return False
        return True

    except Exception as e:
        logger.error(f"❌ Error resizing image: {e}, for image: {image_path}", exc_info=True)
        return False
    
def write_excel_image(local_filename, temp_dir, column="A", row_offset=0, logger=None):
    """
    Write images to an Excel file in a specified column (default 'A') with an optional row offset.
    
    Args:
        local_filename (str): Path to the Excel file
        temp_dir (str): Path to the directory containing images
        column (str): Column letter to insert images (default 'A')
        row_offset (int): Number of rows to offset (default 0)
        logger (logging.Logger, optional): Logger instance to use
        
    Returns:
        list: List of row numbers that failed
    """
    # Handle incorrect argument types (temporary workaround)
    if isinstance(column, logging.Logger):
        logger = column
        column = "A"
        row_offset = 0
    elif isinstance(row_offset, logging.Logger):
        logger = row_offset
        row_offset = 0

    logger = logger or default_logger
    # Log all arguments for debugging
    logger.debug(f"write_excel_image called with: local_filename={local_filename}, temp_dir={temp_dir}, column={column}, row_offset={row_offset}, logger={logger}")
    
    failed_rows = []
    try:
        if not isinstance(row_offset, int):
            logger.error(f"❌ row_offset must be an integer, got {type(row_offset)}: {row_offset}")
            raise TypeError(f"row_offset must be an integer, got {type(row_offset)}: {row_offset}")

        logger.debug(f"📂 Loading workbook from {local_filename}")
        wb = load_workbook(local_filename)
        ws = wb.active
        logger.info(f"🖼️ Processing images in {temp_dir} for Excel file {local_filename} in column {column} with row offset {row_offset}")
        
        if not os.path.exists(temp_dir):
            logger.error(f"❌ Temp directory does not exist: {temp_dir}")
            return failed_rows
        
        image_files = os.listdir(temp_dir)
        if not image_files:
            logger.warning(f"⚠️ No images found in {temp_dir}")
            return failed_rows
        
        for image_file in image_files:
            image_path = os.path.join(temp_dir, image_file)
            logger.debug(f"📄 Found image file: {image_path}")
            try:
                row_number = int(image_file.split('.')[0]) + row_offset
                logger.info(f"🔍 Processing row {row_number - row_offset} (adjusted to {row_number}), image path: {image_path}")
            except ValueError:
                logger.warning(f"⚠️ Skipping file {image_file}: does not match expected naming convention")
                continue
            
            if verify_png_image_single(image_path, logger=logger):
                try:
                    logger.debug(f"🖼️ Inserting image at row {row_number}")
                    img = Image(image_path)
                    anchor = f"{column}{row_number}"
                    logger.debug(f"📍 Assigned anchor: {anchor}")
                    img.anchor = anchor
                    ws.add_image(img)
                    logger.info(f"✅ Image added at {anchor}")
                except Exception as e:
                    logger.error(f"❌ Failed to add image at row {row_number}: {e}", exc_info=True)
                    failed_rows.append(row_number - row_offset)
            else:
                failed_rows.append(row_number - row_offset)
                logger.warning(f"⚠️ Inserting image skipped due to verify_png_image_single failure for row {row_number}")
        
        logger.debug(f"💾 Saving workbook to {local_filename}")
        wb.save(local_filename)
        logger.info("🏁 Finished processing all images.")
        return failed_rows
    except Exception as e:
        logger.error(f"❌ Error writing images to Excel: {e}", exc_info=True)
        return failed_rows

def highlight_cell(excel_file, cell_reference, logger=None):
    """Highlight a cell in an Excel file with a yellow fill."""
    logger = logger or default_logger
    try:
        if not os.path.exists(excel_file):
            logger.error(f"❌ Excel file does not exist: {excel_file}")
            raise FileNotFoundError(f"Excel file not found: {excel_file}")
        
        logger.debug(f"📂 Loading workbook from {excel_file}")
        wb = load_workbook(excel_file)
        ws = wb.active
        
        if cell_reference not in ws:
            logger.error(f"❌ Invalid cell reference: {cell_reference} not in worksheet")
            raise ValueError(f"Invalid cell reference: {cell_reference}")
        
        logger.debug(f"🖌️ Applying yellow fill to cell {cell_reference}")
        ws[cell_reference].fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
        
        logger.debug(f"💾 Saving workbook to {excel_file}")
        wb.save(excel_file)
        
        wb_verify = load_workbook(excel_file)
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

def write_failed_downloads_to_excel(failed_downloads, excel_file, logger=None):
    """Write failed downloads to an Excel file and highlight them."""
    logger = logger or default_logger
    if failed_downloads:
        try:
            logger.debug(f"📂 Loading workbook from {excel_file}")
            wb = load_workbook(excel_file)
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
                        highlight_cell(excel_file, cell_reference, logger=logger)
                    except ValueError as e:
                        logger.warning(f"⚠️ Skipping highlight for {cell_reference}: {e}")
                        continue  # Skip highlighting if cell isn’t valid, but still write URL
            
            logger.debug(f"💾 Saving workbook to {excel_file}")
            wb.save(excel_file)
            logger.info(f"✅ Failed downloads written to Excel file: {excel_file}")
            return True
        except Exception as e:
            logger.error(f"❌ Error writing failed downloads to Excel: {e}", exc_info=True)
            return False
    else:
        logger.info("ℹ️ No failed downloads to write to Excel.")
        return True