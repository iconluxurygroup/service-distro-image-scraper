# excel_utils.py
import os
import logging
from openpyxl import load_workbook
from openpyxl.drawing.image import Image
from openpyxl.styles import PatternFill
from openpyxl.utils import get_column_letter
from PIL import Image as IMG2
from io import BytesIO

# Module-level logger
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def verify_png_image_single(image_path, logger=None):
    """
    Verify that an image is a valid PNG.
    
    Args:
        image_path (str): Path to the image to verify
        logger (logging.Logger, optional): Logger instance to use
        
    Returns:
        bool: True if the image is valid, False otherwise
    """
    logger = logger or default_logger
    try:
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
        resize_image(image_path, logger=logger)
    except Exception as e:
        logger.error(f"❌ Error resizing image: {e}, for image: {image_path}", exc_info=True)
        return False
    return True

def resize_image(image_path, logger=None):
    """
    Resize an image to a maximum size.
    
    Args:
        image_path (str): Path to the image to resize
        logger (logging.Logger, optional): Logger instance to use
        
    Returns:
        bool: True if resizing was successful, False otherwise
    """
    logger = logger or default_logger
    try:
        img = IMG2.open(image_path)
        MAXSIZE = 145  # Maximum size in pixels
        if img:
            h, w = img.height, img.width  # original size
            logger.debug(f"📐 Original size: height={h}, width={w}")
            if h > MAXSIZE or w > MAXSIZE:
                if h > w:
                    w = int(w * MAXSIZE / h)
                    h = MAXSIZE
                else:
                    h = int(h * MAXSIZE / w)
                    w = MAXSIZE
            logger.debug(f"🔍 Resized to: height={h}, width={w}")
            newImg = img.resize((w, h))
            newImg.save(image_path)
            logger.info(f"✅ Image resized and saved: {image_path}")
            return True
    except Exception as e:
        logger.error(f"❌ Error resizing image: {e}, for image: {image_path}", exc_info=True)
        return False

def write_excel_image(local_filename, temp_dir, preferred_image_method, logger=None):
    """
    Write images to an Excel file.
    
    Args:
        local_filename (str): Path to the Excel file
        temp_dir (str): Path to the directory containing images
        preferred_image_method (str): Preferred method for inserting images ('append', 'overwrite', 'NewColumn')
        logger (logging.Logger, optional): Logger instance to use
        
    Returns:
        list: List of row numbers that failed
    """
    logger = logger or default_logger
    failed_rows = []
    try:
        logger.debug(f"📂 Loading workbook from {local_filename}")
        wb = load_workbook(local_filename)
        ws = wb.active
        logger.info(f"🖼️ Processing images in {temp_dir} for Excel file {local_filename}")
        
        for image_file in os.listdir(temp_dir):
            image_path = os.path.join(temp_dir, image_file)
            try:
                row_number = int(image_file.split('.')[0])
                logger.info(f"🔍 Processing row {row_number}, image path: {image_path}")
            except ValueError:
                logger.warning(f"⚠️ Skipping file {image_file}: does not match expected naming convention")
                continue
            
            if verify_png_image_single(image_path, logger=logger):
                logger.debug(f"🖼️ Inserting image at row {row_number}")
                img = Image(image_path)
                if preferred_image_method in ["overwrite", "append"]:
                    anchor = "A" + str(row_number)
                elif preferred_image_method == "NewColumn":
                    anchor = "B" + str(row_number)
                else:
                    logger.error(f"❌ Unrecognized preferred image method: {preferred_image_method}")
                    continue
                
                logger.debug(f"📍 Assigned anchor: {anchor}")
                img.anchor = anchor
                ws.add_image(img)
                logger.info(f"✅ Image added at {anchor}")
            else:
                failed_rows.append(row_number)
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
# excel_utils.py (partial update)
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