import os
import logging
from openpyxl import load_workbook
from openpyxl.drawing.image import Image
from openpyxl.styles import PatternFill
from openpyxl.utils import get_column_letter
from PIL import Image as IMG2
from PIL import UnidentifiedImageError
from io import BytesIO

logging.getLogger(__name__)


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
        logging.info(f"Processing images in {temp_dir} for Excel file {local_filename}")
        
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
    except Exception as e:
        logging.error(f"Error writing images to Excel: {e}")
        return failed_rows

def highlight_cell(excel_file, cell_reference):
    """Highlight a cell in an Excel file."""
    try:
        wb = load_workbook(excel_file)
        ws = wb.active
        ws[cell_reference].fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
        wb.save(excel_file)
        logging.info(f"Highlighted cell {cell_reference} in {excel_file}")
    except Exception as e:
        logging.error(f"Error highlighting cell: {e}")
        raise

def write_failed_downloads_to_excel(failed_downloads, excel_file):
    """Write failed downloads to an Excel file."""
    if failed_downloads:
        try:
            wb = load_workbook(excel_file)
            ws = wb.active
            for url, row_id in failed_downloads:
                if url and url != 'None found in this filter':
                    cell_reference = f"{get_column_letter(1)}{row_id}"  # Column A
                    ws[cell_reference] = str(url)
                    highlight_cell(excel_file, cell_reference)
            wb.save(excel_file)
            logging.info(f"Failed downloads written to Excel file: {excel_file}")
            return True
        except Exception as e:
            logging.error(f"Error writing failed downloads to Excel: {e}")
            return False
    else:
        logging.info("No failed downloads to write to Excel.")
        return True