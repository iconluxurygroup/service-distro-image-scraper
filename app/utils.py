import os
import logging
from typing import List, Tuple, Optional

async def create_temp_dirs(file_id: int, logger: Optional[logging.Logger] = None) -> Tuple[str, str]:
    """
    Create temporary directories for images and Excel files based on the provided file_id.
    
    Args:
        file_id (int): The file identifier used to create unique directory names.
        logger (Optional[logging.Logger]): Logger instance for logging directory creation. If None, a default logger is used.
    
    Returns:
        Tuple[str, str]: Paths to the created temporary image and Excel directories.
    """
    logger = logger or logging.getLogger(__name__)
    temp_images_dir = f"temp_images_{file_id}"
    temp_excel_dir = f"temp_excel_{file_id}"
    os.makedirs(temp_images_dir, exist_ok=True)
    os.makedirs(temp_excel_dir, exist_ok=True)
    logger.debug(f"Created temp directories: {temp_images_dir}, {temp_excel_dir}")
    return temp_images_dir, temp_excel_dir

async def cleanup_temp_dirs(dirs: List[str], logger: Optional[logging.Logger] = None):
    """
    Clean up temporary directories and their contents.
    
    Args:
        dirs (List[str]): List of directory paths to clean up.
        logger (Optional[logging.Logger]): Logger instance for logging cleanup actions. If None, a default logger is used.
    """
    logger = logger or logging.getLogger(__name__)
    for dir_path in dirs:
        if os.path.exists(dir_path):
            try:
                for root, _, files in os.walk(dir_path, topdown=False):
                    for file in files:
                        os.remove(os.path.join(root, file))
                    os.rmdir(root)
                logger.debug(f"Cleaned up directory: {dir_path}")
            except Exception as e:
                logger.warning(f"Failed to clean up directory {dir_path}: {e}")