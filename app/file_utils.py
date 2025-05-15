import os
import shutil
import asyncio
import logging
from typing import List, Optional, Tuple

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

async def create_temp_dirs(unique_id: str | int, logger: Optional[logging.Logger] = None) -> Tuple[str, str]:
    """Create temporary directories for images and Excel files."""
    logger = logger or default_logger
    loop = asyncio.get_running_loop()
    # Use logged path for consistency, configurable via env
    base_dir = os.getenv("TEMP_FILES_DIR", "/root/service-distro-image/temp_files")
    temp_images_dir = os.path.join(base_dir, 'images', str(unique_id))
    temp_excel_dir = os.path.join(base_dir, 'excel', str(unique_id))

    try:
        await loop.run_in_executor(None, lambda: os.makedirs(temp_images_dir, exist_ok=True))
        await loop.run_in_executor(None, lambda: os.makedirs(temp_excel_dir, exist_ok=True))
        
        # Verify directories are writable
        for dir_path in [temp_images_dir, temp_excel_dir]:
            if not os.access(dir_path, os.W_OK):
                logger.error(f"Directory not writable: {dir_path}")
                raise OSError(f"Directory not writable: {dir_path}")
        
        logger.info(f"Created temporary directories for ID {unique_id}: {temp_images_dir}, {temp_excel_dir}")
        return temp_images_dir, temp_excel_dir
    except Exception as e:
        logger.error(f"🔴 Failed to create temp directories for ID {unique_id}: {e}", exc_info=True)
        raise

async def cleanup_temp_dirs(directories: List[str], logger: Optional[logging.Logger] = None) -> None:
    """Clean up temporary directories."""
    logger = logger or default_logger
    loop = asyncio.get_running_loop()
    
    for dir_path in directories:
        try:
            if os.path.exists(dir_path):
                await loop.run_in_executor(None, lambda dp=dir_path: shutil.rmtree(dp, ignore_errors=True))
                logger.info(f"Cleaned up directory: {dir_path}")
        except Exception as e:
            logger.error(f"🔴 Failed to clean up directory {dir_path}: {e}", exc_info=True)