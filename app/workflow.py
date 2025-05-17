import asyncio
import logging
import os
import datetime
from typing import Optional, Dict, List, Tuple
from search_utils import update_search_sort_order, insert_search_results
from db_utils import (
    get_send_to_email,
    get_images_excel_db,
    update_file_location_complete,
    update_file_generate_complete,
    export_dai_json,
    update_log_url_in_db,
    fetch_last_valid_entry,
)
from vision_utils import fetch_missing_images
from endpoint_utils import sync_get_endpoint
from image_utils import download_all_images
from excel_utils import write_excel_image, write_failed_downloads_to_excel
from common import fetch_brand_rules
from utils import (
    create_temp_dirs,
    cleanup_temp_dirs,
    process_and_tag_results,
    generate_search_variations,
    search_variation,
    process_search_row_gcloud
)
from logging_config import setup_job_logger
from aws_s3 import upload_file_to_space
import psutil
from email_utils import send_message_email
from image_reason import process_entry
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import aiohttp
import httpx
import aiofiles
from database_config import conn_str, async_engine, engine
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd

async def generate_download_file(
    file_id: int,
    logger: Optional[logging.Logger] = None,
    file_id_param: Optional[int] = None
) -> Dict[str, str]:
    logger, log_filename = setup_job_logger(job_id=str(file_id), log_dir="job_logs", console_output=True)
    process = psutil.Process()
    temp_images_dir, temp_excel_dir = None, None

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
        mem_info = process.memory_info()
        logger.debug(f"Memory before fetching images: RSS={mem_info.rss / 1024**2:.2f} MB")
        
        selected_images_df = await get_images_excel_db(str(file_id), logger=logger)
        logger.info(f"Fetched DataFrame for ID {file_id}, shape: {selected_images_df.shape}, columns: {list(selected_images_df.columns)}")
        
        expected_columns = ["ExcelRowID", "ImageUrl", "ImageUrlThumbnail", "Brand", "Style", "Color", "Category"]
        if list(selected_images_df.columns) != expected_columns:
            logger.error(f"Invalid columns in DataFrame for ID {file_id}. Got: {list(selected_images_df.columns)}, Expected: {expected_columns}")
            return {"error": f"Invalid DataFrame columns: {list(selected_images_df.columns)}", "log_filename": log_filename}
        
        if selected_images_df.shape[1] != 7:
            logger.error(f"Invalid DataFrame shape for ID {file_id}: got {selected_images_df.shape}, expected (N, 7)")
            return {"error": f"Invalid DataFrame shape: {selected_images_df.shape}", "log_filename": log_filename}
        
        if selected_images_df.empty:
            logger.warning(f"No images found for ID {file_id}. Creating empty Excel file.")
            async with async_engine.connect() as conn:
                result = await conn.execute(
                    text("SELECT COUNT(*) FROM utb_ImageScraperFiles WHERE ID = :file_id"),
                    {"file_id": file_id}
                )
                file_count = result.fetchone()[0]
                result = await conn.execute(
                    text("SELECT COUNT(*) FROM utb_ImageScraperRecords WHERE FileID = :file_id"),
                    {"file_id": file_id}
                )
                record_count = result.fetchone()[0]
                result = await conn.execute(
                    text("""
                        SELECT COUNT(*) FROM utb_ImageScraperResult r 
                        INNER JOIN utb_ImageScraperRecords s ON r.EntryID = s.EntryID 
                        WHERE s.FileID = :file_id AND r.SortOrder >= 0
                    """),
                    {"file_id": file_id}
                )
                result_count = result.fetchone()[0]
                result.close()
                logger.info(f"Diagnostics: Files={file_count}, Records={record_count}, Results={result_count}")
            
            template_file_path = "https://iconluxurygroup.s3.us-east-2.amazonaws.com/ICON_DISTRO_USD_20250312.xlsx"
            base_name, extension = os.path.splitext(original_filename)
            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            unique_id = base_name[-8:] if len(base_name) >= 8 else base_name
            processed_file_name = f"super_scraper/jobs/{file_id}/{base_name}_scraper_{timestamp}_{unique_id}{extension}"
            
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
            
            with pd.ExcelWriter(local_filename, engine='openpyxl', mode='a') as writer:
                pd.DataFrame({"Message": [f"No images found for FileID {file_id}. Check utb_ImageScraperResult for valid images."]}).to_excel(writer, sheet_name="NoImages", index=False)
            
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
            
            await update_file_location_complete(str(file_id), public_url, logger=logger)
            await update_file_generate_complete(str(file_id), logger=logger)
            
            send_to_email_addr = await get_send_to_email(file_id, logger=logger)
            if not send_to_email_addr:
                logger.error(f"No email address for ID {file_id}")
                return {"error": "Failed to retrieve email address", "log_filename": log_filename}
            subject_line = f"{original_filename} Job Notification - No Images"
            await send_message_email(
                to_emails=send_to_email_addr,
                subject=subject_line,
                message=(
                    f"No images were found for FileID {file_id}. An empty Excel file has been generated.\n"
                    f"Download URL: {public_url}\n"
                    f"Diagnostics: {file_count} files, {record_count} records, {result_count} image results."
                ),
                logger=logger
            )
            
            logger.info(f"Completed ID {file_id} with no images")
            return {"message": "No images found, empty Excel file generated", "public_url": public_url, "log_filename": log_filename}
        
        logger.debug(f"Sample DataFrame rows: {selected_images_df.head(2).to_dict(orient='records')}")
        
        selected_image_list = [
            {
                'ExcelRowID': int(row['ExcelRowID']),
                'ImageUrl': row['ImageUrl'],
                'ImageUrlThumbnail': row['ImageUrlThumbnail'],
                'Brand': row.get('Brand', ''),
                'Style': row.get('Style', ''),
                'Color': row.get('Color', ''),
                'Category': row.get('Category', '')
            }
            for _, row in selected_images_df.iterrows()
            if pd.notna(row['ImageUrl']) and row['ImageUrl']
        ]
        logger.info(f"Selected {len(selected_image_list)} valid images after filtering")
        
        if not selected_image_list:
            logger.warning(f"No valid images after filtering for ID {file_id}")
            template_file_path = "https://iconluxurygroup.s3.us-east-2.amazonaws.com/ICON_DISTRO_USD_20250312.xlsx"
            base_name, extension = os.path.splitext(original_filename)
            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            unique_id = base_name[-8:] if len(base_name) >= 8 else base_name
            processed_file_name = f"super_scraper/jobs/{file_id}/{base_name}_scraper_{timestamp}_{unique_id}{extension}"
            
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
            
            with pd.ExcelWriter(local_filename, engine='openpyxl', mode='a') as writer:
                pd.DataFrame({"Message": [f"No valid images found for FileID {file_id} after filtering."]}).to_excel(writer, sheet_name="NoImages", index=False)
            
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
            
            await update_file_location_complete(str(file_id), public_url, logger=logger)
            await update_file_generate_complete(str(file_id), logger=logger)
            
            send_to_email_addr = await get_send_to_email(file_id, logger=logger)
            if not send_to_email_addr:
                logger.error(f"No email address for ID {file_id}")
                return {"error": "Failed to retrieve email address", "log_filename": log_filename}
            subject_line = f"{original_filename} Job Notification - No Images"
            await send_message_email(
                to_emails=send_to_email_addr,
                subject=subject_line,
                message=(
                    f"No valid images were found for FileID {file_id} after filtering.\n"
                    f"An empty Excel file has been generated.\n"
                    f"Download URL: {public_url}\n"
                    f"Diagnostics: {file_count} files, {record_count} records, {result_count} image results."
                ),
                logger=logger
            )
            
            logger.info(f"Completed ID {file_id} with no images")
            return {"message": "No valid images found, empty Excel file generated", "public_url": public_url, "log_filename": log_filename}
        
        logger.debug(f"Selected image list sample: {selected_image_list[:2]}")
        
        template_file_path = "https://iconluxurygroup.s3.us-east-2.amazonaws.com/ICON_DISTRO_USD_20250312.xlsx"
        header_index = 5
        base_name, extension = os.path.splitext(original_filename)
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = base_name[-8:] if len(base_name) >= 8 else base_name
        processed_file_name = f"super_scraper/jobs/{file_id}/{base_name}_scraper_{timestamp}_{unique_id}{extension}"
        logger.info(f"Generated filename: {processed_file_name}")

        temp_images_dir, temp_excel_dir = await create_temp_dirs(file_id, logger=logger)
        local_filename = os.path.join(temp_excel_dir, original_filename)

        logger.info(f"Downloading template file from {template_file_path}")
        async with httpx.AsyncClient() as client:
            response = await client.get(template_file_path, timeout=httpx.Timeout(30, connect=10))
            response.raise_for_status()
            async with aiofiles.open(local_filename, 'wb') as f:
                await f.write(response.content)
        
        if not os.path.exists(local_filename):
            logger.error(f"Template file not found at {local_filename}")
            return {"error": f"Failed to download template file", "log_filename": log_filename}
        logger.debug(f"Template file saved: {local_filename}, size: {os.path.getsize(local_filename)} bytes")

        logger.debug(f"Using temp_images_dir: {temp_images_dir}")
        failed_downloads = await download_all_images(selected_image_list, temp_images_dir, logger=logger)
        logger.info(f"Downloaded images, {len(failed_downloads)} failed")

        failed_downloads = [(url, int(row_id)) for url, row_id in failed_downloads]
        
        logger.info(f"Writing images with row_offset={header_index}")
        failed_rows = await write_excel_image(
            local_filename, temp_images_dir, selected_image_list, "A", header_index, logger
        )

        if failed_downloads:
            logger.info(f"Writing {len(failed_downloads)} failed downloads to Excel")
            success = await write_failed_downloads_to_excel(failed_downloads, local_filename, logger=logger)
            if not success:
                logger.warning(f"Failed to write some failed downloads to Excel")

        if not os.path.exists(local_filename):
            logger.error(f"Excel file not found at {local_filename}")
            return {"error": f"Excel file not found", "log_filename": log_filename}
        logger.debug(f"Excel file exists: {local_filename}, size: {os.path.getsize(local_filename)} bytes")
        logger.debug(f"Temp excel dir contents: {os.listdir(temp_excel_dir)}")

        mem_info = process.memory_info()
        logger.debug(f"Memory before S3 upload: RSS={mem_info.rss / 1024**2:.2f} MB")
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

        await update_file_location_complete(str(file_id), public_url, logger=logger)
        await update_file_generate_complete(str(file_id), logger=logger)

        send_to_email_addr = await get_send_to_email(file_id, logger=logger)
        if not send_to_email_addr:
            logger.error(f"No email address for ID {file_id}")
            return {"error": "Failed to retrieve email address", "log_filename": log_filename}
        subject_line = f"{original_filename} Job Notification"
        await send_message_email(
            to_emails=send_to_email_addr,
            subject=subject_line,
            message=f"Excel file generation for FileID {file_id} completed successfully.\nDownload URL: {public_url}",
            logger=logger
        )

        logger.info(f"Completed ID {file_id}")
        mem_info = process.memory_info()
        logger.debug(f"Memory after completion: RSS={mem_info.rss / 1024**2:.2f} MB")
        return {"message": "Processing completed successfully", "public_url": public_url, "log_filename": log_filename}
    except Exception as e:
        logger.error(f"Error for ID {file_id}: {e}", exc_info=True)
        send_to_email_addr = await get_send_to_email(file_id, logger=logger)
        if send_to_email_addr:
            await send_message_email(
                to_emails=send_to_email_addr,
                subject=f"Error: Job Failed for FileID {file_id}",
                message=f"Excel file generation for FileID {file_id} failed.\nError: {str(e)}\nLog file: {log_filename}",
                logger=logger
            )
        return {"error": f"An error occurred: {str(e)}", "log_filename": log_filename}
    finally:
        if temp_images_dir and temp_excel_dir:
            await cleanup_temp_dirs([temp_images_dir, temp_excel_dir], logger=logger)
        logger.info(f"Cleaned up temporary directories for ID {file_id}")
        await async_engine.dispose()
        engine.dispose()
        logger.info(f"Disposed database engines")

async def detect_job_failure(log_filename: str, logger: logging.Logger) -> bool:
    try:
        async with aiofiles.open(log_filename, 'r') as f:
            content = await f.read()
            error_keywords = ['ERROR', 'Traceback', 'Exception', 'NameError', 'TypeError']
            for keyword in error_keywords:
                if keyword in content:
                    logger.warning(f"Detected failure in log {log_filename}: {keyword} found")
                    return True
        logger.info(f"No failure detected in log {log_filename}")
        return False
    except Exception as e:
        logger.error(f"Error reading log {log_filename}: {e}", exc_info=True)
        return True