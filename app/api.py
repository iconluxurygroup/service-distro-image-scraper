import logging
import asyncio
import os
from typing import Dict, Any, Callable, Union, List, Optional
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, APIRouter
from workflow import process_restart_batch, generate_download_file, batch_vision_reason
from db_utils import (
    update_initial_sort_order,
    update_sort_order,
    update_log_url_in_db,
    fetch_missing_images,
    get_images_excel_db,
    get_send_to_email,
    update_file_generate_complete,
    update_file_location_complete,
    update_sort_no_image_entry,
    update_sort_order_per_entry,
    fetch_last_valid_entry,
)
from aws_s3 import upload_file_to_space, upload_file_to_space_sync
from email_utils import send_message_email
from logging_config import setup_job_logger
import traceback
import psutil
from config import VERSION

# Initialize FastAPI app
app = FastAPI(title="super_scraper", version=VERSION)

# Default logger
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Create APIRouter instance
router = APIRouter()

async def run_job_with_logging(job_func: Callable[..., Any], file_id: Union[str, int], **kwargs) -> Dict:
    """
    Run a job function with logging and upload logs to storage.
    Returns standardized response with status_code, message, and data.
    """
    file_id_str = str(file_id)
    logger, log_file = setup_job_logger(job_id=file_id_str, console_output=True)
    result = None
    try:
        func_name = getattr(job_func, '_name', 'unknown_function') if hasattr(job_func, '_remote') else job_func.__name__
        logger.info(f"Starting job {func_name} for FileID: {file_id}")

        # Monitor memory usage
        process = psutil.Process()
        mem_before = process.memory_info().rss / 1024 / 1024  # MB
        logger.debug(f"Memory before job {func_name}: RSS={mem_before:.2f} MB")

        if asyncio.iscoroutinefunction(job_func) or hasattr(job_func, '_remote'):
            result = await job_func(file_id, **kwargs)
        else:
            result = job_func(file_id, **kwargs)

        mem_after = process.memory_info().rss / 1024 / 1024  # MB
        logger.debug(f"Memory after job {func_name}: RSS={mem_after:.2f} MB")
        if mem_after > 1000:  # Warn if memory usage exceeds 1GB
            logger.warning(f"High memory usage after job {func_name}: RSS={mem_after:.2f} MB")

        logger.info(f"Completed job {func_name} for FileID: {file_id}")
        return {"status_code": 200, "message": f"Job {func_name} completed successfully for FileID: {file_id}", "data": result}
    except Exception as e:
        func_name = getattr(job_func, '_name', 'unknown_function') if hasattr(job_func, '_remote') else job_func.__name__
        logger.error(f"Error in job {func_name} for FileID: {file_id}: {str(e)}")
        logger.debug(f"Traceback: {traceback.format_exc()}")
        return {"status_code": 500, "message": f"Error in job {func_name} for FileID {file_id}: {str(e)}"}
    finally:
        if os.path.exists(log_file):
            try:
                upload_result = upload_file_to_space(
                    file_src=log_file,
                    save_as=f"super_scraper/logs/job_{file_id_str}.log",
                    is_public=True,
                    logger=logger,
                    file_id=file_id_str
                )
                if asyncio.iscoroutine(upload_result):
                    upload_url = await upload_result
                else:
                    upload_url = upload_result
                await update_log_url_in_db(file_id_str, upload_url, logger)
                logger.info(f"Log uploaded to: {upload_url}")
            except Exception as upload_error:
                logger.warning(f"Async upload failed for FileID {file_id_str}: {upload_error}")
                upload_url = upload_file_to_space_sync(
                    file_src=log_file,
                    save_as=f"super_scraper/logs/job_{file_id_str}.log",
                    is_public=True,
                    logger=logger,
                    file_id=file_id_str
                )
                await update_log_url_in_db(file_id_str, upload_url, logger)
                logger.info(f"Log uploaded to: {upload_url}")
        else:
            logger.warning(f"Log file {log_file} does not exist, skipping upload")

async def monitor_and_resubmit_failed_jobs(file_id: str, logger: logging.Logger):
    """
    Monitor job logs for failures and resubmit if necessary.
    """
    log_file = f"job_logs/job_{file_id}.log"
    max_attempts = 3
    attempt = 1

    while attempt <= max_attempts:
        if os.path.exists(log_file):
            with open(log_file, 'r') as f:
                log_content = f.read()
                if "WORKER TIMEOUT" in log_content or "SIGKILL" in log_content:
                    logger.warning(f"Detected failure in job for FileID: {file_id}, attempt {attempt}/{max_attempts}")
                    last_entry_id = await fetch_last_valid_entry(file_id, logger)
                    if last_entry_id:
                        # Resume from the next EntryID
                        with pyodbc.connect(conn_str) as conn:
                            cursor = conn.cursor()
                            cursor.execute(
                                "SELECT MIN(EntryID) FROM utb_ImageScraperRecords WHERE FileID = ? AND EntryID > ?",
                                (int(file_id), last_entry_id)
                            )
                            next_entry = cursor.fetchone()
                            resume_entry_id = next_entry[0] if next_entry and next_entry[0] else None
                            logger.info(f"Resubmitting job for FileID: {file_id} starting from EntryID: {resume_entry_id}")
                            result = await process_restart_batch(
                                file_id_db=int(file_id),
                                logger=logger,
                                entry_id=resume_entry_id,
                                use_all_variations=False
                            )
                            if "error" not in result:
                                logger.info(f"Resubmission successful for FileID: {file_id}")
                                return
                            else:
                                logger.error(f"Resubmission failed for FileID: {file_id}: {result['error']}")
                    else:
                        logger.warning(f"No last valid EntryID found for FileID: {file_id}, restarting from beginning")
                        result = await process_restart_batch(
                            file_id_db=int(file_id),
                            logger=logger,
                            entry_id=None,
                            use_all_variations=False
                        )
                        if "error" not in result:
                            logger.info(f"Resubmission successful for FileID: {file_id}")
                            return
                        else:
                            logger.error(f"Resubmission failed for FileID: {file_id}: {result['error']}")
                    attempt += 1
                else:
                    logger.info(f"No failure detected in logs for FileID: {file_id}")
                    return
        else:
            logger.warning(f"Log file {log_file} does not exist for FileID: {file_id}")
            return
        await asyncio.sleep(60)  # Wait before retrying

@router.post("/restart-job/{file_id}", tags=["Processing"])
async def api_process_restart(file_id: str, entry_id: Optional[int] = None, background_tasks: BackgroundTasks = None):
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Queueing restart of batch for FileID: {file_id}" + (f", EntryID: {entry_id}" if entry_id else ""))
    try:
        # Check for last processed EntryID if not provided
        if not entry_id:
            entry_id = await fetch_last_valid_entry(file_id, logger)
            if entry_id:
                # Resume from the next EntryID
                with pyodbc.connect(conn_str) as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT MIN(EntryID) FROM utb_ImageScraperRecords WHERE FileID = ? AND EntryID > ?",
                        (int(file_id), entry_id)
                    )
                    next_entry = cursor.fetchone()
                    entry_id = next_entry[0] if next_entry and next_entry[0] else None
                    logger.info(f"Retrieved last valid EntryID: {entry_id} for FileID: {file_id}")

        result = await process_restart_batch(
            file_id_db=int(file_id),
            logger=logger,
            entry_id=entry_id
        )
        if "error" in result:
            logger.error(f"Failed to process restart batch for FileID {file_id}: {result['error']}")
            raise HTTPException(status_code=500, detail=result["error"])

        # Schedule failure monitoring
        if background_tasks:
            background_tasks.add_task(monitor_and_resubmit_failed_jobs, file_id, logger)

        logger.info(f"Completed restart batch for FileID: {file_id}. Result: {result}")
        return {"status_code": 200, "message": f"Processing restart completed for FileID: {file_id}", "data": result}
    except Exception as e:
        logger.error(f"Error queuing restart batch for FileID {file_id}: {e}", exc_info=True)
        if os.path.exists(log_filename):
            upload_url = await upload_file_to_space(
                log_filename, f"super_scraper/logs/job_{file_id}.log", True, logger, file_id
            )
            await update_log_url_in_db(file_id, upload_url, logger)
        raise HTTPException(status_code=500, detail=f"Error restarting batch for FileID {file_id}: {str(e)}")

@router.post("/restart-search-all/{file_id}", tags=["Processing"])
async def api_restart_search_all(file_id: str, entry_id: Optional[int] = None, background_tasks: BackgroundTasks = None):
    """Restart batch processing for a file, searching all variations for each entry."""
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Queueing restart of batch for FileID: {file_id}" + (f", EntryID: {entry_id}" if entry_id else "") + " with all variations")
    try:
        # Check for last processed EntryID if not provided
        if not entry_id:
            entry_id = await fetch_last_valid_entry(file_id, logger)
            if entry_id:
                # Resume from the next EntryID
                with pyodbc.connect(conn_str) as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT MIN(EntryID) FROM utb_ImageScraperRecords WHERE FileID = ? AND EntryID > ?",
                        (int(file_id), entry_id)
                    )
                    next_entry = cursor.fetchone()
                    entry_id = next_entry[0] if next_entry and next_entry[0] else None
                    logger.info(f"Retrieved last valid EntryID: {entry_id} for FileID: {file_id}")

        result = await run_job_with_logging(
            process_restart_batch,
            file_id,
            entry_id=entry_id,
            use_all_variations=True
        )
        if result["status_code"] != 200:
            logger.error(f"Failed to process restart batch for FileID {file_id}: {result['message']}")
            raise HTTPException(status_code=result["status_code"], detail=result["message"])

        # Schedule failure monitoring
        if background_tasks:
            background_tasks.add_task(monitor_and_resubmit_failed_jobs, file_id, logger)

        logger.info(f"Completed restart batch for FileID: {file_id}. Result: {result}")
        return {"status_code": 200, "message": f"Processing restart with all variations completed for FileID: {file_id}", "data": result["data"]}
    except Exception as e:
        logger.error(f"Error queuing restart batch for FileID {file_id}: {e}", exc_info=True)
        if os.path.exists(log_filename):
            upload_url = await upload_file_to_space(
                log_filename, f"super_scraper/logs/job_{file_id}.log", True, logger, file_id
            )
            await update_log_url_in_db(file_id, upload_url, logger)
            # Send email on failure
            subject = f"Failure: Batch Restart for FileID {file_id}"
            message = f"Batch restart for FileID {file_id} failed.\nError: {str(e)}\nLog file: {upload_url}"
            await send_message_email(
                to_emails=["nik@iconluxurygroup.com"],
                subject=subject,
                message=message,
                logger=logger
            )
        raise HTTPException(status_code=500, detail=f"Error restarting batch with all variations for FileID {file_id}: {str(e)}")

# Include the router in the FastAPI app
app.include_router(router, prefix="/api/v3")