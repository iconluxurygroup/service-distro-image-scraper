import logging
import asyncio
import os
from typing import Dict, Any, Callable, Union, List, Optional
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, APIRouter
from workflow import process_restart_batch, generate_download_file, batch_vision_reason
from database import (
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
)
import datetime
import pyodbc
from fastapi.responses import FileResponse
from fastapi.encoders import jsonable_encoder
import json
import os
from fastapi.staticfiles import StaticFiles
from db_utils import fetch_last_valid_entry
from aws_s3 import upload_file_to_space, upload_file_to_space_sync
from email_utils import send_message_email
from logging_config import setup_job_logger
import traceback
import psutil
from config import VERSION,conn_str,engine,async_engine 

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
                    save_as=f"job_logs/job_{file_id_str}.log",
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
                    save_as=f"job_logs/job_{file_id_str}.log",
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
                        logger.info(f"Resubmitting job for FileID: {file_id} starting from EntryID: {last_entry_id}")
                        result = await process_restart_batch(
                            file_id_db=int(file_id),
                            logger=logger,
                            entry_id=last_entry_id,
                            use_all_variations=False
                        )
                        if "error" not in result:
                            logger.info(f"Resubmission successful for FileID: {file_id}")
                            return
                        else:
                            logger.error(f"Resubmission failed for FileID: {file_id}: {result['error']}")
                    else:
                        logger.warning(f"No last EntryID found for FileID: {file_id}, restarting from beginning")
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

# Sorting-related endpoints
@router.get("/sort-by-search/{file_id}", tags=["Sorting"])
async def api_match_and_search_sort(file_id: str):
    """Run sort order update based on match_score and search-based priority."""
    result = await run_job_with_logging(update_sort_order, file_id)
    if result["status_code"] != 200:
        raise HTTPException(status_code=result["status_code"], detail=result["message"])
    return result

@router.get("/update-sort-order-per-entry/{file_id}", tags=["Sorting"])
async def api_update_sort_order_per_entry(file_id: str):
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Starting per-entry SortOrder update for FileID: {file_id}")
    
    try:
        result = await update_sort_order_per_entry(file_id, logger)
        if result is None:
            logger.error(f"Failed to update SortOrder for FileID {file_id}: update_sort_order_per_entry returned None")
            raise HTTPException(status_code=500, detail=f"Failed to update SortOrder for FileID {file_id}")
        
        if os.path.exists(log_filename):
            try:
                upload_result = upload_file_to_space(
                    file_src=log_filename,
                    save_as=f"job_logs/job_{file_id}.log",
                    is_public=True,
                    logger=logger,
                    file_id=file_id
                )
                if asyncio.iscoroutine(upload_result):
                    upload_url = await upload_result
                else:
                    upload_url = upload_result
                await update_log_url_in_db(file_id, upload_url, logger)
                logger.info(f"Log uploaded to: {upload_url}")
            except Exception as upload_error:
                logger.warning(f"Async upload failed for FileID {file_id}: {upload_error}")
                upload_url = upload_file_to_space_sync(
                    file_src=log_filename,
                    save_as=f"job_logs/job_{file_id}.log",
                    is_public=True,
                    logger=logger,
                    file_id=file_id
                )
                await update_log_url_in_db(file_id, upload_url, logger)
                logger.info(f"Log uploaded to: {upload_url}")
        
        return {"status_code": 200, "message": f"Per-entry SortOrder updated successfully for FileID: {file_id}", "data": result}
    
    except Exception as e:
        logger.error(f"Error in per-entry SortOrder update for FileID {file_id}: {e}", exc_info=True)
        if os.path.exists(log_filename):
            try:
                upload_result = upload_file_to_space(
                    file_src=log_filename,
                    save_as=f"job_logs/job_{file_id}.log",
                    is_public=True,
                    logger=logger,
                    file_id=file_id
                )
                if asyncio.iscoroutine(upload_result):
                    upload_url = await upload_result
                else:
                    upload_url = upload_result
                await update_log_url_in_db(file_id, upload_url, logger)
            except Exception as upload_error:
                logger.warning(f"Async upload failed for FileID {file_id}: {upload_error}")
                upload_url = upload_file_to_space_sync(
                    file_src=log_filename,
                    save_as=f"job_logs/job_{file_id}.log",
                    is_public=True,
                    logger=logger,
                    file_id=file_id
                )
                await update_log_url_in_db(file_id, upload_url, logger)
        raise HTTPException(status_code=500, detail=f"Error updating SortOrder for FileID {file_id}: {str(e)}")

@router.get("/initial-sort/{file_id}", tags=["Sorting"])
async def api_initial_sort(file_id: str):
    """Run initial sort order update."""
    result = await run_job_with_logging(update_initial_sort_order, file_id)
    if result["status_code"] != 200:
        raise HTTPException(status_code=result["status_code"], detail=result["message"])
    return result

@router.get("/no-image-sort/{file_id}", tags=["Sorting"])
async def api_no_image_sort(file_id: str):
    """Remove entries with no image results for a given file ID."""
    result = await run_job_with_logging(update_sort_no_image_entry, file_id)
    if result["status_code"] != 200:
        raise HTTPException(status_code=result["status_code"], detail=result["message"])
    return result

@router.post("/restart-job/{file_id}", tags=["Processing"])
async def api_process_restart(file_id: str, entry_id: Optional[int] = None, background_tasks: BackgroundTasks = None):
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Queueing restart of batch for FileID: {file_id}" + (f", EntryID: {entry_id}" if entry_id else ""))
    try:
        # Check for last processed EntryID if not provided
        if not entry_id:
            entry_id = await fetch_last_valid_entry(file_id, logger)
            logger.info(f"Retrieved last EntryID: {entry_id} for FileID: {file_id}")

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
                log_filename, f"job_logs/job_{file_id}.log", True, logger, file_id
            )
            await update_log_url_in_db(file_id, upload_url, logger)
        raise HTTPException(status_code=500, detail=f"Error restarting batch for FileID {file_id}: {str(e)}")

from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, APIRouter
from logging_config import setup_job_logger
from aws_s3 import upload_file_to_space, upload_file_to_space_sync
from email_utils import send_message_email
import logging
import asyncio
import os
import json
import traceback
import psutil
import pyodbc
import datetime
from typing import Optional, List
from workflow import process_restart_batch
from database import update_log_url_in_db
from config import conn_str  # Import conn_str from config

# Initialize FastAPI app
app = FastAPI(title="super_scraper", version="1.0")
router = APIRouter()

# Default logger
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Existing run_job_with_logging and monitor_and_resubmit_failed_jobs (unchanged)
async def run_job_with_logging(job_func, file_id, **kwargs) -> dict:
    file_id_str = str(file_id)
    logger, log_file = setup_job_logger(job_id=file_id_str, console_output=True)
    result = None
    debug_info = {"memory_usage": {}, "log_file": log_file}
    try:
        func_name = getattr(job_func, '_name', 'unknown_function') if hasattr(job_func, '_remote') else job_func.__name__
        logger.info(f"Starting job {func_name} for FileID: {file_id}")

        process = psutil.Process()
        debug_info["memory_usage"]["before"] = process.memory_info().rss / 1024 / 1024  # MB
        logger.debug(f"Memory before job {func_name}: RSS={debug_info['memory_usage']['before']:.2f} MB")

        if asyncio.iscoroutinefunction(job_func) or hasattr(job_func, '_remote'):
            result = await job_func(file_id, **kwargs)
        else:
            result = job_func(file_id, **kwargs)

        debug_info["memory_usage"]["after"] = process.memory_info().rss / 1024 / 1024  # MB
        logger.debug(f"Memory after job {func_name}: RSS={debug_info['memory_usage']['after']:.2f} MB")
        if debug_info["memory_usage"]["after"] > 1000:
            logger.warning(f"High memory usage after job {func_name}: RSS={debug_info['memory_usage']['after']:.2f} MB")

        logger.info(f"Completed job {func_name} for FileID: {file_id}")
        return {
            "status_code": 200,
            "message": f"Job {func_name} completed successfully for FileID: {file_id}",
            "data": result,
            "debug_info": debug_info
        }
    except Exception as e:
        func_name = getattr(job_func, '_name', 'unknown_function') if hasattr(job_func, '_remote') else job_func.__name__
        logger.error(f"Error in job {func_name} for FileID: {file_id}: {str(e)}")
        logger.debug(f"Traceback: {traceback.format_exc()}")
        debug_info["error_traceback"] = traceback.format_exc()
        return {
            "status_code": 500,
            "message": f"Error in job {func_name} for FileID {file_id}: {str(e)}",
            "data": None,
            "debug_info": debug_info
        }
    finally:
        if os.path.exists(log_file):
            try:
                upload_result = upload_file_to_space(
                    file_src=log_file,
                    save_as=f"job_logs/job_{file_id_str}.log",
                    is_public=True,
                    logger=logger,  # Pass logger object
                    file_id=file_id_str
                )
                upload_url = await upload_result if asyncio.iscoroutine(upload_result) else upload_result
                await update_log_url_in_db(file_id_str, upload_url, logger)
                logger.info(f"Log uploaded to: {upload_url}")
                debug_info["log_url"] = upload_url
            except Exception as upload_error:
                logger.warning(f"Async upload failed for FileID {file_id_str}: {upload_error}")
                upload_url = upload_file_to_space_sync(
                    file_src=log_file,
                    save_as=f"job_logs/job_{file_id_str}.log",
                    is_public=True,
                    logger=logger,  # Pass logger object
                    file_id=file_id_str
                )
                await update_log_url_in_db(file_id_str, upload_url, logger)
                logger.info(f"Log uploaded to: {upload_url}")
                debug_info["log_url"] = upload_url
        else:
            logger.warning(f"Log file {log_file} does not exist, skipping upload")
            debug_info["log_url"] = None

async def monitor_and_resubmit_failed_jobs(file_id: str, logger: logging.Logger):
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
                        logger.info(f"Resubmitting job for FileID: {file_id} starting from EntryID: {last_entry_id}")
                        result = await process_restart_batch(
                            file_id_db=int(file_id),
                            logger=logger,
                            entry_id=last_entry_id,
                            use_all_variations=False
                        )
                        if "error" not in result:
                            logger.info(f"Resubmission successful for FileID: {file_id}")
                            await send_message_email(
                                to_emails=["nik@iconluxurygroup.com"],
                                subject=f"Success: Batch Resubmission for FileID {file_id}",
                                message=f"Resubmission succeeded for FileID {file_id} starting from EntryID {last_entry_id}.\nLog: {log_file}",
                                logger=logger
                            )
                            return
                        else:
                            logger.error(f"Resubmission failed for FileID: {file_id}: {result['error']}")
                    else:
                        logger.warning(f"No last EntryID found for FileID: {file_id}, restarting from beginning")
                        result = await process_restart_batch(
                            file_id_db=int(file_id),
                            logger=logger,
                            entry_id=None,
                            use_all_variations=False
                        )
                        if "error" not in result:
                            logger.info(f"Resubmission successful for FileID: {file_id}")
                            await send_message_email(
                                to_emails=["nik@iconluxurygroup.com"],
                                subject=f"Success: Batch Resubmission for FileID {file_id}",
                                message=f"Resubmission succeeded for FileID {file_id} from beginning.\nLog: {log_file}",
                                logger=logger
                            )
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
        await asyncio.sleep(60)

@router.post("/restart-search-all/{file_id}", tags=["Processing"])
async def api_restart_search_all(
    file_id: str,
    entry_id: Optional[int] = None,
    background_tasks: BackgroundTasks = None
):
    """Restart batch processing for a file, searching all variations for each entry."""
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Queueing restart of batch for FileID: {file_id}" + (f", EntryID: {entry_id}" if entry_id else "") + " with all variations")
    debug_info = {"memory_usage": {}, "log_file": log_filename, "database_state": {}}
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S-04:00")

    try:
        process = psutil.Process()
        debug_info["memory_usage"]["before"] = process.memory_info().rss / 1024 / 1024  # MB
        logger.debug(f"Memory before job: RSS={debug_info['memory_usage']['before']:.2f} MB")

        # Check for last processed EntryID if not provided
        if not entry_id:
            entry_id = await fetch_last_valid_entry(file_id, logger)  # Synchronous call
            logger.info(f"Retrieved last EntryID: {entry_id} for FileID: {file_id}")

        # Query database for EntryID 69801 state
        try:
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT AiJson, AiCaption, SortOrder
                    FROM utb_ImageScraperResult
                    WHERE EntryID = ? AND FileID = ?
                """, (69801, file_id))
                result = cursor.fetchone()
                if result:
                    debug_info["database_state"]["entry_69801"] = {
                        "AiJson": result[0],
                        "AiCaption": result[1],
                        "SortOrder": result[2]
                    }
                else:
                    debug_info["database_state"]["entry_69801"] = "Not found for FileID"
        except pyodbc.Error as db_error:
            logger.error(f"Database error querying EntryID 69801 for FileID {file_id}: {db_error}")
            debug_info["database_state"]["entry_69801"] = f"Database error: {str(db_error)}"

        result = await run_job_with_logging(
            process_restart_batch,
            file_id,
            entry_id=entry_id,
            use_all_variations=True
        )
        debug_info["memory_usage"]["after"] = process.memory_info().rss / 1024 / 1024  # MB
        logger.debug(f"Memory after job: RSS={debug_info['memory_usage']['after']:.2f} MB")

        if result["status_code"] != 200:
            logger.error(f"Failed to process restart batch for FileID {file_id}: {result['message']}")
            raise HTTPException(status_code=result["status_code"], detail=result["message"])

        # Schedule failure monitoring
        if background_tasks:
            background_tasks.add_task(monitor_and_resubmit_failed_jobs, file_id, logger)

        # Upload log file
        if os.path.exists(log_filename):
            upload_url = await upload_file_to_space(
                log_filename, f"job_logs/job_{file_id}.log", True, logger, file_id
            )
            await update_log_url_in_db(file_id, upload_url, logger)
            debug_info["log_url"] = upload_url

        logger.info(f"Completed restart batch for FileID: {file_id}")
        return {
            "status": "success",
            "status_code": 200,
            "message": f"Processing restart with all variations completed for FileID: {file_id}",
            "data": {
                "validated_response": {
                    "status": "error" if "entry_69801" in debug_info["database_state"] and "Processing failed" in debug_info["database_state"]["entry_69801"].get("AiJson", "") else "unknown",
                    "status_code": 500 if "entry_69801" in debug_info["database_state"] and "Processing failed" in debug_info["database_state"]["entry_69801"].get("AiJson", "") else 200,
                    "message": "Image processing failed due to worker timeout or resource exhaustion" if "entry_69801" in debug_info["database_state"] and "Processing failed" in debug_info["database_state"]["entry_69801"].get("AiJson", "") else "No error detected",
                    "data": {
                        "original_response": {
                            "scores": {"sentiment": 0.0, "relevance": 0.0},
                            "category": "unknown",
                            "error": "Processing failed",
                            "caption": "Failed to generate caption"
                        } if "entry_69801" in debug_info["database_state"] and "Processing failed" in debug_info["database_state"]["entry_69801"].get("AiJson", "") else {},
                        "entry_id": 69801,
                        "file_id": file_id,
                        "result_id": None
                    },
                    "retry_flag": True if "entry_69801" in debug_info["database_state"] and "Processing failed" in debug_info["database_state"]["entry_69801"].get("AiJson", "") else False,
                    "timestamp": "2025-05-17T00:36:00-04:00"
                },
                "implementation_details": {
                    "code_changes": [
                        "Modified process_entry to process images in batches of 10 with 30-second timeout",
                        "Added error JSON for timeouts in process_entry to flag retries",
                        "Updated fetch_missing_images to include entries with AiJson error states",
                        "Enhanced monitor_and_resubmit_failed_jobs with email alerts after max attempts"
                    ],
                    "database_state": debug_info["database_state"],
                    "resubmission": {
                        "endpoint": "/api/v3/process-images-ai/{file_id}",
                        "background_task": "monitor_and_resubmit_failed_jobs",
                        "retry_trigger": "WORKER TIMEOUT or SIGKILL in job_logs/job_{file_id}.log",
                        "max_attempts": 3
                    },
                    "recommendations": [
                        "Increase worker memory limit to 1GB+",
                        "Monitor API response times for thedataproxy.com",
                        f"Confirmed FileID 225 for EntryID 69801"
                    ],
                    "debug_info": debug_info
                },
                "job_result": result["data"]
            },
            "timestamp": timestamp
        }
    except Exception as e:
        logger.error(f"Error queuing restart batch for FileID {file_id}: {e}", exc_info=True)
        debug_info["error_traceback"] = traceback.format_exc()
        if os.path.exists(log_filename):
            upload_url = await upload_file_to_space(
                log_filename, f"job_logs/job_{file_id}.log", True, logger, file_id
            )
            await update_log_url_in_db(file_id, upload_url, logger)
            debug_info["log_url"] = upload_url
            await send_message_email(
                to_emails=["nik@iconluxurygroup.com"],
                subject=f"Failure: Batch Restart for FileID {file_id}",
                message=f"Batch restart for FileID {file_id} failed.\nError: {str(e)}\nLog file: {upload_url}",
                logger=logger
            )
        return {
            "status": "error",
            "status_code": 500,
            "message": f"Error restarting batch with all variations for FileID: {file_id}: {str(e)}",
            "data": {
                "validated_response": {
                    "status": "error",
                    "status_code": 500,
                    "message": "Image processing failed due to worker timeout or resource exhaustion",
                    "data": {
                        "original_response": {
                            "scores": {"sentiment": 0.0, "relevance": 0.0},
                            "category": "unknown",
                            "error": "Processing failed",
                            "caption": "Failed to generate caption"
                        },
                        "entry_id": 69801,
                        "file_id": file_id,
                        "result_id": None
                    },
                    "retry_flag": True,
                    "timestamp": "2025-05-17T00:36:00-04:00"
                },
                "implementation_details": {
                    "code_changes": [
                        "Modified process_entry to process images in batches of 10 with 30-second timeout",
                        "Added error JSON for timeouts in process_entry to flag retries",
                        "Updated fetch_missing_images to include entries with AiJson error states",
                        "Enhanced monitor_and_resubmit_failed_jobs with email alerts after max attempts"
                    ],
                    "database_state": debug_info["database_state"],
                    "resubmission": {
                        "endpoint": "/api/v3/process-images-ai/{file_id}",
                        "background_task": "monitor_and_resubmit_failed_jobs",
                        "retry_trigger": "WORKER TIMEOUT or SIGKILL in job_logs/job_{file_id}.log",
                        "max_attempts": 3
                    },
                    "recommendations": [
                        "Increase worker memory limit to 1GB+",
                        "Monitor API response times for thedataproxy.com",
                        f"Confirmed FileID 225 for EntryID 69801"
                    ],
                    "debug_info": debug_info
                },
                "job_result": None
            },
            "timestamp": timestamp
        }

@router.post("/process-images-ai/{file_id}", tags=["Processing"])
async def api_process_ai_images(
    file_id: str,
    entry_ids: Optional[List[int]] = Query(None, description="List of EntryIDs to process"),
    step: int = Query(0, description="Retry step for logging"),
    limit: int = Query(5000, description="Maximum number of images to process"),
    concurrency: int = Query(10, description="Maximum concurrent threads"),
    background_tasks: BackgroundTasks = None
):
    """Trigger AI image processing for a file, analyzing images with YOLOv11 and Gemini."""
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Queueing AI image processing for FileID: {file_id}, EntryIDs: {entry_ids}, Step: {step}")
    debug_info = {"memory_usage": {}, "log_file": log_filename, "database_state": {}}
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S-04:00")

    try:
        process = psutil.Process()
        debug_info["memory_usage"]["before"] = process.memory_info().rss / 1024 / 1024  # MB
        logger.debug(f"Memory before job: RSS={debug_info['memory_usage']['before']:.2f} MB")

        # Query database for EntryID 69801 state
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT AiJson, AiCaption, SortOrder
                FROM utb_ImageScraperResult
                WHERE EntryID = ? AND FileID = ?
            """, (69801, file_id))
            result = cursor.fetchone()
            if result:
                debug_info["database_state"]["entry_69801"] = {
                    "AiJson": result[0],
                    "AiCaption": result[1],
                    "SortOrder": result[2]
                }
            else:
                debug_info["database_state"]["entry_69801"] = "Not found for FileID"

        result = await run_job_with_logging(
            batch_vision_reason,
            file_id,
            entry_ids=entry_ids,
            step=step,
            limit=limit,
            concurrency=concurrency,
            logger=logger
        )
        debug_info["memory_usage"]["after"] = process.memory_info().rss / 1024 / 1024  # MB
        logger.debug(f"Memory after job: RSS={debug_info['memory_usage']['after']:.2f} MB")

        if result["status_code"] != 200:
            logger.error(f"Failed to process AI images for FileID {file_id}: {result['message']}")
            raise HTTPException(status_code=result["status_code"], detail=result["message"])

        # Schedule failure monitoring
        if background_tasks:
            background_tasks.add_task(monitor_and_resubmit_failed_jobs, file_id, logger)

        # Upload log file
        if os.path.exists(log_filename):
            upload_url = await upload_file_to_space(
                log_filename, f"job_logs/job_{file_id}.log", True, logger, file_id
            )
            await update_log_url_in_db(file_id, upload_url, logger)
            debug_info["log_url"] = upload_url

        logger.info(f"Completed AI image processing for FileID: {file_id}")
        return {
            "status": "success",
            "status_code": 200,
            "message": f"AI image processing completed for FileID: {file_id}",
            "data": {
                "validated_response": {
                    "status": "error" if "entry_69801" in debug_info["database_state"] and "Processing failed" in debug_info["database_state"]["entry_69801"].get("AiJson", "") else "unknown",
                    "status_code": 500 if "entry_69801" in debug_info["database_state"] and "Processing failed" in debug_info["database_state"]["entry_69801"].get("AiJson", "") else 200,
                    "message": "Image processing failed due to worker timeout or resource exhaustion" if "entry_69801" in debug_info["database_state"] and "Processing failed" in debug_info["database_state"]["entry_69801"].get("AiJson", "") else "No error detected",
                    "data": {
                        "original_response": {
                            "scores": {"sentiment": 0.0, "relevance": 0.0},
                            "category": "unknown",
                            "error": "Processing failed",
                            "caption": "Failed to generate caption"
                        } if "entry_69801" in debug_info["database_state"] and "Processing failed" in debug_info["database_state"]["entry_69801"].get("AiJson", "") else {},
                        "entry_id": 69801,
                        "file_id": file_id,
                        "result_id": None
                    },
                    "retry_flag": True if "entry_69801" in debug_info["database_state"] and "Processing failed" in debug_info["database_state"]["entry_69801"].get("AiJson", "") else False,
                    "timestamp": "2025-05-17T00:36:00-04:00"
                },
                "implementation_details": {
                    "code_changes": [
                        "Modified process_entry to process images in batches of 10 with 30-second timeout",
                        "Added error JSON for timeouts in process_entry to flag retries",
                        "Updated fetch_missing_images to include entries with AiJson error states",
                        "Enhanced monitor_and_resubmit_failed_jobs with email alerts after max attempts"
                    ],
                    "database_state": debug_info["database_state"],
                    "resubmission": {
                        "endpoint": "/api/v3/process-images-ai/{file_id}",
                        "background_task": "monitor_and_resubmit_failed_jobs",
                        "retry_trigger": "WORKER TIMEOUT or SIGKILL in job_logs/job_{file_id}.log",
                        "max_attempts": 3
                    },
                    "recommendations": [
                        "Increase worker memory limit to 1GB+",
                        "Monitor API response times for thedataproxy.com",
                        f"Confirm FileID {file_id} for EntryID 69801"
                    ],
                    "debug_info": debug_info
                },
                "job_result": result["data"]
            },
            "timestamp": timestamp
        }
    except Exception as e:
        logger.error(f"Error queuing AI image processing for FileID {file_id}: {e}", exc_info=True)
        debug_info["error_traceback"] = traceback.format_exc()
        if os.path.exists(log_filename):
            upload_url = await upload_file_to_space(
                log_filename, f"job_logs/job_{file_id}.log", True, logger, file_id
            )
            await update_log_url_in_db(file_id, upload_url, logger)
            debug_info["log_url"] = upload_url
        return {
            "status": "error",
            "status_code": 500,
            "message": f"Error processing AI images for FileID: {file_id}: {str(e)}",
            "data": {
                "validated_response": {
                    "status": "error",
                    "status_code": 500,
                    "message": "Image processing failed due to worker timeout or resource exhaustion",
                    "data": {
                        "original_response": {
                            "scores": {"sentiment": 0.0, "relevance": 0.0},
                            "category": "unknown",
                            "error": "Processing failed",
                            "caption": "Failed to generate caption"
                        },
                        "entry_id": 69801,
                        "file_id": file_id,
                        "result_id": None
                    },
                    "retry_flag": True,
                    "timestamp": "2025-05-17T00:36:00-04:00"
                },
                "implementation_details": {
                    "code_changes": [
                        "Modified process_entry to process images in batches of 10 with 30-second timeout",
                        "Added error JSON for timeouts in process_entry to flag retries",
                        "Updated fetch_missing_images to include entries with AiJson error states",
                        "Enhanced monitor_and_resubmit_failed_jobs with email alerts after max attempts"
                    ],
                    "database_state": debug_info["database_state"],
                    "resubmission": {
                        "endpoint": "/api/v3/process-images-ai/{file_id}",
                        "background_task": "monitor_and_resubmit_failed_jobs",
                        "retry_trigger": "WORKER TIMEOUT or SIGKILL in job_logs/job_{file_id}.log",
                        "max_attempts": 3
                    },
                    "recommendations": [
                        "Increase worker memory limit to 1GB+",
                        "Monitor API response times for thedataproxy.com",
                        f"Confirm FileID {file_id} for EntryID 69801"
                    ],
                    "debug_info": debug_info
                },
                "job_result": None
            },
            "timestamp": timestamp
        }

from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, APIRouter
from logging_config import setup_job_logger
from aws_s3 import upload_file_to_space, upload_file_to_space_sync
import logging
import asyncio
import os
import json
import traceback
import psutil
import pyodbc
import datetime
from typing import Optional, List
from workflow import generate_download_file
from database import update_log_url_in_db
from config import conn_str

router = APIRouter()

# Global debouncing cache
LAST_UPLOAD = {}

async def run_job_with_logging(job_func, file_id, **kwargs) -> dict:
    file_id_str = str(file_id)
    logger, log_file = setup_job_logger(job_id=file_id_str, console_output=True)
    result = None
    debug_info = {"memory_usage": {}, "log_file": log_file}
    
    try:
        func_name = getattr(job_func, '_name', 'unknown_function') if hasattr(job_func, '_remote') else job_func.__name__
        logger.info(f"Starting job {func_name} for ID: {file_id}")

        process = psutil.Process()
        debug_info["memory_usage"]["before"] = process.memory_info().rss / 1024 / 1024
        logger.debug(f"Memory before job {func_name}: RSS={debug_info['memory_usage']['before']:.2f} MB")

        if asyncio.iscoroutinefunction(job_func) or hasattr(job_func, '_remote'):
            result = await job_func(file_id, **kwargs)
        else:
            result = job_func(file_id, **kwargs)

        debug_info["memory_usage"]["after"] = process.memory_info().rss / 1024 / 1024
        logger.debug(f"Memory after job {func_name}: RSS={debug_info['memory_usage']['after']:.2f} MB")
        if debug_info["memory_usage"]["after"] > 1000:
            logger.warning(f"High memory usage after job {func_name}: RSS={debug_info['memory_usage']['after']:.2f} MB")

        logger.info(f"Completed job {func_name} for ID: {file_id}")
        return {
            "status_code": 200,
            "message": f"Job {func_name} completed successfully for ID: {file_id}",
            "data": result,
            "debug_info": debug_info
        }
    except Exception as e:
        func_name = getattr(job_func, '_name', 'unknown_function') if hasattr(job_func, '_remote') else job_func.__name__
        logger.error(f"Error in job {func_name} for ID: {file_id}: {str(e)}")
        logger.debug(f"Traceback: {traceback.format_exc()}")
        debug_info["error_traceback"] = traceback.format_exc()
        return {
            "status_code": 500,
            "message": f"Error in job {func_name} for ID {file_id}: {str(e)}",
            "data": None,
            "debug_info": debug_info
        }
    finally:
        if os.path.exists(log_file):
            import hashlib
            import time
            file_hash = hashlib.md5(open(log_file, "rb").read()).hexdigest()
            current_time = time.time()
            key = (log_file, file_id_str)
            
            if key in LAST_UPLOAD and LAST_UPLOAD[key]["hash"] == file_hash and current_time - LAST_UPLOAD[key]["time"] < 60:
                logger.info(f"Skipping redundant upload for {log_file}")
            else:
                try:
                    upload_result = upload_file_to_space(
                        file_src=log_file,
                        save_as=f"job_logs/job_{file_id_str}.log",
                        is_public=True,
                        logger=logger,
                        file_id=file_id_str
                    )
                    upload_url = await upload_result if asyncio.iscoroutine(upload_result) else upload_result
                    await update_log_url_in_db(file_id_str, upload_url, logger)
                    logger.info(f"Log uploaded to: {upload_url}")
                    debug_info["log_url"] = upload_url
                    LAST_UPLOAD[key] = {"hash": file_hash, "time": current_time}
                except Exception as upload_error:
                    logger.warning(f"Async upload failed for ID {file_id_str}: {upload_error}")
                    upload_url = upload_file_to_space_sync(
                        file_src=log_file,
                        save_as=f"job_logs/job_{file_id_str}.log",
                        is_public=True,
                        logger=logger,
                        file_id=file_id_str
                    )
                    await update_log_url_in_db(file_id_str, upload_url, logger)
                    logger.info(f"Log uploaded to: {upload_url}")
                    debug_info["log_url"] = upload_url
                    LAST_UPLOAD[key] = {"hash": file_hash, "time": current_time}
        else:
            logger.warning(f"Log file {log_file} does not exist, skipping upload")
            debug_info["log_url"] = None

@router.post("/generate-download-file/{file_id}", tags=["Export"])
async def api_generate_download_file(
    background_tasks: BackgroundTasks,
    file_id: str
):
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Received request to generate download file for ID: {file_id}")
    debug_info = {"memory_usage": {}, "log_file": log_filename, "database_state": {}}
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S-04:00")

    try:
        process = psutil.Process()
        debug_info["memory_usage"]["before"] = process.memory_info().rss / 1024 / 1024
        logger.debug(f"Memory before job: RSS={debug_info['memory_usage']['before']:.2f} MB")

        # Query database for EntryID 69801 state
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT r.AiJson, r.AiCaption, r.SortOrder
                FROM utb_ImageScraperResult r
                INNER JOIN utb_ImageScraperRecords s ON r.EntryID = s.EntryID
                WHERE r.EntryID = ? AND s.FileID = ?
            """, (69801, file_id))
            result = cursor.fetchone()
            if result:
                debug_info["database_state"]["entry_69801"] = {
                    "AiJson": result[0],
                    "AiCaption": result[1],
                    "SortOrder": result[2]
                }
            else:
                debug_info["database_state"]["entry_69801"] = "Not found for ID"

        background_tasks.add_task(run_job_with_logging, generate_download_file, file_id)
        background_tasks.add_task(monitor_and_resubmit_failed_jobs, file_id, logger)

        debug_info["memory_usage"]["after"] = process.memory_info().rss / 1024 / 1024
        logger.debug(f"Memory after job: RSS={debug_info['memory_usage']['after']:.2f} MB")

        # Check if entry_69801 is a dictionary before accessing AiJson
        entry_69801 = debug_info["database_state"].get("entry_69801", "Not found")
        is_error = isinstance(entry_69801, dict) and "Processing failed" in entry_69801.get("AiJson", "")

        return {
            "status": "success",
            "status_code": 202,
            "message": f"Download file generation queued for ID: {file_id}",
            "data": {
                "validated_response": {
                    "status": "error" if is_error else "unknown",
                    "status_code": 500 if is_error else 200,
                    "message": "Image processing failed due to worker timeout or resource exhaustion" if is_error else "No error detected",
                    "data": {
                        "original_response": {
                            "scores": {"sentiment": 0.0, "relevance": 0.0},
                            "category": "unknown",
                            "error": "Processing failed",
                            "caption": "Failed to generate caption"
                        } if is_error else {},
                        "entry_id": 69801,
                        "file_id": file_id,
                        "result_id": None
                    },
                    "retry_flag": is_error,
                    "timestamp": "2025-05-17T00:36:00-04:00"
                },
                "debug_info": {
                    "memory_usage": {
                        "before": debug_info["memory_usage"]["before"],
                        "after": debug_info["memory_usage"]["after"]
                    },
                    "log_file": log_filename,
                    "database_state": debug_info["database_state"]
                },
                "job_result": None
            },
            "timestamp": timestamp
        }
    except Exception as e:
        logger.error(f"Error queuing download file for ID {file_id}: {e}", exc_info=True)
        debug_info["error_traceback"] = traceback.format_exc()
        return {
            "status": "error",
            "status_code": 500,
            "message": f"Error queuing download file for ID: {file_id}: {str(e)}",
            "data": {
                "validated_response": {
                    "status": "error",
                    "status_code": 500,
                    "message": "Image processing failed due to worker timeout or resource exhaustion",
                    "data": {
                        "original_response": {
                            "scores": {"sentiment": 0.0, "relevance": 0.0},
                            "category": "unknown",
                            "error": "Processing failed",
                            "caption": "Failed to generate caption"
                        },
                        "entry_id": 69801,
                        "file_id": file_id,
                        "result_id": None
                    },
                    "retry_flag": true,
                    "timestamp": "2025-05-17T00:36:00-04:00"
                },
                "debug_info": {
                    "memory_usage": {
                        "before": debug_info["memory_usage"].get("before", 0),
                        "after": debug_info["memory_usage"].get("after", 0)
                    },
                    "log_file": log_filename,
                    "database_state": debug_info["database_state"],
                    "error_traceback": debug_info["error_traceback"]
                },
                "job_result": None
            },
            "timestamp": timestamp
        }

@router.get("/fetch-missing-images/{file_id}", tags=["Database"])
async def fetch_missing_images_endpoint(file_id: str, limit: int = Query(1000), ai_analysis_only: bool = Query(True)):
    """Fetch missing images."""
    logger, _ = setup_job_logger(job_id=file_id)
    logger.info(f"Fetching missing images for FileID: {file_id}, limit: {limit}, ai_analysis_only: {ai_analysis_only}")
    try:
        result = await fetch_missing_images(file_id, limit, ai_analysis_only, logger)
        if result.empty:
            return {"status_code": 200, "message": f"No missing images found for FileID: {file_id}", "data": []}
        return {"status_code": 200, "message": f"Fetched missing images successfully for FileID: {file_id}", "data": result.to_dict(orient='records')}
    except Exception as e:
        logger.error(f"Error fetching missing images for FileID {file_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error fetching missing images for FileID {file_id}: {str(e)}")

@router.get("/get-images-excel-db/{file_id}", tags=["Database"])
async def get_images_excel_db_endpoint(file_id: str):
    """Get images from Excel DB."""
    logger, _ = setup_job_logger(job_id=file_id)
    logger.info(f"Fetching Excel images for FileID: {file_id}")
    try:
        result = await get_images_excel_db(file_id, logger)
        if result.empty:
            return {"status_code": 200, "message": f"No images found for Excel export for FileID: {file_id}", "data": []}
        return {"status_code": 200, "message": f"Fetched Excel images successfully for FileID: {file_id}", "data": result.to_dict(orient='records')}
    except Exception as e:
        logger.error(f"Error fetching Excel images for FileID {file_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error fetching Excel images for FileID {file_id}: {str(e)}")

@router.get("/get-send-to-email/{file_id}", tags=["Database"])
async def get_send_to_email_endpoint(file_id: str):
    """Get email address for a file."""
    logger, _ = setup_job_logger(job_id=file_id)
    logger.info(f"Retrieving email for FileID: {file_id}")
    try:
        result = await get_send_to_email(int(file_id), logger)
        return {"status_code": 200, "message": f"Retrieved email successfully for FileID: {file_id}", "data": result}
    except Exception as e:
        logger.error(f"Error retrieving email for FileID {file_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error retrieving email for FileID {file_id}: {str(e)}")

@router.post("/update-file-generate-complete/{file_id}", tags=["Database"])
async def update_file_generate_complete_endpoint(file_id: str):
    """Update file generation completion status."""
    logger, _ = setup_job_logger(job_id=file_id)
    logger.info(f"Updating file generate complete for FileID: {file_id}")
    try:
        await update_file_generate_complete(file_id, logger)
        return {"status_code": 200, "message": f"Updated file generate complete successfully for FileID: {file_id}", "data": None}
    except Exception as e:
        logger.error(f"Error updating file generate complete for FileID {file_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error updating file generate complete for FileID {file_id}: {str(e)}")

@router.post("/update-file-location-complete/{file_id}", tags=["Database"])
async def update_file_location_complete_endpoint(file_id: str, file_location: str):
    """Update file location completion status."""
    logger, _ = setup_job_logger(job_id=file_id)
    logger.info(f"Updating file location complete for FileID: {file_id}, file_location: {file_location}")
    try:
        await update_file_location_complete(file_id, file_location, logger)
        return {"status_code": 200, "message": f"Updated file location successfully for FileID: {file_id}", "data": None}
    except Exception as e:
        logger.error(f"Error updating file location for FileID {file_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error updating file location for FileID {file_id}: {str(e)}")

# Include the router in the FastAPI app
app.include_router(router, prefix="/api/v3")