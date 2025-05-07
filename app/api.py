import logging
import asyncio
import os
import ray
from typing import Dict
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, APIRouter
from workflow import run_process_restart_batch, generate_download_file, process_file_with_retries
from database import (
    update_initial_sort_order,
    update_sort_order,
    set_sort_order_negative_four_for_zero_match,
    update_log_url_in_db,
    fetch_missing_images,
    get_images_excel_db,
    get_send_to_email,
    update_file_generate_complete,
    update_file_location_complete,
    update_sort_order_per_entry,
)
from aws_s3 import upload_file_to_space, upload_file_to_space_sync
from logging_config import setup_job_logger
import traceback
from typing import Callable, Any, Union

# Initialize FastAPI app
app = FastAPI(title="super_scraper", version="3.0.4")

# Default logger
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Create APIRouter instance
router = APIRouter()

async def run_job_with_logging(job_func: Callable[..., Any], file_id: int, **kwargs) -> Any:
    """
    Run a job function with logging and upload logs to storage.
    Handles both synchronous and asynchronous job functions and upload functions.
    """
    logger, _ = setup_job_logger(job_id=file_id, console_output=True)
    result = None
    try:
        func_name = getattr(job_func, '_name', 'unknown_function') if hasattr(job_func, '_remote') else job_func.__name__
        logger.info(f"Starting job {func_name} for FileID: {file_id}")
        
        if asyncio.iscoroutinefunction(job_func) or hasattr(job_func, '_remote'):
            result = await job_func(file_id, **kwargs)
        else:
            result = job_func(file_id, **kwargs)
            
        logger.info(f"Completed job {func_name} for FileID: {file_id}")
    except Exception as e:
        func_name = getattr(job_func, '_name', 'unknown_function') if hasattr(job_func, '_remote') else job_func.__name__
        logger.error(f"Error in job {func_name} for FileID: {file_id}: {str(e)}")
        logger.debug(f"Traceback: {traceback.format_exc()}")
        raise
    finally:
        log_file = f"job_logs/job_{file_id}.log"
        if os.path.exists(log_file):
            try:
                upload_result = upload_file_to_space(
                    file_src=log_file,
                    save_as=f"job_logs/job_{file_id}.log",
                    is_public=True,
                    logger=logger,
                    file_id=file_id
                )
                # Check if upload_result is awaitable
                if asyncio.iscoroutine(upload_result):
                    upload_url = await upload_result
                else:
                    upload_url = upload_result
            except Exception as upload_error:
                logger.warning(f"Async upload failed for FileID {file_id}: {upload_error}")
                upload_url = upload_file_to_space_sync(
                    file_src=log_file,
                    save_as=f"job_logs/job_{file_id}.log",
                    is_public=True,
                    logger=logger,
                    file_id=file_id
                )
            logger.info(f"Log uploaded to: {upload_url}")
        else:
            logger.warning(f"Log file {log_file} does not exist, skipping upload")
    
    return result

# Sorting-related endpoints
@router.get("/sort-by-search/{file_id}", tags=["Sorting"])
async def api_match_and_search_sort(file_id: str):
    """Run sort order update based on match_score and search-based priority."""
    return await run_job_with_logging(update_sort_order, file_id)

@router.get("/update-sort-order-per-entry/{file_id}", tags=["Sorting"])
async def api_update_sort_order_per_entry(file_id: str):
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Starting per-entry SortOrder update for FileID: {file_id}")
    
    try:
        result = await update_sort_order_per_entry(file_id, logger)
        if result is None:
            logger.error(f"Failed to update SortOrder for FileID {file_id}: update_sort_order_per_entry returned None")
            raise HTTPException(status_code=500, detail=f"Failed to update SortOrder for FileID {file_id}")
        
        # Upload logs
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
        
        return result
    
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
        raise HTTPException(status_code=500, detail=f"Error processing FileID {file_id}: {str(e)}")

@router.get("/initial-sort/{file_id}", tags=["Sorting"])
async def api_initial_sort(file_id: str):
    """Run initial sort order update."""
    return await run_job_with_logging(update_initial_sort_order, file_id, file_id=file_id)

@router.get("/set-negative-four-for-zero-match/{file_id}", tags=["Sorting"])
async def api_set_negative_four_for_zero_match(file_id: str):
    """Set SortOrder to -4 for records with match_score = 0."""
    return await run_job_with_logging(set_sort_order_negative_four_for_zero_match, file_id, file_id=file_id)

@router.post("/restart-job/{file_id}", tags=["Processing"])
async def api_process_restart(file_id: str, entry_id: int = None):
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Queueing restart of batch for FileID: {file_id}" + (f", EntryID: {entry_id}" if entry_id else ""))
    try:
        result = await run_process_restart_batch(
            file_id_db=int(file_id),
            max_retries=7,
            logger=logger,
            entry_id=entry_id
        )
        if "error" in result:
            logger.error(f"Failed to process restart batch for FileID {file_id}: {result['error']}")
            raise HTTPException(status_code=500, detail=result["error"])
        logger.info(f"Completed restart batch for FileID {file_id}. Result: {result}")
        return {"message": f"Processing restart completed for FileID: {file_id}", **result}
    except Exception as e:
        logger.error(f"Error queuing restart batch for FileID {file_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.post("/restart-search-all/{file_id}", tags=["Processing"])
async def api_restart_search_all(
    file_id: str,
    entry_id: int = None
):
    """Restart batch processing for a file, searching all variations for each entry."""
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Queueing restart of batch for FileID: {file_id}" + (f", EntryID: {entry_id}" if entry_id else "") + " with all variations")
    try:
        result = await run_process_restart_batch(
            file_id_db=int(file_id),
            max_retries=7,
            logger=logger,
            entry_id=entry_id,
            use_all_variations=True
        )
        if "error" in result:
            logger.error(f"Failed to process restart batch for FileID {file_id}: {result['error']}")
            raise HTTPException(status_code=500, detail=result["error"])
        logger.info(f"Completed restart batch for FileID {file_id}. Result: {result}")
        return {"message": f"Processing restart completed for FileID: {file_id} with all variations", **result}
    except Exception as e:
        logger.error(f"Error queuing restart batch for FileID {file_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

# Export-related endpoints
@router.post("/generate-download-file/{file_id}", tags=["Export"])
async def api_generate_download_file(background_tasks: BackgroundTasks, file_id: str):
    """Queue generation of a download file."""
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Received request to generate download file for FileID: {file_id}")
    try:
        background_tasks.add_task(run_job_with_logging, generate_download_file, file_id, file_id=int(file_id))
        return {"message": f"Processing started successfully for FileID: {file_id}"}
    except Exception as e:
        logger.error(f"Error generating download file for FileID {file_id}: {e}", exc_info=True)
        if os.path.exists(log_filename):
            upload_url = await upload_file_to_space(
                log_filename, f"job_logs/job_{file_id}.log", True, logger, file_id
            )
            await update_log_url_in_db(file_id, upload_url, logger)
        return {"error": f"An error occurred: {str(e)}"}

# Database-related endpoints
@router.get("/fetch-missing-images/{file_id}", tags=["Database"])
async def fetch_missing_images_endpoint(file_id: str, limit: int = Query(1000), ai_analysis_only: bool = Query(True)):
    """Fetch missing images."""
    logger, _ = setup_job_logger(job_id=file_id)
    logger.info(f"Fetching missing images for FileID: {file_id}, limit: {limit}, ai_analysis_only: {ai_analysis_only}")
    result = await fetch_missing_images(file_id, limit, ai_analysis_only, logger)
    if result.empty:
        logger.info("No missing images found")
        return {"success": True, "output": [], "message": "No missing images found"}
    return {"success": True, "output": result.to_dict(orient='records'), "message": "Fetched missing images successfully"}

@router.get("/get-images-excel-db/{file_id}", tags=["Database"])
async def get_images_excel_db_endpoint(file_id: str):
    """Get images from Excel DB."""
    logger, _ = setup_job_logger(job_id=file_id)
    logger.info(f"Fetching Excel images for FileID: {file_id}")
    result = await get_images_excel_db(file_id, logger)
    if result.empty:
        logger.info("No images found for Excel export")
        return {"success": True, "output": [], "message": "No images found for Excel export"}
    return {"success": True, "output": result.to_dict(orient='records'), "message": "Fetched Excel images successfully"}

@router.get("/get-send-to-email/{file_id}", tags=["Database"])
async def get_send_to_email_endpoint(file_id: str):
    """Get email address for a file."""
    logger, _ = setup_job_logger(job_id=file_id)
    logger.info(f"Retrieving email for FileID: {file_id}")
    result = await get_send_to_email(int(file_id), logger)
    return {"success": True, "output": result, "message": "Retrieved email successfully"}

@router.post("/update-file-generate-complete/{file_id}", tags=["Database"])
async def update_file_generate_complete_endpoint(file_id: str):
    """Update file generation completion status."""
    logger, _ = setup_job_logger(job_id=file_id)
    logger.info(f"Updating file generate complete for FileID: {file_id}")
    await update_file_generate_complete(file_id, logger)
    return {"success": True, "output": None, "message": "Updated file generate complete successfully"}

@router.post("/update-file-location-complete/{file_id}", tags=["Database"])
async def update_file_location_complete_endpoint(file_id: str, file_location: str):
    """Update file location completion status."""
    logger, _ = setup_job_logger(job_id=file_id)
    logger.info(f"Updating file location complete for FileID: {file_id}, file_location: {file_location}")
    await update_file_location_complete(file_id, file_location, logger)
    return {"success": True, "output": None, "message": "Updated file location successfully"}

# Include the router in the FastAPI app
app.include_router(router, prefix="/api/v3")