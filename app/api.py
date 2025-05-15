import logging
import asyncio
import os
import ray
from typing import Dict, Any, Callable, Union, List
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
from aws_s3 import upload_file_to_space, upload_file_to_space_sync
from email_utils import send_message_email

from logging_config import setup_job_logger
import traceback
from typing import Optional
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
    logger, _ = setup_job_logger(job_id=file_id_str, console_output=True)
    result = None
    try:
        func_name = getattr(job_func, '_name', 'unknown_function') if hasattr(job_func, '_remote') else job_func.__name__
        logger.info(f"Starting job {func_name} for FileID: {file_id}")
        
        if asyncio.iscoroutinefunction(job_func) or hasattr(job_func, '_remote'):
            result = await job_func(file_id, **kwargs)
        else:
            result = job_func(file_id, **kwargs)
            
        logger.info(f"Completed job {func_name} for FileID: {file_id}")
        return {"status_code": 200, "message": f"Job {func_name} completed successfully for FileID: {file_id}", "data": result}
    except Exception as e:
        func_name = getattr(job_func, '_name', 'unknown_function') if hasattr(job_func, '_remote') else job_func.__name__
        logger.error(f"Error in job {func_name} for FileID: {file_id}: {str(e)}")
        logger.debug(f"Traceback: {traceback.format_exc()}")
        return {"status_code": 500, "message": f"Error in job {func_name} for FileID {file_id}: {str(e)}"}
    finally:
        log_file = f"job_logs/job_{file_id_str}.log"
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
async def api_process_restart(file_id: str, entry_id: int = None):
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Queueing restart of batch for FileID: {file_id}" + (f", EntryID: {entry_id}" if entry_id else ""))
    try:
        result = await process_restart_batch(
    file_id_db=int(file_id),
    logger=logger,
    entry_id=entry_id
)
        if "error" in result:
            logger.error(f"Failed to process restart batch for FileID {file_id}: {result['error']}")
            raise HTTPException(status_code=500, detail=result["error"])
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

@router.post("/restart-search-all/{file_id}", tags=["Processing"])
async def api_restart_search_all(file_id: str, entry_id: int = None):
    """Restart batch processing for a file, searching all variations for each entry."""
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Queueing restart of batch for FileID: {file_id}" + (f", EntryID: {entry_id}" if entry_id else "") + " with all variations")
    try:
        result = await run_job_with_logging(
            process_restart_batch,
            file_id,
            entry_id=entry_id,
            use_all_variations=True
        )
        if result["status_code"] != 200:
            logger.error(f"Failed to process restart batch for FileID {file_id}: {result['message']}")
            raise HTTPException(status_code=result["status_code"], detail=result["message"])
        logger.info(f"Completed restart batch for FileID: {file_id}. Result: {result}")
        return {"status_code": 200, "message": f"Processing restart with all variations completed for FileID: {file_id}", "data": result["data"]}
    except Exception as e:
        logger.error(f"Error queuing restart batch for FileID: {file_id}: {e}", exc_info=True)
        if os.path.exists(log_filename):
            upload_url = await upload_file_to_space(
                log_filename, f"job_logs/job_{file_id}.log", True, logger, file_id
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
@router.post("/process-images-ai/{file_id}", tags=["Processing"])
async def api_process_ai_images(
    file_id: str,
    entry_ids: Optional[List[int]] = Query(None, description="List of EntryIDs to process"),
    step: int = Query(0, description="Retry step for logging"),
    limit: int = Query(5000, description="Maximum number of images to process"),
    concurrency: int = Query(10, description="Maximum concurrent Ray tasks")
):
    """Trigger AI image processing for a file, analyzing images with YOLOv11 and Gemini."""
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Queueing AI image processing for FileID: {file_id}, EntryIDs: {entry_ids}, Step: {step}")
    try:
        result = await run_job_with_logging(
            batch_vision_reason,
            file_id,
            entry_ids=entry_ids,
            step=step,
            limit=limit,
            concurrency=concurrency,
            logger=logger
        )
        if result["status_code"] != 200:
            logger.error(f"Failed to process AI images for FileID {file_id}: {result['message']}")
            raise HTTPException(status_code=result["status_code"], detail=result["message"])
        logger.info(f"Completed AI image processing for FileID {file_id}")
        return {"status_code": 200, "message": f"AI image processing completed for FileID: {file_id}", "data": result["data"]}
    except Exception as e:
        logger.error(f"Error queuing AI image processing for FileID {file_id}: {e}", exc_info=True)
        if os.path.exists(log_filename):
            upload_url = await upload_file_to_space(
                log_filename, f"job_logs/job_{file_id}.log", True, logger, file_id
            )
            await update_log_url_in_db(file_id, upload_url, logger)
        raise HTTPException(status_code=500, detail=f"Error processing AI images for FileID {file_id}: {str(e)}")

# Export-related endpoints
@router.post("/generate-download-file/{file_id}", tags=["Export"])
async def api_generate_download_file(background_tasks: BackgroundTasks, file_id: str):
    """Queue generation of a download file."""
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Received request to generate download file for FileID: {file_id}")
    try:
        background_tasks.add_task(run_job_with_logging, generate_download_file, file_id)
        return {"status_code": 202, "message": f"Download file generation queued for FileID: {file_id}", "data": None}
    except Exception as e:
        logger.error(f"Error queuing download file for FileID {file_id}: {e}", exc_info=True)
        if os.path.exists(log_filename):
            upload_url = await upload_file_to_space(
                log_filename, f"job_logs/job_{file_id}.log", True, logger, file_id
            )
            await update_log_url_in_db(file_id, upload_url, logger)
        raise HTTPException(status_code=500, detail=f"Error queuing download file for FileID {file_id}: {str(e)}")

# Database-related endpoints
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