from fastapi import FastAPI, BackgroundTasks, Query, APIRouter
import logging
import asyncio
from typing import Dict
import os
import uuid
import ray
from fastapi.middleware.cors import CORSMiddleware

from ray_workers import process_file_with_retries
from workflow import generate_download_file, process_restart_batch
from database import (
    update_initial_sort_order,
    set_sort_order_negative_four_for_zero_match,
    update_log_url_in_db,
    update_search_sort_order,
    update_sort_order,
    call_fetch_missing_images,
    call_get_images_excel_db,
    call_get_send_to_email,
    call_update_file_generate_complete,
    call_update_file_location_complete
)
from aws_s3 import upload_file_to_space
from logging_config import setup_job_logger
from image_process import batch_process_images

# Initialize FastAPI app
app = FastAPI(title="superscaper_dev")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# Initialize Ray
ray.init(ignore_reinit_error=True)

# Default logger
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Create APIRouter instance
router = APIRouter()

async def run_job_with_logging(job_func, file_id: str, *args, **kwargs) -> Dict:
    """Run a job with logging and upload logs to S3."""
    file_id = str(file_id)
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Starting job {job_func.__name__} for FileID: {file_id}")

    try:
        result = job_func(*args, logger=logger, file_id=file_id, **kwargs) if args else job_func(file_id, logger=logger, **kwargs)
        if asyncio.iscoroutine(result):
            result = await result
        else:
            logger.info("Result is not awaitable, proceeding directly")

        if os.path.exists(log_filename):
            upload_url = upload_file_to_space(log_filename, f"job_logs/job_{file_id}.log", logger=logger, file_id=file_id)
            logger.info(f"Log file uploaded to: {upload_url}")
            await update_log_url_in_db(file_id, upload_url, logger)
        
        logger.info(f"✅ Job {job_func.__name__} completed successfully")
        return result or {"message": "Job completed"}

    except Exception as e:
        logger.error(f"🔴 Error in job {job_func.__name__} for FileID {file_id}: {e}", exc_info=True)
        if os.path.exists(log_filename):
            upload_url = upload_file_to_space(log_filename, f"job_logs/job_{file_id}.log", logger=logger, file_id=file_id)
            await update_log_url_in_db(file_id, upload_url, logger)
        raise

# Sorting-related endpoints
@router.get("/sort-by-match-and-search/{file_id}", tags=["Sorting"])
async def api_match_and_search_sort(file_id: str):
    """Run sort order update based on match_score and search-based priority for a given file ID."""
    logger, _ = setup_job_logger(job_id=file_id)
    logger.info(f"Running match and search sort for FileID: {file_id}")
    return await run_job_with_logging(update_sort_order, file_id)

@router.get("/initial_sort/{file_id}", tags=["Sorting"])
async def api_initial_sort(file_id: str):
    """Run initial sort order update for a given file ID."""
    logger, _ = setup_job_logger(job_id=file_id)
    logger.info(f"Running initial sort for FileID: {file_id}")
    return await run_job_with_logging(update_initial_sort_order, file_id)


@router.get("/set-negative-four-for-zero-match/{file_id}", tags=["Sorting"])
async def api_set_negative_four_for_zero_match(file_id: str):
    """Set SortOrder to -4 for records with match_score = 0."""
    logger, _ = setup_job_logger(job_id=file_id)
    logger.info(f"Running set negative four for zero match for FileID: {file_id}")
    return await run_job_with_logging(set_sort_order_negative_four_for_zero_match, file_id)

# Processing-related endpoints
@router.post("/restart-job/", tags=["Processing"])
async def api_process_restart(background_tasks: BackgroundTasks, file_id: str):
    """Queue a restart of a failed batch processing task."""
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Queueing restart of failed batch for FileID: {file_id}")
    try:
        background_tasks.add_task(run_job_with_logging, process_restart_batch, file_id)
        return {"message": f"Processing restart initiated for FileID: {file_id}"}
    except Exception as e:
        logger.error(f"Error queuing restart batch for FileID {file_id}: {e}", exc_info=True)
        if os.path.exists(log_filename):
            upload_url = upload_file_to_space(log_filename, f"job_logs/job_{file_id}.log", logger=logger, file_id=file_id)
            await update_log_url_in_db(file_id, upload_url, logger)
        return {"error": f"An error occurred: {str(e)}"}

@router.post("/generate-download-file/", tags=["Export"])
async def api_generate_download_file(background_tasks: BackgroundTasks, file_id: int):
    """Queue generation of a download file for a given file ID."""
    file_id_str = str(file_id)
    logger, _ = setup_job_logger(job_id=file_id_str)
    logger.info(f"Received request to generate download file for FileID: {file_id}")
    try:
        background_tasks.add_task(run_job_with_logging, generate_download_file, file_id_str)
        return {"message": "Processing started successfully"}
    except Exception as e:
        logger.error(f"Error generating download file: {e}", exc_info=True)
        return {"error": f"An error occurred: {str(e)}"}

@router.post("/process-ai-analysis/", tags=["Processing"])
async def api_process_ai_analysis(background_tasks: BackgroundTasks, file_id: str):
    """Queue AI analysis for images associated with a given file ID."""
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Queueing AI analysis for FileID: {file_id}")
    try:
        background_tasks.add_task(run_job_with_logging, batch_process_images, file_id)
        return {"message": f"AI analysis initiated for FileID: {file_id}"}
    except Exception as e:
        logger.error(f"Error queuing AI analysis for FileID {file_id}: {e}", exc_info=True)
        if os.path.exists(log_filename):
            upload_url = upload_file_to_space(log_filename, f"job_logs/job_{file_id}.log", logger=logger, file_id=file_id)
            await update_log_url_in_db(file_id, upload_url, logger)
        return {"error": f"An error occurred: {str(e)}"}

@router.post("/retry-process-batch/{file_id}", tags=["Processing"])
async def api_process_file(
    background_tasks: BackgroundTasks,
    file_id: str,
    max_retries: int = Query(2, description="Maximum number of retry attempts")
):
    """Queue the processing of entries for a given file ID with retry logic."""
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Queueing process_file_with_retries for FileID: {file_id} with max_retries: {max_retries}")
    try:
        background_tasks.add_task(run_job_with_logging, process_file_with_retries, file_id, max_retries=max_retries)
        return {"message": f"Processing initiated for FileID: {file_id}"}
    except Exception as e:
        logger.error(f"Error queuing process_file_with_retries for FileID {file_id}: {e}", exc_info=True)
        if os.path.exists(log_filename):
            upload_url = upload_file_to_space(log_filename, f"job_logs/job_{file_id}.log", logger=logger, file_id=file_id)
            await update_log_url_in_db(file_id, upload_url, logger)
        return {"error": f"An error occurred: {str(e)}"}

# Database-related endpoints
@router.get("/call-fetch-missing-images/{file_id}", tags=["Database"])
async def call_fetch_missing_images_endpoint(file_id: str):
    """Fetch missing images for a given file ID."""
    logger, _ = setup_job_logger(job_id=file_id)
    result = call_fetch_missing_images(file_id, limit=10, ai_analysis_only=False, logger=logger)
    return result

@router.get("/call-get-images-excel-db/{file_id}", tags=["Database"])
async def call_get_images_excel_db_endpoint(file_id: str):
    """Get images from Excel DB for a given file ID."""
    logger, _ = setup_job_logger(job_id=file_id)
    result = call_get_images_excel_db(file_id, logger=logger)
    return result

@router.get("/call-get-send-to-email/{file_id}", tags=["Database"])
async def call_get_send_to_email_endpoint(file_id: str):
    """Get email-related data for a given file ID."""
    logger, _ = setup_job_logger(job_id=file_id)
    result = call_get_send_to_email(int(file_id), logger=logger)
    return result

@router.get("/call-update-file-generate-complete/{file_id}", tags=["Database"])
async def call_update_file_generate_complete_endpoint(file_id: str):
    """Update file generation completion status."""
    logger, _ = setup_job_logger(job_id=file_id)
    result = call_update_file_generate_complete(file_id, logger=logger)
    return result

@router.get("/call-update-file-location-complete/{file_id}", tags=["Database"])
async def call_update_file_location_complete_endpoint(file_id: str, file_location: str = "call_location_url"):
    """Update file location completion status."""
    logger, _ = setup_job_logger(job_id=file_id)
    result = await call_update_file_location_complete(file_id, file_location, logger=logger)
    return result

# Include the router in the FastAPI app
app.include_router(router, prefix="/api/v2")