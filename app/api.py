from fastapi import FastAPI, BackgroundTasks
import logging
import asyncio
import os
import uuid
import ray
from typing import Dict
from workflow import generate_download_file, process_restart_batch
from database import update_initial_sort_order, update_log_url_in_db, update_search_sort_order, update_sort_order_based_on_match_score, call_fetch_missing_images,call_get_images_excel_db,call_get_send_to_email,call_update_file_generate_complete,call_update_file_location_complete
from aws_s3 import upload_file_to_space
from logging_config import setup_job_logger
from image_process import batch_process_images

# Initialize FastAPI app
app = FastAPI(title="superscaper_dev")

# Initialize Ray once at startup
ray.init(ignore_reinit_error=True)

# Default logger for non-job contexts
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

async def run_job_with_logging(job_func, file_id: str, *args, **kwargs) -> Dict:
    """Run a job with logging and upload logs to S3, handling both sync and async functions."""
    file_id = str(file_id)
    logger, log_filename = setup_job_logger(job_id=file_id or str(uuid.uuid4()))
    logger.info(f"Starting job {job_func.__name__} for FileID: {file_id}")

    try:
        # Execute the job function
        if args:
            result = job_func(args[0], logger=logger, file_id=file_id, **kwargs)
        else:
            result = job_func(file_id, logger=logger, **kwargs)

        # Handle async results
        if asyncio.iscoroutine(result):
            result = await result
        else:
            logger.info("Result is not awaitable, proceeding directly")

        # Upload logs to S3 if log file exists
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
            logger.info(f"Log file uploaded to: {upload_url}")
            await update_log_url_in_db(file_id, upload_url, logger)
        raise

@app.get("/match_ai_sort/{file_id}")
async def api_match_ai_sort(file_id: str):
    """Run AI-based sort order update for a given file ID."""
    logger, _ = setup_job_logger(job_id=file_id)
    logger.info(f"Running match AI sort for FileID: {file_id}")
    return await run_job_with_logging(update_sort_order_based_on_match_score, file_id)

@app.get("/initial_sort/{file_id}")
async def api_initial_sort(file_id: str):
    """Run initial sort order update for a given file ID."""
    logger, _ = setup_job_logger(job_id=file_id)
    logger.info(f"Running initial sort for FileID: {file_id}")
    return await run_job_with_logging(update_initial_sort_order, file_id)

@app.get("/search_sort/{file_id}")
async def api_search_sort(file_id: str):
    """Run search-based sort order update for a given file ID."""
    logger, _ = setup_job_logger(job_id=file_id)
    logger.info(f"Running search sort for FileID: {file_id}")
    return await run_job_with_logging(update_search_sort_order, file_id)

@app.post("/restart-failed-batch/")
async def api_process_restart(background_tasks: BackgroundTasks, file_id_db: str):
    """Queue a restart of a failed batch processing task."""
    logger, log_filename = setup_job_logger(job_id=file_id_db)
    logger.info(f"Queueing restart of failed batch for FileID: {file_id_db}")
    try:
        background_tasks.add_task(run_job_with_logging, process_restart_batch, file_id_db)
        return {"message": f"Processing restart initiated for FileID: {file_id_db}"}
    except Exception as e:
        logger.error(f"Error queuing restart batch for FileID {file_id_db}: {e}", exc_info=True)
        if os.path.exists(log_filename):
            upload_url = upload_file_to_space(log_filename, f"job_logs/job_{file_id_db}.log", logger=logger, file_id=file_id_db)
            await update_log_url_in_db(file_id_db, upload_url, logger)
        return {"error": f"An error occurred: {str(e)}"}

@app.post("/generate-download-file/")
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

@app.post("/process-ai-analysis/")
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
# In api.py
@app.get("/call-fetch-missing-images/{file_id}")
async def call_fetch_missing_images_endpoint(file_id: str):
    logger, _ = setup_job_logger(job_id=file_id)
    result = call_fetch_missing_images(file_id, limit=10, ai_analysis_only=False, logger=logger)
    return result

@app.get("/call-get-images-excel-db/{file_id}")
async def call_get_images_excel_db_endpoint(file_id: str):
    logger, _ = setup_job_logger(job_id=file_id)
    result = call_get_images_excel_db(file_id, logger=logger)
    return result

@app.get("/call-get-send-to-email/{file_id}")
async def call_get_send_to_email_endpoint(file_id: str):
    logger, _ = setup_job_logger(job_id=file_id)
    result = call_get_send_to_email(int(file_id), logger=logger)
    return result

@app.get("/call-update-file-generate-complete/{file_id}")
async def call_update_file_generate_complete_endpoint(file_id: str):
    logger, _ = setup_job_logger(job_id=file_id)
    result = call_update_file_generate_complete(file_id, logger=logger)
    return result

@app.get("/call-update-file-location-complete/{file_id}")
async def call_update_file_location_complete_endpoint(file_id: str, file_location: str = "call_location_url"):
    logger, _ = setup_job_logger(job_id=file_id)
    result = await call_update_file_location_complete(file_id, file_location, logger=logger)
    return result