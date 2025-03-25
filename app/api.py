# api.py
from fastapi import FastAPI, BackgroundTasks
import logging
from workflow import generate_download_file, process_restart_batch
from database import update_initial_sort_order,update_log_url_in_db,update_search_sort_order,update_sort_order_based_on_match_score
from aws_s3 import upload_file_to_space
from logging_config import setup_job_logger
import os
import uuid
import ray
from image_process import batch_process_images
app = FastAPI(title='superscaper_dev')

# Initialize Ray once at startup
ray.init(ignore_reinit_error=True)

# Define default logger for non-job contexts
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
import asyncio

async def run_job_with_logging(job_func, file_id, *args, **kwargs):
    file_id = str(file_id)
    logger, log_filename = setup_job_logger(job_id=file_id or str(uuid.uuid4()))
    logger.info(f"Starting job {job_func.__name__} for FileID: {file_id}")
    logger.info(f"job_func type: {type(job_func)}")
    
    try:
        if args:
            result = job_func(args[0], logger=logger, file_id=file_id, **kwargs)
        else:
            result = job_func(file_id, logger=logger, **kwargs)
        
        logger.info(f"Result type: {type(result)}, Result: {result}")
        if asyncio.iscoroutine(result):
            logger.info("Result is awaitable, awaiting it")
            result = await result
        else:
            logger.info("Result is not awaitable, skipping await")
        
        if os.path.exists(log_filename):
            upload_url = upload_file_to_space(log_filename, f"job_logs/job_{file_id}.log", logger=logger, file_id=file_id)
            logger.info(f"Log file uploaded to: {upload_url}")
        logger.info(f"✅✅Job {job_func.__name__} completed")
        return result
    except Exception as e:
        logger.error(f"🔴 Error in job {job_func.__name__} for FileID {file_id}: {e}", exc_info=True)
        if os.path.exists(log_filename):
            upload_url = upload_file_to_space(log_filename, f"job_logs/job_{file_id}.log", logger=logger, file_id=file_id)
            logger.info(f"Log file uploaded to: {upload_url}")
        raise
@app.get("/match_ai_sort/{file_id}")
async def api_initial_sort(file_id: str):
    logger, _ = setup_job_logger(job_id=file_id)
    logger.info(f"Running initial sort for FileID: {file_id}")
    return update_sort_order_based_on_match_score(file_id, logger=logger)

@app.get("/initial_sort/{file_id}")
async def api_initial_sort(file_id: str):
    logger, _ = setup_job_logger(job_id=file_id)
    logger.info(f"Running initial sort for FileID: {file_id}")
    return update_initial_sort_order(file_id, logger=logger)
@app.get("/search_sort/{file_id}")
async def api_initial_sort(file_id: str):
    logger, _ = setup_job_logger(job_id=file_id)
    logger.info(f"Running search sort for FileID: {file_id}")
    return update_search_sort_order(file_id, logger=logger)

@app.post("/restart-failed-batch/")
async def api_process_restart(background_tasks: BackgroundTasks, file_id_db: str):
    logger, log_filename = setup_job_logger(job_id=file_id_db)
    logger.info(f"Queueing restart of failed batch for FileID: {file_id_db}")
    try:
        background_tasks.add_task(run_job_with_logging, process_restart_batch, file_id_db)
        return {"message": f"Processing restart initiated for FileID: {file_id_db}"}
    except Exception as e:
        logger.error(f"Error queuing restart batch for FileID {file_id_db}: {e}", exc_info=True)
        if os.path.exists(log_filename):
            upload_url = upload_file_to_space(log_filename, f"job_logs/job_{file_id_db}.log", logger=logger, file_id=file_id_db)
            logger.info(f"Log file uploaded to: {upload_url}")
            await asyncio.get_running_loop().run_in_executor(
                None, update_log_url_in_db, file_id_db, log_filename, logger
            )
        return {"error": f"An error occurred: {str(e)}"}

@app.post("/generate-download-file/")
async def api_generate_download_file(background_tasks: BackgroundTasks, file_id: int):
    file_id_str = str(file_id)
    logger, _ = setup_job_logger(job_id=file_id_str)
    logger.info(f"Received request to generate download file for FileID: {file_id}")
    try:
        background_tasks.add_task(run_job_with_logging, generate_download_file, file_id_str)
        return {"message": "Processing started successfully"}
    except Exception as e:
        logger.error(f"Error generating download file: {e}")
        return {"error": f"An error occurred: {str(e)}"}
    
# New endpoint to trigger the image processing pipeline
@app.post("/process-ai-analysis/")
async def api_process_ai_analysis(background_tasks: BackgroundTasks, file_id: str):
    """
    Endpoint to initiate AI analysis for images associated with a given file_id.
    
    Args:
        background_tasks (BackgroundTasks): FastAPI background tasks for async execution.
        file_id (str): The identifier for the batch of images to process.
    
    Returns:
        dict: A message indicating success or an error if something goes wrong.
    """
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Queueing AI analysis for FileID: {file_id}")
    try:
        background_tasks.add_task(run_job_with_logging, batch_process_images, file_id)
        return {"message": f"AI analysis initiated for FileID: {file_id}"}
    except Exception as e:
        logger.error(f"Error queueing AI analysis for FileID {file_id}: {e}", exc_info=True)
        if os.path.exists(log_filename):
            upload_url = upload_file_to_space(log_filename, f"job_logs/job_{file_id}.log", logger=logger, file_id=file_id)
            logger.info(f"Log file uploaded to: {upload_url}")
            await update_log_url_in_db(file_id, upload_url, logger)
        return {"error": f"An error occurred: {str(e)}"}