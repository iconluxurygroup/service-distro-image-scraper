from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, APIRouter
from pydantic import BaseModel, Field
import logging
import asyncio
import os
import json
import traceback
import psutil
import pyodbc
import datetime
import hashlib
import time
from typing import Optional, List, Dict, Any, Callable
from logging_config import setup_job_logger
from workflow import upload_file_to_space  # Import from image_scraper instead of aws_s3
from search_utils import update_sort_order, update_sort_no_image_entry
from email_utils import send_message_email
from vision_utils import fetch_missing_images
from workflow import generate_download_file, process_restart_batch  # Import from image_scraper
from db_utils import (
    update_log_url_in_db,
    get_send_to_email,
    fetch_last_valid_entry,
    update_initial_sort_order,
    get_images_excel_db,
    update_file_generate_complete,
    update_file_location_complete,
)
from database_config import conn_str, async_engine
from config import VERSION
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from ai_utils import batch_vision_reason

app = FastAPI(title="super_scraper", version=VERSION)

default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

router = APIRouter()

JOB_STATUS = {}
LAST_UPLOAD = {}

class JobStatusResponse(BaseModel):
    status: str = Field(..., description="Job status (e.g., queued, running, completed, failed)")
    message: str = Field(..., description="Descriptive message about the job status")
    public_url: Optional[str] = Field(None, description="S3 URL of the generated Excel file, if available")
    log_url: Optional[str] = Field(None, description="S3 URL of the job log file, if available")
    timestamp: str = Field(..., description="ISO timestamp of the response")

async def upload_log_file(file_id: str, log_filename: str, logger: logging.Logger) -> Optional[str]:
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(Exception),
        before_sleep=lambda retry_state: logger.info(
            f"Retrying log upload for FileID {file_id} (attempt {retry_state.attempt_number}/3)"
        )
    )
    async def try_upload():
        if not os.path.exists(log_filename):
            logger.warning(f"Log file {log_filename} does not exist, skipping upload")
            return None

        file_hash = hashlib.md5(open(log_filename, "rb").read()).hexdigest()
        current_time = time.time()
        key = (log_filename, file_id)

        if key in LAST_UPLOAD and LAST_UPLOAD[key]["hash"] == file_hash and current_time - LAST_UPLOAD[key]["time"] < 60:
            logger.info(f"Skipping redundant upload for {log_filename}")
            return LAST_UPLOAD[key]["url"]

        try:
            upload_url = await upload_file_to_space(
                file_src=log_filename,
                save_as=f"job_logs/job_{file_id}.log",
                is_public=True,
                logger=logger,
                file_id=file_id
            )
            await update_log_url_in_db(file_id, upload_url, logger)
            LAST_UPLOAD[key] = {"hash": file_hash, "time": current_time, "url": upload_url}
            logger.info(f"Log uploaded to: {upload_url}")
            return upload_url
        except Exception as e:
            logger.error(f"Failed to upload log for FileID {file_id}: {e}", exc_info=True)
            raise

    try:
        return await try_upload()
    except Exception as e:
        logger.error(f"Failed to upload log for FileID {file_id} after retries: {e}", exc_info=True)
        return None

async def run_job_with_logging(job_func: Callable[..., Any], file_id: str, **kwargs) -> Dict:
    file_id_str = str(file_id)
    logger, log_file = setup_job_logger(job_id=file_id_str, console_output=True)
    result = None
    debug_info = {"memory_usage": {}, "log_file": log_file, "endpoint_errors": []}
    
    try:
        func_name = getattr(job_func, '_name', 'unknown_function') if hasattr(job_func, '_remote') else job_func.__name__
        logger.info(f"Starting job {func_name} for FileID: {file_id}")
        
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
        
        logger.info(f"Completed job {func_name} for FileID: {file_id}")
        return {
            "status_code": 200,
            "message": f"Job {func_name} completed successfully for FileID: {file_id}",
            "data": result,
            "debug_info": debug_info
        }
    except Exception as e:
        func_name = getattr(job_func, '_name', 'unknown_function') if hasattr(job_func, '_remote') else job_func.__name__
        logger.error(f"Error in job {func_name} for FileID: {file_id}: {e}")
        logger.debug(f"Traceback: {traceback.format_exc()}")
        debug_info["error_traceback"] = traceback.format_exc()
        if "placeholder://error" in str(e):
            debug_info["endpoint_errors"].append({"error": str(e), "timestamp": datetime.datetime.now().isoformat()})
            logger.warning(f"Detected placeholder error in job {func_name} for FileID: {file_id}")
        return {
            "status_code": 500,
            "message": f"Error in job {func_name} for FileID {file_id}: {str(e)}",
            "data": None,
            "debug_info": debug_info
        }
    finally:
        debug_info["log_url"] = await upload_log_file(file_id_str, log_file, logger)

async def run_generate_download_file(file_id: str, logger: logging.Logger, log_filename: str, background_tasks: BackgroundTasks):
    try:
        JOB_STATUS[file_id] = {
            "status": "running",
            "message": "Job is running",
            "timestamp": datetime.datetime.now().isoformat()
        }
        
        result = await generate_download_file(int(file_id), background_tasks, logger=logger)
        
        if "error" in result:
            JOB_STATUS[file_id] = {
                "status": "failed",
                "message": f"Error: {result['error']}",
                "log_url": result.get("log_filename") if os.path.exists(result.get("log_filename", "")) else None,
                "timestamp": datetime.datetime.now().isoformat()
            }
            logger.error(f"Job failed for FileID {file_id}: {result['error']}")
        else:
            JOB_STATUS[file_id] = {
                "status": "completed",
                "message": "Job completed successfully",
                "public_url": result.get("public_url"),
                "log_url": result.get("log_filename") if os.path.exists(result.get("log_filename", "")) else None,
                "timestamp": datetime.datetime.now().isoformat()
            }
            logger.info(f"Job completed for FileID {file_id}")
    except Exception as e:
        logger.error(f"Unexpected error in job for FileID {file_id}: {e}", exc_info=True)
        JOB_STATUS[file_id] = {
            "status": "failed",
            "message": f"Unexpected error: {str(e)}",
            "log_url": log_filename if os.path.exists(log_filename) else None,
            "timestamp": datetime.datetime.now().isoformat()
        }

async def monitor_and_resubmit_failed_jobs(file_id: str, logger: logging.Logger):
    log_file = f"job_logs/job_{file_id}.log"
    max_attempts = 3
    attempt = 1

    while attempt <= max_attempts:
        if os.path.exists(log_file):
            with open(log_file, 'r') as f:
                log_content = f.read()
                if "WORKER TIMEOUT" in log_content or "SIGKILL" in log_content or "placeholder://error" in log_content:
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
                                to_emails=["nik@luxurymarket.com"],
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
                                to_emails=["nik@luxurymarket.com"],
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

@router.post("/generate-download-file/{file_id}", tags=["Export"], response_model=JobStatusResponse)
async def api_generate_download_file(file_id: str, background_tasks: BackgroundTasks):
    logger, log_filename = setup_job_logger(job_id=file_id, console_output=True)
    logger.info(f"Received request to generate download file for FileID: {file_id}")
    
    try:
        async with async_engine.connect() as conn:
            result = await conn.execute(
                text("SELECT FileName FROM utb_ImageScraperFiles WHERE ID = :file_id"),
                {"file_id": int(file_id)}
            )
            if not result.fetchone():
                logger.error(f"Invalid FileID: {file_id}")
                raise HTTPException(status_code=404, detail=f"FileID {file_id} not found")
        
        JOB_STATUS[file_id] = {
            "status": "queued",
            "message": "Job queued for processing",
            "timestamp": datetime.datetime.now().isoformat()
        }
        
        background_tasks.add_task(run_generate_download_file, file_id, logger, log_filename, background_tasks)
        
        send_to_email = await get_send_to_email(int(file_id), logger=logger)
        if send_to_email:
            await send_message_email(
                to_emails=send_to_email,
                subject=f"Job Queued for FileID: {file_id}",
                message=f"Excel file generation for FileID {file_id} has been queued.",
                logger=logger
            )
        
        return JobStatusResponse(
            status="queued",
            message=f"Download file generation queued for FileID: {file_id}",
            timestamp=datetime.datetime.now().isoformat()
        )
    except SQLAlchemyError as e:
        logger.error(f"Database error for FileID {file_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Error queuing download file for FileID {file_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error queuing job: {str(e)}")

@router.get("/job-status/{file_id}", tags=["Export"], response_model=JobStatusResponse)
async def api_get_job_status(file_id: str):
    logger, _ = setup_job_logger(job_id=file_id, console_output=True)
    logger.info(f"Checking job status for FileID: {file_id}")
    
    job_status = JOB_STATUS.get(file_id)
    if not job_status:
        logger.warning(f"No job found for FileID: {file_id}")
        raise HTTPException(status_code=404, detail=f"No job found for FileID {file_id}")
    
    return JobStatusResponse(
        status=job_status["status"],
        message=job_status["message"],
        public_url=job_status.get("public_url"),
        log_url=job_status.get("log_url"),
        timestamp=job_status["timestamp"]
    )

@router.get("/sort-by-search/{file_id}", tags=["Sorting"])
async def api_match_and_search_sort(file_id: str):
    result = await run_job_with_logging(update_sort_order, file_id)
    if result["status_code"] != 200:
        raise HTTPException(status_code=result["status_code"], detail=result["message"])
    return result

@router.get("/initial-sort/{file_id}", tags=["Sorting"])
async def api_initial_sort(file_id: str):
    result = await run_job_with_logging(update_initial_sort_order, file_id)
    if result["status_code"] != 200:
        raise HTTPException(status_code=result["status_code"], detail=result["message"])
    return result

@router.get("/no-image-sort/{file_id}", tags=["Sorting"])
async def api_no_image_sort(file_id: str):
    result = await run_job_with_logging(update_sort_no_image_entry, file_id)
    if result["status_code"] != 200:
        raise HTTPException(status_code=result["status_code"], detail=result["message"])
    return result

@router.post("/restart-job/{file_id}", tags=["Processing"])
async def api_process_restart(file_id: str, entry_id: Optional[int] = None, background_tasks: BackgroundTasks = None):
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Queueing restart of batch for FileID: {file_id}" + (f", EntryID: {entry_id}" if entry_id else ""))
    try:
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

@router.post("/restart-search-all/{file_id}", tags=["Processing"])
async def api_restart_search_all(
    file_id: str,
    entry_id: Optional[int] = None,
    background_tasks: BackgroundTasks = None
):
    logger, log_filename = setup_job_logger(job_id=file_id)
    logger.info(f"Queueing restart of batch for FileID: {file_id}" + (f", EntryID: {entry_id}" if entry_id else "") + " with all variations")
    
    try:
        if not entry_id:
            entry_id = await fetch_last_valid_entry(file_id, logger)
            logger.info(f"Retrieved last EntryID: {entry_id} for FileID: {file_id}")
        
        result = await run_job_with_logging(
            process_restart_batch,
            file_id,
            entry_id=entry_id,
            use_all_variations=True
        )
        
        if result["status_code"] != 200:
            logger.error(f"Failed to process restart batch for FileID {file_id}: {result['message']}")
            if "placeholder://error" in result["message"]:
                logger.warning(f"Placeholder error detected; check endpoint logs for FileID {file_id}")
                debug_info = result.get("debug_info", {})
                endpoint_errors = debug_info.get("endpoint_errors", [])
                for error in endpoint_errors:
                    logger.error(f"Endpoint error: {error['error']} at {error['timestamp']}")
            raise HTTPException(status_code=result["status_code"], detail=result["message"])
        
        if background_tasks:
            background_tasks.add_task(monitor_and_resubmit_failed_jobs, file_id, logger)
        
        logger.info(f"Completed restart batch for FileID: {file_id}")
        return {
            "status": "success",
            "status_code": 200,
            "message": f"Processing restart with all variations completed for FileID: {file_id}",
            "data": result["data"]
        }
    except Exception as e:
        logger.error(f"Error queuing restart batch for FileID {file_id}: {e}", exc_info=True)
        if os.path.exists(log_filename):
            upload_url = await upload_file_to_space(
                log_filename, f"job_logs/job_{file_id}.log", True, logger, file_id
            )
            await update_log_url_in_db(file_id, upload_url, logger)
        raise HTTPException(status_code=500, detail=f"Error restarting batch with all variations for FileID {file_id}: {str(e)}")

@router.post("/process-images-ai/{file_id}", tags=["Processing"])
async def api_process_ai_images(
    file_id: str,
    entry_ids: Optional[List[int]] = Query(None, description="List of EntryIDs to process"),
    step: int = Query(0, description="Retry step for logging"),
    limit: int = Query(5000, description="Maximum number of images to process"),
    concurrency: int = Query(10, description="Maximum concurrent threads"),
    background_tasks: BackgroundTasks = None
):
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
        
        if background_tasks:
            background_tasks.add_task(monitor_and_resubmit_failed_jobs, file_id, logger)
        
        logger.info(f"Completed AI image processing for FileID: {file_id}")
        return {
            "status": "success",
            "status_code": 200,
            "message": f"AI image processing completed for FileID: {file_id}",
            "data": result["data"]
        }
    except Exception as e:
        logger.error(f"Error queuing AI image processing for FileID {file_id}: {e}", exc_info=True)
        if os.path.exists(log_filename):
            upload_url = await upload_file_to_space(
                log_filename, f"job_logs/job_{file_id}.log", True, logger, file_id
            )
            await update_log_url_in_db(file_id, upload_url, logger)
        raise HTTPException(status_code=500, detail=f"Error processing AI images for FileID {file_id}: {str(e)}")

@router.get("/get-images-excel-db/{file_id}", tags=["Database"])
async def get_images_excel_db_endpoint(file_id: str):
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
    logger, _ = setup_job_logger(job_id=file_id)
    logger.info(f"Updating file location complete for FileID: {file_id}, file_location: {file_location}")
    try:
        await update_file_location_complete(file_id, file_location, logger)
        return {"status_code": 200, "message": f"Updated file location successfully for FileID: {file_id}", "data": None}
    except Exception as e:
        logger.error(f"Error updating file location for FileID {file_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error updating file location for FileID {file_id}: {str(e)}")

app.include_router(router, prefix="/api/v3")