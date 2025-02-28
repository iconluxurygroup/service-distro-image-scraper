from fastapi import FastAPI, BackgroundTasks
import logging
from workflow import process_image_batch, generate_download_file, process_restart_batch
from database import update_ai_sort_order, check_json_status, fix_json_data

logging.basicConfig(level=logging.INFO)

app = FastAPI()

@app.get("/check_json_status/{file_id}")
async def api_check_json_status(file_id: str):
    return check_json_status(file_id)

@app.post("/update_sort_llama/")
async def api_update_sort(background_tasks: BackgroundTasks, file_id_db: str):
    background_tasks.add_task(update_ai_sort_order, file_id_db)
    return {"message": f"Sort order update for FileID: {file_id_db} has been initiated in the background", "status": "processing"}

@app.post("/fix_json_data/")
async def api_fix_json_data(background_tasks: BackgroundTasks, file_id: str = None, limit: int = 1000):
    return fix_json_data(background_tasks, file_id, limit)

@app.post("/restart-failed-batch/")
async def api_process_restart(background_tasks: BackgroundTasks, file_id_db: str):
    background_tasks.add_task(process_restart_batch, file_id_db)
    return {"message": f"Processing restart initiated for FileID: {file_id_db}. You will be notified upon completion."}

@app.post("/process-image-batch/")
async def api_process_payload(background_tasks: BackgroundTasks, payload: dict):
    try:
        logging.info("Received request to process image batch")
        background_tasks.add_task(process_image_batch, payload)
        return {"message": "Processing started successfully. You will be notified upon completion."}
    except Exception as e:
        logging.error(f"Error processing payload: {e}")
        return {"error": f"An error occurred: {str(e)}"}

@app.post("/generate-download-file/")
async def api_generate_download_file(background_tasks: BackgroundTasks, file_id: int):
    try:
        logging.info(f"Received request to generate download file for FileID: {file_id}")
        background_tasks.add_task(generate_download_file, str(file_id))
        return {"message": "Processing started successfully. You will be notified upon completion."}
    except Exception as e:
        logging.error(f"Error generating download file: {e}")
        return {"error": f"An error occurred: {str(e)}"}