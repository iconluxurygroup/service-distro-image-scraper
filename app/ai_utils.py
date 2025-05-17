import logging
import pandas as pd
import asyncio
import json
from typing import Optional, List, Tuple
from vision_utils import fetch_missing_images
from image_reason import process_entry
from search_utils import update_search_sort_order
from db_utils import update_log_url_in_db, export_dai_json
from database_config import conn_str, async_engine
import pyodbc
import psutil
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import aiofiles

async def batch_vision_reason(
    file_id: str,
    entry_ids: Optional[List[int]] = None,
    step: int = 0,
    limit: int = 5000,
    concurrency: int = 10,
    logger: Optional[logging.Logger] = None
) -> dict:
    logger, log_filename = setup_job_logger(job_id=str(file_id), log_dir="job_logs", console_output=True)
    process = psutil.Process()
    try:
        file_id = int(file_id)
        logger.info(f"Starting batch image processing for FileID: {file_id}, Step: {step}, Limit: {limit}")
        mem_info = process.memory_info()
        logger.debug(f"Memory before processing: RSS={mem_info.rss / 1024**2:.2f} MB")

        df = await fetch_missing_images(file_id, limit, True, logger)
        if df.empty:
            logger.warning(f"No missing images found for FileID: {file_id}")
            return {"status_code": 200, "message": "No missing images found", "data": []}

        if entry_ids is not None:
            df = df[df['EntryID'].isin(entry_ids)]
            if df.empty:
                logger.warning(f"No missing images found for specified EntryIDs: {entry_ids}")
                return {"status_code": 200, "message": f"No missing images for EntryIDs: {entry_ids}", "data": []}

        columns_to_drop = ['Step1', 'Step2', 'Step3', 'Step4', 'CreateTime_1', 'CreateTime_2']
        df = df.drop(columns=[col for col in columns_to_drop if col in df.columns], errors='ignore')
        logger.info(f"Retrieved {len(df)} image rows for FileID: {file_id}")
        entry_ids_to_process = list(df.groupby('EntryID').groups.keys())

        valid_updates = []
        semaphore = asyncio.Semaphore(concurrency)
        async def process_with_semaphore(entry_id, df_subset):
            async with semaphore:
                return await process_entry_wrapper(file_id, entry_id, df_subset, logger)

        tasks = [
            process_with_semaphore(entry_id, df[df['EntryID'] == entry_id])
            for entry_id in entry_ids_to_process
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for entry_id, updates in zip(entry_ids_to_process, results):
            if isinstance(updates, Exception):
                logger.error(f"Error processing EntryID {entry_id}: {updates}", exc_info=True)
                continue
            if not updates:
                logger.warning(f"No valid updates for EntryID: {entry_id}")
                continue
            valid_updates.extend(updates)
            logger.info(f"Collected {len(updates)} updates for EntryID: {entry_id}")

        if valid_updates:
            async with async_engine.connect() as conn:
                for update in valid_updates:
                    ai_json, image_is_fashion, ai_caption, result_id = update
                    await conn.execute(
                        text("""
                            UPDATE utb_ImageScraperResult 
                            SET AiJson = :ai_json, ImageIsFashion = :image_is_fashion, AiCaption = :ai_caption 
                            WHERE ResultID = :result_id
                        """),
                        {
                            "ai_json": ai_json,
                            "image_is_fashion": image_is_fashion,
                            "ai_caption": ai_caption,
                            "result_id": result_id
                        }
                    )
                await conn.commit()
                logger.info(f"Updated {len(valid_updates)} records: {[update[3] for update in valid_updates]}")

        for entry_id in entry_ids_to_process:
            async with async_engine.connect() as conn:
                result = await conn.execute(
                    text("""
                        SELECT ProductBrand, ProductModel, ProductColor, ProductCategory
                        FROM utb_ImageScraperRecords
                        WHERE FileID = :file_id AND EntryID = :entry_id
                    """),
                    {"file_id": file_id, "entry_id": entry_id}
                )
                row = result.fetchone()
                result.close()
                product_brand = product_model = product_color = product_category = ''
                if row:
                    product_brand, product_model, product_color, product_category = row
                else:
                    logger.warning(f"No attributes for FileID: {file_id}, EntryID: {entry_id}")

            await update_search_sort_order(
                file_id=str(file_id),
                entry_id=str(entry_id),
                brand=product_brand,
                model=product_model,
                color=product_color,
                category=product_category,
                logger=logger
            )
            logger.info(f"Updated sort order for FileID: {file_id}, EntryID: {entry_id}")

        mem_info = process.memory_info()
        logger.debug(f"Memory before JSON export: RSS={mem_info.rss / 1024**2:.2f} MB")
        json_url = await export_dai_json(file_id, entry_ids, logger)
        if json_url:
            logger.info(f"DAI JSON exported to {json_url}")
            await update_log_url_in_db(file_id, json_url, logger)
        else:
            logger.warning(f"Failed to export DAI JSON for FileID: {file_id}")

        mem_info = process.memory_info()
        logger.debug(f"Memory after processing: RSS={mem_info.rss / 1024**2:.2f} MB")

        return {"status_code": 200, "message": f"Processed {len(valid_updates)} updates for FileID: {file_id}", "data": valid_updates}

    except Exception as e:
        logger.error(f"Error in batch_vision_reason for FileID {file_id}: {e}", exc_info=True)
        return {"status_code": 500, "message": f"Error: {str(e)}", "data": []}
    finally:
        await async_engine.dispose()
        logger.info(f"Disposed database engines")

async def process_entry_wrapper(
    file_id: int,
    entry_id: int,
    entry_df: pd.DataFrame,
    logger: logging.Logger,
    max_retries: int = 3
) -> List[Tuple[str, bool, str, int]]:
    process = psutil.Process()
    attempt = 1
    while attempt <= max_retries:
        logger.info(f"Processing EntryID {entry_id}, attempt {attempt}/{max_retries}")
        try:
            mem_info = process.memory_info()
            logger.debug(f"Memory before processing EntryID {entry_id}: RSS={mem_info.rss / 1024**2:.2f} MB")
            
            if not all(pd.notna(entry_df.get('ResultID', pd.Series([])))):
                logger.error(f"Invalid ResultID in entry_df for EntryID {entry_id}")
                return []

            updates = await process_entry(file_id, entry_id, entry_df, logger)
            if not updates:
                logger.warning(f"No updates returned for EntryID {entry_id} on attempt {attempt}")
                attempt += 1
                await asyncio.sleep(2)
                continue

            valid_updates = []
            for update in updates:
                if not isinstance(update, (list, tuple)) or len(update) != 4:
                    logger.error(f"Invalid update tuple for EntryID {entry_id}: {update}")
                    continue
                
                ai_json, image_is_fashion, ai_caption, result_id = update
                if not isinstance(ai_json, str):
                    logger.error(f"Invalid ai_json type for ResultID {result_id}: {type(ai_json).__name__}")
                    ai_json = json.dumps({"error": f"Invalid ai_json type: {type(ai_json).__name__}", "result_id": result_id, "scores": {"sentiment": 0.0, "relevance": 0.0}})
                
                if is_valid_ai_result(ai_json, ai_caption or "", logger):
                    valid_updates.append((ai_json, image_is_fashion, ai_caption, result_id))
                else:
                    logger.warning(f"Invalid AI result for ResultID {result_id} on attempt {attempt}")

            if valid_updates:
                logger.info(f"Valid updates for EntryID {entry_id}: {len(valid_updates)}")
                mem_info = process.memory_info()
                logger.debug(f"Memory after processing EntryID {entry_id}: RSS={mem_info.rss / 1024**2:.2f} MB")
                return valid_updates
            else:
                logger.warning(f"No valid updates for EntryID {entry_id} on attempt {attempt}")
                attempt += 1
                await asyncio.sleep(2)
        
        except Exception as e:
            logger.error(f"Error processing EntryID {entry_id} on attempt {attempt}: {e}", exc_info=True)
            attempt += 1
            await asyncio.sleep(2)
    
    logger.error(f"Failed to process EntryID {entry_id} after {max_retries} attempts")
    return [
        (
            json.dumps({"scores": {"sentiment": 0.0, "relevance": 0.0}, "category": "unknown", "error": "Processing failed"}),
            False,
            "Failed to generate caption",
            int(row.get('ResultID', 0))
        ) for _, row in entry_df.iterrows() if pd.notna(row.get('ResultID'))
    ]

def is_valid_ai_result(ai_json: str, ai_caption: str, logger: logging.Logger) -> bool:
    process = psutil.Process()
    try:
        if not ai_caption or ai_caption.strip() == "":
            logger.warning(f"Invalid AI result: AiCaption is empty")
            return False
        
        parsed_json = json.loads(ai_json)
        if not isinstance(parsed_json, dict):
            logger.warning(f"Invalid AI result: AiJson is not a dictionary")
            return False
        
        if "scores" not in parsed_json or not parsed_json["scores"]:
            logger.warning(f"Invalid AI result: AiJson missing or empty 'scores' field, AiJson: {ai_json}")
            return False
        
        return True
    except json.JSONDecodeError as e:
        logger.warning(f"Invalid AI result: AiJson is not valid JSON: {e}, AiJson: {ai_json}")
        return False