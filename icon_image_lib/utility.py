#utility.py
import asyncio
import httpx
import time,os
from httpx import ConnectTimeout
import logging
#import mysql.connector
from dotenv import load_dotenv
load_dotenv()
#from mysql.connector import pooling

from concurrent.futures import ThreadPoolExecutor
from asyncio.exceptions import TimeoutError
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
#from mysql.connector.errors import PoolError
# global_connection_pool = mysql.connector.pooling.MySQLConnectionPool(
#     pool_name="mypool3",
#     pool_size=32,
#     host=os.getenv('DBHOST'),
#     database='defaultdb',
#     user='doadmin',
#     password=os.getenv('DBPASS'),
#     port=25060
# )

async def create_image_task(dataset_split):
    try:
        print(dataset_split)
        logger.info("Attempting to create an image task")
        async with httpx.AsyncClient(timeout=None) as client:
            print(f"{str(os.environ.get('PRODUCTAPIENDPOINT'))}/api/v1/image/create")
            response = await client.post(f"{str(os.environ.get('PRODUCTAPIENDPOINT'))}/api/v1/image/create", json={"dataset_split": dataset_split})
            result = response.json()
            print(result)
            logger.info(f"Image task created successfully with response: {result}")
            return result
    except Exception as e:
        logger.exception(f"Failed to create image task with exception: {e}")
        raise


async def poll_task_status(task_id, timeout=5000):
    try:
        logger.info(f"Starting to poll task status for task_id: {task_id}")
        async with httpx.AsyncClient(timeout=None) as client:
            start_time = asyncio.get_event_loop().time()
            while True:
                current_time = asyncio.get_event_loop().time()
                if current_time - start_time > timeout:
                    logger.warning(f"Timeout reached for task {task_id}. Abandoning task.")
                    return {'error': 'Polling timeout reached. Task abandoned.'}
                response = await client.get(f"{os.environ.get('PRODUCTAPIENDPOINT')}/api/v1/image/poll/{task_id}")
                data = response.json()
                if data['status'] == 'Completed':
                    logger.info(f"Task {task_id} completed successfully with data: {data}")
                    return data
                elif data['status'] in ['Failed', 'Error']:
                    logger.error(f"Task {task_id} failed or encountered an error with data: {data}")
                    return {'error': 'Task failed or encountered an error'}
                else:
                    await asyncio.sleep(int(os.environ.get('POLL_INTERVAL'))) # Poll every 60 seconds
    except Exception as e:
        logger.exception(f"Exception occurred while polling task {task_id} Exception: {e}")
        raise

def get_task_status(task_id,pool):
    """
    Synchronously checks the task status from the database.
    """
    conn = pool.get_connection()
    #conn = mysql.connector.connect(**db_config)
    try:
        with conn.cursor(dictionary=True) as cursor:
            query = """
            SELECT completeTime, EntryID, ImageUrl, ImageDesc
            FROM utb_ImageScraperResult
            WHERE ResultID = %s
            """
            cursor.execute(query, (task_id,))
            result = cursor.fetchone()
    finally:
        conn.close()
    return result

async def process_queued_item(queue, result_id):
    while True:
        item = await queue.get()
        if item[0] == result_id:
            entry_id, image_url, image_desc = await wait_for_completion(result_id, item[1], queue)
            return entry_id, image_url, image_desc
        else:
            # If the item is not the one we're waiting for, put it back in the queue
            await queue.put(item)

async def wait_for_completion(result_id, db_pool, queue):
    async def poll_database():
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor() as pool:
            while True:
                try:
                    result = await loop.run_in_executor(pool, get_task_status, result_id, db_pool)
                    if result and result['completeTime']:
                        print(f"Process completed for ResultID {result_id}.")
                        return result['EntryID'], result['ImageUrl'], result['ImageDesc']
                    print("Waiting for process to complete...")
                    await asyncio.sleep(5)  # Async sleep
                except PoolError as e:
                    if "pool exhausted" in str(e):
                        print(f"Pool exhausted for ResultID {result_id}. Queueing for the next available worker.")
                        await queue.put((result_id, db_pool))  # Queue the item for the next available worker
                        return await process_queued_item(queue, result_id)  # Process the queued item
                    else:
                        raise e

    try:
        # Attempt to complete the task within 10 minutes
        return await asyncio.wait_for(poll_database(), timeout=1800)
    except TimeoutError:
        # Handle the case where the task does not complete within 10 minutes
        print(f"Process did not complete within 10 minutes for ResultID {result_id}. Assuming it broke.")
        return None, None, None  # Or any other placeholder values you prefer


# async def wait_for_completion(result_id,db_pool):
#     """
#     Asynchronously polls the database for the completeTime of the specified result_id.
#     Uses a ThreadPoolExecutor to run synchronous DB operations without blocking the event loop.
#     """
#     loop = asyncio.get_running_loop()
#     with ThreadPoolExecutor() as pool:
#         while True:
#             result = await loop.run_in_executor(pool, get_task_status, result_id,db_pool)
#             if result and result['completeTime']:
#                 print(f"Process completed for ResultID {result_id}.")
#                 return result['EntryID'], result['ImageUrl'], result['ImageDesc']
#             print("Waiting for process to complete...")
#             await asyncio.sleep(5)  # Async sleep
# async def wait_for_completion2(result_id, db_pool):
#     async def poll_database():
#         loop = asyncio.get_running_loop()
#         with ThreadPoolExecutor() as pool:
#             while True:
#                 result = await loop.run_in_executor(pool, get_task_status, result_id,db_pool)
#                 if result and result['completeTime']:
#                     print(f"Process completed for ResultID {result_id}.")
#                     return result['EntryID'], result['ImageUrl'], result['ImageDesc']
#                 print("Waiting for process to complete...")
#                 await asyncio.sleep(5)  # Async sleep
#
#     try:
#         # Attempt to complete the task within 10 minutes
#         return await asyncio.wait_for(poll_database(), timeout=1800)
#     except TimeoutError:
#         # Handle the case where the task does not complete within 10 minutes
#         print(f"Process did not complete within 10 minutes for ResultID {result_id}. Assuming it broke.")
#         return None, None, None  # Or any other placeholder values you prefer
async def process_row(row,uniqueid):
    global global_connection_pool
    try:
        logger.info(f"Processing row: {row}")

        absolute_row_index = row.get('absoluteRowIndex')
        original_search_value = row.get('searchValue')

        dataset_split = [str(row.get('brandValue')), str(row.get('searchValue')),str(absolute_row_index),str(uniqueid)]
        create_response = await create_image_task(dataset_split)
        task_id = create_response.get('task_id')
        #connection = mysql.connector.connect(**conn_params)
        connection = global_connection_pool.get_connection()
        cursor = connection.cursor()
        sql_query = f"INSERT INTO utb_ImageScraperResult (EntryID,FileID,SearchValue) values ({absolute_row_index},'{uniqueid}', '{original_search_value}')"
        print(sql_query)
        cursor.execute(str(sql_query))
        connection.commit()
        result_id = cursor.lastrowid
        cursor.close()
        connection.close()

        # if task_id:
        #     logger.info(f"Task ID {task_id} received, starting to poll for completion...")
        #     #await asyncio.sleep(int(os.environ.get('POLL_AFTER'))) # Wait for 3 minutes before polling, asynchronously
        #
        #     #result = await asyncio.wait_for(poll_task_status(task_id), timeout=10000)  # Example timeout
        #     result = await wait_for_completion(result_id,global_connection_pool)
        #
        #     if result:
        #         logger.info(f"Task {task_id} completed with result: {result}")
        #         entry_id, image_url, image_desc = result
        #         result_f = {"url":image_url}
        #         return {
        #             "result": result_f,
        #             "absoluteRowIndex": absolute_row_index,
        #             "originalSearchValue": original_search_value
        #         }
        # else:
        #     logger.warning("Failed to create task. No task ID received.")
        if task_id:
            logger.info(f"Task ID {task_id} received, starting to poll for completion...")
            queue = asyncio.Queue()

            entry_id, image_url, image_desc = await wait_for_completion(result_id, global_connection_pool, queue)

            if entry_id is not None and image_url is not None and image_desc is not None:
                logger.info(
                    f"Task {task_id} completed with result: (entry_id={entry_id}, image_url={image_url}, image_desc={image_desc})")
                result_f = {"url": image_url}
                return {
                    "result": result_f,
                    "absoluteRowIndex": absolute_row_index,
                    "originalSearchValue": original_search_value
                }
            else:
                logger.warning(f"Task {task_id} did not complete successfully.")
                return {
                    "error": "Task did not complete successfully.",
                    "absoluteRowIndex": absolute_row_index,
                    "originalSearchValue": original_search_value
                }
        else:
            logger.warning("Failed to create task. No task ID received.")
            return {
                "error": "Failed to start task.",
                "absoluteRowIndex": absolute_row_index,
                "originalSearchValue": original_search_value
            }
    except Exception as e:
        logger.exception("An error occurred while processing row")
        return {
            "error": str(e),
            "absoluteRowIndex": absolute_row_index,
            "originalSearchValue": original_search_value
        }
