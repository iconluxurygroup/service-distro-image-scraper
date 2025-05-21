#!/usr/bin/env python
import pika
import json
import logging
import time
import asyncio
import signal
import sys
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from database_config import async_engine
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from search_utils import update_search_sort_order
from common import clean_string, normalize_model, generate_aliases
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class RabbitMQConsumer:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5672,
        username: str = "guest",
        password: str = "guest",
        queue_name: str = "db_update_queue",
    ):
        self.host = host
        self.port = port
        self.credentials = pika.PlainCredentials(username, password)
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(pika.exceptions.AMQPConnectionError),
        before_sleep=lambda retry_state: logger.info(
            f"Retrying RabbitMQ connection (attempt {retry_state.attempt_number}/3)"
        ),
    )
    def connect(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                credentials=self.credentials,
            )
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=True)
        self.channel.basic_qos(prefetch_count=1)
        logger.info(f"Connected to RabbitMQ, consuming from queue: {self.queue_name}")

    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.info("Closed RabbitMQ connection")

    def start_consuming(self):
        try:
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback)
            logger.info("Waiting for messages. To exit press CTRL+C")
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"AMQP connection error: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Error starting consumer: {e}", exc_info=True)
            raise

    async def execute_update(self, sql: str, params: dict, task_type: str, file_id: str):
        try:
            async with async_engine.begin() as conn:
                # Normalize CreateTime format if present
                if "CreateTime" in params and params["CreateTime"]:
                    try:
                        params["CreateTime"] = datetime.datetime.strptime(
                            params["CreateTime"], "%Y-%m-%d %H:%M:%S"
                        )
                    except ValueError as e:
                        logger.warning(f"Invalid CreateTime format for FileID: {file_id}: {e}")
                        params["CreateTime"] = datetime.datetime.now()

                result = await conn.execute(text(sql), params)
                await conn.commit()
                rowcount = result.rowcount if result.rowcount is not None else 0
                logger.info(
                    f"TaskType: {task_type}, FileID: {file_id}, Executed SQL: {sql}, "
                    f"params: {params}, affected {rowcount} rows"
                )
                return True
        except SQLAlchemyError as e:
            logger.error(
                f"TaskType: {task_type}, FileID: {file_id}, Database error executing SQL: {sql}, "
                f"params: {params}, error: {e}",
                exc_info=True
            )
            return False
        except Exception as e:
            logger.error(
                f"TaskType: {task_type}, FileID: {file_id}, Unexpected error executing SQL: {sql}, "
                f"params: {params}, error: {e}",
                exc_info=True
            )
            return False

    async def execute_sort_order_update(self, params: dict, file_id: str):
        try:
            entry_id = params.get("entry_id")
            result_id = params.get("result_id")
            sort_order = params.get("sort_order")

            # Case 1: New direct UPDATE task
            if all([entry_id, result_id, sort_order is not None]):
                sql = """
                    UPDATE utb_ImageScraperResult
                    SET SortOrder = :sort_order
                    WHERE EntryID = :entry_id AND ResultID = :result_id
                """
                update_params = {
                    "sort_order": sort_order,
                    "entry_id": entry_id,
                    "result_id": result_id
                }

                async with async_engine.begin() as conn:
                    result = await conn.execute(text(sql), update_params)
                    await conn.commit()
                    rowcount = result.rowcount if result.rowcount is not None else 0
                    logger.info(
                        f"TaskType: update_sort_order, FileID: {file_id}, Executed SQL: {sql}, "
                        f"params: {update_params}, affected {rowcount} rows"
                    )

                    if rowcount == 0:
                        logger.warning(f"No rows updated for FileID: {file_id}, EntryID: {entry_id}, ResultID: {result_id}")
                        return False

            # Case 2: Old UPDATE_SORT_ORDER task
            elif entry_id and params.get("brand") and params.get("search_string"):
                # Call update_search_sort_order to compute sort orders
                brand = params.get("brand")
                search_string = params.get("search_string")
                color = params.get("color")
                category = params.get("category")
                brand_rules = json.loads(params.get("brand_rules", "{}"))

                # Fetch results and compute priorities (mimicking update_search_sort_order logic)
                async with async_engine.connect() as conn:
                    query = text("""
                        SELECT ResultID, ImageUrl, ImageDesc, ImageSource, ImageUrlThumbnail
                        FROM utb_ImageScraperResult
                        WHERE EntryID = :entry_id
                    """)
                    result = await conn.execute(query, {"entry_id": entry_id})
                    rows = result.fetchall()
                    columns = result.keys()
                    result.close()

                if not rows:
                    logger.warning(f"No results found for FileID: {file_id}, EntryID: {entry_id}")
                    return False

                results = [dict(zip(columns, row)) for row in rows]

                # Process brand and model aliases
                brand_clean = clean_string(brand).lower() if brand else ""
                model_clean = normalize_model(search_string) if search_string else ""
                brand_aliases = []
                if brand_clean and brand_rules and "brand_rules" in brand_rules:
                    for rule in brand_rules["brand_rules"]:
                        if any(brand.lower() in name.lower() for name in rule.get("names", [])):
                            brand_aliases = rule.get("names", [])
                            break
                if not brand_aliases and brand_clean:
                    brand_aliases = [brand_clean, brand_clean.replace(" & ", " and "), brand_clean.replace(" ", "")]
                brand_aliases = [clean_string(alias).lower() for alias in brand_aliases]
                model_aliases = generate_aliases(model_clean) if model_clean else []
                if model_clean and not model_aliases:
                    model_aliases = [model_clean, model_clean.replace("-", ""), model_clean.replace(" ", "")]

                # Assign priorities
                for res in results:
                    image_desc = clean_string(res.get("ImageDesc", ""), preserve_url=False).lower()
                    image_source = clean_string(res.get("ImageSource", ""), preserve_url=True).lower()
                    image_url = clean_string(res.get("ImageUrl", ""), preserve_url=True).lower()
                    model_matched = any(alias in image_desc or alias in image_source or alias in image_url for alias in model_aliases)
                    brand_matched = any(alias in image_desc or alias in image_source or alias in image_url for alias in brand_aliases)
                    if model_matched and brand_matched:
                        res["priority"] = 1
                    elif model_matched:
                        res["priority"] = 2
                    elif brand_matched:
                        res["priority"] = 3
                    else:
                        res["priority"] = 4

                # Sort and assign SortOrder
                sorted_results = sorted(results, key=lambda x: x["priority"])
                update_success = True
                rows_affected = 0

                async with async_engine.begin() as conn:
                    for index, res in enumerate(sorted_results):
                        sort_order = -2 if res["priority"] == 4 else (index + 1)
                        sql = """
                            UPDATE utb_ImageScraperResult
                            SET SortOrder = :sort_order
                            WHERE EntryID = :entry_id AND ResultID = :result_id
                        """
                        update_params = {
                            "sort_order": sort_order,
                            "entry_id": entry_id,
                            "result_id": res["ResultID"]
                        }
                        result = await conn.execute(text(sql), update_params)
                        rowcount = result.rowcount if result.rowcount is not None else 0
                        rows_affected += rowcount
                        if rowcount == 0:
                            logger.warning(f"No rows updated for FileID: {file_id}, EntryID: {entry_id}, ResultID: {res['ResultID']}")
                            update_success = False
                        else:
                            logger.info(
                                f"TaskType: update_sort_order, FileID: {file_id}, Executed SQL: {sql}, "
                                f"params: {update_params}, affected {rowcount} rows"
                            )

                    await conn.commit()

                if not update_success:
                    logger.warning(f"Some updates failed for FileID: {file_id}, EntryID: {entry_id}")
                    return False

            else:
                logger.error(f"Invalid params for FileID: {file_id}, params: {params}")
                return False

            # Validate positive SortOrder values
            async with async_engine.connect() as conn:
                result = await conn.execute(
                    text("""
                        SELECT COUNT(*) 
                        FROM utb_ImageScraperResult 
                        WHERE EntryID = :entry_id 
                        AND SortOrder > 0
                    """),
                    {"entry_id": entry_id}
                )
                positive_sort_count = result.scalar()
                result.close()
                if positive_sort_count == 0:
                    logger.warning(f"No positive SortOrder for FileID: {file_id}, EntryID: {entry_id}")
                else:
                    logger.info(f"Validated {positive_sort_count} positive SortOrder for FileID: {file_id}, EntryID: {entry_id}")

            return True

        except SQLAlchemyError as e:
            logger.error(f"Database error updating SortOrder for FileID: {file_id}, EntryID: {entry_id}: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Error updating SortOrder for FileID: {file_id}, EntryID: {entry_id}: {e}", exc_info=True)
            return False

    def callback(self, ch, method, properties, body):
        try:
            message = body.decode()
            task = json.loads(message)
            file_id = task.get("file_id", "unknown")
            task_type = task.get("task_type", "unknown")
            logger.info(f"Received task for FileID: {file_id}, TaskType: {task_type}, Task: {message[:200]}")

            sql = task.get("sql")
            params = task.get("params", {})

            loop = asyncio.get_event_loop()
            if task_type == "update_sort_order" and sql == "UPDATE_SORT_ORDER":
                success = loop.run_until_complete(self.execute_sort_order_update(params, file_id))
            else:
                success = loop.run_until_complete(self.execute_update(sql, params, task_type, file_id))

            if success:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                logger.info(f"Successfully processed {task_type} for FileID: {file_id}, Acknowledged")
            else:
                logger.warning(f"Failed to process {task_type} for FileID: {file_id}; re-queueing")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                time.sleep(2)
        except Exception as e:
            logger.error(
                f"Error processing message for FileID: {file_id}, TaskType: {task_type}: {e}",
                exc_info=True
            )
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            time.sleep(2)

    async def test_task(self, task: dict):
        """Simulate processing a single task for testing purposes."""
        file_id = task.get("file_id", "unknown")
        task_type = task.get("task_type", "unknown")
        sql = task.get("sql")
        params = task.get("params", {})
        
        logger.info(f"Testing task for FileID: {file_id}, TaskType: {task_type}")
        if task_type == "update_sort_order" and sql == "UPDATE_SORT_ORDER":
            success = await self.execute_sort_order_update(params, file_id)
        else:
            success = await self.execute_update(sql, params, task_type, file_id)
        
        logger.info(f"Test task result for FileID: {file_id}, TaskType: {task_type}: {'Success' if success else 'Failed'}")
        return success

def signal_handler(sig, frame):
    logger.info("Received SIGINT, shutting down gracefully...")
    consumer.close()
    sys.exit(0)

if __name__ == "__main__":
    import datetime
    consumer = RabbitMQConsumer()
    signal.signal(signal.SIGINT, signal_handler)

    # Sample task from logs, updated to include CreateTime
    sample_task = {
        "file_id": "321",
        "task_type": "insert_result",
        "sql": """
            INSERT INTO utb_ImageScraperResult (EntryID, ImageUrl, ImageDesc, ImageSource, ImageUrlThumbnail, CreateTime)
            VALUES (:EntryID, :ImageUrl, :ImageDesc, :ImageSource, :ImageUrlThumbnail, :CreateTime)
        """,
        "params": {
            "EntryID": 119061,
            "ImageUrl": "https://image.goat.com/transform/v1/attachments/product_template_pictures/images/105/346/991/original/OMIA295F24FAB001_0145.png",
            "ImageDesc": "Off-White Be Right Back 'White Blue' - OMIA295F24FAB001 0145 | The Home Depot Canada",
            "ImageSource": "image.goat.com",
            "ImageUrlThumbnail": "https://image.goat.com/transform/v1/attachments/product_template_pictures/images/105/346/991/original/OMIA295F24FAB001_0145.png",
            "CreateTime": "2025-05-21 12:34:08"
        },
        "timestamp": "2025-05-21T12:34:08.307076"
    }

    try:
        # Test a single task first
        loop = asyncio.get_event_loop()
        loop.run_until_complete(consumer.test_task(sample_task))
        
        # Then start consuming from queue
        consumer.connect()
        consumer.start_consuming()
    except KeyboardInterrupt:
        consumer.close()
    except Exception as e:
        logger.error(f"Error in consumer: {e}", exc_info=True)
        consumer.close()