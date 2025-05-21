#!/usr/bin/env python
import pika
import json
import logging
import time
import asyncio
import signal
import sys
import datetime
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from database_config import async_engine
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

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
        self.is_consuming = False

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
        if self.channel and not self.channel.is_closed:
            if self.is_consuming:
                try:
                    self.channel.stop_consuming()
                    logger.info("Stopped consuming messages")
                except Exception as e:
                    logger.error(f"Error stopping consumption: {e}", exc_info=True)
            self.channel.close()
            logger.info("Closed RabbitMQ channel")
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.info("Closed RabbitMQ connection")
        self.is_consuming = False

    def purge_queue(self):
        """Purge all messages from the queue. Use with caution."""
        if not self.channel or self.channel.is_closed:
            self.connect()
        try:
            purge_count = self.channel.queue_purge(self.queue_name)
            logger.info(f"Purged {purge_count} messages from queue: {self.queue_name}")
            return purge_count
        except Exception as e:
            logger.error(f"Error purging queue {self.queue_name}: {e}", exc_info=True)
            raise

    def start_consuming(self):
        while True:
            try:
                if not self.connection or self.connection.is_closed:
                    self.connect()
                self.is_consuming = True
                self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback)
                logger.info("Waiting for messages. To exit press CTRL+C")
                self.channel.start_consuming()
            except pika.exceptions.AMQPConnectionError as e:
                logger.error(f"AMQP connection error: {e}, reconnecting in 5 seconds", exc_info=True)
                self.is_consuming = False
                time.sleep(5)
            except KeyboardInterrupt:
                logger.info("Received KeyboardInterrupt, shutting down consumer")
                self.close()
                break
            except Exception as e:
                logger.error(f"Unexpected error in consumer: {e}, reconnecting in 5 seconds", exc_info=True)
                self.is_consuming = False
                self.close()
                time.sleep(5)

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

            # Validate required parameters
            if not all([entry_id, result_id, sort_order is not None]):
                logger.error(
                    f"Invalid parameters for update_sort_order task, FileID: {file_id}. "
                    f"Required: entry_id, result_id, sort_order. Got: {params}"
                )
                return False

            # Direct UPDATE task for a single result
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

def signal_handler(consumer):
    def handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down gracefully...")
        consumer.close()
        sys.exit(0)
    return handler

if __name__ == "__main__":
    consumer = RabbitMQConsumer()
    signal.signal(signal.SIGINT, signal_handler(consumer))
    signal.signal(signal.SIGTERM, signal_handler(consumer))

    # Sample task for insert_result
    insert_task = {
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

    # Sample task for update_sort_order
    sort_task = {
        "file_id": "321",
        "task_type": "update_sort_order",
        "sql": "UPDATE_SORT_ORDER",
        "params": {
            "entry_id": "119061",
            "result_id": "1868277",
            "sort_order": 1
        },
        "timestamp": "2025-05-21T12:34:08.307076"
    }

    try:
        loop = asyncio.get_event_loop()
        # Test both tasks
        loop.run_until_complete(consumer.test_task(insert_task))
        loop.run_until_complete(consumer.test_task(sort_task))
        
        consumer.connect()
        consumer.start_consuming()
    except KeyboardInterrupt:
        consumer.close()
    except Exception as e:
        logger.error(f"Error in consumer: {e}", exc_info=True)
        consumer.close()