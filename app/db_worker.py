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
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback)
        logger.info("Waiting for messages. To exit press CTRL+C")
        self.channel.start_consuming()

    async def execute_update(self, sql: str, params: dict):
        try:
            async with async_engine.connect() as conn:
                result = await conn.execute(text(sql), params)
                await conn.commit()
                logger.info(f"Executed SQL: {sql[:100]} with params: {params}")
                return True
        except SQLAlchemyError as e:
            logger.error(f"Database error executing SQL: {sql[:100]}, params: {params}, error: {e}", exc_info=True)
            return False

    async def execute_sort_order_update(self, params: dict):
        try:
            file_id = params.get("file_id")
            entry_id = params.get("entry_id")
            brand = params.get("brand")
            search_string = params.get("search_string")
            color = params.get("color")
            category = params.get("category")
            brand_rules = json.loads(params.get("brand_rules", "{}"))

            result = await update_search_sort_order(
                file_id=file_id,
                entry_id=entry_id,
                brand=brand,
                model=search_string,
                color=color,
                category=category,
                logger=logger,
                brand_rules=brand_rules
            )
            if result:
                logger.info(f"Updated SortOrder for FileID: {file_id}, EntryID: {entry_id}, {len(result)} rows")
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
                        logger.warning(f"No positive SortOrder for EntryID {entry_id}")
                        return False
                    logger.info(f"Validated {positive_sort_count} positive SortOrder for EntryID {entry_id}")
                return True
            else:
                logger.warning(f"No SortOrder updated for FileID: {file_id}, EntryID: {entry_id}")
                return False
        except Exception as e:
            logger.error(f"Error updating SortOrder for FileID: {file_id}, EntryID: {entry_id}: {e}", exc_info=True)
            return False

    def callback(self, ch, method, properties, body):
        try:
            message = body.decode()
            task = json.loads(message)
            logger.info(f"Received task for FileID: {task.get('file_id')}, TaskType: {task.get('task_type')}")

            sql = task.get("sql")
            params = task.get("params", {})
            file_id = task.get("file_id")
            task_type = task.get("task_type")

            loop = asyncio.get_event_loop()
            if task_type == "update_sort_order" and sql == "UPDATE_SORT_ORDER":
                success = loop.run_until_complete(self.execute_sort_order_update(params))
            else:
                success = loop.run_until_complete(self.execute_update(sql, params))

            if success:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                logger.info(f"Processed {task_type} for FileID: {file_id}")
            else:
                logger.warning(f"Failed {task_type} for FileID: {file_id}; re-queued")
                time.sleep(2)
        except Exception as e:
            logger.error(f"Error processing message for FileID: {task.get('file_id', 'unknown')}: {e}", exc_info=True)
            time.sleep(2)

def signal_handler(sig, frame):
    logger.info("Received SIGINT, shutting down gracefully...")
    consumer.close()
    sys.exit(0)

if __name__ == "__main__":
    consumer = RabbitMQConsumer()
    signal.signal(signal.SIGINT, signal_handler)
    try:
        consumer.connect()
        consumer.start_consuming()
    except KeyboardInterrupt:
        consumer.close()
    except Exception as e:
        logger.error(f"Error in consumer: {e}", exc_info=True)
        consumer.close()