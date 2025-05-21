#!/usr/bin/env python
import pika
import json
import logging
import time
import asyncio
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from database_config import async_engine

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

async def execute_update(sql: str, params: dict):
    """Execute a database update asynchronously."""
    try:
        async with async_engine.connect() as conn:
            result = await conn.execute(text(sql), params)
            await conn.commit()
            logger.info(f"Executed SQL: {sql} with params: {params}")
            return True
    except SQLAlchemyError as e:
        logger.error(f"Database error executing SQL: {sql}, params: {params}, error: {e}", exc_info=True)
        return False

def callback(ch, method, properties, body):
    """Callback function to process messages from the queue."""
    try:
        # Decode and parse the message
        message = body.decode()
        task = json.loads(message)
        logger.info(f"Received task: {task}")

        # Extract task details
        sql = task.get("sql")
        params = task.get("params", {})
        file_id = task.get("file_id")

        # Execute the update asynchronously
        loop = asyncio.get_event_loop()
        success = loop.run_until_complete(execute_update(sql, params))

        if success:
            # Acknowledge the message
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info(f"Successfully processed update for FileID: {file_id}")
        else:
            # Do not acknowledge; message will be re-queued
            logger.warning(f"Failed to process update for FileID: {file_id}; message will be re-queued")
    except Exception as e:
        logger.error(f"Error processing message: {e}", exc_info=True)
        # Do not acknowledge; message will be re-queued
        time.sleep(2)  # Prevent rapid re-queue loops

def main():
    """Main function to start the RabbitMQ consumer."""
    queue_name = "db_update_queue"
    try:
        # Connect to RabbitMQ
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host="localhost")
        )
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)
        logger.info(f"Connected to RabbitMQ, consuming from queue: {queue_name}")

        # Set prefetch to 1 for fair dispatch
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=queue_name, on_message_callback=callback)

        logger.info("Waiting for messages. To exit press CTRL+C")
        channel.start_consuming()
    except Exception as e:
        logger.error(f"Error in consumer: {e}", exc_info=True)
    finally:
        if "connection" in locals() and connection.is_open:
            connection.close()
            logger.info("Closed RabbitMQ connection")

if __name__ == "__main__":
    main()