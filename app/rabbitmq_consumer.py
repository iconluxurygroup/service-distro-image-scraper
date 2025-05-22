
import aio_pika
import json
import logging
from typing import Dict, Any, Optional
import datetime
from fastapi import BackgroundTasks
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import asyncio
import psutil
import sys
import signal
from database_config import async_engine
from rabbitmq_producer import RabbitMQProducer
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

class RabbitMQConsumer:
    def __init__(
        self,
        amqp_url: str = "amqp://guest:guest@localhost:5672/",
        queue_name: str = "db_update_queue",
        connection_timeout: float = 10.0,
        operation_timeout: float = 5.0,
    ):
        self.amqp_url = amqp_url
        self.queue_name = queue_name
        self.connection_timeout = connection_timeout
        self.operation_timeout = operation_timeout
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.is_consuming = False

    async def connect(self):
        """Establish an async robust connection to RabbitMQ."""
        if self.connection and not self.connection.is_closed:
            return
        try:
            async with asyncio.timeout(self.connection_timeout):
                self.connection = await aio_pika.connect_robust(
                    self.amqp_url,
                    connection_attempts=3,
                    retry_delay=5,
                )
                self.channel = await self.connection.channel()
                await self.channel.set_qos(prefetch_count=1)
                await self.channel.declare_queue(self.queue_name, durable=True)
                logger.info(f"Connected to RabbitMQ, consuming from queue: {self.queue_name}")
        except asyncio.TimeoutError:
            logger.error("Timeout connecting to RabbitMQ")
            raise
        except aio_pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}", exc_info=True)
            raise

    async def close(self):
        """Close the RabbitMQ connection and channel."""
        try:
            if self.is_consuming:
                self.is_consuming = False
                logger.info("Stopped consuming messages")
            if self.channel and not self.channel.is_closed:
                await self.channel.close()
                logger.info("Closed RabbitMQ channel")
            if self.connection and not self.connection.is_closed:
                await self.connection.close()
                logger.info("Closed RabbitMQ connection")
        except Exception as e:
            logger.error(f"Error closing RabbitMQ connection: {e}", exc_info=True)
        self.is_consuming = False

    async def purge_queue(self):
        """Purge all messages from the queue."""
        try:
            if not self.connection or self.connection.is_closed or not self.channel or self.channel.is_closed:
                await self.connect()
            queue = await self.channel.get_queue(self.queue_name)
            purge_count = await queue.purge()
            logger.info(f"Purged {purge_count} messages from queue: {self.queue_name}")
            return purge_count
        except Exception as e:
            logger.error(f"Error purging queue {self.queue_name}: {e}", exc_info=True)
            raise

    async def start_consuming(self):
        """Start consuming messages with robust reconnection."""
        try:
            await self.connect()
            self.is_consuming = True
            queue = await self.channel.get_queue(self.queue_name)
            await queue.consume(self.callback)
            logger.info("Started consuming messages. To exit press CTRL+C")
            await asyncio.Event().wait()  # Keep running until interrupted
        except (aio_pika.exceptions.AMQPConnectionError, aio_pika.exceptions.ChannelClosed, asyncio.TimeoutError) as e:
            logger.error(f"Connection error: {e}, reconnecting in 5 seconds", exc_info=True)
            self.is_consuming = False
            await self.close()
            await asyncio.sleep(5)
            await self.start_consuming()
        except KeyboardInterrupt:
            logger.info("Received KeyboardInterrupt, shutting down consumer")
            await self.close()
        except Exception as e:
            logger.error(f"Unexpected error in consumer: {e}, reconnecting in 5 seconds", exc_info=True)
            self.is_consuming = False
            await self.close()
            await asyncio.sleep(5)
            await self.start_consuming()

    async def callback(self, message: aio_pika.IncomingMessage):
        """Process incoming messages asynchronously."""
        async with message.process(requeue=True, ignore_processed=True):
            try:
                task = json.loads(message.body.decode())
                file_id = task.get("file_id", "unknown")
                task_type = task.get("task_type", "unknown")
                response_queue = task.get("response_queue")
                logger.info(
                    f"Received task for FileID: {file_id}, TaskType: {task_type}, "
                    f"CorrelationID: {message.correlation_id}, Task: {json.dumps(task)[:200]}"
                )

                async with asyncio.timeout(self.operation_timeout):
                    if task_type == "select_deduplication":
                        async with async_engine.connect() as conn:
                            result = await self.execute_select(task, conn, logger)
                            response = {"file_id": file_id, "results": result}
                            if response_queue:
                                producer = RabbitMQProducer(amqp_url=self.amqp_url)
                                try:
                                    await producer.connect()
                                    await producer.publish_message(
                                        response,
                                        routing_key=response_queue,
                                        correlation_id=file_id,
                                    )
                                    logger.info(
                                        f"Sent {len(result)} deduplication results to {response_queue} "
                                        f"for FileID: {file_id}"
                                    )
                                finally:
                                    await producer.close()
                        success = True
                    elif task_type == "update_sort_order" and task.get("sql") == "UPDATE_SORT_ORDER":
                        success = await self.execute_sort_order_update(task.get("params", {}), file_id)
                    else:
                        async with async_engine.begin() as conn:
                            result = await self.execute_update(task, conn, logger)
                            success = True

                if success:
                    await message.ack()
                    logger.info(f"Successfully processed {task_type} for FileID: {file_id}, Acknowledged")
                else:
                    logger.warning(f"Failed to process {task_type} for FileID: {file_id}; re-queueing")
                    await message.nack(requeue=True)
                    await asyncio.sleep(2)
            except asyncio.TimeoutError:
                logger.error(f"Timeout processing message for FileID: {file_id}, TaskType: {task_type}")
                await message.nack(requeue=True)
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(
                    f"Error processing message for FileID: {file_id}, TaskType: {task_type}: {e}",
                    exc_info=True
                )
                await message.nack(requeue=True)
                await asyncio.sleep(2)

async def test_task(consumer: RabbitMQConsumer, task: dict):
    file_id = task.get("file_id", "unknown")
    task_type = task.get("task_type", "unknown")
    logger.info(f"Testing task for FileID: {file_id}, TaskType: {task_type}")
    try:
        async with asyncio.timeout(consumer.operation_timeout):
            if task_type == "select_deduplication":
                async with async_engine.connect() as conn:
                    success = await consumer.execute_select(task, conn, logger)
            elif task_type == "update_sort_order" and task.get("sql") == "UPDATE_SORT_ORDER":
                success = await consumer.execute_sort_order_update(task.get("params", {}), file_id)
            else:
                async with async_engine.begin() as conn:
                    success = await consumer.execute_update(task, conn, logger)
            logger.info(f"Test task result for FileID: {file_id}, TaskType: {task_type}: {'Success' if success else 'Failed'}")
            return success
    except asyncio.TimeoutError:
        logger.error(f"Timeout testing task for FileID: {file_id}, TaskType: {task_type}")
        return False
    except Exception as e:
        logger.error(f"Error testing task for FileID: {file_id}, TaskType: {task_type}: {e}", exc_info=True)
        return False

def signal_handler(consumer: RabbitMQConsumer):
    def handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down gracefully...")
        loop = asyncio.get_event_loop()
        loop.run_until_complete(consumer.close())
        loop.run_until_complete(async_engine.dispose())
        sys.exit(0)
    return handler

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="RabbitMQ Consumer with manual queue clear")
    parser.add_argument("--clear-queue", action="store_true", help="Manually clear the queue and exit")
    args = parser.parse_args()

    consumer = RabbitMQConsumer()
    signal.signal(signal.SIGINT, signal_handler(consumer))
    signal.signal(signal.SIGTERM, signal_handler(consumer))

    if args.clear_queue:
        try:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(consumer.purge_queue())
            logger.info("Queue cleared successfully. Exiting.")
            sys.exit(0)
        except Exception as e:
            logger.error(f"Failed to clear queue: {e}", exc_info=True)
            sys.exit(1)

    sample_task = {
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
        loop.run_until_complete(test_task(consumer, sample_task))
        loop.run_until_complete(consumer.start_consuming())
    except KeyboardInterrupt:
        loop.run_until_complete(consumer.close())
    except Exception as e:
        logger.error(f"Error in consumer: {e}", exc_info=True)
        loop.run_until_complete(consumer.close())