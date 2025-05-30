import aio_pika
import json
import logging
import asyncio
import signal
import sys
import datetime
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from database_config import async_engine
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import psutil 
from typing import Optional, Dict, Any
from rabbitmq_producer import RabbitMQProducer
import aiormq.exceptions  # Add this import at the top of rabbitmq_consumer.py
import uuid
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class RabbitMQConsumer:
    def __init__(
        self,
        amqp_url: str = "amqp://app_user:app_password@localhost:5672/app_vhost",
        queue_name: str = "db_update_queue",
        connection_timeout: float = 10.0,
        operation_timeout: float = 5.0,
    ):
        """
        Initialize RabbitMQ consumer with connection parameters.
        
        Args:
            amqp_url: RabbitMQ connection URL.
            queue_name: Name of the queue to consume from.
            connection_timeout: Timeout for establishing connection (seconds).
            operation_timeout: Timeout for processing messages (seconds).
        """
        self.amqp_url = amqp_url
        self.queue_name = queue_name
        self.connection_timeout = connection_timeout
        self.operation_timeout = operation_timeout
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.RobustChannel] = None
        self.is_consuming = False
        self._lock = asyncio.Lock()

    async def connect(self):
        """Establish a robust connection to RabbitMQ and declare the queue."""
        async with self._lock:
            if self.connection and not self.connection.is_closed:
                logger.debug("RabbitMQ connection already established")
                return
            try:
                async with asyncio.timeout(self.connection_timeout):
                    self.connection = await aio_pika.connect_robust(
                        self.amqp_url,
                        connection_attempts=3,
                        retry_delay=5,
                    )
                    self.channel = await self.connection.channel()
                    await self.channel.set_qos(prefetch_count=1)  # Process one message at a time
                    await self.channel.declare_queue(self.queue_name, durable=True)
                    logger.info(f"Connected to RabbitMQ, consuming from queue: {self.queue_name}")
            except asyncio.TimeoutError:
                logger.error("Timeout connecting to RabbitMQ")
                raise
            except aio_pika.exceptions.ProbableAuthenticationError as e:
                logger.error(
                    f"Authentication error connecting to RabbitMQ: {e}. "
                    f"Check username, password, and virtual host in {self.amqp_url}.",
                    exc_info=True
                )
                raise
            except aio_pika.exceptions.AMQPConnectionError as e:
                logger.error(f"Failed to connect to RabbitMQ: {e}", exc_info=True)
                raise
            except aio_pika.exceptions.ChannelInvalidStateError as e:
                logger.error(
                    f"Channel error connecting to RabbitMQ: {e}. "
                    f"Ensure channel is properly configured.",
                    exc_info=True
                )
                raise

    async def close(self):
        """Close the RabbitMQ connection and channel gracefully."""
        async with self._lock:
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

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=10),
        retry=retry_if_exception_type((
            aio_pika.exceptions.AMQPError,
            aio_pika.exceptions.ChannelClosed,
            asyncio.TimeoutError
        )),
        before_sleep=lambda retry_state: logger.info(
            f"Retrying start_consuming (attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
        )
    )
    async def start_consuming(self):
        """Start consuming messages with robust reconnection."""
        try:
            await self.connect()
            self.is_consuming = True
            queue = await self.channel.get_queue(self.queue_name)
            await queue.consume(self.callback)
            logger.info("Started consuming messages. To exit press CTRL+C")
            await asyncio.Event().wait()
        except (aio_pika.exceptions.AMQPConnectionError, aio_pika.exceptions.ChannelClosed, asyncio.TimeoutError) as e:
            logger.error(f"Connection error: {e}, reconnecting in 5 seconds", exc_info=True)
            self.is_consuming = False
            await self.close()
            await asyncio.sleep(5)
            await self.start_consuming()
        except aio_pika.exceptions.ProbableAuthenticationError as e:
            logger.error(
                f"Authentication error: {e}. "
                f"Check username, password, and virtual host in {self.amqp_url}.",
                exc_info=True
            )
            self.is_consuming = False
            await self.close()
            raise
        except aio_pika.exceptions.ChannelInvalidStateError as e:
            logger.error(
                f"Channel error: {e}. "
                f"Ensure channel is properly configured.",
                exc_info=True
            )
            self.is_consuming = False
            await self.close()
            raise
        except KeyboardInterrupt:
            logger.info("Received KeyboardInterrupt, shutting down consumer")
            await self.close()
            raise
        except Exception as e:
            logger.error(f"Unexpected error in consumer: {e}, reconnecting in 5 seconds", exc_info=True)
            self.is_consuming = False
            await self.close()
            await asyncio.sleep(5)
            await self.start_consuming()

    async def execute_update(self, task: Dict[str, Any], conn, logger: logging.Logger) -> Dict[str, Any]:
        """Execute an UPDATE or INSERT query."""
        file_id = task.get("file_id", "unknown")
        task_type = task.get("task_type", "unknown")
        try:
            sql = task.get("sql")
            params = task.get("params", {})
            if not isinstance(params, dict):
                raise ValueError(f"Invalid params format: {params}, expected dict")
            logger.debug(f"Executing UPDATE/INSERT for FileID {file_id}: {sql[:100]}, params: {params}")
            result = await conn.execute(text(sql), params)
            await conn.commit()
            rowcount = result.rowcount if result.rowcount is not None else 0
            logger.info(f"Worker PID {psutil.Process().pid}: {task_type} affected {rowcount} rows for FileID {file_id}")
            return {"rowcount": rowcount}
        except SQLAlchemyError as e:
            logger.error(
                f"TaskType: {task_type}, FileID: {file_id}, "
                f"Database error executing UPDATE/INSERT: {sql[:100]}, params: {params}, error: {str(e)}",
                exc_info=True
            )
            raise
        except Exception as e:
            logger.error(
                f"TaskType: {task_type}, FileID: {file_id}, "
                f"Unexpected error executing UPDATE/INSERT: {sql[:100]}, params: {params}, error: {str(e)}",
                exc_info=True
            )
            raise

    async def execute_select(self, task: Dict[str, Any], conn, logger: logging.Logger) -> Dict[str, Any]:
        """Execute a SELECT query and optionally send results to a response queue."""
        file_id = task.get("file_id", "unknown")
        select_sql = task.get("sql")
        params = task.get("params", {})
        response_queue = task.get("response_queue", "shared_response_queue")  # Fallback to shared queue
        correlation_id = task.get("correlation_id")
        logger.debug(f"Executing SELECT for FileID {file_id}: {select_sql[:100]}, params: {params}")
        if not params:
            raise ValueError(f"No parameters provided for SELECT query: {select_sql}")
        try:
            result = await conn.execute(text(select_sql), params)
            rows = result.fetchall()
            columns = result.keys()
            result.close()
            results = [dict(zip(columns, row)) for row in rows]
            logger.info(f"Worker PID {psutil.Process().pid}: SELECT returned {len(results)} rows for FileID {file_id}")
            if response_queue:
                producer = RabbitMQProducer(amqp_url=self.amqp_url)
                try:
                    await producer.connect()
                    response = {"file_id": file_id, "results": results, "correlation_id": correlation_id}
                    @retry(
                        stop=stop_after_attempt(3),
                        wait=wait_exponential(multiplier=1, min=2, max=10),
                        retry=retry_if_exception_type(aiormq.exceptions.ChannelLockedResource)
                    )
                    async def publish_with_retry():
                        await producer.publish_message(
                            response,
                            routing_key=response_queue,
                            correlation_id=correlation_id
                        )
                        logger.debug(f"Sent {len(results)} SELECT results to {response_queue} for FileID {file_id}")
                    
                    await publish_with_retry()
                except aiormq.exceptions.ChannelLockedResource as e:
                    logger.error(f"Failed to publish to {response_queue} after retries: {e}", exc_info=True)
                    raise
                finally:
                    await producer.close()
            return {"results": results}
        except SQLAlchemyError as e:
            logger.error(f"Database error executing SELECT for FileID {file_id}: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error executing SELECT for FileID {file_id}: {e}", exc_info=True)
            raise

    async def execute_sort_order_update(self, params: Dict[str, Any], file_id: str) -> bool:
        """Execute an update_sort_order task."""
        try:
            entry_id = params.get("entry_id")
            result_id = params.get("result_id")
            sort_order = params.get("sort_order")
            if not all([entry_id, result_id, sort_order is not None]):
                logger.error(
                    f"Invalid parameters for update_sort_order task, FileID: {file_id}. "
                    f"Required: entry_id, result_id, sort_order. Got: {params}"
                )
                return False
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
                    f"TaskType: update_sort_order, FileID: {file_id}, "
                    f"Updated SortOrder for EntryID: {entry_id}, ResultID: {result_id}, affected {rowcount} rows"
                )
                return rowcount > 0
        except SQLAlchemyError as e:
            logger.error(f"Database error updating SortOrder for FileID: {file_id}, EntryID: {entry_id}: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Unexpected error updating SortOrder for FileID: {file_id}, EntryID: {entry_id}: {e}", exc_info=True)
            return False

    async def callback(self, message: aio_pika.IncomingMessage):
        """Process incoming messages from the queue."""
        async with self._lock:
            # Ensure channel is valid; reconnect if necessary
            if not self.channel or self.channel.is_closed:
                logger.warning("Channel is closed or invalid, attempting to reconnect")
                await self.connect()
        
        async with message.process(requeue=True, ignore_processed=True):
            try:
                task = json.loads(message.body.decode())
                file_id = task.get("file_id", "unknown")
                task_type = task.get("task_type", "unknown")
                correlation_id = message.correlation_id
                logger.info(
                    f"Worker PID {psutil.Process().pid}: Received task for FileID: {file_id}, "
                    f"TaskType: {task_type}, CorrelationID: {correlation_id}, Task: {json.dumps(task)[:200]}"
                )
                async with asyncio.timeout(self.operation_timeout):
                    success = False
                    if task_type == "select_deduplication":
                        async with async_engine.connect() as conn:
                            await self.execute_select(task, conn, logger)
                            success = True
                    elif task_type == "update_sort_order":
                        success = await self.execute_sort_order_update(task.get("params", {}), file_id)
                    else:
                        async with async_engine.begin() as conn:
                            await self.execute_update(task, conn, logger)
                            success = True
                    if success:
                        await message.ack()
                        logger.info(f"Successfully processed {task_type} for FileID: {file_id}, Acknowledged")
                    else:
                        logger.warning(f"Failed to process {task_type} for FileID: {file_id}, Re-queueing")
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

    async def test_task(self, task: Dict[str, Any]) -> bool:
        """Test a task without consuming from the queue."""
        file_id = task.get("file_id", "unknown")
        task_type = task.get("task_type", "unknown")
        logger.info(f"Testing task for FileID: {file_id}, TaskType: {task_type}")
        try:
            async with asyncio.timeout(self.operation_timeout):
                if task_type == "select_deduplication":
                    async with async_engine.connect() as conn:
                        result = await self.execute_select(task, conn, logger)
                        success = bool(result["results"])
                elif task_type == "update_sort_order":
                    success = await self.execute_sort_order_update(task.get("params", {}), file_id)
                else:
                    async with async_engine.begin() as conn:
                        result = await self.execute_update(task, conn, logger)
                        success = result["rowcount"] > 0
                logger.info(f"Test task result for FileID: {file_id}, TaskType: {task_type}: {'Success' if success else 'Failed'}")
                return success
        except asyncio.TimeoutError:
            logger.error(f"Timeout testing task for FileID: {file_id}, TaskType: {task_type}")
            return False
        except Exception as e:
            logger.error(f"Error testing task for FileID: {file_id}, TaskType: {task_type}: {e}", exc_info=True)
            return False

async def shutdown(consumer: RabbitMQConsumer, loop: asyncio.AbstractEventLoop):
    """Perform async shutdown of consumer and resources."""
    try:
        await consumer.close()
        await async_engine.dispose()
        # Cancel all running tasks except the current one
        tasks = [task for task in asyncio.all_tasks(loop) if task is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        # Wait for tasks to complete or cancel
        await asyncio.gather(*tasks, return_exceptions=True)
        # Shutdown async generators
        await loop.shutdown_asyncgens()
    except Exception as e:
        logger.error(f"Shutdown error: {e}", exc_info=True)
    finally:
        loop.stop()
        loop.close()

def signal_handler(consumer: RabbitMQConsumer, loop: asyncio.AbstractEventLoop):
    """Create a signal handler for graceful shutdown."""
    def handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down gracefully...")
        # Ensure shutdown is awaited
        task = asyncio.ensure_future(shutdown(consumer, loop))
        loop.run_until_complete(task)
        exit(0)
    return handler

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="RabbitMQ Consumer with manual queue clear")
    parser.add_argument("--clear-queue", action="store_true", help="Manually clear the queue and exit")
    args = parser.parse_args()

    consumer = RabbitMQConsumer()
    loop = asyncio.get_event_loop()
    signal.signal(signal.SIGINT, signal_handler(consumer, loop))
    signal.signal(signal.SIGTERM, signal_handler(consumer, loop))

    if args.clear_queue:
        try:
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

    async def main():
        await consumer.test_task(sample_task)
        await consumer.start_consuming()

    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, initiating shutdown")
        loop.run_until_complete(shutdown(consumer, loop))
    except Exception as e:
        logger.error(f"Error in consumer: {e}", exc_info=True)
        loop.run_until_complete(shutdown(consumer, loop))
    finally:
        if not loop.is_closed():
            loop.close()