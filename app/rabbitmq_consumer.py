import aio_pika
import json
import logging
import asyncio
import signal
import sys
import datetime
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
import aiormq
from database_config import async_engine
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import psutil
from typing import Optional, Dict, Any
from rabbitmq_producer import RabbitMQProducer
import uuid
from config import RABBITMQ_URL

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class RabbitMQConsumer:
    def __init__(
        self,
        amqp_url: str = RABBITMQ_URL,
        queue_name: str = "db_update_queue",
        connection_timeout: float = 10.0,
        operation_timeout: float = 5.0,
        producer: Optional[RabbitMQProducer] = None
    ):
        self.amqp_url = amqp_url
        self.queue_name = queue_name
        self.new_queue_name = "new_task_queue"
        self.response_queue_name = "shared_response_queue"
        self.connection_timeout = connection_timeout
        self.operation_timeout = operation_timeout
        self.connection = None
        self.channel = None
        self.is_consuming = False
        self._lock = asyncio.Lock()
        self.producer = producer or RabbitMQProducer(amqp_url=amqp_url)

    async def check_queue_durability(self, channel, queue_name: str, expected_durable: bool, delete_if_mismatched: bool = False) -> bool:
        try:
            queue = await channel.declare_queue(queue_name, passive=True)
            is_durable = queue.declaration_result.fields.get('durable', False)
            logger.debug(f"Queue {queue_name} exists with durable={is_durable}, arguments={queue.declaration_result.fields}")
            if is_durable != expected_durable:
                logger.warning(f"Queue {queue_name} has durable={is_durable}, but expected durable={expected_durable}.")
                if delete_if_mismatched:
                    try:
                        await channel.queue_delete(queue_name)
                        logger.info(f"Deleted queue {queue_name} due to mismatched durability")
                        return True  # Safe to recreate
                    except Exception as e:
                        logger.error(f"Failed to delete queue {queue_name}: {e}", exc_info=True)
                        return False
                return False
            return True
        except (aio_pika.exceptions.QueueEmpty, aiormq.exceptions.ChannelNotFoundEntity):
            logger.debug(f"Queue {queue_name} does not exist")
            return True  # Safe to create
        except Exception as e:
            logger.error(f"Error checking queue {queue_name} durability: {e}", exc_info=True)
            return False

    async def connect(self):
        async with self._lock:
            if self.connection and not self.connection.is_closed and self.channel and not self.channel.is_closed:
                logger.debug("RabbitMQ connection already established")
                return
            try:
                async with asyncio.timeout(self.connection_timeout):
                    if self.connection and not self.connection.is_closed:
                        await self.connection.close()
                    self.connection = await aio_pika.connect_robust(
                        self.amqp_url,
                        connection_attempts=3,
                        retry_delay=5,
                    )
                    self.channel = await self.connection.channel()
                    await self.channel.set_qos(prefetch_count=1)

                    queues = [
                        (self.queue_name, True),
                        (self.response_queue_name, True),
                        (self.new_queue_name, True),
                    ]
                    for queue_name, durable in queues:
                        if not await self.check_queue_durability(self.channel, queue_name, durable, delete_if_mismatched=args.delete_mismatched):
                            raise ValueError(
                                f"Queue {queue_name} has mismatched durability settings. "
                                f"Delete the queue in RabbitMQ (http://localhost:15672, vhost app_vhost) or "
                                f"run with --delete-mismatched to auto-delete."
                            )
                        queue = await self.channel.declare_queue(queue_name, durable=durable)
                        await queue.bind('logs')
                        logger.debug(f"Queue {queue_name} declared and bound to logs exchange")

                    logger.info(f"Connected to RabbitMQ, consuming from queues: {self.queue_name}, {self.new_queue_name}")
            except (asyncio.TimeoutError, aio_pika.exceptions.AMQPConnectionError) as e:
                logger.error(f"Failed to connect to RabbitMQ: {e}", exc_info=True)
                raise

    async def close(self):
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
        try:
            await self.connect()
            self.is_consuming = True
            queue = await self.channel.get_queue(self.queue_name)
            await queue.consume(self.callback)
            new_queue = await self.channel.get_queue(self.new_queue_name)
            await new_queue.consume(self.callback)
            logger.info(f"Started consuming messages from {self.queue_name} and {self.new_queue_name}. To exit press CTRL+C")
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            logger.info("Consumer cancelled, shutting down...")
            self.is_consuming = False
            await self.close()
            raise
        except (aio_pika.exceptions.AMQPConnectionError, aio_pika.exceptions.ChannelClosed, asyncio.TimeoutError) as e:
            logger.error(f"Connection error: {e}, attempting retry...", exc_info=True)
            self.is_consuming = False
            await self.close()
            raise
        except aio_pika.exceptions.ProbableAuthenticationError as e:
            logger.error(
                f"Authentication error: {e}. Check username, password, and virtual host in {self.amqp_url}.",
                exc_info=True
            )
            self.is_consuming = False
            await self.close()
            raise
        except aio_pika.exceptions.ChannelInvalidStateError as e:
            logger.error(f"Channel invalid state: {e}. Reconnecting...", exc_info=True)
            self.is_consuming = False
            await self.close()
            await asyncio.sleep(2)
            raise
        except KeyboardInterrupt:
            logger.info("Received KeyboardInterrupt, shutting down consumer")
            self.is_consuming = False
            await self.close()
            raise
        except Exception as e:
            logger.error(f"Unexpected error in consumer: {e}", exc_info=True)
            self.is_consuming = False
            await self.close()
            await asyncio.sleep(2)
            raise

    async def execute_update(self, task: Dict[str, Any], conn, logger: logging.Logger) -> Dict[str, Any]:
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
            await conn.rollback()
            raise
        except Exception as e:
            logger.error(
                f"TaskType: {task_type}, FileID: {file_id}, "
                f"Unexpected error executing UPDATE/INSERT: {sql[:100]}, params: {params}, error: {str(e)}",
                exc_info=True
            )
            await conn.rollback()
            raise
        finally:
            if hasattr(conn, 'close'):
                await conn.close()

    async def execute_select(self, task: Dict[str, Any], conn, logger: logging.Logger) -> Dict[str, Any]:
        file_id = task.get("file_id", "unknown")
        select_sql = task.get("sql")
        params = task.get("params", {})
        response_queue = task.get("response_queue", self.producer.response_queue_name)
        correlation_id = task.get("correlation_id")
        logger.debug(f"Executing SELECT for FileID {file_id}: {select_sql[:100]}, params: {params}, response_queue: {response_queue}")
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
                try:
                    await self.producer.check_connection()
                    response = {
                        "file_id": file_id,
                        "results": results,
                        "correlation_id": correlation_id,
                        "result": len(results)
                    }
                    @retry(
                        stop=stop_after_attempt(3),
                        wait=wait_exponential(multiplier=1, min=2, max=10),
                        retry=retry_if_exception_type(aio_pika.exceptions.AMQPError)
                    )
                    async def publish_with_retry():
                        await self.producer.publish_message(
                            response,
                            routing_key=response_queue,
                            correlation_id=correlation_id
                        )
                        logger.debug(f"Sent {len(results)} SELECT results to {response_queue} for FileID {file_id}")
                    
                    await publish_with_retry()
                except Exception as e:
                    logger.error(f"Failed to publish SELECT results for FileID {file_id}: {e}", exc_info=True)
                    raise
            return {"results": results}
        except SQLAlchemyError as e:
            logger.error(f"Database error executing SELECT for FileID {file_id}: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error executing SELECT for FileID {file_id}: {e}", exc_info=True)
            raise
        finally:
            if hasattr(conn, 'close'):
                await conn.close()

    async def execute_sort_order_update(self, params: Dict[str, Any], file_id: str) -> bool:
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
            if 'conn' in locals() and hasattr(conn, 'rollback'):
                await conn.rollback()
            return False
        except Exception as e:
            logger.error(f"Unexpected error updating SortOrder for FileID: {file_id}, EntryID: {entry_id}: {e}", exc_info=True)
            if 'conn' in locals() and hasattr(conn, 'rollback'):
                await conn.rollback()
            return False

    async def execute_new_task(self, task: Dict[str, Any], logger: logging.Logger) -> bool:
        file_id = task.get("file_id", "unknown")
        try:
            logger.info(f"Processing new_task for FileID {file_id}: {task}")
            async with async_engine.connect() as conn:
                result = await conn.execute(text("SELECT 1 AS test"))
                row = result.fetchone()
                logger.info(f"New task test query returned: {row}")
                return True
        except Exception as e:
            logger.error(f"Error executing new_task for FileID {file_id}: {e}", exc_info=True)
            return False

    async def callback(self, message: aio_pika.IncomingMessage):
        async with self._lock:
            for attempt in range(3):
                if not self.channel or self.channel.is_closed:
                    logger.warning(f"Channel is closed, attempting reconnect (attempt {attempt + 1}/3)")
                    try:
                        await self.connect()
                        break
                    except Exception as e:
                        logger.error(f"Reconnect attempt {attempt + 1} failed: {e}", exc_info=True)
                        if attempt == 2:
                            await message.nack(requeue=True)
                            return
                        await asyncio.sleep(2)
        
        async with message.process(requeue=True, ignore_processed=True):
            try:
                task = json.loads(message.body.decode())
                file_id = task.get("file_id", "unknown")
                task_type = task.get("task_type", "unknown")
                correlation_id = message.correlation_id
                logger.info(
                    f"Worker PID {psutil.Process().pid}: Received task for FileID: {file_id}, "
                    f"TaskType: {task_type}, CorrelationID: {correlation_id}"
                )
                async with asyncio.timeout(self.operation_timeout):
                    success = False
                    if task_type == "select_deduplication":
                        async with async_engine.connect() as conn:
                            await self.execute_select(task, conn, logger)
                            success = True
                    elif task_type == "update_sort_order":
                        success = await self.execute_sort_order_update(task.get("params", {}), file_id)
                    elif task_type == "new_task":
                        success = await self.execute_new_task(task, logger)
                    else:
                        async with async_engine.begin() as conn:
                            await self.execute_update(task, conn, logger)
                            success = True
                    if success:
                        await message.ack()
                        logger.info(f"Successfully processed {task_type} for FileID: {file_id}")
                    else:
                        await message.nack(requeue=True)
                        await asyncio.sleep(2)
            except asyncio.TimeoutError:
                logger.error(f"Timeout processing message for FileID: {file_id}, TaskType: {task_type}")
                await message.nack(requeue=True)
                await asyncio.sleep(2)
            except (aio_pika.exceptions.ChannelClosed, aiormq.exceptions.ConnectionChannelError) as e:
                logger.error(f"Channel error processing message for FileID: {file_id}: {e}", exc_info=True)
                await self.connect()
                await message.nack(requeue=True)
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Error processing message for FileID: {file_id}, TaskType: {task_type}: {e}", exc_info=True)
                await message.nack(requeue=True)
                await asyncio.sleep(2)

    async def test_task(self, task: Dict[str, Any]) -> bool:
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
                elif task_type == "new_task":
                    success = await self.execute_new_task(task, logger)
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

def signal_handler(consumer: RabbitMQConsumer, loop: asyncio.AbstractEventLoop):
    import time
    last_signal_time = 0
    debounce_interval = 1.0

    def handler(sig, frame):
        nonlocal last_signal_time
        current_time = time.time()
        if current_time - last_signal_time < debounce_interval:
            logger.debug("Debouncing rapid SIGINT/SIGTERM")
            return
        last_signal_time = current_time
        logger.info(f"Received signal {sig}, shutting down gracefully...")
        if not loop.is_closed():
            loop.call_soon_threadsafe(lambda: asyncio.create_task(shutdown(consumer, loop)))

    return handler

async def shutdown(consumer: RabbitMQConsumer, loop: asyncio.AbstractEventLoop):
    logger.info("Initiating shutdown...")
    try:
        tasks = [task for task in asyncio.all_tasks(loop) if task is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                logger.debug(f"Task {task.get_name()} cancelled or timed out during shutdown")
        await consumer.close()
        await consumer.producer.close()
        if loop.is_running():
            loop.stop()
        await loop.shutdown_asyncgens()
        await loop.shutdown_default_executor()
    except Exception as e:
        logger.error(f"Error during shutdown: {e}", exc_info=True)
    finally:
        if not loop.is_closed():
            try:
                loop.close()
                logger.info("Event loop closed")
            except Exception as e:
                logger.error(f"Error closing event loop: {e}", exc_info=True)
        logger.info("Shutdown complete.")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="RabbitMQ Consumer with queue management")
    parser.add_argument("--clear", action="store_true", help="Purge the main queue and exit")
    parser.add_argument("--delete-mismatched", action="store_true", help="Delete queues with mismatched durability settings")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    logger = logging.getLogger(__name__)

    producer = RabbitMQProducer()
    consumer = RabbitMQConsumer(producer=producer)
    loop = asyncio.get_event_loop()

    try:
        signal.signal(signal.SIGINT, signal_handler(consumer, loop))
        signal.signal(signal.SIGTERM, signal_handler(consumer, loop))
    except ValueError as e:
        logger.warning(f"Could not set signal handlers: {e}")

    async def main():
        try:
            if args.clear:
                logger.info("Clearing queue...")
                await consumer.purge_queue()
                logger.info("Queue cleared successfully")
                return
            logger.info("Starting consumer...")
            await consumer.start_consuming()
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt in main, shutting down...")
            await shutdown(consumer, loop)
        except SystemExit as e:
            logger.info(f"Exiting with code {e.code}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in main: {e}", exc_info=True)
            await shutdown(consumer, loop)

    try:
        loop.run_until_complete(main())
    except SystemExit:
        pass
    except RuntimeError as e:
        logger.error(f"Error running main: {e}", exc_info=True)
    finally:
        try:
            if not loop.is_closed():
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.run_until_complete(loop.shutdown_default_executor())
                loop.close()
        except Exception as e:
            logger.error(f"Error during final cleanup: {e}", exc_info=True)