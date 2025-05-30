import aio_pika
import json
import logging
import asyncio
import signal
import sys
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
import aiormq
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import psutil
from typing import Optional, Dict, Any
import uuid
from database_config import async_engine
from config import RABBITMQ_URL
from rabbitmq_producer import RabbitMQProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class RabbitMQConsumer:
    def __init__(
        self,
        amqp_url: str = RABBITMQ_URL,
        queue_name: str = "db_update_queue",
        connection_timeout: float = 60.0,
        operation_timeout: float = 15.0,
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

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((aio_pika.exceptions.AMQPError, aiormq.exceptions.ChannelInvalidStateError)),
        before_sleep=lambda retry_state: logger.info(
            f"Retrying connect (attempt {retry_state.attempt_number}/5) after {retry_state.next_action.sleep}s"
        )
    )
    async def connect(self):
        async with self._lock:
            try:
                await self.close()
                self.connection = await aio_pika.connect_robust(
                    self.amqp_url,
                    timeout=self.connection_timeout,
                    loop=asyncio.get_running_loop(),
                    connection_attempts=10,
                    retry_delay=5
                )
                self.channel = await self.connection.channel()
                await self.channel.set_qos(prefetch_count=1)
                await self.channel.declare_exchange('logs', aio_pika.ExchangeType.FANOUT)
                for queue_name in [self.queue_name, self.new_queue_name, self.response_queue_name]:
                    queue = await self.channel.declare_queue(queue_name, durable=True)
                    await queue.bind('logs')
                    logger.debug(f"Declared and bound queue {queue_name} to logs exchange")
                logger.info("Successfully connected to RabbitMQ")
            except aio_pika.exceptions.AMQPError as e:
                logger.error(f"Failed to connect to RabbitMQ: {e}", exc_info=True)
                await self.close()
                raise
            except Exception as e:
                logger.error(f"Unexpected error during connection: {e}", exc_info=True)
                await self.close()
                raise

    async def close(self):
        async with self._lock:
            try:
                if self.is_consuming:
                    self.is_consuming = False
                    logger.info("Stopped consuming messages")
                if self.channel and not self.channel.is_closed:
                    await asyncio.wait_for(self.channel.close(), timeout=5.0)
                    logger.info("Closed RabbitMQ channel")
                if self.connection and not self.connection.is_closed:
                    await asyncio.wait_for(self.connection.close(), timeout=5.0)
                    logger.info("Closed RabbitMQ connection")
            except asyncio.TimeoutError:
                logger.warning("Timeout closing RabbitMQ resources")
            except Exception as e:
                logger.error(f"Error closing RabbitMQ: {e}", exc_info=True)
            finally:
                self.channel = None
                self.connection = None
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
            asyncio.TimeoutError,
            aiormq.exceptions.ChannelInvalidStateError
        ))
    )
    async def start_consuming(self):
        try:
            await self.connect()
            self.is_consuming = True
            for queue_name in [self.queue_name, self.new_queue_name, self.response_queue_name]:
                queue = await self.channel.get_queue(queue_name)
                await queue.consume(self.callback)
                logger.info(f"Consuming from {queue_name}")
            logger.info(f"Started consuming messages from {self.queue_name}, {self.new_queue_name}, and {self.response_queue_name}")
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            logger.info("Consumer cancelled, shutting down...")
            await self.close()
            raise
        except Exception as e:
            logger.error(f"Error in start_consuming: {e}", exc_info=True)
            await self.close()
            raise

    async def callback(self, message: aio_pika.IncomingMessage):
        async with self._lock:
            if not self.channel or self.channel.is_closed:
                logger.warning("Channel closed, attempting reconnect")
                for attempt in range(3):
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
                    f"TaskType: {task_type}, CorrelationID: {correlation_id}, Queue: {message.routing_key}"
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
                    elif task_type == "insert_search_result":
                        async with async_engine.begin() as conn:
                            await self.execute_update(task, conn, logger)
                            success = True
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
            except Exception as e:
                logger.error(f"Error processing message for FileID: {file_id}, TaskType: {task_type}: {e}", exc_info=True)
                await message.nack(requeue=True)
                await asyncio.sleep(2)

    async def execute_update(self, task: Dict[str, Any], conn, logger: logging.Logger) -> Dict[str, Any]:
        file_id = task.get("file_id", "unknown")
        task_type = task.get("task_type", "unknown")
        try:
            sql = task.get("sql")
            params = task.get("params", {})
            if not sql:
                raise ValueError("SQL statement is missing")
            if not isinstance(params, dict):
                raise ValueError(f"Invalid params format: {params}, expected dict")
            logger.debug(f"Executing UPDATE/INSERT for FileID {file_id}: {sql[:100]}, params: {params}")
            result = await conn.execute(text(sql), params)
            await conn.commit()
            rowcount = result.rowcount if result.rowcount is not None else 0
            logger.info(f"Worker PID {psutil.Process().pid}: {task_type} affected {rowcount} rows for FileID {file_id}")
            return {"rowcount": rowcount}
        except SQLAlchemyError as e:
            logger.error(f"SQLAlchemyError for FileID {file_id}, TaskType {task_type}: {e}", exc_info=True)
            await conn.rollback()
            raise
        except Exception as e:
            logger.error(f"Unexpected error for FileID {file_id}, TaskType {task_type}: {e}", exc_info=True)
            await conn.rollback()
            raise

    async def execute_select(self, task: Dict[str, Any], conn, logger: logging.Logger) -> Dict[str, Any]:
        file_id = task.get("file_id", "unknown")
        select_sql = task.get("sql")
        params = task.get("params", {})
        response_queue = task.get("response_queue", self.producer.response_queue_name)
        correlation_id = task.get("correlation_id")
        logger.debug(f"Executing SELECT for FileID {file_id}: {select_sql[:100]}, params: {params}")
        try:
            result = await conn.execute(text(select_sql), params)
            rows = result.fetchall()
            columns = result.keys()
            results = [dict(zip(columns, row)) for row in rows]
            logger.info(f"Worker PID {psutil.Process().pid}: SELECT returned {len(results)} rows for FileID {file_id}")
            if response_queue:
                response = {
                    "file_id": file_id,
                    "results": results,
                    "correlation_id": correlation_id,
                    "result": len(results)
                }
                await self.producer.publish_message(response, routing_key='', correlation_id=correlation_id)
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
                logger.error(f"Invalid parameters for update_sort_order task, FileID: {file_id}")
                return False
            sql = """
                UPDATE utb_ImageScraperResult
                SET SortOrder = :sort_order
                WHERE EntryID = :entry_id AND ResultID = :result_id
            """
            async with async_engine.begin() as conn:
                result = await conn.execute(text(sql), {"sort_order": sort_order, "entry_id": entry_id, "result_id": result_id})
                await conn.commit()
                rowcount = result.rowcount or 0
                logger.info(f"Updated SortOrder for FileID: {file_id}, EntryID: {entry_id}, affected {rowcount} rows")
                return rowcount > 0
        except SQLAlchemyError as e:
            logger.error(f"Database error updating SortOrder for FileID: {file_id}: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Unexpected error updating SortOrder for FileID: {file_id}: {e}", exc_info=True)
            return False

    async def execute_new_task(self, task: Dict[str, Any], logger: logging.Logger) -> bool:
        file_id = task.get("file_id", "unknown")
        try:
            async with async_engine.connect() as conn:
                result = await conn.execute(text("SELECT 1 AS test"))
                row = result.fetchone()
                logger.info(f"New task test query for FileID {file_id} returned: {row}")
                return True
        except Exception as e:
            logger.error(f"Error executing new_task for FileID {file_id}: {e}", exc_info=True)
            return False

async def shutdown(consumer: RabbitMQConsumer, loop: asyncio.AbstractEventLoop):
    logger.info("Initiating shutdown...")
    try:
        tasks = [task for task in asyncio.all_tasks(loop) if task is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                logger.debug(f"Task {task.get_name()} cancelled or timed out")
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
            loop.close()
            logger.info("Event loop closed")
        logger.info("Shutdown complete.")

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
        logger.info(f"Received signal {sig}, shutting down...")
        loop.call_soon_threadsafe(lambda: asyncio.create_task(shutdown(consumer, loop)))

    return handler

async def main():
    import argparse
    parser = argparse.ArgumentParser(description="RabbitMQ Consumer")
    parser.add_argument("--clear", action="store_true", help="Purge the main queue and exit")
    parser.add_argument("--delete-mismatched", action="store_true", help="Delete queues with mismatched durability")
    args = parser.parse_args()

    producer = await RabbitMQProducer.get_producer()
    consumer = RabbitMQConsumer(producer=producer)

    try:
        if args.clear:
            logger.info("Clearing queue...")
            await consumer.purge_queue()
            logger.info("Queue cleared")
            return
        if args.delete_mismatched:
            logger.info("Checking and deleting queues with mismatched durability...")
            await consumer.connect()
            for queue_name in [consumer.queue_name, consumer.new_queue_name, consumer.response_queue_name]:
                await consumer.channel.queue_delete(queue_name)
                logger.info(f"Deleted queue {queue_name}")
            await consumer.close()
            return
        logger.info("Starting consumer...")
        await consumer.start_consuming()
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        await shutdown(consumer, loop)

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    producer = loop.run_until_complete(RabbitMQProducer.get_producer())
    consumer = RabbitMQConsumer(producer=producer)
    try:
        signal.signal(signal.SIGINT, signal_handler(consumer, loop))
        signal.signal(signal.SIGTERM, signal_handler(consumer, loop))
    except ValueError as e:
        logger.warning(f"Could not set signal handlers: {e}")
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt, shutting down...")
        loop.run_until_complete(shutdown(consumer, loop))
    finally:
        if not loop.is_closed():
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.run_until_complete(loop.shutdown_default_executor())
            loop.close()