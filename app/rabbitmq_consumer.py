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
from database_config import async_engine  # Assume this is your SQLAlchemy async engine
from config import RABBITMQ_URL  # Assume RABBITMQ_URL = "amqp://app_user:app_password@localhost:5672/app_vhost"
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
        producer: Optional['RabbitMQProducer'] = None
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
                    loop=asyncio.get_running_loop()
                )
                self.channel = await self.connection.channel()
                await self.channel.set_qos(prefetch_count=1)
                for queue_name, durable in [
                    (self.queue_name, True),
                    (self.new_queue_name, True),
                    (self.response_queue_name, True)
                ]:
                    queue = await self.channel.declare_queue(queue_name, durable=durable)
                    logger.info(f"Declared queue {queue_name} with durable={durable}")
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
                try:
                    await self.connect()
                except Exception as e:
                    logger.error(f"Reconnect failed: {e}", exc_info=True)
                    await message.nack(requeue=True)
                    return
        
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
            except Exception as e:
                logger.error(f"Error processing message for FileID: {file_id}, TaskType: {task_type}: {e}", exc_info=True)
                await message.nack(requeue=True)

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
                await self.producer.publish_message(response, routing_key=response_queue, correlation_id=correlation_id)
            return {"results": results}
        except SQLAlchemyError as e:
            logger.error(f"Database error executing SELECT for FileID {file_id}: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error executing SELECT for FileID {file_id}: {e}", exc_info=True)
            raise
        finally:
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