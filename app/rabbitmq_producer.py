import aio_pika
import json
import logging
import asyncio
import datetime
import uuid
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import aiormq
from typing import Dict, Any, Optional
from config import RABBITMQ_URL
from fastapi import BackgroundTasks
import psutil

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
default_logger = logging.getLogger(__name__)

class RabbitMQProducer:
    _instance = None
    _lock = asyncio.Lock()

    def __init__(
        self,
        amqp_url: str = RABBITMQ_URL,
        queue_name: str = "db_update_queue",
        connection_timeout: float = 60.0,
        operation_timeout: float = 15.0,
    ):
        self.amqp_url = amqp_url
        self.queue_name = queue_name
        self.response_queue_name = "shared_response_queue"
        self.new_queue_name = "new_task_queue"
        self.connection_timeout = connection_timeout
        self.operation_timeout = operation_timeout
        self.connection = None
        self.channel = None
        self.is_connected = False
        self._lock = asyncio.Lock()

    @classmethod
    async def get_producer(cls, logger: Optional[logging.Logger] = None) -> 'RabbitMQProducer':
        logger = logger or default_logger
        async with cls._lock:
            if cls._instance is None or not cls._instance.is_connected:
                cls._instance = cls()
                await cls._instance.connect()
                logger.info("Initialized RabbitMQProducer")
            return cls._instance

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((
            aio_pika.exceptions.AMQPError,
            aio_pika.exceptions.ChannelClosed,
            asyncio.TimeoutError,
            aiormq.exceptions.ChannelInvalidStateError,
        )),
        before_sleep=lambda retry_state: default_logger.info(
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
                for queue_name in [self.queue_name, self.new_queue_name, self.response_queue_name]:
                    await self.channel.declare_queue(queue_name, durable=True)
                    default_logger.debug(f"Declared queue {queue_name}")
                self.is_connected = True
                default_logger.info("RabbitMQProducer connected")
            except Exception as e:
                default_logger.error(f"Failed to connect RabbitMQProducer: {e}", exc_info=True)
                await self.close()
                raise

    async def close(self):
        async with self._lock:
            try:
                if self.channel and not self.channel.is_closed:
                    await asyncio.wait_for(self.channel.close(), timeout=5.0)
                    default_logger.info("Closed RabbitMQ channel")
                if self.connection and not self.connection.is_closed:
                    await asyncio.wait_for(self.connection.close(), timeout=5.0)
                    default_logger.info("Closed RabbitMQ connection")
                self.is_connected = False
            except asyncio.TimeoutError:
                default_logger.warning("Timeout closing RabbitMQ resources")
            except Exception as e:
                default_logger.error(f"Error closing RabbitMQProducer: {e}", exc_info=True)
            finally:
                self.channel = None
                self.connection = None

    async def check_connection(self):
        async with self._lock:
            if (
                not self.is_connected
                or not self.connection
                or self.connection.is_closed
                or not self.channel
                or self.channel.is_closed
            ):
                await self.connect()
            try:
                await self.channel.nop()
            except (aio_pika.exceptions.AMQPError, aiormq.exceptions.ConnectionChannelError) as e:
                default_logger.warning(f"Channel invalid, reconnecting: {e}")
                await self.connect()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(aio_pika.exceptions.AMQPError)
    )
    async def publish_message(
        self,
        message: Dict[str, Any],
        routing_key: str,
        correlation_id: Optional[str] = None,
        reply_to: Optional[str] = None
    ):
        try:
            await self.check_connection()
            async with asyncio.timeout(self.operation_timeout):
                message_body = json.dumps(message, default=self.custom_json_serializer)
                await self.channel.default_exchange.publish(
                    aio_pika.Message(
                        body=message_body.encode(),
                        correlation_id=correlation_id,
                        reply_to=reply_to,
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        content_type="application/json"
                    ),
                    routing_key=routing_key
                )
                default_logger.info(f"Published message to {routing_key}, correlation_id: {correlation_id}")
        except Exception as e:
            default_logger.error(f"Failed to publish message to {routing_key}: {e}", exc_info=True)
            self.is_connected = False
            await self.close()
            raise

    @staticmethod
    def custom_json_serializer(obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((
            aio_pika.exceptions.AMQPError,
            aio_pika.exceptions.ChannelClosed,
            asyncio.TimeoutError,
            aiormq.exceptions.ChannelLockedResource
        ))
    )
    async def enqueue_db_update(
        self,
        file_id: str,
        sql: str,
        params: Dict[str, Any],
        background_tasks: Optional[BackgroundTasks] = None,
        task_type: str = "db_update",
        response_queue: Optional[str] = None,
        correlation_id: Optional[str] = None,
        return_result: bool = False,
        queue_name: str = "db_update_queue"
    ) -> Optional[int]:
        correlation_id = correlation_id or str(uuid.uuid4())
        try:
            await self.check_connection()
            sql_str = str(sql) if hasattr(sql, '__clause_element__') else sql
            response_queue = self.response_queue_name if return_result and not response_queue else response_queue

            sanitized_params = {
                key: value.isoformat() if isinstance(value, datetime.datetime) else value
                for key, value in params.items()
            }

            update_task = {
                "file_id": file_id,
                "task_type": task_type,
                "sql": sql_str,
                "params": sanitized_params,
                "timestamp": datetime.datetime.now().isoformat(),
                "response_queue": response_queue,
                "correlation_id": correlation_id
            }

            await self.publish_message(
                message=update_task,
                routing_key=queue_name,
                correlation_id=correlation_id,
                reply_to=response_queue
            )
            default_logger.info(
                f"Enqueued task for FileID: {file_id}, TaskType: {task_type}, "
                f"Queue: {queue_name}, CorrelationID: {correlation_id}"
            )

            if return_result and response_queue:
                try:
                    channel = self.channel
                    if not channel or channel.is_closed:
                        default_logger.debug(f"Reopening channel for FileID {file_id}")
                        await self.connect()
                        channel = self.channel

                    try:
                        queue = await channel.get_queue(response_queue)
                    except aio_pika.exceptions.QueueEmpty:
                        queue = await channel.declare_queue(
                            response_queue, durable=True, exclusive=False, auto_delete=False
                        )
                        default_logger.debug(f"Declared response queue {response_queue} for FileID {file_id}")

                    response_received = asyncio.Event()
                    response_data = None

                    @retry(
                        stop=stop_after_attempt(3),
                        wait=wait_exponential(multiplier=1, min=2, max=10),
                        retry=retry_if_exception_type((
                            aio_pika.exceptions.AMQPError,
                            asyncio.TimeoutError,
                            aiormq.exceptions.ChannelNotFoundEntity
                        ))
                    )
                    async def consume_response(queue):
                        nonlocal response_data
                        async with queue.iterator() as queue_iter:
                            async for message in queue_iter:
                                async with message.process():
                                    if message.correlation_id == correlation_id:
                                        response_data = json.loads(message.body.decode())
                                        response_received.set()
                                        break

                    consume_task = asyncio.create_task(consume_response(queue))
                    try:
                        async with asyncio.timeout(300):
                            await response_received.wait()
                        if response_data and "result" in response_data:
                            default_logger.debug(f"Received response for FileID {file_id}, CorrelationID: {correlation_id}")
                            return response_data["result"]
                        default_logger.warning(f"No valid response received for FileID {file_id}, CorrelationID: {correlation_id}")
                        return 0
                    except asyncio.TimeoutError:
                        default_logger.warning(f"Timeout waiting for response for FileID {file_id}, CorrelationID: {correlation_id}")
                        return 0
                    finally:
                        consume_task.cancel()
                        await asyncio.sleep(0.1)
                except Exception as e:
                    default_logger.error(f"Error consuming response for FileID {file_id}: {e}", exc_info=True)
                    return 0
            return None
        except Exception as e:
            default_logger.error(f"Error enqueuing task for FileID {file_id}: {e}", exc_info=True)
            raise