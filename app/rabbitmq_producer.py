import logging
import json
import asyncio
import aio_pika
import psutil
import uuid
from typing import Dict, Any, Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from config import RABBITMQ_URL
from fastapi import BackgroundTasks
import datetime
import aiormq.exceptions

producer = None

# Logging setup
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    default_logger.setLevel(logging.INFO)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

class RabbitMQProducer:
    def __init__(
        self,
        amqp_url: str = RABBITMQ_URL,
        queue_name: str = "db_update_queue",
        connection_timeout: float = 15.0,
        operation_timeout: float = 5.0,
    ):
        self.amqp_url = amqp_url
        self.queue_name = queue_name
        self.response_queue_name = "shared_response_queue"
        self.connection_timeout = connection_timeout
        self.operation_timeout = operation_timeout
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.RobustChannel] = None
        self.is_connected = False
        self._lock = asyncio.Lock()
        self._cleanup_lock = asyncio.Lock()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((
            aio_pika.exceptions.AMQPError,
            aio_pika.exceptions.ChannelClosed,
            asyncio.TimeoutError,
            asyncio.CancelledError,
        )),
        before_sleep=lambda retry_state: default_logger.info(
            f"Retrying connect (attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
        )
    )
    async def connect(self) -> 'RabbitMQProducer':
        async with self._lock:
            if self.is_connected and self.connection and not self.connection.is_closed:
                default_logger.debug("RabbitMQ connection already established")
                return self
            try:
                async with asyncio.timeout(self.connection_timeout):
                    self.connection = await aio_pika.connect_robust(
                        self.amqp_url,
                        connection_attempts=3,
                        retry_delay=5,
                    )
                    self.channel = await self.connection.channel()
                    await self.channel.set_qos(prefetch_count=2)
                    await self.channel.declare_queue(self.queue_name, durable=True)
                    try:
                        await self.cleanup_queue(self.response_queue_name)
                    except Exception as e:
                        default_logger.warning(f"Failed to cleanup queue during connect: {e}")
                    await self.channel.declare_queue(
                        self.response_queue_name, durable=False, exclusive=False, auto_delete=True
                    )
                    self.is_connected = True
                    default_logger.info(f"Connected to RabbitMQ and declared queues: {self.queue_name}, {self.response_queue_name}")
                    return self
            except asyncio.TimeoutError:
                default_logger.error("Timeout connecting to RabbitMQ")
                self.is_connected = False
                raise
            except aio_pika.exceptions.AMQPConnectionError as e:
                default_logger.error(f"Failed to connect to RabbitMQ: {e}", exc_info=True)
                self.is_connected = False
                raise

    @staticmethod
    def custom_json_serializer(obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    async def publish_message(
        self,
        message: Dict[str, Any],
        routing_key: Optional[str] = None,
        correlation_id: Optional[str] = None,
        reply_to: Optional[str] = None
    ):
        async with self._lock:
            if not self.is_connected or not self.connection or self.connection.is_closed:
                await self.connect()
            queue_name = routing_key or self.queue_name
            try:
                async with asyncio.timeout(self.operation_timeout):
                    message_body = json.dumps(message, default=self.custom_json_serializer)
                    await self.channel.default_exchange.publish(
                        aio_pika.Message(
                            body=message_body.encode(),
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                            correlation_id=correlation_id,
                            reply_to=reply_to,
                            content_type="application/json",
                        ),
                        routing_key=queue_name,
                    )
                    default_logger.debug(
                        f"Published message to queue: {queue_name}, "
                        f"correlation_id: {correlation_id}, task: {message_body[:200]}"
                    )
            except Exception as e:
                default_logger.error(f"Failed to publish message to {queue_name}: {e}", exc_info=True)
                self.is_connected = False
                raise

    async def cleanup_queue(self, queue_name: str):
        async with self._cleanup_lock:
            try:
                if self.channel and not self.channel.is_closed:
                    try:
                        await self.channel.get_queue(queue_name)
                        await self.channel.queue_delete(queue_name)
                        default_logger.info(f"Deleted stale queue {queue_name}")
                    except aio_pika.exceptions.QueueEmpty:
                        default_logger.debug(f"Queue {queue_name} does not exist, no cleanup needed")
            except Exception as e:
                default_logger.warning(f"Failed to clean up queue {queue_name}: {e}")

    async def close(self):
        async with self._lock:
            try:
                if self.channel and not self.channel.is_closed:
                    await self.channel.close()
                    default_logger.info("Closed RabbitMQ channel")
                if self.connection and not self.connection.is_closed:
                    await self.connection.close()
                    default_logger.info("Closed RabbitMQ connection")
                self.is_connected = False
            except Exception as e:
                default_logger.error(f"Error closing RabbitMQ connection: {e}", exc_info=True)

    async def check_connection(self):
        async with self._lock:
            if not self.is_connected or not self.connection or self.connection.is_closed or not self.channel or self.channel.is_closed:
                await self.connect()
            return self.is_connected

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type((
        aio_pika.exceptions.AMQPError,
        aio_pika.exceptions.ChannelClosed,
        asyncio.TimeoutError,
        asyncio.CancelledError,
    )),
    before_sleep=lambda retry_state: default_logger.info(
        f"Retrying get_producer (attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def get_producer(logger: Optional[logging.Logger] = None) -> RabbitMQProducer:
    global producer
    logger = logger or default_logger
    
    if not RABBITMQ_URL:
        logger.error("RABBITMQ_URL environment variable not set")
        raise ValueError("RABBITMQ_URL not configured")
    
    async with asyncio.Lock():
        if producer is None or not producer.is_connected:
            try:
                async with asyncio.timeout(15):
                    producer = RabbitMQProducer(amqp_url=RABBITMQ_URL)
                    await producer.connect()
                    logger.info(f"Worker PID {psutil.Process().pid}: Initialized RabbitMQ producer")
            except Exception as e:
                logger.error(f"Failed to initialize RabbitMQ producer: {e}", exc_info=True)
                producer = None
                raise ValueError(f"Failed to initialize RabbitMQ producer: {str(e)}")
    
    return producer

async def cleanup_producer():
    global producer
    if producer is not None and producer.is_connected:
        await producer.close()
        producer = None
        default_logger.info("Cleaned up RabbitMQ producer")

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((
        aio_pika.exceptions.AMQPError,
        aio_pika.exceptions.ChannelClosed,
        asyncio.TimeoutError,
        aiormq.exceptions.ChannelLockedResource,
        asyncio.CancelledError,
    )),
    before_sleep=lambda retry_state: default_logger.info(
        f"Retrying enqueue_db_update for FileID {retry_state.kwargs.get('file_id')} "
        f"(attempt {retry_state.attempt_number}/5) after {retry_state.next_action.sleep}s"
    )
)
async def enqueue_db_update(
    file_id: str,
    sql: str,
    params: Dict[str, Any],
    background_tasks: Optional[BackgroundTasks] = None,
    task_type: str = "db_update",
    producer: Optional[RabbitMQProducer] = None,
    response_queue: Optional[str] = None,
    correlation_id: Optional[str] = None,
    return_result: bool = False,
    logger: Optional[logging.Logger] = None
) -> Optional[int]:
    logger = logger or default_logger
    correlation_id = correlation_id or str(uuid.uuid4())
    
    try:
        if producer is None or not producer.is_connected:
            logger.debug(f"Producer not provided or disconnected for FileID {file_id}, initializing new producer")
            producer = await get_producer(logger)
        
        sql_str = str(sql) if hasattr(sql, '__clause_element__') else sql
        response_queue = response_queue or f"response_{correlation_id}" if return_result else None

        # Sanitize params
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
            "correlation_id": correlation_id,
        }

        await producer.publish_message(
            message=update_task,
            routing_key="db_update_queue",
            correlation_id=correlation_id,
            reply_to=response_queue
        )
        logger.info(f"Enqueued database update for FileID: {file_id}, TaskType: {task_type}, SQL: {sql_str[:100]}, CorrelationID: {correlation_id}")

        if return_result and response_queue:
            try:
                channel = producer.channel
                if not channel or channel.is_closed:
                    logger.debug(f"Reopening channel for FileID {file_id}")
                    await producer.connect()
                    channel = producer.channel

                # Declare response queue without cleanup
                @retry(
                    stop=stop_after_attempt(3),
                    wait=wait_exponential(multiplier=1, min=1, max=5),
                    retry=retry_if_exception_type((aiormq.exceptions.ChannelLockedResource, aio_pika.exceptions.AMQPError)),
                    before_sleep=lambda retry_state: logger.info(
                        f"Retrying queue declaration for {response_queue} (attempt {retry_state.attempt_number}/3)"
                    )
                )
                async def declare_queue():
                    try:
                        queue = await channel.declare_queue(
                            response_queue, durable=False, exclusive=False, auto_delete=True
                        )
                        await asyncio.sleep(0.1)  # Brief delay to ensure queue is ready
                        return queue
                    except aiormq.exceptions.ChannelLockedResource:
                        logger.warning(f"Queue {response_queue} locked, attempting to delete and retry")
                        await producer.cleanup_queue(response_queue)
                        raise

                try:
                    queue = await declare_queue()
                    logger.debug(f"Declared response queue {response_queue} for FileID {file_id}")
                except Exception as e:
                    logger.error(f"Failed to declare response queue {response_queue} for FileID {file_id}: {e}", exc_info=True)
                    return 0

                response_received = asyncio.Event()
                response_data = None

                @retry(
                    stop=stop_after_attempt(3),
                    wait=wait_exponential(multiplier=1, min=2, max=10),
                    retry=retry_if_exception_type((aio_pika.exceptions.AMQPError, asyncio.TimeoutError, aiormq.exceptions.ChannelNotFoundEntity)),
                    before_sleep=lambda retry_state: logger.info(
                        f"Retrying consume_response for FileID {file_id} (attempt {retry_state.attempt_number}/3)"
                    )
                )
                async def consume_response(queue):
                    nonlocal response_data
                    try:
                        async with queue.iterator() as queue_iter:
                            async for message in queue_iter:
                                async with message.process():
                                    if message.correlation_id == correlation_id:
                                        response_data = json.loads(message.body.decode())
                                        response_received.set()
                                        break
                    except Exception as e:
                        logger.error(f"Error in consume_response for FileID {file_id}: {e}", exc_info=True)
                        raise

                consume_task = asyncio.create_task(consume_response(queue))
                try:
                    async with asyncio.timeout(120):
                        await response_received.wait()
                    if response_data and "result" in response_data:
                        logger.debug(f"Received response for FileID {file_id}, CorrelationID: {correlation_id}")
                        return response_data["result"]
                    else:
                        logger.warning(f"No valid response received for FileID {file_id}, CorrelationID: {correlation_id}")
                        return 0
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout waiting for response for FileID {file_id}, CorrelationID: {correlation_id}")
                    return 0
                finally:
                    consume_task.cancel()
                    try:
                        await channel.queue_delete(response_queue)
                        logger.debug(f"Deleted response queue {response_queue}")
                    except Exception as e:
                        logger.warning(f"Failed to delete response queue {response_queue}: {e}")
                    await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Error consuming response for FileID {file_id}: {e}", exc_info=True)
                return 0
        return None

    except Exception as e:
        logger.error(f"Error enqueuing task for FileID {file_id}: {e}", exc_info=True)
        raise

def setup_signal_handlers(loop):
    import signal
    last_signal_time = 0
    debounce_interval = 1.0  # seconds

    async def handle_shutdown():
        try:
            tasks = [task for task in asyncio.all_tasks(loop) if task is not asyncio.current_task()]
            for task in tasks:
                task.cancel()
            await cleanup_producer()
            loop.stop()
            await loop.shutdown_asyncgens()
            await loop.shutdown_default_executor()
        except Exception as e:
            default_logger.error(f"Error during shutdown: {e}", exc_info=True)
        finally:
            loop.close()
    
    def signal_handler():
        nonlocal last_signal_time
        current_time = time.time()
        if current_time - last_signal_time < debounce_interval:
            default_logger.debug("Debouncing rapid SIGINT/SIGTERM")
            return
        last_signal_time = current_time
        default_logger.info("Received SIGINT/SIGTERM, initiating graceful shutdown")
        asyncio.ensure_future(handle_shutdown())
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            default_logger.warning(f"Signal handler for {sig} not supported on this platform")