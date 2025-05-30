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
        connection_timeout: float = 10.0,
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
                    # Declare shared response queue
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
            try:
                async with asyncio.timeout(self.operation_timeout):
                    message_body = json.dumps(message)
                    queue_name = routing_key or self.queue_name
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

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type((
        aio_pika.exceptions.AMQPError,
        aio_pika.exceptions.ChannelClosed,
        asyncio.TimeoutError
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
                async with asyncio.timeout(10):
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
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((
        aio_pika.exceptions.AMQPError,
        aio_pika.exceptions.ChannelClosed,
        asyncio.TimeoutError
    )),  # Removed ChannelLockedResource to handle explicitly
    before_sleep=lambda retry_state: default_logger.info(
        f"Retrying enqueue_db_update for FileID {retry_state.kwargs.get('file_id')} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
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
        response_queue = response_queue or producer.response_queue_name if return_result else None

        update_task = {
            "file_id": file_id,
            "task_type": task_type,
            "sql": sql_str,
            "params": params,
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

                # Declare shared response queue (non-exclusive)
                queue = await channel.declare_queue(
                    response_queue, durable=False, exclusive=False, auto_delete=True
                )

                response_received = asyncio.Event()
                response_data = None

                async def consume_response():
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

                consume_task = asyncio.create_task(consume_response())
                try:
                    async with asyncio.timeout(60):
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

# Signal handler for graceful shutdown
def setup_signal_handlers(loop):
    import signal
    def handle_shutdown():
        tasks = [task for task in asyncio.all_tasks(loop) if task is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        loop.run_until_complete(cleanup_producer())
        loop.stop()
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_shutdown)