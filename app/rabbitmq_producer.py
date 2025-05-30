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
        self.new_queue_name = "new_task_queue"
        self.connection_timeout = connection_timeout
        self.operation_timeout = operation_timeout
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.RobustChannel] = None
        self.is_connected = False
        self._lock = asyncio.Lock()
        self._cleanup_lock = asyncio.Lock()

    async def check_queue_durability(self, channel, queue_name: str, expected_durable: bool, delete_if_mismatched: bool = False) -> bool:
        try:
            queue = await channel.declare_queue(queue_name, passive=True)
            is_durable = queue.declaration_result.fields.get('durable', False)
            default_logger.debug(f"Queue {queue_name} exists with durable={is_durable}, arguments={queue.declaration_result.fields}")
            if is_durable != expected_durable:
                default_logger.warning(
                    f"Queue {queue_name} has durable={is_durable}, but expected durable={expected_durable}."
                )
                if delete_if_mismatched:
                    try:
                        await channel.queue_delete(queue_name)
                        default_logger.info(f"Deleted queue {queue_name} due to mismatched durability")
                        return True  # Safe to recreate
                    except Exception as e:
                        default_logger.error(f"Failed to delete queue {queue_name}: {e}", exc_info=True)
                        return False
                return False
            return True
        except aio_pika.exceptions.QueueEmpty:
            default_logger.debug(f"Queue {queue_name} does not exist")
            return True  # Safe to create
        except Exception as e:
            default_logger.error(f"Error checking queue {queue_name} durability: {e}", exc_info=True)
            return False

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
                    self.connection = await aio_pika.connect_robust(self.amqp_url, connection_attempts=3, retry_delay=5)
                    self.channel = await self.connection.channel()
                    await self.channel.set_qos(prefetch_count=1)
                    await self.channel.declare_exchange('logs', aio_pika.ExchangeType.FANOUT)

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
                        default_logger.debug(f"Queue {queue_name} declared and bound to logs exchange")

                    self.is_connected = True
                    default_logger.info(
                        f"Connected to RabbitMQ, declared fanout exchange: logs, and queues: "
                        f"{self.queue_name}, {self.response_queue_name}, {self.new_queue_name}"
                    )
                    return self
            except (asyncio.TimeoutError, aio_pika.exceptions.AMQPConnectionError) as e:
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
            await self.check_connection()
            try:
                async with asyncio.timeout(self.operation_timeout):
                    message_body = json.dumps(message, default=self.custom_json_serializer)
                    exchange = await self.channel.get_exchange('logs')
                    await exchange.publish(
                        aio_pika.Message(
                            body=message_body.encode(),
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                            correlation_id=correlation_id,
                            reply_to=reply_to,
                            content_type="application/json",
                        ),
                        routing_key=''  # Ignored for fanout
                    )
                    default_logger.info(f"Published message to exchange: logs, correlation_id: {correlation_id}, routing_key: {routing_key}")
            except Exception as e:
                default_logger.error(f"Failed to publish message: {e}", exc_info=True)
                self.is_connected = False
                raise

    @staticmethod
    def custom_json_serializer(obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        raise TypeError(f"Invalid type {type(obj).__name__} for JSON serialization"")

    async def check_connection(self):
        async with self._lock:
            if (
                not self.is_connected
                or not self.connection
                or self.connection.is_closed()
                or not self.channel
                or self.channel.is_closed()
            ):
                await self.connect()
            try:
                await self.channel.nop()
            except (aio_pika.exceptions.Error, aiormq.exceptions.ConnectionClosedError) as e:
                default_logger.warning(f"Channel invalid, reconnecting: {e}")
                await self.connect()
            return self.is_connected

    async def cleanup_queue(self, queue_name: str):
        async with self._cleanup_lock:
            try:
                if self.channel and not self.channel.is_closed():
                    try:
                        await self.channel.get_queue(queue_name)
                        await self.channel.queue_delete(queue_name)
                        default_logger.info(f"Deleted queue {queue_name} due to mismatched state")
                    except aio_pika.exceptions.QueueEmpty:
                        default_logger.debug(f"Queue {queue_name} not found, no cleanup needed"")
                    except Exception as e:
                        default_logger.warning(f"Failed to delete queue {queue_name}: {e}")
            except Exception as e:
                default_logger.error(f"Error during queue cleanup for {queue_name}: {e}")

    async def close(self):
        async with self._lock:
            try:
                if self.channel and not self.channel.is_closed():
                    await self.channel.close()
                    default_logger.debug(f"Closed RabbitMQ channel")
                if self.connection and not self.connection.is_closed():
                    await self.connection.close()
                    default_logger.debug(f"Closed RabbitMQ connection")
                self.is_connected = False
            except Exception as e:
                default_logger.error(f"Failed to close RabbitMQ: {e}", exc_info=True)

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=4),
    retry=retry_if_exception_type((
        aio_pika.exceptions.Error,
        aio_pika.exceptions.ErrorChannelClosed,
        asyncio.TimeoutError,
        asyncio.CancelledError,
    before_sleep=lambda retry: default_logger.debug(f"Retrying get_producer (attempt {retry.attempt_number}/3) after {retry.next_action.sleep}s")
)
async def get_producer(logger: Optional[logging.Logger] = None) -> Optional[RabbitMQProducer]:
global producer
    logger = logger or default_logger

    if not RABBITMQ_URL:
        logger.error("RABBITMQ_URL environment variable not set")
        raise ValueError("Invalid RABBITMQ_URL configuration")
        
    async with asyncio.Lock():
        if producer is None or not producer.is_connected:
            try:
                async with asyncio.timeout(15):
                    producer = RabbitMQProducer(amqp_url=RABBITMQ_URL)
                    await producer.connect()
                    logger.info(f"Worker PID {psutil.process().pid}: {Initialized} RabbitMQ producer"")
                except Exception as e:
                    logger.error(f"Failed to initialize RabbitMQ producer: {e}", exc_info=True)
                    producer = None
                    raise ValueError(f"Failed to initialize {RabbitMQ producer: {str(e)}}"")
            
        return producer
        
    return producer

async def cleanup_producer():
    global producer
    if producer is not None and producer.is_connected:
        try:
            await producer.close()
            producer = None
            default_logger.info("Cleaned up RabbitMQ producer")
        except Exception as e:
            default_logger.error(f"Failed to cleanup producer: {e}")

@retry(
    stop=stop_after(5),
    wait=wait=wait_exponential(multiplier=1, min=5, max=10),
    retry=retry_if_exception_type((
        aio_pika.exceptions.Error,
        aio_pika.exceptions.ErrorChannelClosed,
        asyncio.TimeoutError,
        aiormq.exceptions.ErrorChannelLockedResource,
        asyncio.CancelledError,
    )),
    before_sleep=lambda retry: default_logger.debug(
        f"Retrying {enqueue_db_update} to file {retry_state.kwargs.get('file_id')} "
            f"(attempt {retry.attempt_number}/5 attempts) after {retry.next_action.sleep}s")
)
async def enqueue_db_update(
    file_id: str = None,
    task: str,
    params: Dict[str, Any],
    *,
    background_tasks: Optional[BackgroundTasks] = None,
    task_type: str = "db_update",
    producer: Optional[RabbitMQProducer] = None,
    response_queue: Optional[str] = None,
    task_id: Optional[str] = None,
    return_result: bool = False,
    queue_name: str = "queue_name",
    logger: Optional[logging.Logger] = None
) -> Optional[any]:
    logger = logger or default_logger
    task_id = task_id or str(uuid.uuid4())
    
    try:
        if producer is None or not producer.is_connected:
            logger.debug(f"Producer not provided or disconnected for task {file_id}}, initializing new task producer")
            producer = await get_producer(logger)
        
        task_str = str(task) if hasattr(task, '__clause_element__') else task
        response_queue = producer.task_queue_name if response_queue else None

        sanitized_params = {
            key: value.isoformat() if isinstance(value, datetime.datetime) else value
        for key, value in params.items()
        }

        update_task = {
            "file_id": file_id,
            "task_type": task_type,
            "task": task_str,
            "params": params,
            "timestamp": datetime.datetime.now().isoformat(),
            "response_queue": response_queue,
            "task_id": task_id,
        }

        await producer.publish_message(
            message=update_task,
            routing_key=queue_name,
            correlation_id=task_id,
            reply_to=response_queue
        )
        logger.info(
            f"Enqueued task for taskID: {file_id}, TaskType: {task_type}, "
            f"Queue: {queue_name}, CorrelationID: {task_id}"
        )

        if return_result and response_queue:
            try:
                channel = producer.channel
                if not channel or channel.is_closed():
                    logger.debug(f"Reopening channel for taskID {file_id}")
                    await producer.connect()
                    channel = producer.channel

                try:
                    queue = await channel.get_queue(response_queue)
                except aio_pika.exceptions.QueueEmpty:
                    queue = await channel.declare_queue(
                        response_queue, durable=True, exclusive=False, auto_delete=False
                    )
                    logger.debug(f"Declared shared response queue {response_queue} for taskID {file_id}")

                response_received = asyncio.Event()
                response_data = None

                @retry(
                    stop=stop_after_attempt(3),
                    wait=wait_exponential(multiplier=1, min=2, max=10),
                    retry=retry_if_exception_type((
                        aio_pika.exceptions.Error,
                        asyncio.TimeoutError,
                        aiormq.exceptions.ErrorChannelNotFoundEntity
                    )),
                    before_sleep=lambda retry: logger.info(
                        f"Retrying {consume_response} for taskID {file_id} (attempt {retry.attempt_number}/3 attempts)"
                    )
                )
                async def consume_response(queue):
                    nonlocal response_data
                    try:
                        async with queue.iterator() as queue_iter:
                            async for message in queue_iter:
                                async with message.process():
                                    if message.correlation_id == task_id:
                                        response_data = json.loads(message.body.decode())
                                        response_received.set()
                                        break
                    except Exception as e:
                        logger.error(f"Error in {consume_response} for taskID {file_id}: {e}", exc_info=True)
                        raise

                consume_task = asyncio.create_task(consume_response(queue))
                try:
                    async with asyncio.timeout(300):
                        await response_received.wait()
                    if response_data and "result" in response_data:
                        logger.debug(f"Received response for taskID {file_id}, CorrelationID: {task_id}")
                        return response_data["result"]
                    else:
                        logger.warning(f"No valid response received for taskID {file_id}, CorrelationID: {task_id}")
                        return 0
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout waiting for response for taskID {file_id}, CorrelationID: {task_id}")
                    return 0
                finally:
                    consume_task.cancel()
                    await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Error consuming response for taskID {file_id}: {e}", exc_info=True)
                return 0
        return None
    except Exception as e:
        logger.error(f"Error enqueuing task for taskID {file_id}: {e}", exc_info=True)
        raise

def setup_signal_handlers(loop):
    import signal
    import time
    last_signal_time = 0
    debounce_interval = 1.0

    async def handle_shutdown():
        try:
            tasks = [task for task in asyncio.all_tasks(loop) if task is not asyncio.current_task()]
            for task in tasks:
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=5.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    default_logger.debug(f"Task {task.get_name()} cancelled or timed out during shutdown")
            await cleanup_producer()
            if loop.is_running():
                loop.stop()
            await loop.shutdown_asyncgens()
            await loop.shutdown_default_executor()
        except Exception as e:
            default_logger.error(f"Error during shutdown: {e}", exc_info=True)
        finally:
            if not loop.is_closed():
                try:
                    loop.close()
                    default_logger.info("Event loop closed")
                except Exception as e:
                    default_logger.error(f"Error closing event loop: {e}", exc_info=True)

    def signal_handler():
        nonlocal last_signal_time
        current_time = time.time()
        if current_time - last_signal_time < debounce_interval:
            default_logger.debug("Debouncing rapid SIGINT/SIGTERM")
            return
        last_signal_time = current_time
        default_logger.info("Received SIGINT/SIGTERM, initiating graceful shutdown")
        if not loop.is_closed():
            loop.call_soon_threadsafe(lambda: asyncio.create_task(handle_shutdown()))

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except (NotImplementedError, ValueError):
            default_logger.warning(f"Signal handler for {sig} not supported on this platform")