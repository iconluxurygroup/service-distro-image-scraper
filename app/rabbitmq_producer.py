import aio_pika
import json
import logging
import datetime
from typing import Dict, Any, Optional
from fastapi import BackgroundTasks
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import asyncio
import aiormq.exceptions
import uuid

import uuid
import datetime
import json
from typing import Dict, Any, Optional
from fastapi import BackgroundTasks
import logging
import aio_pika
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

default_logger = logging.getLogger(__name__)

# Global RabbitMQ producer
producer = None
class RabbitMQProducer:
    def __init__(
        self,
        amqp_url: str = "amqp://app_user:app_password@localhost:5672/app_vhost",
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
        self.is_connected = False

    async def connect(self):
        """Establish an async robust connection to RabbitMQ and declare a durable queue."""
        if self.is_connected and self.connection and not self.connection.is_closed:
            return
        try:
            async with asyncio.timeout(self.connection_timeout):
                self.connection = await aio_pika.connect_robust(
                    self.amqp_url,
                    connection_attempts=3,
                    retry_delay=5,
                )
                self.channel = await self.connection.channel()
                await self.channel.declare_queue(self.queue_name, durable=True)
                self.is_connected = True
                logger.info(f"Connected to RabbitMQ and declared queue: {self.queue_name}")
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
        except aiormq.exceptions.ChannelAccessRefused as e:
            logger.error(
                f"Permissions error connecting to RabbitMQ: {e}. "
                f"Ensure user has permissions on virtual host and default exchange.",
                exc_info=True
            )
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=10),
        retry=retry_if_exception_type((
            aio_pika.exceptions.AMQPError,
            aio_pika.exceptions.ChannelClosed,
            aiormq.exceptions.ChannelAccessRefused,
            asyncio.TimeoutError
        )),
        before_sleep=lambda retry_state: logger.info(
            f"Retrying RabbitMQ operation (attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
        ),
    )
    async def publish_message(self, message: Dict[str, Any], routing_key: str = None, correlation_id: Optional[str] = None):
        """Publish a message to the queue with optional correlation_id."""
        if not self.is_connected or not self.connection or self.connection.is_closed:
            await self.connect()

        try:
            async with asyncio.timeout(self.operation_timeout):
                message_body = json.dumps(message)
                queue_name = routing_key or self.queue_name
                if queue_name != self.queue_name and not queue_name.startswith("select_response_"):
                    await self.channel.declare_queue(queue_name, durable=False, exclusive=True, auto_delete=True)
                
                await self.channel.default_exchange.publish(
                    aio_pika.Message(
                        body=message_body.encode(),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        correlation_id=correlation_id,
                        content_type="application/json",
                    ),
                    routing_key=queue_name,
                )
                logger.info(
                    f"Published message to queue: {queue_name}, "
                    f"correlation_id: {correlation_id}, task: {message_body[:200]}"
                )
        except aio_pika.exceptions.ProbableAuthenticationError as e:
            logger.error(
                f"Authentication error publishing to queue {queue_name}: {e}. "
                f"Check username, password, and virtual host in {self.amqp_url}.",
                exc_info=True
            )
            raise
        except aiormq.exceptions.ChannelAccessRefused as e:
            logger.error(
                f"Permissions error publishing to queue {queue_name}: {e}. "
                f"Ensure user has write permissions on the default exchange.",
                exc_info=True
            )
            raise
        except asyncio.TimeoutError:
            logger.error(f"Timeout publishing message to {queue_name}")
            raise
        except Exception as e:
            logger.error(f"Failed to publish message to {queue_name}: {e}", exc_info=True)
            self.is_connected = False
            raise

    async def publish_update(self, update_task: Dict[str, Any], routing_key: str = None, correlation_id: Optional[str] = None):
        """Publish an update task to the queue with persistent delivery."""
        await self.publish_message(update_task, routing_key, correlation_id)

    async def close(self):
        """Close the RabbitMQ connection and channel."""
        try:
            if self.channel and not self.channel.is_closed:
                await self.channel.close()
                logger.info("Closed RabbitMQ channel")
            if self.connection and not self.connection.is_closed:
                await self.connection.close()
                logger.info("Closed RabbitMQ connection")
            self.is_connected = False
        except Exception as e:
            logger.error(f"Error closing RabbitMQ connection: {e}", exc_info=True)

import aio_pika
import json
import logging
import datetime
from typing import Dict, Any, Optional
from fastapi import BackgroundTasks
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import asyncio
import aiormq.exceptions
import uuid
logger = logging.getLogger(__name__)

class RabbitMQProducer:
    def __init__(
        self,
        amqp_url: str = "amqp://app_user:app_password@localhost:5672/app_vhost",
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
        self.is_connected = False

    async def connect(self):
        """Establish an async robust connection to RabbitMQ and declare a durable queue."""
        if self.is_connected and self.connection and not self.connection.is_closed:
            return
        try:
            async with asyncio.timeout(self.connection_timeout):
                self.connection = await aio_pika.connect_robust(
                    self.amqp_url,
                    connection_attempts=3,
                    retry_delay=5,
                )
                self.channel = await self.connection.channel()
                await self.channel.declare_queue(self.queue_name, durable=True)
                self.is_connected = True
                logger.info(f"Connected to RabbitMQ and declared queue: {self.queue_name}")
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
        except aiormq.exceptions.ChannelAccessRefused as e:
            logger.error(
                f"Permissions error connecting to RabbitMQ: {e}. "
                f"Ensure user has permissions on virtual host and default exchange.",
                exc_info=True
            )
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=10),
        retry=retry_if_exception_type((
            aio_pika.exceptions.AMQPError,
            aio_pika.exceptions.ChannelClosed,
            aiormq.exceptions.ChannelAccessRefused,
            asyncio.TimeoutError
        )),
        before_sleep=lambda retry_state: logger.info(
            f"Retrying RabbitMQ operation (attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
        ),
    )
    async def publish_message(self, message: Dict[str, Any], routing_key: str = None, correlation_id: Optional[str] = None):
        """Publish a message to the queue with optional correlation_id."""
        if not self.is_connected or not self.connection or self.connection.is_closed:
            await self.connect()

        try:
            async with asyncio.timeout(self.operation_timeout):
                message_body = json.dumps(message)
                queue_name = routing_key or self.queue_name
                if queue_name != self.queue_name and not queue_name.startswith("select_response_"):
                    await self.channel.declare_queue(queue_name, durable=False, exclusive=True, auto_delete=True)
                
                await self.channel.default_exchange.publish(
                    aio_pika.Message(
                        body=message_body.encode(),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        correlation_id=correlation_id,
                        content_type="application/json",
                    ),
                    routing_key=queue_name,
                )
                logger.info(
                    f"Published message to queue: {queue_name}, "
                    f"correlation_id: {correlation_id}, task: {message_body[:200]}"
                )
        except aio_pika.exceptions.ProbableAuthenticationError as e:
            logger.error(
                f"Authentication error publishing to queue {queue_name}: {e}. "
                f"Check username, password, and virtual host in {self.amqp_url}.",
                exc_info=True
            )
            raise
        except aiormq.exceptions.ChannelAccessRefused as e:
            logger.error(
                f"Permissions error publishing to queue {queue_name}: {e}. "
                f"Ensure user has write permissions on the default exchange.",
                exc_info=True
            )
            raise
        except asyncio.TimeoutError:
            logger.error(f"Timeout publishing message to {queue_name}")
            raise
        except Exception as e:
            logger.error(f"Failed to publish message to {queue_name}: {e}", exc_info=True)
            self.is_connected = False
            raise

    async def publish_update(self, update_task: Dict[str, Any], routing_key: str = None, correlation_id: Optional[str] = None):
        """Publish an update task to the queue with persistent delivery."""
        await self.publish_message(update_task, routing_key, correlation_id)

    async def close(self):
        """Close the RabbitMQ connection and channel."""
        try:
            if self.channel and not self.channel.is_closed:
                await self.channel.close()
                logger.info("Closed RabbitMQ channel")
            if self.connection and not self.connection.is_closed:
                await self.connection.close()
                logger.info("Closed RabbitMQ connection")
            self.is_connected = False
        except Exception as e:
            logger.error(f"Error closing RabbitMQ connection: {e}", exc_info=True)
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(aio_pika.exceptions.AMQPError),
    before_sleep=lambda retry_state: default_logger.info(
        f"Retrying enqueue_db_update for FileID {retry_state.kwargs.get('file_id')} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def enqueue_db_update(
    file_id: str,
    sql: str,
    params: Dict[str, Any],
    background_tasks: BackgroundTasks,
    task_type: str = "db_update",
    producer: Optional[RabbitMQProducer] = None,
    response_queue: Optional[str] = None,
    correlation_id: Optional[str] = None,
    return_result: bool = False,
) -> Optional[int]:
    if producer is None:
        default_logger.error("RabbitMQ producer not initialized")
        raise ValueError("RabbitMQ producer not initialized")

    try:
        # Ensure producer is connected
        if producer.connection is None or producer.connection.is_closed:
            await producer.connect()

        # Convert SQLAlchemy text object to string if necessary
        sql_str = str(sql) if hasattr(sql, '__clause_element__') else sql
        correlation_id = correlation_id or str(uuid.uuid4())

        # Use a shared response queue for return_result tasks if not specified
        response_queue = response_queue or f"response_{file_id}_{correlation_id}" if return_result else None

        update_task = {
            "file_id": file_id,
            "task_type": task_type,
            "sql": sql_str,
            "params": params,
            "timestamp": datetime.datetime.now().isoformat(),
            "response_queue": response_queue,
            "correlation_id": correlation_id,
        }

        # Publish the task to RabbitMQ
        message_body = json.dumps(update_task).encode()
        await producer.publish(
            message=message_body,
            routing_key="db_update_queue",  # Adjust to your queue name
            correlation_id=correlation_id,
            reply_to=response_queue
        )
        default_logger.debug(f"Enqueued task {task_type} for FileID {file_id}, CorrelationID: {correlation_id}")

        if return_result and response_queue:
            connection = None
            try:
                connection = await aio_pika.connect_robust(producer.amqp_url)
                channel = await connection.channel()
                queue = await channel.declare_queue(response_queue, exclusive=True, auto_delete=True)
                response_received = asyncio.Event()
                response_data = None

                async def consume_response():
                    nonlocal response_data
                    async with queue.iterator() as queue_iter:
                        async for message in queue_iter:
                            async with message.process():
                                if message.correlation_id == correlation_id:
                                    response_data = json.loads(message.body.decode())
                                    response_received.set()
                                    break

                consume_task = asyncio.create_task(consume_response())
                try:
                    async with asyncio.timeout(60):  # Adjustable timeout
                        await response_received.wait()
                    if response_data and "result" in response_data:
                        default_logger.debug(f"Received response for FileID {file_id}, CorrelationID: {correlation_id}")
                        return response_data["result"]
                    else:
                        default_logger.warning(f"No valid response received for FileID {file_id}, CorrelationID: {correlation_id}")
                        return 0
                except asyncio.TimeoutError:
                    default_logger.warning(f"Timeout waiting for response for FileID {file_id}, CorrelationID: {correlation_id}")
                    return 0
                finally:
                    consume_task.cancel()
                    await asyncio.sleep(0.5)
            finally:
                if connection and not connection.is_closed:
                    await connection.close()
        return None

    except Exception as e:
        default_logger.error(f"Error enqueuing task for FileID {file_id}: {e}", exc_info=True)
        raise
