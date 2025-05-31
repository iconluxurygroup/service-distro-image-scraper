import aio_pika
import json
import logging
import datetime
from typing import Dict, Any, Optional
from fastapi import BackgroundTasks
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import asyncio
import aiormq.exceptions

logger = logging.getLogger(__name__)
import aio_pika
import json
import logging
import datetime
from typing import Dict, Any, Optional
from fastapi import BackgroundTasks
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import asyncio
import aiormq.exceptions


class RabbitMQProducer:
    _instance = None
    _lock = asyncio.Lock()  # Class-level lock for thread-safe singleton

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

    @classmethod
    async def get_producer(cls, **kwargs) -> 'RabbitMQProducer':
        """Get or create a singleton RabbitMQProducer instance.

        Args:
            **kwargs: Optional overrides for amqp_url, queue_name, connection_timeout, operation_timeout.

        Returns:
            RabbitMQProducer: The singleton producer instance, connected to RabbitMQ.
        """
        async with cls._lock:
            if cls._instance is None or not cls._instance.is_connected:
                # Use default values or kwargs overrides
                amqp_url = kwargs.get("amqp_url", "amqp://app_user:app_password@localhost:5672/app_vhost")
                queue_name = kwargs.get("queue_name", "db_update_queue")
                connection_timeout = kwargs.get("connection_timeout", 10.0)
                operation_timeout = kwargs.get("operation_timeout", 5.0)

                cls._instance = cls(
                    amqp_url=amqp_url,
                    queue_name=queue_name,
                    connection_timeout=connection_timeout,
                    operation_timeout=operation_timeout
                )
                await cls._instance.connect()
                logger.info("Initialized new RabbitMQProducer instance")
            else:
                logger.debug("Reusing existing RabbitMQProducer instance")
            return cls._instance

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
        asyncio.TimeoutError,
        aiormq.exceptions.ChannelNotFoundEntity
    )),
)
    async def publish_message(self, message: Dict[str, Any], routing_key: str = None, correlation_id: Optional[str] = None):
        """Publish a message to the queue with optional correlation_id."""
        if not self.is_connected or not self.connection or self.connection.is_closed:
            await self.connect()

        try:
            async with asyncio.timeout(self.operation_timeout):
                message_body = json.dumps(message)
                queue_name = routing_key or self.queue_name
                # Declare queue dynamically for response queues
                if queue_name != self.queue_name and queue_name.startswith("select_response_"):
                    try:
                        await self.channel.declare_queue(
                            queue_name,
                            durable=False,
                            exclusive=False,
                            auto_delete=True,
                            arguments={"x-expires": 600000}
                        )
                        logger.debug(f"Declared response queue: {queue_name}")
                    except aiormq.exceptions.ChannelNotFoundEntity:
                        logger.warning(f"Queue {queue_name} not found, declared anew")
                elif queue_name == self.queue_name:
                    # Ensure main queue is declared
                    await self.channel.declare_queue(
                        queue_name,
                        durable=True,
                        exclusive=False,
                        auto_delete=False
                    )

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
            logger.error(f"Authentication error: {e}", exc_info=True)
            raise
        except aiormq.exceptions.ChannelAccessRefused as e:
            logger.error(f"Permissions error: {e}", exc_info=True)
            raise
        except asyncio.TimeoutError:
            logger.error(f"Timeout publishing message to {queue_name}")
            raise
        except Exception as e:
            logger.error(f"Failed to publish message to {queue_name}: {e}", exc_info=True)
            self.is_connected = False
            raise

    async def publish_update(self, update_task: Dict[str, Any], routing_key: str = None, correlation_id: Optional[str] = None):
        await self.publish_message(update_task, routing_key, correlation_id)

    async def close(self):
        """Close the RabbitMQ connection and channel gracefully."""
        try:
            if self.channel and not self.channel.is_closed:
                try:
                    await asyncio.wait_for(self.channel.close(), timeout=5)
                    logger.info("Closed RabbitMQ channel")
                except (asyncio.TimeoutError, aio_pika.exceptions.AMQPError) as e:
                    logger.warning(f"Error closing channel: {e}")
            if self.connection and not self.connection.is_closed:
                try:
                    await asyncio.wait_for(self.connection.close(), timeout=5)
                    logger.info("Closed RabbitMQ connection")
                except (asyncio.TimeoutError, aio_pika.exceptions.AMQPError) as e:
                    logger.warning(f"Error closing connection: {e}")
        except asyncio.CancelledError:
            logger.info("Close operation cancelled, ensuring cleanup")
            raise
        except Exception as e:
            logger.error(f"Error closing RabbitMQ connection: {e}", exc_info=True)
        finally:
            self.is_connected = False
            self.channel = None
            self.connection = None