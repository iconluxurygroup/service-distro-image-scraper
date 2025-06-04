import aio_pika
import json
import logging
import datetime # Ensure datetime is imported
from typing import Dict, Any, Optional
# from fastapi import BackgroundTasks # Not used in this class directly
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import asyncio
import aiormq.exceptions

logger = logging.getLogger(__name__)

def datetime_converter(o: Any) -> str:
    """
    Converts datetime.datetime and datetime.date objects to ISO 8601 string format.
    To be used as the 'default' function for json.dumps().
    """
    if isinstance(o, datetime.datetime) or isinstance(o, datetime.date):
        return o.isoformat()
    # If you have other non-serializable types in the future, add their handlers here.
    # For example:
    # if isinstance(o, decimal.Decimal):
    #     return str(o)
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")


class RabbitMQProducer:
    _instance: Optional['RabbitMQProducer'] = None
    _lock = asyncio.Lock()

    def __init__(
        self,
        amqp_url: str = "amqp://app_user:app_password@localhost:5672/app_vhost", # Sensitive data, use env vars
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

    @property
    def is_connected(self) -> bool:
        """Dynamically checks if the connection and channel are active and not closed."""
        conn_ok = self.connection is not None and \
                    hasattr(self.connection, 'is_closed') and \
                    not self.connection.is_closed
        chan_ok = self.channel is not None and \
                    hasattr(self.channel, 'is_closed') and \
                    not self.channel.is_closed
        return conn_ok and chan_ok

    @classmethod
    async def get_producer(cls, **kwargs) -> 'RabbitMQProducer':
        async with cls._lock:
            if cls._instance is None or not cls._instance.is_connected:
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
                try:
                    await cls._instance.connect()
                    logger.info("Initialized/Re-initialized RabbitMQProducer instance and connected.")
                except Exception as e:
                    logger.error(f"Failed to connect during get_producer: {e}", exc_info=True)
                    raise
            else:
                logger.debug("Reusing existing connected RabbitMQProducer instance.")
            return cls._instance

    async def connect(self):
        if self.is_connected:
            logger.debug("Connect called, but already connected.")
            return
        try:
            logger.info("Attempting to connect to RabbitMQ...")
            async with asyncio.timeout(self.connection_timeout):
                self.connection = await aio_pika.connect_robust(
                    self.amqp_url,
                    connection_attempts=3,
                    retry_delay=5,
                )
                self.channel = await self.connection.channel()
                await self.channel.declare_queue(self.queue_name, durable=True)
                logger.info(f"Successfully connected to RabbitMQ and declared queue: {self.queue_name}")
        except asyncio.TimeoutError:
            logger.error("Timeout connecting to RabbitMQ.")
            self.connection = None
            self.channel = None
            raise
        except (aio_pika.exceptions.AMQPConnectionError,
                aio_pika.exceptions.ProbableAuthenticationError,
                aiormq.exceptions.ChannelAccessRefused) as e:
            logger.error(f"Error connecting to RabbitMQ: {e}", exc_info=True)
            self.connection = None
            self.channel = None
            raise
        except Exception as e:
            logger.error(f"Unexpected error during RabbitMQ connect: {e}", exc_info=True)
            self.connection = None
            self.channel = None
            raise


    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=10),
        retry=retry_if_exception_type((
            aio_pika.exceptions.AMQPError,
            asyncio.TimeoutError,
        )),
    )
    async def publish_message(self, message: Dict[str, Any], routing_key: Optional[str] = None, correlation_id: Optional[str] = None):
        if not self.is_connected:
            logger.warning("Publish called but not connected. Attempting to reconnect...")
            await self.connect()

        if not self.is_connected:
            logger.error("Still not connected after connect attempt in publish. Cannot publish.")
            raise RuntimeError("RabbitMQ connection unavailable for publishing.")

        try:
            actual_routing_key = routing_key or self.queue_name
            
            if self.channel is None or self.channel.is_closed:
                 logger.warning("Channel is None or closed in publish_message. Attempting to re-open.")
                 if self.connection is None or self.connection.is_closed:
                     logger.error("Connection is also closed/None. Cannot re-open channel.")
                     raise RuntimeError("RabbitMQ connection lost, cannot re-open channel.")
                 self.channel = await self.connection.channel()
                 logger.info("Channel re-opened.")

            if actual_routing_key != self.queue_name and actual_routing_key.startswith("select_response_"):
                await self.channel.declare_queue(
                    actual_routing_key,
                    durable=False,
                    exclusive=False,
                    auto_delete=True,
                    arguments={"x-expires": 600000}
                )
                logger.debug(f"Declared dynamic/response queue: {actual_routing_key}")

            async with asyncio.timeout(self.operation_timeout):
                # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                # MODIFIED LINE: Add the 'default' argument to json.dumps
                # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                message_body = json.dumps(message, default=datetime_converter)
                # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                await self.channel.default_exchange.publish(
                    aio_pika.Message(
                        body=message_body.encode(),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        correlation_id=correlation_id,
                        content_type="application/json",
                    ),
                    routing_key=actual_routing_key,
                )
                logger.info(
                    f"Published message to queue: {actual_routing_key}, "
                    f"correlation_id: {correlation_id}, task: {message_body[:200]}"
                )
        except asyncio.TimeoutError:
            logger.error(f"Timeout publishing message to {actual_routing_key}")
            raise
        except (aio_pika.exceptions.AMQPError, Exception) as e:
            logger.error(f"Failed to publish message to {actual_routing_key}: {e}", exc_info=True)
            raise

    async def publish_update(self, update_task: Dict[str, Any], routing_key: Optional[str] = None, correlation_id: Optional[str] = None):
        await self.publish_message(update_task, routing_key or self.queue_name, correlation_id)

    async def close(self):
        logger.info("Attempting to close RabbitMQ connection and channel.")
        channel_to_close = self.channel
        connection_to_close = self.connection

        self.channel = None
        self.connection = None

        try:
            if channel_to_close and not channel_to_close.is_closed:
                try:
                    await asyncio.wait_for(channel_to_close.close(), timeout=self.operation_timeout)
                    logger.info("Closed RabbitMQ channel.")
                except (asyncio.TimeoutError, aio_pika.exceptions.AMQPError, Exception) as e:
                    logger.warning(f"Error/Timeout closing RabbitMQ channel: {e}")
            
            if connection_to_close and not connection_to_close.is_closed:
                try:
                    await asyncio.wait_for(connection_to_close.close(), timeout=self.operation_timeout)
                    logger.info("Closed RabbitMQ connection.")
                except (asyncio.TimeoutError, aio_pika.exceptions.AMQPError, Exception) as e:
                    logger.warning(f"Error/Timeout closing RabbitMQ connection: {e}")
        except asyncio.CancelledError:
            logger.info("Close operation cancelled.")
            self.channel = None
            self.connection = None
            raise
        except Exception as e:
            logger.error(f"Unexpected error during RabbitMQ close: {e}", exc_info=True)
        finally:
            self.channel = None
            self.connection = None
            logger.info("RabbitMQProducer close operation finished.")
