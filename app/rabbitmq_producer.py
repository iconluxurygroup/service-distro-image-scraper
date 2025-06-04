import aio_pika
import json
import logging
import datetime
from typing import Dict, Any, Optional
# from fastapi import BackgroundTasks # Not used in this class directly
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import asyncio
import aiormq.exceptions
def datetime_converter(o: Any) -> str:
    if isinstance(o, datetime.datetime) or isinstance(o, datetime.date):
        return o.isoformat()
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")
logger = logging.getLogger(__name__)

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
        # self.is_connected = False # REMOVE this attribute

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
            # Use the new 'is_connected' property for the check
            if cls._instance is None or not cls._instance.is_connected:
                amqp_url = kwargs.get("amqp_url", "amqp://app_user:app_password@localhost:5672/app_vhost")
                queue_name = kwargs.get("queue_name", "db_update_queue")
                connection_timeout = kwargs.get("connection_timeout", 10.0)
                operation_timeout = kwargs.get("operation_timeout", 5.0)

                # Create a new instance or re-initialize if the existing one is disconnected
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
                    # If connect fails, _instance might be partially set up but not connected.
                    # The next call to get_producer will try again due to 'not cls._instance.is_connected'.
                    # Optionally, set cls._instance = None here if connect fails to force full re-creation.
                    # For now, relying on the 'is_connected' check for the next call.
                    raise # Re-raise the exception so the caller knows connection failed
            else:
                logger.debug("Reusing existing connected RabbitMQProducer instance.")
            return cls._instance

    async def connect(self):
        if self.is_connected: # Use the property
            logger.debug("Connect called, but already connected.")
            return
        try:
            logger.info("Attempting to connect to RabbitMQ...")
            async with asyncio.timeout(self.connection_timeout):
                self.connection = await aio_pika.connect_robust(
                    self.amqp_url,
                    connection_attempts=3, # RobustConnection handles this
                    retry_delay=5,         # RobustConnection handles this
                )
                self.channel = await self.connection.channel()
                # Declare the main default queue for the producer
                await self.channel.declare_queue(self.queue_name, durable=True)
                # self.is_connected = True # REMOVE: Property handles this
                logger.info(f"Successfully connected to RabbitMQ and declared queue: {self.queue_name}")
        except asyncio.TimeoutError:
            logger.error("Timeout connecting to RabbitMQ.")
            self.connection = None # Ensure connection is None if timeout
            self.channel = None
            raise
        except (aio_pika.exceptions.AMQPConnectionError,
                aio_pika.exceptions.ProbableAuthenticationError,
                aiormq.exceptions.ChannelAccessRefused) as e:
            logger.error(f"Error connecting to RabbitMQ: {e}", exc_info=True)
            self.connection = None # Ensure connection is None on error
            self.channel = None
            raise
        except Exception as e: # Catch any other unexpected errors during connect
            logger.error(f"Unexpected error during RabbitMQ connect: {e}", exc_info=True)
            self.connection = None
            self.channel = None
            raise


    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=10),
        retry=retry_if_exception_type((
            aio_pika.exceptions.AMQPError, # Broad category for RabbitMQ issues
            # aio_pika.exceptions.ChannelClosed, # Covered by AMQPError often
            # aiormq.exceptions.ChannelAccessRefused, # Should be caught on connect
            asyncio.TimeoutError,
            # aiormq.exceptions.ChannelNotFoundEntity # Should be handled by declare_queue logic
        )),
    )
    async def publish_message(self, message: Dict[str, Any], routing_key: Optional[str] = None, correlation_id: Optional[str] = None):
        if not self.is_connected: # Use the property
            logger.warning("Publish called but not connected. Attempting to reconnect...")
            await self.connect() # Try to connect first

        # Double check after connect attempt
        if not self.is_connected:
            logger.error("Still not connected after connect attempt in publish. Cannot publish.")
            raise RuntimeError("RabbitMQ connection unavailable for publishing.")

        try:
            actual_routing_key = routing_key or self.queue_name # Use provided routing_key or default producer queue
            
            # It's good practice to ensure the channel exists before using it,
            # especially if connect_robust had to re-establish things.
            if self.channel is None or self.channel.is_closed:
                 logger.warning("Channel is None or closed in publish_message. Attempting to re-open.")
                 if self.connection is None or self.connection.is_closed:
                     logger.error("Connection is also closed/None. Cannot re-open channel.")
                     raise RuntimeError("RabbitMQ connection lost, cannot re-open channel.")
                 self.channel = await self.connection.channel()
                 logger.info("Channel re-opened.")

            # Dynamically declare queue if it's a special response queue
            # For the main queue, it's declared in connect() and assumed to exist.
            # Robust channel should re-declare on reconnect if needed.
            if actual_routing_key != self.queue_name and actual_routing_key.startswith("select_response_"):
                await self.channel.declare_queue(
                    actual_routing_key,
                    durable=False,      # Response queues are often not durable
                    exclusive=False,    # Not exclusive if multiple consumers might (unlikely for response)
                    auto_delete=True,   # Auto-delete when consumer disconnects
                    arguments={"x-expires": 600000} # 10 min expiry
                )
                logger.debug(f"Declared dynamic/response queue: {actual_routing_key}")
            # No need to re-declare self.queue_name here, connect() and RobustConnection handle it.

            async with asyncio.timeout(self.operation_timeout):
                message_body = json.dumps(message, default=datetime_converter)
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
            # Do not set self.is_connected = False here; RobustConnection might recover.
            # The property will reflect the actual state.
            raise
        except (aio_pika.exceptions.AMQPError, Exception) as e: # Catch AMQP and other errors
            logger.error(f"Failed to publish message to {actual_routing_key}: {e}", exc_info=True)
            # The 'is_connected' property will reflect if connection/channel is truly lost
            raise

    async def publish_update(self, update_task: Dict[str, Any], routing_key: Optional[str] = None, correlation_id: Optional[str] = None):
        # This is just an alias, which is fine.
        await self.publish_message(update_task, routing_key or self.queue_name, correlation_id)

    async def close(self):
        logger.info("Attempting to close RabbitMQ connection and channel.")
        # Store connection/channel locally to avoid issues if self.channel/self.connection
        # are set to None by another coroutine during this close operation.
        channel_to_close = self.channel
        connection_to_close = self.connection

        self.channel = None       # Set to None first to prevent new operations
        self.connection = None    # from using them while closing.

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
            # Ensure resources are still marked as None if cancellation happened before that.
            self.channel = None
            self.connection = None
            raise
        except Exception as e:
            logger.error(f"Unexpected error during RabbitMQ close: {e}", exc_info=True)
        finally:
            # Ensure these are None so 'is_connected' property becomes False
            self.channel = None
            self.connection = None
            logger.info("RabbitMQProducer close operation finished.")
            # If this was the singleton, we might want to clear _instance so next get_producer starts fresh
            # async with RabbitMQProducer._lock: # Ensure thread safety for _instance modification
            #     if RabbitMQProducer._instance is self:
            #         RabbitMQProducer._instance = None
            #         logger.info("Cleared singleton _instance reference.")
            # Note: Clearing _instance on close can be debated. If closed explicitly,
            # it might be intended to be permanently shut. If it's a shared singleton,
            # perhaps it shouldn't be cleared, and get_producer should always revive it.
            # For now, relying on is_connected for get_producer to re-init.