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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

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
        logger = logger or logging.getLogger(__name__)
        async with cls._lock:
            if cls._instance is None or not cls._instance.is_connected:
                cls._instance = cls()
                await cls._instance.connect()
                logger.info("Initialized RabbitMQProducer")
            return cls._instance

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((aio_pika.exceptions.AMQPError, aiormq.exceptions.ChannelInvalidStateError))
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
                for queue_name in [self.queue_name, self.new_queue_name, self.response_queue_name]:
                    await self.channel.declare_queue(queue_name, durable=True)
                    logger.debug(f"Declared queue {queue_name}")
                self.is_connected = True
                logger.info("RabbitMQProducer connected")
            except Exception as e:
                logger.error(f"Failed to connect RabbitMQProducer: {e}", exc_info=True)
                await self.close()
                raise

    async def close(self):
        async with self._lock:
            try:
                if self.channel and not self.channel.is_closed:
                    await asyncio.wait_for(self.channel.close(), timeout=5.0)
                    logger.info("Closed RabbitMQ channel")
                if self.connection and not self.connection.is_closed:
                    await asyncio.wait_for(self.connection.close(), timeout=5.0)
                    logger.info("Closed RabbitMQ connection")
                self.is_connected = False
            except asyncio.TimeoutError:
                logger.warning("Timeout closing RabbitMQ resources")
            except Exception as e:
                logger.error(f"Error closing RabbitMQProducer: {e}", exc_info=True)
            finally:
                self.channel = None
                self.connection = None

    async def check_connection(self):
        if not self.is_connected or not self.connection or self.connection.is_closed or not self.channel or self.channel.is_closed:
            await self.connect()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(aio_pika.exceptions.AMQPError)
    )
    async def publish_message(self, message: Dict[str, Any], routing_key: str, correlation_id: str = None):
        try:
            await self.check_connection()
            message_body = json.dumps(message, default=lambda o: o.isoformat() if isinstance(o, datetime.datetime) else str(o))
            await self.channel.default_exchange.publish(
                aio_pika.Message(
                    body=message_body.encode(),
                    correlation_id=correlation_id,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                ),
                routing_key=routing_key
            )
            logger.info(f"Published message to {routing_key}: {message}")
        except Exception as e:
            logger.error(f"Failed to publish message to {routing_key}: {e}", exc_info=True)
            self.is_connected = False
            await self.close()
            raise

    async def enqueue_db_update(
        self,
        file_id: str,
        sql: str,
        params: Dict[str, Any],
        task_type: str = "db_update",
        queue_name: str = "db_update_queue",
        correlation_id: str = None
    ):
        correlation_id = correlation_id or str(uuid.uuid4())
        task = {
            "file_id": file_id,
            "task_type": task_type,
            "sql": sql,
            "params": params,
            "timestamp": datetime.datetime.now().isoformat(),
            "correlation_id": correlation_id
        }
        await self.publish_message(task, routing_key=queue_name, correlation_id=correlation_id)