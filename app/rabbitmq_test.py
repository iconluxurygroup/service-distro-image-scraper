import asyncio
import logging
import aio_pika
import json
import uuid
import datetime
import aiormq
from typing import Optional
from rabbitmq_producer import RabbitMQProducer, get_producer
from rabbitmq_consumer import RabbitMQConsumer
from config import RABBITMQ_URL
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

import asyncio
import logging
import aio_pika
import json
import uuid
from rabbitmq_producer import RabbitMQProducer, get_producer
from rabbitmq_consumer import RabbitMQConsumer
from config import RABBITMQ_URL
from tenacity import retry, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

async def test_rabbitmq_producer(
    logger: Optional[logging.Logger] = None,
    timeout: float = 15.0
) -> bool:
    logger = logger or logging.getLogger(__name__)
    test_queue_name = f"test_queue_{uuid.uuid4().hex}"
    test_correlation_id = str(uuid.uuid4())
    producer = None

    try:
        async with asyncio.timeout(timeout):
            logger.info("Testing RabbitMQ producer connection...")
            producer = await get_producer(logger)
            await producer.connect()
            logger.info("Producer connected successfully")

            channel = producer.channel
            if not channel or channel.is_closed:
                await producer.connect()
                channel = producer.channel

            try:
                await channel.get_queue(test_queue_name)
                await channel.queue_delete(test_queue_name)
                logger.debug(f"Deleted existing test queue: {test_queue_name}")
            except aio_pika.exceptions.QueueEmpty:
                logger.debug(f"No existing test queue: {test_queue_name}")

            await channel.declare_queue(test_queue_name, durable=False, exclusive=False, auto_delete=True)
            logger.debug(f"Declared test queue: {test_queue_name}")

            test_task = {
                "file_id": "test_123",
                "task_type": "test_task",
                "sql": "SELECT 1",
                "params": {"test_param": "test_value"},
                "timestamp": datetime.datetime.now().isoformat(),
                "correlation_id": test_correlation_id,
            }
            await producer.publish_message(
                message=test_task,
                routing_key=test_queue_name,
                correlation_id=test_correlation_id
            )
            logger.info(f"Published test message to queue: {test_queue_name}, correlation_id: {test_correlation_id}")

            try:
                await channel.get_queue(test_queue_name)
                logger.info("Test queue verified successfully")
            except aio_pika.exceptions.QueueEmpty:
                logger.error("Test queue not found after declaration")
                return False

            logger.info("RabbitMQ producer test successful")
            return True

    except asyncio.TimeoutError:
        logger.error("Timeout during RabbitMQ producer test")
        return False
    except aio_pika.exceptions.ProbableAuthenticationError as e:
        logger.error(f"Authentication error: {e}. Check username, password, and virtual host in {RABBITMQ_URL}")
        return False
    except aio_pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}", exc_info=True)
        return False
    except aiormq.exceptions.ConnectionChannelError as e:
        logger.error(f"Channel error during test: {e}", exc_info=True)
        return False
    except aiormq.exceptions.ChannelPreconditionFailed as e:
        logger.error(f"Queue declaration conflict: {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Unexpected error during RabbitMQ producer test: {e}", exc_info=True)
        return False
    finally:
        try:
            if producer and producer.is_connected:
                if producer.channel and not producer.channel.is_closed:
                    try:
                        await producer.channel.queue_delete(test_queue_name)
                        logger.debug(f"Deleted test queue: {test_queue_name}")
                    except:
                        pass
                # Do not close producer for FastAPI
        except Exception as e:
            logger.warning(f"Error cleaning up test resources: {e}")

async def test_rabbitmq_connection(timeout: float = 30.0) -> bool:
    test_queue_name = f"test_queue_{uuid.uuid4().hex}"
    test_correlation_id = str(uuid.uuid4())
    test_message_received = asyncio.Event()

    producer = None
    consumer = None

    try:
        async with asyncio.timeout(timeout):
            logger.info("Testing RabbitMQ producer connection...")
            producer = await get_producer(logger)
            await producer.connect()
            logger.info("Producer connected successfully")

            logger.info("Testing RabbitMQ consumer connection...")
            consumer = RabbitMQConsumer(amqp_url=RABBITMQ_URL, queue_name=test_queue_name, producer=producer)
            await consumer.connect()
            logger.info("Consumer connected successfully")

            # Declare test queue
            channel = producer.channel
            await channel.declare_queue(test_queue_name, durable=False, auto_delete=True)
            logger.debug(f"Declared test queue: {test_queue_name}")

            # Callback to process message
            async def test_callback(message: aio_pika.IncomingMessage):
                logger.debug(f"Callback triggered: {message.body}")
                async with message.process():
                    if message.correlation_id == test_correlation_id:
                        test_message_received.set()
                        logger.debug("Test message received")

            await consumer.channel.get_queue(test_queue_name).consume(test_callback)
            logger.debug(f"Started consuming from {test_queue_name}")

            # Send test message
            test_task = {"correlation_id": test_correlation_id, "test": "data"}
            await producer.publish_message(
                message=test_task,
                routing_key=test_queue_name,
                correlation_id=test_correlation_id
            )
            logger.info(f"Published test message to {test_queue_name}")

            # Retry waiting for message
            @retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
            async def wait_for_message():
                async with asyncio.timeout(30.0):
                    await test_message_received.wait()

            await wait_for_message()
            logger.info("RabbitMQ connection test successful")
            return True

    except asyncio.TimeoutError:
        logger.error("Timeout during RabbitMQ connection test")
        return False
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        return False
    finally:
        if consumer:
            await consumer.close()
        if producer:
            await producer.close()