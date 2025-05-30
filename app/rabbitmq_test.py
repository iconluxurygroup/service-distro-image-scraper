import asyncio
import logging
import aio_pika
import json
import uuid
import datetime
import aiormq
from typing import Optional
from rabbitmq_producer import RabbitMQProducer, get_producer
from config import RABBITMQ_URL

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
    """
    Test RabbitMQ producer connection on app startup.
    Publishes a test message to a temporary queue and verifies queue operations.
    Returns True if successful, False otherwise.
    """
    logger = logger or logging.getLogger(__name__)
    test_queue_name = f"test_queue_{uuid.uuid4().hex}"
    test_correlation_id = str(uuid.uuid4())
    producer = None

    try:
        async with asyncio.timeout(timeout):
            # Initialize producer
            logger.info("Testing RabbitMQ producer connection...")
            producer = await get_producer(logger)
            await producer.connect()
            logger.info("Producer connected successfully")

            # Declare test queue
            channel = producer.channel
            if not channel or channel.is_closed:
                await producer.connect()
                channel = producer.channel
            await channel.declare_queue(test_queue_name, durable=False, exclusive=False, auto_delete=True)
            logger.debug(f"Declared test queue: {test_queue_name}")

            # Publish test message
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

            # Verify queue existence
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
    except Exception as e:
        logger.error(f"Unexpected error during RabbitMQ producer test: {e}", exc_info=True)
        return False
    finally:
        # Clean up
        try:
            if producer and producer.is_connected:
                if producer.channel and not producer.channel.is_closed:
                    await producer.channel.queue_delete(test_queue_name)
                    logger.debug(f"Deleted test queue: {test_queue_name}")
                # Do not close producer, as it's used globally
        except Exception as e:
            logger.warning(f"Error cleaning up test resources: {e}")