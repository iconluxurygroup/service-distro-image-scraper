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
                # Do not close producer, as it's used globally
        except Exception as e:
            logger.warning(f"Error cleaning up test resources: {e}")

async def test_rabbitmq_connection(
    logger: Optional[logging.Logger] = None,
    timeout: float = 30.0
) -> bool:
    logger = logger or logging.getLogger(__name__)
    test_queue_name = f"test_queue_{uuid.uuid4().hex}"
    test_correlation_id = str(uuid.uuid4())
    test_message_received = asyncio.Event()
    received_message = None

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

            async def test_callback(message: aio_pika.IncomingMessage):
                nonlocal received_message
                logger.debug(f"Callback triggered for message with correlation_id: {message.correlation_id}")
                try:
                    async with message.process():
                        logger.debug(f"Processing message: {message.body[:100]}")
                        if message.correlation_id == test_correlation_id:
                            received_message = json.loads(message.body.decode())
                            logger.debug(f"Received test message: {received_message}")
                            test_message_received.set()
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error in callback: {e}", exc_info=True)
                except Exception as e:
                    logger.error(f"Error in callback: {e}", exc_info=True)

            queue = await consumer.channel.get_queue(test_queue_name)
            await queue.consume(test_callback)
            logger.debug(f"Started consuming from test queue: {test_queue_name}")

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

            # Check queue message count
            queue_state = await channel.get_queue(test_queue_name)
            logger.debug(f"Queue {test_queue_name} has {queue_state.message_count} messages")

            # Increase timeout to 20 seconds
            async with asyncio.timeout(20.0):
                await test_message_received.wait()
            
            if received_message and received_message.get("correlation_id") == test_correlation_id:
                logger.info("RabbitMQ connection test successful: message sent and received correctly")
                return True
            else:
                logger.error("RabbitMQ connection test failed: message not received or mismatched")
                return False

    except asyncio.TimeoutError:
        logger.error("Timeout during RabbitMQ connection test")
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
        logger.error(f"Unexpected error during RabbitMQ connection test: {e}", exc_info=True)
        return False
    finally:
        try:
            if consumer:
                await consumer.close()
            if producer and producer.is_connected:
                if producer.channel and not producer.channel.is_closed:
                    try:
                        await producer.channel.queue_delete(test_queue_name)
                        logger.debug(f"Deleted test queue: {test_queue_name}")
                    except:
                        pass
                await producer.close()
        except Exception as e:
            logger.warning(f"Error cleaning up test resources: {e}")
        tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        await asyncio.sleep(0.5)  # Increased to ensure task cancellation