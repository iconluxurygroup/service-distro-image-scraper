import pika
import json
import logging
from typing import Dict, Any, Optional
import datetime
from fastapi import BackgroundTasks
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

class RabbitMQProducer:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5672,
        username: str = "guest",
        password: str = "guest",
        queue_name: str = "db_update_queue",
    ):
        self.host = host
        self.port = port
        self.credentials = pika.PlainCredentials(username, password)
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.is_connected = False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(pika.exceptions.AMQPConnectionError),
        before_sleep=lambda retry_state: logger.info(
            f"Retrying RabbitMQ connection (attempt {retry_state.attempt_number}/3)"
        ),
    )
    def connect(self):
        """Establish connection to RabbitMQ and declare a durable queue."""
        if self.is_connected:
            return
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.host,
                    port=self.port,
                    credentials=self.credentials,
                    connection_attempts=3,
                    retry_delay=5,
                )
            )
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            self.is_connected = True
            logger.info(f"Connected to RabbitMQ and declared queue: {self.queue_name}")
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}", exc_info=True)
            raise

    def publish_update(self, update_task: Dict[str, Any]):
        """Publish an update task to the queue with persistent delivery."""
        if not self.is_connected or not self.connection or self.connection.is_closed:
            self.connect()

        try:
            message = json.dumps(update_task)
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent
                ),
            )
            logger.info(f"Published update task to queue: {self.queue_name}, task: {message}")
        except Exception as e:
            logger.error(f"Failed to publish update task: {e}", exc_info=True)
            self.is_connected = False
            raise

    def close(self):
        """Close the RabbitMQ connection."""
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            self.is_connected = False
            logger.info("Closed RabbitMQ connection")

from typing import Dict, Any, Optional
import logging
from fastapi import BackgroundTasks
from .rabbitmq_producer import RabbitMQProducer

logger = logging.getLogger(__name__)

async def enqueue_db_update(
    file_id: str,
    sql: str,
    params: Dict[str, Any],
    background_tasks: BackgroundTasks,
    task_type: str = "db_update",
    producer: Optional[RabbitMQProducer] = None,
    response_queue: Optional[str] = None,
):
    """Enqueue a database update task to RabbitMQ."""
    if producer is None:
        producer = RabbitMQProducer()
        should_close = True
    else:
        should_close = False

    try:
        producer.connect()
        update_task = {
            "file_id": file_id,
            "task_type": task_type,
            "sql": sql,
            "params": params,
            "timestamp": datetime.datetime.now().isoformat(),
            "response_queue": response_queue,
        }
        producer.publish_update(update_task)
        logger.info(f"Enqueued database update for FileID: {file_id}, TaskType: {task_type}, SQL: {sql[:100]}")
    except Exception as e:
        logger.error(f"Error enqueuing database update for FileID: {file_id}: {e}", exc_info=True)
        raise
    finally:
        if should_close:
            producer.close()