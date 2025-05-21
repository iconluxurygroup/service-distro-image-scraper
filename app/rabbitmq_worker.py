import pika
import json
import logging
from typing import Dict, Any
from fastapi import BackgroundTasks

logger = logging.getLogger(__name__)

class RabbitMQProducer:
    def __init__(self, host: str = "localhost", queue_name: str = "db_update_queue"):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

    def connect(self):
        """Establish connection to RabbitMQ and declare a durable queue."""
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.host)
            )
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            logger.info(f"Connected to RabbitMQ and declared queue: {self.queue_name}")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}", exc_info=True)
            raise

    def publish_update(self, update_task: Dict[str, Any]):
        """Publish an update task to the queue with persistent delivery."""
        if not self.connection or self.connection.is_closed:
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
            raise

    def close(self):
        """Close the RabbitMQ connection."""
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.info("Closed RabbitMQ connection")

async def enqueue_db_update(
    file_id: str,
    sql: str,
    params: Dict[str, Any],
    background_tasks: BackgroundTasks,
    task_type: str = "db_update",
):
    """Enqueue a database update task to RabbitMQ."""
    producer = RabbitMQProducer()
    try:
        producer.connect()
        update_task = {
            "file_id": file_id,
            "task_type": task_type,
            "sql": sql,
            "params": params,
            "timestamp": datetime.datetime.now().isoformat(),
        }
        producer.publish_update(update_task)
        logger.info(f"Enqueued database update for FileID: {file_id}, SQL: {sql}")
    except Exception as e:
        logger.error(f"Error enqueuing database update for FileID: {file_id}: {e}", exc_info=True)
        raise
    finally:
        producer.close()