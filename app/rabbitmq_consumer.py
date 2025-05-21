#!/usr/bin/env python
import pika
import json
import logging
import time
import asyncio
import psutil
import signal
import sys
import datetime
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from database_config import async_engine
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class RabbitMQConsumer:
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
        self.is_consuming = False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(pika.exceptions.AMQPConnectionError),
        before_sleep=lambda retry_state: logger.info(
            f"Retrying RabbitMQ connection (attempt {retry_state.attempt_number}/3)"
        ),
    )
    def connect(self):
        if self.connection and not self.connection.is_closed:
            return
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                credentials=self.credentials,
            )
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=True)
        self.channel.basic_qos(prefetch_count=1)
        logger.info(f"Connected to RabbitMQ, consuming from queue: {self.queue_name}")

    def close(self):
        if self.channel and not self.channel.is_closed:
            if self.is_consuming:
                try:
                    self.channel.stop_consuming()
                    logger.info("Stopped consuming messages")
                except Exception as e:
                    logger.error(f"Error stopping consumption: {e}", exc_info=True)
            self.channel.close()
            logger.info("Closed RabbitMQ channel")
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.info("Closed RabbitMQ connection")
        self.is_consuming = False

    def purge_queue(self):
        try:
            if not self.connection or self.connection.is_closed or not self.channel or self.channel.is_closed:
                self.connect()
            purge_count = self.channel.queue_purge(self.queue_name)
            logger.info(f"Manually purged {purge_count} messages from queue: {self.queue_name}")
            return purge_count
        except Exception as e:
            logger.error(f"Error purging queue {self.queue_name}: {e}", exc_info=True)
            raise

    def start_consuming(self):
        while True:
            try:
                self.connect()
                self.is_consuming = True
                self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback)
                logger.info("Waiting for messages. To exit press CTRL+C")
                self.channel.start_consuming()
            except pika.exceptions.AMQPConnectionError as e:
                logger.error(f"AMQP connection error: {e}, reconnecting in 5 seconds", exc_info=True)
                self.is_consuming = False
                time.sleep(5)
            except KeyboardInterrupt:
                logger.info("Received KeyboardInterrupt, shutting down consumer")
                self.close()
                break
            except Exception as e:
                logger.error(f"Unexpected error in consumer: {e}, reconnecting in 5 seconds", exc_info=True)
                self.is_consuming = False
                self.close()
                time.sleep(5)

    async def execute_update(task, conn, logger):
        async with conn:
            try:
                file_id = task.get("file_id")
                sql = task.get("sql")
                params = task.get("params", {})
                
                if not isinstance(params, dict):
                    raise ValueError(f"Invalid params format: {params}, expected dict")
                
                logger.debug(f"Executing UPDATE/INSERT for FileID {file_id}: {sql}, params: {params}")
                result = await conn.execute(text(sql), params)
                await conn.commit()
                rowcount = result.rowcount
                logger.info(f"Worker PID {psutil.Process().pid}: UPDATE/INSERT affected {rowcount} rows for FileID {file_id}")
                return {"rowcount": rowcount}
            
            except SQLAlchemyError as e:
                logger.error(f"TaskType: {task.get('task_type')}, FileID: {file_id}, "
                            f"Database error executing UPDATE/INSERT: {sql}, params: {params}, error: {str(e)}", 
                            exc_info=True)
                raise
            except Exception as e:
                logger.error(f"TaskType: {task.get('task_type')}, FileID: {file_id}, "
                            f"Unexpected error executing UPDATE/INSERT: {sql}, params: {params}, error: {str(e)}", 
                            exc_info=True)
                raise
    async def execute_select(task, conn, logger):
        async with conn:
            try:
                file_id = task.get("file_id")
                sql = task.get("sql")
                params = task.get("params", {})
                
                # Extract ids from params and convert to tuple for IN clause
                if isinstance(params, dict) and "ids" in params:
                    query_params = tuple(params["ids"])
                else:
                    raise ValueError(f"Invalid params format: {params}, expected dict with 'ids' key")
                
                logger.debug(f"Executing SELECT for FileID {file_id}: {sql}, params: {query_params}")
                result = await conn.execute(text(sql), query_params)
                rows = result.fetchall()
                columns = result.keys()
                results = [dict(zip(columns, row)) for row in rows]
                logger.info(f"Worker PID {psutil.Process().pid}: SELECT returned {len(results)} rows for FileID {file_id}")
                return {"results": results}
            
            except SQLAlchemyError as e:
                logger.error(f"TaskType: {task.get('task_type')}, FileID: {file_id}, "
                            f"Database error executing SELECT: {sql}, params: {params}, error: {str(e)}", 
                            exc_info=True)
                raise
            except Exception as e:
                logger.error(f"TaskType: {task.get('task_type')}, FileID: {file_id}, "
                            f"Unexpected error executing SELECT: {sql}, params: {params}, error: {str(e)}", 
                            exc_info=True)
                raise

    async def execute_sort_order_update(self, params: dict, file_id: str):
        try:
            entry_id = params.get("entry_id")
            result_id = params.get("result_id")
            sort_order = params.get("sort_order")

            if not all([entry_id, result_id, sort_order is not None]):
                logger.error(
                    f"Invalid parameters for update_sort_order task, FileID: {file_id}. "
                    f"Required: entry_id, result_id, sort_order. Got: {params}"
                )
                return False

            sql = """
                UPDATE utb_ImageScraperResult
                SET SortOrder = :sort_order
                WHERE EntryID = :entry_id AND ResultID = :result_id
            """
            update_params = {
                "sort_order": sort_order,
                "entry_id": entry_id,
                "result_id": result_id
            }

            async with async_engine.begin() as conn:
                result = await conn.execute(text(sql), update_params)
                await conn.commit()
                rowcount = result.rowcount if result.rowcount is not None else 0
                logger.info(
                    f"TaskType: update_sort_order, FileID: {file_id}, Executed SQL: {sql[:100]}, "
                    f"params: {update_params}, affected {rowcount} rows"
                )

                if rowcount == 0:
                    logger.warning(f"No rows updated for FileID: {file_id}, EntryID: {entry_id}, ResultID: {result_id}")
                    return False
                return True
        except SQLAlchemyError as e:
            logger.error(f"Database error updating SortOrder for FileID: {file_id}, EntryID: {entry_id}: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Error updating SortOrder for FileID: {file_id}, EntryID: {entry_id}: {e}", exc_info=True)
            return False

    def callback(self, ch, method, properties, body):
        conn = None
        try:
            message = body.decode()
            task = json.loads(message)
            file_id = task.get("file_id", "unknown")
            task_type = task.get("task_type", "unknown")
            response_queue = task.get("response_queue")
            logger.info(f"Received task for FileID: {file_id}, TaskType: {task_type}, Task: {message[:200]}")

            loop = asyncio.get_event_loop()
            if task_type == "select_deduplication":
                async def run_select():
                    nonlocal conn
                    async with async_engine.connect() as conn:
                        result = await self.execute_select(task, conn, logger)
                        if response_queue:
                            response_message = json.dumps(result)
                            ch.basic_publish(
                                exchange="",
                                routing_key=response_queue,
                                body=response_message,
                                properties=pika.BasicProperties(
                                    correlation_id=properties.correlation_id
                                )
                            )
                            logger.info(f"Sent {len(result['results'])} deduplication results to {response_queue} for FileID: {file_id}")
                        return result
                success = loop.run_until_complete(run_select())
            elif task_type == "update_sort_order" and task.get("sql") == "UPDATE_SORT_ORDER":
                success = loop.run_until_complete(self.execute_sort_order_update(task.get("params", {}), file_id))
            else:
                async def run_update():
                    nonlocal conn
                    async with async_engine.begin() as conn:
                        result = await self.execute_update(task, conn, logger)
                        return result
                success = loop.run_until_complete(run_update())

            if success:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                logger.info(f"Successfully processed {task_type} for FileID: {file_id}, Acknowledged")
            else:
                logger.warning(f"Failed to process {task_type} for FileID: {file_id}; re-queueing")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                time.sleep(2)
        except Exception as e:
            logger.error(
                f"Error processing message for FileID: {file_id}, TaskType: {task_type}: {e}",
                exc_info=True
            )
            # Ensure connection is closed
            if conn is not None and not conn.closed:
                loop.run_until_complete(conn.close())
                logger.info(f"Closed database connection for FileID: {file_id}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            time.sleep(2)

    async def test_task(self, task: dict):
        file_id = task.get("file_id", "unknown")
        task_type = task.get("task_type", "unknown")
        sql = task.get("sql")
        params = task.get("params", {})
        response_queue = task.get("response_queue")
        
        logger.info(f"Testing task for FileID: {file_id}, TaskType: {task_type}")
        if task_type == "select_deduplication":
            success = await self.execute_select(sql, params, task_type, file_id, response_queue)
        elif task_type == "update_sort_order" and sql == "UPDATE_SORT_ORDER":
            success = await self.execute_sort_order_update(params, file_id)
        else:
            success = await self.execute_update(sql, params, task_type, file_id)
        
        logger.info(f"Test task result for FileID: {file_id}, TaskType: {task_type}: {'Success' if success else 'Failed'}")
        return success

def signal_handler(consumer):
    def handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down gracefully...")
        consumer.close()
        sys.exit(0)
    return handler

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="RabbitMQ Consumer with manual queue clear")
    parser.add_argument("--clear-queue", action="store_true", help="Manually clear the queue and exit")
    args = parser.parse_args()

    consumer = RabbitMQConsumer()
    signal.signal(signal.SIGINT, signal_handler(consumer))
    signal.signal(signal.SIGTERM, signal_handler(consumer))

    if args.clear_queue:
        try:
            consumer.purge_queue()
            logger.info("Queue cleared successfully. Exiting.")
            sys.exit(0)
        except Exception as e:
            logger.error(f"Failed to clear queue: {e}", exc_info=True)
            sys.exit(1)

    sample_task = {
        "file_id": "321",
        "task_type": "update_sort_order",
        "sql": "UPDATE_SORT_ORDER",
        "params": {
            "entry_id": "119061",
            "result_id": "1868277",
            "sort_order": 1
        },
        "timestamp": "2025-05-21T12:34:08.307076"
    }

    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(consumer.test_task(sample_task))
        consumer.start_consuming()
    except KeyboardInterrupt:
        consumer.close()
    except Exception as e:
        logger.error(f"Error in consumer: {e}", exc_info=True)
        consumer.close()