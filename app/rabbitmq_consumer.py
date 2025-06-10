import aio_pika
import json
import logging
import asyncio
import signal
import sys
import uuid
from rabbitmq_producer import RabbitMQProducer
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from database_config import async_engine
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import psutil
from typing import Optional, Dict, Any,Union,Tuple
import aiormq.exceptions

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

import aio_pika
import json
import logging
import asyncio
import signal
import sys
import datetime
from rabbitmq_producer import RabbitMQProducer
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from database_config import async_engine
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import psutil
from typing import Optional, Dict, Any
import aiormq.exceptions

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class RabbitMQConsumer:
    def __init__(
        self,
        amqp_url: str = "amqp://app_user:app_password@localhost:5672/app_vhost",
        queue_name: str = "db_update_queue",
        connection_timeout: float = 30.0,  # Increased from 10.0
        operation_timeout: float = 15.0,
    ):
        logger.debug("Initializing RabbitMQConsumer")
        self.amqp_url = amqp_url
        self.queue_name = queue_name
        self.connection_timeout = connection_timeout
        self.operation_timeout = operation_timeout
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.is_consuming = False
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((aiormq.exceptions.ChannelInvalidStateError, aio_pika.exceptions.AMQPConnectionError))
    )
    async def declare_queue_with_retry(self, channel, queue_name, **kwargs):
        """Declare a queue with retry logic."""
        return await channel.declare_queue(queue_name, **kwargs)
    async def connect(self):
        """Establish an async robust connection to RabbitMQ."""
        if self.connection and not self.connection.is_closed:
            return
        try:
            async with asyncio.timeout(self.connection_timeout):
                self.connection = await aio_pika.connect_robust(
                    self.amqp_url,
                    connection_attempts=3,
                    retry_delay=5,
                )
                self.channel = await self.connection.channel()
                if not self.channel:
                    raise aio_pika.exceptions.AMQPConnectionError("Failed to create channel")
                await self.channel.set_qos(prefetch_count=1)

                if self.queue_name.startswith("select_response_"):
                    try:
                        await self.declare_queue_with_retry(
                            self.channel,
                            self.queue_name,
                            durable=False,  # Consistent with producer
                            exclusive=False,
                            auto_delete=True,
                            arguments={"x-expires": 600000}
                        )
                        logger.debug(f"Declared response queue: {self.queue_name}")
                    except aiormq.exceptions.ChannelNotFoundEntity:
                        logger.warning(f"Queue {self.queue_name} not found, creating it")
                        await self.declare_queue_with_retry(
                            self.channel,
                            self.queue_name,
                            durable=False,
                            exclusive=False,
                            auto_delete=True,
                            arguments={"x-expires": 600000}
                        )
                        logger.debug(f"Created response queue: {self.queue_name}")
                    except aiormq.exceptions.ChannelPreconditionFailed:
                        logger.warning(f"Queue {self.queue_name} has incompatible settings, generating new name")
                        self.queue_name = f"{self.queue_name}_{uuid.uuid4().hex[:8]}"
                        await self.declare_queue_with_retry(
                            self.channel,
                            self.queue_name,
                            durable=False,
                            exclusive=False,
                            auto_delete=True,
                            arguments={"x-expires": 600000}
                        )
                        logger.debug(f"Created new response queue: {self.queue_name}")
                else:
                    await self.declare_queue_with_retry(
                        self.channel,
                        self.queue_name,
                        durable=True,
                        exclusive=False,
                        auto_delete=False
                    )
                    logger.debug(f"Declared main queue: {self.queue_name}")
                logger.info(f"Connected to RabbitMQ, consuming from queue: {self.queue_name}")
        except asyncio.TimeoutError:
            logger.error("Timeout connecting to RabbitMQ")
            raise
        except aio_pika.exceptions.ProbableAuthenticationError as e:
            logger.error(f"Authentication error: {e}", exc_info=True)
            raise
        except aio_pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}", exc_info=True)
            raise
    async def get_channel(self) -> aio_pika.Channel:
        """Get the RabbitMQ channel, connecting if necessary."""
        if not self.channel or self.channel.is_closed:
            await self.connect()
        if not self.channel:
            raise aio_pika.exceptions.AMQPConnectionError("Failed to establish channel")
        return self.channel
    async def close(self):
        """Close the RabbitMQ connection and channel gracefully."""
        try:
            if self.is_consuming:
                self.is_consuming = False
                logger.info("Stopped consuming messages")
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
            self.is_consuming = False
            self.channel = None
            self.connection = None
    async def purge_queue(self):
        """Purge all messages from the queue."""
        try:
            if not self.connection or self.connection.is_closed or not self.channel or self.channel.is_closed:
                await self.connect()
            queue = await self.channel.get_queue(self.queue_name)
            purge_count = await queue.purge()
            logger.info(f"Purged {purge_count} messages from queue: {self.queue_name}")
            return purge_count
        except Exception as e:
            logger.error(f"Error purging queue {self.queue_name}: {e}", exc_info=True)
            raise

    async def start_consuming(self):
        """Start consuming messages with robust reconnection."""
        try:
            await self.connect()
            self.is_consuming = True
            queue = await self.channel.get_queue(self.queue_name)
            await queue.consume(self.callback)
            logger.info("Started consuming messages. To exit press CTRL+C")
            await asyncio.Event().wait()
        except (aio_pika.exceptions.AMQPConnectionError, aio_pika.exceptions.ChannelClosed, asyncio.TimeoutError) as e:
            logger.error(f"Connection error: {e}, reconnecting in 5 seconds", exc_info=True)
            self.is_consuming = False
            await self.close()
            await asyncio.sleep(5)
            await self.start_consuming()
        except aio_pika.exceptions.ProbableAuthenticationError as e:
            logger.error(
                f"Authentication error: {e}. "
                f"Check username, password, and virtual host in {self.amqp_url}.",
                exc_info=True
            )
            self.is_consuming = False
            await self.close()
            raise
        except aiormq.exceptions.ChannelAccessRefused as e:
            logger.error(
                f"Permissions error: {e}. "
                f"Ensure user has permissions on virtual host and default exchange.",
                exc_info=True
            )
            self.is_consuming = False
            await self.close()
            raise
        except KeyboardInterrupt:
            logger.info("Received KeyboardInterrupt, shutting down consumer")
            await self.close()
        except Exception as e:
            logger.error(f"Unexpected error in consumer: {e}, reconnecting in 5 seconds", exc_info=True)
            self.is_consuming = False
            await self.close()
            await asyncio.sleep(5)
            await self.start_consuming()
        # ... (other parts of RabbitMQConsumer class) ...

    # In your RabbitMQConsumer class

    async def execute_update(self, task: dict, conn, logger: logging.Logger):
        """
        Executes a SQL UPDATE/INSERT statement from a task.
        Relies on SQLAlchemy to correctly handle parameter binding, including expanding
        lists for "IN" clauses.
        """
        file_id = task.get("file_id", "unknown")
        task_type = task.get("task_type", "unknown")
        sql_from_task = task.get("sql")
        params_from_task = task.get("params", {})

        # --- Basic Validation ---
        if not sql_from_task or not isinstance(params_from_task, dict):
            logger.error(
                f"TaskType: {task_type}, FileID: {file_id}, Invalid task format. "
                "Missing 'sql' or 'params' is not a dictionary."
            )
            raise ValueError("Invalid task format: Missing 'sql' or 'params' is not a dictionary.")

        try:
            # --- Execution (The SQLAlchemy Way) ---
            # 1. Create a TextClause object from the SQL string.
            stmt = text(sql_from_task)

            # 2. Log what we are about to do, for clarity.
            params_to_log = {
                k: (f"{str(v)[:100]}... (total {len(v)})" if isinstance(v, list) and len(str(v)) > 100 else v)
                for k, v in params_from_task.items()
            }
            logger.debug(
                f"TaskType: {task_type}, FileID: {file_id}, "
                f"Executing SQL: {sql_from_task}. "
                f"Params: {params_to_log}"
            )

            # 3. Execute directly. SQLAlchemy will expand the 'eids_list' parameter correctly.
            result = await conn.execute(stmt, params_from_task)

            # 4. NOTE: You do NOT need conn.commit() here.
            # Your callback uses `async with async_engine.begin() as conn:`, which
            # creates a transaction that automatically commits on success or rolls back on error.

            rowcount = result.rowcount if hasattr(result, 'rowcount') else 0
            logger.info(f"TaskType: {task_type}, FileID: {file_id}: DB operation successful, {rowcount} rows affected.")
            
            return {"rowcount": rowcount}

        except SQLAlchemyError as e:
            # The calling function will catch and log this. Re-raise it.
            logger.error(
                f"TaskType: {task_type}, FileID: {file_id}, Database error on SQL: {sql_from_task}. Error: {e}",
                exc_info=True
            )
            raise

    # ... (rest of RabbitMQConsumer class and main script) ...
    
    async def execute_select(self, task, conn, logger):
        file_id = task.get("file_id")
        select_sql = task.get("sql")
        params = task.get("params", {})
        response_queue = task.get("response_queue")
        logger.debug(f"[{task.get('correlation_id', 'N/A')}] Executing SELECT for FileID {file_id}: {select_sql}, params: {params}")
        if not params:
            raise ValueError(f"No parameters provided for SELECT query: {select_sql}")
        if any(k.startswith("id") for k in params.keys()):
            logger.debug(f"[{task.get('correlation_id', 'N/A')}] Detected named placeholder parameters: {params}")
        else:
            raise ValueError(f"Invalid parameters for SELECT query, expected named placeholders (e.g., id0), got: {params}")
        try:
            start_time = datetime.datetime.now()
            result = await conn.execute(text(select_sql), params)
            rows = result.fetchall()
            columns = result.keys()
            result.close()
            results = [dict(zip(columns, row)) for row in rows]
            elapsed_time = (datetime.datetime.now() - start_time).total_seconds()
            logger.info(f"[{task.get('correlation_id', 'N/A')}] Worker PID {psutil.Process().pid}: SELECT returned {len(results)} rows for FileID {file_id} in {elapsed_time:.2f}s")
            if response_queue:
                producer = await RabbitMQProducer.get_producer(logger=logger)
                await producer.connect()
                response = {
                    "file_id": file_id,
                    "results": results,
                    "correlation_id": task.get("correlation_id", str(uuid.uuid4()))
                }
                await producer.publish_message(response, routing_key=response_queue, correlation_id=task.get("correlation_id"))
                logger.debug(f"[{task.get('correlation_id', 'N/A')}] Sent SELECT results to {response_queue} for FileID {file_id}")
            return results
        except SQLAlchemyError as e:
            logger.error(f"[{task.get('correlation_id', 'N/A')}] Database error executing SELECT for FileID {file_id}: {e}", exc_info=True)
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
    async def callback(self, message: aio_pika.IncomingMessage):
        async with message.process(requeue=True, ignore_processed=True):
            try:
                task = json.loads(message.body.decode())
                file_id = task.get("file_id", "unknown")
                task_type = task.get("task_type", "unknown")
                response_queue = task.get("response_queue")
                logger.info(
                    f"Received task for FileID: {file_id}, TaskType: {task_type}, "
                    f"CorrelationID: {message.correlation_id}"
                )
                async with asyncio.timeout(self.operation_timeout):
                    if task_type == "select_deduplication":
                        async with async_engine.connect() as conn:
                            result = await self.execute_select(task, conn, logger)
                            response = {"file_id": file_id, "results": result}
                            if response_queue:
                                producer = await RabbitMQProducer.get_producer(logger=logger, amqp_url=self.amqp_url)
                                try:
                                    await producer.publish_message(
                                        response,
                                        routing_key=response_queue,
                                        correlation_id=file_id,
                                    )
                                    logger.info(
                                        f"Sent {len(result)} deduplication results to {response_queue} "
                                        f"for FileID: {file_id}"
                                    )
                                except Exception as e:
                                    logger.error(f"Failed to publish response for FileID: {file_id}: {e}", exc_info=True)
                                    raise
                    elif task_type == "update_sort_order" and task.get("sql") == "UPDATE_SORT_ORDER":
                        success = await self.execute_sort_order_update(task.get("params", {}), file_id)
                    else:
                        async with async_engine.begin() as conn:
                            result = await self.execute_update(task, conn, logger)
                            success = True
                    if success:
                        await message.ack()
                        logger.info(f"Successfully processed {task_type} for FileID: {file_id}, Acknowledged")
                        await asyncio.sleep(0.1)  # Adjustable delay for pacing
                    else:
                        logger.warning(f"Failed to process {task_type} for FileID: {file_id}; re-queueing")
                        await message.nack(requeue=True)
                        await asyncio.sleep(2)
            except asyncio.TimeoutError:
                logger.error(f"Timeout processing message for FileID: {file_id}, TaskType: {task_type}")
                await message.nack(requeue=True)
                await asyncio.sleep(2)
            except asyncio.CancelledError:
                logger.info(f"Task cancelled for FileID: {file_id}, TaskType: {task_type}, re-queueing")
                await message.nack(requeue=True)
                raise
            except Exception as e:
                logger.error(
                    f"Error processing message for FileID: {file_id}, TaskType: {task_type}: {e}",
                    exc_info=True
                )
                await message.nack(requeue=True)
                await asyncio.sleep(2)
    async def test_task(self, task: dict):
        file_id = task.get("file_id", "unknown")
        task_type = task.get("task_type", "unknown")
        logger.info(f"Testing task for FileID: {file_id}, TaskType: {task_type}")
        try:
            async with asyncio.timeout(self.operation_timeout):
                if task_type == "select_deduplication":
                    async with async_engine.connect() as conn:
                        success = await self.execute_select(task, conn, logger)
                elif task_type == "update_sort_order" and task.get("sql") == "UPDATE_SORT_ORDER":
                    success = await self.execute_sort_order_update(task.get("params", {}), file_id)
                else:
                    async with async_engine.begin() as conn:
                        success = await self.execute_update(task, conn, logger)
                logger.info(f"Test task result for FileID: {file_id}, TaskType: {task_type}: {'Success' if success else 'Failed'}")
                return success
        except asyncio.TimeoutError:
            logger.error(f"Timeout testing task for FileID: {file_id}, TaskType: {task_type}")
            return False
        except Exception as e:
            logger.error(f"Error testing task for FileID: {file_id}, TaskType: {task_type}: {e}", exc_info=True)
            return False

async def shutdown(consumer: RabbitMQConsumer, loop: asyncio.AbstractEventLoop):
    """Perform async shutdown gracefully."""
    try:
        await consumer.close()
        await async_engine.dispose()
        tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=5)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                logger.warning(f"Task {task.get_name()} did not complete during shutdown")
        loop.stop()
        loop.run_until_complete(loop.shutdown_asyncgens())
    except Exception as e:
        logger.error(f"Error during shutdown: {e}", exc_info=True)
    finally:
        loop.close()
def signal_handler(consumer: RabbitMQConsumer, loop: asyncio.AbstractEventLoop):
    def handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down gracefully...")
        task = asyncio.create_task(shutdown(consumer, loop))
        loop.run_until_complete(task)
        sys.exit(0)
    return handler

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="RabbitMQ Consumer with manual queue clear")
    parser.add_argument("--clear-queue", action="store_true", help="Manually clear the queue and exit")
    args = parser.parse_args()

    consumer = RabbitMQConsumer()
    loop = asyncio.get_event_loop()
    signal.signal(signal.SIGINT, signal_handler(consumer, loop))
    signal.signal(signal.SIGTERM, signal_handler(consumer, loop))

    if args.clear_queue:
        try:
            loop.run_until_complete(consumer.purge_queue())
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
        loop.run_until_complete(consumer.test_task(sample_task))
        loop.run_until_complete(consumer.start_consuming())
    except KeyboardInterrupt:
        loop.run_until_complete(shutdown(consumer, loop))
    except Exception as e:
        logger.error(f"Error in consumer: {e}", exc_info=True)
        loop.run_until_complete(shutdown(consumer, loop))