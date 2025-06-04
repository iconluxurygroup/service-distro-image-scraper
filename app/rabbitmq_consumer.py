import aio_pika
import json
import logging
import asyncio
import signal
import sys
import uuid
import argparse
import datetime # Added missing import
from rabbitmq_producer import RabbitMQProducer
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from database_config import async_engine
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import psutil
from typing import Optional, Dict, Any, List, Tuple, Union # Added more types
import aiormq.exceptions

# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# Changed to DEBUG as in the provided snippet
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class RabbitMQConsumer:
    def __init__(
        self,
        amqp_url: str = "amqp://app_user:app_password@localhost:5672/app_vhost",
        queue_name: str = "db_update_queue",
        connection_timeout: float = 30.0,
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
        self._shutdown_event = asyncio.Event() # For graceful shutdown signaling

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
                            durable=False,
                            exclusive=False,
                            auto_delete=True,
                            arguments={"x-expires": 600000}
                        )
                        logger.debug(f"Declared response queue: {self.queue_name}")
                    except aiormq.exceptions.ChannelNotFoundEntity: # type: ignore
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
                    except aiormq.exceptions.ChannelPreconditionFailed: # type: ignore
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
        if not self.channel: # Should be caught by connect if channel creation fails
            raise aio_pika.exceptions.AMQPConnectionError("Channel is not available after connect attempt.")
        return self.channel

    async def close(self):
        """Close the RabbitMQ connection and channel gracefully."""
        logger.info("Starting RabbitMQ consumer cleanup...")
        self._shutdown_event.set() # Signal consumption loop to stop

        # Give active task some time to finish before forceful close
        # This depends on your callback structure, may need adjustment
        await asyncio.sleep(1)

        if self.is_consuming: # This flag might be set by start_consuming
            self.is_consuming = False # Prevent new tasks
            logger.info("Stopped accepting new messages for consumption.")

        try:
            if self.channel and not self.channel.is_closed:
                try:
                    logger.info("Closing RabbitMQ channel...")
                    await asyncio.wait_for(self.channel.close(), timeout=5)
                    logger.info("Closed RabbitMQ channel")
                except (asyncio.TimeoutError, aio_pika.exceptions.AMQPError, aiormq.exceptions.ChannelInvalidStateError) as e: # type: ignore
                    logger.warning(f"Error closing channel: {e}")
            if self.connection and not self.connection.is_closed:
                try:
                    logger.info("Closing RabbitMQ connection...")
                    await asyncio.wait_for(self.connection.close(), timeout=5)
                    logger.info("Closed RabbitMQ connection")
                except (asyncio.TimeoutError, aio_pika.exceptions.AMQPError) as e:
                    logger.warning(f"Error closing connection: {e}")
        except asyncio.CancelledError:
            logger.info("Close operation cancelled during cleanup.")
            # Don't re-raise, allow finally to run
        except Exception as e:
            logger.error(f"Unexpected error closing RabbitMQ connection: {e}", exc_info=True)
        finally:
            self.is_consuming = False # Ensure it's false
            self.channel = None
            self.connection = None
            logger.info("RabbitMQ consumer cleanup finished.")


    async def purge_queue(self):
        """Purge all messages from the queue."""
        try:
            channel = await self.get_channel() # Ensures connection and channel
            queue = await channel.get_queue(self.queue_name)
            purge_count = await queue.purge()
            logger.info(f"Purged {purge_count} messages from queue: {self.queue_name}")
            return purge_count
        except Exception as e:
            logger.error(f"Error purging queue {self.queue_name}: {e}", exc_info=True)
            raise

    async def start_consuming(self):
        """Start consuming messages with robust reconnection."""
        while not self._shutdown_event.is_set():
            try:
                await self.connect()
                self.is_consuming = True # Mark as consuming
                queue = await self.channel.get_queue(self.queue_name) # type: ignore
                logger.info(f"Starting consumption from queue: {self.queue_name}. To exit press CTRL+C")
                
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        if self._shutdown_event.is_set():
                            logger.info("Shutdown event received, nacking message and stopping consumption.")
                            await message.nack(requeue=True)
                            break 
                        await self.callback(message) # Process the message

                if self._shutdown_event.is_set():
                    logger.info("Consumption loop gracefully exited due to shutdown signal.")
                    break

            except (aio_pika.exceptions.AMQPConnectionError, 
                    aio_pika.exceptions.ChannelClosed, # type: ignore
                    aiormq.exceptions.AMQPConnectionError, # type: ignore
                    asyncio.TimeoutError) as e:
                logger.error(f"Connection error: {e}, attempting to reconnect in 5 seconds.", exc_info=True)
                self.is_consuming = False
                # Don't call self.close() here as it might interfere with robust connection logic
                # or shutdown. Let the loop retry.
            except aio_pika.exceptions.ProbableAuthenticationError as e:
                logger.error(
                    f"Authentication error: {e}. "
                    f"Check username, password, and virtual host in {self.amqp_url}.",
                    exc_info=True
                )
                self.is_consuming = False
                self._shutdown_event.set() # Stop trying
                break # Exit loop on auth error
            except aiormq.exceptions.ChannelAccessRefused as e: # type: ignore
                logger.error(
                    f"Permissions error: {e}. "
                    f"Ensure user has permissions on virtual host and default exchange.",
                    exc_info=True
                )
                self.is_consuming = False
                self._shutdown_event.set() # Stop trying
                break # Exit loop on permission error
            except KeyboardInterrupt: # Should be handled by signal handler primarily
                logger.info("KeyboardInterrupt caught in start_consuming, initiating shutdown.")
                self._shutdown_event.set()
                break
            except Exception as e:
                logger.error(f"Unexpected error in consumer: {e}, attempting to reconnect in 5 seconds.", exc_info=True)
                self.is_consuming = False
            
            if not self._shutdown_event.is_set():
                 await asyncio.sleep(5) # Wait before retrying connection/consumption
            
        logger.info("start_consuming loop has finished.")
        # Perform final cleanup if it hasn't been done by a signal handler already
        if self.connection or self.channel:
             await self.close()


    async def execute_update(self, task: Dict[str, Any], conn, logger_instance: logging.Logger):
        file_id = task.get("file_id", "unknown")
        task_type = task.get("task_type", "unknown")
        try:
            sql = task.get("sql")
            if not sql:
                raise ValueError("SQL statement is missing in the task.")
                
            params_from_task = task.get("params", {})
            if not isinstance(params_from_task, dict):
                # If params can be a list/tuple for positional args with "?", handle it.
                # For now, strictly expecting dict as per original code's check.
                raise ValueError(f"Invalid params format: {params_from_task}, expected dict for named parameters or sequence for positional.")

            # --- SOLUTION FOR TVP ERROR ---
            # If SQL uses named parameters (e.g., :name) and a parameter value is a list
            # intended for an IN clause, pyodbc might treat it as a TVP.
            # For TVPs, each item in the list should be a tuple.
            # E.g., if params_from_task is {'ids': [1, 2, 3]}, it becomes {'ids': [(1,), (2,), (3,)]}
            
            final_params: Union[Dict[str, Any], List[Any], Tuple[Any, ...]]
            if isinstance(params_from_task, dict):
                processed_dict_params: Dict[str, Any] = {}
                for key, value in params_from_task.items():
                    if isinstance(value, list):
                        # Only transform if it's a flat list of non-sequence items
                        if value and all(not isinstance(item, (list, tuple)) for item in value):
                            processed_dict_params[key] = [(item,) for item in value]
                            logger_instance.debug(f"Transformed list for key '{key}' for TVP compatibility.")
                        else:
                            processed_dict_params[key] = value # Already list of lists/tuples, or empty
                    else:
                        processed_dict_params[key] = value
                final_params = processed_dict_params
            else:
                # If params_from_task could be a list/tuple for positional SQL params
                # A similar transformation might be needed if one of its elements is a list
                # E.g. if params_from_task = ([1,2,3], "other_val") for "IN ? AND col = ?"
                # then first element should become ([(1,),(2,)], "other_val")
                # This part is more complex and depends on how positional params are structured.
                # For simplicity, assuming named parameters are preferred with complex types.
                final_params = params_from_task


            logger_instance.debug(f"Executing UPDATE/INSERT for FileID {file_id}: {sql}, final_params: {final_params}")
            result = await conn.execute(text(sql), final_params)
            await conn.commit()
            rowcount = result.rowcount if result.rowcount is not None else 0
            logger_instance.info(f"Worker PID {psutil.Process().pid}: UPDATE/INSERT affected {rowcount} rows for FileID {file_id}")
            return {"rowcount": rowcount, "success": True}
        except SQLAlchemyError as e:
            logger_instance.error(
                f"TaskType: {task_type}, FileID: {file_id}, "
                f"Database error executing UPDATE/INSERT: {sql}, params: {params_from_task}, error: {str(e)}",
                exc_info=True
            )
            await conn.rollback() # Ensure rollback on error
            raise
        except Exception as e:
            logger_instance.error(
                f"TaskType: {task_type}, FileID: {file_id}, "
                f"Unexpected error executing UPDATE/INSERT: {sql}, params: {params_from_task}, error: {str(e)}",
                exc_info=True
            )
            if 'conn' in locals() and conn.in_transaction(): # Check if conn was acquired and in transaction
                 await conn.rollback()
            raise

    async def execute_select(self, task: Dict[str, Any], conn, logger_instance: logging.Logger):
        file_id = task.get("file_id")
        select_sql = task.get("sql")
        if not select_sql:
            raise ValueError("SQL statement for SELECT is missing.")

        params_from_task = task.get("params", {})
        response_queue = task.get("response_queue")
        correlation_id = task.get('correlation_id', 'N/A')
        logger_instance.debug(f"[{correlation_id}] Executing SELECT for FileID {file_id}: {select_sql}, params: {params_from_task}")

        # Similar parameter processing as execute_update if IN clauses are used in SELECTs
        final_params: Union[Dict[str, Any], List[Any], Tuple[Any, ...]]
        if isinstance(params_from_task, dict):
            processed_dict_params: Dict[str, Any] = {}
            for key, value in params_from_task.items():
                if isinstance(value, list):
                    if value and all(not isinstance(item, (list, tuple)) for item in value):
                        processed_dict_params[key] = [(item,) for item in value]
                    else:
                        processed_dict_params[key] = value
                else:
                    processed_dict_params[key] = value
            final_params = processed_dict_params
        else:
            final_params = params_from_task

        # The original check for named placeholders was too strict, SQLAlchemy handles various param styles
        # if not params:
        #     raise ValueError(f"No parameters provided for SELECT query: {select_sql}")
        # if any(k.startswith("id") for k in params.keys()):
        #     logger_instance.debug(f"[{correlation_id}] Detected named placeholder parameters: {params}")
        # else:
        #     raise ValueError(f"Invalid parameters for SELECT query, expected named placeholders (e.g., id0), got: {params}")

        try:
            start_time = datetime.datetime.now()
            result = await conn.execute(text(select_sql), final_params)
            rows = result.fetchall()
            columns = result.keys()
            # result.close() # Not needed with async context manager for conn or if not transaction
            
            results_list = [dict(zip(columns, row)) for row in rows]
            elapsed_time = (datetime.datetime.now() - start_time).total_seconds()
            logger_instance.info(f"[{correlation_id}] Worker PID {psutil.Process().pid}: SELECT returned {len(results_list)} rows for FileID {file_id} in {elapsed_time:.2f}s")
            
            if response_queue:
                # Assuming RabbitMQProducer.get_producer is a class method or static method
                # that correctly handles producer instances.
                producer = await RabbitMQProducer.get_producer(logger=logger_instance, amqp_url=self.amqp_url)
                # await producer.connect() # Producer should handle its own connection state

                response_payload = {
                    "file_id": file_id,
                    "results": results_list,
                    "correlation_id": task.get("correlation_id", str(uuid.uuid4()))
                }
                await producer.publish_message(response_payload, routing_key=response_queue, correlation_id=task.get("correlation_id"))
                logger_instance.debug(f"[{correlation_id}] Sent SELECT results to {response_queue} for FileID {file_id}")
            
            return {"results": results_list, "success": True}
        except SQLAlchemyError as e:
            logger_instance.error(f"[{correlation_id}] Database error executing SELECT for FileID {file_id}: {e}", exc_info=True)
            raise
        except Exception as e: # Catch other exceptions like producer errors
            logger_instance.error(f"[{correlation_id}] Unexpected error during SELECT execution or response publishing for FileID {file_id}: {e}", exc_info=True)
            raise


    async def execute_sort_order_update(self, params: dict, file_id: str):
        try:
            entry_id = params.get("entry_id")
            result_id = params.get("result_id")
            sort_order = params.get("sort_order") # Can be 0, so check for None
            if entry_id is None or result_id is None or sort_order is None: # Ensure all required params are present
                logger.error(
                    f"Invalid parameters for update_sort_order task, FileID: {file_id}. "
                    f"Required: entry_id, result_id, sort_order. Got: {params}"
                )
                return {"success": False, "message": "Invalid parameters"}

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
            async with async_engine.begin() as conn: # Use begin for auto-commit/rollback
                result = await conn.execute(text(sql), update_params)
                # await conn.commit() # Not needed with async_engine.begin()
                rowcount = result.rowcount if result.rowcount is not None else 0
                logger.info(
                    f"TaskType: update_sort_order, FileID: {file_id}, Executed SQL: {sql[:100].strip()}, "
                    f"params: {update_params}, affected {rowcount} rows"
                )
                if rowcount == 0:
                    logger.warning(f"No rows updated for FileID: {file_id}, EntryID: {entry_id}, ResultID: {result_id}")
                    return {"success": False, "message": "No rows updated"}
                return {"success": True, "rowcount": rowcount}
        except SQLAlchemyError as e:
            logger.error(f"Database error updating SortOrder for FileID: {file_id}, EntryID: {entry_id}: {e}", exc_info=True)
            return {"success": False, "message": f"Database error: {e}"}
        except Exception as e:
            logger.error(f"Error updating SortOrder for FileID: {file_id}, EntryID: {entry_id}: {e}", exc_info=True)
            return {"success": False, "message": f"Unexpected error: {e}"}

    async def callback(self, message: aio_pika.IncomingMessage):
        task_processed_successfully = False
        task = None
        file_id = "unknown"
        task_type = "unknown"

        try:
            task_body = message.body.decode()
            task = json.loads(task_body)
            file_id = task.get("file_id", "unknown")
            task_type = task.get("task_type", "unknown")
            response_queue = task.get("response_queue") # May not exist for all tasks
            
            logger.info(
                f"Received task for FileID: {file_id}, TaskType: {task_type}, "
                f"CorrelationID: {message.correlation_id}"
            )

            operation_outcome: Dict[str, Any] = {"success": False} # Default to failure

            async with asyncio.timeout(self.operation_timeout):
                if task_type == "select_deduplication": # Assumed this maps to execute_select
                    async with async_engine.connect() as conn: # connect for SELECT
                        operation_outcome = await self.execute_select(task, conn, logger)
                elif task_type == "update_sort_order" and task.get("sql") == "UPDATE_SORT_ORDER": # Specific handling
                    operation_outcome = await self.execute_sort_order_update(task.get("params", {}), file_id)
                else: # General update/insert
                    async with async_engine.begin() as conn: # begin for UPDATE/INSERT
                        operation_outcome = await self.execute_update(task, conn, logger)
            
            task_processed_successfully = operation_outcome.get("success", False)

        except asyncio.TimeoutError:
            logger.error(f"Timeout processing message for FileID: {file_id}, TaskType: {task_type}")
            task_processed_successfully = False # Ensure it's false
        except asyncio.CancelledError:
            logger.info(f"Task cancelled for FileID: {file_id}, TaskType: {task_type}, will attempt re-queueing if message not acked.")
            # Do not ack/nack here, let the outer message.process() handle it on exit
            # if the cancellation came from shutdown.
            raise # Re-raise to be handled by message.process() context manager
        except json.JSONDecodeError:
            logger.error(f"Failed to decode JSON from message body: {task_body[:200] if task_body else 'empty'}", exc_info=True)
            task_processed_successfully = False # Cannot process, treat as failure for ack/nack
        except Exception as e:
            logger.error(
                f"Error processing message for FileID: {file_id}, TaskType: {task_type}: {e}",
                exc_info=True
            )
            task_processed_successfully = False # Ensure it's false
        
        # Message ack/nack logic based on processing outcome
        # This runs *outside* the try/except for task processing logic,
        # but *inside* the message.process() context manager.
        try:
            if task_processed_successfully:
                await message.ack()
                logger.info(f"Successfully processed {task_type} for FileID: {file_id}, Acknowledged.")
            else:
                logger.warning(f"Failed to process {task_type} for FileID: {file_id}; Nacking for re-queue.")
                await message.nack(requeue=True)
                await asyncio.sleep(2) # Delay before broker re-delivers
        except aiormq.exceptions.ChannelInvalidStateError: # type: ignore
             logger.warning(f"Channel closed while trying to ack/nack message for FileID: {file_id}. Message might be redelivered or lost if not requeued by broker on timeout.")
        except Exception as e_ack_nack:
            logger.error(f"Error during ack/nack for FileID: {file_id}: {e_ack_nack}", exc_info=True)
            # If nack fails, message might be stuck until delivery timeout


    async def test_task(self, task: dict):
        file_id = task.get("file_id", "unknown")
        task_type = task.get("task_type", "unknown")
        logger.info(f"Testing task for FileID: {file_id}, TaskType: {task_type}")
        operation_outcome = {"success": False}
        try:
            async with asyncio.timeout(self.operation_timeout):
                if task_type == "select_deduplication":
                    async with async_engine.connect() as conn:
                        operation_outcome = await self.execute_select(task, conn, logger)
                elif task_type == "update_sort_order" and task.get("sql") == "UPDATE_SORT_ORDER":
                    operation_outcome = await self.execute_sort_order_update(task.get("params", {}), file_id)
                else:
                    async with async_engine.begin() as conn:
                        operation_outcome = await self.execute_update(task, conn, logger)
                
                success_status = operation_outcome.get("success", False)
                logger.info(f"Test task result for FileID: {file_id}, TaskType: {task_type}: {'Success' if success_status else 'Failed'}")
                return success_status
        except asyncio.TimeoutError:
            logger.error(f"Timeout testing task for FileID: {file_id}, TaskType: {task_type}")
            return False
        except Exception as e:
            logger.error(f"Error testing task for FileID: {file_id}, TaskType: {task_type}: {e}", exc_info=True)
            return False

consumer_instance_for_signal: Optional[RabbitMQConsumer] = None # Global for signal handler access

async def shutdown(loop: asyncio.AbstractEventLoop, sig: Optional[signal.Signals] = None):
    """Perform async shutdown gracefully."""
    if sig:
        logger.info(f"Received signal {sig.name}, initiating shutdown...")
    else:
        logger.info("Initiating shutdown...")

    global consumer_instance_for_signal
    if consumer_instance_for_signal:
        await consumer_instance_for_signal.close() # This now sets _shutdown_event

    logger.info("Disposing database engine...")
    await async_engine.dispose()
    logger.info("Database engine disposed.")

    # Cancel all other running tasks
    tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
    if tasks:
        logger.info(f"Cancelling {len(tasks)} outstanding tasks...")
        for task in tasks:
            task.cancel()
        
        # Wait for tasks to cancel
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, result in enumerate(results):
            if isinstance(result, asyncio.CancelledError):
                logger.debug(f"Task {tasks[i].get_name()} was cancelled successfully.")
            elif isinstance(result, Exception):
                logger.warning(f"Task {tasks[i].get_name()} raised an exception during cancellation: {result}", exc_info=result)
        logger.info("Outstanding tasks cancelled.")
    else:
        logger.info("No outstanding tasks to cancel.")

    # Stop the loop, allowing run_forever or run_until_complete to exit
    # This might not be strictly necessary if start_consuming exits due to _shutdown_event
    # but it's a good failsafe.
    if loop.is_running():
         logger.info("Stopping event loop...")
         loop.stop()


def signal_handler_closure(loop: asyncio.AbstractEventLoop):
    def handler(sig, frame):
        # Create a task to run the shutdown coroutine
        # This allows the signal handler to return quickly
        logger.info(f"Signal {sig.name} received in handler. Scheduling shutdown task.")
        asyncio.create_task(shutdown(loop, sig))
    return handler

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RabbitMQ Consumer with manual queue clear")
    parser.add_argument("--clear-queue", action="store_true", help="Manually clear the queue and exit")
    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    
    # Create consumer instance and assign to global for signal handler
    consumer = RabbitMQConsumer()
    consumer_instance_for_signal = consumer

    # Setup signal handlers
    sig_handler_func = signal_handler_closure(loop)
    for sig_name in ('SIGINT', 'SIGTERM'):
        if hasattr(signal, sig_name):
            try:
                loop.add_signal_handler(getattr(signal, sig_name), sig_handler_func, getattr(signal, sig_name))
                logger.info(f"Registered signal handler for {sig_name}")
            except NotImplementedError: # e.g. on Windows for SIGTERM sometimes
                signal.signal(getattr(signal, sig_name), sig_handler_func)
                logger.info(f"Registered signal handler for {sig_name} using signal.signal()")


    if args.clear_queue:
        try:
            logger.info("Attempting to clear queue...")
            loop.run_until_complete(consumer.purge_queue())
            logger.info("Queue cleared successfully. Exiting.")
        except Exception as e:
            logger.error(f"Failed to clear queue: {e}", exc_info=True)
            sys.exit(1)
        finally:
            # Cleanly close consumer resources even after purge
            loop.run_until_complete(consumer.close())
            loop.run_until_complete(async_engine.dispose())
            loop.close()
            sys.exit(0)


    sample_task = {
        "file_id": "321",
        "task_type": "update_sort_order", # This will use execute_sort_order_update
        "sql": "UPDATE_SORT_ORDER", # Matches condition in callback
        "params": {
            "entry_id": "119061",
            "result_id": "1868277",
            "sort_order": 1
        },
        "timestamp": "2025-05-21T12:34:08.307076"
    }
    
    # Sample task that would use execute_update and the TVP fix
    sample_update_task = {
        "file_id": "test_update_123",
        "task_type": "batch_update_scraper_status_warehouse_complete", # Generic type
        "sql": "UPDATE utb_ImageScraperRecords SET EntryStatus = :status WHERE EntryID IN :entry_ids;", # Named params
        "params": {
            "status": 2,
            "entry_ids": [134517, 134518, 134519] # This list will be transformed
        },
        "timestamp": datetime.datetime.utcnow().isoformat()
    }


    main_task = None
    try:
        logger.info("Running test task for update_sort_order...")
        loop.run_until_complete(consumer.test_task(sample_task))
        
        logger.info("Running test task for general update (TVP scenario)...")
        # Ensure your DB has a table utb_ImageScraperRecords with EntryID and EntryStatus columns
        # and records with EntryID 134517, 134518, 134519 for this to actually update something.
        loop.run_until_complete(consumer.test_task(sample_update_task))

        logger.info("Starting main consumer loop...")
        main_task = asyncio.create_task(consumer.start_consuming())
        loop.run_until_complete(main_task)

    except KeyboardInterrupt: # Should be handled by signals, but as a fallback
        logger.info("KeyboardInterrupt caught in main __name__ block.")
        if not (main_task and main_task.done()): # If main task isn't already finishing due to signal
            if not loop.is_closed():
                loop.run_until_complete(shutdown(loop))
    except Exception as e:
        logger.error(f"Unhandled error in main: {e}", exc_info=True)
        if not loop.is_closed():
             loop.run_until_complete(shutdown(loop)) # Attempt graceful shutdown
    finally:
        if not loop.is_closed():
            # Ensure async generators are closed if loop wasn't stopped by shutdown
            if loop.is_running(): # If loop.stop() wasn't called for some reason
                loop.stop() # Stop it now
            logger.info("Shutting down async generators...")
            loop.run_until_complete(loop.shutdown_asyncgens())
            logger.info("Closing event loop...")
            loop.close()
            logger.info("Event loop closed.")
        logger.info("Application exiting.")
        sys.exit(0)
