import aio_pika
import json
import logging
import asyncio
import signal
import sys
import uuid
import datetime # Ensure datetime is imported
from rabbitmq_producer import RabbitMQProducer # Assuming this exists and is correct
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from database_config import async_engine # Assuming this exists and is correct
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import psutil
from typing import Optional, Dict, Any, Union, List, Tuple # Added more type hints
import aiormq.exceptions

# The original script had two logging.basicConfig calls. The last one takes precedence.
# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# logger = logging.getLogger(__name__)

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
        self.is_consuming = False # Tracks if actively trying to consume
        self._shutdown_event = asyncio.Event() # For graceful shutdown
        self._consuming_task: Optional[asyncio.Task] = None # To store the queue.consume task

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((aiormq.exceptions.ChannelInvalidStateError, aio_pika.exceptions.AMQPConnectionError))
    )
    async def declare_queue_with_retry(self, channel: aio_pika.Channel, queue_name: str, **kwargs):
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
                    connection_attempts=3, # connect_robust handles its own attempts
                    retry_delay=5,
                )
                # Robust connection handles channel re-creation, but we get one initially
                self.channel = await self.connection.channel()
                if not self.channel: # Should not happen if connection is successful
                    raise aio_pika.exceptions.AMQPConnectionError("Failed to create channel on robust connection")
                
                await self.channel.set_qos(prefetch_count=1)

                # Queue declaration logic (unchanged from original)
                if self.queue_name.startswith("select_response_"):
                    try:
                        await self.declare_queue_with_retry(
                            self.channel, self.queue_name, durable=False, exclusive=False,
                            auto_delete=True, arguments={"x-expires": 600000}
                        )
                        logger.debug(f"Declared response queue: {self.queue_name}")
                    except aiormq.exceptions.ChannelNotFoundEntity: # type: ignore
                        logger.warning(f"Queue {self.queue_name} not found, creating it")
                        await self.declare_queue_with_retry(
                            self.channel, self.queue_name, durable=False, exclusive=False,
                            auto_delete=True, arguments={"x-expires": 600000}
                        )
                        logger.debug(f"Created response queue: {self.queue_name}")
                    except aiormq.exceptions.ChannelPreconditionFailed: # type: ignore
                        logger.warning(f"Queue {self.queue_name} has incompatible settings, generating new name")
                        self.queue_name = f"{self.queue_name}_{uuid.uuid4().hex[:8]}"
                        await self.declare_queue_with_retry(
                            self.channel, self.queue_name, durable=False, exclusive=False,
                            auto_delete=True, arguments={"x-expires": 600000}
                        )
                        logger.debug(f"Created new response queue: {self.queue_name}")
                else:
                    await self.declare_queue_with_retry(
                        self.channel, self.queue_name, durable=True, exclusive=False, auto_delete=False
                    )
                    logger.debug(f"Declared main queue: {self.queue_name}")
                logger.info(f"Connected to RabbitMQ, channel established for queue: {self.queue_name}")
        except asyncio.TimeoutError:
            logger.error("Timeout connecting to RabbitMQ")
            raise
        except aio_pika.exceptions.ProbableAuthenticationError as e:
            logger.error(f"Authentication error: {e}", exc_info=True)
            raise
        except aio_pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}", exc_info=True)
            raise

    async def get_channel(self) -> aio_pika.Channel: # Unchanged
        """Get the RabbitMQ channel, connecting if necessary."""
        if not self.channel or self.channel.is_closed:
            await self.connect()
        if not self.channel: # Should be set by connect
            raise aio_pika.exceptions.AMQPConnectionError("Failed to establish channel")
        return self.channel

    async def close_resources_only(self):
        """Closes current channel and connection, typically for retrying connection."""
        logger.debug("Closing current AMQP resources for retry or reset...")
        if self._consuming_task and not self._consuming_task.done():
            self._consuming_task.cancel() # Cancel any active consume operation on this channel
            try:
                await asyncio.wait_for(self._consuming_task, timeout=1.0) # Short wait
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass # Expected if it was consuming
        self._consuming_task = None

        ch = self.channel
        self.channel = None # Clear instance var before await
        if ch and not ch.is_closed:
            try:
                await asyncio.wait_for(ch.close(), timeout=2)
                logger.debug("Closed channel for resource reset.")
            except Exception as e:
                logger.warning(f"Error closing channel for resource reset: {e}")
        
        conn = self.connection
        self.connection = None # Clear instance var
        if conn and not conn.is_closed:
            try:
                await asyncio.wait_for(conn.close(), timeout=2)
                logger.debug("Closed connection for resource reset.")
            except Exception as e:
                logger.warning(f"Error closing connection for resource reset: {e}")
        self.is_consuming = False


    async def close(self):
        """Close the RabbitMQ connection and channel gracefully as part of shutdown."""
        logger.info("Initiating RabbitMQ consumer shutdown sequence...")
        self._shutdown_event.set() # Signal all loops to stop, primary shutdown signal

        if self._consuming_task and not self._consuming_task.done():
            logger.info("Attempting to cancel the main consuming task.")
            self._consuming_task.cancel()
            try:
                await asyncio.wait_for(self._consuming_task, timeout=5.0)
                logger.info("Main consuming task cancelled/finished.")
            except asyncio.TimeoutError:
                logger.warning("Main consuming task did not finish cancelling in time.")
            except asyncio.CancelledError: # Already cancelled
                logger.info("Main consuming task was already cancelled.")
            except Exception as e: # Other errors during task wait
                logger.error(f"Error awaiting consuming task cancellation: {e}")
        self._consuming_task = None

        # Now close channel and connection. RobustConnection might handle this,
        # but explicit close is good for shutdown.
        ch = self.channel
        self.channel = None
        if ch and not ch.is_closed:
            try:
                logger.info("Closing RabbitMQ channel...")
                await asyncio.wait_for(ch.close(), timeout=5)
                logger.info("Closed RabbitMQ channel")
            except (asyncio.TimeoutError, aio_pika.exceptions.AMQPError, aiormq.exceptions.ChannelInvalidStateError) as e: # type: ignore
                logger.warning(f"Error closing channel during shutdown: {e}")
        
        conn = self.connection
        self.connection = None
        if conn and not conn.is_closed:
            try:
                logger.info("Closing RabbitMQ connection...")
                await asyncio.wait_for(conn.close(), timeout=5)
                logger.info("Closed RabbitMQ connection")
            except (asyncio.TimeoutError, aio_pika.exceptions.AMQPError) as e:
                logger.warning(f"Error closing connection during shutdown: {e}")
        
        self.is_consuming = False # Final state
        logger.info("RabbitMQ consumer resources cleanup finished.")


    async def purge_queue(self): # Unchanged
        """Purge all messages from the queue."""
        try:
            # Use get_channel to ensure connection is active
            channel = await self.get_channel()
            queue = await channel.get_queue(self.queue_name) # type: ignore
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
                await self.connect() # Establishes self.connection and self.channel
                self.is_consuming = True
                
                # channel should be valid here due to connect()
                if not self.channel: # Defensive check
                    raise aio_pika.exceptions.AMQPConnectionError("Channel not available after connect")

                queue = await self.channel.get_queue(self.queue_name)
                logger.info(f"Starting consumption from queue: {self.queue_name}. Press CTRL+C to exit.")
                
                # Store the task created by queue.consume
                self._consuming_task = asyncio.create_task(queue.consume(self.callback))
                
                await self._shutdown_event.wait() # Wait here until shutdown is signaled

                logger.info("Shutdown event received, stopping consumption.")
                if self._consuming_task and not self._consuming_task.done():
                    self._consuming_task.cancel() # Attempt to cancel consumption
                    try:
                        await asyncio.wait_for(self._consuming_task, timeout=2.0)
                    except (asyncio.TimeoutError, asyncio.CancelledError):
                        pass # Expected
                self._consuming_task = None
                break # Exit the while loop as we are shutting down

            except (aio_pika.exceptions.AMQPConnectionError, 
                    aio_pika.exceptions.ChannelClosed, # type: ignore
                    aiormq.exceptions.AMQPConnectionError, # type: ignore
                    asyncio.TimeoutError) as e:
                logger.error(f"AMQP Connection/Channel error: {e}. Reconnecting in 5 seconds.", exc_info=True)
                self.is_consuming = False
                if self._shutdown_event.is_set(): break
                await self.close_resources_only() # Clean up before retry
                await asyncio.sleep(5)
            except aio_pika.exceptions.ProbableAuthenticationError as e:
                logger.error(f"Authentication error: {e}. Halting.", exc_info=True)
                self.is_consuming = False
                self._shutdown_event.set() # Signal shutdown on critical error
                break
            except aiormq.exceptions.ChannelAccessRefused as e: # type: ignore
                logger.error(f"Permissions error: {e}. Halting.", exc_info=True)
                self.is_consuming = False
                self._shutdown_event.set() # Signal shutdown on critical error
                break
            except KeyboardInterrupt: # Should be caught by global signal handler
                logger.info("KeyboardInterrupt in start_consuming; signaling shutdown.")
                self._shutdown_event.set() # Ensure shutdown event is set
                break
            except Exception as e:
                logger.error(f"Unexpected error in consumer's main loop: {e}. Reconnecting in 5 seconds.", exc_info=True)
                self.is_consuming = False
                if self._shutdown_event.is_set(): break
                await self.close_resources_only() # Clean up before retry
                await asyncio.sleep(5)
        
        logger.info("start_consuming loop has finished.")
        # Final cleanup if not already done by a signal handler triggering self.close()
        if not (self.connection is None and self.channel is None): # If resources might still exist
            await self.close()


    async def execute_update(self, task: Dict[str, Any], conn, logger_instance: logging.Logger):
        file_id = task.get("file_id", "unknown")
        task_type = task.get("task_type", "unknown")
        sql = task.get("sql")
        raw_params = task.get("params", {})

        try:
            if not sql:
                raise ValueError("SQL statement is missing in the task.")

            processed_params: Any
            if isinstance(raw_params, dict):
                processed_params = {}
                for key, value in raw_params.items():
                    if isinstance(value, list):
                        if value and all(not isinstance(item, (list, tuple)) for item in value):
                            processed_params[key] = [(item,) for item in value]
                            logger_instance.debug(f"Transformed list for param '{key}' for TVP compatibility.")
                        else:
                            processed_params[key] = value
                    else:
                        processed_params[key] = value
            elif isinstance(raw_params, tuple) and len(raw_params) == 1 and isinstance(raw_params[0], list):
                # Handles the specific case from traceback: params was ([134517],)
                # This implies the SQL was `... IN ?` and params to conn.execute was `([ (134517,), ... ],)`
                list_val = raw_params[0]
                if list_val and all(not isinstance(item, (list, tuple)) for item in list_val):
                    processed_params = ([(item,) for item in list_val],)
                    logger_instance.debug("Transformed list in positional tuple for TVP compatibility.")
                else:
                    processed_params = raw_params # Pass as is if already list of tuples or empty
            else:
                # For other types or structures of raw_params (e.g. simple list for multiple positional params)
                # pass through, assuming SQLAlchemy or DBAPI handles them or they don't involve TVPs.
                # Or, if the original strict dict check is desired:
                # if not isinstance(raw_params, dict):
                #     raise ValueError(f"Invalid params format: {raw_params}, expected dict for named params or specific tuple for IN ?")
                processed_params = raw_params


            logger_instance.debug(f"Executing UPDATE/INSERT for FileID {file_id}: SQL: {sql}, processed_params: {processed_params}")
            result = await conn.execute(text(sql), processed_params)
            await conn.commit()
            rowcount = result.rowcount if result.rowcount is not None else 0
            logger_instance.info(f"Worker PID {psutil.Process().pid}: UPDATE/INSERT affected {rowcount} rows for FileID {file_id}")
            return {"rowcount": rowcount, "success": True}
        except SQLAlchemyError as e:
            logger_instance.error(
                f"TaskType: {task_type}, FileID: {file_id}, "
                f"Database error executing UPDATE/INSERT: SQL: {sql}, raw_params: {raw_params}, error: {str(e)}",
                exc_info=True
            )
            if conn.in_transaction(): await conn.rollback()
            raise
        except ValueError as ve: # Catch ValueErrors from param processing
             logger_instance.error(
                f"TaskType: {task_type}, FileID: {file_id}, "
                f"ValueError in UPDATE/INSERT: {str(ve)}", exc_info=True)
             raise
        except Exception as e:
            logger_instance.error(
                f"TaskType: {task_type}, FileID: {file_id}, "
                f"Unexpected error executing UPDATE/INSERT: SQL: {sql}, raw_params: {raw_params}, error: {str(e)}",
                exc_info=True
            )
            # Check if conn was acquired and in transaction
            if 'conn' in locals() and hasattr(conn, 'in_transaction') and conn.in_transaction():
                 await conn.rollback()
            raise

    async def execute_select(self, task: Dict[str, Any], conn, logger_instance: logging.Logger):
        file_id = task.get("file_id")
        select_sql = task.get("sql")
        raw_params = task.get("params", {})
        response_queue = task.get("response_queue")
        correlation_id = task.get('correlation_id', 'N/A')

        logger_instance.debug(f"[{correlation_id}] Executing SELECT for FileID {file_id}: {select_sql}, raw_params: {raw_params}")

        # Original param check for "not params" (empty dict/None)
        if not raw_params and raw_params != {} : # Allow empty dict for no params, but not None if some params are expected by SQL
            # This check might need refinement based on whether SQL always expects params or not.
            # If SQL can have no params, an empty dict is fine.
            # If task["params"] is None, raw_params will be None.
             pass # Allow execute if SQL has no bind params

        # The original check `any(k.startswith("id") ...)` was removed as it's too specific/brittle.
        # Parameter processing for TVP (similar to execute_update)
        processed_params: Any
        if isinstance(raw_params, dict):
            processed_params = {}
            for key, value in raw_params.items():
                if isinstance(value, list):
                    if value and all(not isinstance(item, (list, tuple)) for item in value):
                        processed_params[key] = [(item,) for item in value]
                    else:
                        processed_params[key] = value
                else:
                    processed_params[key] = value
        elif isinstance(raw_params, tuple) and len(raw_params) == 1 and isinstance(raw_params[0], list):
            list_val = raw_params[0]
            if list_val and all(not isinstance(item, (list, tuple)) for item in list_val):
                processed_params = ([(item,) for item in list_val],)
            else:
                processed_params = raw_params
        else:
            processed_params = raw_params
        
        try:
            start_time = datetime.datetime.now()
            result = await conn.execute(text(select_sql), processed_params)
            rows = result.fetchall()
            columns = result.keys()
            # result.close() # Not strictly necessary, connection context handles it.

            results_list = [dict(zip(columns, row)) for row in rows]
            elapsed_time = (datetime.datetime.now() - start_time).total_seconds()
            logger_instance.info(f"[{correlation_id}] Worker PID {psutil.Process().pid}: SELECT returned {len(results_list)} rows for FileID {file_id} in {elapsed_time:.2f}s")
            
            if response_queue:
                producer = await RabbitMQProducer.get_producer(logger=logger_instance, amqp_url=self.amqp_url)
                # await producer.connect() # Producer should manage its connection state internally

                response_payload = {
                    "file_id": file_id,
                    "results": results_list,
                    "correlation_id": task.get("correlation_id", str(uuid.uuid4()))
                }
                await producer.publish_message(response_payload, routing_key=response_queue, correlation_id=task.get("correlation_id"))
                logger_instance.debug(f"[{correlation_id}] Sent SELECT results to {response_queue} for FileID {file_id}")
            
            return {"results": results_list, "success": True}
        except SQLAlchemyError as e:
            logger_instance.error(f"[{correlation_id}] Database error executing SELECT for FileID {file_id}: {select_sql}, params: {raw_params}, error: {e}", exc_info=True)
            raise
        except Exception as e:
            logger_instance.error(f"[{correlation_id}] Unexpected error during SELECT for FileID {file_id}: {select_sql}, params: {raw_params}, error: {e}", exc_info=True)
            raise

    async def execute_sort_order_update(self, params: dict, file_id: str) -> Dict[str, Any]:
        try:
            entry_id = params.get("entry_id")
            result_id = params.get("result_id")
            sort_order = params.get("sort_order")
            if not all([entry_id is not None, result_id is not None, sort_order is not None]): # Check for None explicitly
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
            update_params = { "sort_order": sort_order, "entry_id": entry_id, "result_id": result_id }
            async with async_engine.begin() as conn:
                result = await conn.execute(text(sql), update_params)
                # await conn.commit() # .begin() handles commit
                rowcount = result.rowcount if result.rowcount is not None else 0
                logger.info(
                    f"TaskType: update_sort_order, FileID: {file_id}, Executed SQL (snippet): {sql[:100].replace_all('\n',' ')}, "
                    f"params: {update_params}, affected {rowcount} rows"
                )
                if rowcount == 0:
                    logger.warning(f"No rows updated for FileID: {file_id}, EntryID: {entry_id}, ResultID: {result_id}")
                    return {"success": False, "message": "No rows updated", "rowcount": 0}
                return {"success": True, "rowcount": rowcount}
        except SQLAlchemyError as e:
            logger.error(f"Database error updating SortOrder for FileID: {file_id}, EntryID: {entry_id if 'entry_id' in locals() else 'N/A'}: {e}", exc_info=True)
            return {"success": False, "message": f"Database error: {e}"}
        except Exception as e:
            logger.error(f"Error updating SortOrder for FileID: {file_id}, EntryID: {entry_id if 'entry_id' in locals() else 'N/A'}: {e}", exc_info=True)
            return {"success": False, "message": f"Unexpected error: {e}"}

    async def callback(self, message: aio_pika.IncomingMessage):
        # Renamed local logger variable to avoid conflict if self.logger existed
        callback_logger = logger 
        file_id = "unknown_file_id" # Default before parsing
        task_type = "unknown_task_type" # Default before parsing
        
        # The message.process() context manager handles ack/nack based on exceptions.
        # If an exception escapes this block, message is nack(requeue=True).
        # If no exception, message is acked.
        async with message.process(requeue=True, ignore_processed=True):
            try:
                task_body = message.body.decode()
                task = json.loads(task_body)
                file_id = task.get("file_id", file_id) # Update with parsed value
                task_type = task.get("task_type", task_type) # Update with parsed value
                response_queue = task.get("response_queue")
                
                callback_logger.info(
                    f"Received task for FileID: {file_id}, TaskType: {task_type}, "
                    f"CorrelationID: {message.correlation_id}"
                )

                op_result: Dict[str, Any] = {"success": False} # Default operation outcome

                async with asyncio.timeout(self.operation_timeout):
                    if task_type == "select_deduplication":
                        async with async_engine.connect() as conn: # SELECT uses connect, not begin
                            op_result = await self.execute_select(task, conn, callback_logger)
                            # Publishing response for select_deduplication happens inside execute_select
                    elif task_type == "update_sort_order" and task.get("sql") == "UPDATE_SORT_ORDER":
                        op_result = await self.execute_sort_order_update(task.get("params", {}), file_id)
                    else: # General update/insert type tasks
                        async with async_engine.begin() as conn: # UPDATE/INSERT uses begin for auto transaction
                            op_result = await self.execute_update(task, conn, callback_logger)
                
                success = op_result.get("success", False)
                if not success:
                    # If operation explicitly returned success: False, raise an exception
                    # so message.process() nacks it.
                    error_message = op_result.get("message", "Operation reported failure.")
                    raise RuntimeError(f"Task processing failed for FileID {file_id}, TaskType {task_type}: {error_message}")

                # If we reach here, success is True, message.process() will ACK.
                callback_logger.info(f"Successfully processed {task_type} for FileID: {file_id}. Message will be ACKed.")
                await asyncio.sleep(0.1) # Original pacing delay

            except json.JSONDecodeError as e:
                callback_logger.error(f"JSONDecodeError for message: {e}. Message body (partial): {message.body[:200] if message.body else 'N/A'}", exc_info=True)
                raise # Let message.process nack
            except asyncio.TimeoutError:
                callback_logger.error(f"Timeout processing message for FileID: {file_id}, TaskType: {task_type}")
                raise # Let message.process nack
            except asyncio.CancelledError:
                callback_logger.info(f"Task cancelled during processing for FileID: {file_id}, TaskType: {task_type}. Message will be NACKed.")
                raise # Let message.process nack and requeue
            except Exception as e: # Catch other exceptions like SQLAlchemyError, RuntimeError from above
                callback_logger.error(
                    f"Error processing message for FileID: {file_id}, TaskType: {task_type}: {e}",
                    exc_info=True
                )
                raise # Let message.process nack

    async def test_task(self, task: dict): # Largely unchanged, adapted for new return types
        test_logger = logger # Use module logger
        file_id = task.get("file_id", "unknown")
        task_type = task.get("task_type", "unknown")
        test_logger.info(f"Testing task for FileID: {file_id}, TaskType: {task_type}")
        op_result: Dict[str, Any] = {"success": False}
        try:
            async with asyncio.timeout(self.operation_timeout):
                if task_type == "select_deduplication":
                    async with async_engine.connect() as conn:
                        op_result = await self.execute_select(task, conn, test_logger)
                elif task_type == "update_sort_order" and task.get("sql") == "UPDATE_SORT_ORDER":
                    op_result = await self.execute_sort_order_update(task.get("params", {}), file_id)
                else:
                    async with async_engine.begin() as conn:
                        op_result = await self.execute_update(task, conn, test_logger)
            
            success_status = op_result.get("success", False)
            test_logger.info(f"Test task result for FileID: {file_id}, TaskType: {task_type}: {'Success' if success_status else 'Failed'}")
            return success_status # Return boolean for simple test outcome
        except asyncio.TimeoutError:
            test_logger.error(f"Timeout testing task for FileID: {file_id}, TaskType: {task_type}")
            return False
        except Exception as e:
            test_logger.error(f"Error testing task for FileID: {file_id}, TaskType: {task_type}: {e}", exc_info=True)
            return False

# Global variable to hold the consumer instance for the signal handler
_consumer_instance_for_signal: Optional[RabbitMQConsumer] = None

async def shutdown(consumer: RabbitMQConsumer, loop: asyncio.AbstractEventLoop, sig: Optional[Union[signal.Signals, int]] = None):
    """Perform async shutdown gracefully."""
    signal_name = sig.name if isinstance(sig, signal.Signals) else str(sig)
    if sig:
        logger.info(f"Received signal {signal_name}, shutting down gracefully...")
    else:
        logger.info("Shutdown initiated programmatically...")

    if not consumer._shutdown_event.is_set():
        await consumer.close() # This sets the shutdown event and closes AMQP resources

    logger.info("Disposing database engine...")
    await async_engine.dispose()
    logger.info("Database engine disposed.")

    tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
    if tasks:
        logger.info(f"Cancelling {len(tasks)} outstanding tasks...")
        for task_to_cancel in tasks:
            if not task_to_cancel.done():
                task_to_cancel.cancel()
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        # Optionally log results of cancellation
        for i, res in enumerate(results):
            task_name = tasks[i].get_name() if hasattr(tasks[i], 'get_name') else f"Task-{i}"
            if isinstance(res, asyncio.CancelledError):
                logger.debug(f"Task {task_name} cancelled.")
            elif isinstance(res, Exception):
                 logger.warning(f"Task {task_name} raised {type(res).__name__} during/after cancellation: {res}")
        logger.info("Outstanding tasks cancellation process complete.")
    else:
        logger.info("No outstanding tasks to cancel.")


def signal_handler_closure(loop: asyncio.AbstractEventLoop):
    """Closure to create a signal handler that calls the async shutdown."""
    def handler(sig, frame):
        global _consumer_instance_for_signal
        if _consumer_instance_for_signal:
            logger.info(f"Signal {sig} received by handler. Scheduling shutdown.")
            asyncio.create_task(shutdown(_consumer_instance_for_signal, loop, sig))
        else:
            logger.warning("Signal received but consumer instance not found for shutdown.")
    return handler

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="RabbitMQ Consumer with manual queue clear")
    parser.add_argument("--clear-queue", action="store_true", help="Manually clear the queue and exit")
    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    consumer = RabbitMQConsumer()
    _consumer_instance_for_signal = consumer # Assign to global for signal handler

    # Setup signal handlers using loop.add_signal_handler for better asyncio integration
    sig_handler_func = signal_handler_closure(loop)
    for sig_name in ('SIGINT', 'SIGTERM'):
        sig_val = getattr(signal, sig_name, None)
        if sig_val:
            try:
                loop.add_signal_handler(sig_val, sig_handler_func, sig_val, None) # Pass sig_val for handler
                logger.info(f"Registered asyncio signal handler for {sig_name}")
            except (NotImplementedError, RuntimeError): # Fallback for environments like Windows
                signal.signal(sig_val, sig_handler_func)
                logger.info(f"Registered fallback signal.signal handler for {sig_name}")


    main_consumer_task = None
    try:
        if args.clear_queue:
            logger.info("Attempting to clear queue...")
            loop.run_until_complete(consumer.purge_queue())
            logger.info("Queue cleared successfully. Exiting.")
            # No need to call full shutdown here, just clean up consumer and exit
            loop.run_until_complete(consumer.close())
            loop.run_until_complete(async_engine.dispose())
            sys.exit(0)

        sample_task = {
            "file_id": "321_test",
            "task_type": "update_sort_order",
            "sql": "UPDATE_SORT_ORDER", # This special SQL value triggers execute_sort_order_update
            "params": { "entry_id": "119061", "result_id": "1868277", "sort_order": 1 },
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
        # Example for TVP issue (ensure table and columns exist for testing)
        # Create a dummy table: CREATE TABLE utb_ImageScraperRecords (EntryID INT PRIMARY KEY, EntryStatus INT, WarehouseMatchTime DATETIME);
        # Insert some dummy data: INSERT INTO utb_ImageScraperRecords (EntryID) VALUES (134517), (134518);
        tvp_test_task = {
            "file_id": "tvp_test_001",
            "task_type": "batch_update_records", # Generic task type
            "sql": "UPDATE utb_ImageScraperRecords SET EntryStatus = :status, WarehouseMatchTime = GETUTCDATE() WHERE EntryID IN :entry_ids;",
            "params": {
                "status": 5,
                "entry_ids": [134517, 134518, 99999] # List that will be transformed for TVP
            },
            "timestamp": datetime.datetime.utcnow().isoformat()
        }

        logger.info("Running test_task (update_sort_order)...")
        loop.run_until_complete(consumer.test_task(sample_task))
        
        logger.info("Running test_task (TVP scenario)...")
        loop.run_until_complete(consumer.test_task(tvp_test_task))


        logger.info("Starting main consumer...")
        main_consumer_task = loop.create_task(consumer.start_consuming())
        loop.run_until_complete(main_consumer_task)

    except KeyboardInterrupt: # Should be handled by signals, but as a final catch
        logger.info("KeyboardInterrupt caught in __main__.")
        # Signal handler should have already initiated shutdown.
        # If loop is still running, it means signal handler might not have completed fully.
    except Exception as e:
        logger.error(f"Unhandled exception in __main__: {e}", exc_info=True)
    finally:
        logger.info("Main block finally: ensuring shutdown completes.")
        if not consumer._shutdown_event.is_set():
            # If shutdown wasn't triggered (e.g. error before signals were handled properly)
            logger.info("Shutdown event not set, initiating shutdown from finally block.")
            if loop.is_running() and not loop.is_closed():
                loop.run_until_complete(shutdown(consumer, loop))
            elif not loop.is_closed(): # Loop not running but not closed
                 # Manually run close if loop.run_until_complete can't be used
                async def _final_close():
                    await consumer.close()
                    await async_engine.dispose()
                loop.run_until_complete(_final_close())


        # Graceful loop cleanup
        if not loop.is_closed():
            if loop.is_running(): # Should have been stopped by shutdown
                logger.info("Loop is still running, stopping now.")
                loop.stop() 
            
            logger.info("Shutting down async generators...")
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            except RuntimeError as e: # Can happen if loop is already closed/closing
                logger.warning(f"Error during loop.shutdown_asyncgens: {e}")

            logger.info("Closing event loop...")
            loop.close()
            logger.info("Event loop closed.")
        
        logger.info("Application finished.")
        # sys.exit code might depend on whether shutdown was clean or due to error
        # For simplicity, always exit 0 if we reach here gracefully.
        # Consider sys.exit(1) if specific unhandled errors occurred.