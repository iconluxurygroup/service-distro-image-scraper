import aio_pika
import json
import logging
import asyncio
import signal
import sys
import uuid # Added for UUID type check
import datetime # Added for select execution
from rabbitmq_producer import RabbitMQProducer # Assuming this exists and works
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError, ArgumentError
from database_config import async_engine # Assuming this provides an async SQLAlchemy engine
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import psutil
from typing import Optional, Dict, Any, List, Tuple, Union # Expanded type hints
import aiormq.exceptions

# Ensure logging is configured (using the one from the provided snippet)
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class RabbitMQConsumer:
    def __init__(
        self,
        amqp_url: str = "amqp://app_user:app_password@localhost:5672/app_vhost", # Example URL
        queue_name: str = "db_update_queue",
        connection_timeout: float = 30.0,
        operation_timeout: float = 15.0, # Timeout for DB operations within a task
    ):
        logger.debug("Initializing RabbitMQConsumer")
        self.amqp_url = amqp_url
        self.queue_name = queue_name
        self.connection_timeout = connection_timeout
        self.operation_timeout = operation_timeout # Timeout for individual task processing (DB op)
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.is_consuming = False
        self._consumer_tag: Optional[str] = None # To store consumer tag for cancellation

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((aiormq.exceptions.ChannelInvalidStateError, aio_pika.exceptions.AMQPConnectionError, aiormq.exceptions.ChannelNotFoundEntity))
    )
    async def declare_queue_with_retry(self, channel: aio_pika.Channel, queue_name: str, **kwargs):
        logger.debug(f"Attempting to declare queue: {queue_name} with settings: {kwargs}")
        return await channel.declare_queue(queue_name, **kwargs)

    async def connect(self):
        if self.connection and not self.connection.is_closed:
            logger.debug("Connection already established.")
            if self.channel and not self.channel.is_closed:
                logger.debug("Channel already open.")
                return
            else: # Connection exists but channel might be closed/None
                logger.debug("Connection exists, but channel needs to be (re)opened.")
                try:
                    self.channel = await self.connection.channel()
                    if not self.channel:
                        raise aio_pika.exceptions.AMQPConnectionError("Failed to create channel on existing connection")
                    await self.channel.set_qos(prefetch_count=1)
                    logger.info("Channel (re)opened successfully on existing connection.")
                    return # Successfully reopened channel
                except Exception as e:
                    logger.error(f"Failed to reopen channel on existing connection: {e}. Will attempt full reconnect.")
                    # Fall through to full reconnect logic

        logger.info(f"Attempting to connect to RabbitMQ at {self.amqp_url.split('@')[-1] if '@' in self.amqp_url else self.amqp_url}")
        try:
            async with asyncio.timeout(self.connection_timeout):
                self.connection = await aio_pika.connect_robust(
                    self.amqp_url,
                    # connection_attempts=5, # connect_robust handles this internally
                    # retry_delay=5,
                    timeout=self.connection_timeout # Timeout for individual connection attempt
                )
                self.channel = await self.connection.channel()
                if not self.channel: # Should not happen if connection.channel() succeeds
                    raise aio_pika.exceptions.AMQPConnectionError("Failed to create channel after connection")
                
                self.connection.add_on_close_callback(self._on_connection_closed)
                self.connection.add_on_reconnect_callback(self._on_connection_reconnected)
                self.channel.add_on_close_callback(self._on_channel_closed)

                await self.channel.set_qos(prefetch_count=1) # Process one message at a time

                # Queue declaration logic (simplified from provided snippet)
                if self.queue_name.startswith("select_response_"):
                    # For temporary response queues
                    await self.declare_queue_with_retry(
                        self.channel,
                        self.queue_name,
                        durable=False,
                        exclusive=False, # Not exclusive if another instance might need to declare/check it
                        auto_delete=True,
                        arguments={"x-expires": 600000} # 10 minutes
                    )
                else:
                    # For main durable work queues
                    await self.declare_queue_with_retry(
                        self.channel,
                        self.queue_name,
                        durable=True,
                        exclusive=False,
                        auto_delete=False
                    )
                logger.info(f"Successfully connected to RabbitMQ, channel established, queue '{self.queue_name}' declared/verified.")
        except asyncio.TimeoutError:
            logger.error(f"Timeout connecting to RabbitMQ (timeout: {self.connection_timeout}s)")
            await self.close() # Ensure cleanup on failure
            raise
        except aio_pika.exceptions.ProbableAuthenticationError as e:
            logger.error(f"Authentication error connecting to RabbitMQ: {e}", exc_info=True)
            await self.close()
            raise
        except (aio_pika.exceptions.AMQPConnectionError, OSError) as e: # OSError for network issues
            logger.error(f"Failed to connect to RabbitMQ: {e}", exc_info=True)
            await self.close()
            raise
        except Exception as e: # Catch-all for other unexpected errors during connect
            logger.error(f"Unexpected error during RabbitMQ connection: {e}", exc_info=True)
            await self.close()
            raise

    def _on_connection_closed(self, connection, reason):
        logger.warning(f"RabbitMQ connection closed. Reason: {reason}")
        self.is_consuming = False # Stop consuming if connection drops

    def _on_connection_reconnected(self, connection):
        logger.info("RabbitMQ connection re-established.")
        # Re-setup channel and consumption if needed, connect_robust might handle some of this.
        # If start_consuming was running, it might need to be re-invoked or its loop re-entered.
        # For now, we rely on the main consuming loop to handle reconnection attempts.

    def _on_channel_closed(self, channel, reason):
        logger.warning(f"RabbitMQ channel closed. Reason: {reason}")
        # If robust connection is still open, channel might reopen automatically or need manual intervention.


    async def execute_update(self, task: Dict, conn, logger_param: logging.Logger) -> Dict[str, Any]:
        # Use passed logger_param if different from self.logger for task-specific logging context
        log = logger_param if logger_param else self.logger

        file_id = task.get("file_id", "unknown_file_id")
        task_type = task.get("task_type", "unknown_task_type")
        sql_from_task: str = task["sql"]
        params_from_task: Dict[str, Any] = task.get("params", {})

        final_sql_to_execute: str = sql_from_task
        # final_params_for_db will be a dictionary for named, or list_of_tuples/tuple for positional
        final_params_for_db: Union[Dict[str, Any], List[Tuple[Any, ...]], Tuple[Any, ...]] 

        try:
            if not isinstance(params_from_task, dict):
                log.error(
                    f"TaskType: {task_type}, FileID: {file_id}, Invalid params format: {type(params_from_task)}, expected dict. SQL: {sql_from_task}"
                )
                raise ValueError(f"Invalid params format: {params_from_task}, expected dict")

            list_param_key_for_in_clause = "invalid_ids_list" # The key in params_from_task for the list
            named_placeholder_for_list = f":{list_param_key_for_in_clause}" # e.g., ":invalid_ids_list"

            use_manual_expansion = False
            if list_param_key_for_in_clause in params_from_task and \
               isinstance(params_from_task[list_param_key_for_in_clause], list) and \
               params_from_task[list_param_key_for_in_clause]: # Ensure list is not empty
                
                id_list = params_from_task[list_param_key_for_in_clause]
                # Check if all items are suitable for IN clause (e.g., int, str, UUID)
                if all(isinstance(item, (int, str, uuid.UUID)) for item in id_list):
                    # Only attempt manual expansion if it's the ONLY parameter and the SQL structure is known
                    # This is a safeguard to avoid breaking more complex queries.
                    if f" IN {named_placeholder_for_list}" in sql_from_task and len(params_from_task) == 1:
                        use_manual_expansion = True
            
            if use_manual_expansion:
                id_list_values = params_from_task[list_param_key_for_in_clause]
                log.info(
                    f"TaskType: {task_type}, FileID: {file_id}, "
                    f"Attempting manual SQL expansion for parameter '{list_param_key_for_in_clause}' into IN (?, ?, ...)."
                )
                
                question_mark_placeholders = ", ".join(["?"] * len(id_list_values))
                
                # Robustly replace the named placeholder for the IN clause
                # This assumes the placeholder is well-defined, e.g., " IN :param_name" or " IN (:param_name)"
                # For "IN :param_name"
                target_pattern_simple = f" IN {named_placeholder_for_list}"
                replacement_simple = f" IN ({question_mark_placeholders})"
                # For "IN (:param_name)"
                target_pattern_bracketed = f" IN ({named_placeholder_for_list})"
                replacement_bracketed = f" IN ({question_mark_placeholders})"

                if target_pattern_bracketed in sql_from_task:
                    final_sql_to_execute = sql_from_task.replace(target_pattern_bracketed, replacement_bracketed, 1)
                elif target_pattern_simple in sql_from_task:
                     final_sql_to_execute = sql_from_task.replace(target_pattern_simple, replacement_simple, 1)
                else:
                    log.warning(f"TaskType: {task_type}, FileID: {file_id}, Could not find '{target_pattern_simple}' or '{target_pattern_bracketed}' for manual expansion. Falling back to named params.")
                    use_manual_expansion = False # Force fallback

                if use_manual_expansion:
                    # For manually constructed SQL with '?', SQLAlchemy expects parameters for a single execution
                    # to be a list containing one tuple of values if the `parameters` arg to `execute` is a list.
                    # Or a simple tuple if the `parameters` arg to `execute` is that tuple.
                    # The error "List argument must consist only of tuples or dictionaries" suggests
                    # it's treating the `parameters` arg as a list for `executemany`.
                    final_params_for_db = [tuple(id_list_values)]
                    log.debug(f"TaskType: {task_type}, FileID: {file_id}, Formatted params for manual expansion as List[Tuple]: {str(final_params_for_db)[:200]}...")
            
            if not use_manual_expansion: # Fallback or standard named parameter handling
                log.debug(
                    f"TaskType: {task_type}, FileID: {file_id}, "
                    f"Using named parameter handling. Adapting lists for TVP-style if applicable."
                )
                final_params_for_db = {} # This will be a dictionary for named parameters
                for k, v_val in params_from_task.items():
                    if isinstance(v_val, list) and v_val: # If it's a non-empty list
                        # Check if elements are simple types (e.g., int, str) and need wrapping in tuples for TVP.
                        if not isinstance(v_val[0], (list, tuple)):
                            log.debug(f"TaskType: {task_type}, FileID: {file_id}, Adapting param '{k}' to list of tuples for potential TVP.")
                            final_params_for_db[k] = [(item,) for item in v_val]
                        else:
                            final_params_for_db[k] = v_val # Already list of sequences (tuples/lists)
                    else:
                        final_params_for_db[k] = v_val
                # SQL remains the original template with named placeholders: final_sql_to_execute = sql_from_task

            # --- Database Execution ---
            stmt_for_db = text(final_sql_to_execute)

            params_type_for_log = type(final_params_for_db).__name__
            params_content_for_log = str(final_params_for_db)[:250] # Truncate long params for logging
            if isinstance(final_params_for_db, list) and len(final_params_for_db) > 3:
                 params_content_for_log += "..."
            elif isinstance(final_params_for_db, dict) and len(final_params_for_db) > 3 : # Approx check for many keys
                 params_content_for_log += "..."


            log.debug(
                f"TaskType: {task_type}, FileID: {file_id}, "
                f"Executing DB. Final SQL: \"{final_sql_to_execute}\". "
                f"Params Type: {params_type_for_log}, Params (sample): {params_content_for_log}"
            )
            
            # The second argument to conn.execute() can be a dict (for named params in SQL)
            # or a list of dicts/tuples (for executemany).
            # For a single execution with qmark SQL, it typically expects a tuple of values,
            # OR a list containing a single tuple of values.
            result = await conn.execute(stmt_for_db, final_params_for_db)
            await conn.commit()
            
            rowcount = result.rowcount if result is not None and hasattr(result, 'rowcount') and result.rowcount is not None else 0
            log.info(f"Worker PID {psutil.Process().pid if psutil else 'N/A'}: UPDATE/INSERT affected {rowcount} rows for FileID {file_id}, TaskType {task_type}")
            return {"rowcount": rowcount, "status": "success"}

        except ArgumentError as sa_arg_err: # Specifically catch SQLAlchemy's ArgumentError
            params_at_error_type = type(final_params_for_db).__name__ if 'final_params_for_db' in locals() else "UNKNOWN"
            params_at_error_content = str(final_params_for_db)[:250] if 'final_params_for_db' in locals() else str(params_from_task)[:250]
            log.error(
                f"TaskType: {task_type}, FileID: {file_id}, SQLAlchemy ArgumentError. "
                f"Original SQL: \"{sql_from_task}\", Final SQL: \"{final_sql_to_execute if 'final_sql_to_execute' in locals() else sql_from_task}\", "
                f"Params Type: {params_at_error_type}, Params (sample): {params_at_error_content}, Error: {sa_arg_err}",
                exc_info=True
            )
            raise # Re-raise to be handled by the callback's error handling
        except SQLAlchemyError as db_err:
            params_at_error_type_db = type(final_params_for_db).__name__ if 'final_params_for_db' in locals() else "UNKNOWN"
            params_at_error_content_db = str(final_params_for_db)[:250] if 'final_params_for_db' in locals() else str(params_from_task)[:250]
            log.error(
                f"TaskType: {task_type}, FileID: {file_id}, Database error (SQLAlchemyError). "
                f"Original SQL: \"{sql_from_task}\", Final SQL: \"{final_sql_to_execute if 'final_sql_to_execute' in locals() else sql_from_task}\", "
                f"Params Type: {params_at_error_type_db}, Params (sample): {params_at_error_content_db}, Error: {db_err}",
                exc_info=True
            )
            raise
        except Exception as e:
            params_at_error_type_gen = type(final_params_for_db).__name__ if 'final_params_for_db' in locals() else "UNKNOWN"
            params_at_error_content_gen = str(final_params_for_db)[:250] if 'final_params_for_db' in locals() else str(params_from_task)[:250]
            log.error(
                f"TaskType: {task_type}, FileID: {file_id}, Unexpected error in execute_update. "
                f"Original SQL: \"{sql_from_task}\", Final SQL: \"{final_sql_to_execute if 'final_sql_to_execute' in locals() else sql_from_task}\", "
                f"Params Type: {params_at_error_type_gen}, Params (sample): {params_at_error_content_gen}, Error: {e}",
                exc_info=True
            )
            raise

    async def execute_select(self, task: Dict, conn, logger_param: logging.Logger):
        log = logger_param if logger_param else self.logger
        file_id = task.get("file_id", "unknown_file_id")
        task_type = task.get("task_type", "unknown_select_task")
        select_sql = task.get("sql")
        params = task.get("params", {})
        response_queue_name = task.get("response_queue") # Name of the queue to send results back to
        correlation_id = task.get("correlation_id", str(uuid.uuid4()))

        log.debug(f"[{correlation_id}] Executing SELECT for FileID {file_id}, TaskType {task_type}: {select_sql}, params: {params}")

        # Parameter adaptation for SELECT (if needed, similar to UPDATE but usually simpler)
        # For SELECT with IN clauses, named params with list values are common.
        # SQLAlchemy typically handles these well by expanding to IN (?, ?, ?).
        # If TVP-style is needed for SELECTs, similar adaptation as in execute_update might be required.
        # For now, assume standard named parameter handling for SELECTs.
        adapted_params_select = params.copy() # Use a copy

        try:
            start_time = datetime.datetime.now()
            db_cursor = await conn.execute(text(select_sql), adapted_params_select)
            rows = await db_cursor.fetchall() # Ensure this is awaited if cursor is async
            columns = db_cursor.keys()
            # db_cursor.close() # Not always necessary for async result objects, depends on dialect/usage

            results_list = [dict(zip(columns, row)) for row in rows]
            elapsed_time = (datetime.datetime.now() - start_time).total_seconds()
            log.info(
                f"[{correlation_id}] Worker PID {psutil.Process().pid if psutil else 'N/A'}: "
                f"SELECT for FileID {file_id}, TaskType {task_type}, returned {len(results_list)} rows in {elapsed_time:.2f}s."
            )

            if response_queue_name:
                # Assuming RabbitMQProducer.get_producer is an async class method or similar
                # and handles its own connection/channel.
                # This part needs a robust RabbitMQProducer.
                try:
                    producer = await RabbitMQProducer.get_producer(logger=log, amqp_url=self.amqp_url) # Pass AMQP URL
                    # await producer.connect() # Producer should handle its own connect logic if needed
                    
                    response_payload = {
                        "file_id": file_id,
                        "task_type": task_type,
                        "results": results_list,
                        "status": "success",
                        "correlation_id": correlation_id
                    }
                    await producer.publish_message(
                        response_payload,
                        routing_key=response_queue_name, # This should be the queue name directly
                        correlation_id=correlation_id
                    )
                    log.debug(f"[{correlation_id}] Sent SELECT results to queue '{response_queue_name}' for FileID {file_id}.")
                except Exception as e_pub:
                    log.error(f"[{correlation_id}] Failed to publish SELECT results for FileID {file_id} to '{response_queue_name}': {e_pub}", exc_info=True)
                    # Decide if this error should fail the task or just be logged.
                    # For now, we'll consider the DB part successful.
            return results_list # Return results even if publishing fails, for direct use if any.

        except SQLAlchemyError as e_select:
            log.error(f"[{correlation_id}] Database error executing SELECT for FileID {file_id}, TaskType {task_type}: {e_select}", exc_info=True)
            raise
        except Exception as e_gen_select:
            log.error(f"[{correlation_id}] Unexpected error in execute_select for FileID {file_id}, TaskType {task_type}: {e_gen_select}", exc_info=True)
            raise

    async def callback(self, message: aio_pika.IncomingMessage):
        task_info_str = "unknown_task" # For logging in case of early parsing failure
        try:
            task = json.loads(message.body.decode())
            file_id = task.get("file_id", "unknown_file_id")
            task_type = task.get("task_type", "unknown_task_type")
            correlation_id = message.correlation_id or task.get("correlation_id", str(uuid.uuid4()))
            task_info_str = f"FileID: {file_id}, TaskType: {task_type}, CorrID: {correlation_id}"

            logger.info(f"Received task: {task_info_str}. Message Details: {message. krátký() if hasattr(message,'krátký') else message.delivery_tag}")

            async with asyncio.timeout(self.operation_timeout): # Overall timeout for task processing
                task_processed_successfully = False
                if task_type.startswith("select_"): # Convention for select tasks
                    async with async_engine.connect() as conn: # Use a new connection for each task
                        # Select tasks usually don't modify data, so transaction context might be less critical,
                        # but using `conn` is fine.
                        await self.execute_select(task, conn, logger) # Assumes execute_select handles response publishing
                        task_processed_successfully = True 
                        # ^ execute_select raises on DB error, so if it returns, DB part is okay.
                        # Response publishing failure is logged within execute_select.
                # Example for a specific update task type (add more elif as needed)
                elif task_type == "update_sort_order" and task.get("sql") == "UPDATE_SORT_ORDER": # Special handling if needed
                     async with async_engine.begin() as conn: # Use 'begin' for transaction
                        # This implies execute_sort_order_update is a method in this class
                        # await self.execute_sort_order_update(task.get("params", {}), file_id, conn, logger)
                        # For now, let's assume it's a generic update handled by execute_update
                        result_data = await self.execute_update(task, conn, logger)
                        task_processed_successfully = result_data.get("status") == "success"
                else: # Default to general update/insert
                    async with async_engine.begin() as conn: # Use 'begin' for transaction (commit/rollback)
                        result_data = await self.execute_update(task, conn, logger)
                        task_processed_successfully = result_data.get("status") == "success"
            
            if task_processed_successfully:
                await message.ack()
                logger.info(f"Successfully processed and ACKed task: {task_info_str}")
            else:
                # This 'else' might not be reached if execute_update/select raises an exception,
                # as exceptions are caught below and lead to NACK.
                # This path is for cases where the methods return a failure status without raising.
                logger.warning(f"Task processing reported failure (but no exception raised): {task_info_str}. NACKing for re-queue.")
                await message.nack(requeue=True) # Requeue for another attempt

        except asyncio.TimeoutError:
            logger.error(f"Timeout processing task: {task_info_str} (op timeout: {self.operation_timeout}s). NACKing for re-queue.")
            await message.nack(requeue=True)
        except json.JSONDecodeError as json_err:
            logger.error(f"Failed to decode JSON from message body: {message.body[:100]}... Error: {json_err}. Message rejected (not re-queued).")
            await message.reject(requeue=False) # Bad message, don't requeue
        except ValueError as val_err: # e.g., from invalid params format check in execute_update
            logger.error(f"ValueError during task processing: {task_info_str}. Error: {val_err}. NACKing for re-queue (check if should be reject).")
            # Consider if this should be reject(requeue=False) if it's a consistently bad message.
            await message.nack(requeue=True)
        except (SQLAlchemyError, ArgumentError) as db_processing_err: # Catch errors from execute_update/select
            logger.error(f"Database-related error processing task: {task_info_str}. Error: {db_processing_err}. NACKing for re-queue.")
            await message.nack(requeue=True) # Requeue on DB errors, could be transient
        except Exception as e:
            logger.error(f"Unexpected error processing task: {task_info_str}. Error: {e}. NACKing for re-queue.", exc_info=True)
            await message.nack(requeue=True) # Requeue for other unexpected errors
        finally:
            # Ensure some brief pause to prevent tight loops on repeated failures for the same message
            # if nack(requeue=True) is used. This is a simple form of backoff.
            # A more sophisticated backoff/dead-lettering strategy would be better for production.
            await asyncio.sleep(0.1) # Small delay

    async def start_consuming(self):
        if self.is_consuming:
            logger.info("Consumer is already consuming.")
            return

        await self.connect() # Ensure connection is established before starting consumer
        
        if not self.channel or self.channel.is_closed:
            logger.error("Cannot start consuming, channel is not available.")
            # Attempt to reconnect before giving up or re-raising
            try:
                await self.connect() # Try one more time
                if not self.channel or self.channel.is_closed:
                     raise aio_pika.exceptions.ChannelInvalidStateError("Channel unavailable after reconnect attempt.")
            except Exception as e_reconnect:
                logger.error(f"Failed to re-establish channel before starting consumer: {e_reconnect}")
                raise # Propagate error if channel cannot be established

        logger.info(f"Starting to consume messages from queue: '{self.queue_name}'")
        self.is_consuming = True
        try:
            queue = await self.channel.get_queue(self.queue_name, ensure=True) # ensure=True if not sure it exists
            self._consumer_tag = await queue.consume(self.callback)
            logger.info(f"Consumer started with tag '{self._consumer_tag}'. Waiting for messages. Press CTRL+C to exit.")
            # Keep the consumer running indefinitely until an exit signal or error
            await asyncio.Event().wait() # This will wait forever unless event is set
        except aiormq.exceptions.ChannelClosed as e_chan_closed:
            logger.error(f"Channel closed while trying to consume: {e_chan_closed}. Attempting to restart consumption.")
            self.is_consuming = False
            await self.close_channel_only() # Close only channel, robust connection might still be there
            # Consider a delay before restarting
            await asyncio.sleep(5)
            # await self.start_consuming() # Recursive call, be careful with depth
        except aio_pika.exceptions.AMQPConnectionError as e_conn_err:
            logger.error(f"Connection error during consumption: {e_conn_err}. Attempting to restart consumption.")
            self.is_consuming = False
            await self.close() # Full close and reconnect
            await asyncio.sleep(5)
            # await self.start_consuming()
        except asyncio.CancelledError:
            logger.info("Consumption task cancelled. Shutting down consumer.")
            # self.is_consuming should be set to False by the caller initiating cancellation, or in close()
            await self.close() # Graceful shutdown
        except Exception as e:
            logger.error(f"Unexpected error during consumption: {e}", exc_info=True)
            self.is_consuming = False
            await self.close() # Attempt to cleanup
            # Consider re-raising or specific handling
            # await asyncio.sleep(5)
            # await self.start_consuming()
        finally:
            logger.info("Consumption loop finished or exited.")
            self.is_consuming = False


    async def close_channel_only(self):
        if self.channel and not self.channel.is_closed:
            try:
                # If there's an active consumer, cancel it first
                if self._consumer_tag and self.channel and not self.channel.is_closed: # Check channel again
                    logger.info(f"Attempting to cancel consumer with tag: {self._consumer_tag}")
                    await self.channel.basic_cancel(self._consumer_tag, nowait=False)
                    logger.info(f"Consumer '{self._consumer_tag}' cancelled.")
                self._consumer_tag = None
                await asyncio.wait_for(self.channel.close(), timeout=5)
                logger.info("RabbitMQ channel closed.")
            except asyncio.TimeoutError:
                logger.warning("Timeout closing RabbitMQ channel.")
            except (aio_pika.exceptions.AMQPError, aiormq.exceptions.AMQPError) as e:
                logger.warning(f"Error closing RabbitMQ channel: {e}")
            finally:
                self.channel = None

    async def close(self):
        logger.info("Attempting to close RabbitMQ consumer gracefully...")
        self.is_consuming = False # Signal to stop any consumption loops

        await self.close_channel_only() # Close channel and cancel consumer first

        if self.connection and not self.connection.is_closed:
            try:
                await asyncio.wait_for(self.connection.close(), timeout=10) # Increased timeout for connection close
                logger.info("RabbitMQ connection closed.")
            except asyncio.TimeoutError:
                logger.warning("Timeout closing RabbitMQ connection.")
            except (aio_pika.exceptions.AMQPError, aiormq.exceptions.AMQPError, OSError) as e: # Add OSError for underlying network issues
                logger.warning(f"Error closing RabbitMQ connection: {e}")
            finally:
                self.connection = None
        
        logger.info("RabbitMQ consumer resources released.")

# --- Main execution and shutdown handling ---
# Global variable to hold the main consuming task
main_consumer_task_global: Optional[asyncio.Task] = None

async def graceful_shutdown(loop: asyncio.AbstractEventLoop, consumer: RabbitMQConsumer, sig: Optional[signal.Signals] = None):
    if sig:
        logger.info(f"Received signal {sig.name}, initiating graceful shutdown...")
    else:
        logger.info("Initiating graceful shutdown...")

    global main_consumer_task_global
    if main_consumer_task_global and not main_consumer_task_global.done():
        logger.info("Cancelling main consumer task...")
        main_consumer_task_global.cancel()
        try:
            await main_consumer_task_global # Wait for it to finish cancellation
        except asyncio.CancelledError:
            logger.info("Main consumer task successfully cancelled.")
        except Exception as e:
            logger.error(f"Exception during main consumer task cancellation: {e}", exc_info=True)
    
    await consumer.close() # Close RabbitMQ connection and channel
    
    logger.info("Disposing database engine...")
    await async_engine.dispose() # Dispose of the SQLAlchemy engine pool

    # Cancel all other remaining tasks
    # This needs to be done carefully to avoid cancelling the shutdown task itself.
    current_task = asyncio.current_task()
    tasks = [t for t in asyncio.all_tasks(loop) if t is not current_task and t is not main_consumer_task_global]
    if tasks:
        logger.info(f"Cancelling {len(tasks)} outstanding tasks...")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True) # Wait for them to cancel
        logger.info("Outstanding tasks cancelled.")

    logger.info("Shutting down async generators...")
    try:
        # This should be called when the loop is still running or just before it's stopped.
        await loop.shutdown_asyncgens()
    except RuntimeError as e:
        logger.warning(f"Could not shutdown async generators (loop state issue?): {e}")


    if loop.is_running(): # Should only stop if it was started with run_forever
        logger.info("Stopping event loop...")
        loop.stop()


def signal_handler_wrapper(loop: asyncio.AbstractEventLoop, consumer: RabbitMQConsumer):
    # Flag to prevent multiple shutdown initiations
    _shutdown_initiated_flag = asyncio.Event() 

    def actual_handler(sig, frame):
        if _shutdown_initiated_flag.is_set():
            logger.warning("Shutdown already in progress, ignoring signal.")
            return
        _shutdown_initiated_flag.set()
        logger.info(f"Signal {sig} received by handler. Scheduling graceful_shutdown.")
        # Schedule the shutdown coroutine to run on the event loop
        # This is safer than calling loop.run_until_complete from the signal handler directly
        asyncio.create_task(graceful_shutdown(loop, consumer, sig))
    return actual_handler


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    consumer = RabbitMQConsumer() # Use your actual AMQP URL and queue name

    # Setup signal handlers
    shutdown_func = signal_handler_wrapper(loop, consumer)
    for sig_name in ('SIGINT', 'SIGTERM'):
        sig = getattr(signal, sig_name, None)
        if sig:
            try:
                loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(graceful_shutdown(loop, consumer, s)))
                logger.info(f"Registered signal handler for {sig_name}")
            except NotImplementedError: # e.g. on Windows for add_signal_handler
                 signal.signal(sig, lambda s, f: asyncio.create_task(graceful_shutdown(loop, consumer, s))) # Fallback
                 logger.info(f"Registered fallback signal handler for {sig_name}")


    try:
        logger.info("Starting RabbitMQ consumer application...")
        main_consumer_task_global = loop.create_task(consumer.start_consuming())
        loop.run_forever() # Runs until loop.stop() is called
    except KeyboardInterrupt: # Should be handled by signals, but as a fallback
        logger.info("KeyboardInterrupt received directly in main loop (should be rare).")
    except Exception as e_main:
        logger.critical(f"Critical unhandled exception in main application thread: {e_main}", exc_info=True)
    finally:
        logger.info("Application main loop has exited. Ensuring final cleanup...")
        
        # If loop wasn't stopped by graceful_shutdown for some reason (e.g. error before stop)
        if loop.is_running():
            logger.warning("Loop was still running in final finally block. Manually scheduling final shutdown phase.")
            # This ensures that if run_forever exited due to an unhandled error in a task
            # *before* graceful_shutdown could stop the loop, we still try to cleanup.
            loop.run_until_complete(graceful_shutdown(loop, consumer)) # Try to run the full shutdown
        
        logger.info("Closing event loop...")
        if not loop.is_closed(): # Check if it's not already closed
            loop.close()
        logger.info("Application shut down.")

