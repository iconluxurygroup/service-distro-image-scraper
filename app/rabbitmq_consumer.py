@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type((SQLAlchemyError, aio_pika.exceptions.AMQPError, asyncio.TimeoutError)),
    before_sleep=lambda retry_state: default_logger.info(
        f"Retrying insert_search_results for FileID {retry_state.kwargs.get('file_id')} "
        f"(attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def insert_search_results(
    results: List[Dict],
    logger: Optional[logging.Logger] = None,
    file_id: str = None,
    background_tasks: Optional[BackgroundTasks] = None
) -> bool:
    logger = logger or default_logger
    process = psutil.Process()
    logger.info(f"Worker PID {process.pid}: Starting insert_search_results for FileID {file_id}")
    log_filename = f"job_logs/job_{file_id}.log"
    
    if not results:
        logger.warning(f"Worker PID {process.pid}: Empty results provided")
        return False

    global producer
    try:
        async with asyncio.timeout(60):
            if not producer or not producer.is_connected or not producer.channel or producer.channel.is_closed:
                logger.warning("RabbitMQ producer not initialized or disconnected, reconnecting")
                producer = await get_producer(logger)  # Await get_producer
    except Exception as e:
        logger.error(f"Worker PID {process.pid}: Failed to initialize RabbitMQ producer: {e}", exc_info=True)
        raise ValueError(f"Failed to initialize RabbitMQ producer: {str(e)}")

    channel = producer.channel
    if channel is None:
        logger.error(f"Worker PID {process.pid}: RabbitMQ channel is not available for FileID {file_id}")
        return False

    correlation_id = str(uuid.uuid4())
    response_queue = f"select_response_{correlation_id}"

    try:
        queue = await channel.declare_queue(response_queue, exclusive=True, auto_delete=True)
        
        entry_ids = list(set(row["EntryID"] for row in results))
        existing_keys = set()
        if entry_ids:
            placeholders = ",".join([f":id{i}" for i in range(len(entry_ids))])
            select_query = f"""
                SELECT EntryID, ImageUrl
                FROM utb_ImageScraperResult
                WHERE EntryID IN ({placeholders})
            """
            params = {f"id{i}": entry_id for i, entry_id in enumerate(entry_ids)}
            await enqueue_db_update(
                file_id=file_id,
                sql=select_query,
                params=params,
                background_tasks=background_tasks,
                task_type="select_deduplication",
                producer=producer,
                response_queue=response_queue,
                correlation_id=correlation_id,
                return_result=True
            )
            logger.info(f"Worker PID {process.pid}: Enqueued SELECT query for {len(entry_ids)} EntryIDs")

            response_received = asyncio.Event()
            response_data = None

            async def consume_responses():
                nonlocal response_data
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        async with message.process():
                            if message.correlation_id == correlation_id:
                                response_data = json.loads(message.body.decode())
                                response_received.set()
                                break

            consume_task = asyncio.create_task(consume_responses())
            try:
                async with asyncio.timeout(30):
                    await response_received.wait()
                if response_data and "results" in response_data:
                    existing_keys = {(row["EntryID"], row["ImageUrl"]) for row in response_data["results"]}
                    logger.info(f"Worker PID {process.pid}: Received {len(existing_keys)} deduplication results")
                else:
                    logger.warning(f"Worker PID {process.pid}: No deduplication results received")
            except asyncio.TimeoutError:
                logger.warning(f"Worker PID {process.pid}: Timeout waiting for SELECT results")
                return False
            finally:
                consume_task.cancel()
                await asyncio.sleep(0.1)

        update_query = """
            UPDATE utb_ImageScraperResult
            SET ImageDesc = :ImageDesc,
                ImageSource = :ImageSource,
                ImageUrlThumbnail = :ImageUrlThumbnail,
                CreateTime = :CreateTime
            WHERE EntryID = :EntryID AND ImageUrl = :ImageUrl
        """
        insert_query = """
            INSERT INTO utb_ImageScraperResult (EntryID, ImageUrl, ImageDesc, ImageSource, ImageUrlThumbnail, CreateTime)
            VALUES (:EntryID, :ImageUrl, :ImageDesc, :ImageSource, :ImageUrlThumbnail, :CreateTime)
        """

        update_batch = []
        insert_batch = []
        create_time = datetime.datetime.now().isoformat()
        for row in results:
            key = (row["EntryID"], row["ImageUrl"])
            params = {
                "EntryID": row["EntryID"],
                "ImageUrl": row["ImageUrl"],
                "ImageDesc": row.get("ImageDesc", ""),
                "ImageSource": row.get("ImageSource", ""),
                "ImageUrlThumbnail": row.get("ImageUrlThumbnail", ""),
                "CreateTime": create_time
            }
            if key in existing_keys:
                update_batch.append((update_query, params))
            else:
                insert_batch.append((insert_query, params))

        batch_size = 100
        for i in range(0, len(update_batch), batch_size):
            batch = update_batch[i:i + batch_size]
            for sql, params in batch:
                await enqueue_db_update(
                    file_id=file_id,
                    sql=sql,
                    params=params,
                    background_tasks=background_tasks,
                    task_type="update_search_result",
                    producer=producer,
                    correlation_id=str(uuid.uuid4())
                )
        logger.info(f"Worker PID {process.pid}: Enqueued {len(update_batch)} updates")

        for i in range(0, len(insert_batch), batch_size):
            batch = insert_batch[i:i + batch_size]
            for sql, params in batch:
                await enqueue_db_update(
                    file_id=file_id,
                    sql=sql,
                    params=params,
                    background_tasks=background_tasks,
                    task_type="insert_search_result",
                    producer=producer,
                    correlation_id=str(uuid.uuid4())
                )
        logger.info(f"Worker PID {process.pid}: Enqueued {len(insert_batch)} inserts")

        return len(insert_batch) > 0 or len(update_batch) > 0

    except Exception as e:
        logger.error(f"Worker PID {process.pid}: Error in insert_search_results for FileID {file_id}: {e}", exc_info=True)
        return False
    finally:
        try:
            await channel.delete_queue(response_queue)
        except Exception as e:
            logger.warning(f"Worker PID {process.pid}: Failed to delete response queue {response_queue}: {e}")