import logging
import platform
import signal
import sys
import asyncio
from fastapi.middleware.cors import CORSMiddleware
from fastapi import BackgroundTasks
from api import app
import os
from waitress import serve
from sqlalchemy.sql import text
from rabbitmq_producer import cleanup_producer, get_producer
from logging_config import setup_job_logger
from search_utils import insert_search_results
from database_config import async_engine

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

async def test_insert_search_result():
    """Test inserting a sample search result on application startup."""
    file_id = "test_file_123"
    logger, log_filename = setup_job_logger(job_id=file_id, console_output=True)
    logger.info("Running test insertion of search result on startup")

    local_producer = None
    try:
        # Sample search result
        sample_result = [
            {
                "EntryID": 9999,  # Dummy EntryID
                "ImageUrl": "https://example.com/test_image.jpg",
                "ImageDesc": "Test image description",
                "ImageSource": "example.com",
                "ImageUrlThumbnail": "https://example.com/test_thumbnail.jpg"
            }
        ]

        # Initialize local RabbitMQ producer
        try:
            async with asyncio.timeout(10):
                local_producer = await get_producer(logger)
            if not local_producer or not local_producer.is_connected:
                logger.error("Failed to initialize RabbitMQ producer for test")
                return
            logger.info("Local RabbitMQ producer initialized for test")
        except Exception as e:
            logger.error(f"Failed to initialize RabbitMQ producer: {e}", exc_info=True)
            return

        # Use BackgroundTasks for enqueuing DB update
        background_tasks = BackgroundTasks()

        # Insert the sample result with local producer
        try:
            success = await insert_search_results(
                results=sample_result,
                logger=logger,
                file_id=file_id,
                background_tasks=background_tasks,
                producer=local_producer  # Pass local producer
            )
            if success:
                logger.info(f"Test search result inserted successfully for EntryID 9999, FileID {file_id}")
            else:
                logger.error(f"Failed to insert test search result for EntryID 9999, FileID {file_id}")
        except Exception as e:
            logger.error(f"Error inserting test search result: {e}", exc_info=True)

        # Verify the insertion
        try:
            async with async_engine.connect() as conn:
                result = await conn.execute(
                    text("""
                        SELECT COUNT(*) 
                        FROM utb_ImageScraperResult 
                        WHERE EntryID = :entry_id
                    """),
                    {"entry_id": 9999}
                )
                count = result.fetchone()[0]
                result.close()
                if count > 0:
                    logger.info(f"Verification: Found {count} record(s) for EntryID 9999")
                else:
                    logger.warning(f"Verification failed: No records found for EntryID 9999")
        except Exception as e:
            logger.error(f"Error verifying insertion: {e}", exc_info=True)

    except Exception as e:
        logger.error(f"Unexpected error during test insertion: {e}", exc_info=True)
    finally:
        # Clean up local producer
        if local_producer and local_producer.is_connected:
            try:
                await local_producer.close()
                logger.info("Local RabbitMQ producer closed")
            except Exception as e:
                logger.error(f"Error closing local producer: {e}", exc_info=True)
        await async_engine.dispose()
        logger.info("Test insertion completed, database engine disposed")

async def shutdown(signalnum):
    logger.info(f"Received shutdown signal {signalnum}, stopping gracefully")
    try:
        await cleanup_producer()
    except Exception as e:
        logger.error(f"Error during cleanup: {e}", exc_info=True)
    sys.exit(0)

def main():
    logger.info("Starting application")

    # Fix Ultralytics config
    os.environ["YOLO_CONFIG_DIR"] = "/tmp/ultralytics_config"

    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["*"],
    )

    # Create a new event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Register shutdown handlers
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: loop.create_task(shutdown(sig)))

    # Run test insertion on startup
    try:
        loop.run_until_complete(test_insert_search_result())
    except Exception as e:
        logger.error(f"Error running test insertion: {e}", exc_info=True)
        loop.close()
        sys.exit(1)

    # Run server
    try:
        if platform.system() == "Windows":
            logger.info("Running Waitress on Windows")
            loop.close()  # Close loop before Waitress
            serve(
                app,
                host="0.0.0.0",
                port=8080,
                threads=int(os.cpu_count() / 2 + 1),
                connection_limit=1000,
                asyncore_loop_timeout=120
            )
        else:
            logger.info("Running Gunicorn with Uvicorn workers on Unix")
            from gunicorn.app.base import BaseApplication
            from gunicorn.config import Config
            from uvicorn.workers import UvicornWorker

            class StandaloneApplication(BaseApplication):
                def __init__(self, app, options=None):
                    self.options = options or {}
                    self.application = app
                    super().__init__()

                def load_config(self):
                    config = Config()
                    for key, value in self.options.items():
                        config.set(key, value)
                    self.cfg = config

                def load(self):
                    return self.application

            options = {
                "bind": "0.0.0.0:8080",
                "workers": int(os.cpu_count() / 2 + 1),
                "worker_class": "uvicorn.workers.UvicornWorker",
                "loglevel": "info",
                "timeout": 600,
                "graceful_timeout": 580,
                "proc_name": "gunicorn_large_batch",
                "accesslog": "-",
                "errorlog": "-",
                "logconfig_dict": {
                    "loggers": {
                        "gunicorn": {"level": "INFO", "handlers": ["console"], "propagate": False},
                        "uvicorn": {"level": "INFO", "handlers": ["console"], "propagate": False},
                    },
                    "handlers": {
                        "console": {
                            "class": "logging.StreamHandler",
                            "formatter": "generic",
                            "stream": "ext://sys.stdout",
                        },
                    },
                    "formatters": {
                        "generic": {
                            "format": "%(asctime)s [%(process)d] [%(levelname)s] %(message)s",
                            "datefmt": "[%Y-%m-%d %H:%M:%S %z]",
                        },
                    },
                },
            }
            loop.close()  # Close loop before Gunicorn
            StandaloneApplication(app, options).run()
    except Exception as e:
        logger.error(f"Error starting server: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if not loop.is_closed():
            loop.close()

if __name__ == "__main__":
    main()