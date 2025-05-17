import logging
import platform
import signal
import sys
import os
import psutil
import aiohttp
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from waitress import serve

# Configure logging
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(process)d] [%(levelname)s] %(message)s',
        datefmt='[%Y-%m-%d %H:%M:%S %z]'
    )

# FastAPI app
app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# Pydantic model for request body
class ProcessEntryRequest(BaseModel):
    entry_id: int
    query: str
    region: str
    search_type: str
    max_results: int = 50

# Retry logic for API calls
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((aiohttp.ClientError, TimeoutError)),
    before_sleep=lambda retry_state: logger.info(
        f"Retrying API call (attempt {retry_state.attempt_number}/3) after {retry_state.next_action.sleep}s"
    )
)
async def fetch_image_results(query: str, region: str, timeout: int = 60):
    """Fetch image search results asynchronously with retry."""
    process = psutil.Process()
    mem_info = process.memory_info()
    logger.debug(f"Memory usage before fetch: RSS={mem_info.rss / 1024**2:.2f}MB")

    url = f"https://api.thedataproxy.com/v2/proxy/fetch?region={region}"
    params = {"q": query, "tbm": "isch"}
    logger.info(f"Worker PID {process.pid}: Fetching {url} with query: {query}")

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, timeout=timeout) as response:
            response.raise_for_status()
            results = await response.json()
            valid_results = [r for r in results.get('results', []) if r.get('thumbnail_url')]
            logger.debug(f"Worker PID {process.pid}: Received {len(valid_results)} valid results")

            mem_info = process.memory_info()
            logger.debug(f"Memory usage after fetch: RSS={mem_info.rss / 1024**2:.2f}MB")
            if not valid_results:
                logger.warning("No valid thumbnail URLs found in results")
            return valid_results

@app.post("/process_entry")
async def process_entry(request: ProcessEntryRequest):
    """Process a single entry with retryable API call."""
    process = psutil.Process()
    logger.info(f"Worker PID {process.pid}: Processing EntryID {request.entry_id} with search type '{request.search_type}'")
    try:
        results = await fetch_image_results(request.query, request.region)
        filtered_results = results[:request.max_results]
        logger.info(f"Worker PID {process.pid}: Filtered {len(filtered_results)} results for EntryID {request.entry_id}")
        logger.info(f"Worker PID {process.pid}: GCloud attempt succeeded for EntryID {request.entry_id} with {len(filtered_results)} images")
        return {"entry_id": request.entry_id, "results": filtered_results}
    except (aiohttp.ClientError, TimeoutError) as e:
        logger.error(f"Worker PID {process.pid}: Task failed for EntryID {request.entry_id}: {e}")
        raise HTTPException(status_code=503, detail=f"Task failed: {str(e)}")
    except Exception as e:
        logger.critical(f"Worker PID {process.pid}: Unexpected error for EntryID {request.entry_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

def shutdown(signalnum, frame):
    logger.info(f"Master PID {os.getpid()}: Received shutdown signal, stopping gracefully")
    sys.exit(0)

if __name__ == "__main__":
    logger.info(f"Master PID {os.getpid()}: Starting application")

    # Fix Ultralytics config
    os.environ["YOLO_CONFIG_DIR"] = "/tmp/ultralytics_config"

    # Register shutdown handlers
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    # Run server
    if platform.system() == "Windows":
        logger.info("Running Waitress on Windows")
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
            "timeout": 600,  # Increased to 10 minutes
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
        StandaloneApplication(app, options).run()