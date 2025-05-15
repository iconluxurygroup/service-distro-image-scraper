import logging
import ray
from fastapi.middleware.cors import CORSMiddleware
from api import app
import os
import platform
import signal
import sys
from waitress import serve
import shutil
import tempfile

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def shutdown(signalnum, frame):
    logger.info("Received shutdown signal, stopping gracefully")
    if ray.is_initialized():
        ray.shutdown()
    sys.exit(0)

if __name__ == "__main__":
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

    # Clean up previous Ray sessions
    ray_temp_dir = os.path.join(tempfile.gettempdir(), "ray")
    if os.path.exists(ray_temp_dir):
        try:
            shutil.rmtree(ray_temp_dir)
            logger.info("Cleaned up previous Ray session directory")
        except Exception as e:
            logger.warning(f"Failed to clean up Ray session directory: {e}")

    # Initialize Ray
    if ray.is_initialized():
        ray.shutdown()
    if platform.system() == "Windows":
        ray.init(
            include_dashboard=False,
            logging_level=logging.ERROR,
            configure_logging=True,
            log_to_driver=True
        )
        logger.info("Ray initialized without dashboard on Windows")
    else:
        ray.init(
            dashboard_host="127.0.0.1",
            dashboard_port=8265,  # Changed to avoid conflict
            include_dashboard=True
        )
        logger.info("Ray initialized with dashboard on Unix at http://127.0.0.1:8266")

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
            "bind": f"0.0.0.0:8080",
            "workers": int(os.cpu_count() / 2 + 1),
            "worker_class": "uvicorn.workers.UvicornWorker",
            "loglevel": "info",
            "timeout": 300,  # Increase to 300 seconds
            "graceful_timeout": 280,
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