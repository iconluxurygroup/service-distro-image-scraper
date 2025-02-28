import uvicorn
import ray
import logging
from api import app

logging.getLogger(__name__)

if __name__ == "__main__":
    logging.info("Starting Uvicorn server")
    if ray.is_initialized():
        ray.shutdown()
    ray.init()  # Start a local Ray cluster
    uvicorn.run("main:app", port=8080, host='0.0.0.0')