import uvicorn
import ray
import logging
from api import app

# Configure basic logging
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

if __name__ == "__main__":
    logger.info("Starting Uvicorn server")

    # Shutdown any existing Ray instance to avoid conflicts
    if ray.is_initialized():
        ray.shutdown()

    # Define dashboard host and port
    dashboard_host = "0.0.0.0"  # Accessible externally
    dashboard_port = 8265       # Default Ray dashboard port

    # Initialize Ray with dashboard enabled
    ray.init(
        dashboard_host=dashboard_host,
        dashboard_port=dashboard_port,
        include_dashboard=True
    )

    # Manually construct the dashboard URL
    logger.info(f"Ray initialized with dashboard at http://localhost:{dashboard_port}")

    # Run Uvicorn server
    uvicorn.run("main:app", port=8080, host='0.0.0.0')
