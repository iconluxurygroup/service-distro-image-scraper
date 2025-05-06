import uvicorn
import logging
import ray
from fastapi.middleware.cors import CORSMiddleware
from api import app


# Configure basic logging
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

if __name__ == "__main__":
    logger.info("Starting Uvicorn server")

    # Add CORS middleware to the FastAPI app
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["*"],
    )

    # Shutdown any existing Ray instance to avoid conflicts
    if ray.is_initialized():
        ray.shutdown()

    # Initialize Ray with dashboard enabled
    dashboard_host = "0.0.0.0"
    dashboard_port = 8265
    ray.init(
        dashboard_host=dashboard_host,
        dashboard_port=dashboard_port,
        include_dashboard=True
    )
    logger.info(f"Ray initialized with dashboard at http://localhost:{dashboard_port}")

    # Run Uvicorn server
    uvicorn.run(app, port=8080, host='0.0.0.0')