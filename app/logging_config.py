# logging_config.py
import logging
import os
from logging.handlers import RotatingFileHandler
import uuid

def setup_job_logger(job_id=None, log_dir="job_logs", console_output=True):
    """
    Set up a logger for a specific job or process.
    
    Args:
        job_id (str, optional): Identifier (e.g., file_id or UUID). Defaults to UUID if None.
        log_dir (str): Directory for log files.
        console_output (bool): Whether to include console output in addition to file logging.
    
    Returns:
        tuple: (logger, log_filename)
    """
    if job_id is None:
        job_id = str(uuid.uuid4())  # Unique identifier if no job_id provided
    
    # Ensure log directory exists
    os.makedirs(log_dir, exist_ok=True)
    log_filename = os.path.join(log_dir, f"job_{job_id}.log")
    
    # Get or create a logger specific to this job
    logger = logging.getLogger(f"job_{job_id}")
    logger.setLevel(logging.INFO)
    
    # Prevent propagation to root logger to avoid duplicate logging
    logger.propagate = False
    
    # Clear existing handlers to ensure no duplicates
    if logger.handlers:
        logger.handlers.clear()
    
    # Set up file handler with rotation
    file_handler = RotatingFileHandler(
        log_filename,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=1
    )
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Optionally add console handler
    if console_output:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    return logger, log_filename

# Optional: Configure root logger minimally if needed (avoid handlers unless explicitly required)
root_logger = logging.getLogger()
if not root_logger.handlers:
    root_logger.setLevel(logging.INFO)