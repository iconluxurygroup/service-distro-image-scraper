import logging
import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine
from typing import Optional, Dict
from config import DB_PASSWORD
from sqlalchemy.sql import text
logger = logging.getLogger(__name__)

# Configure logging if no handlers exist
if not logger.handlers:
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)

# Load database configuration from environment variables
DB_SERVER = os.getenv("DB_SERVER", "35.172.243.170")
DB_NAME = os.getenv("DB_NAME", "luxurymarket_p4")
DB_USER = os.getenv("DB_USER", "luxurysitescraper")
DB_PASSWORD = os.getenv("DB_PASSWORD", DB_PASSWORD)

if not all([DB_SERVER, DB_NAME, DB_USER, DB_PASSWORD]):
    missing = [k for k, v in {"DB_SERVER": DB_SERVER, "DB_NAME": DB_NAME, "DB_USER": DB_USER, "DB_PASSWORD": DB_PASSWORD}.items() if not v]
    logger.error(f"Missing environment variables: {missing}")
    raise ValueError(f"Missing required database configuration: {missing}")

# Connection string
conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"Server={DB_SERVER};Database={DB_NAME};"
    f"Uid={DB_USER};PWD={DB_PASSWORD};"
    f"MultipleActiveResultSets=False"  # Disable MARS unless required
)

required_params = ["DRIVER", "Server", "Database", "Uid", "PWD", "MultipleActiveResultSets"]
if not all(param in conn_str for param in required_params):
    missing = [param for param in required_params if param not in conn_str]
    logger.error(f"Invalid conn_str: missing parameters {missing}")
    raise ValueError(f"Invalid conn_str: missing {missing}")

encoded_conn_str = quote_plus(conn_str)

# Create engines with error handling
try:
    async_engine = create_async_engine(
        f"mssql+aioodbc:///?odbc_connect={encoded_conn_str}",
        echo=False,
        pool_size=10,           # Reduced for typical workloads
        max_overflow=20,        # Allow moderate overflow
        pool_timeout=30,        # Reduced timeout
        pool_pre_ping=True,
        pool_recycle=3600,
        isolation_level="READ COMMITTED"
    )
    logger.info(f"Initialized async_engine with server: {DB_SERVER}, database: {DB_NAME}")

    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={encoded_conn_str}",
        echo=False,
        pool_size=5,
        max_overflow=5,
        pool_timeout=30,
        pool_pre_ping=True,
        pool_recycle=3600,
        isolation_level="READ COMMITTED"
    )
    logger.info(f"Initialized sync engine with server: {DB_SERVER}, database: {DB_NAME}")

    # Test connection
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    logger.info("Database connection test successful")
except Exception as e:
    logger.error(f"Failed to initialize database engines: {e}", exc_info=True)
    raise