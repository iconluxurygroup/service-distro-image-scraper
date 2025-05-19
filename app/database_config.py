from urllib.parse import quote_plus
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine
import logging
import os
from config import DB_PASSWORD
from typing import Optional, Dict

logger = logging.getLogger(__name__)

db_password = DB_PASSWORD
if not db_password:
    logger.error("DB_PASSWORD environment variable not set")
    raise ValueError("Database password not provided")

conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"Server=35.172.243.170;Database=luxurymarket_p4;"
    f"Uid=luxurysitescraper;PWD={db_password};"
    f"MultipleActiveResultSets=True"
)

required_params = ["DRIVER", "Server", "Database", "Uid", "PWD", "MultipleActiveResultSets"]
if not all(param in conn_str for param in required_params):
    missing = [param for param in required_params if param not in conn_str]
    logger.error(f"Invalid conn_str: missing parameters {missing}")
    raise ValueError(f"Invalid conn_str: missing {missing}")

encoded_conn_str = quote_plus(conn_str)

async_engine = create_async_engine(
    f"mssql+aioodbc:///?odbc_connect={encoded_conn_str}",
    echo=False,
    pool_size=20,  
    max_overflow=10,
    pool_timeout=60,
    pool_pre_ping=True,
    pool_recycle=3600,
    isolation_level="READ COMMITTED"
)

engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={encoded_conn_str}",
    echo=False,
    pool_size=5,  # Aligned with async_engine
    max_overflow=5,
    pool_timeout=60,  # Aligned with async_engine
    pool_pre_ping=True,
    pool_recycle=3600,
    isolation_level="READ COMMITTED"
)

logger.info(f"Initialized async_engine with conn_str: {conn_str.split(';')[0]}...; MARS=True")
logger.info(f"Initialized engine with conn_str: {conn_str.split(';')[0]}...; MARS=True")