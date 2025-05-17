
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine
import logging

# Setup logger
logger = logging.getLogger(__name__)

# Base connection string (replace with actual values)

MSSQLS_PWD =  'Ftu5675FDG54hjhiuu$'
# Database connection string
pwd_str = f"Pwd={MSSQLS_PWD};"
conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};Server=35.172.243.170;Database=luxurymarket_p4;Uid=luxurysitescraper;{pwd_str}MultipleActiveResultSets=True" 
# URL-encode connection string for both engines
encoded_conn_str = quote_plus(conn_str)

# Asynchronous engine configuration
async_engine = create_async_engine(
    f"mssql+aioodbc:///?odbc_connect={encoded_conn_str}",
    echo=False,  # Disable verbose logging in production
    pool_size=10,  # Max concurrent connections
    max_overflow=5,  # Allow temporary extra connections
    pool_timeout=30,  # Wait time for a free connection
    pool_pre_ping=True,  # Check connection health
    isolation_level="READ COMMITTED"  # Consistent transaction isolation
)

# Synchronous engine configuration
engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={encoded_conn_str}",
    echo=False,  # Disable verbose logging
    pool_size=10,
    max_overflow=5,
    pool_timeout=30,
    pool_pre_ping=True,
    isolation_level="READ COMMITTED"
)

# Log engine initialization
logger.info(f"Initialized async_engine with conn_str: {conn_str.split(';')[0]}...; MARS=True")
logger.info(f"Initialized engine with conn_str: {conn_str.split(';')[0]}...; MARS=True")
