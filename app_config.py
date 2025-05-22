from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import create_engine
S3_CONFIG = {
    "endpoint": "https://s3.us-east-2.amazonaws.com",
    "region": "us-east-2",
    "access_key": "AKIA2CUNLEV6V627SWI7",
    "secret_key": "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",
    "bucket_name": "iconluxurygroup",
    "r2_endpoint": "https://aa2f6aae69e7fb4bd8e2cd4311c411cb.r2.cloudflarestorage.com",
    "r2_access_key": "8b5a4a988c474205e0172eab5479d6f2",
    "r2_secret_key": "8ff719bbf2946c1b6a81fcf2121e1a41604a0b6f2890f308871b381e98a8d725",
    "r2_account_id": "aa2f6aae69e7fb4bd8e2cd4311c411cb",
    "r2_bucket_name": "iconluxurygroup",
    "r2_custom_domain": "https://iconluxury.group",
}
VERSION="3.8.2"
SENDER_EMAIL="nik@luxurymarket.com"
SENDER_PASSWORD="wvug kynd dfhd xrjh"
SENDER_NAME='superscraper'
GOOGLE_API_KEY='AIzaSyDXfc_kdxa5UX2h9D3WwktefCqdyjHasn8'
# AWS credentials and region
conn_str = "DRIVER={ODBC Driver 17 for SQL Server};Server=35.172.243.170;Database=luxurymarket_p4;Uid=luxurysitescraper;Pwd=Ftu5675FDG54hjhiuu$;"
async_engine = create_async_engine("mssql+aioodbc:///?odbc_connect=" + conn_str)
engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={conn_str}",
    echo=False,
    pool_size=5,  # Aligned with async_engine
    max_overflow=5,
    pool_timeout=60,  # Aligned with async_engine
    pool_pre_ping=True,
    pool_recycle=3600,
    isolation_level="READ COMMITTED"
)
