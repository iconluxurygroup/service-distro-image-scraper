from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import create_engine
S3_CONFIG = {
    "endpoint": "https://s3.us-east-2.amazonaws.com",
    "region": "us-east-2",
    "access_key": "AKIA2CUNLEV6V627SWI7",
    "secret_key": "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",
    "bucket_name": "iconluxurygroup",
    "r2_endpoint": "https://97d91ece470eb7b9aa71ca0c781cfacc.r2.cloudflarestorage.com",
    "r2_access_key": "5547ff7ffb8f3b16a15d6f38322cd8bd",
    "r2_secret_key": "771014b01093eceb212dfea5eec0673842ca4a39456575ca7ff43f768cf42978",
    "r2_account_id": "97d91ece470eb7b9aa71ca0c781cfacc",
    "r2_bucket_name": "iconluxurygroup",
    "r2_custom_domain": "https://iconluxury.shop",
}
VERSION="3.8.6"
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
