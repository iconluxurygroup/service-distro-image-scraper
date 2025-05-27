import os
#from dotenv import load_dotenv 
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
import logging
from typing import Optional, Dict
from sqlalchemy.sql import text
from urllib.parse import quote_plus
VERSION="4.0.8"
SENDER_EMAIL="nik@luxurymarket.com"
SENDER_PASSWORD="wvug kynd dfhd xrjh"
SENDER_NAME='superscraper'
GOOGLE_API_KEY='AIzaSyDXfc_kdxa5UX2h9D3WwktefCqdyjHasn8'
# AWS credentials and region
# config.py
SEARCH_PROXY_API_URL = "https://api.thedataproxy.com/v2/proxy/fetch"
BRAND_RULES_URL = "https://raw.githubusercontent.com/iconluxurygroup/legacy-icon-product-api/refs/heads/main/task_settings/brand_settings.json"
AWS_ACCESS_KEY_ID ='AKIA2CUNLEV6V627SWI7'
AWS_SECRET_ACCESS_KEY = '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'
REGION = "us-east-2"
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

DB_PASSWORD=  'Ftu5675FDG54hjhiuu$'

# Existing imports and conn_str
BASE_CONFIG_URL = "https://iconluxury.group/static_settings/"
# Grok API settings for image processing
GROK_API_KEY = os.getenv('GROK_API_KEY', 'xai-ucA8EcERzruUwHAa1duxYallTxycDumI5n3eVY7EJqhZVD0ywiiza3zEmRB4Tw7eNC5k0VuXVndYOUj9')
GROK_ENDPOINT = os.getenv('GROK_ENDPOINT', 'https://api.x.ai/v1/chat/completions')

