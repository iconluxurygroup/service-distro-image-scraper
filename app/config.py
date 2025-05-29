import logging
from load_config import load_config_constants  # Adjust import based on your file structure

# Setup logging
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Load configuration
CONFIG_URL = "https://iconluxury.group/static_settings/config.json"
config = load_config_constants(CONFIG_URL, fallback_db_password="Ftu5675FDG54hjhiuu$")
if not config:
    logger.error("Failed to load configuration. Exiting.")
    raise SystemExit(1)

# Assign config values to variables
VERSION = config['VERSION']
SENDER_EMAIL = config['SENDER_EMAIL']
SENDER_PASSWORD = config['SENDER_PASSWORD']
SENDER_NAME = config['SENDER_NAME']
GOOGLE_API_KEY = config['GOOGLE_API_KEY']
GROK_API_KEY = config['GROK_API_KEY']
GROK_ENDPOINT = config['GROK_ENDPOINT']
SEARCH_PROXY_API_URL = config['SEARCH_PROXY_API_URL']
BRAND_RULES_URL = config['BRAND_RULES_URL']
AWS_ACCESS_KEY_ID = config['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = config['AWS_SECRET_ACCESS_KEY']
REGION = config['REGION']
S3_CONFIG = config['S3_CONFIG']
DB_PASSWORD = config['DB_PASSWORD']
BASE_CONFIG_URL = config['BASE_CONFIG_URL']

# Example: Log loaded config for debugging
logger.info(f"Loaded config: VERSION={VERSION}, BASE_CONFIG_URL={BASE_CONFIG_URL}")