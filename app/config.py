import logging
from load_config import load_config_constants  # Adjust import based on your file structure

# Setup logging
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Load configuration
CONFIG_URL = "https://iconluxury.shop/secrets/secrets_scraper_config.json"
config = load_config_constants(CONFIG_URL, fallback_db_password="Ftu5675FDG54hjhiuu$")
if not config:
    logger.error("Failed to load configuration. Exiting.")
    raise SystemExit(1)

# Assign config values to variables
VERSION = config['version']
SENDER_EMAIL = config['email_settings']['sender_email']
SENDER_PASSWORD = config['email_settings']['sender_password']
SENDER_NAME = config['email_settings']['sender_name']
GOOGLE_API_KEY = config['google_api_settings']['api_key']
GROK_API_KEY = config['grok_settings']['api_key']
GROK_ENDPOINT = config['grok_settings']['endpoint']
DATAPROXY_API_KEY = config['dataproxy_settings']['api_key']
SEARCH_PROXY_API_URL = config['dataproxy_settings']['api_url']
ROAMINGPROXY_API_KEY = config['roamingproxy_settings']['api_key']
ROAMINGPROXY_API_URL = config['roamingproxy_settings']['api_url']
PROXY_STRATEGY = config.get('proxy_strategy', 'round_robin')
BRAND_RULES_URL = config['brand_settings']['brand_rules_url']
AWS_ACCESS_KEY_ID = config['aws_settings']['access_key_id']
AWS_SECRET_ACCESS_KEY = config['aws_settings']['secret_access_key']
REGION = config['aws_settings']['region']
S3_CONFIG = config['s3_config']
DB_PASSWORD = config['database_settings']['db_password']
BASE_CONFIG_URL = config['base_config']['base_config_url']

# RabbitMQ configuration
RABBITMQ_URL = config['rabbitmq_settings']['url']
RABBITMQ_USER = config['rabbitmq_settings']['user']
RABBITMQ_PASSWORD = config['rabbitmq_settings']['password']
RABBITMQ_HOST = config['rabbitmq_settings']['host']
RABBITMQ_PORT = config['rabbitmq_settings']['port']
RABBITMQ_VHOST = config['rabbitmq_settings']['vhost']

# Example: Log loaded config for debugging
logger.info(f"Loaded config: VERSION={VERSION}, BASE_CONFIG_URL={BASE_CONFIG_URL}, RABBITMQ_URL={RABBITMQ_URL}")