import requests
import logging
from typing import Optional, Dict

# Setup logging
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def load_config_constants(config_url: str, fallback_db_password: Optional[str] = None) -> Optional[Dict[str, any]]:
    """
    Fetch and parse a JSON config file from a URL, returning all settings as a dictionary.
    
    Args:
        config_url (str): URL of the JSON configuration file
        fallback_db_password (Optional[str]): Fallback DB password if config fetch fails
        
    Returns:
        Optional[Dict[str, any]]: Dictionary of config values, or None if loading fails
    """
    try:
        # Fetch the JSON config file
        response = requests.get(config_url)
        response.raise_for_status()
        
        # Parse JSON
        config_data = response.json()
        
        # Build config dictionary
        config = {
            'VERSION': config_data.get('version'),
            'SENDER_EMAIL': config_data.get('email_settings', {}).get('sender_email'),
            'SENDER_PASSWORD': config_data.get('email_settings', {}).get('sender_password'),
            'SENDER_NAME': config_data.get('email_settings', {}).get('sender_name'),
            'GOOGLE_API_KEY': config_data.get('api_keys', {}).get('google_api_key'),
            'GROK_API_KEY': config_data.get('api_keys', {}).get('grok_api_key'),
            'GROK_ENDPOINT': config_data.get('api_keys', {}).get('grok_endpoint'),
            'SEARCH_PROXY_API_URL': config_data.get('proxy_settings', {}).get('search_proxy_api_url'),
            'BRAND_RULES_URL': config_data.get('brand_settings', {}).get('brand_rules_url'),
            'AWS_ACCESS_KEY_ID': config_data.get('aws_settings', {}).get('access_key_id'),
            'AWS_SECRET_ACCESS_KEY': config_data.get('aws_settings', {}).get('secret_access_key'),
            'REGION': config_data.get('aws_settings', {}).get('region'),
            'S3_CONFIG': config_data.get('s3_config', {}),
            'DB_PASSWORD': config_data.get('database_settings', {}).get('db_password', fallback_db_password),
            'BASE_CONFIG_URL': config_data.get('base_config', {}).get('base_config_url')
        }
        
        # Verify all critical constants are set
        required_keys = [
            'VERSION', 'SENDER_EMAIL', 'SENDER_PASSWORD', 'SENDER_NAME',
            'GOOGLE_API_KEY', 'GROK_API_KEY', 'GROK_ENDPOINT',
            'SEARCH_PROXY_API_URL', 'BRAND_RULES_URL',
            'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'REGION',
            'S3_CONFIG', 'DB_PASSWORD', 'BASE_CONFIG_URL'
        ]
        
        missing = [key for key in required_keys if config.get(key) is None]
        if missing:
            logger.error(f"Missing config values: {', '.join(missing)}")
            return None
            
        logger.info("Configuration loaded successfully")
        return config
        
    except requests.RequestException as e:
        logger.error(f"Error fetching config file: {e}")
        return None
    except ValueError as e:
        logger.error(f"Error parsing JSON: {e}")
        return None