import requests
import logging
from typing import Optional, Dict, Any
from requests.exceptions import RequestException

# Setup logging
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

def load_config_constants(config_url: str, fallback_db_password: Optional[str] = None, timeout: int = 10, retries: int = 3) -> Optional[Dict[str, Any]]:
    """
    Fetch and parse a JSON config file from a URL, returning all settings as a dictionary.
    
    Args:
        config_url (str): URL of the JSON configuration file.
        fallback_db_password (Optional[str]): Fallback DB password if config fetch fails.
        timeout (int): Request timeout in seconds (default: 10).
        retries (int): Number of retry attempts for HTTP requests (default: 3).
        
    Returns:
        Optional[Dict[str, Any]]: Dictionary of config values, or None if loading fails.
    """
    def fetch_config(url: str) -> Optional[Dict[str, Any]]:
        for attempt in range(1, retries + 1):
            try:
                response = requests.get(url, timeout=timeout)
                response.raise_for_status()
                return response.json()
            except RequestException as e:
                logger.warning(f"Attempt {attempt}/{retries} failed: {e}")
                if attempt == retries:
                    logger.error(f"Failed to fetch config after {retries} attempts: {e}")
                    return None
            except ValueError as e:
                logger.error(f"Error parsing JSON: {e}")
                return None
        return None

    def get_nested_value(data: Dict[str, Any], key_path: str) -> Any:
        """Safely retrieve a nested value from a dictionary."""
        value = data
        for part in key_path.split('.'):
            if not isinstance(value, dict):
                return None
            value = value.get(part)
            if value is None:
                return None
        return value

    # Fetch JSON config
    config_data = fetch_config(config_url)
    if not config_data:
        return None

    # Build config dictionary
    config = {
        'version': config_data.get('version'),
        'email_settings': {
            'sender_email': config_data.get('email_settings', {}).get('sender_email'),
            'sender_password': config_data.get('email_settings', {}).get('sender_password'),
            'sender_name': config_data.get('email_settings', {}).get('sender_name')
        },
        'grok_settings': {
            'api_key': config_data.get('grok_settings', {}).get('api_key'),
            'endpoint': config_data.get('grok_settings', {}).get('endpoint')
        },
        'dataproxy_settings': {
            'api_key': config_data.get('dataproxy_settings', {}).get('api_key'),
            'api_url': config_data.get('dataproxy_settings', {}).get('api_url')
        },
        'roamingproxy_settings': {
            'api_key': config_data.get('roamingproxy_settings', {}).get('api_key'),
            'api_url': config_data.get('roamingproxy_settings', {}).get('api_url')
        },
        'proxy_strategy': config_data.get('proxy_strategy', 'round_robin'),
        'google_api_settings': {
            'api_key': config_data.get('google_api_settings', {}).get('api_key')
        },
        'brand_settings': {
            'brand_rules_url': config_data.get('brand_settings', {}).get('brand_rules_url')
        },
        'aws_settings': {
            'access_key_id': config_data.get('aws_settings', {}).get('access_key_id'),
            'secret_access_key': config_data.get('aws_settings', {}).get('secret_access_key'),
            'region': config_data.get('aws_settings', {}).get('region')
        },
        's3_config': config_data.get('s3_config', {}),
        'database_settings': {
            'db_password': config_data.get('database_settings', {}).get('db_password', fallback_db_password)
        },
        'base_config': {
            'base_config_url': config_data.get('base_config', {}).get('base_config_url')
        },
        'rabbitmq_settings': {
            'url': config_data.get('rabbitmq_settings', {}).get('url'),
            'user': config_data.get('rabbitmq_settings', {}).get('user'),
            'password': config_data.get('rabbitmq_settings', {}).get('password'),
            'host': config_data.get('rabbitmq_settings', {}).get('host'),
            'port': config_data.get('rabbitmq_settings', {}).get('port'),
            'vhost': config_data.get('rabbitmq_settings', {}).get('vhost')
        }
    }

    # Define required keys with expected types
    required_keys = {
        'version': str,
        'email_settings.sender_email': str,
        'email_settings.sender_password': str,
        'email_settings.sender_name': str,
        'grok_settings.api_key': str,
        'grok_settings.endpoint': str,
        'dataproxy_settings.api_key': str,
        'dataproxy_settings.api_url': str,
        'google_api_settings.api_key': str,
        'brand_settings.brand_rules_url': str,
        'aws_settings.access_key_id': str,
        'aws_settings.secret_access_key': str,
        'aws_settings.region': str,
        's3_config': dict,
        'database_settings.db_password': str,
        'base_config.base_config_url': str,
        'rabbitmq_settings.url': str,
        'rabbitmq_settings.user': str,
        'rabbitmq_settings.password': str,
        'rabbitmq_settings.host': str,
        'rabbitmq_settings.port': (int, str),  # Allow string for conversion
        'rabbitmq_settings.vhost': str
    }

    # Validate required keys and types
    missing = []
    for key, expected_type in required_keys.items():
        value = get_nested_value(config, key)
        if value is None:
            missing.append(key)
        elif not isinstance(value, expected_type):
            logger.error(f"Invalid type for {key}: expected {expected_type}, got {type(value)}")
            missing.append(key)

    if missing:
        logger.error(f"Missing or invalid config values: {', '.join(missing)}")
        return None

    # Convert port to int if it's a string
    port = config['rabbitmq_settings']['port']
    if isinstance(port, str) and port.isdigit():
        config['rabbitmq_settings']['port'] = int(port)

    logger.info("Configuration loaded successfully")
    return config