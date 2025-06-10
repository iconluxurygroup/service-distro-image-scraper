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
        
        # Verify all critical constants are set
        required_keys = [
            'version',
            'email_settings.sender_email', 'email_settings.sender_password', 'email_settings.sender_name',
            'grok_settings.api_key', 'grok_settings.endpoint',
            'dataproxy_settings.api_key', 'dataproxy_settings.api_url',
            'google_api_settings.api_key',
            'brand_settings.brand_rules_url',
            'aws_settings.access_key_id', 'aws_settings.secret_access_key', 'aws_settings.region',
            's3_config',
            'database_settings.db_password',
            'base_config.base_config_url',
            'rabbitmq_settings.url', 'rabbitmq_settings.user', 'rabbitmq_settings.password',
            'rabbitmq_settings.host', 'rabbitmq_settings.port', 'rabbitmq_settings.vhost'
        ]
        
        missing = []
        for key in required_keys:
            # Handle nested keys
            value = config
            for part in key.split('.'):
                value = value.get(part)
                if value is None:
                    missing.append(key)
                    break
        
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