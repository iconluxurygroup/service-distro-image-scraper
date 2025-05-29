import requests
from typing import Optional
import json

def load_config_constants(config_url: str) -> bool:
    """
    Fetch and parse a JSON config file from a URL, loading all settings as constant variables.
    
    Args:
        config_url (str): URL of the JSON configuration file
        
    Returns:
        bool: True if config is loaded successfully, False otherwise
    """
    try:
        # Fetch the JSON config file
        response = requests.get(config_url)
        response.raise_for_status()
        
        # Parse JSON
        config_data = response.json()
        
        # Define constants in global namespace
        globals()['VERSION'] = config_data.get('version')
        
        # Email settings
        email_settings = config_data.get('email_settings', {})
        globals()['SENDER_EMAIL'] = email_settings.get('sender_email')
        globals()['SENDER_PASSWORD'] = email_settings.get('sender_password')
        globals()['SENDER_NAME'] = email_settings.get('sender_name')
        
        # API keys
        api_keys = config_data.get('api_keys', {})
        globals()['GOOGLE_API_KEY'] = api_keys.get('google_api_key')
        globals()['GROK_API_KEY'] = api_keys.get('grok_api_key')
        globals()['GROK_ENDPOINT'] = api_keys.get('grok_endpoint')
        
        # Proxy settings
        proxy_settings = config_data.get('proxy_settings', {})
        globals()['SEARCH_PROXY_API_URL'] = proxy_settings.get('search_proxy_api_url')
        
        # Brand settings
        brand_settings = config_data.get('brand_settings', {})
        globals()['BRAND_RULES_URL'] = brand_settings.get('brand_rules_url')
        
        # AWS settings
        aws_settings = config_data.get('aws_settings', {})
        globals()['AWS_ACCESS_KEY_ID'] = aws_settings.get('access_key_id')
        globals()['AWS_SECRET_ACCESS_KEY'] = aws_settings.get('secret_access_key')
        globals()['REGION'] = aws_settings.get('region')
        
        # S3 config (keep as dictionary for nested structure)
        globals()['S3_CONFIG'] = config_data.get('s3_config', {})
        
        # Database settings
        db_settings = config_data.get('database_settings', {})
        globals()['DB_PASSWORD'] = db_settings.get('db_password')
        
        # Base config
        base_config = config_data.get('base_config', {})
        globals()['BASE_CONFIG_URL'] = base_config.get('base_config_url')
        
        # Verify all critical top-level constants are set
        required_constants = [
            'VERSION', 'SENDER_EMAIL', 'SENDER_PASSWORD', 'SENDER_NAME',
            'GOOGLE_API_KEY', 'GROK_API_KEY', 'GROK_ENDPOINT',
            'SEARCH_PROXY_API_URL', 'BRAND_RULES_URL',
            'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'REGION',
            'S3_CONFIG', 'DB_PASSWORD', 'BASE_CONFIG_URL'
        ]
        
        missing = [const for const in required_constants if globals().get(const) is None]
        if missing:
            print(f"Error: Missing config values: {', '.join(missing)}")
            return False
            
        return True
        
    except requests.RequestException as e:
        print(f"Error fetching config file: {e}")
        return False
    except ValueError as e:
        print(f"Error parsing JSON: {e}")
        return False

# Example usage
if __name__ == "__main__":
    config_url = "https://iconluxury.group/static_settings/config.json"
    load_config_constants(config_url)
