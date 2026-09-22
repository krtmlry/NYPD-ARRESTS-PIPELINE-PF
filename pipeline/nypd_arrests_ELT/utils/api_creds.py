import os

def api_creds_gen():
    api_secrets = {
    'app_token': os.getenv('app_token'),#"X-App-Token" value for headers
    'api_key_id': os.getenv('api_key_id'),# username for basicauth
    'api_key_secret': os.getenv('api_key_secret'),# password for basicauth
    'url': os.getenv('url')
    }
    return api_secrets
