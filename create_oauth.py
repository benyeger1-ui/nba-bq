import os
import json

# Get Yahoo credentials from environment
client_id = os.environ.get('YAHOO_CLIENT_ID')
client_secret = os.environ.get('YAHOO_CLIENT_SECRET')

if client_id and client_secret:
    oauth_config = {
        "consumer_key": client_id,
        "consumer_secret": client_secret
    }
    
    with open('oauth2.json', 'w') as f:
        json.dump(oauth_config, f, indent=2)
    
    print("✅ OAuth config file created!")
else:
    print("❌ Yahoo credentials not found in environment variables")
