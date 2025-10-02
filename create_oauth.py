import os
import json

# Get Yahoo credentials from environment
client_id = os.environ.get('YAHOO_CLIENT_ID')
client_secret = os.environ.get('YAHOO_CLIENT_SECRET')
oauth_token = os.environ.get('YAHOO_OAUTH_TOKEN')

if oauth_token:
    # Use the full token if provided (for automated runs)
    try:
        token_data = json.loads(oauth_token)
        with open('oauth2.json', 'w') as f:
            json.dump(token_data, f, indent=2)
        print("✅ OAuth token file created from secret!")
    except json.JSONDecodeError:
        print("❌ Failed to parse YAHOO_OAUTH_TOKEN")
elif client_id and client_secret:
    # Create basic config for interactive auth
    oauth_config = {
        "consumer_key": client_id,
        "consumer_secret": client_secret
    }
    
    with open('oauth2.json', 'w') as f:
        json.dump(oauth_config, f, indent=2)
    
    print("✅ OAuth config file created (interactive auth required)")
else:
    print("❌ Yahoo credentials not found in environment variables")
