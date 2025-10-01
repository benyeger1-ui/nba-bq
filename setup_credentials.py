import os
import json
import base64

# Get the service account key from environment variable
sa_key = os.environ.get('GCP_SA_KEY')

if sa_key:
    # Decode and write to file
    with open('google-key.json', 'w') as f:
        # If it's base64 encoded, decode it first
        try:
            decoded = base64.b64decode(sa_key).decode('utf-8')
            f.write(decoded)
        except:
            # If not base64, write as is
            f.write(sa_key)
    print("✅ Google credentials file created!")
else:
    print("❌ GCP_SA_KEY not found in environment variables")
