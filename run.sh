#!/bin/bash

echo "Setting up credentials..."
python setup_credentials.py
python create_oauth.py

echo ""
echo "Running data fetch..."
python fetch_data.py
