import os
from yahoo_oauth import OAuth2
from yahoo_fantasy_api import league, game
from google.cloud import bigquery
import pandas as pd
from datetime import datetime

# Set Google credentials from the file we'll create
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'google-key.json'

print("🔄 Step 1: Connecting to Yahoo...")
# Connect to Yahoo
sc = OAuth2(None, None, from_file='oauth2.json')
print("✅ Connected to Yahoo!")

print("\n🔄 Step 2: Connecting to BigQuery...")
# Get project ID from environment
project_id = os.environ.get('GCP_PROJECT_ID')
# Connect to BigQuery
client = bigquery.Client(project=project_id)
print("✅ Connected to BigQuery!")

print("\n🔄 Step 3: Getting your NBA fantasy league...")
# Get NBA game
gm = game.Game(sc, 'nba')

# YOUR LEAGUE ID GOES HERE
league_id = '75582'  
lg = league.League(sc, league_id)
print("✅ Found your league!")

print("\n🔄 Step 4: Fetching standings data...")
# Get standings
standings = lg.standings()
print(f"✅ Got data for {len(standings)} teams!")

print("\n🔄 Step 5: Preparing data...")
# Convert to DataFrame
df_standings = pd.DataFrame(standings)
df_standings['extracted_at'] = datetime.now()
print("✅ Data prepared!")

print("\n🔄 Step 6: Sending to BigQuery...")
# Create table name
dataset = os.environ.get('BQ_DATASET')
table_id = f"{project_id}.{dataset}.standings"

# Load to BigQuery
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
)

job = client.load_table_from_dataframe(
    df_standings, table_id, job_config=job_config
)
job.result()
print(f"✅ Successfully loaded {len(df_standings)} rows!")

print("\n🎉 ALL DONE! Your data is in BigQuery!")
print(f"📊 Table: {table_id}")
