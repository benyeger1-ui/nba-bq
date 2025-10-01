import os
from yahoo_oauth import OAuth2
from yahoo_fantasy_api import league, game
from google.cloud import bigquery
import pandas as pd
from datetime import datetime

# Set Google credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'google-key.json'

print("🔄 Step 1: Connecting to Yahoo...")
sc = OAuth2(None, None, from_file='oauth2.json')
print("✅ Connected to Yahoo!")

print("\n🔄 Step 2: Connecting to BigQuery...")
project_id = os.environ.get('GCP_PROJECT_ID')
client = bigquery.Client(project=project_id)
print("✅ Connected to BigQuery!")

print("\n🔄 Step 3: Getting your NBA fantasy league...")
gm = game.Game(sc, 'nba')

# YOUR LEAGUE ID - update this with your league ID from find_league.py
league_id = '466.l.52855'  # Example: '449.l.75582'
lg = league.League(sc, league_id)
print("✅ Found your league!")

# Get dataset name
dataset = os.environ.get('BQ_DATASET')
timestamp = datetime.now()

# ==================== STANDINGS ====================
print("\n🔄 Step 4: Fetching standings data...")
try:
    standings = lg.standings()
    df_standings = pd.DataFrame(standings)
    df_standings['extracted_at'] = timestamp
    df_standings['league_id'] = league_id
    
    table_id = f"{project_id}.{dataset}.standings"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(df_standings, table_id, job_config=job_config)
    job.result()
    print(f"✅ Loaded {len(df_standings)} teams to standings table!")
except Exception as e:
    print(f"❌ Error with standings: {e}")

# ==================== MATCHUPS ====================
print("\n🔄 Step 5: Fetching matchups data...")
try:
    # Get current week matchups
    matchups = lg.matchups()
    
    # Flatten matchups data
    matchup_records = []
    for week, week_matchups in matchups.items():
        for matchup in week_matchups:
            matchup_records.append({
                'week': week,
                'team1': matchup.get('team1', ''),
                'team2': matchup.get('team2', ''),
                'team1_points': matchup.get('team1_points', 0),
                'team2_points': matchup.get('team2_points', 0),
                'winner': matchup.get('winner', ''),
                'extracted_at': timestamp,
                'league_id': league_id
            })
    
    df_matchups = pd.DataFrame(matchup_records)
    
    if not df_matchups.empty:
        table_id = f"{project_id}.{dataset}.matchups"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(df_matchups, table_id, job_config=job_config)
        job.result()
        print(f"✅ Loaded {len(df_matchups)} matchups to matchups table!")
    else:
        print("⚠️ No matchup data available")
except Exception as e:
    print(f"❌ Error with matchups: {e}")

# ==================== ROSTERS/PLAYERS ====================
print("\n🔄 Step 6: Fetching player rosters...")
try:
    # Get all team rosters
    teams = lg.teams()
    
    player_records = []
    for team_key, team_info in teams.items():
        team_name = team_info.get('name', '')
        roster = lg.to_team(team_key).roster()
        
        for player in roster:
            player_records.append({
                'team_key': team_key,
                'team_name': team_name,
                'player_id': player.get('player_id', ''),
                'player_name': player.get('name', ''),
                'position': player.get('position_type', ''),
                'status': player.get('status', ''),
                'nba_team': player.get('editorial_team_abbr', ''),
                'extracted_at': timestamp,
                'league_id': league_id
            })
    
    df_players = pd.DataFrame(player_records)
    
    if not df_players.empty:
        table_id = f"{project_id}.{dataset}.players"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(df_players, table_id, job_config=job_config)
        job.result()
        print(f"✅ Loaded {len(df_players)} player records to players table!")
    else:
        print("⚠️ No player data available")
except Exception as e:
    print(f"❌ Error with players: {e}")

print("\n🎉 ALL DONE! Your data is in BigQuery!")
print(f"📊 Tables created/updated:")
print(f"   - {project_id}.{dataset}.standings")
print(f"   - {project_id}.{dataset}.matchups")
print(f"   - {project_id}.{dataset}.players")
