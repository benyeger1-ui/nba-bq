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
league_id = '454.l.21729'  # LAST YEAR
# league_id = '466.l.52855'  # CURRENT LEAGUE
lg = league.League(sc, league_id)
print("✅ Found your league!")

# Get dataset name
dataset = os.environ.get('BQ_DATASET_NBA_YAHOO', 'nba_yahoo')
timestamp = datetime.now()

# ==================== STANDINGS ====================
print("\n🔄 Step 4: Fetching standings data...")
try:
    standings = lg.standings()
    df_standings = pd.DataFrame(standings)
    
    # Convert rank to integer (use regular int, not Int64)
    df_standings['rank'] = pd.to_numeric(df_standings['rank'], errors='coerce').fillna(0).astype(int)
    
    # Extract outcome totals into separate columns
    if 'outcome_totals' in df_standings.columns:
        # Parse the outcome_totals dictionary
        outcome_data = df_standings['outcome_totals'].apply(
            lambda x: x if isinstance(x, dict) else {}
        )
        df_standings['wins'] = outcome_data.apply(lambda x: int(x.get('wins', 0)) if x.get('wins') else 0)
        df_standings['losses'] = outcome_data.apply(lambda x: int(x.get('losses', 0)) if x.get('losses') else 0)
        df_standings['ties'] = outcome_data.apply(lambda x: int(x.get('ties', 0)) if x.get('ties') else 0)
        df_standings['percentage'] = outcome_data.apply(lambda x: float(x.get('percentage', 0)) if x.get('percentage') else 0.0)
        
        # Drop the original outcome_totals column
        df_standings = df_standings.drop('outcome_totals', axis=1)
    
    # Convert games_back to float (handle '-' as 0)
    if 'games_back' in df_standings.columns:
        df_standings['games_back'] = df_standings['games_back'].replace('-', '0')
        df_standings['games_back'] = pd.to_numeric(df_standings['games_back'], errors='coerce').fillna(0).astype(float)
    
    # Convert playoff_seed to integer (use regular int, not Int64)
    if 'playoff_seed' in df_standings.columns:
        df_standings['playoff_seed'] = pd.to_numeric(df_standings['playoff_seed'], errors='coerce').fillna(0).astype(int)
    
    df_standings['extracted_at'] = timestamp
    df_standings['league_id'] = league_id
    
    # Convert all column types to basic types for BigQuery compatibility
    for col in df_standings.columns:
        if df_standings[col].dtype == 'object':
            df_standings[col] = df_standings[col].astype(str)
    
    print(f"   Preview: {df_standings.head()}")
    print(f"   Columns: {df_standings.columns.tolist()}")
    print(f"   Data types: {df_standings.dtypes.to_dict()}")
    
    table_id = f"{project_id}.{dataset}.standings"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True
    )
    job = client.load_table_from_dataframe(df_standings, table_id, job_config=job_config)
    job.result()
    print(f"✅ Loaded {len(df_standings)} teams to standings table!")
except Exception as e:
    print(f"❌ Error with standings: {e}")
    import traceback
    traceback.print_exc()
# ==================== MATCHUPS ====================
print("\n🔄 Step 5: Fetching matchups data...")
try:
    current_week = lg.current_week()
    matchups_data = lg.matchups(week=current_week)
    
    print(f"   Current week: {current_week}")
    
    # Parse the complex nested structure
    matchup_records = []
    
    if isinstance(matchups_data, dict) and 'fantasy_content' in matchups_data:
        # Navigate the nested structure
        content = matchups_data['fantasy_content']
        if 'league' in content and isinstance(content['league'], list):
            for item in content['league']:
                if isinstance(item, dict) and 'scoreboard' in item:
                    scoreboard = item['scoreboard']
                    if '0' in scoreboard and 'matchups' in scoreboard['0']:
                        matchups = scoreboard['0']['matchups']
                        
                        # Iterate through matchups
                        for key in matchups:
                            if key != 'count' and 'matchup' in matchups[key]:
                                matchup = matchups[key]['matchup']
                                
                                # Get team data
                                teams_data = matchup.get('0', {}).get('teams', {})
                                team1 = teams_data.get('0', {}).get('team', [[]])[0]
                                team2 = teams_data.get('1', {}).get('team', [[]])[0]
                                
                                # Extract team info
                                team1_key = next((item.get('team_key') for item in team1 if isinstance(item, dict) and 'team_key' in item), '')
                                team1_name = next((item.get('name') for item in team1 if isinstance(item, dict) and 'name' in item), '')
                                team2_key = next((item.get('team_key') for item in team2 if isinstance(item, dict) and 'team_key' in item), '')
                                team2_name = next((item.get('name') for item in team2 if isinstance(item, dict) and 'name' in item), '')
                                
                                # Extract points
                                team1_points_data = next((item.get('team_points') for item in team1 if isinstance(item, dict) and 'team_points' in item), {})
                                team2_points_data = next((item.get('team_points') for item in team2 if isinstance(item, dict) and 'team_points' in item), {})
                                
                                team1_points = team1_points_data.get('total', 0) if isinstance(team1_points_data, dict) else 0
                                team2_points = team2_points_data.get('total', 0) if isinstance(team2_points_data, dict) else 0
                                
                                matchup_records.append({
                                    'week': matchup.get('week', current_week),
                                    'week_start': matchup.get('week_start', ''),
                                    'week_end': matchup.get('week_end', ''),
                                    'status': matchup.get('status', ''),
                                    'is_playoffs': matchup.get('is_playoffs', '0'),
                                    'is_tied': matchup.get('is_tied', 0),
                                    'winner_team_key': matchup.get('winner_team_key', ''),
                                    'team1_key': team1_key,
                                    'team1_name': team1_name,
                                    'team1_points': float(team1_points) if team1_points else 0.0,
                                    'team2_key': team2_key,
                                    'team2_name': team2_name,
                                    'team2_points': float(team2_points) if team2_points else 0.0,
                                    'extracted_at': timestamp,
                                    'league_id': league_id
                                })
    
    if matchup_records:
        df_matchups = pd.DataFrame(matchup_records)
        print(f"   Preview: {df_matchups.head()}")
        
        table_id = f"{project_id}.{dataset}.matchups"
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            autodetect=True
        )
        job = client.load_table_from_dataframe(df_matchups, table_id, job_config=job_config)
        job.result()
        print(f"✅ Loaded {len(df_matchups)} matchups to matchups table!")
    else:
        print("⚠️ No matchup data available for current week")
        
except Exception as e:
    print(f"❌ Error with matchups: {e}")
    import traceback
    traceback.print_exc()
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
