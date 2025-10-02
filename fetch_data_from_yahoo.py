import os
from yahoo_oauth import OAuth2
from yahoo_fantasy_api import league, game
from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import time

# Set Google credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'google-key.json'

print("Step 1: Connecting to Yahoo...")
sc = OAuth2(None, None, from_file='oauth2.json')
print("Connected to Yahoo!")

print("\nStep 2: Connecting to BigQuery...")
project_id = os.environ.get('GCP_PROJECT_ID')
client = bigquery.Client(project=project_id)
print("Connected to BigQuery!")

print("\nStep 3: Getting your NBA fantasy league...")
gm = game.Game(sc, 'nba')

# YOUR LEAGUE ID
league_id = '454.l.21729'  # LAST YEAR
# league_id = '466.l.52855'  # CURRENT LEAGUE
lg = league.League(sc, league_id)
print("Found your league!")

# Get dataset name
dataset = os.environ.get('BQ_DATASET_NBA_YAHOO')
timestamp = datetime.now()

# Get league settings to find total weeks
settings = lg.settings()
start_week = int(settings.get('start_week', 1))
end_week = int(settings.get('end_week', 21))
current_week = lg.current_week()

print(f"\nLeague runs from week {start_week} to week {end_week}")
print(f"Current week: {current_week}")

# ==================== STANDINGS ====================
print("\n=== FETCHING STANDINGS ===")
try:
    standings = lg.standings()
    df_standings = pd.DataFrame(standings)
    
    # Convert rank to integer
    if 'rank' in df_standings.columns:
        df_standings['rank'] = pd.to_numeric(df_standings['rank'], errors='coerce').fillna(0).astype(int)
    
    # Extract outcome totals
    if 'outcome_totals' in df_standings.columns:
        outcome_data = df_standings['outcome_totals'].apply(
            lambda x: x if isinstance(x, dict) else {}
        )
        df_standings['wins'] = outcome_data.apply(lambda x: int(x.get('wins', 0)) if x.get('wins') else 0)
        df_standings['losses'] = outcome_data.apply(lambda x: int(x.get('losses', 0)) if x.get('losses') else 0)
        df_standings['ties'] = outcome_data.apply(lambda x: int(x.get('ties', 0)) if x.get('ties') else 0)
        df_standings['percentage'] = outcome_data.apply(lambda x: float(x.get('percentage', 0)) if x.get('percentage') else 0.0)
        df_standings = df_standings.drop('outcome_totals', axis=1)
    
    # Clean up other fields
    if 'games_back' in df_standings.columns:
        df_standings['games_back'] = df_standings['games_back'].replace('-', '0')
        df_standings['games_back'] = pd.to_numeric(df_standings['games_back'], errors='coerce').fillna(0).astype(float)
    
    if 'playoff_seed' in df_standings.columns:
        df_standings['playoff_seed'] = pd.to_numeric(df_standings['playoff_seed'], errors='coerce').fillna(0).astype(int)
    
    df_standings['extracted_at'] = timestamp
    df_standings['league_id'] = league_id
    
    table_id = f"{project_id}.{dataset}.standings"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
    job = client.load_table_from_dataframe(df_standings, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df_standings)} teams to standings table!")
except Exception as e:
    print(f"Error with standings: {e}")

# ==================== ALL MATCHUPS (HISTORICAL) ====================
print("\n=== FETCHING ALL MATCHUPS (HISTORICAL) ===")
all_matchup_records = []

for week in range(start_week, current_week + 1):
    print(f"Fetching week {week}...")
    try:
        matchups_data = lg.matchups(week=week)
        
        if isinstance(matchups_data, dict) and 'fantasy_content' in matchups_data:
            content = matchups_data['fantasy_content']
            if 'league' in content and isinstance(content['league'], list):
                for item in content['league']:
                    if isinstance(item, dict) and 'scoreboard' in item:
                        scoreboard = item['scoreboard']
                        if '0' in scoreboard and 'matchups' in scoreboard['0']:
                            matchups = scoreboard['0']['matchups']
                            
                            for key in matchups:
                                if key != 'count' and 'matchup' in matchups[key]:
                                    matchup = matchups[key]['matchup']
                                    teams_data = matchup.get('0', {}).get('teams', {})
                                    team1 = teams_data.get('0', {}).get('team', [[]])[0]
                                    team2 = teams_data.get('1', {}).get('team', [[]])[0]
                                    
                                    team1_key = next((item.get('team_key') for item in team1 if isinstance(item, dict) and 'team_key' in item), '')
                                    team1_name = next((item.get('name') for item in team1 if isinstance(item, dict) and 'name' in item), '')
                                    team2_key = next((item.get('team_key') for item in team2 if isinstance(item, dict) and 'team_key' in item), '')
                                    team2_name = next((item.get('name') for item in team2 if isinstance(item, dict) and 'name' in item), '')
                                    
                                    team1_points_data = next((item.get('team_points') for item in team1 if isinstance(item, dict) and 'team_points' in item), {})
                                    team2_points_data = next((item.get('team_points') for item in team2 if isinstance(item, dict) and 'team_points' in item), {})
                                    
                                    team1_points = team1_points_data.get('total', 0) if isinstance(team1_points_data, dict) else 0
                                    team2_points = team2_points_data.get('total', 0) if isinstance(team2_points_data, dict) else 0
                                    
                                    all_matchup_records.append({
                                        'week': matchup.get('week', week),
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
        
        time.sleep(0.5)  # Rate limiting
    except Exception as e:
        print(f"Error fetching week {week}: {e}")

if all_matchup_records:
    df_matchups = pd.DataFrame(all_matchup_records)
    print(f"\nTotal matchups collected: {len(df_matchups)}")
    
    table_id = f"{project_id}.{dataset}.matchups"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
    job = client.load_table_from_dataframe(df_matchups, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df_matchups)} matchups!")
else:
    print("No matchup data collected")

# ==================== TRANSACTIONS (ADD/DROP HISTORY) ====================
print("\n=== FETCHING TRANSACTION HISTORY ===")
try:
    transactions = lg.transactions('add,drop')
    transaction_records = []
    
    for trans in transactions:
        if isinstance(trans, dict):
            transaction_records.append({
                'transaction_id': trans.get('transaction_id', ''),
                'type': trans.get('type', ''),
                'status': trans.get('status', ''),
                'timestamp': trans.get('timestamp', ''),
                'player_key': trans.get('player_key', ''),
                'player_name': trans.get('player_name', ''),
                'team_key': trans.get('team_key', ''),
                'team_name': trans.get('team_name', ''),
                'extracted_at': timestamp,
                'league_id': league_id
            })
    
    if transaction_records:
        df_transactions = pd.DataFrame(transaction_records)
        print(f"Collected {len(df_transactions)} transactions")
        
        table_id = f"{project_id}.{dataset}.transactions"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
        job = client.load_table_from_dataframe(df_transactions, table_id, job_config=job_config)
        job.result()
        print(f"Loaded {len(df_transactions)} transactions!")
    else:
        print("No transactions found")
except Exception as e:
    print(f"Error with transactions: {e}")
    import traceback
    traceback.print_exc()

# ==================== CURRENT ROSTERS ====================
print("\n=== FETCHING CURRENT ROSTERS ===")
try:
    teams = lg.teams()
    player_records = []
    
    for team_key, team_data in teams.items():
        team_name = team_data.get('name', '') if isinstance(team_data, dict) else str(team_data)
        
        try:
            team_obj = lg.to_team(team_key)
            roster = team_obj.roster()
            
            for player in roster:
                if isinstance(player, dict):
                    player_records.append({
                        'team_key': team_key,
                        'team_name': team_name,
                        'player_id': player.get('player_id', ''),
                        'player_name': player.get('name', ''),
                        'position': player.get('position_type', ''),
                        'selected_position': player.get('selected_position', ''),
                        'status': player.get('status', ''),
                        'nba_team': player.get('editorial_team_abbr', ''),
                        'extracted_at': timestamp,
                        'league_id': league_id
                    })
        except Exception as e:
            print(f"Error getting roster for {team_key}: {e}")
    
    if player_records:
        df_players = pd.DataFrame(player_records)
        
        table_id = f"{project_id}.{dataset}.rosters"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
        job = client.load_table_from_dataframe(df_players, table_id, job_config=job_config)
        job.result()
        print(f"Loaded {len(df_players)} player records!")
except Exception as e:
    print(f"Error with rosters: {e}")

# ==================== ALL AVAILABLE PLAYERS (PLAYER POOL) ====================
print("\n=== FETCHING PLAYER POOL ===")
try:
    # Get free agents (available players)
    free_agents = lg.free_agents('PG')  # Position filter, try different ones
    
    all_players = []
    positions = ['PG', 'SG', 'SF', 'PF', 'C', 'G', 'F', 'UTIL']
    
    for pos in positions:
        print(f"Fetching {pos} players...")
        try:
            players = lg.free_agents(pos)
            for player in players:
                if isinstance(player, dict):
                    all_players.append({
                        'player_id': player.get('player_id', ''),
                        'player_name': player.get('name', ''),
                        'position': player.get('position_type', ''),
                        'status': player.get('status', ''),
                        'nba_team': player.get('editorial_team_abbr', ''),
                        'ownership_type': player.get('ownership', {}).get('ownership_type', 'available'),
                        'percent_owned': player.get('percent_owned', {}).get('value', 0),
                        'extracted_at': timestamp,
                        'league_id': league_id
                    })
            time.sleep(0.5)
        except Exception as e:
            print(f"Error fetching {pos}: {e}")
    
    if all_players:
        df_all_players = pd.DataFrame(all_players).drop_duplicates(subset=['player_id'])
        
        table_id = f"{project_id}.{dataset}.player_pool"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
        job = client.load_table_from_dataframe(df_all_players, table_id, job_config=job_config)
        job.result()
        print(f"Loaded {len(df_all_players)} players to pool!")
except Exception as e:
    print(f"Error with player pool: {e}")
    import traceback
    traceback.print_exc()

print("\n=== ALL DONE ===")
print(f"Tables updated:")
print(f"  - {project_id}.{dataset}.standings")
print(f"  - {project_id}.{dataset}.matchups (all weeks)")
print(f"  - {project_id}.{dataset}.transactions")
print(f"  - {project_id}.{dataset}.rosters")
print(f"  - {project_id}.{dataset}.player_pool")
