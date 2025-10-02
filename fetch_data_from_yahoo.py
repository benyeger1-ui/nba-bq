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
                                    
                                    # Extract basic team info
                                    team1_key = next((item.get('team_key') for item in team1 if isinstance(item, dict) and 'team_key' in item), '')
                                    team1_name = next((item.get('name') for item in team1 if isinstance(item, dict) and 'name' in item), '')
                                    team2_key = next((item.get('team_key') for item in team2 if isinstance(item, dict) and 'team_key' in item), '')
                                    team2_name = next((item.get('name') for item in team2 if isinstance(item, dict) and 'name' in item), '')
                                    
                                    # Extract category points
                                    team1_points_data = next((item.get('team_points') for item in team1 if isinstance(item, dict) and 'team_points' in item), {})
                                    team2_points_data = next((item.get('team_points') for item in team2 if isinstance(item, dict) and 'team_points' in item), {})
                                    
                                    team1_points = team1_points_data.get('total', 0) if isinstance(team1_points_data, dict) else 0
                                    team2_points = team2_points_data.get('total', 0) if isinstance(team2_points_data, dict) else 0
                                    
                                    # Extract category stats
                                    team1_stats = next((item.get('team_stats') for item in team1 if isinstance(item, dict) and 'team_stats' in item), {})
                                    team2_stats = next((item.get('team_stats') for item in team2 if isinstance(item, dict) and 'team_stats' in item), {})
                                    
                                    # Parse stats into dictionary
                                    def parse_stats(stats_data):
                                        stats_dict = {}
                                        if isinstance(stats_data, dict) and 'stats' in stats_data:
                                            stats_list = stats_data['stats']
                                            if isinstance(stats_list, list):
                                                for stat in stats_list:
                                                    if isinstance(stat, dict) and 'stat' in stat:
                                                        stat_info = stat['stat']
                                                        stat_id = stat_info.get('stat_id', '')
                                                        stat_value = stat_info.get('value', '')
                                                        
                                                        # Map stat IDs to names (9-cat standard)
                                                        stat_names = {
                                                            '5': 'FG_PCT',
                                                            '8': 'FT_PCT',
                                                            '10': 'THREE_PM',
                                                            '12': 'PTS',
                                                            '15': 'REB',
                                                            '16': 'AST',
                                                            '17': 'STL',
                                                            '18': 'BLK',
                                                            '19': 'TO'
                                                        }
                                                        
                                                        if stat_id in stat_names:
                                                            stats_dict[stat_names[stat_id]] = stat_value
                                        return stats_dict
                                    
                                    team1_stats_dict = parse_stats(team1_stats)
                                    team2_stats_dict = parse_stats(team2_stats)
                                    
                                    # Extract stat winners for each category
                                    stat_winners = matchup.get('stat_winners', [])
                                    category_winners = {}
                                    
                                    if isinstance(stat_winners, list):
                                        for winner_item in stat_winners:
                                            if isinstance(winner_item, dict) and 'stat_winner' in winner_item:
                                                stat_winner = winner_item['stat_winner']
                                                stat_id = stat_winner.get('stat_id', '')
                                                winner_key = stat_winner.get('winner_team_key', '')
                                                
                                                stat_names = {
                                                    '5': 'FG_PCT',
                                                    '8': 'FT_PCT',
                                                    '10': 'THREE_PM',
                                                    '12': 'PTS',
                                                    '15': 'REB',
                                                    '16': 'AST',
                                                    '17': 'STL',
                                                    '18': 'BLK',
                                                    '19': 'TO'
                                                }
                                                
                                                if stat_id in stat_names:
                                                    category_winners[stat_names[stat_id]] = winner_key
                                    
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
                                        'team1_fg_pct': team1_stats_dict.get('FG_PCT', ''),
                                        'team1_ft_pct': team1_stats_dict.get('FT_PCT', ''),
                                        'team1_3pm': team1_stats_dict.get('THREE_PM', ''),
                                        'team1_pts': team1_stats_dict.get('PTS', ''),
                                        'team1_reb': team1_stats_dict.get('REB', ''),
                                        'team1_ast': team1_stats_dict.get('AST', ''),
                                        'team1_stl': team1_stats_dict.get('STL', ''),
                                        'team1_blk': team1_stats_dict.get('BLK', ''),
                                        'team1_to': team1_stats_dict.get('TO', ''),
                                        'team2_key': team2_key,
                                        'team2_name': team2_name,
                                        'team2_points': float(team2_points) if team2_points else 0.0,
                                        'team2_fg_pct': team2_stats_dict.get('FG_PCT', ''),
                                        'team2_ft_pct': team2_stats_dict.get('FT_PCT', ''),
                                        'team2_3pm': team2_stats_dict.get('THREE_PM', ''),
                                        'team2_pts': team2_stats_dict.get('PTS', ''),
                                        'team2_reb': team2_stats_dict.get('REB', ''),
                                        'team2_ast': team2_stats_dict.get('AST', ''),
                                        'team2_stl': team2_stats_dict.get('STL', ''),
                                        'team2_blk': team2_stats_dict.get('BLK', ''),
                                        'team2_to': team2_stats_dict.get('TO', ''),
                                        'winner_fg_pct': category_winners.get('FG_PCT', ''),
                                        'winner_ft_pct': category_winners.get('FT_PCT', ''),
                                        'winner_3pm': category_winners.get('THREE_PM', ''),
                                        'winner_pts': category_winners.get('PTS', ''),
                                        'winner_reb': category_winners.get('REB', ''),
                                        'winner_ast': category_winners.get('AST', ''),
                                        'winner_stl': category_winners.get('STL', ''),
                                        'winner_blk': category_winners.get('BLK', ''),
                                        'winner_to': category_winners.get('TO', ''),
                                        'extracted_at': timestamp,
                                        'league_id': league_id
                                    })
        
        time.sleep(0.5)  # Rate limiting
    except Exception as e:
        print(f"Error fetching week {week}: {e}")

if all_matchup_records:
    df_matchups = pd.DataFrame(all_matchup_records)
    print(f"\nTotal matchups collected: {len(df_matchups)}")
    print(f"Preview:\n{df_matchups[['week', 'team1_name', 'team1_points', 'team2_name', 'team2_points']].head()}")
    
    table_id = f"{project_id}.{dataset}.matchups"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
    job = client.load_table_from_dataframe(df_matchups, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df_matchups)} matchups with category stats!")
else:
    print("No matchup data collected")
    
# ==================== TRANSACTIONS (ADD/DROP HISTORY) ====================
print("\n=== FETCHING TRANSACTION HISTORY ===")
try:
    # Get transactions with count parameter (max 100 per call)
    # You may need to paginate for complete history
    transactions = lg.transactions('add,drop', 100)
    transaction_records = []
    
    if isinstance(transactions, list):
        for trans in transactions:
            if isinstance(trans, dict):
                # Handle different transaction structures
                transaction_key = trans.get('transaction_key', '')
                trans_type = trans.get('type', '')
                status = trans.get('status', '')
                timestamp_val = trans.get('timestamp', '')
                
                # Get players involved
                players = trans.get('players', [])
                if isinstance(players, list):
                    for player_info in players:
                        if isinstance(player_info, dict):
                            player = player_info.get('player', [[]])[0] if 'player' in player_info else {}
                            transaction_data = player_info.get('transaction_data', [{}])[0] if 'transaction_data' in player_info else {}
                            
                            # Extract player details
                            player_key = ''
                            player_name = ''
                            if isinstance(player, list):
                                for item in player:
                                    if isinstance(item, dict):
                                        if 'player_key' in item:
                                            player_key = item['player_key']
                                        if 'name' in item:
                                            player_name = item['name']['full']
                            
                            # Extract transaction details
                            dest_team = transaction_data.get('destination_team_key', '')
                            source_team = transaction_data.get('source_team_key', '')
                            trans_type_detail = transaction_data.get('type', trans_type)
                            
                            transaction_records.append({
                                'transaction_key': transaction_key,
                                'type': trans_type_detail,
                                'status': status,
                                'timestamp': timestamp_val,
                                'player_key': player_key,
                                'player_name': player_name,
                                'destination_team_key': dest_team,
                                'source_team_key': source_team,
                                'extracted_at': timestamp,
                                'league_id': league_id
                            })
    
    if transaction_records:
        df_transactions = pd.DataFrame(transaction_records)
        print(f"Collected {len(df_transactions)} transaction records")
        
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
    all_players = []
    positions = ['PG', 'SG', 'SF', 'PF', 'C']
    
    for pos in positions:
        print(f"Fetching {pos} players...")
        try:
            # Get free agents by position (limit to 50 per position)
            players = lg.free_agents(pos)
            
            if isinstance(players, list):
                for player in players:
                    # Handle if player is a dict directly
                    if isinstance(player, dict):
                        player_data = player
                    # Handle if player is wrapped in a list structure
                    elif isinstance(player, list):
                        player_data = {}
                        for item in player:
                            if isinstance(item, dict):
                                player_data.update(item)
                    else:
                        continue
                    
                    # Extract player information
                    player_id = player_data.get('player_id', '')
                    player_name = ''
                    if 'name' in player_data:
                        name_field = player_data['name']
                        if isinstance(name_field, dict):
                            player_name = name_field.get('full', '')
                        else:
                            player_name = str(name_field)
                    
                    # Get ownership info
                    ownership = player_data.get('ownership', {})
                    ownership_type = 'available'
                    if isinstance(ownership, dict):
                        ownership_type = ownership.get('ownership_type', 'available')
                    
                    # Get percent owned
                    percent_owned = 0
                    if 'percent_owned' in player_data:
                        po = player_data['percent_owned']
                        if isinstance(po, dict):
                            percent_owned = float(po.get('value', 0))
                        else:
                            percent_owned = float(po) if po else 0
                    
                    all_players.append({
                        'player_id': player_id,
                        'player_name': player_name,
                        'position': player_data.get('position_type', pos),
                        'status': player_data.get('status', ''),
                        'nba_team': player_data.get('editorial_team_abbr', ''),
                        'ownership_type': ownership_type,
                        'percent_owned': percent_owned,
                        'extracted_at': timestamp,
                        'league_id': league_id
                    })
            
            time.sleep(0.5)  # Rate limiting
        except Exception as e:
            print(f"Error fetching {pos}: {e}")
            import traceback
            traceback.print_exc()
    
    if all_players:
        df_all_players = pd.DataFrame(all_players).drop_duplicates(subset=['player_id'])
        print(f"Collected {len(df_all_players)} unique players")
        
        table_id = f"{project_id}.{dataset}.player_pool"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
        job = client.load_table_from_dataframe(df_all_players, table_id, job_config=job_config)
        job.result()
        print(f"Loaded {len(df_all_players)} players to pool!")
    else:
        print("No players collected for pool")
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
