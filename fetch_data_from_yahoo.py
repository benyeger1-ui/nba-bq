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

# Get league settings
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
    
    if 'rank' in df_standings.columns:
        df_standings['rank'] = pd.to_numeric(df_standings['rank'], errors='coerce').fillna(0).astype(int)
    
    if 'outcome_totals' in df_standings.columns:
        outcome_data = df_standings['outcome_totals'].apply(lambda x: x if isinstance(x, dict) else {})
        df_standings['wins'] = outcome_data.apply(lambda x: int(x.get('wins', 0)) if x.get('wins') else 0)
        df_standings['losses'] = outcome_data.apply(lambda x: int(x.get('losses', 0)) if x.get('losses') else 0)
        df_standings['ties'] = outcome_data.apply(lambda x: int(x.get('ties', 0)) if x.get('ties') else 0)
        df_standings['percentage'] = outcome_data.apply(lambda x: float(x.get('percentage', 0)) if x.get('percentage') else 0.0)
        df_standings = df_standings.drop('outcome_totals', axis=1)
    
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

# ==================== ALL MATCHUPS WITH DETAILED STATS ====================
print("\n=== FETCHING ALL MATCHUPS (HISTORICAL) ===")
all_matchup_records = []

for week in range(start_week, min(current_week + 1, end_week + 1)):
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
                                    team1_list = teams_data.get('0', {}).get('team', [[]])[0]
                                    team2_list = teams_data.get('1', {}).get('team', [[]])[0]
                                    
                                    # Parse team 1
                                    team1_key = ''
                                    team1_name = ''
                                    team1_points = 0
                                    team1_stats_dict = {}
                                    
                                    for item in team1_list:
                                        if isinstance(item, dict):
                                            if 'team_key' in item:
                                                team1_key = item['team_key']
                                            if 'name' in item:
                                                team1_name = item['name']
                                            if 'team_points' in item:
                                                tp = item['team_points']
                                                if isinstance(tp, dict):
                                                    team1_points = float(tp.get('total', 0))
                                                else:
                                                    team1_points = float(tp) if tp else 0
                                            if 'team_stats' in item:
                                                stats = item['team_stats']
                                                if isinstance(stats, dict) and 'stats' in stats:
                                                    for stat in stats['stats']:
                                                        if isinstance(stat, dict) and 'stat' in stat:
                                                            s = stat['stat']
                                                            sid = s.get('stat_id', '')
                                                            sval = s.get('value', '')
                                                            stat_map = {
                                                                '5': 'fg_pct', '8': 'ft_pct', '10': 'threes',
                                                                '12': 'pts', '15': 'reb', '16': 'ast',
                                                                '17': 'stl', '18': 'blk', '19': 'to'
                                                            }
                                                            if sid in stat_map:
                                                                team1_stats_dict[stat_map[sid]] = sval
                                    
                                    # Parse team 2
                                    team2_key = ''
                                    team2_name = ''
                                    team2_points = 0
                                    team2_stats_dict = {}
                                    
                                    for item in team2_list:
                                        if isinstance(item, dict):
                                            if 'team_key' in item:
                                                team2_key = item['team_key']
                                            if 'name' in item:
                                                team2_name = item['name']
                                            if 'team_points' in item:
                                                tp = item['team_points']
                                                if isinstance(tp, dict):
                                                    team2_points = float(tp.get('total', 0))
                                                else:
                                                    team2_points = float(tp) if tp else 0
                                            if 'team_stats' in item:
                                                stats = item['team_stats']
                                                if isinstance(stats, dict) and 'stats' in stats:
                                                    for stat in stats['stats']:
                                                        if isinstance(stat, dict) and 'stat' in stat:
                                                            s = stat['stat']
                                                            sid = s.get('stat_id', '')
                                                            sval = s.get('value', '')
                                                            stat_map = {
                                                                '5': 'fg_pct', '8': 'ft_pct', '10': 'threes',
                                                                '12': 'pts', '15': 'reb', '16': 'ast',
                                                                '17': 'stl', '18': 'blk', '19': 'to'
                                                            }
                                                            if sid in stat_map:
                                                                team2_stats_dict[stat_map[sid]] = sval
                                    
                                    # Get category winners
                                    stat_winners = matchup.get('stat_winners', [])
                                    winners_dict = {}
                                    if isinstance(stat_winners, list):
                                        for sw in stat_winners:
                                            if isinstance(sw, dict) and 'stat_winner' in sw:
                                                w = sw['stat_winner']
                                                sid = w.get('stat_id', '')
                                                wkey = w.get('winner_team_key', '')
                                                stat_map = {
                                                    '5': 'fg_pct', '8': 'ft_pct', '10': 'threes',
                                                    '12': 'pts', '15': 'reb', '16': 'ast',
                                                    '17': 'stl', '18': 'blk', '19': 'to'
                                                }
                                                if sid in stat_map:
                                                    winners_dict[f'winner_{stat_map[sid]}'] = wkey
                                    
                                    all_matchup_records.append({
                                        'week': matchup.get('week', week),
                                        'week_start': matchup.get('week_start', ''),
                                        'week_end': matchup.get('week_end', ''),
                                        'status': matchup.get('status', ''),
                                        'is_playoffs': matchup.get('is_playoffs', '0'),
                                        'winner_team_key': matchup.get('winner_team_key', ''),
                                        'team1_key': team1_key,
                                        'team1_name': team1_name,
                                        'team1_points': team1_points,
                                        'team1_fg_pct': team1_stats_dict.get('fg_pct', ''),
                                        'team1_ft_pct': team1_stats_dict.get('ft_pct', ''),
                                        'team1_threes': team1_stats_dict.get('threes', ''),
                                        'team1_pts': team1_stats_dict.get('pts', ''),
                                        'team1_reb': team1_stats_dict.get('reb', ''),
                                        'team1_ast': team1_stats_dict.get('ast', ''),
                                        'team1_stl': team1_stats_dict.get('stl', ''),
                                        'team1_blk': team1_stats_dict.get('blk', ''),
                                        'team1_to': team1_stats_dict.get('to', ''),
                                        'team2_key': team2_key,
                                        'team2_name': team2_name,
                                        'team2_points': team2_points,
                                        'team2_fg_pct': team2_stats_dict.get('fg_pct', ''),
                                        'team2_ft_pct': team2_stats_dict.get('ft_pct', ''),
                                        'team2_threes': team2_stats_dict.get('threes', ''),
                                        'team2_pts': team2_stats_dict.get('pts', ''),
                                        'team2_reb': team2_stats_dict.get('reb', ''),
                                        'team2_ast': team2_stats_dict.get('ast', ''),
                                        'team2_stl': team2_stats_dict.get('stl', ''),
                                        'team2_blk': team2_stats_dict.get('blk', ''),
                                        'team2_to': team2_stats_dict.get('to', ''),
                                        **winners_dict,
                                        'extracted_at': timestamp,
                                        'league_id': league_id
                                    })
        
        time.sleep(0.3)
    except Exception as e:
        print(f"Error fetching week {week}: {e}")

if all_matchup_records:
    df_matchups = pd.DataFrame(all_matchup_records)
    print(f"\nTotal matchups: {len(df_matchups)}")
    print(f"Sample:\n{df_matchups[['week', 'team1_name', 'team1_points', 'team2_name', 'team2_points']].head()}")
    
    table_id = f"{project_id}.{dataset}.matchups"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
    job = client.load_table_from_dataframe(df_matchups, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df_matchups)} matchups!")
else:
    print("No matchups collected")

# ==================== TRANSACTIONS ====================
print("\n=== FETCHING TRANSACTIONS ===")
try:
    # Try getting more transactions
    for trans_type in ['add', 'drop', 'trade']:
        print(f"Fetching {trans_type} transactions...")
        try:
            trans_list = lg.transactions(trans_type, 500)  # Get up to 500
            print(f"  Raw response type: {type(trans_list)}")
            if trans_list:
                print(f"  Found {len(trans_list) if isinstance(trans_list, list) else 'N/A'} {trans_type} transactions")
        except Exception as e:
            print(f"  Error: {e}")
    
    print("Transaction fetching complete (no data structure handler yet)")
except Exception as e:
    print(f"Error: {e}")

# ==================== ROSTERS ====================
print("\n=== FETCHING ROSTERS ===")
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
                        'is_rostered': True,
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
        print(f"Loaded {len(df_players)} rostered players!")
except Exception as e:
    print(f"Error: {e}")

# ==================== ALL PLAYERS (ROSTERED + AVAILABLE) ====================
print("\n=== FETCHING COMPLETE PLAYER POOL ===")
try:
    # Method 1: Get all players using player search
    all_players_list = []
    
    # Get top 500 players by searching
    print("Fetching all NBA players...")
    try:
        # Use the league players endpoint
        response = sc.session.get(
            f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_id}/players",
            params={'format': 'json', 'count': 500}
        )
        data = response.json()
        
        if 'fantasy_content' in data:
            content = data['fantasy_content']
            if 'league' in content:
                league_data = content['league']
                if isinstance(league_data, list):
                    for item in league_data:
                        if isinstance(item, dict) and 'players' in item:
                            players = item['players']
                            for key in players:
                                if key != 'count' and 'player' in players[key]:
                                    player_data = players[key]['player']
                                    if isinstance(player_data, list):
                                        p = {}
                                        for pitem in player_data:
                                            if isinstance(pitem, dict):
                                                p.update(pitem)
                                        
                                        player_name = ''
                                        if 'name' in p:
                                            name_field = p['name']
                                            if isinstance(name_field, dict):
                                                player_name = name_field.get('full', '')
                                        
                                        ownership_type = 'available'
                                        if 'ownership' in p:
                                            own = p['ownership']
                                            if isinstance(own, dict):
                                                ownership_type = own.get('ownership_type', 'available')
                                        
                                        all_players_list.append({
                                            'player_id': p.get('player_id', ''),
                                            'player_name': player_name,
                                            'position': p.get('position_type', ''),
                                            'status': p.get('status', ''),
                                            'nba_team': p.get('editorial_team_abbr', ''),
                                            'ownership_type': ownership_type,
                                            'extracted_at': timestamp,
                                            'league_id': league_id
                                        })
        
        print(f"Fetched {len(all_players_list)} players from league")
    except Exception as e:
        print(f"Error fetching all players: {e}")
        import traceback
        traceback.print_exc()
    
    if all_players_list:
        df_all_players = pd.DataFrame(all_players_list).drop_duplicates(subset=['player_id'])
        
        table_id = f"{project_id}.{dataset}.player_pool"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
        job = client.load_table_from_dataframe(df_all_players, table_id, job_config=job_config)
        job.result()
        print(f"Loaded {len(df_all_players)} players to pool!")
    else:
        print("No players collected")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

print("\n=== ALL DONE ===")
print(f"Tables updated in {project_id}.{dataset}")
