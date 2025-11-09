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

league_id = '466.l.75582'  # CURRENT LEAGUE
lg = league.League(sc, league_id)
print("Found your league!")

dataset = os.environ.get('BQ_DATASET_NBA_YAHOO_ZILBER')
timestamp = datetime.now()

settings = lg.settings()
start_week = int(settings.get('start_week', 1))
end_week = int(settings.get('end_week', 21))
current_week = lg.current_week()

print(f"\nLeague: week {start_week} to {end_week}, current: {current_week}")

# ==================== GET TEAM NAMES FIRST ====================
print("\n=== FETCHING TEAM INFO ===")
try:
    teams = lg.teams()
    team_names = {}
    for tk, td in teams.items():
        team_names[tk] = td.get('name', '') if isinstance(td, dict) else str(td)
    print(f"Got {len(team_names)} team names")
    time.sleep(1)  # Rate limit protection
except Exception as e:
    print(f"Error getting teams: {e}")
    team_names = {}

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
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
    job = client.load_table_from_dataframe(df_standings, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df_standings)} teams!")
    time.sleep(1)
except Exception as e:
    print(f"Error: {e}")

# ==================== MATCHUPS ====================
print("\n=== FETCHING MATCHUPS ===")
all_matchup_records = []

for week in range(start_week, min(current_week + 1, end_week + 1)):
    print(f"Week {week}...", end=" ")
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
                                if key == 'count':
                                    continue
                                if 'matchup' not in matchups[key]:
                                    continue
                                    
                                matchup = matchups[key]['matchup']
                                teams_container = matchup.get('0', {})
                                if 'teams' not in teams_container:
                                    continue
                                
                                teams = teams_container['teams']
                                team1_data = teams.get('0', {}).get('team', [[]])
                                team2_data = teams.get('1', {}).get('team', [[]])
                                
                                # Parse team 1
                                team1_info = {}
                                team1_stats = {}
                                team1_points = 0
                                
                                if isinstance(team1_data, list):
                                    if len(team1_data) > 0:
                                        for item in team1_data[0]:
                                            if isinstance(item, dict):
                                                team1_info.update(item)
                                    
                                    if len(team1_data) > 1 and isinstance(team1_data[1], dict):
                                        if 'team_stats' in team1_data[1]:
                                            stats_obj = team1_data[1]['team_stats']
                                            if isinstance(stats_obj, dict) and 'stats' in stats_obj:
                                                for stat in stats_obj['stats']:
                                                    if isinstance(stat, dict) and 'stat' in stat:
                                                        s = stat['stat']
                                                        sid = s.get('stat_id', '')
                                                        sval = s.get('value', '')
                                                        stat_map = {'5': 'fg_pct', '8': 'ft_pct', '10': 'threes', '12': 'pts', '15': 'reb', '16': 'ast', '17': 'stl', '18': 'blk', '19': 'to'}
                                                        if sid in stat_map:
                                                            team1_stats[stat_map[sid]] = sval
                                        
                                        if 'team_points' in team1_data[1]:
                                            tp = team1_data[1]['team_points']
                                            if isinstance(tp, dict):
                                                team1_points = float(tp.get('total', 0))
                                
                                # Parse team 2
                                team2_info = {}
                                team2_stats = {}
                                team2_points = 0
                                
                                if isinstance(team2_data, list):
                                    if len(team2_data) > 0:
                                        for item in team2_data[0]:
                                            if isinstance(item, dict):
                                                team2_info.update(item)
                                    
                                    if len(team2_data) > 1 and isinstance(team2_data[1], dict):
                                        if 'team_stats' in team2_data[1]:
                                            stats_obj = team2_data[1]['team_stats']
                                            if isinstance(stats_obj, dict) and 'stats' in stats_obj:
                                                for stat in stats_obj['stats']:
                                                    if isinstance(stat, dict) and 'stat' in stat:
                                                        s = stat['stat']
                                                        sid = s.get('stat_id', '')
                                                        sval = s.get('value', '')
                                                        stat_map = {'5': 'fg_pct', '8': 'ft_pct', '10': 'threes', '12': 'pts', '15': 'reb', '16': 'ast', '17': 'stl', '18': 'blk', '19': 'to'}
                                                        if sid in stat_map:
                                                            team2_stats[stat_map[sid]] = sval
                                        
                                        if 'team_points' in team2_data[1]:
                                            tp = team2_data[1]['team_points']
                                            if isinstance(tp, dict):
                                                team2_points = float(tp.get('total', 0))
                                
                                # Get winners
                                stat_winners = matchup.get('stat_winners', [])
                                winners = {}
                                if isinstance(stat_winners, list):
                                    for sw in stat_winners:
                                        if isinstance(sw, dict) and 'stat_winner' in sw:
                                            w = sw['stat_winner']
                                            sid = w.get('stat_id', '')
                                            wkey = w.get('winner_team_key', '')
                                            stat_map = {'5': 'fg_pct', '8': 'ft_pct', '10': 'threes', '12': 'pts', '15': 'reb', '16': 'ast', '17': 'stl', '18': 'blk', '19': 'to'}
                                            if sid in stat_map:
                                                winners[f'winner_{stat_map[sid]}'] = wkey
                                
                                all_matchup_records.append({
                                    'week': matchup.get('week', week),
                                    'week_start': matchup.get('week_start', ''),
                                    'week_end': matchup.get('week_end', ''),
                                    'status': matchup.get('status', ''),
                                    'is_playoffs': matchup.get('is_playoffs', '0'),
                                    'winner_team_key': matchup.get('winner_team_key', ''),
                                    'team1_key': team1_info.get('team_key', ''),
                                    'team1_name': team1_info.get('name', ''),
                                    'team1_points': team1_points,
                                    'team1_fg_pct': team1_stats.get('fg_pct', ''),
                                    'team1_ft_pct': team1_stats.get('ft_pct', ''),
                                    'team1_threes': team1_stats.get('threes', ''),
                                    'team1_pts': team1_stats.get('pts', ''),
                                    'team1_reb': team1_stats.get('reb', ''),
                                    'team1_ast': team1_stats.get('ast', ''),
                                    'team1_stl': team1_stats.get('stl', ''),
                                    'team1_blk': team1_stats.get('blk', ''),
                                    'team1_to': team1_stats.get('to', ''),
                                    'team2_key': team2_info.get('team_key', ''),
                                    'team2_name': team2_info.get('name', ''),
                                    'team2_points': team2_points,
                                    'team2_fg_pct': team2_stats.get('fg_pct', ''),
                                    'team2_ft_pct': team2_stats.get('ft_pct', ''),
                                    'team2_threes': team2_stats.get('threes', ''),
                                    'team2_pts': team2_stats.get('pts', ''),
                                    'team2_reb': team2_stats.get('reb', ''),
                                    'team2_ast': team2_stats.get('ast', ''),
                                    'team2_stl': team2_stats.get('stl', ''),
                                    'team2_blk': team2_stats.get('blk', ''),
                                    'team2_to': team2_stats.get('to', ''),
                                    **winners,
                                    'extracted_at': timestamp,
                                    'league_id': league_id
                                })
        print("✓")
        time.sleep(0.5)  # Rate limit protection
    except Exception as e:
        print(f"✗ {e}")

if all_matchup_records:
    df_matchups = pd.DataFrame(all_matchup_records)
    print(f"Total: {len(df_matchups)}")
    
    # NEW: Added these lines to properly handle data types
    numeric_cols = ['week', 'team1_points', 'team2_points']
    for col in numeric_cols:
        if col in df_matchups.columns:
            df_matchups[col] = pd.to_numeric(df_matchups[col], errors='coerce').fillna(0)
    
    # NEW: Handle is_playoffs as string (it comes as '0' or '1' from Yahoo)
    if 'is_playoffs' in df_matchups.columns:
        df_matchups['is_playoffs'] = df_matchups['is_playoffs'].astype(str)
    
    table_id = f"{project_id}.{dataset}.matchups"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
    job = client.load_table_from_dataframe(df_matchups, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df_matchups)} matchups!")

# ==================== TRANSACTIONS ====================
print("\n=== FETCHING TRANSACTIONS ===")
all_transactions = []
seen_trans_players = set()  # Track (transaction_id, player_id) to avoid duplicates

for trans_type in ['add', 'drop', 'trade']:
    try:
        print(f"Fetching {trans_type}...", end=" ")
        
        # Yahoo API typically limits to max ~1000 transactions per type
        # Try increasing the count to get more
        trans_list = lg.transactions(trans_type, 1000)
        
        print(f"(Got {len(trans_list) if trans_list else 0} from API)...", end=" ")
        
        for trans in trans_list:
            if not isinstance(trans, dict):
                continue
            
            trans_key = trans.get('transaction_key', '')
            trans_id = trans.get('transaction_id', '')
            trans_type_val = trans.get('type', trans_type)
            status = trans.get('status', '')
            trans_timestamp = trans.get('timestamp', '')
            
            players = trans.get('players', {})
            if isinstance(players, dict):
                for pkey in players:
                    if pkey == 'count':
                        continue
                    
                    # Initialize variables
                    player_id = ''
                    player_name = ''
                    dest_team = ''
                    dest_team_name = ''
                    source_team = ''
                    source_team_name = ''
                    source_type = ''
                    dest_type = ''
                    transaction_type = ''
                    
                    player_obj = players[pkey]
                    
                    if isinstance(player_obj, dict) and 'player' in player_obj:
                        player_data = player_obj['player']
                        
                        # Parse player info from first element
                        if isinstance(player_data, list) and len(player_data) > 0:
                            player_info_list = player_data[0]
                            if isinstance(player_info_list, list):
                                for item in player_info_list:
                                    if isinstance(item, dict):
                                        if 'player_id' in item:
                                            player_id = item['player_id']
                                        if 'name' in item:
                                            name_obj = item['name']
                                            if isinstance(name_obj, dict):
                                                player_name = name_obj.get('full', '')
                            
                            # Parse transaction_data from second element
                            if len(player_data) > 1:
                                trans_data_obj = player_data[1]
                                if isinstance(trans_data_obj, dict) and 'transaction_data' in trans_data_obj:
                                    trans_data_list = trans_data_obj['transaction_data']
                                    if isinstance(trans_data_list, list) and len(trans_data_list) > 0:
                                        trans_data = trans_data_list[0]
                                        
                                        transaction_type = trans_data.get('type', '')
                                        dest_team = trans_data.get('destination_team_key', '')
                                        dest_team_name = trans_data.get('destination_team_name', '')
                                        source_team = trans_data.get('source_team_key', '')
                                        source_team_name = trans_data.get('source_team_name', '')
                                        source_type = trans_data.get('source_type', '')
                                        dest_type = trans_data.get('destination_type', '')
                    
                    # Skip if no player name or if we've seen this transaction+player combo
                    if not player_name:
                        continue
                    
                    trans_player_key = f"{trans_id}_{player_id}"
                    if trans_player_key in seen_trans_players:
                        continue
                    
                    seen_trans_players.add(trans_player_key)
                    
                    # Fix drop transactions - Yahoo puts the dropping team in destination field
                    if transaction_type == 'drop' and dest_team and not source_team:
                        source_team = dest_team
                        source_team_name = dest_team_name
                        dest_team = 'waivers'
                        dest_team_name = 'Waivers'
                        dest_type = 'waivers'
                        source_type = 'team'
                    
                    # Infer action
                    action = transaction_type
                    if not action:
                        if trans_type_val == 'drop':
                            action = 'drop'
                        elif trans_type_val == 'add':
                            action = 'add'
                        elif trans_type_val == 'trade':
                            action = 'trade'
                        elif dest_team and source_type == 'freeagents':
                            action = 'add'
                        elif source_team and dest_type == 'waivers':
                            action = 'drop'
                    
                    all_transactions.append({
                        'transaction_key': trans_key,
                        'transaction_id': trans_id,
                        'type': trans_type_val,
                        'player_action': action,
                        'status': status,
                        'timestamp': trans_timestamp,
                        'player_id': player_id,
                        'player_name': player_name,
                        'destination_team_key': dest_team if dest_team else None,
                        'destination_team_name': dest_team_name if dest_team_name else None,
                        'source_team_key': source_team if source_team else None,
                        'source_team_name': source_team_name if source_team_name else None,
                        'source_type': source_type if source_type else None,
                        'destination_type': dest_type if dest_type else None,
                        'extracted_at': timestamp,
                        'league_id': league_id
                    })
        
        unique_count = len([t for t in all_transactions if t['type'] == trans_type])
        print(f"✓ {unique_count} unique records")
        time.sleep(2)
        
    except Exception as e:
        print(f"✗ {e}")
        import traceback
        traceback.print_exc()

if all_transactions:
    df_trans = pd.DataFrame(all_transactions)
    print(f"\nTotal unique transactions: {len(df_trans)}")
    
    table_id = f"{project_id}.{dataset}.transactions"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
    job = client.load_table_from_dataframe(df_trans, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df_trans)} transactions!")
else:
    print("⚠️ No transactions collected")
    

# ==================== ROSTERS ====================
print("\n=== FETCHING ROSTERS ===")
try:
    player_records = []
    
    for team_key, team_name in team_names.items():
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
        
        time.sleep(0.3)  # Rate limit protection
    
    if player_records:
        df_players = pd.DataFrame(player_records)
        
        table_id = f"{project_id}.{dataset}.rosters"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
        job = client.load_table_from_dataframe(df_players, table_id, job_config=job_config)
        job.result()
        print(f"Loaded {len(df_players)} rostered players!")
    else:
        print("No roster data collected")
        
except Exception as e:
    print(f"Error with rosters: {e}")
    import traceback
    traceback.print_exc()

# ==================== PLAYER POOL ====================
print("\n=== FETCHING PLAYER POOL ===")
try:
    all_players_list = []
    
    # Fetch in batches
    for start in range(0, 1000, 25):  # Get up to 500 players in batches of 25
        try:
            response = sc.session.get(
                f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_id}/players",
                params={'format': 'json', 'start': start, 'count': 25}
            )
            data = response.json()
            
            found_players = False
            if 'fantasy_content' in data and 'league' in data['fantasy_content']:
                league_data = data['fantasy_content']['league']
                if isinstance(league_data, list):
                    for item in league_data:
                        if isinstance(item, dict) and 'players' in item:
                            players = item['players']
                            
                            for key in players:
                                if key == 'count':
                                    continue
                                
                                if 'player' in players[key]:
                                    found_players = True
                                    player_list = players[key]['player']
                                    p = {}
                                    
                                    if isinstance(player_list, list):
                                        for pitem in player_list:
                                            if isinstance(pitem, list):
                                                for pi in pitem:
                                                    if isinstance(pi, dict):
                                                        p.update(pi)
                                            elif isinstance(pitem, dict):
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
                                    
                                    if player_name:
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
            
            if not found_players:
                break  # No more players
            
            time.sleep(0.5)  # Rate limit protection
            
        except Exception as e:
            print(f"Error at start={start}: {e}")
            break
    
    if all_players_list:
        df_all_players = pd.DataFrame(all_players_list).drop_duplicates(subset=['player_id'])
        print(f"Collected {len(df_all_players)} unique players")
        
        table_id = f"{project_id}.{dataset}.player_pool"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
        job = client.load_table_from_dataframe(df_all_players, table_id, job_config=job_config)
        job.result()
        print(f"Loaded {len(df_all_players)} players to pool!")
    else:
        print("No players collected for pool")
        
except Exception as e:
    print(f"Error with player pool: {e}")
    import traceback
    traceback.print_exc()
    
# ==================== PLAYER RANKINGS ====================
print("\n=== FETCHING PLAYER RANKINGS ===")
try:
    all_rankings = []
    
    # Fetch all players with their rankings
    print(f"Fetching player rankings...", end=" ")
    
    for start in range(0, 600, 25):  # Get top 300 players
        try:
            response = sc.session.get(
                f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_id}/players",
                params={
                    'format': 'json',
                    'start': start,
                    'count': 25,
                    'sort': 'AR',  # Sort by Average Rank
                    'sort_type': 'season'
                }
            )
            data = response.json()
            
            found_players = False
            if 'fantasy_content' in data and 'league' in data['fantasy_content']:
                league_data = data['fantasy_content']['league']
                if isinstance(league_data, list):
                    for item in league_data:
                        if isinstance(item, dict) and 'players' in item:
                            players = item['players']
                            
                            for key in players:
                                if key == 'count':
                                    continue
                                
                                if 'player' in players[key]:
                                    found_players = True
                                    player_list = players[key]['player']
                                    
                                    # Parse player data
                                    player_id = ''
                                    player_name = ''
                                    display_position = ''
                                    position_type = ''
                                    nba_team = ''
                                    status = ''
                                    
                                    # Ranking data
                                    season_rank = None
                                    last_7_rank = None
                                    last_14_rank = None
                                    last_30_rank = None
                                    
                                    if isinstance(player_list, list):
                                        for pitem in player_list:
                                            if isinstance(pitem, list):
                                                for pi in pitem:
                                                    if isinstance(pi, dict):
                                                        if 'player_id' in pi:
                                                            player_id = pi['player_id']
                                                        if 'name' in pi:
                                                            name_obj = pi['name']
                                                            if isinstance(name_obj, dict):
                                                                player_name = name_obj.get('full', '')
                                                        if 'display_position' in pi:
                                                            display_position = pi['display_position']
                                                        if 'position_type' in pi:
                                                            position_type = pi['position_type']
                                                        if 'editorial_team_abbr' in pi:
                                                            nba_team = pi['editorial_team_abbr']
                                                        if 'status' in pi:
                                                            status = pi['status']
                                            
                                            elif isinstance(pitem, dict):
                                                # Check for player_stats
                                                if 'player_stats' in pitem:
                                                    stats_obj = pitem['player_stats']
                                                    if isinstance(stats_obj, dict):
                                                        # Get stats for different periods
                                                        stats = stats_obj.get('stats', [])
                                                        if isinstance(stats, list):
                                                            for stat in stats:
                                                                if isinstance(stat, dict) and 'stat' in stat:
                                                                    s = stat['stat']
                                                                    stat_id = s.get('stat_id', '')
                                                                    stat_val = s.get('value', '')
                                                                    
                                                                    # Check if this is a ranking stat
                                                                    # Usually ranking stats have specific IDs
                                                
                                                # Check for player_points (sometimes contains rank)
                                                if 'player_points' in pitem:
                                                    pp = pitem['player_points']
                                                    if isinstance(pp, dict):
                                                        coverage = pp.get('coverage_type', '')
                                                        # This might contain ranking info
                                                
                                                # Check for player_season_stats
                                                if 'player_season_stats' in pitem:
                                                    season_stats = pitem['player_season_stats']
                                                    # Extract season ranking if available
                                    
                                    # For now, use the position in the results as the rank
                                    # since players are returned sorted by rank
                                    calculated_rank = start + int(key)
                                    
                                    if player_name:
                                        all_rankings.append({
                                            'player_id': player_id,
                                            'player_name': player_name,
                                            'display_position': display_position,
                                            'position_type': position_type,
                                            'nba_team': nba_team,
                                            'status': status,
                                            'season_rank': calculated_rank,
                                            'extracted_at': timestamp,
                                            'league_id': league_id
                                        })
            
            if not found_players:
                break
            
            time.sleep(0.5)
            
        except Exception as e:
            print(f"Error at start={start}: {e}")
            break
    
    print(f"✓ Collected {len(all_rankings)} players")
    
    if all_rankings:
        df_rankings = pd.DataFrame(all_rankings)
        
        table_id = f"{project_id}.{dataset}.player_rankings"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
        job = client.load_table_from_dataframe(df_rankings, table_id, job_config=job_config)
        job.result()
        print(f"Loaded {len(df_rankings)} player rankings!")
    else:
        print("No rankings collected")
        
except Exception as e:
    print(f"Error with rankings: {e}")
    import traceback
    traceback.print_exc()

print("\n=== COMPLETE ===")
