#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import argparse
import datetime
from typing import List, Optional, Dict, Any

import pandas as pd
from pandas.api.types import is_object_dtype
import requests

from google.cloud import bigquery
from google.oauth2 import service_account


# -----------------------------
# Config via environment
# -----------------------------

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nba.com/"
}

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET = os.environ.get("BQ_DATASET", "nba_data")

SA_INFO = json.loads(os.environ["GCP_SA_KEY"])
CREDS = service_account.Credentials.from_service_account_info(SA_INFO)
BQ = bigquery.Client(project=PROJECT_ID, credentials=CREDS)

# ESPN endpoints (for game discovery)
ESPN_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"

# NBA endpoints with fallbacks
NBA_SCOREBOARD_V2 = "https://stats.nba.com/stats/scoreboardv2"
NBA_BOXSCORE_TRADITIONAL = "https://stats.nba.com/stats/boxscoretraditionalv2"
NBA_CDN_SCOREBOARD = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
NBA_CDN_BOXSCORE = "https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"

# Team mapping between ESPN and NBA IDs
ESPN_TO_NBA_TEAMS = {
    1: 1610612737,   # ATL
    2: 1610612738,   # BOS  
    3: 1610612751,   # BKN
    4: 1610612766,   # CHA
    5: 1610612741,   # CHI
    6: 1610612739,   # CLE
    7: 1610612742,   # DAL
    8: 1610612743,   # DEN
    9: 1610612765,   # DET
    10: 1610612744,  # GSW
    11: 1610612745,  # HOU
    12: 1610612754,  # IND
    13: 1610612746,  # LAC
    14: 1610612747,  # LAL
    15: 1610612763,  # MEM
    16: 1610612748,  # MIA
    17: 1610612749,  # MIL
    18: 1610612750,  # MIN
    19: 1610612740,  # NOP
    20: 1610612752,  # NYK
    21: 1610612760,  # OKC
    22: 1610612753,  # ORL
    23: 1610612755,  # PHI
    24: 1610612756,  # PHX
    25: 1610612757,  # POR
    26: 1610612758,  # SAC
    27: 1610612759,  # SAS
    28: 1610612761,  # TOR
    29: 1610612762,  # UTA
    30: 1610612764,  # WAS
}

# Reverse mapping for NBA team IDs to abbreviations
NBA_TEAM_ABBR = {
    1610612737: "ATL", 1610612738: "BOS", 1610612751: "BKN", 1610612766: "CHA",
    1610612741: "CHI", 1610612739: "CLE", 1610612742: "DAL", 1610612743: "DEN",
    1610612765: "DET", 1610612744: "GSW", 1610612745: "HOU", 1610612754: "IND",
    1610612746: "LAC", 1610612747: "LAL", 1610612763: "MEM", 1610612748: "MIA",
    1610612749: "MIL", 1610612750: "MIN", 1610612740: "NOP", 1610612752: "NYK",
    1610612760: "OKC", 1610612753: "ORL", 1610612755: "PHI", 1610612756: "PHX",
    1610612757: "POR", 1610612758: "SAC", 1610612759: "SAS", 1610612761: "TOR",
    1610612762: "UTA", 1610612764: "WAS"
}

# -----------------------------
# BigQuery schemas
# -----------------------------
GAMES_SCHEMA = [
    bigquery.SchemaField("game_id", "STRING"),
    bigquery.SchemaField("nba_game_id", "STRING"),
    bigquery.SchemaField("game_date", "DATE"),
    bigquery.SchemaField("season", "INT64"),
    bigquery.SchemaField("home_team_id", "INT64"),
    bigquery.SchemaField("home_team_abbr", "STRING"),
    bigquery.SchemaField("home_score", "INT64"),
    bigquery.SchemaField("away_team_id", "INT64"),
    bigquery.SchemaField("away_team_abbr", "STRING"),
    bigquery.SchemaField("away_score", "INT64"),
    bigquery.SchemaField("game_status", "STRING"),
]

BOX_SCHEMA = [
    bigquery.SchemaField("game_id", "STRING"),
    bigquery.SchemaField("nba_game_id", "STRING"),
    bigquery.SchemaField("game_date", "DATE"),
    bigquery.SchemaField("season", "INT64"),
    bigquery.SchemaField("team_id", "INT64"),
    bigquery.SchemaField("team_abbr", "STRING"),
    bigquery.SchemaField("player_id", "INT64"),
    bigquery.SchemaField("player_name", "STRING"),
    bigquery.SchemaField("starter", "BOOL"),
    bigquery.SchemaField("minutes", "STRING"),
    bigquery.SchemaField("pts", "INT64"),
    bigquery.SchemaField("fgm", "INT64"),
    bigquery.SchemaField("fga", "INT64"),
    bigquery.SchemaField("fg_pct", "FLOAT"),
    bigquery.SchemaField("fg3m", "INT64"),
    bigquery.SchemaField("fg3a", "INT64"),
    bigquery.SchemaField("fg3_pct", "FLOAT"),
    bigquery.SchemaField("ftm", "INT64"),
    bigquery.SchemaField("fta", "INT64"),
    bigquery.SchemaField("ft_pct", "FLOAT"),
    bigquery.SchemaField("oreb", "INT64"),
    bigquery.SchemaField("dreb", "INT64"),
    bigquery.SchemaField("reb", "INT64"),
    bigquery.SchemaField("ast", "INT64"),
    bigquery.SchemaField("stl", "INT64"),
    bigquery.SchemaField("blk", "INT64"),
    bigquery.SchemaField("tov", "INT64"),
    bigquery.SchemaField("pf", "INT64"),
    bigquery.SchemaField("plus_minus", "INT64"),
]

# -----------------------------
# BigQuery helpers
# -----------------------------
def ensure_dataset() -> None:
    ds_id = f"{PROJECT_ID}.{DATASET}"
    try:
        BQ.get_dataset(ds_id)
    except Exception:
        BQ.create_dataset(bigquery.Dataset(ds_id))

def ensure_tables() -> None:
    ensure_dataset()

    games_table_id = f"{PROJECT_ID}.{DATASET}.games_daily"
    try:
        BQ.get_table(games_table_id)
    except Exception:
        t = bigquery.Table(games_table_id, schema=GAMES_SCHEMA)
        BQ.create_table(t)

    box_table_id = f"{PROJECT_ID}.{DATASET}.player_boxscores"
    try:
        BQ.get_table(box_table_id)
    except Exception:
        t = bigquery.Table(box_table_id, schema=BOX_SCHEMA)
        BQ.create_table(t)

def load_df(df: pd.DataFrame, table: str) -> None:
    if df is None or df.empty:
        return
    table_id = f"{PROJECT_ID}.{DATASET}.{table}"
    schema = GAMES_SCHEMA if table == "games_daily" else BOX_SCHEMA

    if "game_date" in df.columns:
        df = df.dropna(subset=["game_date"])

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    )
    BQ.load_table_from_dataframe(df, table_id, job_config=job_config).result()

# -----------------------------
# Utilities
# -----------------------------
def safe_int(x: Any) -> Optional[int]:
    try:
        return int(x) if x is not None and x != "" and x != "N/A" else None
    except Exception:
        return None

def safe_float(x: Any) -> Optional[float]:
    try:
        return float(x) if x is not None and x != "" and x != "N/A" else None
    except Exception:
        return None

def http_get_json_robust(url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 30, max_retries: int = 3) -> Dict[str, Any]:
    """Robust HTTP request with multiple retry strategies"""
    
    for attempt in range(max_retries):
        try:
            # Increase timeout on retries
            current_timeout = timeout + (attempt * 10)
            
            print(f"  Attempt {attempt + 1}/{max_retries}: {url} (timeout: {current_timeout}s)")
            
            r = requests.get(url, params=params, headers=HEADERS, timeout=current_timeout)
            r.raise_for_status()
            return r.json()
            
        except requests.exceptions.Timeout:
            print(f"  Timeout on attempt {attempt + 1}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
                
        except requests.exceptions.RequestException as e:
            print(f"  Request error on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                time.sleep(1)
                
        except Exception as e:
            print(f"  Unexpected error on attempt {attempt + 1}: {e}")
            break
    
    print(f"  Failed all {max_retries} attempts for {url}")
    return {}

# -----------------------------
# ESPN Game Discovery
# -----------------------------
def get_espn_games_for_date(date_yyyymmdd: str) -> List[Dict[str, Any]]:
    """Get games using ESPN scoreboard (reliable for game discovery)"""
    try:
        data = http_get_json_robust(ESPN_SCOREBOARD, params={"dates": date_yyyymmdd})
        games = []
        
        if 'events' in data:
            for event in data['events']:
                # Only process completed games
                status = event.get('status', {}).get('type', {}).get('name', '')
                if status not in ['STATUS_FINAL']:
                    continue
                
                competitors = event.get('competitions', [{}])[0].get('competitors', [])
                home_team = next((c for c in competitors if c.get('homeAway') == 'home'), {})
                away_team = next((c for c in competitors if c.get('homeAway') == 'away'), {})
                
                game = {
                    'espn_game_id': event['id'],
                    'game_date': event['date'][:10],  # YYYY-MM-DD
                    'season': safe_int(event.get('season', {}).get('year', 2025)),
                    'home_team_espn_id': safe_int(home_team.get('team', {}).get('id')),
                    'home_team_abbr': home_team.get('team', {}).get('abbreviation'),
                    'home_score': safe_int(home_team.get('score')),
                    'away_team_espn_id': safe_int(away_team.get('team', {}).get('id')),
                    'away_team_abbr': away_team.get('team', {}).get('abbreviation'),
                    'away_score': safe_int(away_team.get('score')),
                    'game_status': status,
                }
                games.append(game)
        
        print(f"Found {len(games)} completed games from ESPN for {date_yyyymmdd}")
        return games
        
    except Exception as e:
        print(f"Error getting ESPN games for {date_yyyymmdd}: {e}")
        return []

# -----------------------------
# Multi-source NBA Data with Fallbacks
# -----------------------------
def get_nba_games_multi_source(date_str: str) -> Dict[str, Dict[str, Any]]:
    """
    Get NBA games using multiple sources with fallbacks
    Returns: {team_matchup_key: nba_game_data}
    """
    date_obj = datetime.datetime.strptime(date_str, '%m/%d/%Y')
    
    # Try multiple dates for timezone issues
    dates_to_check = [
        (date_obj - datetime.timedelta(days=1)).strftime('%m/%d/%Y'),
        date_str,
        (date_obj + datetime.timedelta(days=1)).strftime('%m/%d/%Y')
    ]
    
    games_map = {}
    
    # Method 1: Try NBA stats API first (most complete data)
    for check_date in dates_to_check:
        print(f"Trying NBA stats API for {check_date}")
        try:
            params = {
                'GameDate': check_date,
                'LeagueID': '00',
                'DayOffset': '0'
            }
            
            data = http_get_json_robust(NBA_SCOREBOARD_V2, params=params, timeout=20, max_retries=2)
            
            if data and 'resultSets' in data and len(data['resultSets']) > 0:
                headers = data['resultSets'][0]['headers']
                rows = data['resultSets'][0]['rowSet']
                
                for row in rows:
                    game_data = dict(zip(headers, row))
                    
                    home_abbr = game_data.get('HOME_TEAM_ABBREVIATION', '')
                    away_abbr = game_data.get('VISITOR_TEAM_ABBREVIATION', '')
                    
                    # Create mapping keys
                    keys = [f"{home_abbr}-{away_abbr}", f"{away_abbr}-{home_abbr}"]
                    
                    game_data['SEARCH_DATE'] = check_date
                    game_data['SOURCE'] = 'NBA_STATS_API'
                    
                    for key in keys:
                        games_map[key] = game_data
                    
                    print(f"  Found: {home_abbr}-{away_abbr} (ID: {game_data.get('GAME_ID')})")
            
            if games_map:
                break  # Found games, no need to check other dates
                
        except Exception as e:
            print(f"  NBA stats API failed for {check_date}: {e}")
    
    # Method 2: Fallback to NBA CDN if stats API failed
    if not games_map:
        print("NBA stats API failed, trying CDN fallback...")
        try:
            # Generate NBA game IDs based on date and try CDN
            # NBA game IDs format: 00{season}{month}{day}{game_num}
            target_date = datetime.datetime.strptime(date_str, '%m/%d/%Y')
            season = target_date.year if target_date.month >= 10 else target_date.year - 1
            
            # Try a few possible game IDs (this is less reliable but worth trying)
            month_day = target_date.strftime('%m%d')
            season_suffix = str(season)[-2:]  # Last 2 digits of season
            
            for game_num in range(1, 16):  # Try up to 15 games per day
                potential_game_id = f"00{season_suffix}{month_day}{game_num:02d}"
                
                try:
                    cdn_url = NBA_CDN_BOXSCORE.format(game_id=potential_game_id)
                    cdn_data = http_get_json_robust(cdn_url, timeout=10, max_retries=1)
                    
                    if cdn_data and 'game' in cdn_data:
                        game = cdn_data['game']
                        home_team = game.get('homeTeam', {})
                        away_team = game.get('awayTeam', {})
                        
                        home_abbr = home_team.get('teamTricode', '')
                        away_abbr = away_team.get('teamTricode', '')
                        
                        if home_abbr and away_abbr:
                            game_data = {
                                'GAME_ID': potential_game_id,
                                'HOME_TEAM_ABBREVIATION': home_abbr,
                                'VISITOR_TEAM_ABBREVIATION': away_abbr,
                                'SEARCH_DATE': date_str,
                                'SOURCE': 'NBA_CDN'
                            }
                            
                            keys = [f"{home_abbr}-{away_abbr}", f"{away_abbr}-{home_abbr}"]
                            for key in keys:
                                games_map[key] = game_data
                            
                            print(f"  Found via CDN: {home_abbr}-{away_abbr} (ID: {potential_game_id})")
                
                except:
                    continue  # Try next game ID
                
                time.sleep(0.1)  # Small delay between CDN requests
                
        except Exception as e:
            print(f"CDN fallback failed: {e}")
    
    unique_games = len(set(game['GAME_ID'] for game in games_map.values() if 'GAME_ID' in game))
    print(f"Total NBA games found: {unique_games}")
    return games_map

def get_nba_player_stats_multi_source(nba_game_id: str) -> List[Dict[str, Any]]:
    """Get NBA player stats using multiple sources with fallbacks"""
    
    # Method 1: Try NBA stats API first
    print(f"  Trying NBA stats API for player data: {nba_game_id}")
    try:
        params = {
            'GameID': nba_game_id,
            'StartPeriod': '1',
            'EndPeriod': '10',
            'StartRange': '0',
            'EndRange': '55800',
            'RangeType': '2'
        }
        
        data = http_get_json_robust(NBA_BOXSCORE_TRADITIONAL, params=params, timeout=25, max_retries=2)
        
        if data and 'resultSets' in data:
            for result_set in data['resultSets']:
                if result_set['name'] == 'PlayerStats':
                    headers = result_set['headers']
                    rows = result_set['rowSet']
                    
                    player_stats = []
                    for row in rows:
                        player_data = dict(zip(headers, row))
                        
                        minutes = player_data.get('MIN')
                        if not minutes or minutes == '0:00':
                            continue
                        
                        player_stat = {
                            'nba_game_id': nba_game_id,
                            'team_id': safe_int(player_data.get('TEAM_ID')),
                            'team_abbr': player_data.get('TEAM_ABBREVIATION'),
                            'player_id': safe_int(player_data.get('PLAYER_ID')),
                            'player_name': player_data.get('PLAYER_NAME'),
                            'starter': player_data.get('START_POSITION') not in ['', None],
                            'minutes': minutes,
                            'pts': safe_int(player_data.get('PTS')),
                            'fgm': safe_int(player_data.get('FGM')),
                            'fga': safe_int(player_data.get('FGA')),
                            'fg_pct': safe_float(player_data.get('FG_PCT')),
                            'fg3m': safe_int(player_data.get('FG3M')),
                            'fg3a': safe_int(player_data.get('FG3A')),
                            'fg3_pct': safe_float(player_data.get('FG3_PCT')),
                            'ftm': safe_int(player_data.get('FTM')),
                            'fta': safe_int(player_data.get('FTA')),
                            'ft_pct': safe_float(player_data.get('FT_PCT')),
                            'oreb': safe_int(player_data.get('OREB')),
                            'dreb': safe_int(player_data.get('DREB')),
                            'reb': safe_int(player_data.get('REB')),
                            'ast': safe_int(player_data.get('AST')),
                            'stl': safe_int(player_data.get('STL')),
                            'blk': safe_int(player_data.get('BLK')),
                            'tov': safe_int(player_data.get('TO')),
                            'pf': safe_int(player_data.get('PF')),
                            'plus_minus': safe_int(player_data.get('PLUS_MINUS')),
                        }
                        
                        player_stats.append(player_stat)
                    
                    print(f"  ✓ NBA stats API: {len(player_stats)} players")
                    return player_stats
    
    except Exception as e:
        print(f"  NBA stats API failed: {e}")
    
    # Method 2: Fallback to NBA CDN
    print(f"  Trying NBA CDN for player data: {nba_game_id}")
    try:
        cdn_url = NBA_CDN_BOXSCORE.format(game_id=nba_game_id)
        data = http_get_json_robust(cdn_url, timeout=15, max_retries=2)
        
        if data and 'game' in data:
            game_data = data['game']
            home_team = game_data.get('homeTeam', {})
            away_team = game_data.get('awayTeam', {})
            
            player_stats = []
            
            for team in [home_team, away_team]:
                team_id = safe_int(team.get('teamId'))
                team_abbr = team.get('teamTricode')
                
                players = team.get('players', [])
                for player in players:
                    if not player.get('played'):
                        continue
                    
                    stats = player.get('statistics', {})
                    minutes = stats.get('minutes')
                    
                    if not minutes or minutes == 'PT0M':
                        continue
                    
                    player_stat = {
                        'nba_game_id': nba_game_id,
                        'team_id': team_id,
                        'team_abbr': team_abbr,
                        'player_id': safe_int(player.get('personId')),
                        'player_name': player.get('name', '').strip(),
                        'starter': bool(player.get('starter')),
                        'minutes': minutes,
                        'pts': safe_int(stats.get('points')),
                        'fgm': safe_int(stats.get('fieldGoalsMade')),
                        'fga': safe_int(stats.get('fieldGoalsAttempted')),
                        'fg_pct': safe_float(stats.get('fieldGoalsPercentage')),
                        'fg3m': safe_int(stats.get('threePointersMade')),
                        'fg3a': safe_int(stats.get('threePointersAttempted')),
                        'fg3_pct': safe_float(stats.get('threePointersPercentage')),
                        'ftm': safe_int(stats.get('freeThrowsMade')),
                        'fta': safe_int(stats.get('freeThrowsAttempted')),
                        'ft_pct': safe_float(stats.get('freeThrowsPercentage')),
                        'oreb': safe_int(stats.get('reboundsOffensive')),
                        'dreb': safe_int(stats.get('reboundsDefensive')),
                        'reb': safe_int(stats.get('reboundsTotal')),
                        'ast': safe_int(stats.get('assists')),
                        'stl': safe_int(stats.get('steals')),
                        'blk': safe_int(stats.get('blocks')),
                        'tov': safe_int(stats.get('turnovers')),
                        'pf': safe_int(stats.get('foulsPersonal')),
                        'plus_minus': safe_int(stats.get('plusMinusPoints')),
                    }
                    
                    player_stats.append(player_stat)
            
            print(f"  ✓ NBA CDN: {len(player_stats)} players")
            return player_stats
    
    except Exception as e:
        print(f"  NBA CDN failed: {e}")
    
    print(f"  ✗ All methods failed for {nba_game_id}")
    return []

# -----------------------------
# Robust Processing
# -----------------------------
def process_robust_game(espn_game: Dict[str, Any], nba_games_map: Dict[str, Dict[str, Any]]) -> tuple:
    """Process a game with robust error handling and multiple matching strategies"""
    espn_game_id = espn_game['espn_game_id']
    
    # Enhanced team abbreviation mapping
    abbr_mappings = {
        'SA': 'SAS', 'GS': 'GSW', 'NO': 'NOP', 'NY': 'NYK', 
        'UTAH': 'UTA', 'PHIL': 'PHI', 'PHX': 'PHX'
    }
    
    home_abbr = espn_game['home_team_abbr']
    away_abbr = espn_game['away_team_abbr']
    
    # Try all possible combinations
    possible_keys = [
        f"{home_abbr}-{away_abbr}",
        f"{away_abbr}-{home_abbr}",
        f"{abbr_mappings.get(home_abbr, home_abbr)}-{abbr_mappings.get(away_abbr, away_abbr)}",
        f"{abbr_mappings.get(away_abbr, away_abbr)}-{abbr_mappings.get(home_abbr, home_abbr)}"
    ]
    
    nba_game_data = None
    nba_game_id = None
    matched_key = None
    
    for key in possible_keys:
        if key in nba_games_map:
            nba_game_data = nba_games_map[key]
            nba_game_id = nba_game_data.get('GAME_ID')
            matched_key = key
            break
    
    # Prepare game data
    game_data = {
        'game_id': espn_game_id,
        'nba_game_id': nba_game_id,
        'game_date': espn_game['game_date'],
        'season': espn_game['season'],
        'home_team_id': ESPN_TO_NBA_TEAMS.get(espn_game['home_team_espn_id']),
        'home_team_abbr': espn_game['home_team_abbr'],
        'home_score': espn_game['home_score'],
        'away_team_id': ESPN_TO_NBA_TEAMS.get(espn_game['away_team_espn_id']),
        'away_team_abbr': espn_game['away_team_abbr'],
        'away_score': espn_game['away_score'],
        'game_status': espn_game['game_status'],
    }
    
    # Get player statistics
    player_stats = []
    if nba_game_id:
        source = nba_game_data.get('SOURCE', 'unknown')
        print(f"✓ Mapped {espn_game_id} → {nba_game_id} ({matched_key}, {source})")
        
        nba_player_stats = get_nba_player_stats_multi_source(nba_game_id)
        
        # Add ESPN metadata
        for stat in nba_player_stats:
            stat['game_id'] = espn_game_id
            stat['game_date'] = espn_game['game_date']
            stat['season'] = espn_game['season']
        
        player_stats = nba_player_stats
    else:
        print(f"✗ Could not map {espn_game_id} ({home_abbr}-{away_abbr})")
    
    return game_data, player_stats

def process_date_robust(date_yyyymmdd: str) -> tuple:
    """Process a date with maximum robustness"""
    print(f"\n=== ROBUST Processing {date_yyyymmdd} ===")
    
    # Get ESPN games
    espn_games = get_espn_games_for_date(date_yyyymmdd)
    
    if not espn_games:
        print(f"No completed games found for {date_yyyymmdd}")
        return pd.DataFrame(), pd.DataFrame()
    
    # Get NBA games with multi-source approach
    date_obj = datetime.datetime.strptime(date_yyyymmdd, '%Y%m%d')
    nba_date_str = date_obj.strftime('%m/%d/%Y')
    nba_games_map = get_nba_games_multi_source(nba_date_str)
    
    # Process each game
    all_games_data = []
    all_player_stats = []
