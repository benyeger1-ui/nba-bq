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

# NBA.com endpoints (for player data)
NBA_SCOREBOARD = "https://stats.nba.com/stats/scoreboardv2"
NBA_BOXSCORE = "https://stats.nba.com/stats/boxscoretraditionalv2"

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

def http_get_json(url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 15) -> Dict[str, Any]:
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Request failed for {url}: {e}")
        return {}

# -----------------------------
# ESPN Game Discovery
# -----------------------------
def get_espn_games_for_date(date_yyyymmdd: str) -> List[Dict[str, Any]]:
    """Get games using ESPN scoreboard (reliable for game discovery)"""
    try:
        data = http_get_json(ESPN_SCOREBOARD, params={"dates": date_yyyymmdd})
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
# NBA.com Game Mapping and Data
# -----------------------------
def get_nba_games_for_date(date_str: str) -> Dict[str, Dict[str, Any]]:
    """
    Get NBA.com games for a date and create a mapping key
    date_str format: MM/DD/YYYY
    Returns: {team_matchup_key: nba_game_data}
    """
    params = {
        'GameDate': date_str,
        'LeagueID': '00',
        'DayOffset': '0'
    }
    
    try:
        data = http_get_json(NBA_SCOREBOARD, params=params)
        games_map = {}
        
        if 'resultSets' in data and len(data['resultSets']) > 0:
            headers = data['resultSets'][0]['headers']
            rows = data['resultSets'][0]['rowSet']
            
            for row in rows:
                game_data = dict(zip(headers, row))
                
                # Create a unique key based on teams (for matching with ESPN)
                home_abbr = game_data.get('HOME_TEAM_ABBREVIATION', '')
                away_abbr = game_data.get('VISITOR_TEAM_ABBREVIATION', '')
                
                # Create both possible matchup keys (home-away and away-home)
                key1 = f"{home_abbr}-{away_abbr}"
                key2 = f"{away_abbr}-{home_abbr}"
                
                games_map[key1] = game_data
                games_map[key2] = game_data
        
        print(f"Found {len(games_map)//2} NBA games for {date_str}")
        return games_map
        
    except Exception as e:
        print(f"Error getting NBA games for {date_str}: {e}")
        return {}

def get_nba_boxscore_traditional(nba_game_id: str) -> List[Dict[str, Any]]:
    """Get player statistics from NBA.com boxscore API"""
    params = {
        'GameID': nba_game_id,
        'StartPeriod': '1',
        'EndPeriod': '10',
        'StartRange': '0',
        'EndRange': '55800',
        'RangeType': '2'
    }
    
    try:
        data = http_get_json(NBA_BOXSCORE, params=params)
        player_stats = []
        
        if not data or 'resultSets' not in data:
            print(f"No boxscore data for NBA game {nba_game_id}")
            return player_stats
        
        # Find PlayerStats result set
        for result_set in data['resultSets']:
            if result_set['name'] == 'PlayerStats':
                headers = result_set['headers']
                rows = result_set['rowSet']
                
                for row in rows:
                    player_data = dict(zip(headers, row))
                    
                    # Skip if no minutes played
                    minutes = player_data.get('MIN')
                    if not minutes or minutes == '0:00' or minutes is None:
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
                
                break  # Found PlayerStats, no need to continue
        
        print(f"Extracted {len(player_stats)} player stats from NBA.com for game {nba_game_id}")
        return player_stats
        
    except Exception as e:
        print(f"Error getting NBA boxscore for {nba_game_id}: {e}")
        return []

# -----------------------------
# Hybrid Processing
# -----------------------------
def process_hybrid_game(espn_game: Dict[str, Any], nba_games_map: Dict[str, Dict[str, Any]]) -> tuple:
    """
    Process a game using ESPN for basic info and NBA.com for player stats
    """
    espn_game_id = espn_game['espn_game_id']
    
    # Try to find matching NBA game
    home_abbr = espn_game['home_team_abbr']
    away_abbr = espn_game['away_team_abbr']
    matchup_key = f"{home_abbr}-{away_abbr}"
    
    nba_game_data = nba_games_map.get(matchup_key)
    nba_game_id = None
    
    if nba_game_data:
        nba_game_id = nba_game_data.get('GAME_ID')
        print(f"✓ Mapped ESPN game {espn_game_id} to NBA game {nba_game_id} ({matchup_key})")
    else:
        print(f"✗ Could not map ESPN game {espn_game_id} ({matchup_key}) to NBA game")
    
    # Prepare game data with both IDs
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
    
    # Get player statistics from NBA.com if we have the NBA game ID
    player_stats = []
    if nba_game_id:
        nba_player_stats = get_nba_boxscore_traditional(nba_game_id)
        
        # Add ESPN and date info to NBA player stats
        for stat in nba_player_stats:
            stat['game_id'] = espn_game_id
            stat['game_date'] = espn_game['game_date']
            stat['season'] = espn_game['season']
        
        player_stats = nba_player_stats
    
    return game_data, player_stats

def process_date_hybrid(date_yyyymmdd: str) -> tuple:
    """Process a date using hybrid ESPN + NBA.com approach"""
    print(f"\n=== Hybrid Processing {date_yyyymmdd} ===")
    
    # Step 1: Get ESPN games for discovery
    espn_games = get_espn_games_for_date(date_yyyymmdd)
    
    if not espn_games:
        print(f"No completed games found for {date_yyyymmdd}")
        return pd.DataFrame(), pd.DataFrame()
    
    # Step 2: Get NBA games for the same date (for mapping)
    date_obj = datetime.datetime.strptime(date_yyyymmdd, '%Y%m%d')
    nba_date_str = date_obj.strftime('%m/%d/%Y')
    nba_games_map = get_nba_games_for_date(nba_date_str)
    
    # Step 3: Process each ESPN game with NBA data
    all_games_data = []
    all_player_stats = []
    
    for espn_game in espn_games:
        try:
            game_data, player_stats = process_hybrid_game(espn_game, nba_games_map)
            all_games_data.append(game_data)
            all_player_stats.extend(player_stats)
            
            time.sleep(0.6)  # Respectful delay for NBA.com
            
        except Exception as e:
            print(f"Error processing game {espn_game['espn_game_id']}: {e}")
            import traceback
            traceback.print_exc()
    
    # Step 4: Create DataFrames
    games_df = pd.DataFrame(all_games_data) if all_games_data else pd.DataFrame()
    players_df = pd.DataFrame(all_player_stats) if all_player_stats else pd.DataFrame()
    
    # Apply type coercion
    games_df = coerce_games_dtypes(games_df)
    players_df = coerce_box_dtypes(players_df)
    
    print(f"COMPLETED {date_yyyymmdd}: {len(games_df)} games, {len(players_df)} player stats")
    return games_df, players_df

# -----------------------------
# Data type coercion
# -----------------------------
def coerce_games_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()

    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce").dt.date

    int_cols = ["season", "home_team_id", "home_score", "away_team_id", "away_score"]
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    str_cols = ["game_id", "nba_game_id", "home_team_abbr", "away_team_abbr", "game_status"]
    for c in str_cols:
        if c in df.columns and is_object_dtype(df[c]):
            df[c] = df[c].astype("string")

    return df

def coerce_box_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()

    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce").dt.date

    if "starter" in df.columns:
        df["starter"] = df["starter"].astype("boolean")

    int_cols = [
        "season", "team_id", "player_id", "pts", "fgm", "fga", "fg3m", "fg3a", 
        "ftm", "fta", "oreb", "dreb", "reb", "ast", "stl", "blk", "tov", "pf", "plus_minus"
    ]
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    float_cols = ["fg_pct", "fg3_pct", "ft_pct"]
    for c in float_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Float64")

    str_cols = ["game_id", "nba_game_id", "team_abbr", "player_name", "minutes"]
    for c in str_cols:
        if c in df.columns and is_object_dtype(df[c]):
            df[c] = df[c].astype("string")

    return df

# -----------------------------
# Main orchestration
# -----------------------------
def ingest_dates_hybrid(ymd_list: List[str]) -> None:
    """Process multiple dates using hybrid approach"""
    ensure_tables()
    
    all_games_frames = []
    all_box_frames = []
    
    total_games = 0
    total_players = 0
    
    print(f"Processing {len(ymd_list)} dates using HYBRID ESPN + NBA.com approach...")
    
    for ymd in ymd_list:
        try:
            games_df, players_df = process_date_hybrid(ymd)
            
            if not games_df.empty:
                all_games_frames.append(games_df)
                total_games += len(games_df)
            
            if not players_df.empty:
                all_box_frames.append(players_df)
                total_players += len(players_df)
            
            time.sleep(1.0)  # Longer delay between dates for NBA.com
            
        except Exception as e:
            print(f"ERROR processing date {ymd}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # Upload to BigQuery
    print(f"\n=== Uploading to BigQuery ===")
    
    if all_games_frames:
        final_games_df = pd.concat(all_games_frames, ignore_index=True)
        print(f"Loading {len(final_games_df)} game rows")
        load_df(final_games_df, "games_daily")
    else:
        print("No games data to load")
    
    if all_box_frames:
        final_players_df = pd.concat(all_box_frames, ignore_index=True)
        print(f"Loading {len(final_players_df)} player box score rows")
        load_df(final_players_df, "player_boxscores")
    else:
        print("No player box score data to load")
    
    print(f"\n=== FINAL SUMMARY ===")
    print(f"Dates processed: {len(ymd_list)}")
    print(f"Total games: {total_games}")
    print(f"Total player stats: {total_players}")

def yyyymmdd_list(start: datetime.date, end: datetime.date) -> List[str]:
    out = []
    cur = start
    while cur <= end:
        out.append(cur.strftime("%Y%m%d"))
        cur += datetime.timedelta(days=1)
    return out

def main() -> None:
    parser = argparse.ArgumentParser(description="Hybrid NBA data ingestion using ESPN + NBA.com APIs")
    parser.add_argument("--mode", choices=["backfill", "daily"], required=True)
    parser.add_argument("--start", help="YYYY-MM-DD inclusive start for backfill")
    parser.add_argument("--end", help="YYYY-MM-DD inclusive end for backfill")
    args = parser.parse_args()

    if args.mode == "daily":
        yday = datetime.date.today() - datetime.timedelta(days=1)
        ymd = yday.strftime("%Y%m%d")
        print(f"Running HYBRID daily ingest for {ymd}")
        ingest_dates_hybrid([ymd])
        print(f"Daily ingest complete for {ymd}")
        return

    if args.mode == "backfill":
        if not args.start or not args.end:
            print("Error - backfill needs --start and --end like 2024-10-22 and 2024-10-28")
            sys.exit(1)
        try:
            s = datetime.date.fromisoformat(args.start)
            e = datetime.date.fromisoformat(args.end)
        except Exception:
            print("Error - invalid date format. Use YYYY-MM-DD")
            sys.exit(1)
        if e < s:
            print("Error - end date must be on or after start date")
            sys.exit(1)
        
        dates = yyyymmdd_list(s, e)
        print(f"Running HYBRID backfill from {args.start} to {args.end} ({len(dates)} dates)")
        ingest_dates_hybrid(dates)
        print(f"Backfill complete for {args.start} to {args.end}")
        return

if __name__ == "__main__":
    main()
