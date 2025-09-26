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
from urllib.parse import urlencode


# -----------------------------
# Config via environment
# -----------------------------

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com"
}

PROJECT_ID = os.environ["GCP_PROJECT_ID"]  # set in GitHub Actions secrets
DATASET = os.environ.get("BQ_DATASET", "nba_data")

# Service account JSON is stored as a single secret string
SA_INFO = json.loads(os.environ["GCP_SA_KEY"])
CREDS = service_account.Credentials.from_service_account_info(SA_INFO)
BQ = bigquery.Client(project=PROJECT_ID, credentials=CREDS)

# NBA.com Official APIs - More reliable than ESPN
NBA_SCOREBOARD = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
NBA_GAMES_DATE = "https://stats.nba.com/stats/scoreboardv2"
NBA_BOXSCORE = "https://stats.nba.com/stats/boxscoretraditionalv2"
NBA_LIVE_BOXSCORE = "https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"

# Alternative ESPN endpoints that still work
ESPN_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"

# -----------------------------
# BigQuery schemas
# -----------------------------
GAMES_SCHEMA = [
    bigquery.SchemaField("game_id", "STRING"),
    bigquery.SchemaField("game_date", "DATE"),
    bigquery.SchemaField("season", "STRING"),
    bigquery.SchemaField("home_team_id", "INT64"),
    bigquery.SchemaField("home_team_abbr", "STRING"),
    bigquery.SchemaField("home_team_name", "STRING"),
    bigquery.SchemaField("home_score", "INT64"),
    bigquery.SchemaField("away_team_id", "INT64"),
    bigquery.SchemaField("away_team_abbr", "STRING"),
    bigquery.SchemaField("away_team_name", "STRING"),
    bigquery.SchemaField("away_score", "INT64"),
    bigquery.SchemaField("game_status", "STRING"),
    bigquery.SchemaField("season_type", "STRING"),
]

BOX_SCHEMA = [
    bigquery.SchemaField("game_id", "STRING"),
    bigquery.SchemaField("game_date", "DATE"),
    bigquery.SchemaField("season", "STRING"),
    bigquery.SchemaField("team_id", "INT64"),
    bigquery.SchemaField("team_abbr", "STRING"),
    bigquery.SchemaField("team_name", "STRING"),
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

    # drop rows without a date just in case
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

def http_get_json(url: str, params: Optional[Dict[str, Any]] = None, max_retries: int = 3) -> Dict[str, Any]:
    last_err = None
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            print(f"Attempt {attempt + 1} failed for URL {url}: {e}")
            if attempt < max_retries - 1:
                time.sleep(1.0 * (attempt + 1))  # Exponential backoff
    raise last_err if last_err else RuntimeError("request failed")

# -----------------------------
# NBA.com API Functions
# -----------------------------
def get_games_for_date(date_str: str) -> List[Dict[str, Any]]:
    """
    Get games for a specific date using NBA.com API
    date_str format: MM/DD/YYYY
    """
    params = {
        'GameDate': date_str,
        'LeagueID': '00',
        'DayOffset': '0'
    }
    
    try:
        data = http_get_json(NBA_GAMES_DATE, params=params)
        games = []
        
        if 'resultSets' in data and len(data['resultSets']) > 0:
            headers = data['resultSets'][0]['headers']
            rows = data['resultSets'][0]['rowSet']
            
            for row in rows:
                game_data = dict(zip(headers, row))
                games.append({
                    'game_id': game_data.get('GAME_ID'),
                    'game_date': game_data.get('GAME_DATE_EST'),
                    'home_team_id': game_data.get('HOME_TEAM_ID'),
                    'home_team_abbr': game_data.get('HOME_TEAM_ABBREVIATION'),
                    'home_team_name': game_data.get('HOME_TEAM_NAME'),
                    'away_team_id': game_data.get('VISITOR_TEAM_ID'),
                    'away_team_abbr': game_data.get('VISITOR_TEAM_ABBREVIATION'),
                    'away_team_name': game_data.get('VISITOR_TEAM_NAME'),
                    'season': game_data.get('SEASON'),
                    'season_type': 'Regular Season'  # Can be enhanced
                })
        
        print(f"Found {len(games)} games for {date_str}")
        return games
        
    except Exception as e:
        print(f"Error getting games for {date_str}: {e}")
        return []

def get_boxscore_traditional(game_id: str) -> Dict[str, Any]:
    """
    Get traditional box score using NBA.com API
    """
    params = {
        'GameID': game_id,
        'StartPeriod': '1',
        'EndPeriod': '10',
        'StartRange': '0',
        'EndRange': '55800',
        'RangeType': '2'
    }
    
    try:
        data = http_get_json(NBA_BOXSCORE, params=params)
        return data
    except Exception as e:
        print(f"Error getting boxscore for {game_id}: {e}")
        return {}

def get_live_boxscore(game_id: str) -> Dict[str, Any]:
    """
    Get live boxscore from NBA CDN (alternative method)
    """
    url = NBA_LIVE_BOXSCORE.format(game_id=game_id)
    try:
        data = http_get_json(url)
        return data
    except Exception as e:
        print(f"Error getting live boxscore for {game_id}: {e}")
        return {}

def parse_nba_boxscore(game_info: Dict[str, Any], boxscore_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Parse NBA.com boxscore data into player statistics
    """
    player_stats = []
    
    if not boxscore_data or 'resultSets' not in boxscore_data:
        print(f"No boxscore data for game {game_info.get('game_id')}")
        return player_stats
    
    # NBA.com API returns multiple result sets
    # PlayerStats is usually in resultSets[0]
    for result_set in boxscore_data['resultSets']:
        if result_set['name'] == 'PlayerStats':
            headers = result_set['headers']
            rows = result_set['rowSet']
            
            for row in rows:
                player_data = dict(zip(headers, row))
                
                # Skip if no minutes played (DNP)
                minutes = player_data.get('MIN')
                if not minutes or minutes == '0:00':
                    continue
                
                player_stat = {
                    'game_id': game_info['game_id'],
                    'game_date': game_info['game_date'],
                    'season': game_info['season'],
                    'team_id': safe_int(player_data.get('TEAM_ID')),
                    'team_abbr': player_data.get('TEAM_ABBREVIATION'),
                    'team_name': player_data.get('TEAM_NAME'),
                    'player_id': safe_int(player_data.get('PLAYER_ID')),
                    'player_name': player_data.get('PLAYER_NAME'),
                    'starter': player_data.get('START_POSITION') != '',
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
    
    print(f"Parsed {len(player_stats)} player stats for game {game_info.get('game_id')}")
    return player_stats

def parse_live_boxscore(game_info: Dict[str, Any], live_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Parse NBA CDN live boxscore data (fallback method)
    """
    player_stats = []
    
    if not live_data or 'game' not in live_data:
        return player_stats
    
    game_data = live_data['game']
    home_team = game_data.get('homeTeam', {})
    away_team = game_data.get('awayTeam', {})
    
    for team in [home_team, away_team]:
        team_id = safe_int(team.get('teamId'))
        team_abbr = team.get('teamTricode')
        team_name = team.get('teamName')
        
        players = team.get('players', [])
        for player in players:
            if not player.get('played'):
                continue
                
            stats = player.get('statistics', {})
            
            player_stat = {
                'game_id': game_info['game_id'],
                'game_date': game_info['game_date'],
                'season': game_info['season'],
                'team_id': team_id,
                'team_abbr': team_abbr,
                'team_name': team_name,
                'player_id': safe_int(player.get('personId')),
                'player_name': player.get('name'),
                'starter': bool(player.get('starter')),
                'minutes': stats.get('minutes'),
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
    
    print(f"Parsed {len(player_stats)} player stats from live boxscore for game {game_info.get('game_id')}")
    return player_stats

# -----------------------------
# Data type coercion
# -----------------------------
def coerce_games_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()

    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce").dt.date

    int_cols = ["home_team_id", "home_score", "away_team_id", "away_score"]
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    str_cols = ["game_id", "season", "home_team_abbr", "home_team_name", "away_team_abbr", "away_team_name", "game_status", "season_type"]
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
        "team_id", "player_id", "pts", "fgm", "fga", "fg3m", "fg3a", 
        "ftm", "fta", "oreb", "dreb", "reb", "ast", "stl", "blk", "tov", "pf", "plus_minus"
    ]
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    float_cols = ["fg_pct", "fg3_pct", "ft_pct"]
    for c in float_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Float64")

    str_cols = ["game_id", "season", "team_abbr", "team_name", "player_name", "minutes"]
    for c in str_cols:
        if c in df.columns and is_object_dtype(df[c]):
            df[c] = df[c].astype("string")

    return df

# -----------------------------
# Main orchestration
# -----------------------------
def process_date(date_str: str) -> tuple:
    """
    Process all games and player stats for a specific date
    Returns: (games_df, players_df)
    """
    print(f"\n=== Processing date {date_str} ===")
    
    # Convert YYYY-MM-DD to MM/DD/YYYY for NBA API
    date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    nba_date_str = date_obj.strftime('%m/%d/%Y')
    
    games = get_games_for_date(nba_date_str)
    
    games_data = []
    all_player_stats = []
    
    for game in games:
        print(f"\n--- Processing game {game['game_id']} ---")
        
        # Prepare game data
        game_row = {
            'game_id': game['game_id'],
            'game_date': date_str,
            'season': game['season'],
            'home_team_id': game['home_team_id'],
            'home_team_abbr': game['home_team_abbr'],
            'home_team_name': game['home_team_name'],
            'home_score': None,  # Will be filled after getting boxscore
            'away_team_id': game['away_team_id'],
            'away_team_abbr': game['away_team_abbr'],
            'away_team_name': game['away_team_name'],
            'away_score': None,  # Will be filled after getting boxscore
            'game_status': 'Final',  # Default, can be enhanced
            'season_type': game['season_type'],
        }
        
        # Get player statistics
        try:
            # Method 1: NBA.com traditional boxscore
            boxscore_data = get_boxscore_traditional(game['game_id'])
            player_stats = parse_nba_boxscore(game, boxscore_data)
            
            if not player_stats:
                # Method 2: NBA CDN live boxscore
                print(f"Traditional boxscore failed, trying live boxscore for {game['game_id']}")
                live_data = get_live_boxscore(game['game_id'])
                player_stats = parse_live_boxscore(game, live_data)
            
            if player_stats:
                all_player_stats.extend(player_stats)
                print(f"SUCCESS: Found {len(player_stats)} player stats for game {game['game_id']}")
            else:
                print(f"WARNING: No player stats found for game {game['game_id']}")
            
        except Exception as e:
            print(f"ERROR processing player stats for game {game['game_id']}: {e}")
        
        games_data.append(game_row)
        
        # Rate limiting
        time.sleep(0.6)  # NBA.com is more restrictive
    
    # Create DataFrames
    games_df = pd.DataFrame(games_data) if games_data else pd.DataFrame(columns=[f.name for f in GAMES_SCHEMA])
    players_df = pd.DataFrame(all_player_stats) if all_player_stats else pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])
    
    return coerce_games_dtypes(games_df), coerce_box_dtypes(players_df)

def ingest_dates(date_list: List[str]) -> None:
    """
    Process multiple dates and upload to BigQuery
    """
    ensure_tables()
    
    all_games_frames = []
    all_box_frames = []
    
    total_games = 0
    total_players = 0
    
    for date_str in date_list:
        try:
            games_df, players_df = process_date(date_str)
            
            if not games_df.empty:
                all_games_frames.append(games_df)
                total_games += len(games_df)
            
            if not players_df.empty:
                all_box_frames.append(players_df)
                total_players += len(players_df)
            
            # Daily rate limiting
            time.sleep(1.0)
            
        except Exception as e:
            print(f"ERROR processing date {date_str}: {e}")
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
    print(f"Dates processed: {len(date_list)}")
    print(f"Total games: {total_games}")
    print(f"Total player stats: {total_players}")

def yyyymmdd_to_date_list(start_yyyymmdd: str, end_yyyymmdd: str) -> List[str]:
    """Convert YYYYMMDD range to YYYY-MM-DD list"""
    start_date = datetime.datetime.strptime(start_yyyymmdd, '%Y%m%d').date()
    end_date = datetime.datetime.strptime(end_yyyymmdd, '%Y%m%d').date()
    
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.strftime('%Y-%m-%d'))
        current_date += datetime.timedelta(days=1)
    
    return date_list

def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest NBA games and player box scores using NBA.com official API")
    parser.add_argument("--mode", choices=["backfill", "daily"], required=True, help="backfill for a date range, daily for yesterday")
    parser.add_argument("--start", help="YYYY-MM-DD inclusive start for backfill")
    parser.add_argument("--end", help="YYYY-MM-DD inclusive end for backfill")
    args = parser.parse_args()

    if args.mode == "daily":
        yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        print(f"Running daily ingest for {yesterday}")
        ingest_dates([yesterday])
        print(f"Daily ingest complete for {yesterday}")
        return

    if args.mode == "backfill":
        if not args.start or not args.end:
            print("Error - backfill needs --start and --end like 2024-10-01 and 2024-10-28")
            sys.exit(1)
        try:
            # Validate date format
            datetime.date.fromisoformat(args.start)
            datetime.date.fromisoformat(args.end)
        except Exception:
            print("Error - invalid date format. Use YYYY-MM-DD")
            sys.exit(1)
        
        date_list = []
        start_date = datetime.date.fromisoformat(args.start)
        end_date = datetime.date.fromisoformat(args.end)
        
        current_date = start_date
        while current_date <= end_date:
            date_list.append(current_date.strftime('%Y-%m-%d'))
            current_date += datetime.timedelta(days=1)
        
        print(f"Running backfill from {args.start} to {args.end} ({len(date_list)} dates)")
        ingest_dates(date_list)
        print(f"Backfill complete for {args.start} to {args.end}")
        return

if __name__ == "__main__":
    main()
