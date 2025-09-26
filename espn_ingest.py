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

# Get API key with fallback to free tier
API_KEY = os.environ.get('BALDONTLIE_API_KEY') or os.environ.get('BALLDONTLIE_API_KEY')

if API_KEY:
    print(f"Using Ball Don't Lie API with authentication")
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Authorization": f"Bearer {API_KEY}"
    }
else:
    print("No API key found, using Ball Don't Lie free tier")
    print("Note: Free tier has rate limits (30 requests per minute)")
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET = os.environ.get("BQ_DATASET", "nba_data")

SA_INFO = json.loads(os.environ["GCP_SA_KEY"])
CREDS = service_account.Credentials.from_service_account_info(SA_INFO)
BQ = bigquery.Client(project=PROJECT_ID, credentials=CREDS)

# Ball Don't Lie API endpoints
BALLDONTLIE_BASE = "https://api.balldontlie.io/v1"
BALLDONTLIE_GAMES = f"{BALLDONTLIE_BASE}/games"
BALLDONTLIE_STATS = f"{BALLDONTLIE_BASE}/stats"

# -----------------------------
# BigQuery schemas  
# -----------------------------
GAMES_SCHEMA = [
    bigquery.SchemaField("game_id", "INT64"),
    bigquery.SchemaField("game_date", "DATE"),
    bigquery.SchemaField("season", "INT64"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("home_team_id", "INT64"),
    bigquery.SchemaField("home_team_name", "STRING"),
    bigquery.SchemaField("home_team_abbr", "STRING"),
    bigquery.SchemaField("home_score", "INT64"),
    bigquery.SchemaField("away_team_id", "INT64"),
    bigquery.SchemaField("away_team_name", "STRING"),
    bigquery.SchemaField("away_team_abbr", "STRING"),
    bigquery.SchemaField("away_score", "INT64"),
]

BOX_SCHEMA = [
    bigquery.SchemaField("stat_id", "INT64"),
    bigquery.SchemaField("game_id", "INT64"),
    bigquery.SchemaField("game_date", "DATE"),
    bigquery.SchemaField("season", "INT64"),
    bigquery.SchemaField("team_id", "INT64"),
    bigquery.SchemaField("team_name", "STRING"),
    bigquery.SchemaField("team_abbr", "STRING"),
    bigquery.SchemaField("player_id", "INT64"),
    bigquery.SchemaField("player_name", "STRING"),
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
    bigquery.SchemaField("turnover", "INT64"),
    bigquery.SchemaField("pf", "INT64"),
    bigquery.SchemaField("plus_minus", "STRING"),
]

# -----------------------------
# BigQuery helpers
# -----------------------------
def ensure_dataset() -> None:
    ds_id = f"{PROJECT_ID}.{DATASET}"
    try:
        BQ.get_dataset(ds_id)
        print(f"Dataset {ds_id} exists")
    except Exception:
        dataset = bigquery.Dataset(ds_id)
        BQ.create_dataset(dataset)
        print(f"Created dataset {ds_id}")

def ensure_tables() -> None:
    ensure_dataset()

    games_table_id = f"{PROJECT_ID}.{DATASET}.games_daily"
    try:
        BQ.get_table(games_table_id)
        print("Games table exists")
    except Exception:
        table = bigquery.Table(games_table_id, schema=GAMES_SCHEMA)
        BQ.create_table(table)
        print("Created games table")

    box_table_id = f"{PROJECT_ID}.{DATASET}.player_boxscores"
    try:
        BQ.get_table(box_table_id)
        print("Player boxscores table exists")
    except Exception:
        table = bigquery.Table(box_table_id, schema=BOX_SCHEMA)
        BQ.create_table(table)
        print("Created player boxscores table")

def load_df(df: pd.DataFrame, table: str) -> None:
    if df is None or df.empty:
        print(f"No data to load for {table}")
        return
    
    table_id = f"{PROJECT_ID}.{DATASET}.{table}"
    schema = GAMES_SCHEMA if table == "games_daily" else BOX_SCHEMA

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    )
    
    job = BQ.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows to {table}")

# -----------------------------
# Utilities
# -----------------------------
def safe_int(x: Any) -> Optional[int]:
    try:
        return int(x) if x is not None and x != "" and str(x).strip() != "" else None
    except (ValueError, TypeError):
        return None

def safe_float(x: Any) -> Optional[float]:
    try:
        return float(x) if x is not None and x != "" and str(x).strip() != "" else None
    except (ValueError, TypeError):
        return None

def http_get_json(url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 30) -> Dict[str, Any]:
    """Make HTTP request to Ball Don't Lie API"""
    try:
        print(f"Making request: {url}")
        if params:
            print(f"  Params: {params}")
            
        response = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
        response.raise_for_status()
        
        data = response.json()
        print(f"  Response: {len(data.get('data', []))} items" if 'data' in data else "  Response received")
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"Request failed for {url}: {e}")
        return {}
    except json.JSONDecodeError as e:
        print(f"JSON decode error for {url}: {e}")
        return {}

# -----------------------------
# Ball Don't Lie API Functions
# -----------------------------
def get_games_for_date(date_str: str) -> List[Dict[str, Any]]:
    """
    Get games for a specific date using Ball Don't Lie API
    date_str format: YYYY-MM-DD
    """
    params = {
        'dates[]': date_str,
        'per_page': 100  # Should be enough for one day
    }
    
    data = http_get_json(BALLDONTLIE_GAMES, params=params)
    
    if not data or 'data' not in data:
        print(f"No games data returned for {date_str}")
        return []
    
    games = []
    for game in data['data']:
        # Only process completed games
        if game.get('status') != 'Final':
            continue
            
        game_info = {
            'game_id': safe_int(game.get('id')),
            'game_date': date_str,
            'season': safe_int(game.get('season')),
            'status': game.get('status'),
            'home_team_id': safe_int(game.get('home_team', {}).get('id')),
            'home_team_name': game.get('home_team', {}).get('full_name'),
            'home_team_abbr': game.get('home_team', {}).get('abbreviation'),
            'home_score': safe_int(game.get('home_team_score')),
            'away_team_id': safe_int(game.get('visitor_team', {}).get('id')),
            'away_team_name': game.get('visitor_team', {}).get('full_name'),
            'away_team_abbr': game.get('visitor_team', {}).get('abbreviation'),
            'away_score': safe_int(game.get('visitor_team_score')),
        }
        games.append(game_info)
    
    print(f"Found {len(games)} completed games for {date_str}")
    return games

def get_stats_for_game(game_id: int, game_date: str, season: int) -> List[Dict[str, Any]]:
    """
    Get player statistics for a specific game using Ball Don't Lie API
    """
    params = {
        'game_ids[]': game_id,
        'per_page': 100  # Should be enough for one game
    }
    
    data = http_get_json(BALLDONTLIE_STATS, params=params)
    
    if not data or 'data' not in data:
        print(f"No stats data returned for game {game_id}")
        return []
    
    player_stats = []
    for stat in data['data']:
        player_stat = {
            'stat_id': safe_int(stat.get('id')),
            'game_id': game_id,
            'game_date': game_date,
            'season': season,
            'team_id': safe_int(stat.get('team', {}).get('id')),
            'team_name': stat.get('team', {}).get('full_name'),
            'team_abbr': stat.get('team', {}).get('abbreviation'),
            'player_id': safe_int(stat.get('player', {}).get('id')),
            'player_name': f"{stat.get('player', {}).get('first_name', '')} {stat.get('player', {}).get('last_name', '')}".strip(),
            'minutes': stat.get('min'),
            'pts': safe_int(stat.get('pts')),
            'fgm': safe_int(stat.get('fgm')),
            'fga': safe_int(stat.get('fga')),
            'fg_pct': safe_float(stat.get('fg_pct')),
            'fg3m': safe_int(stat.get('fg3m')),
            'fg3a': safe_int(stat.get('fg3a')),
            'fg3_pct': safe_float(stat.get('fg3_pct')),
            'ftm': safe_int(stat.get('ftm')),
            'fta': safe_int(stat.get('fta')),
            'ft_pct': safe_float(stat.get('ft_pct')),
            'oreb': safe_int(stat.get('oreb')),
            'dreb': safe_int(stat.get('dreb')),
            'reb': safe_int(stat.get('reb')),
            'ast': safe_int(stat.get('ast')),
            'stl': safe_int(stat.get('stl')),
            'blk': safe_int(stat.get('blk')),
            'turnover': safe_int(stat.get('turnover')),
            'pf': safe_int(stat.get('pf')),
            'plus_minus': stat.get('plus_minus'),  # Keep as string since it can be "+5", "-3", etc.
        }
        player_stats.append(player_stat)
    
    print(f"Found {len(player_stats)} player stats for game {game_id}")
    return player_stats

def get_stats_for_date_paginated(date_str: str) -> List[Dict[str, Any]]:
    """
    Get all player statistics for a specific date with pagination
    This is more efficient than getting stats for each game individually
    """
    all_stats = []
    page = 1
    per_page = 100
    
    while True:
        params = {
            'dates[]': date_str,
            'per_page': per_page,
            'page': page
        }
        
        data = http_get_json(BALLDONTLIE_STATS, params=params)
        
        if not data or 'data' not in data:
            break
        
        stats_batch = data['data']
        if not stats_batch:
            break
        
        # Process this batch
        for stat in stats_batch:
            player_stat = {
                'stat_id': safe_int(stat.get('id')),
                'game_id': safe_int(stat.get('game', {}).get('id')),
                'game_date': date_str,
                'season': safe_int(stat.get('game', {}).get('season')),
                'team_id': safe_int(stat.get('team', {}).get('id')),
                'team_name': stat.get('team', {}).get('full_name'),
                'team_abbr': stat.get('team', {}).get('abbreviation'),
                'player_id': safe_int(stat.get('player', {}).get('id')),
                'player_name': f"{stat.get('player', {}).get('first_name', '')} {stat.get('player', {}).get('last_name', '')}".strip(),
                'minutes': stat.get('min'),
                'pts': safe_int(stat.get('pts')),
                'fgm': safe_int(stat.get('fgm')),
                'fga': safe_int(stat.get('fga')),
                'fg_pct': safe_float(stat.get('fg_pct')),
                'fg3m': safe_int(stat.get('fg3m')),
                'fg3a': safe_int(stat.get('fg3a')),
                'fg3_pct': safe_float(stat.get('fg3_pct')),
                'ftm': safe_int(stat.get('ftm')),
                'fta': safe_int(stat.get('fta')),
                'ft_pct': safe_float(stat.get('ft_pct')),
                'oreb': safe_int(stat.get('oreb')),
                'dreb': safe_int(stat.get('dreb')),
                'reb': safe_int(stat.get('reb')),
                'ast': safe_int(stat.get('ast')),
                'stl': safe_int(stat.get('stl')),
                'blk': safe_int(stat.get('blk')),
                'turnover': safe_int(stat.get('turnover')),
                'pf': safe_int(stat.get('pf')),
                'plus_minus': stat.get('plus_minus'),
            }
            all_stats.append(player_stat)
        
        # Check if we've got all pages
        meta = data.get('meta', {})
        if page >= meta.get('total_pages', 1):
            break
        
        page += 1
        time.sleep(0.1)  # Small delay between pages
    
    print(f"Found {len(all_stats)} total player stats for {date_str}")
    return all_stats

# -----------------------------
# Data Processing
# -----------------------------
def process_date(date_str: str) -> tuple:
    """
    Process a single date using Ball Don't Lie API
    date_str format: YYYY-MM-DD
    """
    print(f"\n=== Processing {date_str} with Ball Don't Lie API ===")
    
    # Get games for the date
    games = get_games_for_date(date_str)
    
    if not games:
        print(f"No completed games found for {date_str}")
        return pd.DataFrame(), pd.DataFrame()
    
    # Get all player stats for the date (more efficient than per-game requests)
    all_player_stats = get_stats_for_date_paginated(date_str)
    
    # Create DataFrames
    games_df = pd.DataFrame(games) if games else pd.DataFrame()
    players_df = pd.DataFrame(all_player_stats) if all_player_stats else pd.DataFrame()
    
    # Apply type coercion for games
    if not games_df.empty:
        games_df["game_date"] = pd.to_datetime(games_df["game_date"]).dt.date
        
        int_cols = ["game_id", "season", "home_team_id", "home_score", "away_team_id", "away_score"]
        for col in int_cols:
            if col in games_df.columns:
                games_df[col] = pd.to_numeric(games_df[col], errors="coerce").astype("Int64")
        
        str_cols = ["status", "home_team_name", "home_team_abbr", "away_team_name", "away_team_abbr"]
        for col in str_cols:
            if col in games_df.columns:
                games_df[col] = games_df[col].astype("string")
    
    # Apply type coercion for player stats
    if not players_df.empty:
        players_df["game_date"] = pd.to_datetime(players_df["game_date"]).dt.date
        
        int_cols = [
            "stat_id", "game_id", "season", "team_id", "player_id", "pts", "fgm", "fga", 
            "fg3m", "fg3a", "ftm", "fta", "oreb", "dreb", "reb", "ast", "stl", "blk", 
            "turnover", "pf"
        ]
        for col in int_cols:
            if col in players_df.columns:
                players_df[col] = pd.to_numeric(players_df[col], errors="coerce").astype("Int64")
        
        float_cols = ["fg_pct", "fg3_pct", "ft_pct"]
        for col in float_cols:
            if col in players_df.columns:
                players_df[col] = pd.to_numeric(players_df[col], errors="coerce").astype("Float64")
        
        str_cols = ["team_name", "team_abbr", "player_name", "minutes", "plus_minus"]
        for col in str_cols:
            if col in players_df.columns:
                players_df[col] = players_df[col].astype("string")
    
    print(f"COMPLETED {date_str}: {len(games_df)} games, {len(players_df)} player stats")
    return games_df, players_df

# -----------------------------
# Main orchestration
# -----------------------------
def ingest_dates(date_list: List[str]) -> None:
    """Process multiple dates"""
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
            
            # Rate limiting - Ball Don't Lie has limits
            time.sleep(1.0)
            
        except Exception as e:
            print(f"ERROR processing date {date_str}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # Upload to BigQuery
    print(f"\n=== Uploading to BigQuery ===")
    
    if all_games_frames:
        final_games_df = pd.concat(all_games_frames, ignore_index=True)
        print(f"Uploading {len(final_games_df)} game rows")
        load_df(final_games_df, "games_daily")
    else:
        print("No games data to upload")
    
    if all_box_frames:
        final_players_df = pd.concat(all_box_frames, ignore_index=True)
        print(f"Uploading {len(final_players_df)} player box score rows")
        load_df(final_players_df, "player_boxscores")
    else:
        print("No player box score data to upload")
    
    print(f"\n=== SUMMARY ===")
    print(f"Dates processed: {len(date_list)}")
    print(f"Total games: {total_games}")
    print(f"Total player stats: {total_players}")

def date_range_to_list(start_date: datetime.date, end_date: datetime.date) -> List[str]:
    """Convert date range to list of YYYY-MM-DD strings"""
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current.strftime('%Y-%m-%d'))
        current += datetime.timedelta(days=1)
    return dates

def main() -> None:
    parser = argparse.ArgumentParser(description="NBA data ingestion using Ball Don't Lie API")
    parser.add_argument("--mode", choices=["backfill", "daily"], required=True)
    parser.add_argument("--start", help="YYYY-MM-DD start date for backfill")
    parser.add_argument("--end", help="YYYY-MM-DD end date for backfill")
    args = parser.parse_args()

    print("=== Ball Don't Lie NBA Data Pipeline ===")

    if args.mode == "daily":
        yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"Running daily mode for {yesterday}")
        ingest_dates([yesterday])
        
    elif args.mode == "backfill":
        if not args.start or not args.end:
            print("ERROR: backfill mode requires --start and --end dates")
            sys.exit(1)
        
        try:
            start_date = datetime.date.fromisoformat(args.start)
            end_date = datetime.date.fromisoformat(args.end)
        except ValueError:
            print("ERROR: dates must be in YYYY-MM-DD format")
            sys.exit(1)
        
        if end_date < start_date:
            print("ERROR: end date must be >= start date")
            sys.exit(1)
        
        dates = date_range_to_list(start_date, end_date)
        print(f"Running backfill for {len(dates)} dates: {args.start} to {args.end}")
        ingest_dates(dates)

    print("Pipeline completed successfully!")

if __name__ == "__main__":
    main()
