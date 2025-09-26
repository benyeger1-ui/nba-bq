#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import argparse
import datetime
from typing import List, Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from pandas.api.types import is_object_dtype
import requests

from google.cloud import bigquery
from google.oauth2 import service_account


# -----------------------------
# Config via environment
# -----------------------------

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET = os.environ.get("BQ_DATASET", "nba_data")

SA_INFO = json.loads(os.environ["GCP_SA_KEY"])
CREDS = service_account.Credentials.from_service_account_info(SA_INFO)
BQ = bigquery.Client(project=PROJECT_ID, credentials=CREDS)

# Fast ESPN endpoints (still working for basic data)
ESPN_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
ESPN_SUMMARY = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary"

# Fast NBA CDN endpoints (no rate limits)
NBA_LIVE_BOXSCORE = "https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"

# -----------------------------
# BigQuery schemas
# -----------------------------
GAMES_SCHEMA = [
    bigquery.SchemaField("game_id", "STRING"),
    bigquery.SchemaField("game_date", "DATE"),
    bigquery.SchemaField("season", "STRING"),
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
    bigquery.SchemaField("game_date", "DATE"),
    bigquery.SchemaField("season", "STRING"),
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

def http_get_json(url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 10) -> Dict[str, Any]:
    """Fast HTTP request with minimal retries"""
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Request failed for {url}: {e}")
        return {}

# -----------------------------
# Fast data extraction
# -----------------------------
def get_games_for_date_fast(date_yyyymmdd: str) -> List[Dict[str, Any]]:
    """
    FAST: Get games using ESPN scoreboard (still works and fast)
    """
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
                    'game_id': event['id'],
                    'game_date': event['date'][:10],  # YYYY-MM-DD
                    'season': event.get('season', {}).get('year', '2025'),
                    'home_team_id': safe_int(home_team.get('team', {}).get('id')),
                    'home_team_abbr': home_team.get('team', {}).get('abbreviation'),
                    'home_score': safe_int(home_team.get('score')),
                    'away_team_id': safe_int(away_team.get('team', {}).get('id')),
                    'away_team_abbr': away_team.get('team', {}).get('abbreviation'),
                    'away_score': safe_int(away_team.get('score')),
                    'game_status': status,
                }
                games.append(game)
        
        print(f"Found {len(games)} completed games for {date_yyyymmdd}")
        return games
        
    except Exception as e:
        print(f"Error getting games for {date_yyyymmdd}: {e}")
        return []

def extract_player_stats_fast(game_id: str, game_date: str, season: str) -> List[Dict[str, Any]]:
    """
    FAST: Extract player stats using NBA CDN (no rate limits, very fast)
    """
    url = NBA_LIVE_BOXSCORE.format(game_id=game_id)
    
    try:
        data = http_get_json(url, timeout=5)  # Short timeout for speed
        
        if not data or 'game' not in data:
            print(f"No NBA CDN data for game {game_id}")
            return []
        
        game_data = data['game']
        home_team = game_data.get('homeTeam', {})
        away_team = game_data.get('awayTeam', {})
        
        player_stats = []
        
        for team in [home_team, away_team]:
            team_id = safe_int(team.get('teamId'))
            team_abbr = team.get('teamTricode')
            
            players = team.get('players', [])
            for player in players:
                # Skip players who didn't play
                if not player.get('played'):
                    continue
                
                stats = player.get('statistics', {})
                
                # Skip if no minutes
                minutes = stats.get('minutes')
                if not minutes or minutes == 'PT0M':
                    continue
                
                player_stat = {
                    'game_id': game_id,
                    'game_date': game_date,
                    'season': season,
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
        
        print(f"Extracted {len(player_stats)} player stats for game {game_id}")
        return player_stats
        
    except Exception as e:
        print(f"Error extracting player stats for game {game_id}: {e}")
        return []

def process_game_parallel(game: Dict[str, Any]) -> tuple:
    """
    Process a single game (for parallel execution)
    Returns: (game_dict, player_stats_list)
    """
    game_id = game['game_id']
    
    # Extract player stats
    player_stats = extract_player_stats_fast(
        game_id, 
        game['game_date'], 
        str(game['season'])
    )
    
    return game, player_stats

def process_date_fast(date_yyyymmdd: str) -> tuple:
    """
    OPTIMIZED: Process a date with parallel game processing
    """
    print(f"\n=== FAST Processing {date_yyyymmdd} ===")
    
    # Get games quickly
    games = get_games_for_date_fast(date_yyyymmdd)
    
    if not games:
        print(f"No completed games found for {date_yyyymmdd}")
        return pd.DataFrame(), pd.DataFrame()
    
    print(f"Processing {len(games)} games in parallel...")
    
    # Process games in parallel for speed
    all_games_data = []
    all_player_stats = []
    
    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=5) as executor:  # Limit to 5 concurrent requests
        # Submit all games for processing
        future_to_game = {executor.submit(process_game_parallel, game): game for game in games}
        
        # Collect results as they complete
        for future in as_completed(future_to_game):
            try:
                game_data, player_stats = future.result()
                all_games_data.append(game_data)
                all_player_stats.extend(player_stats)
            except Exception as e:
                game = future_to_game[future]
                print(f"Error processing game {game['game_id']}: {e}")
    
    # Create DataFrames
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

    int_cols = ["home_team_id", "home_score", "away_team_id", "away_score"]
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    str_cols = ["game_id", "season", "home_team_abbr", "away_team_abbr", "game_status"]
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

    str_cols = ["game_id", "season", "team_abbr", "player_name", "minutes"]
    for c in str_cols:
        if c in df.columns and is_object_dtype(df[c]):
            df[c] = df[c].astype("string")

    return df

# -----------------------------
# Main orchestration
# -----------------------------
def ingest_dates_fast(ymd_list: List[str]) -> None:
    """
    OPTIMIZED: Process multiple dates efficiently
    """
    ensure_tables()
    
    all_games_frames = []
    all_box_frames = []
    
    total_games = 0
    total_players = 0
    
    print(f"Processing {len(ymd_list)} dates...")
    
    for ymd in ymd_list:
        try:
            games_df, players_df = process_date_fast(ymd)
            
            if not games_df.empty:
                all_games_frames.append(games_df)
                total_games += len(games_df)
            
            if not players_df.empty:
                all_box_frames.append(players_df)
                total_players += len(players_df)
            
            # Minimal delay between dates
            time.sleep(0.1)
            
        except Exception as e:
            print(f"ERROR processing date {ymd}: {e}")
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
    parser = argparse.ArgumentParser(description="FAST NBA data ingestion using optimized APIs")
    parser.add_argument("--mode", choices=["backfill", "daily"], required=True)
    parser.add_argument("--start", help="YYYY-MM-DD inclusive start for backfill")
    parser.add_argument("--end", help="YYYY-MM-DD inclusive end for backfill")
    args = parser.parse_args()

    if args.mode == "daily":
        yday = datetime.date.today() - datetime.timedelta(days=1)
        ymd = yday.strftime("%Y%m%d")
        print(f"Running FAST daily ingest for {ymd}")
        ingest_dates_fast([ymd])
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
        print(f"Running FAST backfill from {args.start} to {args.end} ({len(dates)} dates)")
        ingest_dates_fast(dates)
        print(f"Backfill complete for {args.start} to {args.end}")
        return

if __name__ == "__main__":
    main()
