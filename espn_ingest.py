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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET = os.environ.get("BQ_DATASET", "nba_data")

SA_INFO = json.loads(os.environ["GCP_SA_KEY"])
CREDS = service_account.Credentials.from_service_account_info(SA_INFO)
BQ = bigquery.Client(project=PROJECT_ID, credentials=CREDS)

# ESPN endpoints
ESPN_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"

# NBA CDN endpoints (more reliable than stats.nba.com)
NBA_CDN_BOXSCORE = "https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"

# -----------------------------
# BigQuery schemas  
# -----------------------------
GAMES_SCHEMA = [
    bigquery.SchemaField("game_id", "STRING"),
    bigquery.SchemaField("game_date", "DATE"),
    bigquery.SchemaField("season", "INT64"),
    bigquery.SchemaField("home_team_abbr", "STRING"),
    bigquery.SchemaField("home_score", "INT64"),
    bigquery.SchemaField("away_team_abbr", "STRING"),
    bigquery.SchemaField("away_score", "INT64"),
    bigquery.SchemaField("game_status", "STRING"),
]

BOX_SCHEMA = [
    bigquery.SchemaField("game_id", "STRING"),
    bigquery.SchemaField("game_date", "DATE"),
    bigquery.SchemaField("season", "INT64"),
    bigquery.SchemaField("team_abbr", "STRING"),
    bigquery.SchemaField("player_id", "INT64"),
    bigquery.SchemaField("player_name", "STRING"),
    bigquery.SchemaField("starter", "BOOL"),
    bigquery.SchemaField("minutes", "STRING"),
    bigquery.SchemaField("pts", "INT64"),
    bigquery.SchemaField("fgm", "INT64"),
    bigquery.SchemaField("fga", "INT64"),
    bigquery.SchemaField("fg3m", "INT64"),
    bigquery.SchemaField("fg3a", "INT64"),
    bigquery.SchemaField("ftm", "INT64"),
    bigquery.SchemaField("fta", "INT64"),
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
        print(f"No data to load for {table}")
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

def http_get_json(url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 10) -> Dict[str, Any]:
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
def get_espn_games(date_yyyymmdd: str) -> List[Dict[str, Any]]:
    """Get completed games from ESPN"""
    print(f"Getting ESPN games for {date_yyyymmdd}")
    
    data = http_get_json(ESPN_SCOREBOARD, params={"dates": date_yyyymmdd})
    games = []
    
    if 'events' in data:
        for event in data['events']:
            status = event.get('status', {}).get('type', {}).get('name', '')
            if status != 'STATUS_FINAL':
                continue
            
            competitors = event.get('competitions', [{}])[0].get('competitors', [])
            home_team = next((c for c in competitors if c.get('homeAway') == 'home'), {})
            away_team = next((c for c in competitors if c.get('homeAway') == 'away'), {})
            
            game = {
                'game_id': event['id'],
                'game_date': event['date'][:10],
                'season': safe_int(event.get('season', {}).get('year', 2025)),
                'home_team_abbr': home_team.get('team', {}).get('abbreviation'),
                'home_score': safe_int(home_team.get('score')),
                'away_team_abbr': away_team.get('team', {}).get('abbreviation'),
                'away_score': safe_int(away_team.get('score')),
                'game_status': status,
            }
            games.append(game)
    
    print(f"Found {len(games)} completed games")
    return games

# -----------------------------
# NBA CDN Player Data
# -----------------------------
def generate_nba_game_ids(date_str: str) -> List[str]:
    """Generate possible NBA game IDs for a given date"""
    # NBA game ID format: 00{season_suffix}{month}{day}{game_sequence}
    date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    
    # Determine season (starts in October)
    if date_obj.month >= 10:
        season = date_obj.year
    else:
        season = date_obj.year - 1
    
    season_suffix = str(season)[-2:]  # Last 2 digits
    month = f"{date_obj.month:02d}"
    day = f"{date_obj.day:02d}"
    
    # Generate possible game IDs (try up to 20 games per day)
    game_ids = []
    for i in range(1, 21):
        game_id = f"00{season_suffix}{month}{day}{i:02d}"
        game_ids.append(game_id)
    
    return game_ids

def get_nba_player_stats(espn_game: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Try to get NBA player stats for an ESPN game"""
    game_date = espn_game['game_date']
    home_abbr = espn_game['home_team_abbr']
    away_abbr = espn_game['away_team_abbr']
    
    print(f"Trying to find NBA data for {away_abbr} @ {home_abbr}")
    
    # Generate possible NBA game IDs
    possible_ids = generate_nba_game_ids(game_date)
    
    for nba_game_id in possible_ids:
        try:
            url = NBA_CDN_BOXSCORE.format(game_id=nba_game_id)
            data = http_get_json(url, timeout=5)
            
            if not data or 'game' not in data:
                continue
            
            game_data = data['game']
            home_team = game_data.get('homeTeam', {})
            away_team = game_data.get('awayTeam', {})
            
            nba_home_abbr = home_team.get('teamTricode', '')
            nba_away_abbr = away_team.get('teamTricode', '')
            
            # Check if teams match (with some flexibility for abbreviation differences)
            home_match = home_abbr == nba_home_abbr or (home_abbr == 'SA' and nba_home_abbr == 'SAS')
            away_match = away_abbr == nba_away_abbr or (away_abbr == 'SA' and nba_away_abbr == 'SAS')
            
            if home_match and away_match:
                print(f"  ✓ Found matching NBA game: {nba_game_id}")
                return extract_players_from_nba_data(game_data, espn_game)
            
        except Exception:
            continue  # Try next game ID
        
        time.sleep(0.05)  # Small delay between attempts
    
    print(f"  ✗ No NBA data found for {away_abbr} @ {home_abbr}")
    return []

def extract_players_from_nba_data(game_data: Dict[str, Any], espn_game: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract player stats from NBA CDN data"""
    home_team = game_data.get('homeTeam', {})
    away_team = game_data.get('awayTeam', {})
    
    player_stats = []
    
    for team in [home_team, away_team]:
        team_abbr = team.get('teamTricode', '')
        players = team.get('players', [])
        
        for player in players:
            if not player.get('played'):
                continue
            
            stats = player.get('statistics', {})
            minutes = stats.get('minutes', '')
            
            if not minutes or minutes == 'PT0M':
                continue
            
            player_stat = {
                'game_id': espn_game['game_id'],
                'game_date': espn_game['game_date'],
                'season': espn_game['season'],
                'team_abbr': team_abbr,
                'player_id': safe_int(player.get('personId')),
                'player_name': player.get('name', '').strip(),
                'starter': bool(player.get('starter')),
                'minutes': minutes,
                'pts': safe_int(stats.get('points')),
                'fgm': safe_int(stats.get('fieldGoalsMade')),
                'fga': safe_int(stats.get('fieldGoalsAttempted')),
                'fg3m': safe_int(stats.get('threePointersMade')),
                'fg3a': safe_int(stats.get('threePointersAttempted')),
                'ftm': safe_int(stats.get('freeThrowsMade')),
                'fta': safe_int(stats.get('freeThrowsAttempted')),
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
    
    print(f"  Extracted {len(player_stats)} player stats")
    return player_stats

# -----------------------------
# Data Processing
# -----------------------------
def process_date(date_yyyymmdd: str) -> tuple:
    """Process a single date"""
    print(f"\n=== Processing {date_yyyymmdd} ===")
    
    # Get ESPN games
    espn_games = get_espn_games(date_yyyymmdd)
    
    if not espn_games:
        return pd.DataFrame(), pd.DataFrame()
    
    # Process each game
    all_player_stats = []
    
    for game in espn_games:
        try:
            player_stats = get_nba_player_stats(game)
            all_player_stats.extend(player_stats)
            time.sleep(0.5)  # Respectful delay
        except Exception as e:
            print(f"Error processing game {game['game_id']}: {e}")
    
    # Create DataFrames
    games_df = pd.DataFrame(espn_games)
    players_df = pd.DataFrame(all_player_stats)
    
    # Type coercion
    if not games_df.empty:
        games_df["game_date"] = pd.to_datetime(games_df["game_date"]).dt.date
        games_df["season"] = games_df["season"].astype("Int64")
        games_df["home_score"] = games_df["home_score"].astype("Int64")
        games_df["away_score"] = games_df["away_score"].astype("Int64")
    
    if not players_df.empty:
        players_df["game_date"] = pd.to_datetime(players_df["game_date"]).dt.date
        players_df["season"] = players_df["season"].astype("Int64")
        players_df["starter"] = players_df["starter"].astype("boolean")
        
        int_cols = ["player_id", "pts", "fgm", "fga", "fg3m", "fg3a", "ftm", "fta", 
                   "oreb", "dreb", "reb", "ast", "stl", "blk", "tov", "pf", "plus_minus"]
        for col in int_cols:
            if col in players_df.columns:
                players_df[col] = players_df[col].astype("Int64")
    
    print(f"COMPLETED: {len(games_df)} games, {len(players_df)} player stats")
    return games_df, players_df

# -----------------------------
# Main orchestration
# -----------------------------
def ingest_dates(ymd_list: List[str]) -> None:
    """Process multiple dates"""
    ensure_tables()
    
    all_games_frames = []
    all_box_frames = []
    
    for ymd in ymd_list:
        try:
            games_df, players_df = process_date(ymd)
            
            if not games_df.empty:
                all_games_frames.append(games_df)
            
            if not players_df.empty:
                all_box_frames.append(players_df)
            
            time.sleep(1.0)
            
        except Exception as e:
            print(f"ERROR processing date {ymd}: {e}")
    
    # Upload to BigQuery
    print(f"\n=== Uploading to BigQuery ===")
    
    if all_games_frames:
        final_games_df = pd.concat(all_games_frames, ignore_index=True)
        print(f"Loading {len(final_games_df)} game rows")
        load_df(final_games_df, "games_daily")
    
    if all_box_frames:
        final_players_df = pd.concat(all_box_frames, ignore_index=True)
        print(f"Loading {len(final_players_df)} player box score rows")
        load_df(final_players_df, "player_boxscores")
    
    print(f"Processing complete!")

def yyyymmdd_list(start: datetime.date, end: datetime.date) -> List[str]:
    out = []
    cur = start
    while cur <= end:
        out.append(cur.strftime("%Y%m%d"))
        cur += datetime.timedelta(days=1)
    return out

def main() -> None:
    parser = argparse.ArgumentParser(description="Simple NBA data ingestion")
    parser.add_argument("--mode", choices=["backfill", "daily"], required=True)
    parser.add_argument("--start", help="YYYY-MM-DD start date for backfill")
    parser.add_argument("--end", help="YYYY-MM-DD end date for backfill")
    args = parser.parse_args()

    print("=== NBA Data Pipeline Starting ===")

    if args.mode == "daily":
        yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")
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
        
        dates = yyyymmdd_list(start_date, end_date)
        print(f"Running backfill for {len(dates)} dates: {args.start} to {args.end}")
        ingest_dates(dates)

if __name__ == "__main__":
    main()
