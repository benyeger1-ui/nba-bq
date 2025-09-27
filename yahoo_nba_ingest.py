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

# -----------------------------------
# Config via environment
# -----------------------------------
PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET    = os.environ.get("BQ_DATASET", "nba_data")
SA_INFO    = json.loads(os.environ["GCP_SA_KEY"])
CREDS      = service_account.Credentials.from_service_account_info(SA_INFO)
BQ         = bigquery.Client(project=PROJECT_ID, credentials=CREDS)

# -----------------------------------
# Multiple API options - we'll try them in order
# -----------------------------------

# Option 1: Ball Don't Lie API (free, no key required)
BALLDONTLIE_BASE = "https://www.balldontlie.io/api/v1"

# Option 2: NBA Stats API (official but rate limited)
NBA_STATS_BASE = "https://stats.nba.com/stats"

# Option 3: ESPN (backup)
ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"

# Option 4: RapidAPI NBA (requires free key)
RAPIDAPI_BASE = "https://api-nba-v1.p.rapidapi.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Cache-Control": "no-cache",
}

NBA_STATS_HEADERS = {
    **HEADERS,
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true"
}

def http_get_json(url: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None, max_retries: int = 3) -> Dict[str, Any]:
    if headers is None:
        headers = HEADERS
    
    last_err = None
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            last_err = e
            if attempt < max_retries - 1:
                time.sleep(0.8 * (attempt + 1))  # Exponential backoff
    
    raise last_err if last_err else RuntimeError("Request failed")

# -----------------------------------
# BigQuery schemas (same as before)
# -----------------------------------
GAMES_SCHEMA = [
    bigquery.SchemaField("event_id", "STRING"),
    bigquery.SchemaField("game_uid", "STRING"),
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("season", "INT64"),
    bigquery.SchemaField("status_type", "STRING"),
    bigquery.SchemaField("home_id", "INT64"),
    bigquery.SchemaField("home_abbr", "STRING"),
    bigquery.SchemaField("home_score", "INT64"),
    bigquery.SchemaField("away_id", "INT64"),
    bigquery.SchemaField("away_abbr", "STRING"),
    bigquery.SchemaField("away_score", "INT64"),
]

BOX_SCHEMA = [
    bigquery.SchemaField("event_id", "STRING"),
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("season", "INT64"),
    bigquery.SchemaField("team_id", "INT64"),
    bigquery.SchemaField("team_abbr", "STRING"),
    bigquery.SchemaField("player_id", "INT64"),
    bigquery.SchemaField("player", "STRING"),
    bigquery.SchemaField("starter", "BOOL"),
    bigquery.SchemaField("minutes", "STRING"),
    bigquery.SchemaField("pts", "INT64"),
    bigquery.SchemaField("reb", "INT64"),
    bigquery.SchemaField("ast", "INT64"),
    bigquery.SchemaField("stl", "INT64"),
    bigquery.SchemaField("blk", "INT64"),
    bigquery.SchemaField("tov", "INT64"),
    bigquery.SchemaField("fgm", "INT64"),
    bigquery.SchemaField("fga", "INT64"),
    bigquery.SchemaField("fg3m", "INT64"),
    bigquery.SchemaField("fg3a", "INT64"),
    bigquery.SchemaField("ftm", "INT64"),
    bigquery.SchemaField("fta", "INT64"),
    bigquery.SchemaField("oreb", "INT64"),
    bigquery.SchemaField("dreb", "INT64"),
    bigquery.SchemaField("pf", "INT64"),
]

# (Include all the utility functions from before - ensure_dataset, ensure_tables, load_df, coerce_*_dtypes, safe_int)

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
        BQ.create_table(bigquery.Table(games_table_id, schema=GAMES_SCHEMA))
    box_table_id = f"{PROJECT_ID}.{DATASET}.player_boxscores"
    try:
        BQ.get_table(box_table_id)
    except Exception:
        BQ.create_table(bigquery.Table(box_table_id, schema=BOX_SCHEMA))

def load_df(df: pd.DataFrame, table: str) -> None:
    if df is None or df.empty:
        return
    table_id = f"{PROJECT_ID}.{DATASET}.{table}"
    schema = GAMES_SCHEMA if table == "games_daily" else BOX_SCHEMA
    if "date" in df.columns:
        df = df.dropna(subset=["date"])
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    BQ.load_table_from_dataframe(df, table_id, job_config=job_config).result()

def coerce_games_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    int_cols = ["season", "home_id", "home_score", "away_id", "away_score"]
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    str_cols = ["status_type", "home_abbr", "away_abbr", "game_uid", "event_id"]
    for c in str_cols:
        if c in df.columns and is_object_dtype(df[c]):
            df[c] = df[c].astype("string")
    return df

def coerce_box_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    if "starter" in df.columns:
        df["starter"] = df["starter"].astype("boolean")
    int_cols = [
        "season", "team_id", "player_id", "pts", "reb", "ast", "stl", "blk", "tov",
        "fgm", "fga", "fg3m", "fg3a", "ftm", "fta", "oreb", "dreb", "pf"
    ]
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    str_cols = ["team_abbr", "player", "minutes", "event_id"]
    for c in str_cols:
        if c in df.columns and is_object_dtype(df[c]):
            df[c] = df[c].astype("string")
    return df

def safe_int(x: Any) -> Optional[int]:
    try:
        return int(x) if x is not None and x != "" else None
    except Exception:
        return None

# -----------------------------------
# Ball Don't Lie API Functions
# -----------------------------------
def fetch_games_balldontlie(date_str: str) -> pd.DataFrame:
    """Fetch games for a specific date using Ball Don't Lie API"""
    try:
        url = f"{BALLDONTLIE_BASE}/games"
        params = {"dates[]": date_str}
        
        data = http_get_json(url, params)
        games = data.get("data", [])
        
        if not games:
            print(f"No games found in Ball Don't Lie API for {date_str}")
            return pd.DataFrame(columns=[f.name for f in GAMES_SCHEMA])
        
        games_list = []
        for game in games:
            # Season calculation
            year = int(date_str[:4])
            month = int(date_str[5:7])
            season = year if month >= 10 else year - 1
            
            home_team = game.get("home_team", {})
            visitor_team = game.get("visitor_team", {})
            
            games_list.append({
                "event_id": str(game.get("id")),
                "game_uid": f"bdl_{game.get('id')}",
                "date": date_str,
                "season": season,
                "status_type": game.get("status", "Final"),
                "home_id": safe_int(home_team.get("id")),
                "home_abbr": home_team.get("abbreviation"),
                "home_score": safe_int(game.get("home_team_score")),
                "away_id": safe_int(visitor_team.get("id")),
                "away_abbr": visitor_team.get("abbreviation"),
                "away_score": safe_int(game.get("visitor_team_score"))
            })
        
        df = pd.DataFrame(games_list)
        return coerce_games_dtypes(df)
        
    except Exception as e:
        print(f"Error fetching Ball Don't Lie games for {date_str}: {e}")
        return pd.DataFrame(columns=[f.name for f in GAMES_SCHEMA])

def fetch_player_stats_balldontlie(game_id: str, date_str: str) -> pd.DataFrame:
    """Fetch player stats for a specific game using Ball Don't Lie API"""
    try:
        url = f"{BALLDONTLIE_BASE}/stats"
        params = {"game_ids[]": game_id}
        
        data = http_get_json(url, params)
        stats = data.get("data", [])
        
        if not stats:
            return pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])
        
        # Season calculation
        year = int(date_str[:4])
        month = int(date_str[5:7])
        season = year if month >= 10 else year - 1
        
        players_list = []
        for stat in stats:
            player = stat.get("player", {})
            team = stat.get("team", {})
            
            # Check if player started (Ball Don't Lie doesn't provide this directly)
            minutes_str = stat.get("min", "0:00")
            starter = None  # We'll have to infer this or leave it null
            
            players_list.append({
                "event_id": game_id,
                "date": date_str,
                "season": season,
                "team_id": safe_int(team.get("id")),
                "team_abbr": team.get("abbreviation"),
                "player_id": safe_int(player.get("id")),
                "player": f"{player.get('first_name', '')} {player.get('last_name', '')}".strip(),
                "starter": starter,
                "minutes": minutes_str,
                "pts": safe_int(stat.get("pts")),
                "reb": safe_int(stat.get("reb")),
                "ast": safe_int(stat.get("ast")),
                "stl": safe_int(stat.get("stl")),
                "blk": safe_int(stat.get("blk")),
                "tov": safe_int(stat.get("turnover")),
                "fgm": safe_int(stat.get("fgm")),
                "fga": safe_int(stat.get("fga")),
                "fg3m": safe_int(stat.get("fg3m")),
                "fg3a": safe_int(stat.get("fg3a")),
                "ftm": safe_int(stat.get("ftm")),
                "fta": safe_int(stat.get("fta")),
                "oreb": safe_int(stat.get("oreb")),
                "dreb": safe_int(stat.get("dreb")),
                "pf": safe_int(stat.get("pf"))
            })
        
        df = pd.DataFrame(players_list)
        return coerce_box_dtypes(df)
        
    except Exception as e:
        print(f"Error fetching Ball Don't Lie player stats for game {game_id}: {e}")
        return pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])

# -----------------------------------
# ESPN Fallback Functions (from your original code)
# -----------------------------------
def fetch_games_espn(date_str: str) -> pd.DataFrame:
    """Fallback to ESPN API for games"""
    try:
        yyyymmdd = date_str.replace("-", "")
        url = f"{ESPN_BASE}/scoreboard"
        params = {"dates": yyyymmdd}
        
        data = http_get_json(url, params)
        events = data.get("events", [])
        
        if not events:
            return pd.DataFrame(columns=[f.name for f in GAMES_SCHEMA])
        
        # (Implementation similar to your original ESPN code)
        # Simplified version for brevity
        games_list = []
        for event in events:
            competitions = event.get("competitions", [{}])
            comp = competitions[0] if competitions else {}
            
            competitors = comp.get("competitors", [])
            home = next((c for c in competitors if c.get("homeAway") == "home"), {})
            away = next((c for c in competitors if c.get("homeAway") == "away"), {})
            
            games_list.append({
                "event_id": event.get("id"),
                "game_uid": event.get("uid"),
                "date": date_str,
                "season": int(date_str[:4]) if int(date_str[5:7]) >= 10 else int(date_str[:4]) - 1,
                "status_type": comp.get("status", {}).get("type", {}).get("name"),
                "home_id": safe_int((home.get("team", {}) or {}).get("id")),
                "home_abbr": (home.get("team", {}) or {}).get("abbreviation"),
                "home_score": safe_int(home.get("score")),
                "away_id": safe_int((away.get("team", {}) or {}).get("id")),
                "away_abbr": (away.get("team", {}) or {}).get("abbreviation"),
                "away_score": safe_int(away.get("score"))
            })
        
        df = pd.DataFrame(games_list)
        return coerce_games_dtypes(df)
        
    except Exception as e:
        print(f"Error fetching ESPN games for {date_str}: {e}")
        return pd.DataFrame(columns=[f.name for f in GAMES_SCHEMA])

# -----------------------------------
# Main ingestion function with fallbacks
# -----------------------------------
def ingest_date_with_fallbacks(date_str: str) -> None:
    """Try multiple APIs in order until one works"""
    ensure_tables()
    
    print(f"Fetching NBA data for {date_str}")
    
    # Try Ball Don't Lie first (most reliable for stats)
    games_df = fetch_games_balldontlie(date_str)
    api_used = "Ball Don't Lie"
    
    if games_df.empty:
        print("Ball Don't Lie API failed, trying ESPN...")
        games_df = fetch_games_espn(date_str)
        api_used = "ESPN"
    
    if games_df.empty:
        print(f"No games found for {date_str} in any API")
        return
    
    print(f"Found {len(games_df)} games using {api_used} API")
    
    # Load games
    load_df(games_df, "games_daily")
    
    # Fetch player stats (only Ball Don't Lie supports this easily)
    if api_used == "Ball Don't Lie":
        all_player_stats = []
        
        for _, game_row in games_df.iterrows():
            game_id = game_row["event_id"]
            print(f"Fetching player stats for game {game_id}")
            
            player_stats_df = fetch_player_stats_balldontlie(game_id, date_str)
            if not player_stats_df.empty:
                all_player_stats.append(player_stats_df)
                print(f"Found {len(player_stats_df)} player stat rows")
            
            time.sleep(0.5)  # Be respectful
        
        if all_player_stats:
            combined_stats = pd.concat(all_player_stats, ignore_index=True)
            combined_stats = coerce_box_dtypes(combined_stats)
            print(f"Loading {len(combined_stats)} player stat rows")
            load_df(combined_stats, "player_boxscores")
        else:
            print("No player stats found")
    else:
        print(f"Player stats not available with {api_used} API")

def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest NBA data using multiple APIs with fallbacks")
    parser.add_argument("--mode", choices=["daily", "backfill"], default="daily")
    parser.add_argument("--start", help="YYYY-MM-DD start date for backfill")
    parser.add_argument("--end", help="YYYY-MM-DD end date for backfill")
    parser.add_argument("--date", help="YYYY-MM-DD specific date")
    args = parser.parse_args()

    if args.date:
        ingest_date_with_fallbacks(args.date)
        return

    if args.mode == "daily":
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        ingest_date_with_fallbacks(yesterday.isoformat())
        return

    if args.mode == "backfill":
        if not args.start or not args.end:
            print("Error: backfill requires --start and --end dates")
            sys.exit(1)
        
        start_date = datetime.date.fromisoformat(args.start)
        end_date = datetime.date.fromisoformat(args.end)
        
        current_date = start_date
        while current_date <= end_date:
            ingest_date_with_fallbacks(current_date.isoformat())
            current_date += datetime.timedelta(days=1)
            time.sleep(1)  # Be respectful between dates

if __name__ == "__main__":
    main()
