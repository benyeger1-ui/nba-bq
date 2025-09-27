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
PROJECT_ID = os.environ["GCP_PROJECT_ID"]         # set as repo secret
DATASET    = os.environ.get("BQ_DATASET", "nba_data")
SA_INFO    = json.loads(os.environ["GCP_SA_KEY"])
CREDS      = service_account.Credentials.from_service_account_info(SA_INFO)
BQ         = bigquery.Client(project=PROJECT_ID, credentials=CREDS)

# Yahoo Sports API endpoints
YAHOO_BASE_URL = "https://api.sportradar.us/nba/trial/v8/en"  # Free tier
# Alternative Yahoo Fantasy Sports API (if you have access)
YAHOO_FANTASY_BASE = "https://fantasysports.yahooapis.com/fantasy/v2/game/nba"

# Yahoo Sports unofficial API endpoints (publicly accessible)
YAHOO_SPORTS_SCOREBOARD = "https://api-secure.sports.yahoo.com/v1/editorial/s/scoreboard"
YAHOO_SPORTS_GAME = "https://api-secure.sports.yahoo.com/v1/editorial/game"

# Headers to mimic browser requests
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://sports.yahoo.com/",
    "Origin": "https://sports.yahoo.com",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

def http_get_json(url: str, params: Optional[Dict[str, Any]] = None, max_retries: int = 3) -> Dict[str, Any]:
    last_err = None
    for _ in range(max_retries):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(0.8)
    raise last_err if last_err else RuntimeError("request failed")

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

# -----------------------------------
# Type coercion (same as before)
# -----------------------------------
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

# -----------------------------------
# Utilities
# -----------------------------------
def safe_int(x: Any) -> Optional[int]:
    try:
        return int(x) if x is not None and x != "" else None
    except Exception:
        return None

def safe_str(x: Any) -> Optional[str]:
    try:
        return str(x) if x is not None and x != "" else None
    except Exception:
        return None

# -----------------------------------
# Yahoo Sports API functions
# -----------------------------------
def get_yahoo_scoreboard(date_str: str) -> Dict[str, Any]:
    """
    Fetch scoreboard for a specific date from Yahoo Sports
    date_str should be in YYYY-MM-DD format
    """
    try:
        # Convert date to the format Yahoo expects
        date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        yahoo_date = date_obj.strftime("%Y-%m-%d")
        
        params = {
            "leagues": "nba",
            "date": yahoo_date,
            "region": "US",
            "lang": "en-US"
        }
        
        return http_get_json(YAHOO_SPORTS_SCOREBOARD, params=params)
    except Exception as e:
        print(f"Error fetching Yahoo scoreboard for {date_str}: {e}")
        return {}

def get_yahoo_game_details(game_id: str) -> Dict[str, Any]:
    """
    Fetch detailed game information including box scores from Yahoo Sports
    """
    try:
        params = {
            "game_id": game_id,
            "region": "US",
            "lang": "en-US"
        }
        
        return http_get_json(YAHOO_SPORTS_GAME, params=params)
    except Exception as e:
        print(f"Error fetching Yahoo game details for {game_id}: {e}")
        return {}

def extract_games_from_yahoo_scoreboard(scoreboard_data: Dict[str, Any], date_str: str) -> pd.DataFrame:
    """
    Extract game information from Yahoo scoreboard response
    """
    games_list = []
    
    try:
        # Yahoo's structure may vary, adjust based on actual response
        games = scoreboard_data.get("games", [])
        if not games and "service" in scoreboard_data:
            # Alternative structure
            service_data = scoreboard_data.get("service", {})
            games = service_data.get("games", [])
        
        for game in games:
            if not isinstance(game, dict):
                continue
                
            game_id = safe_str(game.get("game_id") or game.get("id"))
            if not game_id:
                continue
            
            # Extract teams information
            teams = game.get("teams", [])
            if len(teams) < 2:
                continue
            
            home_team = None
            away_team = None
            
            for team in teams:
                if team.get("is_home", False):
                    home_team = team
                else:
                    away_team = team
            
            if not home_team or not away_team:
                # Fallback: assume first is away, second is home
                away_team = teams[0]
                home_team = teams[1]
            
            # Extract game information
            status = game.get("status", {})
            status_type = safe_str(status.get("type") or status.get("state"))
            
            # Season year (approximate from date)
            year = int(date_str[:4])
            month = int(date_str[5:7])
            season = year if month >= 10 else year - 1
            
            game_row = {
                "event_id": game_id,
                "game_uid": safe_str(game.get("uid")),
                "date": date_str,
                "season": season,
                "status_type": status_type,
                "home_id": safe_int(home_team.get("team_id") or home_team.get("id")),
                "home_abbr": safe_str(home_team.get("abbreviation") or home_team.get("abbr")),
                "home_score": safe_int(home_team.get("score")),
                "away_id": safe_int(away_team.get("team_id") or away_team.get("id")),
                "away_abbr": safe_str(away_team.get("abbreviation") or away_team.get("abbr")),
                "away_score": safe_int(away_team.get("score"))
            }
            
            games_list.append(game_row)
            
    except Exception as e:
        print(f"Error parsing Yahoo scoreboard data: {e}")
    
    if not games_list:
        return pd.DataFrame(columns=[f.name for f in GAMES_SCHEMA])
    
    df = pd.DataFrame(games_list)
    return coerce_games_dtypes(df)

def extract_player_stats_from_yahoo_game(game_data: Dict[str, Any], game_id: str, date_str: str) -> pd.DataFrame:
    """
    Extract player box score statistics from Yahoo game details
    """
    players_list = []
    
    try:
        # Season year
        year = int(date_str[:4])
        month = int(date_str[5:7])
        season = year if month >= 10 else year - 1
        
        # Look for player stats in various possible locations
        teams_data = game_data.get("teams", [])
        if not teams_data and "boxscore" in game_data:
            teams_data = game_data["boxscore"].get("teams", [])
        
        for team in teams_data:
            if not isinstance(team, dict):
                continue
                
            team_id = safe_int(team.get("team_id") or team.get("id"))
            team_abbr = safe_str(team.get("abbreviation") or team.get("abbr"))
            
            # Look for players in various possible locations
            players = team.get("players", [])
            if not players and "roster" in team:
                players = team["roster"].get("players", [])
            
            for player in players:
                if not isinstance(player, dict):
                    continue
                
                # Player identification
                player_id = safe_int(player.get("player_id") or player.get("id"))
                player_name = safe_str(player.get("name") or player.get("display_name"))
                
                # Player stats - Yahoo may have different field names
                stats = player.get("stats", {})
                if not stats and "statistics" in player:
                    stats = player["statistics"]
                
                # Extract statistical categories
                def get_stat(*keys):
                    for key in keys:
                        val = stats.get(key)
                        if val is not None:
                            return safe_int(val)
                    return None
                
                # Check if player started
                starter = player.get("starter", False)
                if isinstance(starter, str):
                    starter = starter.lower() in ["true", "1", "yes"]
                elif starter is None:
                    # Check position or other indicators
                    position = player.get("position", "")
                    starter = bool(position and position != "BENCH")
                
                player_row = {
                    "event_id": game_id,
                    "date": date_str,
                    "season": season,
                    "team_id": team_id,
                    "team_abbr": team_abbr,
                    "player_id": player_id,
                    "player": player_name,
                    "starter": starter,
                    "minutes": safe_str(stats.get("minutes") or stats.get("min")),
                    "pts": get_stat("points", "pts"),
                    "reb": get_stat("rebounds", "reb", "total_rebounds"),
                    "ast": get_stat("assists", "ast"),
                    "stl": get_stat("steals", "stl"),
                    "blk": get_stat("blocks", "blk"),
                    "tov": get_stat("turnovers", "tov", "to"),
                    "fgm": get_stat("field_goals_made", "fgm", "fg_made"),
                    "fga": get_stat("field_goals_attempted", "fga", "fg_attempted"),
                    "fg3m": get_stat("three_point_field_goals_made", "fg3m", "3pm"),
                    "fg3a": get_stat("three_point_field_goals_attempted", "fg3a", "3pa"),
                    "ftm": get_stat("free_throws_made", "ftm", "ft_made"),
                    "fta": get_stat("free_throws_attempted", "fta", "ft_attempted"),
                    "oreb": get_stat("offensive_rebounds", "oreb"),
                    "dreb": get_stat("defensive_rebounds", "dreb"),
                    "pf": get_stat("personal_fouls", "pf", "fouls")
                }
                
                players_list.append(player_row)
                
    except Exception as e:
        print(f"Error parsing Yahoo player stats for game {game_id}: {e}")
    
    if not players_list:
        return pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])
    
    df = pd.DataFrame(players_list)
    return coerce_box_dtypes(df)

# -----------------------------------
# Main ingestion functions
# -----------------------------------
def ingest_date_via_yahoo(date_str: str) -> None:
    """
    Ingest games and player stats for a specific date using Yahoo Sports API
    """
    ensure_tables()
    
    print(f"Fetching Yahoo data for {date_str}")
    
    # Get scoreboard for the date
    scoreboard_data = get_yahoo_scoreboard(date_str)
    if not scoreboard_data:
        print(f"No scoreboard data found for {date_str}")
        return
    
    # Extract games
    games_df = extract_games_from_yahoo_scoreboard(scoreboard_data, date_str)
    
    if games_df.empty:
        print(f"No games found for {date_str}")
        return
    
    print(f"Found {len(games_df)} games for {date_str}")
    
    # Load games to BigQuery
    load_df(games_df, "games_daily")
    
    # Fetch detailed game data and player stats
    all_player_stats = []
    
    for _, game_row in games_df.iterrows():
        game_id = game_row["event_id"]
        print(f"Fetching details for game {game_id}")
        
        game_details = get_yahoo_game_details(game_id)
        if not game_details:
            print(f"No game details found for {game_id}")
            continue
        
        player_stats_df = extract_player_stats_from_yahoo_game(game_details, game_id, date_str)
        if not player_stats_df.empty:
            all_player_stats.append(player_stats_df)
            print(f"Found {len(player_stats_df)} player stats for game {game_id}")
        else:
            print(f"No player stats found for game {game_id}")
        
        # Be respectful to the API
        time.sleep(0.5)
    
    # Load all player stats
    if all_player_stats:
        combined_player_stats = pd.concat(all_player_stats, ignore_index=True)
        combined_player_stats = coerce_box_dtypes(combined_player_stats)
        print(f"Loading {len(combined_player_stats)} player stat rows")
        load_df(combined_player_stats, "player_boxscores")
    else:
        print("No player stats to load")

def ingest_dates_via_yahoo(date_list: List[str]) -> None:
    """
    Ingest multiple dates using Yahoo Sports API
    """
    for date_str in date_list:
        ingest_date_via_yahoo(date_str)
        # Be respectful between dates
        time.sleep(1)

# -----------------------------------
# Main function
# -----------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest NBA games and player box scores into BigQuery using Yahoo Sports API")
    parser.add_argument("--mode", choices=["daily", "backfill"], default="daily", help="daily for yesterday's games, backfill for a date range")
    parser.add_argument("--start", help="YYYY-MM-DD inclusive start for backfill")
    parser.add_argument("--end", help="YYYY-MM-DD inclusive end for backfill")
    parser.add_argument("--date", help="YYYY-MM-DD specific date (overrides daily mode)")
    args = parser.parse_args()

    if args.date:
        # Specific date mode
        ingest_date_via_yahoo(args.date)
        print(f"Ingestion complete for {args.date}")
        return

    if args.mode == "daily":
        # Yesterday's games
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        date_str = yesterday.isoformat()
        ingest_date_via_yahoo(date_str)
        print(f"Daily ingestion complete for {date_str}")
        return

    if args.mode == "backfill":
        if not args.start or not args.end:
            print("Error - backfill needs --start and --end like 2024-10-01 and 2025-01-31")
            sys.exit(1)
        
        try:
            start_date = datetime.date.fromisoformat(args.start)
            end_date = datetime.date.fromisoformat(args.end)
        except Exception:
            print("Error - invalid date format. Use YYYY-MM-DD")
            sys.exit(1)
        
        if end_date < start_date:
            print("Error - end date must be on or after start date")
            sys.exit(1)
        
        # Generate date list
        date_list = []
        current_date = start_date
        while current_date <= end_date:
            date_list.append(current_date.isoformat())
            current_date += datetime.timedelta(days=1)
        
        print(f"Backfilling {len(date_list)} dates from {args.start} to {args.end}")
        ingest_dates_via_yahoo(date_list)
        print(f"Backfill complete for {args.start} to {args.end}")
        return

if __name__ == "__main__":
    main()
