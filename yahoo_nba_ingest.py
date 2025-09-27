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

# ESPN API endpoints
ESPN_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
ESPN_SUMMARY = "https://site.web.api.espn.com/apis/site/v2/sports/basketball/nba/summary"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.espn.com/",
    "Origin": "https://www.espn.com",
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
# BigQuery schemas
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

def safe_float(x: Any) -> Optional[float]:
    try:
        return float(x) if x is not None and x != "" else None
    except Exception:
        return None

def extract_games_from_scoreboard(scoreboard_data: Dict[str, Any], date_str: str) -> pd.DataFrame:
    """Extract game information from ESPN scoreboard"""
    games_list = []
    
    events = scoreboard_data.get("events", [])
    
    for event in events:
        event_id = event.get("id")
        game_uid = event.get("uid")
        
        competitions = event.get("competitions", [])
        if not competitions:
            continue
            
        comp = competitions[0]
        competitors = comp.get("competitors", [])
        
        if len(competitors) < 2:
            continue
            
        # Find home and away teams
        home_team = None
        away_team = None
        
        for competitor in competitors:
            if competitor.get("homeAway") == "home":
                home_team = competitor
            elif competitor.get("homeAway") == "away":
                away_team = competitor
        
        if not home_team or not away_team:
            continue
        
        # Extract team info
        home_team_info = home_team.get("team", {})
        away_team_info = away_team.get("team", {})
        
        # Extract status
        status = comp.get("status", {})
        status_type = status.get("type", {}).get("name", "Unknown")
        
        # Calculate season
        year = int(date_str[:4])
        month = int(date_str[5:7])
        season = year if month >= 10 else year - 1
        
        games_list.append({
            "event_id": event_id,
            "game_uid": game_uid,
            "date": date_str,
            "season": season,
            "status_type": status_type,
            "home_id": safe_int(home_team_info.get("id")),
            "home_abbr": home_team_info.get("abbreviation"),
            "home_score": safe_int(home_team.get("score")),
            "away_id": safe_int(away_team_info.get("id")),
            "away_abbr": away_team_info.get("abbreviation"),
            "away_score": safe_int(away_team.get("score"))
        })
    
    if not games_list:
        return pd.DataFrame(columns=[f.name for f in GAMES_SCHEMA])
    
    df = pd.DataFrame(games_list)
    return coerce_games_dtypes(df)

def extract_player_stats_from_scoreboard(scoreboard_data: Dict[str, Any], date_str: str) -> pd.DataFrame:
    """Extract player statistics from ESPN scoreboard response (limited stats from leaders)"""
    players_list = []
    
    # Calculate season
    year = int(date_str[:4])
    month = int(date_str[5:7])
    season = year if month >= 10 else year - 1
    
    events = scoreboard_data.get("events", [])
    
    for event in events:
        event_id = event.get("id")
        competitions = event.get("competitions", [])
        
        if not competitions:
            continue
            
        comp = competitions[0]
        competitors = comp.get("competitors", [])
        
        for competitor in competitors:
            team_info = competitor.get("team", {})
            team_id = safe_int(team_info.get("id"))
            team_abbr = team_info.get("abbreviation")
            
            # Extract leader stats (this gives us top performers)
            leaders = competitor.get("leaders", [])
            
            for leader_category in leaders:
                category_name = leader_category.get("name", "")
                category_leaders = leader_category.get("leaders", [])
                
                for leader in category_leaders:
                    athlete = leader.get("athlete", {})
                    player_id = safe_int(athlete.get("id"))
                    player_name = athlete.get("displayName")
                    
                    # Skip if we already have this player for this game
                    if any(p["player_id"] == player_id and p["event_id"] == event_id for p in players_list):
                        continue
                    
                    # Initialize player stats
                    player_stats = {
                        "event_id": event_id,
                        "date": date_str,
                        "season": season,
                        "team_id": team_id,
                        "team_abbr": team_abbr,
                        "player_id": player_id,
                        "player": player_name,
                        "starter": None,  # Not available in scoreboard
                        "minutes": None,  # Not available in scoreboard
                        "pts": None, "reb": None, "ast": None, "stl": None, "blk": None, "tov": None,
                        "fgm": None, "fga": None, "fg3m": None, "fg3a": None, "ftm": None, "fta": None,
                        "oreb": None, "dreb": None, "pf": None
                    }
                    
                    # Extract the stat value for this category
                    stat_value = safe_int(leader.get("value"))
                    
                    if category_name == "points":
                        player_stats["pts"] = stat_value
                    elif category_name == "rebounds":
                        player_stats["reb"] = stat_value
                    elif category_name == "assists":
                        player_stats["ast"] = stat_value
                    
                    players_list.append(player_stats)
    
    if not players_list:
        return pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])
    
    df = pd.DataFrame(players_list)
    return coerce_box_dtypes(df)

def fetch_detailed_player_stats(event_id: str, date_str: str) -> pd.DataFrame:
    """Fetch detailed player statistics from ESPN summary/boxscore endpoint"""
    try:
        summary_data = http_get_json(ESPN_SUMMARY, params={"event": event_id})
        
        # Calculate season
        year = int(date_str[:4])
        month = int(date_str[5:7])
        season = year if month >= 10 else year - 1
        
        players_list = []
        
        # Look for boxscore data in the summary
        boxscore = summary_data.get("boxscore", {})
        teams = boxscore.get("teams", [])
        
        for team_data in teams:
            team_info = team_data.get("team", {})
            team_id = safe_int(team_info.get("id"))
            team_abbr = team_info.get("abbreviation")
            
            # Look for player statistics
            players = team_data.get("players", [])
            
            for player_group in players:
                # ESPN groups players by position
                position_players = player_group.get("players", [])
                
                for player in position_players:
                    athlete = player.get("athlete", {})
                    player_id = safe_int(athlete.get("id"))
                    player_name = athlete.get("displayName")
                    
                    # Check if player started
                    starter = player.get("starter", False)
                    
                    # Extract statistics
                    stats = player.get("stats", [])
                    stat_dict = {}
                    
                    # ESPN provides stats as an array, convert to dict
                    if isinstance(stats, list) and len(stats) > 0:
                        # Usually the first element contains the main stats
                        if isinstance(stats[0], dict):
                            stat_dict = stats[0]
                        elif isinstance(stats[0], list):
                            # Sometimes stats are in array format like [pts, reb, ast, ...]
                            stat_labels = ["pts", "reb", "ast", "stl", "blk", "tov", "fgm", "fga", "fg3m", "fg3a", "ftm", "fta", "oreb", "dreb", "pf"]
                            stat_dict = {label: safe_int(stats[0][i]) if i < len(stats[0]) else None for i, label in enumerate(stat_labels)}
                    
                    player_stats = {
                        "event_id": event_id,
                        "date": date_str,
                        "season": season,
                        "team_id": team_id,
                        "team_abbr": team_abbr,
                        "player_id": player_id,
                        "player": player_name,
                        "starter": starter,
                        "minutes": stat_dict.get("minutes") or stat_dict.get("min"),
                        "pts": safe_int(stat_dict.get("points") or stat_dict.get("pts")),
                        "reb": safe_int(stat_dict.get("rebounds") or stat_dict.get("reb")),
                        "ast": safe_int(stat_dict.get("assists") or stat_dict.get("ast")),
                        "stl": safe_int(stat_dict.get("steals") or stat_dict.get("stl")),
                        "blk": safe_int(stat_dict.get("blocks") or stat_dict.get("blk")),
                        "tov": safe_int(stat_dict.get("turnovers") or stat_dict.get("tov")),
                        "fgm": safe_int(stat_dict.get("fieldGoalsMade") or stat_dict.get("fgm")),
                        "fga": safe_int(stat_dict.get("fieldGoalsAttempted") or stat_dict.get("fga")),
                        "fg3m": safe_int(stat_dict.get("threePointFieldGoalsMade") or stat_dict.get("fg3m")),
                        "fg3a": safe_int(stat_dict.get("threePointFieldGoalsAttempted") or stat_dict.get("fg3a")),
                        "ftm": safe_int(stat_dict.get("freeThrowsMade") or stat_dict.get("ftm")),
                        "fta": safe_int(stat_dict.get("freeThrowsAttempted") or stat_dict.get("fta")),
                        "oreb": safe_int(stat_dict.get("offensiveRebounds") or stat_dict.get("oreb")),
                        "dreb": safe_int(stat_dict.get("defensiveRebounds") or stat_dict.get("dreb")),
                        "pf": safe_int(stat_dict.get("fouls") or stat_dict.get("pf"))
                    }
                    
                    players_list.append(player_stats)
        
        if players_list:
            df = pd.DataFrame(players_list)
            return coerce_box_dtypes(df)
        else:
            print(f"No detailed player stats found for event {event_id}")
            return pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])
            
    except Exception as e:
        print(f"Error fetching detailed player stats for event {event_id}: {e}")
        return pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])

def ingest_date_via_espn(date_str: str) -> None:
    """Ingest games and player stats for a specific date using ESPN API"""
    ensure_tables()
    
    print(f"Fetching ESPN data for {date_str}")
    
    # Convert date format for ESPN API
    yyyymmdd = date_str.replace("-", "")
    
    # Get scoreboard
    scoreboard_data = http_get_json(ESPN_SCOREBOARD, params={"dates": yyyymmdd})
    
    # Extract games
    games_df = extract_games_from_scoreboard(scoreboard_data, date_str)
    
    if games_df.empty:
        print(f"No games found for {date_str}")
        return
    
    print(f"Found {len(games_df)} games for {date_str}")
    
    # Load games
    load_df(games_df, "games_daily")
    
    # Fetch detailed player stats for each game
    all_player_stats = []
    
    for _, game_row in games_df.iterrows():
        event_id = game_row["event_id"]
        print(f"Fetching detailed player stats for game {event_id}")
        
        detailed_stats = fetch_detailed_player_stats(event_id, date_str)
        
        if not detailed_stats.empty:
            all_player_stats.append(detailed_stats)
            print(f"Found {len(detailed_stats)} player stat rows for game {event_id}")
        else:
            # Fallback to basic stats from scoreboard
            print(f"Falling back to scoreboard stats for game {event_id}")
            basic_stats = extract_player_stats_from_scoreboard({"events": [{"id": event_id, "competitions": [{"competitors": []}]}]}, date_str)
            if not basic_stats.empty:
                all_player_stats.append(basic_stats)
        
        time.sleep(0.3)  # Be respectful to ESPN's servers
    
    # Load all player stats
    if all_player_stats:
        combined_stats = pd.concat(all_player_stats, ignore_index=True)
        combined_stats = coerce_box_dtypes(combined_stats)
        print(f"Loading {len(combined_stats)} player stat rows")
        load_df(combined_stats, "player_boxscores")
    else:
        print("No player stats found")

def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest NBA data using ESPN API")
    parser.add_argument("--mode", choices=["daily", "backfill"], default="daily")
    parser.add_argument("--start", help="YYYY-MM-DD start date for backfill")
    parser.add_argument("--end", help="YYYY-MM-DD end date for backfill")
    parser.add_argument("--date", help="YYYY-MM-DD specific date")
    args = parser.parse_args()

    if args.date:
        ingest_date_via_espn(args.date)
        print(f"Ingestion complete for {args.date}")
        return

    if args.mode == "daily":
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        ingest_date_via_espn(yesterday.isoformat())
        print(f"Daily ingestion complete for {yesterday.isoformat()}")
        return

    if args.mode == "backfill":
        if not args.start or not args.end:
            print("Error: backfill requires --start and --end dates")
            sys.exit(1)
        
        start_date = datetime.date.fromisoformat(args.start)
        end_date = datetime.date.fromisoformat(args.end)
        
        current_date = start_date
        while current_date <= end_date:
            ingest_date_via_espn(current_date.isoformat())
            current_date += datetime.timedelta(days=1)
            time.sleep(1)  # Be respectful between dates
        
        print(f"Backfill complete from {args.start} to {args.end}")

if __name__ == "__main__":
    main()
