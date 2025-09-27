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

# API endpoints
ESPN_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
YAHOO_SCOREBOARD = "https://api-secure.sports.yahoo.com/v1/editorial/s/scoreboard"

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
    bigquery.SchemaField("data_source", "STRING"),  # Track which API provided the data
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
    bigquery.SchemaField("data_source", "STRING"),  # Track which API provided the data
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
    str_cols = ["status_type", "home_abbr", "away_abbr", "game_uid", "event_id", "data_source"]
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
    str_cols = ["team_abbr", "player", "minutes", "event_id", "data_source"]
    for c in str_cols:
        if c in df.columns and is_object_dtype(df[c]):
            df[c] = df[c].astype("string")
    return df

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
# ESPN Functions (for comprehensive game data)
# -----------------------------------
def extract_games_from_espn(date_str: str) -> pd.DataFrame:
    """Extract games from ESPN API"""
    try:
        yyyymmdd = date_str.replace("-", "")
        scoreboard_data = http_get_json(ESPN_SCOREBOARD, params={"dates": yyyymmdd})
        
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
                "away_score": safe_int(away_team.get("score")),
                "data_source": "ESPN"
            })
        
        if not games_list:
            return pd.DataFrame(columns=[f.name for f in GAMES_SCHEMA])
        
        df = pd.DataFrame(games_list)
        return coerce_games_dtypes(df)
        
    except Exception as e:
        print(f"Error fetching ESPN games for {date_str}: {e}")
        return pd.DataFrame(columns=[f.name for f in GAMES_SCHEMA])

def extract_espn_player_leaders(date_str: str) -> pd.DataFrame:
    """Extract limited player data from ESPN leaders"""
    try:
        yyyymmdd = date_str.replace("-", "")
        scoreboard_data = http_get_json(ESPN_SCOREBOARD, params={"dates": yyyymmdd})
        
        # Calculate season
        year = int(date_str[:4])
        month = int(date_str[5:7])
        season = year if month >= 10 else year - 1
        
        players_list = []
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
                
                # Get leaders (top performers by category)
                leaders = competitor.get("leaders", [])
                seen_players = {}
                
                for leader_category in leaders:
                    category_name = leader_category.get("name", "")
                    category_leaders = leader_category.get("leaders", [])
                    
                    for leader in category_leaders:
                        athlete = leader.get("athlete", {})
                        player_id = safe_int(athlete.get("id"))
                        player_name = athlete.get("displayName")
                        
                        if not player_id:
                            continue
                        
                        # If we haven't seen this player, create a new record
                        if player_id not in seen_players:
                            seen_players[player_id] = {
                                "event_id": event_id,
                                "date": date_str,
                                "season": season,
                                "team_id": team_id,
                                "team_abbr": team_abbr,
                                "player_id": player_id,
                                "player": player_name,
                                "starter": None,
                                "minutes": None,
                                "pts": None, "reb": None, "ast": None, "stl": None, "blk": None, "tov": None,
                                "fgm": None, "fga": None, "fg3m": None, "fg3a": None, "ftm": None, "fta": None,
                                "oreb": None, "dreb": None, "pf": None,
                                "data_source": "ESPN_Leaders"
                            }
                        
                        # Update the stat for this category
                        stat_value = safe_int(leader.get("value"))
                        
                        if category_name == "points":
                            seen_players[player_id]["pts"] = stat_value
                        elif category_name == "rebounds":
                            seen_players[player_id]["reb"] = stat_value
                        elif category_name == "assists":
                            seen_players[player_id]["ast"] = stat_value
                
                # Add all players from this team
                players_list.extend(seen_players.values())
        
        if not players_list:
            return pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])
        
        df = pd.DataFrame(players_list)
        return coerce_box_dtypes(df)
        
    except Exception as e:
        print(f"Error fetching ESPN player leaders for {date_str}: {e}")
        return pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])

# -----------------------------------
# Yahoo Functions (for additional player data)
# -----------------------------------
def extract_yahoo_player_data(date_str: str) -> pd.DataFrame:
    """Extract player data from Yahoo API"""
    try:
        params = {"leagues": "nba", "date": date_str}
        yahoo_data = http_get_json(YAHOO_SCOREBOARD, params=params)
        
        # Calculate season
        year = int(date_str[:4])
        month = int(date_str[5:7])
        season = year if month >= 10 else year - 1
        
        players_list = []
        
        # Navigate Yahoo's complex structure
        service = yahoo_data.get("service", {})
        scoreboard = service.get("scoreboard", {})
        
        # Extract player data from gamebyline (top performers)
        gamebyline = scoreboard.get("gamebyline", {})
        players_data = scoreboard.get("players", {})
        
        for game_id, byline_data in gamebyline.items():
            if not isinstance(byline_data, dict):
                continue
                
            # Extract top performers
            for performer_key in ["top_performer_1", "top_performer_2"]:
                if performer_key in byline_data:
                    performer = byline_data[performer_key]
                    player_id_str = performer.get("player_id", "")
                    
                    if player_id_str and player_id_str in players_data:
                        player_info = players_data[player_id_str]
                        
                        # Extract player stats from performer data
                        stats = performer.get("stats", [])
                        pts = None
                        for stat in stats:
                            if stat.get("name") == "Points Scored":
                                pts = safe_int(stat.get("value"))
                        
                        # Get team info (we'll need to map this from the game data)
                        team_id = safe_int(player_info.get("team_id", "").replace("nba.t.", ""))
                        
                        players_list.append({
                            "event_id": game_id,
                            "date": date_str,
                            "season": season,
                            "team_id": team_id,
                            "team_abbr": None,  # We'll need to map this
                            "player_id": safe_int(player_id_str.replace("nba.p.", "")),
                            "player": player_info.get("display_name"),
                            "starter": None,
                            "minutes": None,
                            "pts": pts,
                            "reb": None, "ast": None, "stl": None, "blk": None, "tov": None,
                            "fgm": None, "fga": None, "fg3m": None, "fg3a": None, "ftm": None, "fta": None,
                            "oreb": None, "dreb": None, "pf": None,
                            "data_source": "Yahoo_TopPerformers"
                        })
        
        if not players_list:
            return pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])
        
        df = pd.DataFrame(players_list)
        return coerce_box_dtypes(df)
        
    except Exception as e:
        print(f"Error fetching Yahoo player data for {date_str}: {e}")
        return pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])

# -----------------------------------
# Main ingestion function
# -----------------------------------
def ingest_date_hybrid(date_str: str) -> None:
    """Ingest NBA data using both ESPN and Yahoo APIs"""
    ensure_tables()
    
    print(f"Fetching NBA data for {date_str} using hybrid approach")
    
    # 1. Get comprehensive game data from ESPN
    espn_games = extract_games_from_espn(date_str)
    
    if espn_games.empty:
        print(f"No games found for {date_str}")
        return
    
    print(f"Found {len(espn_games)} games from ESPN")
    
    # Load ESPN games
    load_df(espn_games, "games_daily")
    
    # 2. Get player data from multiple sources
    all_player_data = []
    
    # ESPN leaders (limited but reliable)
    espn_players = extract_espn_player_leaders(date_str)
    if not espn_players.empty:
        all_player_data.append(espn_players)
        print(f"Found {len(espn_players)} player records from ESPN leaders")
    
    # Yahoo top performers (additional data)
    yahoo_players = extract_yahoo_player_data(date_str)
    if not yahoo_players.empty:
        all_player_data.append(yahoo_players)
        print(f"Found {len(yahoo_players)} player records from Yahoo")
    
    # Combine and load player data
    if all_player_data:
        combined_players = pd.concat(all_player_data, ignore_index=True)
        
        # Remove duplicates (same player_id and event_id)
        combined_players = combined_players.drop_duplicates(subset=["event_id", "player_id"], keep="first")
        
        combined_players = coerce_box_dtypes(combined_players)
        print(f"Loading {len(combined_players)} total player records")
        load_df(combined_players, "player_boxscores")
    else:
        print("No player data found from any source")

def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest NBA data using hybrid ESPN + Yahoo approach")
    parser.add_argument("--mode", choices=["daily", "backfill"], default="daily")
    parser.add_argument("--start", help="YYYY-MM-DD start date for backfill")
    parser.add_argument("--end", help="YYYY-MM-DD end date for backfill")
    parser.add_argument("--date", help="YYYY-MM-DD specific date")
    args = parser.parse_args()

    if args.date:
        ingest_date_hybrid(args.date)
        print(f"Ingestion complete for {args.date}")
        return

    if args.mode == "daily":
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        ingest_date_hybrid(yesterday.isoformat())
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
            ingest_date_hybrid(current_date.isoformat())
            current_date += datetime.timedelta(days=1)
            time.sleep(1)  # Be respectful between dates
        
        print(f"Backfill complete from {args.start} to {args.end}")

if __name__ == "__main__":
    main()
