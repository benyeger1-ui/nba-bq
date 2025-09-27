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

# ESPN API endpoints - let's try the most reliable ones
ESPN_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"

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

def fetch_boxscore_data(event_id: str) -> Dict[str, Any]:
    """Try multiple ESPN endpoints to get detailed box score data"""
    
    endpoints = [
        f"https://site.web.api.espn.com/apis/site/v2/sports/basketball/nba/boxscore?event={event_id}",
        f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={event_id}",
        f"https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}",
    ]
    
    for url in endpoints:
        try:
            print(f"  Trying: {url}")
            response = requests.get(url, headers=HEADERS, timeout=15)
            if response.status_code == 200:
                data = response.json()
                print(f"  Success! Got data with keys: {list(data.keys())}")
                return data
            else:
                print(f"  Failed with status: {response.status_code}")
        except Exception as e:
            print(f"  Error: {e}")
            continue
    
    return {}

def extract_team_statistics(team_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract team-level statistics from ESPN competitor data"""
    stats_dict = {}
    
    # ESPN provides statistics as a list of dictionaries
    statistics = team_data.get("statistics", [])
    for stat in statistics:
        name = stat.get("name", "").lower()
        abbr = stat.get("abbreviation", "").lower()
        value = stat.get("displayValue", "")
        
        # Map common field names
        if name in ["fieldgoalsmade", "fgm"] or abbr == "fgm":
            stats_dict["fgm"] = safe_int(value)
        elif name in ["fieldgoalsattempted", "fga"] or abbr == "fga":
            stats_dict["fga"] = safe_int(value)
        elif name in ["freethrowsmade", "ftm"] or abbr == "ftm":
            stats_dict["ftm"] = safe_int(value)
        elif name in ["freethrowsattempted", "fta"] or abbr == "fta":
            stats_dict["fta"] = safe_int(value)
        elif name in ["threepointfieldgoalsmade", "3pm"] or abbr in ["3pm", "3p"]:
            stats_dict["fg3m"] = safe_int(value)
        elif name in ["threepointfieldgoalsattempted", "3pa"] or abbr == "3pa":
            stats_dict["fg3a"] = safe_int(value)
        elif name == "rebounds" or abbr == "reb":
            stats_dict["reb"] = safe_int(value)
        elif name == "assists" or abbr == "ast":
            stats_dict["ast"] = safe_int(value)
    
    return stats_dict

def extract_player_stats_from_leaders(event_data: Dict[str, Any], event_id: str, date_str: str) -> pd.DataFrame:
    """Extract player statistics from ESPN's leaders data in scoreboard"""
    players_list = []
    
    # Calculate season
    year = int(date_str[:4])
    month = int(date_str[5:7])
    season = year if month >= 10 else year - 1
    
    # Look for the event in the events array
    events = event_data.get("events", [])
    target_event = None
    
    for event in events:
        if event.get("id") == event_id:
            target_event = event
            break
    
    if not target_event:
        print(f"  Could not find event {event_id} in response")
        return pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])
    
    competitions = target_event.get("competitions", [])
    if not competitions:
        print(f"  No competitions found for event {event_id}")
        return pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])
    
    comp = competitions[0]
    competitors = comp.get("competitors", [])
    
    print(f"  Found {len(competitors)} teams")
    
    for competitor in competitors:
        team_info = competitor.get("team", {})
        team_id = safe_int(team_info.get("id"))
        team_abbr = team_info.get("abbreviation")
        
        print(f"  Processing team: {team_abbr} (ID: {team_id})")
        
        # Extract team statistics for context
        team_stats = extract_team_statistics(competitor)
        
        # Get leaders (top performers by category)
        leaders = competitor.get("leaders", [])
        print(f"    Found {len(leaders)} leader categories")
        
        # Track players we've seen to avoid duplicates
        seen_players = {}
        
        for leader_category in leaders:
            category_name = leader_category.get("name", "")
            category_leaders = leader_category.get("leaders", [])
            
            print(f"    Category '{category_name}': {len(category_leaders)} leaders")
            
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
                        "starter": None,  # Not available in leaders data
                        "minutes": None,  # Not available in leaders data
                        "pts": None, "reb": None, "ast": None, "stl": None, "blk": None, "tov": None,
                        "fgm": None, "fga": None, "fg3m": None, "fg3a": None, "ftm": None, "fta": None,
                        "oreb": None, "dreb": None, "pf": None
                    }
                
                # Update the stat for this category
                stat_value = safe_int(leader.get("value"))
                display_value = leader.get("displayValue", "")
                
                if category_name == "points":
                    seen_players[player_id]["pts"] = stat_value
                elif category_name == "rebounds":
                    seen_players[player_id]["reb"] = stat_value
                elif category_name == "assists":
                    seen_players[player_id]["ast"] = stat_value
                elif category_name == "rating":
                    # The rating often contains multiple stats in displayValue
                    # Example: "25 PTS, 9 REB, 7 AST, 5 STL, 5 BLK"
                    if "PTS" in display_value:
                        pts_match = display_value.split("PTS")[0].strip().split()[-1]
                        seen_players[player_id]["pts"] = safe_int(pts_match)
                    if "REB" in display_value:
                        reb_match = display_value.split("REB")[0].split(",")[-1].strip().split()[-1]
                        seen_players[player_id]["reb"] = safe_int(reb_match)
                    if "AST" in display_value:
                        ast_match = display_value.split("AST")[0].split(",")[-1].strip().split()[-1]
                        seen_players[player_id]["ast"] = safe_int(ast_match)
                    if "STL" in display_value:
                        stl_match = display_value.split("STL")[0].split(",")[-1].strip().split()[-1]
                        seen_players[player_id]["stl"] = safe_int(stl_match)
                    if "BLK" in display_value:
                        blk_match = display_value.split("BLK")[0].split(",")[-1].strip().split()[-1]
                        seen_players[player_id]["blk"] = safe_int(blk_match)
        
        # Add all players from this team
        players_list.extend(seen_players.values())
        print(f"    Added {len(seen_players)} players from {team_abbr}")
    
    if not players_list:
        print(f"  No player stats extracted for event {event_id}")
        return pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])
    
    print(f"  Total players extracted: {len(players_list)}")
    df = pd.DataFrame(players_list)
    return coerce_box_dtypes(df)

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

def ingest_date_via_espn(date_str: str) -> None:
    """Ingest games and player stats for a specific date using ESPN API"""
    ensure_tables()
    
    print(f"Fetching ESPN data for {date_str}")
    
    # Convert date format for ESPN API
    yyyymmdd = date_str.replace("-", "")
    
    # Get scoreboard (this contains both games and leader stats)
    scoreboard_data = http_get_json(ESPN_SCOREBOARD, params={"dates": yyyymmdd})
    
    # Extract games
    games_df = extract_games_from_scoreboard(scoreboard_data, date_str)
    
    if games_df.empty:
        print(f"No games found for {date_str}")
        return
    
    print(f"Found {len(games_df)} games for {date_str}")
    
    # Load games
    load_df(games_df, "games_daily")
    
    # Extract player stats from the same scoreboard data
    all_player_stats = []
    
    for _, game_row in games_df.iterrows():
        event_id = game_row["event_id"]
        print(f"Extracting player stats for game {event_id}")
        
        # Extract from leaders in scoreboard data
        player_stats = extract_player_stats_from_leaders(scoreboard_data, event_id, date_str)
        
        if not player_stats.empty:
            all_player_stats.append(player_stats)
            print(f"  Found {len(player_stats)} player records")
        else:
            print(f"  No player stats found for game {event_id}")
    
    # Load all player stats
    if all_player_stats:
        combined_stats = pd.concat(all_player_stats, ignore_index=True)
        combined_stats = coerce_box_dtypes(combined_stats)
        print(f"Loading {len(combined_stats)} total player stat rows")
        load_df(combined_stats, "player_boxscores")
    else:
        print("No player stats found for any games")

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
