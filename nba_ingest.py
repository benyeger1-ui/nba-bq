#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import argparse
import datetime
from typing import List, Optional, Dict, Any, Set

import pandas as pd
from pandas.api.types import is_object_dtype

from nba_api.live.nba.endpoints import scoreboard, boxscore

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
    bigquery.SchemaField("game_duration", "INT64"),
    bigquery.SchemaField("attendance", "INT64"),
    bigquery.SchemaField("arena_name", "STRING"),
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
    bigquery.SchemaField("fg_pct", "FLOAT64"),
    bigquery.SchemaField("fg3m", "INT64"),
    bigquery.SchemaField("fg3a", "INT64"),
    bigquery.SchemaField("fg3_pct", "FLOAT64"),
    bigquery.SchemaField("ftm", "INT64"),
    bigquery.SchemaField("fta", "INT64"),
    bigquery.SchemaField("ft_pct", "FLOAT64"),
    bigquery.SchemaField("oreb", "INT64"),
    bigquery.SchemaField("dreb", "INT64"),
    bigquery.SchemaField("pf", "INT64"),
    bigquery.SchemaField("plus_minus", "FLOAT64"),
    bigquery.SchemaField("position", "STRING"),
    bigquery.SchemaField("jersey_num", "STRING"),
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
    int_cols = ["season", "home_id", "home_score", "away_id", "away_score", "game_duration", "attendance"]
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    str_cols = ["status_type", "home_abbr", "away_abbr", "game_uid", "event_id", "arena_name"]
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
    float_cols = ["fg_pct", "fg3_pct", "ft_pct", "plus_minus"]
    for c in float_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Float64")
    str_cols = ["team_abbr", "player", "minutes", "event_id", "position", "jersey_num"]
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

def safe_str(x: Any) -> Optional[str]:
    try:
        return str(x) if x is not None and x != "" else None
    except Exception:
        return None

def parse_minutes(minutes_str: str) -> str:
    """Convert NBA API time format PT32M33.00S to readable format"""
    try:
        if not minutes_str or minutes_str == "PT00M00.00S":
            return "0:00"
        
        clean_str = minutes_str.replace("PT", "").replace("S", "")
        if "M" in clean_str:
            parts = clean_str.split("M")
            minutes = int(parts[0])
            seconds = int(float(parts[1])) if len(parts) > 1 and parts[1] else 0
            return f"{minutes}:{seconds:02d}"
        return "0:00"
    except Exception:
        return "0:00"

def build_date_to_games_mapping(target_date: str, search_window: int = 100) -> Dict[str, List[str]]:
    """
    Build a mapping of dates to game IDs by systematically scanning game IDs.
    
    Args:
        target_date: The date we're looking for (YYYY-MM-DD)
        search_window: Number of game IDs to scan in each direction
        
    Returns:
        Dict mapping dates to lists of game IDs
    """
    print(f"Building date-to-games mapping around {target_date}")
    
    date_to_games: Dict[str, List[str]] = {}
    
    # Start from a known working game ID and scan around it
    # From previous tests, we know games around 140-150 are early November
    base_game_id = 130
    
    # Scan backwards and forwards from base
    start_id = max(base_game_id - search_window, 90)  # Don't go below known working range
    end_id = base_game_id + search_window
    
    print(f"Scanning game IDs {start_id} to {end_id}")
    
    target_found = False
    for game_num in range(start_id, end_id + 1):
        game_id = f"002240{game_num:04d}"
        
        try:
            box = boxscore.BoxScore(game_id)
            box_data = box.get_dict()
            
            if 'game' in box_data:
                game_info = box_data['game']
                game_date = game_info.get('gameTimeUTC', '')[:10]
                
                if game_date:
                    if game_date not in date_to_games:
                        date_to_games[game_date] = []
                    date_to_games[game_date].append(game_id)
                    
                    # Print progress for dates close to target
                    if abs((datetime.datetime.strptime(game_date, "%Y-%m-%d") - 
                           datetime.datetime.strptime(target_date, "%Y-%m-%d")).days) <= 5:
                        teams = f"{game_info.get('awayTeam', {}).get('teamName', 'Unknown')} @ {game_info.get('homeTeam', {}).get('teamName', 'Unknown')}"
                        print(f"  {game_id}: {game_date} - {teams}")
                    
                    if game_date == target_date:
                        target_found = True
        
        except Exception:
            # Game ID doesn't exist, continue
            continue
    
    # Print summary
    sorted_dates = sorted(date_to_games.keys())
    if sorted_dates:
        print(f"\nFound games from {sorted_dates[0]} to {sorted_dates[-1]}")
        for date in sorted_dates:
            print(f"  {date}: {len(date_to_games[date])} games")
    
    if target_found:
        print(f"\n✅ Found {len(date_to_games[target_date])} games for {target_date}")
    else:
        print(f"\n❌ No games found for {target_date}")
        closest_dates = [d for d in sorted_dates if abs((datetime.datetime.strptime(d, "%Y-%m-%d") - 
                                                        datetime.datetime.strptime(target_date, "%Y-%m-%d")).days) <= 3]
        if closest_dates:
            print(f"Closest dates with games: {closest_dates}")
    
    return date_to_games

def get_games_for_date(target_date: str) -> pd.DataFrame:
    """Get games for a specific date by building a date mapping"""
    print(f"Searching for games on {target_date}")
    
    # Build the date-to-games mapping
    date_mapping = build_date_to_games_mapping(target_date)
    
    if target_date not in date_mapping:
        print(f"No games found for {target_date}")
        return pd.DataFrame(columns=[f.name for f in GAMES_SCHEMA])
    
    game_ids = date_mapping[target_date]
    print(f"Found {len(game_ids)} games for {target_date}: {game_ids}")
    
    # Get game data for each game ID
    games_data = []
    for game_id in game_ids:
        try:
            box = boxscore.BoxScore(game_id)
            box_data = box.get_dict()
            
            if 'game' in box_data:
                games_data.append(box_data['game'])
        except Exception as e:
            print(f"Error getting game data for {game_id}: {e}")
    
    if games_data:
        return extract_games_from_game_data(games_data, target_date)
    else:
        return pd.DataFrame(columns=[f.name for f in GAMES_SCHEMA])

def extract_games_from_game_data(games_data: List[Dict], date_str: str) -> pd.DataFrame:
    """Extract game information from box score game data"""
    games_rows = []
    
    for game in games_data:
        year = int(date_str[:4])
        month = int(date_str[5:7])
        season = year if month >= 10 else year - 1
        
        home_team = game.get('homeTeam', {})
        away_team = game.get('awayTeam', {})
        arena = game.get('arena', {})
        
        games_rows.append({
            "event_id": game.get('gameId'),
            "game_uid": game.get('gameCode'),
            "date": date_str,
            "season": season,
            "status_type": game.get('gameStatusText', 'Unknown'),
            "home_id": safe_int(home_team.get('teamId')),
            "home_abbr": home_team.get('teamTricode'),
            "home_score": safe_int(home_team.get('score', 0)),
            "away_id": safe_int(away_team.get('teamId')),
            "away_abbr": away_team.get('teamTricode'),
            "away_score": safe_int(away_team.get('score', 0)),
            "game_duration": safe_int(game.get('duration')),
            "attendance": safe_int(game.get('attendance')),
            "arena_name": arena.get('arenaName')
        })
    
    if not games_rows:
        return pd.DataFrame(columns=[f.name for f in GAMES_SCHEMA])
    
    df = pd.DataFrame(games_rows)
    return coerce_games_dtypes(df)

def get_player_stats_for_game(game_id: str, date_str: str) -> pd.DataFrame:
    """Get complete player statistics for a specific game"""
    try:
        box = boxscore.BoxScore(game_id)
        box_data = box.get_dict()
        
        if 'game' not in box_data:
            print(f"No game data found for {game_id}")
            return pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])
        
        game_info = box_data['game']
        
        year = int(date_str[:4])
        month = int(date_str[5:7])
        season = year if month >= 10 else year - 1
        
        players_data = []
        
        for team_type in ['homeTeam', 'awayTeam']:
            if team_type not in game_info:
                continue
                
            team = game_info[team_type]
            team_id = safe_int(team.get('teamId'))
            team_abbr = team.get('teamTricode')
            
            players = team.get('players', [])
            
            for player in players:
                if player.get('status') != 'ACTIVE':
                    continue
                
                player_id = safe_int(player.get('personId'))
                player_name = player.get('name')
                jersey_num = player.get('jerseyNum')
                position = player.get('position')
                starter = player.get('starter') == '1'
                
                stats = player.get('statistics', {})
                
                player_row = {
                    "event_id": game_id,
                    "date": date_str,
                    "season": season,
                    "team_id": team_id,
                    "team_abbr": team_abbr,
                    "player_id": player_id,
                    "player": player_name,
                    "starter": starter,
                    "minutes": parse_minutes(stats.get('minutes', 'PT00M00.00S')),
                    "pts": safe_int(stats.get('points', 0)),
                    "reb": safe_int(stats.get('reboundsTotal', 0)),
                    "ast": safe_int(stats.get('assists', 0)),
                    "stl": safe_int(stats.get('steals', 0)),
                    "blk": safe_int(stats.get('blocks', 0)),
                    "tov": safe_int(stats.get('turnovers', 0)),
                    "fgm": safe_int(stats.get('fieldGoalsMade', 0)),
                    "fga": safe_int(stats.get('fieldGoalsAttempted', 0)),
                    "fg_pct": safe_float(stats.get('fieldGoalsPercentage', 0)),
                    "fg3m": safe_int(stats.get('threePointersMade', 0)),
                    "fg3a": safe_int(stats.get('threePointersAttempted', 0)),
                    "fg3_pct": safe_float(stats.get('threePointersPercentage', 0)),
                    "ftm": safe_int(stats.get('freeThrowsMade', 0)),
                    "fta": safe_int(stats.get('freeThrowsAttempted', 0)),
                    "ft_pct": safe_float(stats.get('freeThrowsPercentage', 0)),
                    "oreb": safe_int(stats.get('reboundsOffensive', 0)),
                    "dreb": safe_int(stats.get('reboundsDefensive', 0)),
                    "pf": safe_int(stats.get('foulsPersonal', 0)),
                    "plus_minus": safe_float(stats.get('plusMinusPoints', 0)),
                    "position": position,
                    "jersey_num": jersey_num
                }
                
                players_data.append(player_row)
        
        if not players_data:
            print(f"No player data found for game {game_id}")
            return pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])
        
        df = pd.DataFrame(players_data)
        return coerce_box_dtypes(df)
        
    except Exception as e:
        print(f"Error getting player stats for game {game_id}: {e}")
        return pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])

def ingest_date_nba_live(date_str: str) -> None:
    """Ingest NBA data for a specific date using NBA Live API"""
    ensure_tables()
    
    print(f"Starting NBA data ingestion for {date_str}")
    
    games_df = get_games_for_date(date_str)
    
    if games_df.empty:
        print(f"No games found for {date_str}")
        return
    
    print(f"Found {len(games_df)} games for {date_str}")
    
    load_df(games_df, "games_daily")
    print(f"Loaded {len(games_df)} games to BigQuery")
    
    all_player_stats = []
    
    for _, game_row in games_df.iterrows():
        game_id = game_row["event_id"]
        print(f"Fetching player stats for game {game_id}")
        
        player_stats = get_player_stats_for_game(game_id, date_str)
        
        if not player_stats.empty:
            all_player_stats.append(player_stats)
            print(f"Found {len(player_stats)} player records for game {game_id}")
        
        time.sleep(0.3)
    
    if all_player_stats:
        combined_stats = pd.concat(all_player_stats, ignore_index=True)
        combined_stats = coerce_box_dtypes(combined_stats)
        
        print(f"Loading {len(combined_stats)} total player records to BigQuery")
        load_df(combined_stats, "player_boxscores")
    else:
        print("No player statistics found")
    
    print(f"Ingestion complete for {date_str}")

def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest NBA data using NBA Live API")
    parser.add_argument("--mode", choices=["daily", "backfill"], default="daily")
    parser.add_argument("--start", help="YYYY-MM-DD start date for backfill")
    parser.add_argument("--end", help="YYYY-MM-DD end date for backfill")
    parser.add_argument("--date", help="YYYY-MM-DD specific date")
    args = parser.parse_args()

    if args.date:
        ingest_date_nba_live(args.date)
        return

    if args.mode == "daily":
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        ingest_date_nba_live(yesterday.isoformat())
        return

    if args.mode == "backfill":
        if not args.start or not args.end:
            print("Error: backfill requires --start and --end dates")
            sys.exit(1)
        
        start_date = datetime.date.fromisoformat(args.start)
        end_date = datetime.date.fromisoformat(args.end)
        
        current_date = start_date
        while current_date <= end_date:
            ingest_date_nba_live(current_date.isoformat())
            current_date += datetime.timedelta(days=1)
            time.sleep(2)
        
        print(f"Backfill complete from {args.start} to {args.end}")

if __name__ == "__main__":
    main()
