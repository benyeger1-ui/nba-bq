#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import argparse
import datetime
from typing import List, Optional, Dict, Any, Set
import pytz

import pandas as pd
from pandas.api.types import is_object_dtype

# Import the proper NBA API endpoints for date-based searches
from nba_api.stats.endpoints import leaguegamefinder, scoreboardv2
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

# Timezone handling
ET_TZ = pytz.timezone('US/Eastern')
UTC_TZ = pytz.timezone('UTC')

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

def get_games_for_date_proper(target_date: str) -> List[str]:
    """
    Get games for a specific date using NBA API's proper date-based endpoints.
    This is the correct way to search by date.
    """
    print(f"Searching for games on {target_date} using NBA API date search")
    
    try:
        # Method 1: Try scoreboardv2 (stats API) which allows date parameter
        print("Trying ScoreboardV2 with date parameter...")
        scoreboard_v2 = scoreboardv2.ScoreboardV2(game_date=target_date)
        scoreboard_data = scoreboard_v2.get_dict()
        
        game_ids = []
        
        # Extract game IDs from ScoreboardV2 response
        if 'resultSets' in scoreboard_data:
            for result_set in scoreboard_data['resultSets']:
                if result_set['name'] == 'GameHeader':
                    headers = result_set['headers']
                    rows = result_set['rowSet']
                    
                    # Find the GAME_ID column
                    game_id_idx = headers.index('GAME_ID') if 'GAME_ID' in headers else None
                    
                    if game_id_idx is not None:
                        for row in rows:
                            game_id = row[game_id_idx]
                            if game_id:
                                game_ids.append(str(game_id))
                                print(f"  Found game: {game_id}")
        
        if game_ids:
            print(f"✅ Found {len(game_ids)} games for {target_date} via ScoreboardV2")
            return game_ids
        
    except Exception as e:
        print(f"ScoreboardV2 failed: {e}")
    
    try:
        # Method 2: Try LeagueGameFinder for date range
        print("Trying LeagueGameFinder...")
        
        # Convert date to the format NBA API expects
        date_obj = datetime.datetime.strptime(target_date, "%Y-%m-%d")
        nba_date_format = date_obj.strftime("%m/%d/%Y")
        
        game_finder = leaguegamefinder.LeagueGameFinder(
            date_from_nullable=nba_date_format,
            date_to_nullable=nba_date_format
        )
        
        games_df = game_finder.get_data_frames()[0]
        
        if not games_df.empty:
            # Extract unique game IDs (each game appears twice, once for each team)
            game_ids = games_df['GAME_ID'].unique().tolist()
            game_ids = [str(gid) for gid in game_ids]
            
            print(f"✅ Found {len(game_ids)} games for {target_date} via LeagueGameFinder")
            for game_id in game_ids:
                print(f"  Found game: {game_id}")
            
            return game_ids
        
    except Exception as e:
        print(f"LeagueGameFinder failed: {e}")
    
    # Method 3: Fallback to live API (for very recent games)
    try:
        print("Trying live scoreboard API as fallback...")
        sb = scoreboard.ScoreBoard()
        sb_data = sb.get_dict()
        
        game_ids = []
        if 'scoreboard' in sb_data and 'games' in sb_data['scoreboard']:
            games = sb_data['scoreboard']['games']
            
            for game in games:
                game_id = game.get('gameId')
                game_date_utc = game.get('gameTimeUTC', '')
                
                if game_id and game_date_utc:
                    # Convert UTC to ET and check if it matches target date
                    try:
                        game_dt = datetime.datetime.fromisoformat(game_date_utc.replace('Z', '+00:00'))
                        et_dt = game_dt.astimezone(ET_TZ)
                        et_date = et_dt.date().isoformat()
                        
                        if et_date == target_date:
                            game_ids.append(game_id)
                            teams = f"{game.get('awayTeam', {}).get('teamName', 'Unknown')} @ {game.get('homeTeam', {}).get('teamName', 'Unknown')}"
                            print(f"  Found game: {game_id} - {teams}")
                    except Exception:
                        continue
        
        if game_ids:
            print(f"✅ Found {len(game_ids)} games for {target_date} via live scoreboard")
            return game_ids
        
    except Exception as e:
        print(f"Live scoreboard failed: {e}")
    
    print(f"❌ No games found for {target_date} using any method")
    return []

def get_games_for_date(target_date: str) -> pd.DataFrame:
    """Get games for a specific date using proper NBA API date search"""
    print(f"Starting game search for {target_date}")
    
    # Get game IDs using proper date-based search
    game_ids = get_games_for_date_proper(target_date)
    
    if not game_ids:
        print(f"No games found for {target_date}")
        return pd.DataFrame(columns=[f.name for f in GAMES_SCHEMA])
    
    print(f"Found {len(game_ids)} games for {target_date}: {game_ids}")
    
    # Get detailed game data using the live boxscore API
    games_data = []
    for game_id in game_ids:
        try:
            # Convert stats API game ID to live API format if needed
            live_game_id = convert_to_live_game_id(game_id)
            
            print(f"Fetching details for game {live_game_id}")
            box = boxscore.BoxScore(live_game_id)
            box_data = box.get_dict()
            
            if 'game' in box_data:
                games_data.append(box_data['game'])
            else:
                print(f"No game data found for {live_game_id}")
                
        except Exception as e:
            print(f"Error getting game data for {game_id}: {e}")
            continue
    
    if games_data:
        return extract_games_from_game_data(games_data, target_date)
    else:
        return pd.DataFrame(columns=[f.name for f in GAMES_SCHEMA])

def convert_to_live_game_id(stats_game_id: str) -> str:
    """
    Convert stats API game ID format to live API format if needed.
    Stats API: "0022400112" 
    Live API:  "0022400112" (usually the same, but this function handles edge cases)
    """
    # Usually they're the same format, but this function can handle conversions if needed
    if len(stats_game_id) == 10 and stats_game_id.startswith('002'):
        return stats_game_id
    
    # Add any conversion logic here if formats differ
    return stats_game_id

def extract_games_from_game_data(games_data: List[Dict], target_date: str) -> pd.DataFrame:
    """Extract game information from box score game data"""
    games_rows = []
    
    for game in games_data:
        year = int(target_date[:4])
        month = int(target_date[5:7])
        season = year if month >= 10 else year - 1
        
        home_team = game.get('homeTeam', {})
        away_team = game.get('awayTeam', {})
        arena = game.get('arena', {})
        
        games_rows.append({
            "event_id": game.get('gameId'),
            "game_uid": game.get('gameCode'),
            "date": target_date,
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

# All the utility functions remain the same
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
    """Ingest NBA data for a specific date using proper date-based NBA API search"""
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
    parser = argparse.ArgumentParser(description="Ingest NBA data using proper NBA API date search")
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
