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

def normalize_game_date(game_date_str: str, target_date: str) -> str:
    """
    Convert game date to Eastern Time date for consistency.
    NBA games are scheduled in ET, so we want to group by ET date.
    """
    try:
        if not game_date_str:
            return target_date
            
        # Handle different date formats from NBA API
        if 'T' in game_date_str:
            # Full datetime format: "2024-10-28T23:30:00Z" or "2024-10-28T23:30:00.000Z"
            clean_date_str = game_date_str.replace('Z', '+00:00')
            
            # Handle microseconds if present
            if '.' in clean_date_str and len(clean_date_str.split('.')[1]) > 6:
                # Truncate microseconds to 6 digits max
                parts = clean_date_str.split('.')
                microseconds = parts[1][:6] + parts[1][6:].replace('+00:00', '') + '+00:00'
                clean_date_str = parts[0] + '.' + microseconds
            
            game_dt = datetime.datetime.fromisoformat(clean_date_str)
            
            # Ensure it's treated as UTC if no timezone info
            if game_dt.tzinfo is None:
                game_dt = UTC_TZ.localize(game_dt)
        else:
            # If it's just a date, assume it's already in ET
            return game_date_str[:10]
        
        # Convert to Eastern Time
        et_dt = game_dt.astimezone(ET_TZ)
        et_date = et_dt.date().isoformat()
        
        return et_date
        
    except Exception as e:
        print(f"Error parsing game date {game_date_str}: {e}")
        # Fallback: try to extract just the date part
        try:
            return game_date_str[:10]
        except:
            return target_date

def build_optimized_date_range_games_mapping(start_date: str, end_date: str) -> Dict[str, List[str]]:
    """
    Optimized approach: Calculate minimum and maximum game IDs for the entire range,
    then scan once and filter by date range.
    """
    print(f"Building optimized date-to-games mapping for range {start_date} to {end_date}")
    
    start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    
    # Season boundary
    season_boundary = datetime.datetime(2025, 10, 1)
    
    date_to_games: Dict[str, List[str]] = {}
    
    if start_dt < season_boundary and end_dt >= season_boundary:
        # Range crosses seasons - process each season separately
        print("Date range crosses season boundary, processing separately...")
        
        # Process 2024-25 season part
        season_end_2024 = min(end_dt, season_boundary - datetime.timedelta(days=1))
        if start_dt < season_boundary:
            print(f"Processing 2024-25 season: {start_date} to {season_end_2024.strftime('%Y-%m-%d')}")
            mapping1 = scan_season_range(start_date, season_end_2024.strftime('%Y-%m-%d'), 
                                       "002240", 61, datetime.datetime(2024, 10, 22))
            date_to_games.update(mapping1)
        
        # Process 2025-26 season part
        if end_dt >= season_boundary:
            season_start_2025 = max(start_dt, season_boundary)
            print(f"Processing 2025-26 season: {season_start_2025.strftime('%Y-%m-%d')} to {end_date}")
            mapping2 = scan_season_range(season_start_2025.strftime('%Y-%m-%d'), end_date,
                                       "002250", 0, datetime.datetime(2025, 10, 21))
            date_to_games.update(mapping2)
    
    else:
        # Single season range
        if start_dt >= season_boundary:
            # 2025-26 season
            print("Processing 2025-26 season range")
            mapping = scan_season_range(start_date, end_date, "002250", 0, datetime.datetime(2025, 10, 21))
        else:
            # 2024-25 season
            print("Processing 2024-25 season range")
            mapping = scan_season_range(start_date, end_date, "002240", 61, datetime.datetime(2024, 10, 22))
        
        date_to_games.update(mapping)
    
    # Filter results to only include dates within the requested range
    filtered_mapping = {}
    for date_str, game_ids in date_to_games.items():
        date_dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        if start_dt <= date_dt <= end_dt:
            filtered_mapping[date_str] = game_ids
    
    total_games = sum(len(games) for games in filtered_mapping.values())
    print(f"\nOptimized scan complete:")
    print(f"  Date range: {start_date} to {end_date}")
    print(f"  Total games found: {total_games} across {len(filtered_mapping)} dates")
    
    return filtered_mapping

def scan_season_range(start_date: str, end_date: str, season_prefix: str, 
                     first_game_id: int, season_start: datetime.datetime) -> Dict[str, List[str]]:
    """
    Scan a single season range efficiently using fixed minimum game IDs.
    """
    start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    
    date_to_games: Dict[str, List[str]] = {}
    
    # Skip if entire range is before season start
    if end_dt < season_start:
        print(f"Range {start_date} to {end_date} is before season start")
        return date_to_games
    
    # Use fixed minimum game IDs for comprehensive coverage
    if season_prefix == "002240":
        # 2024-25 season: always start from 0022400000
        start_id = 0
    elif season_prefix == "002250":
        # 2025-26 season: always start from 0022500000
        start_id = 0
    else:
        # Fallback for other seasons
        start_id = first_game_id
    
    # Calculate maximum game ID based on the end date
    max_days_from_start = max(0, (end_dt - season_start).days)
    max_game_estimate = start_id + (max_days_from_start * 25)  # Increased to 25 games/day maximum
    end_id = max_game_estimate + 200  # Larger safety buffer
    
    print(f"  Scanning {season_prefix} range: game IDs {start_id} to {end_id}")
    print(f"  Date range: {start_date} to {end_date}")
    print(f"  Fixed minimum: {season_prefix}{start_id:04d}")
    
    games_found = 0
    range_games_found = 0
    
    for game_num in range(start_id, end_id + 1):
        game_id = f"{season_prefix}{game_num:04d}"
        
        try:
            box = boxscore.BoxScore(game_id)
            box_data = box.get_dict()
            
            if 'game' in box_data:
                game_info = box_data['game']
                game_date_utc = game_info.get('gameTimeUTC', '')
                
                if game_date_utc:
                    normalized_date = normalize_game_date(game_date_utc, start_date)
                    normalized_dt = datetime.datetime.strptime(normalized_date, "%Y-%m-%d")
                    
                    # Only include games within our target range
                    if start_dt <= normalized_dt <= end_dt:
                        if normalized_date not in date_to_games:
                            date_to_games[normalized_date] = []
                        date_to_games[normalized_date].append(game_id)
                        range_games_found += 1
                    
                    games_found += 1
                    
                    # Show progress for range games
                    if start_dt <= normalized_dt <= end_dt:
                        teams = f"{game_info.get('awayTeam', {}).get('teamName', 'Unknown')} @ {game_info.get('homeTeam', {}).get('teamName', 'Unknown')}"
                        print(f"  ✓ {game_id}: {normalized_date} - {teams}")
        
        except Exception:
            continue  # Game doesn't exist
    
    print(f"  Season scan complete: {range_games_found} games in range ({games_found} total games found)")
    return date_to_games

def build_date_to_games_mapping(target_date: str, search_window: int = 150) -> Dict[str, List[str]]:
    """
    Build a mapping of dates to game IDs with precise date estimation.
    Updated to handle season transitions automatically.
    """
    print(f"Building date-to-games mapping for {target_date}")
    
    date_to_games: Dict[str, List[str]] = {}
    
    # Skip scoreboard API for historical dates (it only returns current/future games)
    target_dt = datetime.datetime.strptime(target_date, "%Y-%m-%d")
    today = datetime.datetime.now()
    
    if target_dt.date() >= today.date():
        # Only try scoreboard for today/future dates
        try:
            print(f"Trying scoreboard API for current/future date {target_date}")
            sb = scoreboard.ScoreBoard()
            sb_data = sb.get_dict()
            
            if 'scoreboard' in sb_data and 'games' in sb_data['scoreboard']:
                games = sb_data['scoreboard']['games']
                print(f"Scoreboard API returned {len(games)} games")
                
                for game in games:
                    game_id = game.get('gameId')
                    game_date_utc = game.get('gameTimeUTC', '')
                    
                    if game_id and game_date_utc:
                        normalized_date = normalize_game_date(game_date_utc, target_date)
                        
                        if normalized_date not in date_to_games:
                            date_to_games[normalized_date] = []
                        date_to_games[normalized_date].append(game_id)
                        
                        print(f"  {game_id}: {game_date_utc} -> {normalized_date}")
                
                if target_date in date_to_games:
                    print(f"Found {len(date_to_games[target_date])} games via scoreboard API")
                    return date_to_games
            
        except Exception as e:
            print(f"Scoreboard API failed: {e}")
    else:
        print(f"Skipping scoreboard API for historical date {target_date}")
    
    # Use the optimized range scan for single dates too
    return build_optimized_date_range_games_mapping(target_date, target_date)

def ingest_date_range_nba_live(start_date: str, end_date: str) -> None:
    """
    Efficiently ingest NBA data for a date range using optimized scanning.
    """
    ensure_tables()
    
    print(f"Starting optimized NBA data ingestion for range {start_date} to {end_date}")
    
    # Get all games for the entire range at once using optimized method
    date_mapping = build_optimized_date_range_games_mapping(start_date, end_date)
    
    if not date_mapping:
        print(f"No games found for date range {start_date} to {end_date}")
        return
    
    all_games_data = []
    all_player_stats = []
    
    # Process each date's games
    total_games = sum(len(game_ids) for game_ids in date_mapping.values())
    processed_games = 0
    
    for date_str in sorted(date_mapping.keys()):
        game_ids = date_mapping[date_str]
        print(f"\nProcessing {len(game_ids)} games for {date_str}")
        
        # Get game data
        for game_id in game_ids:
            retry_count = 0
            max_retries = 3
            
            while retry_count < max_retries:
                try:
                    box = boxscore.BoxScore(game_id)
                    box_data = box.get_dict()
                    
                    if 'game' in box_data:
                        all_games_data.append((box_data['game'], date_str))
                        
                        # Get player stats for this game
                        player_stats = get_player_stats_for_game(game_id, date_str)
                        if not player_stats.empty:
                            all_player_stats.append(player_stats)
                            print(f"  ✓ {game_id}: {len(player_stats)} players")
                        else:
                            print(f"  ✓ {game_id}: game data only (no player stats)")
                    else:
                        print(f"  ⚠ {game_id}: no game data in response")
                    
                    break  # Success, exit retry loop
                    
                except Exception as e:
                    retry_count += 1
                    error_msg = str(e)
                    
                    if "Expecting value: line 1 column 1" in error_msg:
                        if retry_count < max_retries:
                            print(f"  🔄 {game_id}: Empty response, retrying ({retry_count}/{max_retries})")
                            time.sleep(1.0 * retry_count)  # Exponential backoff
                        else:
                            print(f"  ❌ {game_id}: Failed after {max_retries} retries (empty response)")
                    else:
                        print(f"  ❌ {game_id}: {error_msg}")
                        break  # Don't retry for other errors
            
            processed_games += 1
            if processed_games % 10 == 0:
                print(f"  Progress: {processed_games}/{total_games} games processed")
            
            # Adaptive delay based on success rate
            if retry_count > 0:
                time.sleep(0.5)  # Longer delay after errors
            else:
                time.sleep(0.2)  # Normal delay
    
    # Bulk process games
    if all_games_data:
        print(f"\nProcessing {len(all_games_data)} total games for bulk insert")
        
        # Group by date and create DataFrames
        games_by_date = {}
        for game_data, date_str in all_games_data:
            if date_str not in games_by_date:
                games_by_date[date_str] = []
            games_by_date[date_str].append(game_data)
        
        # Create and insert games DataFrame
        all_games_rows = []
        for date_str, games_data in games_by_date.items():
            date_df = extract_games_from_game_data(games_data, date_str)
            if not date_df.empty:
                all_games_rows.append(date_df)
        
        if all_games_rows:
            combined_games = pd.concat(all_games_rows, ignore_index=True)
            load_df(combined_games, "games_daily")
            print(f"✅ Bulk loaded {len(combined_games)} games to BigQuery")
    
    # Bulk process player stats
    if all_player_stats:
        print(f"Processing {len(all_player_stats)} player stat sets for bulk insert")
        combined_stats = pd.concat(all_player_stats, ignore_index=True)
        load_df(combined_stats, "player_boxscores")
        print(f"✅ Bulk loaded {len(combined_stats)} player records to BigQuery")
    
    print(f"\n🎉 Optimized bulk ingestion complete for {start_date} to {end_date}")
    print(f"   Games: {len(all_games_data)} across {len(date_mapping)} dates")
    print(f"   Players: {len(combined_stats) if all_player_stats else 0} total records")

def extract_games_from_game_data(games_data: List[Dict], target_date: str) -> pd.DataFrame:
    """Extract game information from box score game data with proper date handling"""
    games_rows = []
    
    for game in games_data:
        # Use the target date for consistency (since we've already filtered by date)
        date_for_season = target_date
        year = int(date_for_season[:4])
        month = int(date_for_season[5:7])
        season = year if month >= 10 else year - 1
        
        home_team = game.get('homeTeam', {})
        away_team = game.get('awayTeam', {})
        arena = game.get('arena', {})
        
        games_rows.append({
            "event_id": game.get('gameId'),
            "game_uid": game.get('gameCode'),
            "date": target_date,  # Use the target date consistently
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
    """Get complete player statistics for a specific game with retry logic"""
    max_retries = 2
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            box = boxscore.BoxScore(game_id)
            box_data = box.get_dict()
            
            if 'game' not in box_data:
                return pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])
            
            game_info = box_data['game']
            
            # Use the date_str parameter consistently
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
                        "date": date_str,  # Use the passed date_str consistently
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
                return pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])
            
            df = pd.DataFrame(players_data)
            return coerce_box_dtypes(df)
            
        except Exception as e:
            retry_count += 1
            error_msg = str(e)
            
            if "Expecting value: line 1 column 1" in error_msg and retry_count < max_retries:
                time.sleep(0.5 * retry_count)  # Short delay before retry
                continue
            else:
                # Don't log player stats errors here as they're handled in the main loop
                return pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])
    
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
        
        print(f"Loading {len(combined_stats)} total player records to BigQuery")
        load_df(combined_stats, "player_boxscores")
    else:
        print("No player statistics found")
    
    print(f"Ingestion complete for {date_str}")

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

def get_games_for_date(target_date: str) -> pd.DataFrame:
    """Get games for a specific date with improved date handling"""
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

def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest NBA data using NBA Live API")
    parser.add_argument("--mode", choices=["daily", "backfill"], default="daily")
    parser.add_argument("--start", help="YYYY-MM-DD start date for backfill")
    parser.add_argument("--end", help="YYYY-MM-DD end date for backfill")
    parser.add_argument("--date", help="YYYY-MM-DD specific date")
    args = parser.parse_args()

    if args.date:
        # Single date processing
        ingest_date_nba_live(args.date)
        return

    if args.mode == "daily":
        # Daily processing (yesterday)
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        ingest_date_nba_live(yesterday.isoformat())
        return

    if args.mode == "backfill":
        if not args.start or not args.end:
            print("Error: backfill requires --start and --end dates")
            sys.exit(1)
        
        # Always use efficient range-based ingestion for backfill
        start_date = datetime.date.fromisoformat(args.start)
        end_date = datetime.date.fromisoformat(args.end)
        date_diff = (end_date - start_date).days + 1
        
        if date_diff <= 60:
            # Small to medium range, use efficient bulk method
            print(f"Processing date range: {args.start} to {args.end} ({date_diff} days)")
            ingest_date_range_nba_live(args.start, args.end)
        else:
            # Large range, break into chunks
            print(f"Processing large date range: {args.start} to {args.end} ({date_diff} days)")
            print("Breaking into 60-day chunks for efficiency...")
            
            current_start = start_date
            while current_start <= end_date:
                chunk_end = min(current_start + datetime.timedelta(days=59), end_date)
                
                print(f"\nProcessing chunk: {current_start.isoformat()} to {chunk_end.isoformat()}")
                ingest_date_range_nba_live(current_start.isoformat(), chunk_end.isoformat())
                
                current_start = chunk_end + datetime.timedelta(days=1)
                
                if current_start <= end_date:
                    print("Waiting 10 seconds between chunks...")
                    time.sleep(10)
        
        print(f"\nBackfill complete from {args.start} to {args.end}")

if __name__ == "__main__":
    main()
