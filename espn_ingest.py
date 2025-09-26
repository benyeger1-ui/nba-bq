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

from google.cloud import bigquery
from google.oauth2 import service_account

# NBA API imports
from nba_api.live.nba.endpoints import scoreboard
from nba_api.live.nba.endpoints import boxscore
from nba_api.stats.endpoints import scoreboardv2, boxscoretraditionalv2


# -----------------------------
# Config via environment
# -----------------------------

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET = os.environ.get("BQ_DATASET", "nba_data")

SA_INFO = json.loads(os.environ["GCP_SA_KEY"])
CREDS = service_account.Credentials.from_service_account_info(SA_INFO)
BQ = bigquery.Client(project=PROJECT_ID, credentials=CREDS)

# -----------------------------
# BigQuery schemas  
# -----------------------------
GAMES_SCHEMA = [
    bigquery.SchemaField("game_id", "STRING"),
    bigquery.SchemaField("game_date", "DATE"),
    bigquery.SchemaField("season", "INT64"),
    bigquery.SchemaField("home_team_id", "INT64"),
    bigquery.SchemaField("home_team_name", "STRING"),
    bigquery.SchemaField("home_team_abbr", "STRING"),
    bigquery.SchemaField("home_score", "INT64"),
    bigquery.SchemaField("away_team_id", "INT64"),
    bigquery.SchemaField("away_team_name", "STRING"),
    bigquery.SchemaField("away_team_abbr", "STRING"),
    bigquery.SchemaField("away_score", "INT64"),
    bigquery.SchemaField("game_status", "STRING"),
]

BOX_SCHEMA = [
    bigquery.SchemaField("game_id", "STRING"),
    bigquery.SchemaField("game_date", "DATE"),
    bigquery.SchemaField("season", "INT64"),
    bigquery.SchemaField("team_id", "INT64"),
    bigquery.SchemaField("team_name", "STRING"),
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
        print(f"Dataset {ds_id} exists")
    except Exception:
        dataset = bigquery.Dataset(ds_id)
        BQ.create_dataset(dataset)
        print(f"Created dataset {ds_id}")

def ensure_tables() -> None:
    ensure_dataset()

    games_table_id = f"{PROJECT_ID}.{DATASET}.games_daily"
    try:
        BQ.get_table(games_table_id)
        print("Games table exists")
    except Exception:
        table = bigquery.Table(games_table_id, schema=GAMES_SCHEMA)
        BQ.create_table(table)
        print("Created games table")

    box_table_id = f"{PROJECT_ID}.{DATASET}.player_boxscores"
    try:
        BQ.get_table(box_table_id)
        print("Player boxscores table exists")
    except Exception:
        table = bigquery.Table(box_table_id, schema=BOX_SCHEMA)
        BQ.create_table(table)
        print("Created player boxscores table")

def load_df(df: pd.DataFrame, table: str) -> None:
    if df is None or df.empty:
        print(f"No data to load for {table}")
        return
    
    table_id = f"{PROJECT_ID}.{DATASET}.{table}"
    schema = GAMES_SCHEMA if table == "games_daily" else BOX_SCHEMA

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    )
    
    job = BQ.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows to {table}")

# -----------------------------
# Utilities
# -----------------------------
def safe_int(x: Any) -> Optional[int]:
    try:
        return int(x) if x is not None and x != "" and str(x).strip() != "" else None
    except (ValueError, TypeError):
        return None

def safe_float(x: Any) -> Optional[float]:
    try:
        return float(x) if x is not None and x != "" and str(x).strip() != "" else None
    except (ValueError, TypeError):
        return None

# -----------------------------
# NBA API Functions
# -----------------------------
def get_games_for_date(date_str: str) -> List[Dict[str, Any]]:
    """
    Get games for a specific date using NBA API
    date_str format: YYYY-MM-DD (MM/DD/YYYY for NBA API)
    """
    try:
        # Convert date format for NBA API
        date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d')
        nba_date_str = date_obj.strftime('%m/%d/%Y')
        
        print(f"Getting games for {date_str} (NBA format: {nba_date_str})")
        
        # Use NBA stats API for historical data
        scoreboard_data = scoreboardv2.ScoreboardV2(game_date=nba_date_str)
        games_df = scoreboard_data.get_data_frames()[0]  # GameHeader dataframe
        
        if games_df.empty:
            print(f"No games found for {date_str}")
            return []
        
        games = []
        for _, game_row in games_df.iterrows():
            # Only process completed games
            game_status = str(game_row.get('GAME_STATUS_TEXT', ''))
            if 'Final' not in game_status:
                continue
            
            game_info = {
                'game_id': str(game_row['GAME_ID']),
                'game_date': date_str,
                'season': safe_int(game_row.get('SEASON', 2025)),
                'home_team_id': safe_int(game_row.get('HOME_TEAM_ID')),
                'home_team_name': str(game_row.get('HOME_TEAM_NAME', '')),
                'home_team_abbr': str(game_row.get('HOME_TEAM_ABBREVIATION', '')),
                'home_score': safe_int(game_row.get('PTS_HOME')),
                'away_team_id': safe_int(game_row.get('VISITOR_TEAM_ID')),
                'away_team_name': str(game_row.get('VISITOR_TEAM_NAME', '')),
                'away_team_abbr': str(game_row.get('VISITOR_TEAM_ABBREVIATION', '')),
                'away_score': safe_int(game_row.get('PTS_AWAY')),
                'game_status': game_status,
            }
            games.append(game_info)
        
        print(f"Found {len(games)} completed games for {date_str}")
        return games
        
    except Exception as e:
        print(f"Error getting games for {date_str}: {e}")
        return []

def get_player_stats_for_game(game_id: str, game_date: str, season: int) -> List[Dict[str, Any]]:
    """
    Get player statistics for a specific game using NBA API
    """
    try:
        print(f"Getting player stats for game {game_id}")
        
        # Get traditional box score stats
        box_score_data = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
        player_stats_df = box_score_data.get_data_frames()[0]  # PlayerStats dataframe
        
        if player_stats_df.empty:
            print(f"No player stats found for game {game_id}")
            return []
        
        player_stats = []
        for _, player_row in player_stats_df.iterrows():
            # Skip players who didn't play
            minutes = str(player_row.get('MIN', ''))
            if not minutes or minutes == '0:00' or minutes.strip() == '':
                continue
            
            player_stat = {
                'game_id': game_id,
                'game_date': game_date,
                'season': season,
                'team_id': safe_int(player_row.get('TEAM_ID')),
                'team_name': str(player_row.get('TEAM_NAME', '')),
                'team_abbr': str(player_row.get('TEAM_ABBREVIATION', '')),
                'player_id': safe_int(player_row.get('PLAYER_ID')),
                'player_name': str(player_row.get('PLAYER_NAME', '')),
                'starter': str(player_row.get('START_POSITION', '')) != '',
                'minutes': minutes,
                'pts': safe_int(player_row.get('PTS')),
                'fgm': safe_int(player_row.get('FGM')),
                'fga': safe_int(player_row.get('FGA')),
                'fg_pct': safe_float(player_row.get('FG_PCT')),
                'fg3m': safe_int(player_row.get('FG3M')),
                'fg3a': safe_int(player_row.get('FG3A')),
                'fg3_pct': safe_float(player_row.get('FG3_PCT')),
                'ftm': safe_int(player_row.get('FTM')),
                'fta': safe_int(player_row.get('FTA')),
                'ft_pct': safe_float(player_row.get('FT_PCT')),
                'oreb': safe_int(player_row.get('OREB')),
                'dreb': safe_int(player_row.get('DREB')),
                'reb': safe_int(player_row.get('REB')),
                'ast': safe_int(player_row.get('AST')),
                'stl': safe_int(player_row.get('STL')),
                'blk': safe_int(player_row.get('BLK')),
                'tov': safe_int(player_row.get('TO')),
                'pf': safe_int(player_row.get('PF')),
                'plus_minus': safe_int(player_row.get('PLUS_MINUS')),
            }
            player_stats.append(player_stat)
        
        print(f"Found {len(player_stats)} player stats for game {game_id}")
        return player_stats
        
    except Exception as e:
        print(f"Error getting player stats for game {game_id}: {e}")
        return []

def get_live_scoreboard() -> List[Dict[str, Any]]:
    """
    Get today's games using live NBA API (for current/recent games)
    """
    try:
        print("Getting live scoreboard data")
        
        # Get live scoreboard
        live_scoreboard = scoreboard.ScoreBoard()
        scoreboard_data = live_scoreboard.get_dict()
        
        games = []
        today = datetime.date.today().strftime('%Y-%m-%d')
        
        for game in scoreboard_data.get('scoreboard', {}).get('games', []):
            # Only process completed games
            game_status = game.get('gameStatusText', '')
            if 'Final' not in game_status:
                continue
            
            home_team = game.get('homeTeam', {})
            away_team = game.get('awayTeam', {})
            
            game_info = {
                'game_id': str(game.get('gameId')),
                'game_date': today,
                'season': safe_int(game.get('season')),
                'home_team_id': safe_int(home_team.get('teamId')),
                'home_team_name': home_team.get('teamName', ''),
                'home_team_abbr': home_team.get('teamTricode', ''),
                'home_score': safe_int(home_team.get('score')),
                'away_team_id': safe_int(away_team.get('teamId')),
                'away_team_name': away_team.get('teamName', ''),
                'away_team_abbr': away_team.get('teamTricode', ''),
                'away_score': safe_int(away_team.get('score')),
                'game_status': game_status,
            }
            games.append(game_info)
        
        print(f"Found {len(games)} completed games from live scoreboard")
        return games
        
    except Exception as e:
        print(f"Error getting live scoreboard: {e}")
        return []

def get_live_boxscore_stats(game_id: str, game_date: str, season: int) -> List[Dict[str, Any]]:
    """
    Get player stats using live NBA API
    """
    try:
        print(f"Getting live boxscore for game {game_id}")
        
        # Get live boxscore
        live_boxscore = boxscore.BoxScore(game_id=game_id)
        boxscore_data = live_boxscore.get_dict()
        
        player_stats = []
        
        # Process both teams
        game_data = boxscore_data.get('game', {})
        home_team = game_data.get('homeTeam', {})
        away_team = game_data.get('awayTeam', {})
        
        for team in [home_team, away_team]:
            team_id = safe_int(team.get('teamId'))
            team_name = team.get('teamName', '')
            team_abbr = team.get('teamTricode', '')
            
            for player in team.get('players', []):
                # Skip players who didn't play
                if not player.get('played'):
                    continue
                
                stats = player.get('statistics', {})
                minutes = stats.get('minutes', '')
                
                if not minutes or minutes == 'PT0M':
                    continue
                
                player_stat = {
                    'game_id': game_id,
                    'game_date': game_date,
                    'season': season,
                    'team_id': team_id,
                    'team_name': team_name,
                    'team_abbr': team_abbr,
                    'player_id': safe_int(player.get('personId')),
                    'player_name': player.get('name', ''),
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
        
        print(f"Found {len(player_stats)} player stats from live boxscore for game {game_id}")
        return player_stats
        
    except Exception as e:
        print(f"Error getting live boxscore for game {game_id}: {e}")
        return []

# -----------------------------
# Data Processing
# -----------------------------
def process_date(date_str: str) -> tuple:
    """
    Process a single date using NBA API
    date_str format: YYYY-MM-DD
    """
    print(f"\n=== Processing {date_str} with NBA API ===")
    
    # Check if this is today (use live API) or historical (use stats API)
    today = datetime.date.today().strftime('%Y-%m-%d')
    is_today = date_str == today
    
    if is_today:
        print("Using live NBA API for today's games")
        games = get_live_scoreboard()
    else:
        print("Using NBA stats API for historical games")
        games = get_games_for_date(date_str)
    
    if not games:
        print(f"No completed games found for {date_str}")
        return pd.DataFrame(), pd.DataFrame()
    
    # Get player stats for each game
    all_player_stats = []
    
    for game in games:
        try:
            game_id = game['game_id']
            season = game['season']
            
            if is_today:
                # Use live boxscore API
                player_stats = get_live_boxscore_stats(game_id, date_str, season)
            else:
                # Use traditional boxscore API
                player_stats = get_player_stats_for_game(game_id, date_str, season)
            
            all_player_stats.extend(player_stats)
            
            # Rate limiting to be respectful
            time.sleep(0.6)
            
        except Exception as e:
            print(f"Error processing game {game['game_id']}: {e}")
    
    # Create DataFrames
    games_df = pd.DataFrame(games) if games else pd.DataFrame()
    players_df = pd.DataFrame(all_player_stats) if all_player_stats else pd.DataFrame()
    
    # Apply type coercion for games
    if not games_df.empty:
        games_df["game_date"] = pd.to_datetime(games_df["game_date"]).dt.date
        
        int_cols = ["season", "home_team_id", "home_score", "away_team_id", "away_score"]
        for col in int_cols:
            if col in games_df.columns:
                games_df[col] = pd.to_numeric(games_df[col], errors="coerce").astype("Int64")
        
        str_cols = ["game_id", "home_team_name", "home_team_abbr", "away_team_name", "away_team_abbr", "game_status"]
        for col in str_cols:
            if col in games_df.columns:
                games_df[col] = games_df[col].astype("string")
    
    # Apply type coercion for player stats
    if not players_df.empty:
        players_df["game_date"] = pd.to_datetime(players_df["game_date"]).dt.date
        
        if "starter" in players_df.columns:
            players_df["starter"] = players_df["starter"].astype("boolean")
        
        int_cols = [
            "season", "team_id", "player_id", "pts", "fgm", "fga", 
            "fg3m", "fg3a", "ftm", "fta", "oreb", "dreb", "reb", 
            "ast", "stl", "blk", "tov", "pf", "plus_minus"
        ]
        for col in int_cols:
            if col in players_df.columns:
                players_df[col] = pd.to_numeric(players_df[col], errors="coerce").astype("Int64")
        
        float_cols = ["fg_pct", "fg3_pct", "ft_pct"]
        for col in float_cols:
            if col in players_df.columns:
                players_df[col] = pd.to_numeric(players_df[col], errors="coerce").astype("Float64")
        
        str_cols = ["game_id", "team_name", "team_abbr", "player_name", "minutes"]
        for col in str_cols:
            if col in players_df.columns:
                players_df[col] = players_df[col].astype("string")
    
    print(f"COMPLETED {date_str}: {len(games_df)} games, {len(players_df)} player stats")
    return games_df, players_df

# -----------------------------
# Main orchestration
# -----------------------------
def ingest_dates(date_list: List[str]) -> None:
    """Process multiple dates"""
    ensure_tables()
    
    all_games_frames = []
    all_box_frames = []
    
    total_games = 0
    total_players = 0
    
    for date_str in date_list:
        try:
            games_df, players_df = process_date(date_str)
            
            if not games_df.empty:
                all_games_frames.append(games_df)
                total_games += len(games_df)
            
            if not players_df.empty:
                all_box_frames.append(players_df)
                total_players += len(players_df)
            
            # Rate limiting between dates
            time.sleep(1.0)
            
        except Exception as e:
            print(f"ERROR processing date {date_str}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # Upload to BigQuery
    print(f"\n=== Uploading to BigQuery ===")
    
    if all_games_frames:
        final_games_df = pd.concat(all_games_frames, ignore_index=True)
        print(f"Uploading {len(final_games_df)} game rows")
        load_df(final_games_df, "games_daily")
    else:
        print("No games data to upload")
    
    if all_box_frames:
        final_players_df = pd.concat(all_box_frames, ignore_index=True)
        print(f"Uploading {len(final_players_df)} player box score rows")
        load_df(final_players_df, "player_boxscores")
    else:
        print("No player box score data to upload")
    
    print(f"\n=== SUMMARY ===")
    print(f"Dates processed: {len(date_list)}")
    print(f"Total games: {total_games}")
    print(f"Total player stats: {total_players}")

def date_range_to_list(start_date: datetime.date, end_date: datetime.date) -> List[str]:
    """Convert date range to list of YYYY-MM-DD strings"""
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current.strftime('%Y-%m-%d'))
        current += datetime.timedelta(days=1)
    return dates

def main() -> None:
    parser = argparse.ArgumentParser(description="NBA data ingestion using official NBA API")
    parser.add_argument("--mode", choices=["backfill", "daily"], required=True)
    parser.add_argument("--start", help="YYYY-MM-DD start date for backfill")
    parser.add_argument("--end", help="YYYY-MM-DD end date for backfill")
    args = parser.parse_args()

    print("=== NBA API Data Pipeline ===")

    if args.mode == "daily":
        yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
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
        
        dates = date_range_to_list(start_date, end_date)
        print(f"Running backfill for {len(dates)} dates: {args.start} to {args.end}")
        ingest_dates(dates)

    print("Pipeline completed successfully!")

if __name__ == "__main__":
    main()
