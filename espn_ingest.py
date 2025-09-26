#!/usr/bin/env python3

import os
import sys
import json
import datetime
import argparse
from typing import List, Optional, Dict, Any

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# NBA API imports
from nba_api.stats.endpoints import scoreboardv2, boxscoretraditionalv2

# -----------------------------
# Config from environment
# -----------------------------
PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET = os.environ.get("BQ_DATASET", "nba_data")
SA_KEY = json.loads(os.environ["GCP_SA_KEY"])

# Authenticate to BigQuery
creds = service_account.Credentials.from_service_account_info(SA_KEY)
bq = bigquery.Client(project=PROJECT_ID, credentials=creds)

# Table names
GAMES_TABLE = "games_daily"
PLAYERS_TABLE = "player_boxscores"

# -----------------------------
# BigQuery setup
# -----------------------------
def ensure_dataset():
    ds_id = f"{PROJECT_ID}.{DATASET}"
    try:
        bq.get_dataset(ds_id)
        print(f"Dataset {ds_id} exists")
    except Exception:
        bq.create_dataset(ds_id)
        print(f"Created dataset {ds_id}")

def ensure_tables():
    ensure_dataset()
    
    # Games table
    games_table_id = f"{PROJECT_ID}.{DATASET}.{GAMES_TABLE}"
    try:
        bq.get_table(games_table_id)
        print("Games table exists")
    except Exception:
        games_schema = [
            bigquery.SchemaField("game_id", "STRING"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("season", "INTEGER"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("home_team_id", "INTEGER"),
            bigquery.SchemaField("home_team", "STRING"),
            bigquery.SchemaField("home_team_score", "INTEGER"),
            bigquery.SchemaField("visitor_team_id", "INTEGER"),
            bigquery.SchemaField("visitor_team", "STRING"),
            bigquery.SchemaField("visitor_team_score", "INTEGER"),
        ]
        table = bigquery.Table(games_table_id, schema=games_schema)
        table.time_partitioning = bigquery.TimePartitioning(field="date")
        bq.create_table(table)
        print("Created games table")
    
    # Player boxscores table
    players_table_id = f"{PROJECT_ID}.{DATASET}.{PLAYERS_TABLE}"
    try:
        bq.get_table(players_table_id)
        print("Player boxscores table exists")
    except Exception:
        players_schema = [
            bigquery.SchemaField("game_id", "STRING"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("season", "INTEGER"),
            bigquery.SchemaField("team_id", "INTEGER"),
            bigquery.SchemaField("team_name", "STRING"),
            bigquery.SchemaField("player_id", "INTEGER"),
            bigquery.SchemaField("player_name", "STRING"),
            bigquery.SchemaField("starter", "BOOL"),
            bigquery.SchemaField("minutes", "STRING"),
            bigquery.SchemaField("pts", "INTEGER"),
            bigquery.SchemaField("fgm", "INTEGER"),
            bigquery.SchemaField("fga", "INTEGER"),
            bigquery.SchemaField("fg_pct", "FLOAT"),
            bigquery.SchemaField("fg3m", "INTEGER"),
            bigquery.SchemaField("fg3a", "INTEGER"),
            bigquery.SchemaField("fg3_pct", "FLOAT"),
            bigquery.SchemaField("ftm", "INTEGER"),
            bigquery.SchemaField("fta", "INTEGER"),
            bigquery.SchemaField("ft_pct", "FLOAT"),
            bigquery.SchemaField("oreb", "INTEGER"),
            bigquery.SchemaField("dreb", "INTEGER"),
            bigquery.SchemaField("reb", "INTEGER"),
            bigquery.SchemaField("ast", "INTEGER"),
            bigquery.SchemaField("stl", "INTEGER"),
            bigquery.SchemaField("blk", "INTEGER"),
            bigquery.SchemaField("tov", "INTEGER"),
            bigquery.SchemaField("pf", "INTEGER"),
            bigquery.SchemaField("plus_minus", "INTEGER"),
        ]
        table = bigquery.Table(players_table_id, schema=players_schema)
        table.time_partitioning = bigquery.TimePartitioning(field="date")
        bq.create_table(table)
        print("Created player boxscores table")

def already_loaded(target_date: datetime.date, table_name: str) -> bool:
    """Check if data already exists for this date"""
    table = f"{PROJECT_ID}.{DATASET}.{table_name}"
    sql = f"""
    SELECT COUNT(*) cnt
    FROM `{table}`
    WHERE date = @d
    """
    job = bq.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("d", "DATE", target_date)]
    ))
    for row in job:
        return row["cnt"] > 0
    return False

def load_dataframe(df: pd.DataFrame, table_name: str):
    """Load DataFrame to BigQuery"""
    if df.empty:
        print(f"No data to load for {table_name}")
        return
    
    table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"
    job = bq.load_table_from_dataframe(df, table_id)
    job.result()
    print(f"Loaded {len(df)} rows to {table_name}")

# -----------------------------
# NBA API functions
# -----------------------------
def safe_int(x):
    """Safely convert to int"""
    try:
        return int(x) if x is not None and str(x).strip() != '' else None
    except:
        return None

def safe_float(x):
    """Safely convert to float"""
    try:
        return float(x) if x is not None and str(x).strip() != '' else None
    except:
        return None

def get_games_for_date(date_str: str) -> List[Dict[str, Any]]:
    """
    Get games for date using NBA API
    date_str: YYYY-MM-DD format
    """
    try:
        # Convert to MM/DD/YYYY for NBA API
        date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d')
        nba_date = date_obj.strftime('%m/%d/%Y')
        
        print(f"Getting games for {date_str} (NBA format: {nba_date})")
        
        # Get scoreboard data
        scoreboard_data = scoreboardv2.ScoreboardV2(game_date=nba_date)
        games_df = scoreboard_data.get_data_frames()[0]  # GameHeader
        
        games = []
        for _, row in games_df.iterrows():
            # Only process completed games
            status = str(row.get('GAME_STATUS_TEXT', ''))
            if 'Final' not in status:
                continue
                
            game = {
                'game_id': str(row['GAME_ID']),
                'date': date_str,
                'season': safe_int(row.get('SEASON')),
                'status': status,
                'home_team_id': safe_int(row.get('HOME_TEAM_ID')),
                'home_team': str(row.get('HOME_TEAM_ABBREVIATION', '')),
                'home_team_score': safe_int(row.get('PTS_HOME')),
                'visitor_team_id': safe_int(row.get('VISITOR_TEAM_ID')),
                'visitor_team': str(row.get('VISITOR_TEAM_ABBREVIATION', '')),
                'visitor_team_score': safe_int(row.get('PTS_AWAY')),
            }
            games.append(game)
        
        print(f"Found {len(games)} completed games")
        return games
        
    except Exception as e:
        print(f"Error getting games: {e}")
        return []

def get_player_stats_for_game(game_id: str, date_str: str, season: int) -> List[Dict[str, Any]]:
    """Get player stats for a game"""
    try:
        print(f"Getting player stats for game {game_id}")
        
        # Get boxscore data
        box_data = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
        players_df = box_data.get_data_frames()[0]  # PlayerStats
        
        player_stats = []
        for _, row in players_df.iterrows():
            # Skip players who didn't play
            minutes = str(row.get('MIN', ''))
            if not minutes or minutes == '0:00':
                continue
            
            stat = {
                'game_id': game_id,
                'date': date_str,
                'season': season,
                'team_id': safe_int(row.get('TEAM_ID')),
                'team_name': str(row.get('TEAM_ABBREVIATION', '')),
                'player_id': safe_int(row.get('PLAYER_ID')),
                'player_name': str(row.get('PLAYER_NAME', '')),
                'starter': str(row.get('START_POSITION', '')) != '',
                'minutes': minutes,
                'pts': safe_int(row.get('PTS')),
                'fgm': safe_int(row.get('FGM')),
                'fga': safe_int(row.get('FGA')),
                'fg_pct': safe_float(row.get('FG_PCT')),
                'fg3m': safe_int(row.get('FG3M')),
                'fg3a': safe_int(row.get('FG3A')),
                'fg3_pct': safe_float(row.get('FG3_PCT')),
                'ftm': safe_int(row.get('FTM')),
                'fta': safe_int(row.get('FTA')),
                'ft_pct': safe_float(row.get('FT_PCT')),
                'oreb': safe_int(row.get('OREB')),
                'dreb': safe_int(row.get('DREB')),
                'reb': safe_int(row.get('REB')),
                'ast': safe_int(row.get('AST')),
                'stl': safe_int(row.get('STL')),
                'blk': safe_int(row.get('BLK')),
                'tov': safe_int(row.get('TO')),
                'pf': safe_int(row.get('PF')),
                'plus_minus': safe_int(row.get('PLUS_MINUS')),
            }
            player_stats.append(stat)
        
        print(f"Found {len(player_stats)} player stats")
        return player_stats
        
    except Exception as e:
        print(f"Error getting player stats for {game_id}: {e}")
        return []

# -----------------------------
# Main processing
# -----------------------------
def process_date(date_str: str):
    """Process games and player stats for a date"""
    print(f"\n=== Processing {date_str} ===")
    
    date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
    
    # Check if already loaded
    if already_loaded(date_obj, GAMES_TABLE):
        print(f"Games already loaded for {date_str}")
        return
    
    # Get games
    games = get_games_for_date(date_str)
    if not games:
        print(f"No games found for {date_str}")
        return
    
    # Create games DataFrame
    games_df = pd.DataFrame(games)
    games_df['date'] = pd.to_datetime(games_df['date']).dt.date
    
    # Get all player stats
    all_player_stats = []
    for game in games:
        player_stats = get_player_stats_for_game(
            game['game_id'], 
            date_str, 
            game['season']
        )
        all_player_stats.extend(player_stats)
        
        # Rate limiting
        import time
        time.sleep(0.6)
    
    # Create player stats DataFrame
    if all_player_stats:
        players_df = pd.DataFrame(all_player_stats)
        players_df['date'] = pd.to_datetime(players_df['date']).dt.date
    else:
        players_df = pd.DataFrame()
    
    # Load to BigQuery
    load_dataframe(games_df, GAMES_TABLE)
    if not players_df.empty:
        load_dataframe(players_df, PLAYERS_TABLE)
    
    print(f"Completed {date_str}: {len(games_df)} games, {len(players_df)} player stats")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["daily", "backfill"], required=True)
    parser.add_argument("--start", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", help="End date YYYY-MM-DD")
    args = parser.parse_args()
    
    ensure_tables()
    
    if args.mode == "daily":
        # Process yesterday
        yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        print(f"Processing yesterday: {yesterday}")
        process_date(yesterday)
        
    elif args.mode == "backfill":
        if not args.start or not args.end:
            print("Backfill requires --start and --end dates")
            sys.exit(1)
        
        start_date = datetime.datetime.strptime(args.start, '%Y-%m-%d').date()
        end_date = datetime.datetime.strptime(args.end, '%Y-%m-%d').date()
        
        current = start_date
        while current <= end_date:
            process_date(current.strftime('%Y-%m-%d'))
            current += datetime.timedelta(days=1)
            
            # Rate limiting between dates
            import time
            time.sleep(1.0)
    
    print("Processing completed!")

if __name__ == "__main__":
    main()
