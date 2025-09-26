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


# -----------------------------
# Config via environment
# -----------------------------

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET = os.environ.get("BQ_DATASET", "nba_data")

SA_INFO = json.loads(os.environ["GCP_SA_KEY"])
CREDS = service_account.Credentials.from_service_account_info(SA_INFO)
BQ = bigquery.Client(project=PROJECT_ID, credentials=CREDS)

# Working endpoints
ESPN_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
ESPN_SUMMARY = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary"

# -----------------------------
# BigQuery schemas
# -----------------------------
GAMES_SCHEMA = [
    bigquery.SchemaField("game_id", "STRING"),
    bigquery.SchemaField("game_date", "DATE"),
    bigquery.SchemaField("season", "INT64"),
    bigquery.SchemaField("home_team_id", "INT64"),
    bigquery.SchemaField("home_team_abbr", "STRING"),
    bigquery.SchemaField("home_score", "INT64"),
    bigquery.SchemaField("away_team_id", "INT64"),
    bigquery.SchemaField("away_team_abbr", "STRING"),
    bigquery.SchemaField("away_score", "INT64"),
    bigquery.SchemaField("game_status", "STRING"),
]

BOX_SCHEMA = [
    bigquery.SchemaField("game_id", "STRING"),
    bigquery.SchemaField("game_date", "DATE"),
    bigquery.SchemaField("season", "INT64"),
    bigquery.SchemaField("team_id", "INT64"),
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
    except Exception:
        BQ.create_dataset(bigquery.Dataset(ds_id))

def ensure_tables() -> None:
    ensure_dataset()

    games_table_id = f"{PROJECT_ID}.{DATASET}.games_daily"
    try:
        BQ.get_table(games_table_id)
    except Exception:
        t = bigquery.Table(games_table_id, schema=GAMES_SCHEMA)
        BQ.create_table(t)

    box_table_id = f"{PROJECT_ID}.{DATASET}.player_boxscores"
    try:
        BQ.get_table(box_table_id)
    except Exception:
        t = bigquery.Table(box_table_id, schema=BOX_SCHEMA)
        BQ.create_table(t)

def load_df(df: pd.DataFrame, table: str) -> None:
    if df is None or df.empty:
        return
    table_id = f"{PROJECT_ID}.{DATASET}.{table}"
    schema = GAMES_SCHEMA if table == "games_daily" else BOX_SCHEMA

    if "game_date" in df.columns:
        df = df.dropna(subset=["game_date"])

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    )
    BQ.load_table_from_dataframe(df, table_id, job_config=job_config).result()

# -----------------------------
# Utilities
# -----------------------------
def safe_int(x: Any) -> Optional[int]:
    try:
        return int(x) if x is not None and x != "" and x != "N/A" else None
    except Exception:
        return None

def safe_float(x: Any) -> Optional[float]:
    try:
        return float(x) if x is not None and x != "" and x != "N/A" else None
    except Exception:
        return None

def http_get_json(url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 10) -> Dict[str, Any]:
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Request failed for {url}: {e}")
        return {}

# -----------------------------
# FIXED: Use boxscore.players array
# -----------------------------
def extract_players_from_espn_boxscore_players(espn_game_id: str, game_date: str, season: int) -> List[Dict[str, Any]]:
    """
    FIXED: Extract player stats from ESPN's boxscore.players array
    """
    try:
        print(f"\n--- Processing ESPN Summary for game {espn_game_id} ---")
        summary_data = http_get_json(ESPN_SUMMARY, params={"event": espn_game_id})
        
        if not summary_data:
            print(f"ERROR: No summary data for game {espn_game_id}")
            return []
        
        boxscore = summary_data.get("boxscore", {})
        print(f"Boxscore keys: {list(boxscore.keys())}")
        
        # NEW: Use the players array directly
        if "players" not in boxscore:
            print(f"ERROR: No 'players' key in boxscore for game {espn_game_id}")
            return []
        
        players_array = boxscore["players"]
        print(f"Found players array with {len(players_array)} groups")
        
        # Debug the players array structure
        for i, group in enumerate(players_array):
            print(f"  Players group {i}: keys = {list(group.keys())}")
            if "team" in group:
                team_info = group["team"]
                print(f"    Team: {team_info.get('abbreviation')} (ID: {team_info.get('id')})")
            if "athletes" in group:
                print(f"    Athletes: {len(group['athletes'])} players")
        
        player_stats = []
        
        # Process each team's players
        for team_group in players_array:
            team_info = team_group.get("team", {})
            team_id = safe_int(team_info.get("id"))
            team_abbr = team_info.get("abbreviation", "")
            
            athletes = team_group.get("athletes", [])
            print(f"Processing {len(athletes)} athletes for team {team_abbr}")
            
            for athlete_data in athletes:
                athlete = athlete_data.get("athlete", {})
                player_id = safe_int(athlete.get("id"))
                player_name = athlete.get("displayName", "").strip()
                
                if not player_id or not player_name:
                    print(f"    Skipping player with missing ID or name")
                    continue
                
                # Get player statistics
                stats = athlete_data.get("stats", [])
                starter = athlete_data.get("starter", False)
                
                print(f"    Player: {player_name}, Starter: {starter}, Stats: {len(stats)} items")
                print(f"      Stats array: {stats}")
                
                # Parse stats array - ESPN format
                parsed_stats = parse_espn_stats_array(stats)
                
                # Skip players with no minutes
                if not parsed_stats.get("minutes") or parsed_stats.get("minutes") == "0:00":
                    print(f"    Skipping {player_name} - no minutes played")
                    continue
                
                player_stat = {
                    'game_id': espn_game_id,
                    'game_date': game_date,
                    'season': season,
                    'team_id': team_id,
                    'team_abbr': team_abbr,
                    'player_id': player_id,
                    'player_name': player_name,
                    'starter': starter,
                    **parsed_stats
                }
                
                player_stats.append(player_stat)
                print(f"    ✓ Added {player_name}: {parsed_stats.get('pts', 0)} pts, {parsed_stats.get('minutes')} min")
        
        print(f"FINAL: Extracted {len(player_stats)} player stats from ESPN boxscore.players for game {espn_game_id}")
        return player_stats
        
    except Exception as e:
        print(f"ERROR extracting from ESPN boxscore.players for game {espn_game_id}: {e}")
        import traceback
        traceback.print_exc()
        return []

def parse_espn_stats_array(stats: List[str]) -> Dict[str, Any]:
    """Parse ESPN stats array"""
    parsed = {
        'minutes': None, 'pts': None, 'fgm': None, 'fga': None, 'fg_pct': None,
        'fg3m': None, 'fg3a': None, 'fg3_pct': None, 'ftm': None, 'fta': None, 'ft_pct': None,
        'oreb': None, 'dreb': None, 'reb': None, 'ast': None, 'stl': None, 'blk': None,
        'tov': None, 'pf': None, 'plus_minus': None
    }
    
    if not stats or len(stats) < 14:
        print(f"      Stats array too short or empty: {stats}")
        return parsed
    
    try:
        # ESPN stats order: MIN, FG, 3P, FT, OREB, DREB, REB, AST, STL, BLK, TO, PF, +/-, PTS
        parsed['minutes'] = stats[0] if stats[0] and stats[0] != "--" else None
        
        # Parse shooting stats
        fg_made, fg_attempted, fg_pct = parse_shooting_stat(stats[1])
        parsed['fgm'] = fg_made
        parsed['fga'] = fg_attempted
        parsed['fg_pct'] = fg_pct
        
        three_made, three_attempted, three_pct = parse_shooting_stat(stats[2])
        parsed['fg3m'] = three_made
        parsed['fg3a'] = three_attempted
        parsed['fg3_pct'] = three_pct
        
        ft_made, ft_attempted, ft_pct = parse_shooting_stat(stats[3])
        parsed['ftm'] = ft_made
        parsed['fta'] = ft_attempted
        parsed['ft_pct'] = ft_pct
        
        # Other stats
        parsed['oreb'] = safe_int(stats[4]) if len(stats) > 4 and stats[4] != "--" else None
        parsed['dreb'] = safe_int(stats[5]) if len(stats) > 5 and stats[5] != "--" else None
        parsed['reb'] = safe_int(stats[6]) if len(stats) > 6 and stats[6] != "--" else None
        parsed['ast'] = safe_int(stats[7]) if len(stats) > 7 and stats[7] != "--" else None
        parsed['stl'] = safe_int(stats[8]) if len(stats) > 8 and stats[8] != "--" else None
        parsed['blk'] = safe_int(stats[9]) if len(stats) > 9 and stats[9] != "--" else None
        parsed['tov'] = safe_int(stats[10]) if len(stats) > 10 and stats[10] != "--" else None
        parsed['pf'] = safe_int(stats[11]) if len(stats) > 11 and stats[11] != "--" else None
        parsed['plus_minus'] = safe_int(stats[12]) if len(stats) > 12 and stats[12] != "--" and stats[12] else None
        parsed['pts'] = safe_int(stats[13]) if len(stats) > 13 and stats[13] != "--" else None
        
    except Exception as e:
        print(f"      Error parsing stats array {stats}: {e}")
    
    return parsed

def parse_shooting_stat(stat_str: str) -> tuple:
    """Parse shooting stats like '5-10' into made, attempted, percentage"""
    if not stat_str or stat_str == '--' or stat_str == 'N/A':
        return None, None, None
    
    try:
        if '-' in stat_str:
            parts = stat_str.split('-')
            if len(parts) == 2:
                made = safe_int(parts[0])
                attempted = safe_int(parts[1])
                if made is not None and attempted is not None and attempted > 0:
                    percentage = round((made / attempted), 3)
                    return made, attempted, percentage
                else:
                    return made, attempted, None
    except:
        pass
    
    return None, None, None

# -----------------------------
# Main data processing
# -----------------------------
def get_games_for_date_fast(date_yyyymmdd: str) -> List[Dict[str, Any]]:
    """Get games using ESPN scoreboard"""
    try:
        data = http_get_json(ESPN_SCOREBOARD, params={"dates": date_yyyymmdd})
        games = []
        
        if 'events' in data:
            for event in data['events']:
                # Only process completed games
                status = event.get('status', {}).get('type', {}).get('name', '')
                if status not in ['STATUS_FINAL']:
                    continue
                
                competitors = event.get('competitions', [{}])[0].get('competitors', [])
                home_team = next((c for c in competitors if c.get('homeAway') == 'home'), {})
                away_team = next((c for c in competitors if c.get('homeAway') == 'away'), {})
                
                game = {
                    'game_id': event['id'],
                    'game_date': event['date'][:10],  # YYYY-MM-DD
                    'season': safe_int(event.get('season', {}).get('year', 2025)),
                    'home_team_id': safe_int(home_team.get('team', {}).get('id')),
                    'home_team_abbr': home_team.get('team', {}).get('abbreviation'),
                    'home_score': safe_int(home_team.get('score')),
                    'away_team_id': safe_int(away_team.get('team', {}).get('id')),
                    'away_team_abbr': away_team.get('team', {}).get('abbreviation'),
                    'away_score': safe_int(away_team.get('score')),
                    'game_status': status,
                }
                games.append(game)
        
        print(f"Found {len(games)} completed games for {date_yyyymmdd}")
        return games
        
    except Exception as e:
        print(f"Error getting games for {date_yyyymmdd}: {e}")
        return []

def process_game_with_players_array(game: Dict[str, Any]) -> tuple:
    """Process a single game using boxscore.players array"""
    game_id = game['game_id']
    
    # Extract player stats using the players array
    player_stats = extract_players_from_espn_boxscore_players(
        game_id, 
        game['game_date'], 
        game['season']
    )
    
    return game, player_stats

def process_date_fast(date_yyyymmdd: str) -> tuple:
    """Process a date using the new method"""
    print(f"\n=== Processing {date_yyyymmdd} ===")
    
    # Get games
    games = get_games_for_date_fast(date_yyyymmdd)
    
    if not games:
        print(f"No completed games found for {date_yyyymmdd}")
        return pd.DataFrame(), pd.DataFrame()
    
    print(f"Processing {len(games)} games using boxscore.players array...")
    
    all_games_data = []
    all_player_stats = []
    
    # Process games
    for game in games:
        try:
            game_data, player_stats = process_game_with_players_array(game)
            all_games_data.append(game_data)
            all_player_stats.extend(player_stats)
            
            time.sleep(0.3)  # Respectful delay
            
        except Exception as e:
            print(f"Error processing game {game['game_id']}: {e}")
            import traceback
            traceback.print_exc()
    
    # Create DataFrames
    games_df = pd.DataFrame(all_games_data) if all_games_data else pd.DataFrame()
    players_df = pd.DataFrame(all_player_stats) if all_player_stats else pd.DataFrame()
    
    # Apply type coercion
    games_df = coerce_games_dtypes(games_df)
    players_df = coerce_box_dtypes(players_df)
    
    print(f"COMPLETED {date_yyyymmdd}: {len(games_df)} games, {len(players_df)} player stats")
    return games_df, players_df

# -----------------------------
# Data type coercion
# -----------------------------
def coerce_games_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()

    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce").dt.date

    int_cols = ["season", "home_team_id", "home_score", "away_team_id", "away_score"]
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    str_cols = ["game_id", "home_team_abbr", "away_team_abbr", "game_status"]
    for c in str_cols:
        if c in df.columns and is_object_dtype(df[c]):
            df[c] = df[c].astype("string")

    return df

def coerce_box_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()

    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce").dt.date

    if "starter" in df.columns:
        df["starter"] = df["starter"].astype("boolean")

    int_cols = [
        "season", "team_id", "player_id", "pts", "fgm", "fga", "fg3m", "fg3a", 
        "ftm", "fta", "oreb", "dreb", "reb", "ast", "stl", "blk", "tov", "pf", "plus_minus"
    ]
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    float_cols = ["fg_pct", "fg3_pct", "ft_pct"]
    for c in float_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Float64")

    str_cols = ["game_id", "team_abbr", "player_name", "minutes"]
    for c in str_cols:
        if c in df.columns and is_object_dtype(df[c]):
            df[c] = df[c].astype("string")

    return df

# -----------------------------
# Main orchestration
# -----------------------------
def ingest_dates_fast(ymd_list: List[str]) -> None:
    """Process multiple dates efficiently"""
    ensure_tables()
    
    all_games_frames = []
    all_box_frames = []
    
    total_games = 0
    total_players = 0
    
    print(f"Processing {len(ymd_list)} dates using boxscore.players method...")
    
    for ymd in ymd_list:
        try:
            games_df, players_df = process_date_fast(ymd)
            
            if not games_df.empty:
                all_games_frames.append(games_df)
                total_games += len(games_df)
            
            if not players_df.empty:
                all_box_frames.append(players_df)
                total_players += len(players_df)
            
            time.sleep(0.5)
            
        except Exception as e:
            print(f"ERROR processing date {ymd}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # Upload to BigQuery
    print(f"\n=== Uploading to BigQuery ===")
    
    if all_games_frames:
        final_games_df = pd.concat(all_games_frames, ignore_index=True)
        print(f"Loading {len(final_games_df)} game rows")
        load_df(final_games_df, "games_daily")
    else:
        print("No games data to load")
    
    if all_box_frames:
        final_players_df = pd.concat(all_box_frames, ignore_index=True)
        print(f"Loading {len(final_players_df)} player box score rows")
        load_df(final_players_df, "player_boxscores")
    else:
        print("No player box score data to load")
    
    print(f"\n=== FINAL SUMMARY ===")
    print(f"Dates processed: {len(ymd_list)}")
    print(f"Total games: {total_games}")
    print(f"Total player stats: {total_players}")

def yyyymmdd_list(start: datetime.date, end: datetime.date) -> List[str]:
    out = []
    cur = start
    while cur <= end:
        out.append(cur.strftime("%Y%m%d"))
        cur += datetime.timedelta(days=1)
    return out

def main() -> None:
    parser = argparse.ArgumentParser(description="NBA data ingestion using ESPN boxscore.players array")
    parser.add_argument("--mode", choices=["backfill", "daily"], required=True)
    parser.add_argument("--start", help="YYYY-MM-DD inclusive start for backfill")
    parser.add_argument("--end", help="YYYY-MM-DD inclusive end for backfill")
    args = parser.parse_args()

    if args.mode == "daily":
        yday = datetime.date.today() - datetime.timedelta(days=1)
        ymd = yday.strftime("%Y%m%d")
        print(f"Running daily ingest for {ymd}")
        ingest_dates_fast([ymd])
        print(f"Daily ingest complete for {ymd}")
        return

    if args.mode == "backfill":
        if not args.start or not args.end:
            print("Error - backfill needs --start and --end like 2024-10-22 and 2024-10-28")
            sys.exit(1)
        try:
            s = datetime.date.fromisoformat(args.start)
            e = datetime.date.fromisoformat(args.end)
        except Exception:
            print("Error - invalid date format. Use YYYY-MM-DD")
            sys.exit(1)
        if e < s:
            print("Error - end date must be on or after start date")
            sys.exit(1)
        
        dates = yyyymmdd_list(s, e)
        print(f"Running backfill from {args.start} to {args.end} ({len(dates)} dates)")
        ingest_dates_fast(dates)
        print(f"Backfill complete for {args.start} to {args.end}")
        return

if __name__ == "__main__":
    main()
