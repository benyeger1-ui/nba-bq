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
from urllib.parse import urlencode


# -----------------------------
# Config via environment
# -----------------------------

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

PROJECT_ID = os.environ["GCP_PROJECT_ID"]  # set in GitHub Actions secrets
DATASET = os.environ.get("BQ_DATASET", "nba_data")

# Service account JSON is stored as a single secret string
SA_INFO = json.loads(os.environ["GCP_SA_KEY"])
CREDS = service_account.Credentials.from_service_account_info(SA_INFO)
BQ = bigquery.Client(project=PROJECT_ID, credentials=CREDS)

# ESPN free site APIs
ESPN_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
ESPN_SUMMARY = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary"
ESPN_CORE_BOXSCORE = (
    "https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba"
    "/events/{event}/competitions/{event}/boxscore"
)
ESPN_SITE_BOXSCORE = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/boxscore"


# -----------------------------
# BigQuery schemas
# -----------------------------
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

    # drop rows without a date just in case
    if "date" in df.columns:
        df = df.dropna(subset=["date"])

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    )
    BQ.load_table_from_dataframe(df, table_id, job_config=job_config).result()

# -----------------------------
# Type coercion to keep Arrow happy
# -----------------------------
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

# -----------------------------
# ESPN fetchers
# -----------------------------
def http_get_json(url: str, params: Optional[Dict[str, Any]] = None, max_retries: int = 3) -> Dict[str, Any]:
    last_err = None
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            print(f"Attempt {attempt + 1} failed for URL {url}: {e}")
            if attempt < max_retries - 1:
                time.sleep(0.6 * (attempt + 1))  # Exponential backoff
    raise last_err if last_err else RuntimeError("request failed")

def get_scoreboard_for_date_yyyymmdd(yyyymmdd: str) -> Dict[str, Any]:
    return http_get_json(ESPN_SCOREBOARD, params={"dates": yyyymmdd})

def list_event_ids(scoreboard_json: Dict[str, Any]) -> List[str]:
    events = scoreboard_json.get("events", []) or []
    return [e.get("id") for e in events if e.get("id")]

def get_summary(event_id: str) -> Dict[str, Any]:
    return http_get_json(ESPN_SUMMARY, params={"event": event_id})

# -----------------------------
# Normalizers
# -----------------------------
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

def parse_shooting_stat(stat_str: str) -> tuple:
    """Parse shooting stats like '5-10' into made, attempted"""
    if not stat_str or stat_str == '--' or stat_str == 'N/A':
        return None, None
    
    try:
        if '-' in stat_str:
            parts = stat_str.split('-')
            if len(parts) == 2:
                made = safe_int(parts[0])
                attempted = safe_int(parts[1])
                return made, attempted
    except:
        pass
    
    return None, None

def normalize_game_row(event_id: str, summary: Dict[str, Any], fallback_date_iso: str) -> pd.DataFrame:
    header = summary.get("header", {}) or {}
    competitions = header.get("competitions", []) or []
    comp = competitions[0] if competitions else {}
    season = ((header.get("season") or {}).get("year"))
    status_type = (((comp.get("status") or {}).get("type") or {}).get("name"))

    competitors = comp.get("competitors", []) or []
    home = next((c for c in competitors if c.get("homeAway") == "home"), {})
    away = next((c for c in competitors if c.get("homeAway") == "away"), {})

    def team_fields(c: Dict[str, Any]):
        team = c.get("team") or {}
        abbr = team.get("abbreviation") or team.get("shortDisplayName")
        tid = safe_int(team.get("id"))
        score = safe_int(c.get("score"))
        return tid, abbr, score

    hid, habbr, hscore = team_fields(home)
    aid, aabbr, ascore = team_fields(away)

    iso_date = (comp.get("date", "") or "")[:10] or fallback_date_iso
    uid = header.get("uid")

    df = pd.DataFrame([{
        "event_id": event_id,
        "game_uid": uid,
        "date": iso_date,
        "season": season,
        "status_type": status_type,
        "home_id": hid,
        "home_abbr": habbr,
        "home_score": hscore,
        "away_id": aid,
        "away_abbr": aabbr,
        "away_score": ascore,
    }])
    return coerce_games_dtypes(df)

def fetch_summary_boxscore_players(event_id: str, summary: Dict[str, Any], fallback_date_iso: str, season: int) -> List[Dict[str, Any]]:
    """
    FIXED: Enhanced function to extract player data from ESPN summary endpoint
    with better data parsing and multiple extraction attempts
    """
    rows: List[Dict[str, Any]] = []
    
    try:
        # Check multiple possible locations for boxscore data
        boxscore_data = None
        
        # Location 1: summary.boxscore
        if "boxscore" in summary:
            boxscore_data = summary["boxscore"]
            print(f"Found boxscore in summary for event {event_id}")
        
        # Location 2: summary.content.boxscore (sometimes nested deeper)
        elif "content" in summary and "boxscore" in summary["content"]:
            boxscore_data = summary["content"]["boxscore"]
            print(f"Found boxscore in summary.content for event {event_id}")
        
        # Location 3: Check for teams directly in summary
        elif "teams" in summary:
            boxscore_data = {"teams": summary["teams"]}
            print(f"Found teams directly in summary for event {event_id}")
        
        if not boxscore_data or "teams" not in boxscore_data:
            print(f"No boxscore data found in summary for event {event_id}")
            return []
        
        teams = boxscore_data.get("teams", [])
        if not teams:
            print(f"No teams found in boxscore for event {event_id}")
            return []
        
        print(f"Processing {len(teams)} teams for event {event_id}")
        
        for team_idx, team_data in enumerate(teams):
            team_info = team_data.get("team", {})
            team_id = safe_int(team_info.get("id"))
            team_abbr = team_info.get("abbreviation") or team_info.get("shortDisplayName")
            
            print(f"Processing team {team_abbr} (ID: {team_id}) for event {event_id}")
            
            # Look for player data in multiple locations
            players_data = []
            
            # Location 1: statistics array with athletes
            if "statistics" in team_data:
                for stat_group in team_data["statistics"]:
                    if "athletes" in stat_group:
                        is_starter = stat_group.get("name", "").lower() == "starters"
                        for athlete_data in stat_group["athletes"]:
                            athlete_info = athlete_data.get("athlete", {})
                            stats_array = athlete_data.get("stats", [])
                            
                            player_data = {
                                "athlete": athlete_info,
                                "stats": stats_array,
                                "starter": is_starter
                            }
                            players_data.append(player_data)
            
            # Location 2: players array directly
            elif "players" in team_data:
                for player in team_data["players"]:
                    athlete_info = player.get("athlete", {})
                    stats_array = player.get("stats", [])
                    is_starter = player.get("starter", False)
                    
                    player_data = {
                        "athlete": athlete_info,
                        "stats": stats_array,
                        "starter": is_starter
                    }
                    players_data.append(player_data)
            
            print(f"Found {len(players_data)} players for team {team_abbr}")
            
            # Process each player
            for player_data in players_data:
                athlete = player_data["athlete"]
                stats = player_data["stats"]
                is_starter = player_data["starter"]
                
                player_id = safe_int(athlete.get("id"))
                player_name = athlete.get("displayName") or athlete.get("name", "")
                
                if not player_id or not player_name:
                    print(f"Skipping player with missing ID or name: {athlete}")
                    continue
                
                # Parse stats array - ESPN typically returns stats in a specific order
                # Common order: MIN, FG, 3P, FT, OREB, DREB, REB, AST, STL, BLK, TO, PF, +/-, PTS
                player_stats = {
                    "minutes": None, "pts": None, "reb": None, "ast": None,
                    "stl": None, "blk": None, "tov": None, "fgm": None, "fga": None,
                    "fg3m": None, "fg3a": None, "ftm": None, "fta": None,
                    "oreb": None, "dreb": None, "pf": None
                }
                
                if len(stats) >= 14:  # Standard ESPN stats array length
                    # Parse shooting stats
                    fg_made, fg_attempted = parse_shooting_stat(stats[1] if len(stats) > 1 else "0-0")
                    three_made, three_attempted = parse_shooting_stat(stats[2] if len(stats) > 2 else "0-0")
                    ft_made, ft_attempted = parse_shooting_stat(stats[3] if len(stats) > 3 else "0-0")
                    
                    player_stats.update({
                        "minutes": stats[0] if len(stats) > 0 and stats[0] != "--" else None,
                        "fgm": fg_made,
                        "fga": fg_attempted,
                        "fg3m": three_made,
                        "fg3a": three_attempted,
                        "ftm": ft_made,
                        "fta": ft_attempted,
                        "oreb": safe_int(stats[4]) if len(stats) > 4 and stats[4] != "--" else None,
                        "dreb": safe_int(stats[5]) if len(stats) > 5 and stats[5] != "--" else None,
                        "reb": safe_int(stats[6]) if len(stats) > 6 and stats[6] != "--" else None,
                        "ast": safe_int(stats[7]) if len(stats) > 7 and stats[7] != "--" else None,
                        "stl": safe_int(stats[8]) if len(stats) > 8 and stats[8] != "--" else None,
                        "blk": safe_int(stats[9]) if len(stats) > 9 and stats[9] != "--" else None,
                        "tov": safe_int(stats[10]) if len(stats) > 10 and stats[10] != "--" else None,
                        "pf": safe_int(stats[11]) if len(stats) > 11 and stats[11] != "--" else None,
                        "pts": safe_int(stats[13]) if len(stats) > 13 and stats[13] != "--" else None,
                    })
                
                row = {
                    "event_id": event_id,
                    "date": fallback_date_iso,
                    "season": season,
                    "team_id": team_id,
                    "team_abbr": team_abbr,
                    "player_id": player_id,
                    "player": player_name,
                    "starter": is_starter,
                    **player_stats
                }
                
                rows.append(row)
        
        print(f"Successfully extracted {len(rows)} player records from summary for event {event_id}")
        return rows
    
    except Exception as e:
        print(f"Error extracting players from summary for event {event_id}: {e}")
        return []

def fetch_core_boxscore_players(event_id: str) -> List[Dict[str, Any]]:
    """
    ENHANCED: Core fallback by following $ref with better error handling
    """
    try:
        print(f"Attempting core boxscore for event {event_id}")
        ev = http_get_json(f"https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}")
        comps = ev.get("competitions") or []
        
        if not comps or not isinstance(comps[0], dict) or not comps[0].get("$ref"):
            print(f"No competition reference found for event {event_id}")
            return []
        
        comp = http_get_json(comps[0]["$ref"])
        box_ref = (comp.get("boxscore") or {}).get("$ref")
        
        if not box_ref:
            print(f"No boxscore reference found for event {event_id}")
            return []

        j = http_get_json(box_ref)
        rows: List[Dict[str, Any]] = []
        teams = j.get("teams") or []
        
        print(f"Core API found {len(teams)} teams for event {event_id}")
        
        for t in teams:
            team_ref = t.get("team")
            team_obj = {}
            if isinstance(team_ref, dict) and team_ref.get("$ref"):
                team_obj = http_get_json(team_ref["$ref"])
            team_id = safe_int(team_obj.get("id"))
            team_abbr = team_obj.get("abbreviation") or team_obj.get("shortDisplayName")

            players = t.get("players") or []
            print(f"Core API found {len(players)} players for team {team_abbr}")

            for p in players:
                ath_ref = p.get("athlete")
                ath = {}
                if isinstance(ath_ref, dict) and ath_ref.get("$ref"):
                    ath = http_get_json(ath_ref["$ref"])
                player_id = safe_int(ath.get("id"))
                player_name = ath.get("displayName")

                if not player_id or not player_name:
                    continue

                stat_map: Dict[str, Any] = {}
                for st in (p.get("statistics") or []):
                    nm, val = st.get("name"), st.get("value")
                    if nm:
                        stat_map[nm] = val

                def gi(*keys):
                    for k in keys:
                        if k in stat_map and stat_map[k] is not None:
                            return safe_int(stat_map[k])
                    return None

                rows.append({
                    "team_id": team_id,
                    "team_abbr": team_abbr,
                    "player_id": player_id,
                    "player": player_name,
                    "starter": bool(p.get("starter")) if p.get("starter") is not None else None,
                    "minutes": stat_map.get("minutes"),
                    "pts": gi("points","pts"),
                    "reb": gi("rebounds","reb"),
                    "ast": gi("assists","ast"),
                    "stl": gi("steals","stl"),
                    "blk": gi("blocks","blk"),
                    "tov": gi("turnovers","to","tov"),
                    "fgm": gi("fieldGoalsMade","fgm"),
                    "fga": gi("fieldGoalsAttempted","fga"),
                    "fg3m": gi("threePointFieldGoalsMade","fg3m"),
                    "fg3a": gi("threePointFieldGoalsAttempted","fg3a"),
                    "ftm": gi("freeThrowsMade","ftm"),
                    "fta": gi("freeThrowsAttempted","fta"),
                    "oreb": gi("offensiveRebounds","oreb"),
                    "dreb": gi("defensiveRebounds","dreb"),
                    "pf": gi("fouls","pf"),
                })
        
        print(f"Core API extracted {len(rows)} player records for event {event_id}")
        return rows
        
    except requests.HTTPError as e:
        print(f"Core boxscore HTTP error for event {event_id}: {e}")
        return []
    except Exception as e:
        print(f"Core boxscore unexpected error for event {event_id}: {e}")
        return []

def fetch_site_boxscore_players(event_id: str) -> List[Dict[str, Any]]:
    """
    ENHANCED: Site boxscore endpoint fallback with better parsing
    """
    try:
        print(f"Attempting site boxscore for event {event_id}")
        j = http_get_json(ESPN_SITE_BOXSCORE, params={"event": event_id})
        rows: List[Dict[str, Any]] = []
        
        # Handle different response structures
        box = j.get("boxscore") or j
        teams = box.get("teams") or []
        
        print(f"Site boxscore found {len(teams)} teams for event {event_id}")
        
        for t in teams:
            team = t.get("team") or {}
            tid = safe_int(team.get("id"))
            tabbr = team.get("abbreviation") or team.get("shortDisplayName")
            players = t.get("players") or []
            
            print(f"Site boxscore found {len(players)} players for team {tabbr}")
            
            for p in players:
                ath = p.get("athlete") or {}
                pid = safe_int(ath.get("id"))
                name = ath.get("displayName")
                
                if not pid or not name:
                    continue
                
                starter = bool(p.get("starter")) if p.get("starter") is not None else None

                # stats might be under "stats" or "statistics"
                stat_map: Dict[str, Any] = {}
                if isinstance(p.get("stats"), dict):
                    stat_map.update(p["stats"])
                for st in (p.get("statistics") or []):
                    nm, val = st.get("name"), st.get("value")
                    if nm:
                        stat_map[nm] = val

                def gi(*keys):
                    for k in keys:
                        if k in stat_map and stat_map[k] is not None:
                            return safe_int(stat_map[k])
                    return None

                rows.append({
                    "team_id": tid,
                    "team_abbr": tabbr,
                    "player_id": pid,
                    "player": name,
                    "starter": starter,
                    "minutes": stat_map.get("minutes"),
                    "pts": gi("points","pts"),
                    "reb": gi("rebounds","reb"),
                    "ast": gi("assists","ast"),
                    "stl": gi("steals","stl"),
                    "blk": gi("blocks","blk"),
                    "tov": gi("turnovers","to","tov"),
                    "fgm": gi("fieldGoalsMade","fgm"),
                    "fga": gi("fieldGoalsAttempted","fga"),
                    "fg3m": gi("threePointFieldGoalsMade","fg3m"),
                    "fg3a": gi("threePointFieldGoalsAttempted","fg3a"),
                    "ftm": gi("freeThrowsMade","ftm"),
                    "fta": gi("freeThrowsAttempted","fta"),
                    "oreb": gi("offensiveRebounds","oreb"),
                    "dreb": gi("defensiveRebounds","dreb"),
                    "pf": gi("fouls","pf"),
                })
        
        print(f"Site boxscore extracted {len(rows)} player records for event {event_id}")
        return rows
        
    except Exception as e:
        print(f"Site boxscore fallback failed for event {event_id}: {e}")
        return []

def normalize_player_box(event_id: str, summary: Dict[str, Any], fallback_date_iso: str) -> pd.DataFrame:
    """
    COMPLETELY REWRITTEN: Multi-source player data extraction with comprehensive fallback logic
    """
    header = summary.get("header", {}) or {}
    competitions = header.get("competitions", []) or []
    comp = competitions[0] if competitions else {}
    iso_date = (comp.get("date", "") or "")[:10] or fallback_date_iso
    season = ((header.get("season") or {}).get("year"))

    rows: List[Dict[str, Any]] = []

    print(f"Starting player data extraction for event {event_id}")

    # STRATEGY 1: Enhanced summary parsing
    print(f"Attempting enhanced summary parsing for event {event_id}")
    rows = fetch_summary_boxscore_players(event_id, summary, iso_date, season)
    
    if rows:
        print(f"SUCCESS: Enhanced summary parsing found {len(rows)} players for event {event_id}")
    else:
        # STRATEGY 2: Core API traversal
        print(f"Summary failed, trying core API for event {event_id}")
        rows = fetch_core_boxscore_players(event_id)
        if rows:
            # Add missing fields
            for r in rows:
                r.update({"event_id": event_id, "date": iso_date, "season": season})
            print(f"SUCCESS: Core API found {len(rows)} players for event {event_id}")
        else:
            # STRATEGY 3: Site boxscore endpoint
            print(f"Core API failed, trying site boxscore for event {event_id}")
            rows = fetch_site_boxscore_players(event_id)
            if rows:
                # Add missing fields
                for r in rows:
                    r.update({"event_id": event_id, "date": iso_date, "season": season})
                print(f"SUCCESS: Site boxscore found {len(rows)} players for event {event_id}")
            else:
                print(f"FAILED: All methods failed for event {event_id}")

    # Create DataFrame
    if rows:
        # Ensure all required columns exist
        for row in rows:
            for field in BOX_SCHEMA:
                if field.name not in row:
                    if field.field_type == "BOOL":
                        row[field.name] = False
                    elif field.field_type == "INT64":
                        row[field.name] = None
                    else:
                        row[field.name] = None
        
        df = pd.DataFrame(rows)
        # Reorder columns to match schema
        schema_cols = [f.name for f in BOX_SCHEMA]
        df = df.reindex(columns=schema_cols, fill_value=None)
        return coerce_box_dtypes(df)
    else:
        # Return empty DataFrame with correct schema
        df = pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])
        return coerce_box_dtypes(df)

# -----------------------------
# Orchestration
# -----------------------------
def yyyymmdd_list(start: datetime.date, end: datetime.date) -> List[str]:
    out = []
    cur = start
    while cur <= end:
        out.append(cur.strftime("%Y%m%d"))
        cur += datetime.timedelta(days=1)
    return out

def ingest_dates(ymd_list: List[str]) -> None:
    ensure_tables()

    games_frames: List[pd.DataFrame] = []
    box_frames: List[pd.DataFrame] = []

    total_events = 0
    total_games_rows = 0
    total_box_rows = 0

    for ymd in ymd_list:
        fallback_iso = f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}"
        print(f"\n=== Processing date {fallback_iso} ===")
        
        try:
            sb = get_scoreboard_for_date_yyyymmdd(ymd)
            event_ids = list_event_ids(sb)
            print(f"[{fallback_iso}] Found {len(event_ids)} events")
            total_events += len(event_ids)

            for eid in event_ids:
                print(f"\n--- Processing event {eid} ---")
                try:
                    summary = get_summary(eid)
                    
                    # Process game data
                    gdf = normalize_game_row(eid, summary, fallback_iso)
                    if not gdf.empty:
                        games_frames.append(gdf)
                        total_games_rows += len(gdf)
                        print(f"Game data extracted: {len(gdf)} rows")
                    
                    # Process player data with enhanced extraction
                    pdf = normalize_player_box(eid, summary, fallback_iso)
                    if not pdf.empty:
                        box_frames.append(pdf)
                        total_box_rows += len(pdf)
                        print(f"Player data extracted: {len(pdf)} rows")
                    else:
                        print(f"WARNING: No player data found for event {eid}")
                    
                    # Rate limiting
                    time.sleep(0.2)
                    
                except Exception as e:
                    print(f"ERROR processing event {eid}: {e}")
                    continue

            # Daily rate limiting
            time.sleep(0.5)
            
        except Exception as e:
            print(f"ERROR processing date {fallback_iso}: {e}")
            continue

    # Upload data to BigQuery
    print(f"\n=== Uploading data to BigQuery ===")
    
    if games_frames:
        games_df = pd.concat(games_frames, ignore_index=True)
        games_df = coerce_games_dtypes(games_df)
        print(f"Loading {len(games_df)} game rows to BigQuery")
        load_df(games_df, "games_daily")
    else:
        print("No games data to load")

    if box_frames:
        box_df = pd.concat(box_frames, ignore_index=True)
        box_df = coerce_box_dtypes(box_df)
        print(f"Loading {len(box_df)} player box score rows to BigQuery")
        load_df(box_df, "player_boxscores")
    else:
        print("No player box score data to load")

    print(f"\n=== SUMMARY ===")
    print(f"Dates processed: {len(ymd_list)}")
    print(f"Events found: {total_events}")
    print(f"Game rows: {total_games_rows}")
    print(f"Player box score rows: {total_box_rows}")

def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest NBA games and player box scores from ESPN free site APIs into BigQuery")
    parser.add_argument("--mode", choices=["backfill", "daily"], required=True, help="backfill for a date range, daily for yesterday")
    parser.add_argument("--start", help="YYYY-MM-DD inclusive start for backfill")
    parser.add_argument("--end", help="YYYY-MM-DD inclusive end for backfill")
    args = parser.parse_args()

    if args.mode == "daily":
        yday = datetime.date.today() - datetime.timedelta(days=1)
        ymd = yday.strftime("%Y%m%d")
        print(f"Running daily ingest for {ymd}")
        ingest_dates([ymd])
        print(f"Daily ingest complete for {ymd}")
        return

    if args.mode == "backfill":
        if not args.start or not args.end:
            print("Error - backfill needs --start and --end like 2024-10-01 and 2025-06-30")
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
        ingest_dates(dates)
        print(f"Backfill complete for {args.start} to {args.end}")
        return

if __name__ == "__main__":
    main()
