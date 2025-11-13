#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import argparse
import datetime
from typing import List, Optional, Dict, Any
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
ET_TZ = pytz.timezone("US/Eastern")
UTC_TZ = pytz.timezone("UTC")

# -----------------------------------
# Error tracking
# -----------------------------------
class IngestionErrorTracker:
    def __init__(self):
        self.errors = []
        self.warnings = []
        self.stats = {}
    
    def add_error(self, error_type: str, context: str, exception: str):
        self.errors.append({
            "type": error_type,
            "context": context,
            "exception": str(exception)[:200],
            "timestamp": datetime.datetime.utcnow().isoformat()
        })
        print(f"❌ ERROR [{error_type}]: {context} - {exception}")
    
    def add_warning(self, warning_type: str, context: str):
        self.warnings.append({
            "type": warning_type,
            "context": context,
            "timestamp": datetime.datetime.utcnow().isoformat()
        })
        print(f"⚠️  WARNING [{warning_type}]: {context}")
    
    def set_stat(self, key: str, value: Any):
        self.stats[key] = value
        print(f"📊 {key}: {value}")
    
    def has_critical_errors(self) -> bool:
        critical_types = {"bigquery_load_failure", "data_integrity"}
        return any(e["type"] in critical_types for e in self.errors)
    
    def get_summary(self) -> str:
        summary = f"\n{'='*70}\n🏀 NBA INGESTION SUMMARY\n{'='*70}\n"
        summary += f"STATS:\n{json.dumps(self.stats, indent=2)}\n"
        if self.warnings:
            summary += f"\n⚠️  WARNINGS ({len(self.warnings)}):\n"
            for w in self.warnings:
                summary += f"  - {w['type']}: {w['context']}\n"
        if self.errors:
            summary += f"\n❌ ERRORS ({len(self.errors)}):\n"
            for e in self.errors:
                summary += f"  - {e['type']}: {e['context']}\n    {e['exception']}\n"
        else:
            summary += f"\n✅ No critical errors\n"
        summary += f"{'='*70}\n"
        return summary
    
    def should_exit_with_error(self) -> bool:
        return self.has_critical_errors()

error_tracker = IngestionErrorTracker()

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

# -----------------------------
# Helpers - parsing and safety
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

def safe_str(x: Any) -> Optional[str]:
    try:
        return str(x) if x is not None and x != "" else None
    except Exception:
        return None

def parse_minutes(minutes_str: str) -> str:
    """Convert NBA API time format PT32M33.00S to M:SS"""
    try:
        if not minutes_str or minutes_str == "PT00M00.00S":
            return "0:00"
        clean = minutes_str.replace("PT", "").replace("S", "")
        if "M" in clean:
            m, s = clean.split("M")
            minutes = int(m or 0)
            seconds = int(float(s)) if s else 0
            return f"{minutes}:{seconds:02d}"
        return "0:00"
    except Exception:
        return "0:00"

def normalize_game_date(game_date_str: str, fallback_date: str) -> str:
    """
    Normalize the date to ET calendar date. Input may be ISO UTC like 2024-10-28T23:30:00Z.
    """
    try:
        if not game_date_str:
            return fallback_date
        if "T" in game_date_str:
            s = game_date_str.replace("Z", "+00:00")
            # guard weird microseconds length
            if "." in s:
                left, right = s.split(".", 1)
                if "+" in right:
                    us, tz = right.split("+", 1)
                    us = us[:6]
                    s = f"{left}.{us}+{tz}"
            dt = datetime.datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = UTC_TZ.localize(dt)
        else:
            return game_date_str[:10]
        et_dt = dt.astimezone(ET_TZ)
        return et_dt.date().isoformat()
    except Exception:
        try:
            return game_date_str[:10]
        except Exception:
            return fallback_date

# -----------------------------
# Scoreboard - schedule support
# -----------------------------
def fetch_scoreboard_games_for_date(date_str: str) -> List[Dict[str, Any]]:
    """
    Fetch ScoreBoard for a specific date. Returns a list of game dicts.
    """
    try:
        try:
            sb = scoreboard.ScoreBoard(game_date=date_str)
        except TypeError:
            sb = scoreboard.ScoreBoard()
        data = sb.get_dict()
        games = data.get("scoreboard", {}).get("games", [])
        return games or []
    except Exception as e:
        error_tracker.add_warning("scoreboard_fetch_failed", f"Date: {date_str}, Error: {str(e)}")
        return []

def score_game_to_row(g: Dict[str, Any], target_date: str) -> Dict[str, Any]:
    """Map a ScoreBoard game object to a row matching GAMES_SCHEMA."""
    game_time_utc = g.get("gameTimeUTC") or ""
    norm_date = normalize_game_date(game_time_utc, target_date)
    year = int(norm_date[:4])
    month = int(norm_date[5:7])
    season = year if month >= 10 else year - 1

    home = g.get("homeTeam", {}) or {}
    away = g.get("awayTeam", {}) or {}
    arena = g.get("arena", {}) or {}

    def zero_if_empty(val):
        try:
            return int(val)
        except Exception:
            return 0

    return {
        "event_id": g.get("gameId"),
        "game_uid": g.get("gameCode"),
        "date": norm_date,
        "season": season,
        "status_type": g.get("gameStatusText") or safe_str(g.get("gameStatus")) or "Scheduled",
        "home_id": safe_int(home.get("teamId")),
        "home_abbr": home.get("teamTricode"),
        "home_score": zero_if_empty(home.get("score", 0)),
        "away_id": safe_int(away.get("teamId")),
        "away_abbr": away.get("teamTricode"),
        "away_score": zero_if_empty(away.get("score", 0)),
        "game_duration": safe_int(g.get("duration")),
        "attendance": safe_int(g.get("attendance")),
        "arena_name": arena.get("arenaName"),
    }

# -----------------------------
# Season scanning - BoxScore
# -----------------------------
def scan_season_range(start_date: str, end_date: str, season_prefix: str, first_game_id: int, season_start: datetime.datetime) -> Dict[str, List[str]]:
    """
    Scan a single season range with BoxScore IDs. Returns {date: [game_ids]}.
    """
    start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    result: Dict[str, List[str]] = {}

    if end_dt < season_start:
        return result

    if season_prefix == "002240":
        start_id = 0
    elif season_prefix == "002250":
        start_id = 0
    else:
        start_id = first_game_id

    max_days_from_start = max(0, (end_dt - season_start).days)
    max_game_estimate = start_id + (max_days_from_start * 25)
    end_id = max_game_estimate + 200

    boxscore_errors = 0
    consecutive_errors = 0
    for num in range(start_id, end_id + 1):
        gid = f"{season_prefix}{num:04d}"
        try:
            bx = boxscore.BoxScore(gid)
            d = bx.get_dict()
            if "game" in d:
                info = d["game"]
                utc = info.get("gameTimeUTC", "")
                norm = normalize_game_date(utc, start_date)
                norm_dt = datetime.datetime.strptime(norm, "%Y-%m-%d")
                if start_dt <= norm_dt <= end_dt:
                    result.setdefault(norm, []).append(gid)
            consecutive_errors = 0  # Reset on success
        except json.JSONDecodeError as je:
            boxscore_errors += 1
            consecutive_errors += 1
            # Only warn if 10+ consecutive errors (indicates real problem)
            if consecutive_errors > 10:
                error_tracker.add_warning(
                    "boxscore_api_issues",
                    f"Season {season_prefix}: 10+ consecutive JSON errors - NBA API may be having issues"
                )
                break
        except Exception:
            consecutive_errors = 0  # Reset on other exceptions
            continue
    
    if boxscore_errors > 0 and consecutive_errors <= 10:
        error_tracker.add_warning("boxscore_fetch_errors", f"Season {season_prefix}: {boxscore_errors} JSON parse errors (transient, continuing)")
    
    return result

# -----------------------------
# Mapping dates to games
# -----------------------------
def build_optimized_date_range_games_mapping(start_date: str, end_date: str) -> Dict[str, List[str]]:
    """
    Build date -> game IDs using BoxScore scan, then union with ScoreBoard schedule.
    """
    print(f"\n📅 Building date mapping for {start_date}..{end_date}")
    start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    season_boundary = datetime.datetime(2025, 10, 1)
    date_to_games: Dict[str, List[str]] = {}

    if start_dt < season_boundary and end_dt >= season_boundary:
        # crosses seasons
        part1_end = min(end_dt, season_boundary - datetime.timedelta(days=1))
        if start_dt <= part1_end:
            mapping1 = scan_season_range(start_date, part1_end.strftime("%Y-%m-%d"), "002240", 61, datetime.datetime(2024, 10, 22))
            for k, v in mapping1.items():
                date_to_games.setdefault(k, []).extend(v)
        part2_start = max(start_dt, season_boundary)
        if part2_start <= end_dt:
            mapping2 = scan_season_range(part2_start.strftime("%Y-%m-%d"), end_date, "002250", 0, datetime.datetime(2025, 10, 21))
            for k, v in mapping2.items():
                date_to_games.setdefault(k, []).extend(v)
    else:
        if start_dt >= season_boundary:
            mapping = scan_season_range(start_date, end_date, "002250", 0, datetime.datetime(2025, 10, 21))
        else:
            mapping = scan_season_range(start_date, end_date, "002240", 61, datetime.datetime(2024, 10, 22))
        for k, v in mapping.items():
            date_to_games.setdefault(k, []).extend(v)

    # Always union ScoreBoard schedule for each date in the requested range
    cur = start_dt
    while cur <= end_dt:
        ds = cur.date().isoformat()
        sb_games = fetch_scoreboard_games_for_date(ds)
        for g in sb_games:
            gid = g.get("gameId")
            if not gid:
                continue
            norm = normalize_game_date(g.get("gameTimeUTC", ""), ds)
            if norm < start_date or norm > end_date:
                continue
            if gid not in date_to_games.get(norm, []):
                date_to_games.setdefault(norm, []).append(gid)
        cur += datetime.timedelta(days=1)

    # Filter and sort ids per date
    filtered: Dict[str, List[str]] = {}
    for d, arr in date_to_games.items():
        dt = datetime.datetime.strptime(d, "%Y-%m-%d")
        if start_dt <= dt <= end_dt:
            filtered[d] = sorted(set(arr))
    
    print(f"📊 Found {sum(len(v) for v in filtered.values())} games across {len(filtered)} dates")
    return filtered

def build_date_to_games_mapping(target_date: str) -> Dict[str, List[str]]:
    """Build date mapping - falls back to ScoreBoard if BoxScore fails."""
    mapping = build_optimized_date_range_games_mapping(target_date, target_date)
    
    # If BoxScore scan found nothing, try ScoreBoard directly
    if not mapping or len(mapping.get(target_date, [])) == 0:
        print(f"📡 BoxScore scan found no games, falling back to ScoreBoard...")
        sb_games = fetch_scoreboard_games_for_date(target_date)
        if sb_games:
            game_ids = [g.get("gameId") for g in sb_games if g.get("gameId")]
            if game_ids:
                mapping[target_date] = game_ids
                print(f"📊 ScoreBoard found {len(game_ids)} games for {target_date}")
    
    return mapping

# -----------------------------
# Data extraction
# -----------------------------
def extract_games_from_game_data(games_data: List[Dict[str, Any]], target_date: str) -> pd.DataFrame:
    """Extract rows from a list of BoxScore-style or ScoreBoard-style game dicts."""
    rows = []
    for game in games_data:
        dt_et = normalize_game_date(game.get("gameTimeUTC", ""), target_date)
        year = int(dt_et[:4]); month = int(dt_et[5:7])
        season = year if month >= 10 else year - 1

        home = game.get("homeTeam", {}) or {}
        away = game.get("awayTeam", {}) or {}
        arena = game.get("arena", {}) or {}

        def int_from_score(val):
            try:
                return int(val)
            except Exception:
                return 0

        status_text = game.get("gameStatusText") or safe_str(game.get("gameStatus")) or "Scheduled"

        rows.append({
            "event_id": game.get("gameId"),
            "game_uid": game.get("gameCode"),
            "date": dt_et,
            "season": season,
            "status_type": status_text,
            "home_id": safe_int(home.get("teamId")),
            "home_abbr": home.get("teamTricode"),
            "home_score": int_from_score(home.get("score", 0)),
            "away_id": safe_int(away.get("teamId")),
            "away_abbr": away.get("teamTricode"),
            "away_score": int_from_score(away.get("score", 0)),
            "game_duration": safe_int(game.get("duration")),
            "attendance": safe_int(game.get("attendance")),
            "arena_name": arena.get("arenaName"),
        })

    if not rows:
        return pd.DataFrame(columns=[f.name for f in GAMES_SCHEMA])

    df = pd.DataFrame(rows)
    return coerce_games_dtypes(df)

def get_player_stats_for_game(game_id: str, date_str: str) -> pd.DataFrame:
    """Get player stats. If game not started or no stats yet, returns empty df."""
    max_retries = 2
    for attempt in range(max_retries):
        try:
            bx = boxscore.BoxScore(game_id)
            data = bx.get_dict()
            if "game" not in data:
                return pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])
            game_info = data["game"]
            year = int(date_str[:4])
            month = int(date_str[5:7])
            season = year if month >= 10 else year - 1

            rows = []
            for side in ["homeTeam", "awayTeam"]:
                team = game_info.get(side, {}) or {}
                team_id = safe_int(team.get("teamId"))
                team_abbr = team.get("teamTricode")
                for p in team.get("players", []) or []:
                    if p.get("status") != "ACTIVE":
                        continue
                    stats = p.get("statistics", {}) or {}
                    rows.append({
                        "event_id": game_id,
                        "date": date_str,
                        "season": season,
                        "team_id": team_id,
                        "team_abbr": team_abbr,
                        "player_id": safe_int(p.get("personId")),
                        "player": p.get("name"),
                        "starter": p.get("starter") == "1",
                        "minutes": parse_minutes(stats.get("minutes", "PT00M00.00S")),
                        "pts": safe_int(stats.get("points", 0)),
                        "reb": safe_int(stats.get("reboundsTotal", 0)),
                        "ast": safe_int(stats.get("assists", 0)),
                        "stl": safe_int(stats.get("steals", 0)),
                        "blk": safe_int(stats.get("blocks", 0)),
                        "tov": safe_int(stats.get("turnovers", 0)),
                        "fgm": safe_int(stats.get("fieldGoalsMade", 0)),
                        "fga": safe_int(stats.get("fieldGoalsAttempted", 0)),
                        "fg_pct": safe_float(stats.get("fieldGoalsPercentage", 0)),
                        "fg3m": safe_int(stats.get("threePointersMade", 0)),
                        "fg3a": safe_int(stats.get("threePointersAttempted", 0)),
                        "fg3_pct": safe_float(stats.get("threePointersPercentage", 0)),
                        "ftm": safe_int(stats.get("freeThrowsMade", 0)),
                        "fta": safe_int(stats.get("freeThrowsAttempted", 0)),
                        "ft_pct": safe_float(stats.get("freeThrowsPercentage", 0)),
                        "oreb": safe_int(stats.get("reboundsOffensive", 0)),
                        "dreb": safe_int(stats.get("reboundsDefensive", 0)),
                        "pf": safe_int(stats.get("foulsPersonal", 0)),
                        "plus_minus": safe_float(stats.get("plusMinusPoints", 0)),
                        "position": p.get("position"),
                        "jersey_num": p.get("jerseyNum"),
                    })
            if not rows:
                return pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])
            df = pd.DataFrame(rows)
            return coerce_box_dtypes(df)
        except json.JSONDecodeError as je:
            if attempt < max_retries - 1:
                error_tracker.add_warning("boxscore_json_error", f"Game {game_id}: {str(je)}, retrying...")
                time.sleep(0.5 * (attempt + 1))
                continue
            else:
                # Don't log as critical error - just warning
                error_tracker.add_warning("boxscore_json_error", f"Game {game_id}: Data not available yet - {str(je)}")
                return pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])
        except Exception as e:
            error_tracker.add_warning("boxscore_fetch_error", f"Game {game_id}: {str(e)}")
            return pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])
    return pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])

# -----------------------------
# BigQuery I-O
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
        BQ.create_table(bigquery.Table(games_table_id, schema=GAMES_SCHEMA))
    box_table_id = f"{PROJECT_ID}.{DATASET}.player_boxscores"
    try:
        BQ.get_table(box_table_id)
    except Exception:
        BQ.create_table(bigquery.Table(box_table_id, schema=BOX_SCHEMA))

def load_df(df: pd.DataFrame, table: str) -> bool:
    """Load dataframe to BigQuery. Returns True if successful."""
    if df is None or df.empty:
        return True
    try:
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
        return True
    except Exception as e:
        error_tracker.add_error("bigquery_load_failure", f"Table {table}, rows {len(df)}", str(e))
        return False

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
        "fgm", "fga", "fg3m", "fg3a", "ftm", "fta", "oreb", "dreb", "pf",
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

# -----------------------------
# Game collection by date
# -----------------------------
def get_games_for_date(target_date: str) -> pd.DataFrame:
    """Return a DataFrame of games for target_date."""
    print(f"\n🎮 Collecting games for {target_date}")

    date_mapping = build_date_to_games_mapping(target_date)
    game_ids = date_mapping.get(target_date, [])

    sb_games = fetch_scoreboard_games_for_date(target_date)
    sb_index = {g.get("gameId"): g for g in sb_games if g.get("gameId")}

    collected_games_payloads: List[Dict[str, Any]] = []

    # Try to fetch full BoxScore game dicts first
    for gid in game_ids:
        try:
            bx = boxscore.BoxScore(gid)
            data = bx.get_dict()
            if "game" in data:
                collected_games_payloads.append(data["game"])
                continue
        except Exception:
            pass
        # Fallback to schedule info if BoxScore not available yet
        if gid in sb_index:
            collected_games_payloads.append(sb_index[gid])

    # Add ScoreBoard games not in BoxScore mapping
    for gid, g in sb_index.items():
        if not any(gid == x.get("gameId") for x in collected_games_payloads):
            norm = normalize_game_date(g.get("gameTimeUTC", ""), target_date)
            if norm == target_date:
                collected_games_payloads.append(g)

    if not collected_games_payloads:
        print(f"⚠️  No games found for {target_date}")
        return pd.DataFrame(columns=[f.name for f in GAMES_SCHEMA])

    return extract_games_from_game_data(collected_games_payloads, target_date)

# -----------------------------
# Ingestion flows
# -----------------------------
def ingest_date_nba_live(date_str: str) -> None:
    """Ingest games and stats for a single date."""
    ensure_tables()
    print(f"\n{'='*70}\n🏀 Starting ingestion for {date_str}\n{'='*70}")

    games_df = get_games_for_date(date_str)
    if games_df.empty:
        error_tracker.add_warning("no_games_found", f"Date {date_str}")
        error_tracker.set_stat("games_loaded", 0)
        error_tracker.set_stat("player_rows_loaded", 0)
        return

    if not load_df(games_df, "games_daily"):
        return

    error_tracker.set_stat("games_loaded", len(games_df))
    print(f"✅ Loaded {len(games_df)} games")

    playable_statuses = {"Final", "Halftime", "3rd Qtr", "4th Qtr", "End Q1", "End Q2", "End Q3", "In Progress", "Live"}
    stats_total = 0
    for _, row in games_df.iterrows():
        status = (row.get("status_type") or "").strip()
        if not status or status.lower().startswith("sched") or status.lower().startswith("pre"):
            continue
        gid = row["event_id"]
        ps = get_player_stats_for_game(gid, date_str)
        if not ps.empty:
            if load_df(ps, "player_boxscores"):
                stats_total += len(ps)
            time.sleep(0.3)
    
    error_tracker.set_stat("player_rows_loaded", stats_total)
    print(f"✅ Loaded {stats_total} player stats rows")

def ingest_date_range_nba_live(start_date: str, end_date: str) -> None:
    """Ingest a date range."""
    ensure_tables()
    print(f"\n{'='*70}\n📅 Range ingestion {start_date}..{end_date}\n{'='*70}")

    mapping = build_optimized_date_range_games_mapping(start_date, end_date)
    
    # Preload ScoreBoard per date
    sb_by_date: Dict[str, Dict[str, Any]] = {}
    cur = datetime.datetime.fromisoformat(start_date)
    end = datetime.datetime.fromisoformat(end_date)
    while cur <= end:
        ds = cur.date().isoformat()
        sb_games = fetch_scoreboard_games_for_date(ds)
        sb_by_date[ds] = {g.get("gameId"): g for g in sb_games if g.get("gameId")}
        cur += datetime.timedelta(days=1)

    # If mapping is empty, use ScoreBoard for all dates
    if not mapping:
        print("⚠️  BoxScore mapping empty, using ScoreBoard for all dates...")
        mapping = {}
        for ds, games_dict in sb_by_date.items():
            if games_dict:
                mapping[ds] = list(games_dict.keys())

    if not mapping:
        print("❌ No games found in range")
        error_tracker.set_stat("games_loaded", 0)
        error_tracker.set_stat("player_rows_loaded", 0)
        return

    all_game_rows: List[pd.DataFrame] = []
    all_stats_rows: List[pd.DataFrame] = []

    for ds in sorted(mapping.keys()):
        ids = set(mapping[ds])
        ids |= set(sb_by_date.get(ds, {}).keys())

        daily_payloads: List[Dict[str, Any]] = []
        for gid in sorted(ids):
            used_boxscore = False
            try:
                bx = boxscore.BoxScore(gid)
                d = bx.get_dict()
                if "game" in d:
                    daily_payloads.append(d["game"])
                    used_boxscore = True
            except Exception:
                pass
            if not used_boxscore:
                sg = sb_by_date.get(ds, {}).get(gid)
                if sg:
                    daily_payloads.append(sg)

            time.sleep(0.2)

        if daily_payloads:
            games_df = extract_games_from_game_data(daily_payloads, ds)
            if not games_df.empty:
                all_game_rows.append(games_df)

                for _, r in games_df.iterrows():
                    status = (r.get("status_type") or "").strip()
                    if not status or status.lower().startswith("sched") or status.lower().startswith("pre"):
                        continue
                    gid = r["event_id"]
                    ps = get_player_stats_for_game(gid, ds)
                    if not ps.empty:
                        all_stats_rows.append(ps)

    if all_game_rows:
        combined_games = pd.concat(all_game_rows, ignore_index=True)
        if load_df(combined_games, "games_daily"):
            error_tracker.set_stat("games_loaded", len(combined_games))
            print(f"✅ Loaded {len(combined_games)} games across {len(all_game_rows)} days")

    if all_stats_rows:
        combined_stats = pd.concat(all_stats_rows, ignore_index=True)
        if load_df(combined_stats, "player_boxscores"):
            error_tracker.set_stat("player_rows_loaded", len(combined_stats))
            print(f"✅ Loaded {len(combined_stats)} player rows")

# --------
# CLI
# --------
def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest NBA data - includes schedule for unplayed games")
    parser.add_argument("--mode", choices=["daily", "backfill"], default="daily")
    parser.add_argument("--start", help="YYYY-MM-DD start date for backfill")
    parser.add_argument("--end", help="YYYY-MM-DD end date for backfill")
    parser.add_argument("--date", help="YYYY-MM-DD specific date")
    args = parser.parse_args()

    try:
        if args.date:
            ingest_date_nba_live(args.date)
        elif args.mode == "daily":
            yesterday = datetime.date.today() - datetime.timedelta(days=1)
            ingest_date_nba_live(yesterday.isoformat())
        elif args.mode == "backfill":
            if not args.start or not args.end:
                print("Error: backfill requires --start and --end")
                sys.exit(1)
            start_date = datetime.date.fromisoformat(args.start)
            end_date = datetime.date.fromisoformat(args.end)
            date_diff = (end_date - start_date).days + 1
            if date_diff <= 60:
                ingest_date_range_nba_live(args.start, args.end)
            else:
                current = start_date
                while current <= end_date:
                    chunk_end = min(current + datetime.timedelta(days=59), end_date)
                    print(f"📦 Chunk {current.isoformat()}..{chunk_end.isoformat()}")
                    ingest_date_range_nba_live(current.isoformat(), chunk_end.isoformat())
                    current = chunk_end + datetime.timedelta(days=1)
                    if current <= end_date:
                        print("⏳ Sleeping 10 seconds between chunks...")
                        time.sleep(10)
            print(f"✅ Backfill complete {args.start}..{args.end}")
    finally:
        # Always print summary
        print(error_tracker.get_summary())
        
        # Exit with error code if there were critical errors
        if error_tracker.should_exit_with_error():
            print("🚨 EXITING WITH ERROR DUE TO CRITICAL FAILURES")
            sys.exit(1)

if __name__ == "__main__":
    main()
