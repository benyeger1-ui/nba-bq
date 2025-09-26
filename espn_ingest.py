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
PROJECT_ID = os.environ["GCP_PROJECT_ID"]  # set in GitHub Actions secrets
DATASET = os.environ.get("BQ_DATASET", "nba_data")

# Service account JSON is stored as a single secret string
SA_INFO = json.loads(os.environ["GCP_SA_KEY"])
CREDS = service_account.Credentials.from_service_account_info(SA_INFO)
BQ = bigquery.Client(project=PROJECT_ID, credentials=CREDS)

# ESPN free site APIs
ESPN_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
ESPN_SUMMARY = "https://site.web.api.espn.com/apis/site/v2/sports/basketball/nba/summary"
ESPN_CORE_BOXSCORE = (
    "https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba"
    "/events/{event}/competitions/{event}/boxscore"
)

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

from google.cloud import bigquery

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

def fetch_core_boxscore_players(event_id: str) -> List[Dict[str, Any]]:
    """
    Fallback for when site summary has no players.
    Walks ESPN core boxscore graph and returns a flat list of player stat rows.
    """
    url = ESPN_CORE_BOXSCORE.format(event=event_id)
    j = http_get_json(url)

    rows: List[Dict[str, Any]] = []

    # Core format: j["teams"] is a list; each team has "team" (ref) and "statistics" and "players" (list)
    teams = j.get("teams") or []
    for t in teams:
        team_ref = t.get("team")  # a ref dict with "$ref"
        team_obj = {}
        if isinstance(team_ref, dict) and team_ref.get("$ref"):
            team_obj = http_get_json(team_ref["$ref"])
        team_id = safe_int(team_obj.get("id"))
        team_abbr = team_obj.get("abbreviation") or team_obj.get("shortDisplayName")

        players = t.get("players") or []
        for p in players:
            # each player entry is usually a dict with "athlete" (ref) and "statistics" (list)
            ath_ref = p.get("athlete")
            ath = {}
            if isinstance(ath_ref, dict) and ath_ref.get("$ref"):
                ath = http_get_json(ath_ref["$ref"])
            player_id = safe_int(ath.get("id"))
            player_name = ath.get("displayName")

            # stats list -> normalize by "name"
            stat_map: Dict[str, Any] = {}
            for st in (p.get("statistics") or []):
                name = st.get("name")
                val = st.get("value")
                if name:
                    stat_map[name] = val

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

    return rows



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
    for _ in range(max_retries):
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(0.5)
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

def normalize_player_box(event_id: str, summary: Dict[str, Any], fallback_date_iso: str) -> pd.DataFrame:
    # preferred path - site summary
    header = summary.get("header", {}) or {}
    competitions = header.get("competitions", []) or []
    comp = competitions[0] if competitions else {}
    iso_date = (comp.get("date", "") or "")[:10] or fallback_date_iso
    season = ((header.get("season") or {}).get("year"))

    rows: List[Dict[str, Any]] = []

    box = (summary.get("boxscore") or {})
    teams = box.get("teams", []) or []
    for t in teams:
        team = t.get("team") or {}
        tid = safe_int(team.get("id"))
        tabbr = team.get("abbreviation") or team.get("shortDisplayName")
        players = t.get("players", []) or []
        for p in players:
            ath = p.get("athlete") or {}
            pid = safe_int(ath.get("id"))
            name = ath.get("displayName")
            starter_val = p.get("starter")
            starter = bool(starter_val) if starter_val is not None else None
            stats2 = p.get("stats") or {}

            def gi(*keys):
                for k in keys:
                    if k in stats2 and stats2[k] is not None:
                        return safe_int(stats2[k])
                return None

            rows.append({
                "event_id": event_id,
                "date": iso_date,
                "season": season,
                "team_id": tid,
                "team_abbr": tabbr,
                "player_id": pid,
                "player": name,
                "starter": starter,
                "minutes": p.get("minutes"),
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

    # Fallback to core API if site summary had no players
    if not rows:
        core_rows = fetch_core_boxscore_players(event_id)
        for r in core_rows:
            r.update({"event_id": event_id, "date": iso_date, "season": season})
        rows = core_rows

    df = pd.DataFrame(rows, columns=[f.name for f in BOX_SCHEMA]) if rows else pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])
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
        sb = get_scoreboard_for_date_yyyymmdd(ymd)
        event_ids = list_event_ids(sb)
        print(f"[{fallback_iso}] events: {len(event_ids)}")
        total_events += len(event_ids)

        for eid in event_ids:
            summary = get_summary(eid)
        
            gdf = normalize_game_row(eid, summary, fallback_iso)
            pdf_site = normalize_player_box(eid, summary, fallback_iso)
        
            if pdf_site.empty:
                print(f"[{fallback_iso}] event {eid}: site summary had no players, tried core fallback")
            else:
                print(f"[{fallback_iso}] event {eid}: player rows via site/core -> {len(pdf_site)}")
        
            if not gdf.empty:
                games_frames.append(gdf)
            if not pdf_site.empty:
                box_frames.append(pdf_site)
        
            time.sleep(0.15)

        time.sleep(0.35)

    if games_frames:
        games_df = pd.concat(games_frames, ignore_index=True)
        games_df = coerce_games_dtypes(games_df)
        print(f"loading games rows: {len(games_df)}")
        load_df(games_df, "games_daily")
    else:
        print("no games to load")

    if box_frames:
        box_df = pd.concat(box_frames, ignore_index=True)
        box_df = coerce_box_dtypes(box_df)
        print(f"loading player box rows: {len(box_df)}")
        load_df(box_df, "player_boxscores")
    else:
        print("no player boxes to load")

    print(f"summary - dates:{len(ymd_list)} events:{total_events} games_rows:{total_games_rows} box_rows:{total_box_rows}")

def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest NBA games and player box scores from ESPN free site APIs into BigQuery")
    parser.add_argument("--mode", choices=["backfill", "daily"], required=True, help="backfill for a date range, daily for yesterday")
    parser.add_argument("--start", help="YYYY-MM-DD inclusive start for backfill")
    parser.add_argument("--end", help="YYYY-MM-DD inclusive end for backfill")
    args = parser.parse_args()

    if args.mode == "daily":
        yday = datetime.date.today() - datetime.timedelta(days=1)
        ymd = yday.strftime("%Y%m%d")
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
        ingest_dates(dates)
        print(f"Backfill complete for {args.start} to {args.end}")
        return

if __name__ == "__main__":
    main()
