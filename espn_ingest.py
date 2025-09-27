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
PROJECT_ID = os.environ["GCP_PROJECT_ID"]         # set as repo secret
DATASET    = os.environ.get("BQ_DATASET", "nba_data")
SA_INFO    = json.loads(os.environ["GCP_SA_KEY"])
CREDS      = service_account.Credentials.from_service_account_info(SA_INFO)
BQ         = bigquery.Client(project=PROJECT_ID, credentials=CREDS)

# -----------------------------------
# nba_api - Stats endpoints for daily
# ScoreboardV2 + BoxScoreTraditionalV2
# Docs and community refs confirm params:
# - ScoreboardV2(game_date=MM/DD/YYYY)
# - BoxScoreTraditionalV2(game_id=...)
# -----------------------------------
from nba_api.stats.endpoints import scoreboardv2, boxscoretraditionalv2  # noqa: E402
# refs: ScoreboardV2 args and BoxScoreTraditionalV2 PlayerStats frame
# hoopR docs for these endpoints and frames: scoreboardv2, boxscoretraditionalv2
# and nba_api docs site.  (citations in chat, not in code)

# -----------------------------------
# ESPN endpoints for backfill
# -----------------------------------
ESPN_SCOREBOARD     = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
ESPN_SUMMARY        = "https://site.web.api.espn.com/apis/site/v2/sports/basketball/nba/summary"
ESPN_SITE_BOXSCORE  = "https://site.web.api.espn.com/apis/site/v2/sports/basketball/nba/boxscore"
ESPN_CORE_EVENT_RT  = "https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}"

# some ESPN edges return trimmed payloads unless you look like a browser and set region/lang
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.espn.com/",
    "Origin": "https://www.espn.com",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}
DEFAULT_PARAMS = {"region": "us", "lang": "en", "xhr": "1"}

def http_get_json(url: str, params: Optional[Dict[str, Any]] = None, max_retries: int = 3) -> Dict[str, Any]:
    last_err = None
    merged = dict(DEFAULT_PARAMS)
    if params:
        merged.update(params)
    for _ in range(max_retries):
        try:
            r = requests.get(url, params=merged, headers=HEADERS, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(0.8)
    raise last_err if last_err else RuntimeError("request failed")

# -----------------------------------
# BigQuery schemas and helpers
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
    bigquery.SchemaField("event_id", "STRING"),   # we store NBA game_id for nba_api rows
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

# -----------------------------------
# Type coercion to keep Arrow happy
# -----------------------------------
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

# -----------------------------------
# Utilities and ESPN fetchers for backfill
# -----------------------------------
def safe_int(x: Any) -> Optional[int]:
    try:
        return int(x) if x is not None and x != "" else None
    except Exception:
        return None

def get_scoreboard_for_date_yyyymmdd(yyyymmdd: str) -> Dict[str, Any]:
    return http_get_json(ESPN_SCOREBOARD, params={"dates": yyyymmdd})

def list_event_ids(scoreboard_json: Dict[str, Any]) -> List[str]:
    events = scoreboard_json.get("events", []) or []
    return [e.get("id") for e in events if e.get("id")]

def get_summary(event_id: str) -> Dict[str, Any]:
    return http_get_json(ESPN_SUMMARY, params={"event": event_id})

def fetch_core_boxscore_players(event_id: str) -> List[Dict[str, Any]]:
    try:
        ev = http_get_json(ESPN_CORE_EVENT_RT.format(event_id=event_id))
        comps = ev.get("competitions") or []
        if not comps or not isinstance(comps[0], dict) or not comps[0].get("$ref"):
            return []
        comp = http_get_json(comps[0]["$ref"])
        box_ref = (comp.get("boxscore") or {}).get("$ref")
        if not box_ref:
            return []
        j = http_get_json(box_ref)
        rows: List[Dict[str, Any]] = []
        teams = j.get("teams") or []
        for t in teams:
            team_ref = t.get("team")
            team_obj = {}
            if isinstance(team_ref, dict) and team_ref.get("$ref"):
                team_obj = http_get_json(team_ref["$ref"])
            team_id = safe_int(team_obj.get("id"))
            team_abbr = team_obj.get("abbreviation") or team_obj.get("shortDisplayName")
            for p in (t.get("players") or []):
                ath_ref = p.get("athlete")
                ath = {}
                if isinstance(ath_ref, dict) and ath_ref.get("$ref"):
                    ath = http_get_json(ath_ref["$ref"])
                player_id = safe_int(ath.get("id"))
                player_name = ath.get("displayName")
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
                    "team_id": team_id, "team_abbr": team_abbr,
                    "player_id": player_id, "player": player_name,
                    "starter": bool(p.get("starter")) if p.get("starter") is not None else None,
                    "minutes": stat_map.get("minutes"),
                    "pts": gi("points","pts"), "reb": gi("rebounds","reb"), "ast": gi("assists","ast"),
                    "stl": gi("steals","stl"), "blk": gi("blocks","blk"), "tov": gi("turnovers","to","tov"),
                    "fgm": gi("fieldGoalsMade","fgm"), "fga": gi("fieldGoalsAttempted","fga"),
                    "fg3m": gi("threePointFieldGoalsMade","fg3m"), "fg3a": gi("threePointFieldGoalsAttempted","fg3a"),
                    "ftm": gi("freeThrowsMade","ftm"), "fta": gi("freeThrowsAttempted","fta"),
                    "oreb": gi("offensiveRebounds","oreb"), "dreb": gi("defensiveRebounds","dreb"), "pf": gi("fouls","pf"),
                })
        return rows
    except requests.HTTPError as e:
        print(f"core boxscore traversal failed for event {event_id}: {e}")
        return []
    except Exception as e:
        print(f"core boxscore unexpected error for event {event_id}: {e}")
        return []

def fetch_site_boxscore_players(event_id: str) -> List[Dict[str, Any]]:
    try:
        j = http_get_json(ESPN_SITE_BOXSCORE, params={"event": event_id})
        rows: List[Dict[str, Any]] = []
        box = j.get("boxscore") or j
        teams = (box.get("teams") or [])
        for t in teams:
            team = t.get("team") or {}
            tid = safe_int(team.get("id"))
            tabbr = team.get("abbreviation") or team.get("shortDisplayName")
            players = t.get("players") or []
            for p in players:
                ath = p.get("athlete") or {}
                pid = safe_int(ath.get("id"))
                name = ath.get("displayName")
                starter = bool(p.get("starter")) if p.get("starter") is not None else None
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
                    "team_id": tid, "team_abbr": tabbr,
                    "player_id": pid, "player": name,
                    "starter": starter, "minutes": stat_map.get("minutes"),
                    "pts": gi("points","pts"), "reb": gi("rebounds","reb"), "ast": gi("assists","ast"),
                    "stl": gi("steals","stl"), "blk": gi("blocks","blk"), "tov": gi("turnovers","to","tov"),
                    "fgm": gi("fieldGoalsMade","fgm"), "fga": gi("fieldGoalsAttempted","fga"),
                    "fg3m": gi("threePointFieldGoalsMade","fg3m"), "fg3a": gi("threePointFieldGoalsAttempted","fg3a"),
                    "ftm": gi("freeThrowsMade","ftm"), "fta": gi("freeThrowsAttempted","fta"),
                    "oreb": gi("offensiveRebounds","oreb"), "dreb": gi("defensiveRebounds","dreb"), "pf": gi("fouls","pf"),
                })
        return rows
    except Exception as e:
        print(f"site boxscore fallback failed for event {event_id}: {e}")
        return []

# -----------------------------------
# Normalizers
# -----------------------------------
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
        "event_id": event_id, "game_uid": uid, "date": iso_date,
        "season": season, "status_type": status_type,
        "home_id": hid, "home_abbr": habbr, "home_score": hscore,
        "away_id": aid, "away_abbr": aabbr, "away_score": ascore,
    }])
    return coerce_games_dtypes(df)

def normalize_player_box(event_id: str, summary: Dict[str, Any], fallback_date_iso: str) -> pd.DataFrame:
    header = summary.get("header", {}) or {}
    competitions = header.get("competitions", []) or []
    comp = competitions[0] if competitions else {}
    iso_date = (comp.get("date", "") or "")[:10] or fallback_date_iso
    season = ((header.get("season") or {}).get("year"))
    rows: List[Dict[str, Any]] = []

    # source 1 - site summary
    box = (summary.get("boxscore") or {})
    teams = box.get("teams", []) or []
    for t in teams:
        team = t.get("team") or {}
        tid = safe_int(team.get("id"))
        tabbr = team.get("abbreviation") or team.get("shortDisplayName")
        players = t.get("players") or []
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
                "event_id": event_id, "date": iso_date, "season": season,
                "team_id": tid, "team_abbr": tabbr, "player_id": pid, "player": name,
                "starter": starter, "minutes": p.get("minutes"),
                "pts": gi("points","pts"), "reb": gi("rebounds","reb"), "ast": gi("assists","ast"),
                "stl": gi("steals","stl"), "blk": gi("blocks","blk"), "tov": gi("turnovers","to","tov"),
                "fgm": gi("fieldGoalsMade","fgm"), "fga": gi("fieldGoalsAttempted","fga"),
                "fg3m": gi("threePointFieldGoalsMade","fg3m"), "fg3a": gi("threePointFieldGoalsAttempted","fg3a"),
                "ftm": gi("freeThrowsMade","ftm"), "fta": gi("freeThrowsAttempted","fta"),
                "oreb": gi("offensiveRebounds","oreb"), "dreb": gi("defensiveRebounds","dreb"), "pf": gi("fouls","pf"),
            })

    if rows:
        print(f"event {event_id}: site summary players -> {len(rows)}")
    else:
        # source 2 - core traversal
        core_rows = fetch_core_boxscore_players(event_id)
        if core_rows:
            for r in core_rows:
                r.update({"event_id": event_id, "date": iso_date, "season": season})
            rows = core_rows
            print(f"event {event_id}: core boxscore players -> {len(rows)}")
        else:
            # source 3 - site boxscore endpoint
            site_rows = fetch_site_boxscore_players(event_id)
            if site_rows:
                for r in site_rows:
                    r.update({"event_id": event_id, "date": iso_date, "season": season})
                rows = site_rows
                print(f"event {event_id}: site boxscore fallback -> {len(rows)}")
            else:
                print(f"event {event_id}: no players found in any source")

    df = pd.DataFrame(rows, columns=[f.name for f in BOX_SCHEMA]) if rows else pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])
    return coerce_box_dtypes(df)

# -----------------------------------
# nba_api daily - player boxes by date
# -----------------------------------
def fetch_player_boxes_via_nba_api(date_iso: str) -> pd.DataFrame:
    """
    Use NBA Stats via nba_api for per-player box scores for a single date.
    - ScoreboardV2(game_date=MM/DD/YYYY) -> GAME_IDs
    - BoxScoreTraditionalV2(game_id=...) -> PlayerStats
    """
    mmddyyyy = f"{date_iso[5:7]}/{date_iso[8:10]}/{date_iso[:4]}"
    sb = scoreboardv2.ScoreboardV2(game_date=mmddyyyy, day_offset=0, league_id="00")
    frames = sb.get_data_frames()

    game_header_df = None
    for df in frames:
        if "GAME_ID" in df.columns:
            game_header_df = df
            break
    if game_header_df is None or game_header_df.empty:
        return pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])

    game_ids = game_header_df["GAME_ID"].astype(str).unique().tolist()

    rows = []
    for gid in game_ids:
        bs = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=gid)
        bs_frames = bs.get_data_frames()
        player_stats_df = None
        for df in bs_frames:
            if set(["PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "TEAM_ABBREVIATION"]).issubset(df.columns):
                player_stats_df = df
                break
        if player_stats_df is None or player_stats_df.empty:
            continue

        for _, r in player_stats_df.iterrows():
            rows.append({
                "event_id": gid,          # store NBA game id here
                "date": date_iso,
                "season": None,           # fill below from date
                "team_id": int(r["TEAM_ID"]) if pd.notna(r["TEAM_ID"]) else None,
                "team_abbr": r.get("TEAM_ABBREVIATION"),
                "player_id": int(r["PLAYER_ID"]) if pd.notna(r["PLAYER_ID"]) else None,
                "player": r.get("PLAYER_NAME"),
                "starter": bool(r["START_POSITION"] not in [None, "", ""]) if "START_POSITION" in r else None,
                "minutes": r.get("MIN"),
                "pts": int(r["PTS"]) if pd.notna(r.get("PTS")) else None,
                "reb": int(r["REB"]) if pd.notna(r.get("REB")) else None,
                "ast": int(r["AST"]) if pd.notna(r.get("AST")) else None,
                "stl": int(r["STL"]) if pd.notna(r.get("STL")) else None,
                "blk": int(r["BLK"]) if pd.notna(r.get("BLK")) else None,
                "tov": int(r["TOV"]) if pd.notna(r.get("TOV")) else None,
                "fgm": int(r["FGM"]) if pd.notna(r.get("FGM")) else None,
                "fga": int(r["FGA"]) if pd.notna(r.get("FGA")) else None,
                "fg3m": int(r["FG3M"]) if pd.notna(r.get("FG3M")) else None,
                "fg3a": int(r["FG3A"]) if pd.notna(r.get("FG3A")) else None,
                "ftm": int(r["FTM"]) if pd.notna(r.get("FTM")) else None,
                "fta": int(r["FTA"]) if pd.notna(r.get("FTA")) else None,
                "oreb": int(r["OREB"]) if pd.notna(r.get("OREB"]) else None,
                "dreb": int(r["DREB"]) if pd.notna(r.get("DREB"]) else None,
                "pf": int(r["PF"]) if pd.notna(r.get("PF"]) else None,
            })

    if not rows:
        return pd.DataFrame(columns=[f.name for f in BOX_SCHEMA])

    df = pd.DataFrame(rows, columns=[f.name for f in BOX_SCHEMA])

    # infer season year from calendar date - Oct or later counts as that season year
    try:
        y = int(date_iso[:4]); m = int(date_iso[5:7])
        season_year = y if m >= 10 else (y - 1)
        df["season"] = season_year
    except Exception:
        pass

    return coerce_box_dtypes(df)

# -----------------------------------
# Orchestration
# -----------------------------------
def yyyymmdd_list(start: datetime.date, end: datetime.date) -> List[str]:
    out = []
    cur = start
    while cur <= end:
        out.append(cur.strftime("%Y%m%d"))
        cur += datetime.timedelta(days=1)
    return out

def ingest_dates_via_espn(ymd_list: List[str]) -> None:
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
            has_box = bool((summary.get("boxscore") or {}).get("teams"))
            print(f"event {eid}: summary box teams present? {has_box}")

            gdf = normalize_game_row(eid, summary, fallback_iso)
            pdf = normalize_player_box(eid, summary, fallback_iso)

            if not gdf.empty:
                total_games_rows += len(gdf)
                games_frames.append(gdf)
            if not pdf.empty:
                total_box_rows += len(pdf)
                box_frames.append(pdf)

            time.sleep(0.2)
        time.sleep(0.4)

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
    parser = argparse.ArgumentParser(description="Ingest NBA games and player box scores into BigQuery")
    parser.add_argument("--mode", choices=["backfill", "daily"], required=True, help="backfill for a date range via ESPN, daily uses nba_api for boxes then ESPN for games")
    parser.add_argument("--start", help="YYYY-MM-DD inclusive start for backfill")
    parser.add_argument("--end", help="YYYY-MM-DD inclusive end for backfill")
    args = parser.parse_args()

    if args.mode == "daily":
        # yesterday in UTC - schedule your cron sufficiently late in the morning
        yday = datetime.date.today() - datetime.timedelta(days=1)
        date_iso = yday.isoformat()
        print(f"daily via nba_api for {date_iso}")
        ensure_tables()

        # 1) try nba_api for player boxes on that date
        try:
            nba_df = fetch_player_boxes_via_nba_api(date_iso)
            if not nba_df.empty:
                print(f"nba_api player rows: {len(nba_df)}")
                load_df(nba_df, "player_boxscores")
            else:
                print("nba_api returned 0 player rows - will rely on ESPN for players if present")
        except Exception as e:
            print(f"nba_api daily path errored: {e} - will rely on ESPN only")

        # 2) always ingest games via ESPN for that date
        ingest_dates_via_espn([yday.strftime('%Y%m%d')])
        print(f"Daily ingest complete for {date_iso}")
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
        ingest_dates_via_espn(dates)
        print(f"Backfill complete for {args.start} to {args.end}")
        return

if __name__ == "__main__":
    main()
