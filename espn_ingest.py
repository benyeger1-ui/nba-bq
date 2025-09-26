import os, sys, json, time, math, argparse, datetime
import pandas as pd
import requests
from urllib.parse import urlencode
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET = os.environ.get("BQ_DATASET", "nba_data")

SA_INFO = json.loads(os.environ["GCP_SA_KEY"])
CREDS = service_account.Credentials.from_service_account_info(SA_INFO)
BQ = bigquery.Client(project=PROJECT_ID, credentials=CREDS)

ESPN_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
ESPN_SUMMARY = "https://site.web.api.espn.com/apis/site/v2/sports/basketball/nba/summary"

# ---------- BigQuery helpers ----------
def ensure_tables():
    # games_daily
    games_table_id = f"{PROJECT_ID}.{DATASET}.games_daily"
    try:
        BQ.get_table(games_table_id)
    except Exception:
        schema = [
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
        t = bigquery.Table(games_table_id, schema=schema)
        t.time_partitioning = bigquery.TimePartitioning(field="date")
        BQ.create_table(t)

    # player_boxscores
    pbox_table_id = f"{PROJECT_ID}.{DATASET}.player_boxscores"
    try:
        BQ.get_table(pbox_table_id)
    except Exception:
        schema = [
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
        t = bigquery.Table(pbox_table_id, schema=schema)
        t.time_partitioning = bigquery.TimePartitioning(field="date")
        BQ.create_table(t)

def already_loaded(table, the_date):
    sql = f"SELECT COUNT(*) c FROM `{PROJECT_ID}.{DATASET}.{table}` WHERE date=@d"
    job = BQ.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("d","DATE",the_date)]
    ))
    return next(job.result()).c > 0

def load_df(df, table):
    if df is None or df.empty:
        return
    table_id = f"{PROJECT_ID}.{DATASET}.{table}"
    BQ.load_table_from_dataframe(df, table_id).result()

# ---------- ESPN fetchers ----------
def get_scoreboard(date_yyyymmdd=None, date_range=None):
    # date_yyyymmdd like "20250115"
    # date_range like "20241001-20241007"
    params = {}
    if date_yyyymmdd:
        params["dates"] = date_yyyymmdd
    if date_range:
        params["dates"] = date_range
    url = ESPN_SCOREBOARD + ("?" + urlencode(params) if params else "")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

def list_event_ids(sb_json):
    events = sb_json.get("events", []) or []
    ids = []
    for e in events:
        # event id string like "401585123"
        ids.append(e.get("id"))
    return [i for i in ids if i]

def fetch_summary(event_id):
    r = requests.get(ESPN_SUMMARY, params={"event": event_id}, timeout=30)
    r.raise_for_status()
    return r.json()

# ---------- Normalizers ----------
def normalize_game_row(event_id, summary):
    header = summary.get("header", {})
    competitions = header.get("competitions", []) or []
    comp = competitions[0] if competitions else {}
    season = (header.get("season") or {}).get("year")
    status_type = ((comp.get("status") or {}).get("type") or {}).get("name")
    competitors = comp.get("competitors", []) or []
    home = next((c for c in competitors if c.get("homeAway")=="home"), {})
    away = next((c for c in competitors if c.get("homeAway")=="away"), {})
    def team_bits(c):
        team = c.get("team") or {}
        abbr = team.get("abbreviation")
        tid = int(team.get("id")) if team.get("id") else None
        score = int(c.get("score")) if c.get("score") else None
        return tid, abbr, score
    hid, habbr, hscore = team_bits(home)
    aid, aabbr, ascore = team_bits(away)

    # date
    # header["competitions"][0]["date"] is ISO. Partition by local calendar date of game.
    iso = comp.get("date", "")[:10]  # YYYY-MM-DD

    return pd.DataFrame([{
        "event_id": event_id,
        "game_uid": header.get("uid"),
        "date": iso,
        "season": season,
        "status_type": status_type,
        "home_id": hid, "home_abbr": habbr, "home_score": hscore,
        "away_id": aid, "away_abbr": aabbr, "away_score": ascore,
    }])

def normalize_player_box(event_id, summary):
    box = (summary.get("boxscore") or {})
    teams = box.get("teams", []) or []
    rows = []
    # teams[0]["statistics"] are aggregates. We want "players"
    for t in teams:
        team = t.get("team") or {}
        tid = int(team.get("id")) if team.get("id") else None
        tabbr = team.get("abbreviation")
        players = t.get("players", []) or []
        # Each "player" has "statistics" and "starter" etc
        for p in players:
            ath = p.get("athlete") or {}
            pid = int(ath.get("id")) if ath.get("id") else None
            name = ath.get("displayName")
            starter = bool(p.get("starter"))
            stats = p.get("statistics", []) or []
            # ESPN bundles stats into groups. Find the "statistics" list with common keys.
            # Safest: flatten all numeric-like fields if present.
            flat = {"pts":None,"reb":None,"ast":None,"stl":None,"blk":None,"tov":None,
                    "fgm":None,"fga":None,"fg3m":None,"fg3a":None,"ftm":None,"fta":None,
                    "oreb":None,"dreb":None,"pf":None,"minutes":p.get("minutes")}
            for group in stats:
                for item in group.get("athletes", []) or []:
                    # not needed
                    pass
                # Some responses use group["stats"] with strings like "FG 7-15"
            # Many summaries provide a top-level "statistics" key under player with parsed ints
            # Newer v2 summary often provides "stats" dict directly:
            s2 = p.get("stats") or {}
            def as_int(x):
                try:
                    return int(x) if x is not None else None
                except:
                    return None
            flat["pts"]  = as_int(s2.get("points", s2.get("pts")))
            flat["reb"]  = as_int(s2.get("rebounds", s2.get("reb")))
            flat["ast"]  = as_int(s2.get("assists", s2.get("ast")))
            flat["stl"]  = as_int(s2.get("steals", s2.get("stl")))
            flat["blk"]  = as_int(s2.get("blocks", s2.get("blk")))
            flat["tov"]  = as_int(s2.get("turnovers", s2.get("to", s2.get("tov"))))
            flat["fgm"]  = as_int(s2.get("fieldGoalsMade", s2.get("fgm")))
            flat["fga"]  = as_int(s2.get("fieldGoalsAttempted", s2.get("fga")))
            flat["fg3m"] = as_int(s2.get("threePointFieldGoalsMade", s2.get("fg3m")))
            flat["fg3a"] = as_int(s2.get("threePointFieldGoalsAttempted", s2.get("fg3a")))
            flat["ftm"]  = as_int(s2.get("freeThrowsMade", s2.get("ftm")))
            flat["fta"]  = as_int(s2.get("freeThrowsAttempted", s2.get("fta")))
            flat["oreb"] = as_int(s2.get("offensiveRebounds", s2.get("oreb")))
            flat["dreb"] = as_int(s2.get("defensiveRebounds", s2.get("dreb")))
            flat["pf"]   = as_int(s2.get("fouls", s2.get("pf")))
            rows.append({
                "event_id": event_id,
                "date": (summary.get("header",{}).get("competitions",[{}])[0].get("date","")[:10]),
                "season": (summary.get("header",{}).get("season") or {}).get("year"),
                "team_id": tid, "team_abbr": tabbr,
                "player_id": pid, "player": name, "starter": starter,
                **flat
            })
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=[
        "event_id","date","season","team_id","team_abbr","player_id","player","starter",
        "minutes","pts","reb","ast","stl","blk","tov","fgm","fga","fg3m","fg3a","ftm","fta",
        "oreb","dreb","pf"
    ])

# ---------- Orchestration ----------
def ingest_for_dates(dates_list):
    ensure_tables()
    all_games_df = []
    all_players_df = []
    for ymd in dates_list:
        # skip if games already exist for that calendar date in games_daily
        yyyy_mm_dd = f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}"
        # we do not skip by partition for player_boxscores since we load both together
        sb = get_scoreboard(date_yyyymmdd=ymd)  # ESPN scoreboard for that date
        events = list_event_ids(sb)
        for eid in events:
            summary = fetch_summary(eid)
            gdf = normalize_game_row(eid, summary)
            pdf = normalize_player_box(eid, summary)
            all_games_df.append(gdf)
            all_players_df.append(pdf)
        time.sleep(0.4)  # be polite between days
    if all_games_df:
        load_df(pd.concat(all_games_df, ignore_index=True), "games_daily")
    if all_players_df:
        load_df(pd.concat(all_players_df, ignore_index=True), "player_boxscores")

def daterange(start_date, end_date):
    cur = start_date
    while cur <= end_date:
        yield cur
        cur += datetime.timedelta(days=1)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["backfill","daily"], required=True)
    ap.add_argument("--start", help="YYYY-MM-DD for backfill start")
    ap.add_argument("--end", help="YYYY-MM-DD for backfill end")
    args = ap.parse_args()

    if args.mode == "daily":
        yday = (datetime.date.today() - datetime.timedelta(days=1))
        ymd = yday.strftime("%Y%m%d")
        ingest_for_dates([ymd])
        return

    if args.mode == "backfill":
        if not args.start or not args.end:
            print("need --start and --end")
            sys.exit(1)
        s = datetime.date.fromisoformat(args.start)
        e = datetime.date.fromisoformat(args.end)
        dates = [d.strftime("%Y%m%d") for d in daterange(s, e)]
        # ESPN endpoint supports date ranges too, but single-day loop is simpler
        ingest_for_dates(dates)

if __name__ == "__main__":
    main()
