import os, json, datetime, requests, pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# Env vars from GitHub Secrets
PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET = os.environ.get("BQ_DATASET", "nba_data")
API_KEY = os.environ["BALDONTLIE_API_KEY"]
SA_KEY = json.loads(os.environ["GCP_SA_KEY"])

# Authenticate to BigQuery using the JSON in-memory
creds = service_account.Credentials.from_service_account_info(SA_KEY)
bq = bigquery.Client(project=PROJECT_ID, credentials=creds)

TABLE = "games_daily_old"  # final table name

def ensure_dataset():
    ds_id = f"{PROJECT_ID}.{DATASET}"
    try:
        bq.get_dataset(ds_id)
    except Exception:
        bq.create_dataset(ds_id)

def ensure_table():
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    try:
        bq.get_table(table_id)
    except Exception:
        schema = [
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("season", "INTEGER"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("period", "INTEGER"),
            bigquery.SchemaField("time", "STRING"),
            bigquery.SchemaField("postseason", "BOOL"),
            bigquery.SchemaField("home_team_id", "INTEGER"),
            bigquery.SchemaField("home_team", "STRING"),
            bigquery.SchemaField("home_team_score", "INTEGER"),
            bigquery.SchemaField("visitor_team_id", "INTEGER"),
            bigquery.SchemaField("visitor_team", "STRING"),
            bigquery.SchemaField("visitor_team_score", "INTEGER"),
        ]
        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(field="date")
        bq.create_table(table)

def already_loaded(target_date: datetime.date) -> bool:
    # Avoid duplicates without using DML - skip if partition already has rows
    table = f"{PROJECT_ID}.{DATASET}.{TABLE}"
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

def fetch_games_for_date(yyyy_mm_dd: str):
    # BALLDONTLIE pagination is cursor-based. We’ll loop until there’s no next cursor.
    # Endpoint: GET https://api.balldontlie.io/v1/games?dates[]=YYYY-MM-DD&per_page=100
    # Auth header: Authorization: YOUR_API_KEY
    # See docs for details. 
    base = "https://api.balldontlie.io/v1/games"
    headers = {"Authorization": API_KEY}
    params = {"per_page": 100, "dates[]": yyyy_mm_dd}
    data = []
    cursor = None
    while True:
        p = dict(params)
        if cursor is not None:
            p["cursor"] = cursor
        r = requests.get(base, headers=headers, params=p, timeout=30)
        r.raise_for_status()
        j = r.json()
        data.extend(j.get("data", []))
        meta = j.get("meta", {}) or {}
        cursor = meta.get("next_cursor")
        if not cursor:
            break
    return data

def to_dataframe(rows, iso_date):
    def row(g):
        ht = g.get("home_team") or {}
        vt = g.get("visitor_team") or {}
        return {
            "id": g.get("id"),
            "date": iso_date,  # official date field for partitioning
            "season": g.get("season"),
            "status": g.get("status"),
            "period": g.get("period"),
            "time": g.get("time"),
            "postseason": g.get("postseason"),
            "home_team_id": ht.get("id"),
            "home_team": ht.get("abbreviation") or ht.get("full_name"),
            "home_team_score": g.get("home_team_score"),
            "visitor_team_id": vt.get("id"),
            "visitor_team": vt.get("abbreviation") or vt.get("full_name"),
            "visitor_team_score": g.get("visitor_team_score"),
        }
    cols = ["id","date","season","status","period","time","postseason",
            "home_team_id","home_team","home_team_score",
            "visitor_team_id","visitor_team","visitor_team_score"]
    return pd.DataFrame([row(g) for g in rows], columns=cols)

def load_dataframe(df: pd.DataFrame):
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    job = bq.load_table_from_dataframe(df, table_id)  # load job - OK in Sandbox
    job.result()

def main():
    # Use "yesterday" in UTC - schedule the workflow a bit later in the day to be safe
    yday = (datetime.date.today() - datetime.timedelta(days=1))
    iso = yday.isoformat()

    ensure_dataset()
    ensure_table()

    if already_loaded(yday):
        print(f"Already loaded data for {iso}. Nothing to do.")
        return

    rows = fetch_games_for_date(iso)
    df = to_dataframe(rows, iso)
    if df.empty:
        print(f"No NBA games found for {iso}")
        return
    load_dataframe(df)
    print(f"Loaded {len(df)} games for {iso} into {PROJECT_ID}.{DATASET}.{TABLE}")

if __name__ == "__main__":
    main()
