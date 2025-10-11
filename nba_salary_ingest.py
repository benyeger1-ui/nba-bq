#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
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
PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET    = os.environ.get("BQ_DATASET", "nba_data")
SA_INFO    = json.loads(os.environ["GCP_SA_KEY"])
CREDS      = service_account.Credentials.from_service_account_info(SA_INFO)
BQ         = bigquery.Client(project=PROJECT_ID, credentials=CREDS)

# -----------------------------------
# BigQuery schema
# -----------------------------------
SALARY_SCHEMA = [
    bigquery.SchemaField("player_name", "STRING"),
    bigquery.SchemaField("team", "STRING"),
    bigquery.SchemaField("position", "STRING"),
    bigquery.SchemaField("price", "FLOAT64"),
    bigquery.SchemaField("scrape_date", "DATE"),
    bigquery.SchemaField("scrape_timestamp", "TIMESTAMP"),
]

# NBA Fantasy API endpoint
NBA_FANTASY_API = "https://nbafantasy.nba.com/api/bootstrap-static/"


def fetch_players_from_api() -> List[Dict]:
    """
    Fetch player data from NBA Fantasy API
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
    }
    
    print(f"Fetching data from: {NBA_FANTASY_API}")
    
    try:
        response = requests.get(NBA_FANTASY_API, headers=headers, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        print(f"✅ Successfully retrieved data from API")
        
        # Extract players from the response
        if 'elements' in data:
            players = data['elements']
            print(f"Found {len(players)} players in API response")
        else:
            print(f"❌ No 'elements' key found in response")
            print(f"Available keys: {list(data.keys())}")
            return []
        
        # Extract player information
        salary_data = []
        for player in players:
            player_info = extract_player_info(player, data)
            if player_info:
                salary_data.append(player_info)
        
        print(f"✅ Successfully extracted {len(salary_data)} players")
        return salary_data
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching data from API: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing JSON response: {e}")
        return []


def extract_player_info(player: Dict, full_data: Dict) -> Optional[Dict]:
    """
    Extract player information from API response
    
    Key fields in the API:
    - first_name, second_name, web_name: player names
    - team: team ID (need to map to team name)
    - element_type: position type (1=Backcourt, 2=Frontcourt based on the data)
    - now_cost: price (appears to be in units, need to divide by 10 to get millions)
    """
    try:
        # Extract basic info
        first_name = player.get('first_name', '')
        second_name = player.get('second_name', '')
        
        # Use full name (first + last)
        player_name = f"{first_name} {second_name}".strip()
        
        if not player_name:
            return None
        
        # Extract team (map team ID to team name)
        team_id = player.get('team')
        team_name = get_team_name(team_id, full_data)
        
        # Extract position (element_type: 1=Backcourt, 2=Frontcourt)
        element_type = player.get('element_type')
        position = "Backcourt" if element_type == 1 else "Frontcourt"
        
        # Extract price (now_cost is in units of 0.1M, so divide by 10)
        now_cost = player.get('now_cost')
        price = float(now_cost) / 10.0 if now_cost is not None else None
        
        return {
            "player_name": player_name,
            "team": team_name,
            "position": position,
            "price": price,
        }
    
    except Exception as e:
        print(f"Error extracting player info: {e}")
        return None


def get_team_name(team_id: int, full_data: Dict) -> str:
    """
    Map team ID to team abbreviation/name
    """
    if not team_id:
        return "Unknown"
    
    # Look for teams data in the response
    if 'teams' in full_data:
        teams = full_data['teams']
        for team in teams:
            if team.get('id') == team_id:
                # Try to get short_name or name
                return team.get('short_name') or team.get('name') or f"Team_{team_id}"
    
    return f"Team_{team_id}"


def ensure_dataset() -> None:
    """Ensure dataset exists"""
    ds_id = f"{PROJECT_ID}.{DATASET}"
    try:
        BQ.get_dataset(ds_id)
    except Exception:
        BQ.create_dataset(bigquery.Dataset(ds_id))


def ensure_tables() -> None:
    """Ensure salary table exists"""
    ensure_dataset()
    table_id = f"{PROJECT_ID}.{DATASET}.player_salaries"
    try:
        BQ.get_table(table_id)
        print(f"Table {table_id} already exists")
    except Exception:
        print(f"Creating table {table_id}")
        BQ.create_table(bigquery.Table(table_id, schema=SALARY_SCHEMA))


def load_df(df: pd.DataFrame) -> None:
    """Load dataframe to BigQuery"""
    if df is None or df.empty:
        print("No data to load")
        return
    
    table_id = f"{PROJECT_ID}.{DATASET}.player_salaries"
    
    # Add timestamp columns
    df["scrape_date"] = pd.Timestamp.now().date()
    df["scrape_timestamp"] = pd.Timestamp.now()
    
    # Coerce dtypes
    df["price"] = pd.to_numeric(df["price"], errors="coerce").astype("Float64")
    
    for col in ["player_name", "team", "position"]:
        if col in df.columns:
            if is_object_dtype(df[col]):
                df[col] = df[col].astype("string")
    
    df["scrape_date"] = pd.to_datetime(df["scrape_date"]).dt.date
    
    job_config = bigquery.LoadJobConfig(
        schema=SALARY_SCHEMA,
        write_disposition="WRITE_TRUNCATE",
        # schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    
    print(f"Loading {len(df)} salary records to BigQuery")
    BQ.load_table_from_dataframe(df, table_id, job_config=job_config).result()
    print("Load complete")


def ingest_salary_data() -> None:
    """Main function to fetch and ingest salary cap data"""
    ensure_tables()
    
    print("=" * 70)
    print("NBA Fantasy Salary Cap Data Ingestion")
    print(f"Timestamp: {datetime.datetime.now()}")
    print("=" * 70)
    
    salary_data = fetch_players_from_api()
    
    if not salary_data:
        print("\n❌ No data retrieved from API")
        sys.exit(1)
    
    df = pd.DataFrame(salary_data)
    
    print(f"\n✅ Successfully fetched {len(df)} player salaries")
    
    # Display sample data
    print("\n📊 Sample data (first 10 players):")
    print(df.head(10).to_string(index=False))
    
    # Display statistics
    print("\n📈 Position distribution:")
    print(df["position"].value_counts())
    
    print("\n💰 Price statistics:")
    print(df["price"].describe())
    
    print("\n🏀 Team distribution (top 10):")
    print(df["team"].value_counts().head(10))
    
    # Load to BigQuery
    print("\n📤 Loading to BigQuery...")
    load_df(df)
    
    print("\n" + "=" * 70)
    print("✅ Salary data ingestion complete!")
    print(f"   Total players: {len(df)}")
    print(f"   Backcourt: {len(df[df['position'] == 'Backcourt'])}")
    print(f"   Frontcourt: {len(df[df['position'] == 'Frontcourt'])}")
    print("=" * 70)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch NBA Fantasy salary cap data"
    )
    args = parser.parse_args()
    
    ingest_salary_data()


if __name__ == "__main__":
    main()
