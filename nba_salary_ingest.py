#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import argparse
import datetime
from typing import List, Optional, Dict, Any
import requests

import pandas as pd
from pandas.api.types import is_object_dtype

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

# NBA Fantasy API endpoints (discovered through network inspection)
# These are the internal APIs the website uses
NBA_FANTASY_BASE_URL = "https://nbafantasy.nba.com"
POSSIBLE_API_ENDPOINTS = [
    "/api/players",
    "/api/v1/players",
    "/api/squad-selection/players",
    "/api/roster/players",
    "/players/all",
]


def fetch_players_from_api() -> List[Dict]:
    """
    Try to fetch player data from NBA Fantasy internal API endpoints
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Referer': 'https://nbafantasy.nba.com/squad-selection',
        'Origin': 'https://nbafantasy.nba.com',
    }
    
    salary_data = []
    
    # Try each possible endpoint
    for endpoint in POSSIBLE_API_ENDPOINTS:
        url = f"{NBA_FANTASY_BASE_URL}{endpoint}"
        print(f"Trying endpoint: {url}")
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                print(f"✅ Success! Got response from {endpoint}")
                
                try:
                    data = response.json()
                    print(f"Response type: {type(data)}")
                    
                    # Handle different response structures
                    if isinstance(data, list):
                        print(f"Got list with {len(data)} items")
                        players = data
                    elif isinstance(data, dict):
                        print(f"Got dict with keys: {list(data.keys())[:10]}")
                        # Try common keys where player data might be nested
                        for key in ['players', 'data', 'results', 'roster', 'squad']:
                            if key in data and isinstance(data[key], list):
                                players = data[key]
                                print(f"Found {len(players)} players in '{key}' field")
                                break
                        else:
                            players = [data]  # Single player object
                    else:
                        print(f"Unexpected data type: {type(data)}")
                        continue
                    
                    # Extract player information
                    for player in players:
                        if isinstance(player, dict):
                            player_info = extract_player_info(player)
                            if player_info:
                                salary_data.append(player_info)
                    
                    if salary_data:
                        print(f"✅ Successfully extracted {len(salary_data)} players from API")
                        return salary_data
                    
                except json.JSONDecodeError:
                    print(f"❌ Response is not valid JSON")
                    print(f"Response preview: {response.text[:500]}")
            
            elif response.status_code == 404:
                print(f"❌ Endpoint not found (404)")
            else:
                print(f"❌ Got status code: {response.status_code}")
        
        except requests.exceptions.Timeout:
            print(f"❌ Request timeout")
        except Exception as e:
            print(f"❌ Error: {e}")
        
        time.sleep(0.5)  # Be respectful with requests
    
    return salary_data


def extract_player_info(player_data: Dict) -> Optional[Dict]:
    """
    Extract player information from API response
    Try various possible field names
    """
    # Possible field names for each attribute
    name_fields = ['name', 'playerName', 'player_name', 'fullName', 'full_name', 'displayName']
    team_fields = ['team', 'teamName', 'team_name', 'teamAbbr', 'team_abbr', 'teamTricode']
    position_fields = ['position', 'pos', 'positions', 'eligiblePositions']
    price_fields = ['price', 'salary', 'cost', 'value', 'salaryCap', 'salary_cap', 'fantasyPrice']
    
    name = None
    team = None
    position = None
    price = None
    
    # Extract name
    for field in name_fields:
        if field in player_data:
            name = str(player_data[field])
            break
    
    # Extract team
    for field in team_fields:
        if field in player_data:
            team = str(player_data[field])
            break
    
    # Extract position
    for field in position_fields:
        if field in player_data:
            pos_value = player_data[field]
            if isinstance(pos_value, list):
                position = normalize_position(pos_value[0] if pos_value else "")
            else:
                position = normalize_position(str(pos_value))
            break
    
    # Extract price
    for field in price_fields:
        if field in player_data:
            try:
                price_value = player_data[field]
                if isinstance(price_value, (int, float)):
                    price = float(price_value)
                else:
                    price = parse_price(str(price_value))
                break
            except:
                continue
    
    if name:  # At minimum we need a name
        return {
            "player_name": name,
            "team": team or "Unknown",
            "position": position or "Unknown",
            "price": price,
        }
    
    return None


def normalize_position(position_text: str) -> str:
    """Normalize position to Backcourt or Frontcourt"""
    pos_upper = position_text.upper().strip()
    
    # Backcourt: PG, SG, G
    if any(p in pos_upper for p in ["PG", "SG"]) or pos_upper == "G":
        return "Backcourt"
    
    # Frontcourt: SF, PF, C, F
    if any(p in pos_upper for p in ["SF", "PF", "C"]) or pos_upper == "F":
        return "Frontcourt"
    
    return position_text


def parse_price(price_text: str) -> Optional[float]:
    """Parse price text to float"""
    try:
        cleaned = price_text.replace("$", "").replace("M", "").replace(",", "").strip()
        
        if "." in cleaned or len(cleaned) <= 4:
            return float(cleaned)
        else:
            return float(cleaned) / 1_000_000
    except:
        return None


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
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    
    print(f"Loading {len(df)} salary records to BigQuery")
    BQ.load_table_from_dataframe(df, table_id, job_config=job_config).result()
    print("Load complete")


def ingest_salary_data() -> None:
    """Main function to fetch and ingest salary cap data"""
    ensure_tables()
    
    print("=" * 60)
    print("NBA Fantasy Salary Cap Data Ingestion")
    print(f"Timestamp: {datetime.datetime.now()}")
    print("=" * 60)
    
    print("\n📡 Attempting to fetch data from NBA Fantasy API...")
    salary_data = fetch_players_from_api()
    
    if not salary_data:
        print("\n❌ Could not fetch data from API endpoints")
        print("\n💡 NEXT STEPS:")
        print("1. Visit https://nbafantasy.nba.com/squad-selection in your browser")
        print("2. Open Developer Tools (F12)")
        print("3. Go to Network tab")
        print("4. Look for API calls when the page loads")
        print("5. Find the endpoint that returns player data")
        print("6. Update POSSIBLE_API_ENDPOINTS in this script")
        print("\nAlternatively, you can manually export the data from the website")
        sys.exit(1)
    
    df = pd.DataFrame(salary_data)
    
    print(f"\n✅ Successfully fetched {len(df)} player salaries")
    
    print("\n📊 Sample data:")
    print(df.head(10).to_string())
    
    print("\n📈 Position distribution:")
    print(df["position"].value_counts())
    
    print("\n💰 Price statistics:")
    print(df["price"].describe())
    
    print("\n📤 Loading to BigQuery...")
    load_df(df)
    
    print("\n" + "=" * 60)
    print("✅ Salary data ingestion complete!")
    print("=" * 60)


def test_endpoints() -> None:
    """Test function to discover API endpoints"""
    print("🔍 Testing NBA Fantasy API endpoints...\n")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
    }
    
    # Additional endpoints to try
    test_endpoints = POSSIBLE_API_ENDPOINTS + [
        "/api/gameweek/current",
        "/api/season/current",
        "/api/leaderboard",
    ]
    
    for endpoint in test_endpoints:
        url = f"{NBA_FANTASY_BASE_URL}{endpoint}"
        print(f"Testing: {url}")
        
        try:
            response = requests.get(url, headers=headers, timeout=5)
            print(f"  Status: {response.status_code}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    print(f"  ✅ Valid JSON response")
                    print(f"  Type: {type(data)}")
                    if isinstance(data, dict):
                        print(f"  Keys: {list(data.keys())[:10]}")
                    elif isinstance(data, list):
                        print(f"  Items: {len(data)}")
                        if data:
                            print(f"  First item keys: {list(data[0].keys()) if isinstance(data[0], dict) else 'N/A'}")
                except:
                    print(f"  ❌ Not JSON: {response.text[:100]}")
            
            print()
        except Exception as e:
            print(f"  ❌ Error: {e}\n")
        
        time.sleep(0.5)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch NBA salary cap data from API")
    parser.add_argument("--test", action="store_true", help="Test API endpoints")
    args = parser.parse_args()
    
    if args.test:
        test_endpoints()
    else:
        ingest_salary_data()


if __name__ == "__main__":
    main()
