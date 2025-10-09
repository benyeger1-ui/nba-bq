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

from google.cloud import bigquery
from google.oauth2 import service_account

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException

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


def setup_driver() -> webdriver.Chrome:
    """Setup Chrome driver for headless scraping"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    
    driver = webdriver.Chrome(options=chrome_options)
    return driver


def scrape_nba_salary_data() -> List[Dict]:
    """
    Scrape NBA salary cap game data from NBA.com
    """
    driver = setup_driver()
    salary_data = []
    
    try:
        print("Loading NBA Fantasy Salary Cap page...")
        driver.get("https://nbafantasy.nba.com/squad-selection")
        
        # Wait for page to load
        wait = WebDriverWait(driver, 30)
        
        print("Waiting for page content to load...")
        time.sleep(5)  # Give time for JavaScript to execute
        
        # Try different possible selectors
        selectors_to_try = [
            ".player-item",
            ".player-card",
            ".player-row",
            "[data-player]",
            ".roster-player",
            ".squad-player",
        ]
        
        player_elements = []
        for selector in selectors_to_try:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    print(f"Found {len(elements)} elements with selector: {selector}")
                    player_elements = elements
                    break
            except:
                continue
        
        if not player_elements:
            print("Could not find player elements with known selectors")
            print("Attempting to parse page source...")
            
            # Fallback: try to parse from page source
            page_source = driver.page_source
            
            # Look for JSON data in script tags
            if "var players" in page_source or "window.__INITIAL_STATE__" in page_source:
                print("Found potential JSON data in page source")
                # You would need to extract and parse the JSON here
                # This is a placeholder for that logic
            
            print("\nPage source preview (first 2000 chars):")
            print(page_source[:2000])
            
            return salary_data
        
        print(f"Processing {len(player_elements)} players...")
        
        for idx, player_elem in enumerate(player_elements):
            try:
                # Try multiple possible attribute/text patterns
                name = None
                team = None
                position = None
                price = None
                
                # Try to get player name
                name_selectors = [".player-name", ".name", "[data-name]"]
                for sel in name_selectors:
                    try:
                        name = player_elem.find_element(By.CSS_SELECTOR, sel).text
                        if name:
                            break
                    except:
                        continue
                
                # Try to get team
                team_selectors = [".player-team", ".team", "[data-team]"]
                for sel in team_selectors:
                    try:
                        team = player_elem.find_element(By.CSS_SELECTOR, sel).text
                        if team:
                            break
                    except:
                        continue
                
                # Try to get position
                position_selectors = [".player-position", ".position", "[data-position]"]
                for sel in position_selectors:
                    try:
                        pos_text = player_elem.find_element(By.CSS_SELECTOR, sel).text
                        if pos_text:
                            # Normalize to Backcourt/Frontcourt
                            position = normalize_position(pos_text)
                            break
                    except:
                        continue
                
                # Try to get price/salary
                price_selectors = [".player-salary", ".player-price", ".salary", ".price", "[data-salary]"]
                for sel in price_selectors:
                    try:
                        price_text = player_elem.find_element(By.CSS_SELECTOR, sel).text
                        if price_text:
                            price = parse_price(price_text)
                            break
                    except:
                        continue
                
                if name:  # Only add if we at least have a name
                    salary_data.append({
                        "player_name": name,
                        "team": team or "Unknown",
                        "position": position or "Unknown",
                        "price": price,
                    })
                    
                    if (idx + 1) % 50 == 0:
                        print(f"Processed {idx + 1} players...")
                
            except Exception as e:
                print(f"Error extracting player {idx}: {e}")
                continue
        
        print(f"Successfully scraped {len(salary_data)} players")
        
    except Exception as e:
        print(f"Error during scraping: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        driver.quit()
    
    return salary_data


def normalize_position(position_text: str) -> str:
    """Normalize position to Backcourt or Frontcourt"""
    pos_upper = position_text.upper().strip()
    
    # Backcourt: PG, SG, G
    if any(p in pos_upper for p in ["PG", "SG", "GUARD", "G"]):
        return "Backcourt"
    
    # Frontcourt: SF, PF, C, F
    if any(p in pos_upper for p in ["SF", "PF", "C", "CENTER", "FORWARD", "F"]):
        return "Frontcourt"
    
    return position_text


def parse_price(price_text: str) -> Optional[float]:
    """Parse price text to float (e.g., '$7.5M' -> 7.5 or '7500000' -> 7.5)"""
    try:
        # Remove common characters
        cleaned = price_text.replace("$", "").replace("M", "").replace(",", "").strip()
        
        # Handle different formats
        if "." in cleaned or len(cleaned) <= 4:
            # Already in millions (e.g., "7.5" or "12.3")
            return float(cleaned)
        else:
            # Full dollar amount (e.g., "7500000")
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
    """Main function to scrape and ingest salary cap data"""
    ensure_tables()
    
    print("Starting NBA salary cap data scraping...")
    print(f"Timestamp: {datetime.datetime.now()}")
    
    salary_data = scrape_nba_salary_data()
    
    if not salary_data:
        print("No salary data scraped")
        sys.exit(1)
    
    df = pd.DataFrame(salary_data)
    print(f"\nScraped {len(df)} player salaries")
    print("\nSample data:")
    print(df.head(10))
    
    print("\nPosition distribution:")
    print(df["position"].value_counts())
    
    print("\nPrice statistics:")
    print(df["price"].describe())
    
    load_df(df)
    print("\n✅ Salary data ingestion complete")


def test_scrape() -> None:
    """Test function to inspect page structure"""
    driver = setup_driver()
    
    try:
        print("Loading page for inspection...")
        driver.get("https://nbafantasy.nba.com/squad-selection")
        
        print("Waiting for page to load...")
        time.sleep(10)
        
        # Print page source
        print("\n=== PAGE SOURCE PREVIEW (first 3000 chars) ===")
        print(driver.page_source[:3000])
        
        print("\n=== PAGE SOURCE END (last 1000 chars) ===")
        print(driver.page_source[-1000:])
        
        # Try to find common element patterns
        print("\n=== LOOKING FOR COMMON PATTERNS ===")
        patterns = [
            "player",
            "salary",
            "price",
            "cost",
            "value",
            "squad",
            "roster",
            "name",
            "team",
            "position",
        ]
        
        for pattern in patterns:
            # Check classes
            elements = driver.find_elements(By.XPATH, f"//*[contains(@class, '{pattern}')]")
            if elements:
                print(f"Found {len(elements)} elements with class containing '{pattern}'")
                if len(elements) > 0:
                    print(f"  Example: {elements[0].get_attribute('outerHTML')[:200]}")
            
            # Check IDs
            elements = driver.find_elements(By.XPATH, f"//*[contains(@id, '{pattern}')]")
            if elements:
                print(f"Found {len(elements)} elements with ID containing '{pattern}'")
        
        # Look for data attributes
        print("\n=== CHECKING FOR DATA ATTRIBUTES ===")
        data_attrs = driver.find_elements(By.XPATH, "//*[@data-*]")
        if data_attrs:
            print(f"Found {len(data_attrs)} elements with data attributes")
            for elem in data_attrs[:5]:
                print(f"  {elem.get_attribute('outerHTML')[:200]}")
        
    finally:
        driver.quit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape NBA salary cap data")
    parser.add_argument("--test", action="store_true", help="Run test scrape to inspect page")
    args = parser.parse_args()
    
    if args.test:
        test_scrape()
    else:
        ingest_salary_data()


if __name__ == "__main__":
    main()
