#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import argparse
import datetime
import re
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
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

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
    """Setup Chrome driver for headless scraping with proper version management"""
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    
    # Use webdriver-manager to handle version compatibility
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    # Execute script to remove webdriver property
    driver.execute_cdp_cmd('Network.setUserAgentOverride', {
        "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    })
    
    return driver


def extract_json_from_script_tags(page_source: str) -> List[Dict]:
    """Extract player data from JavaScript/JSON in the page source"""
    salary_data = []
    
    try:
        # Look for common patterns where React apps store initial state
        patterns = [
            r'window\.__INITIAL_STATE__\s*=\s*({.*?});',
            r'window\.__PRELOADED_STATE__\s*=\s*({.*?});',
            r'var\s+players\s*=\s*(\[.*?\]);',
            r'window\.players\s*=\s*(\[.*?\]);',
            r'"players"\s*:\s*(\[.*?\])',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, page_source, re.DOTALL)
            if matches:
                print(f"Found potential JSON data with pattern: {pattern[:50]}")
                for match in matches:
                    try:
                        data = json.loads(match)
                        print(f"Successfully parsed JSON data, type: {type(data)}")
                        
                        # Extract player info based on data structure
                        if isinstance(data, list):
                            for item in data:
                                if isinstance(item, dict):
                                    player = extract_player_from_dict(item)
                                    if player:
                                        salary_data.append(player)
                        elif isinstance(data, dict):
                            # Navigate through nested structure
                            players = find_players_in_dict(data)
                            salary_data.extend(players)
                    except json.JSONDecodeError:
                        continue
    
    except Exception as e:
        print(f"Error extracting JSON: {e}")
    
    return salary_data


def find_players_in_dict(data: Dict, depth: int = 0) -> List[Dict]:
    """Recursively search for player data in nested dictionaries"""
    players = []
    
    if depth > 10:  # Prevent infinite recursion
        return players
    
    for key, value in data.items():
        if key.lower() in ['players', 'playerlist', 'roster', 'squad']:
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        player = extract_player_from_dict(item)
                        if player:
                            players.append(player)
        elif isinstance(value, dict):
            players.extend(find_players_in_dict(value, depth + 1))
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    player = extract_player_from_dict(item)
                    if player:
                        players.append(player)
    
    return players


def extract_player_from_dict(data: Dict) -> Optional[Dict]:
    """Extract player info from a dictionary"""
    # Look for common field names
    name_fields = ['name', 'playerName', 'player_name', 'fullName', 'full_name']
    team_fields = ['team', 'teamName', 'team_name', 'teamAbbr', 'team_abbr']
    position_fields = ['position', 'pos', 'positions']
    price_fields = ['price', 'salary', 'cost', 'value', 'salaryCap', 'salary_cap']
    
    name = None
    team = None
    position = None
    price = None
    
    # Extract name
    for field in name_fields:
        if field in data:
            name = data[field]
            break
    
    # Extract team
    for field in team_fields:
        if field in data:
            team = data[field]
            break
    
    # Extract position
    for field in position_fields:
        if field in data:
            pos_value = data[field]
            if isinstance(pos_value, list):
                position = normalize_position(pos_value[0] if pos_value else "")
            else:
                position = normalize_position(str(pos_value))
            break
    
    # Extract price
    for field in price_fields:
        if field in data:
            price = parse_price(str(data[field]))
            break
    
    if name:  # At minimum we need a name
        return {
            "player_name": name,
            "team": team or "Unknown",
            "position": position or "Unknown",
            "price": price,
        }
    
    return None


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
        time.sleep(8)  # Give extra time for JavaScript to execute
        
        # First, try to find JSON data in the page source
        print("Attempting to extract data from page source...")
        page_source = driver.page_source
        
        salary_data = extract_json_from_script_tags(page_source)
        
        if salary_data:
            print(f"Successfully extracted {len(salary_data)} players from JSON data")
            return salary_data
        
        # If JSON extraction failed, try Selenium selectors
        print("JSON extraction failed, trying Selenium selectors...")
        
        # Wait for any React content to render
        try:
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "main")))
        except:
            pass
        
        time.sleep(3)
        
        # Try different possible selectors
        selectors_to_try = [
            "div[data-testid*='player']",
            "div[class*='player']",
            "div[class*='Player']",
            ".player-item",
            ".player-card",
            ".player-row",
            "[data-player]",
            ".roster-player",
            ".squad-player",
            "tr[class*='player']",
            "li[class*='player']",
        ]
        
        player_elements = []
        for selector in selectors_to_try:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements and len(elements) > 10:  # Should have many players
                    print(f"Found {len(elements)} elements with selector: {selector}")
                    player_elements = elements
                    break
            except:
                continue
        
        if not player_elements:
            print("Could not find player elements with known selectors")
            print("\n=== Saving page source for debugging ===")
            with open("page_source_debug.html", "w", encoding="utf-8") as f:
                f.write(page_source)
            print("Saved to page_source_debug.html")
            
            # Print a sample of the page structure
            print("\n=== Looking for div elements with classes ===")
            all_divs = driver.find_elements(By.TAG_NAME, "div")
            class_counts = {}
            for div in all_divs[:100]:  # Sample first 100
                classes = div.get_attribute("class")
                if classes:
                    for cls in classes.split():
                        if 'player' in cls.lower() or 'squad' in cls.lower() or 'roster' in cls.lower():
                            class_counts[cls] = class_counts.get(cls, 0) + 1
            
            if class_counts:
                print("Classes containing 'player', 'squad', or 'roster':")
                for cls, count in sorted(class_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
                    print(f"  .{cls}: {count} occurrences")
            
            return salary_data
        
        print(f"Processing {len(player_elements)} players...")
        
        for idx, player_elem in enumerate(player_elements):
            try:
                # Get all text in the element
                full_text = player_elem.text
                
                if not full_text:
                    continue
                
                # Try to parse text for player info
                lines = [line.strip() for line in full_text.split('\n') if line.strip()]
                
                if len(lines) >= 3:
                    # Heuristic parsing
                    name = lines[0]
                    team = None
                    position = None
                    price = None
                    
                    for line in lines[1:]:
                        if any(pos in line.upper() for pos in ['PG', 'SG', 'SF', 'PF', 'C', 'G', 'F']):
                            position = normalize_position(line)
                        elif '$' in line or 'M' in line or any(c.isdigit() for c in line):
                            price = parse_price(line)
                        elif len(line) <= 3 and line.isupper():
                            team = line
                    
                    if name:
                        salary_data.append({
                            "player_name": name,
                            "team": team or "Unknown",
                            "position": position or "Unknown",
                            "price": price,
                        })
                
                if (idx + 1) % 50 == 0:
                    print(f"Processed {idx + 1} players...")
            
            except Exception as e:
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
    if any(p in pos_upper for p in ["PG", "SG"]) or pos_upper == "G":
        return "Backcourt"
    
    # Frontcourt: SF, PF, C, F
    if any(p in pos_upper for p in ["SF", "PF", "C"]) or pos_upper == "F":
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
        print("\nℹ️  The NBA Fantasy website may require manual interaction or use a different data structure.")
        print("Check the debug output above for more information.")
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
        
        page_source = driver.page_source
        
        # Save full page source
        with open("page_source_full.html", "w", encoding="utf-8") as f:
            f.write(page_source)
        print("✅ Saved full page source to page_source_full.html")
        
        # Print page source preview
        print("\n=== PAGE SOURCE PREVIEW (first 3000 chars) ===")
        print(page_source[:3000])
        
        # Look for JSON data
        print("\n=== CHECKING FOR JSON DATA ===")
        json_patterns = [
            'window.__INITIAL_STATE__',
            'window.__PRELOADED_STATE__',
            'var players',
            'window.players',
            '"players":',
        ]
        
        for pattern in json_patterns:
            if pattern in page_source:
                print(f"✅ Found pattern: {pattern}")
                start_idx = page_source.find(pattern)
                print(f"   Context: {page_source[start_idx:start_idx+200]}")
        
        # Try to find common element patterns
        print("\n=== LOOKING FOR COMMON PATTERNS ===")
        patterns = [
            "player",
            "salary",
            "price",
            "squad",
            "roster",
        ]
        
        for pattern in patterns:
            elements = driver.find_elements(By.XPATH, f"//*[contains(@class, '{pattern}')]")
            if elements:
                print(f"Found {len(elements)} elements with class containing '{pattern}'")
        
        # Check for data attributes
        print("\n=== CHECKING DATA ATTRIBUTES ===")
        test_attrs = ['data-testid', 'data-player', 'data-id']
        for attr in test_attrs:
            elements = driver.find_elements(By.XPATH, f"//*[@{attr}]")
            if elements:
                print(f"Found {len(elements)} elements with {attr} attribute")
                if elements:
                    print(f"  Example: {elements[0].get_attribute('outerHTML')[:200]}")
        
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
