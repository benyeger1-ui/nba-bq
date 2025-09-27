#!/usr/bin/env python3

import requests
import json

def test_nba_apis():
    """Test different NBA APIs to see which ones work"""
    
    print("=== NBA API Debug Tool ===")
    print("Testing APIs for games on 2024-10-31\n")
    
    # Test 1: Ball Don't Lie API (most reliable)
    print("1. Testing Ball Don't Lie API...")
    try:
        url = "https://www.balldontlie.io/api/v1/games"
        params = {"dates[]": "2024-10-31"}
        response = requests.get(url, params=params, timeout=10)
        
        print(f"   Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            games = data.get("data", [])
            print(f"   Found {len(games)} games")
            if games:
                print(f"   Sample game: {games[0].get('home_team', {}).get('abbreviation')} vs {games[0].get('visitor_team', {}).get('abbreviation')}")
        else:
            print(f"   Error: {response.text[:200]}")
    except Exception as e:
        print(f"   Failed: {e}")
    
    print()
    
    # Test 2: ESPN API
    print("2. Testing ESPN API...")
    try:
        url = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
        params = {"dates": "20241031"}
        response = requests.get(url, params=params, timeout=10)
        
        print(f"   Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            events = data.get("events", [])
            print(f"   Found {len(events)} games")
            if events:
                competitors = events[0].get("competitions", [{}])[0].get("competitors", [])
                if len(competitors) >= 2:
                    team1 = competitors[0].get("team", {}).get("abbreviation", "?")
                    team2 = competitors[1].get("team", {}).get("abbreviation", "?")
                    print(f"   Sample game: {team1} vs {team2}")
        else:
            print(f"   Error: {response.text[:200]}")
    except Exception as e:
        print(f"   Failed: {e}")
    
    print()
    
    # Test 3: NBA Stats (official but sometimes blocked)
    print("3. Testing NBA Stats API...")
    try:
        url = "https://stats.nba.com/stats/scoreboardV2"
        params = {"GameDate": "10/31/2024", "LeagueID": "00", "DayOffset": "0"}
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://www.nba.com/",
            "x-nba-stats-origin": "stats",
            "x-nba-stats-token": "true"
        }
        response = requests.get(url, params=params, headers=headers, timeout=10)
        
        print(f"   Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            game_header = None
            for dataset in data.get("resultSets", []):
                if dataset.get("name") == "GameHeader":
                    game_header = dataset
                    break
            
            if game_header:
                games = game_header.get("rowSet", [])
                print(f"   Found {len(games)} games")
                if games:
                    print(f"   Sample game data: {games[0][:3]}...")  # First few fields
            else:
                print("   No GameHeader found in response")
        else:
            print(f"   Error: {response.text[:200]}")
    except Exception as e:
        print(f"   Failed: {e}")
    
    print()
    print("=== Summary ===")
    print("Run this script and look for:")
    print("- Status: 200 (success)")
    print("- Found X games (where X > 0)")
    print("- The API that works best will be your solution!")

if __name__ == "__main__":
    test_nba_apis()
