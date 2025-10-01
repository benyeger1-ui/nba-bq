import os
from yahoo_oauth import OAuth2
from yahoo_fantasy_api import game

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'google-key.json'

print("🔄 Connecting to Yahoo...")
sc = OAuth2(None, None, from_file='oauth2.json')
print("✅ Connected!\n")

print("🔄 Getting NBA game info...")
gm = game.Game(sc, 'nba')

print("\n📋 Your NBA Fantasy Leagues:")
print("=" * 50)

try:
    league_ids = gm.league_ids()
    
    if league_ids:
        for i, league_id in enumerate(league_ids, 1):
            print(f"\n{i}. League ID: {league_id}")
            
            # Parse the season from the game code
            game_code = league_id.split('.')[0]
            print(f"   Season Code: {game_code}")
        
        print("\n" + "=" * 50)
        print("✅ Use one of these EXACT League IDs in your fetch_data.py file!")
        print(f"   Example: league_id = '{league_ids[0]}'")
    else:
        print("❌ No leagues found. Make sure you're in an active NBA fantasy league.")
        
except Exception as e:
    print(f"❌ Error: {e}")
