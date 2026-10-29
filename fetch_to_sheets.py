"""
Fetch Yahoo Fantasy NBA league data and write it to the Google Sheet behind the League Hub web app.

Runs in GitHub Actions (Yahoo blocks requests coming from Google Apps Script servers).
Env:
  YAHOO_CLIENT_ID, YAHOO_CLIENT_SECRET  - Yahoo app credentials
  YAHOO_OAUTH_TOKEN                     - refresh token (get_yahoo_token.py prints it), or the full oauth2.json
  YAHOO_REDIRECT_URI                    - the Yahoo app's redirect URI, e.g. https://localhost:8000/callback
  GCP_SA_KEY                            - service account JSON (the sheet must be shared with its client_email)
  SHEET_ID                              - id from the sheet URL: docs.google.com/spreadsheets/d/<SHEET_ID>/edit
  LEAGUE_KEY (optional)                 - e.g. 470.l.3183; if empty, your newest NBA league is used
"""
import json
import os
import time

API = 'https://fantasysports.yahooapis.com/fantasy/v2/'
POOL_SIZE = 500


# ==================== YAHOO ====================

class YahooSession:
    """Minimal OAuth2 client: trades the long-lived refresh token for 1-hour access tokens."""

    def __init__(self, client_id, client_secret, refresh_token, redirect_uri):
        import requests

        self.client_id, self.client_secret = client_id, client_secret
        self.refresh_token, self.redirect_uri = refresh_token, redirect_uri
        self.session = requests.Session()
        self.expires_at = 0

    def token_is_valid(self):
        return time.time() < self.expires_at

    def refresh_access_token(self):
        r = self.session.post(
            'https://api.login.yahoo.com/oauth2/get_token',
            auth=(self.client_id, self.client_secret),
            data={'grant_type': 'refresh_token', 'refresh_token': self.refresh_token, 'redirect_uri': self.redirect_uri})
        if r.status_code != 200:
            raise RuntimeError(f'Yahoo token error {r.status_code}: {r.text[:300]}')
        tok = r.json()
        self.session.headers['Authorization'] = 'Bearer ' + tok['access_token']
        self.expires_at = time.time() + int(tok.get('expires_in', 3600)) - 300
        self.refresh_token = tok.get('refresh_token') or self.refresh_token


def connect_yahoo():
    raw = os.environ.get('YAHOO_OAUTH_TOKEN', '').strip()
    try:
        creds = json.loads(raw)  # full oauth2.json from yahoo_oauth also works
    except ValueError:
        creds = {'refresh_token': raw}
    sc = YahooSession(
        os.environ.get('YAHOO_CLIENT_ID') or creds.get('consumer_key'),
        os.environ.get('YAHOO_CLIENT_SECRET') or creds.get('consumer_secret'),
        creds['refresh_token'],
        os.environ.get('YAHOO_REDIRECT_URI') or 'oob')
    sc.refresh_access_token()
    return sc


def get(sc, path, tolerant=False):
    """GET an API path and return its fantasy_content. Retries on expired token / rate limit / server errors."""
    r = None
    for attempt in range(4):
        if not sc.token_is_valid():
            sc.refresh_access_token()
        r = sc.session.get(API + path, params={'format': 'json'})
        if r.status_code == 200:
            return r.json()['fantasy_content']
        if r.status_code == 401:
            sc.refresh_access_token()
        elif r.status_code not in (429, 999) and r.status_code < 500:
            break
        time.sleep(2 * (attempt + 1))
    message = f'Yahoo HTTP {r.status_code} for {path}: {r.text[:300]}'
    if tolerant:
        print('  skipped -', message)
        return None
    raise RuntimeError(message)


def merge(value):
    """Yahoo encodes one object as an array of single-key objects (often nested). Flatten into one dict."""
    out = {}

    def walk(v):
        if isinstance(v, list):
            for x in v:
                walk(x)
        elif isinstance(v, dict):
            out.update(v)

    walk(value)
    return out


def items(collection, name):
    """Yahoo collections look like {"0": {"team": ...}, "1": {"team": ...}, "count": 2}."""
    if not isinstance(collection, dict):
        return []
    return [collection[k][name] for k in collection
            if k != 'count' and isinstance(collection[k], dict) and name in collection[k]]


def num(v):
    if v is None or v == '' or v == '-':
        return ''
    try:
        f = float(v)
    except (TypeError, ValueError):
        return str(v)
    return int(f) if f.is_integer() and '.' not in str(v) else f


def add_stats(row, team_stats, cats):
    """One column per stat category, named by its display name (e.g. "FG%")."""
    values = {}
    for s in (team_stats or {}).get('stats', []):
        if isinstance(s, dict) and 'stat' in s:
            values[str(s['stat'].get('stat_id'))] = s['stat'].get('value')
    for c in cats:
        row[c['display_name']] = num(values.get(c['stat_id']))


def find_league_key(sc):
    try:
        fc = get(sc, 'users;use_login=1/games;game_codes=nba/leagues')
    except RuntimeError:
        fc = get(sc, 'users;use_login=1/games;game_keys=nba/leagues')
    leagues = []
    for u in items(fc.get('users'), 'user'):
        for g in map(merge, items(merge(u).get('games'), 'game')):
            for lg in map(merge, items(g.get('leagues'), 'league')):
                leagues.append((int(lg.get('season') or g.get('season') or 0), lg['league_key'], lg.get('name', '')))
    leagues.sort(reverse=True)
    print('Your NBA leagues:')
    for season, key, name in leagues:
        print(f'  {season}  {key}  {name}')
    if not leagues:
        raise RuntimeError('No NBA leagues found for this Yahoo account')
    return leagues[0][1]


# ==================== FETCH ====================

def fetch_all(sc, key):
    started = time.time()

    # ---- Settings & stat categories
    league = merge(get(sc, f'league/{key}/settings')['league'])
    settings = merge(league.get('settings'))
    cats = []
    for s in (settings.get('stat_categories') or {}).get('stats', []):
        s = s.get('stat') if isinstance(s, dict) else None
        if not s:
            continue
        cats.append({
            'stat_id': str(s.get('stat_id')),
            'display_name': s.get('display_name') or s.get('abbr') or s.get('name'),
            'name': s.get('name', ''),
            'sort_order': int(s.get('sort_order') or 0),  # 1 = higher is better, 0 = lower is better (TO)
            'is_only_display_stat': int(s.get('is_only_display_stat') or 0),
        })
    start_week = int(league.get('start_week') or 1)
    end_week = int(league.get('end_week') or start_week)
    current_week = min(max(int(league.get('current_week') or start_week), start_week), end_week)
    print(f"League: {league.get('name')} ({key}), weeks {start_week}-{end_week}, current {current_week}")

    # ---- Standings
    standings_league = merge(get(sc, f'league/{key}/standings')['league'])
    teams = [merge(t) for t in items(merge(standings_league.get('standings')).get('teams'), 'team')]
    standings = []
    for t in teams:
        st = t.get('team_standings') or {}
        ot = st.get('outcome_totals') or {}
        managers = t.get('managers') or [{}]
        logos = t.get('team_logos') or [{}]
        row = {
            'team_key': t.get('team_key'),
            'team_name': t.get('name'),
            'manager': (managers[0].get('manager') or {}).get('nickname', ''),
            'logo': (logos[0].get('team_logo') or {}).get('url', ''),
            'rank': num(st.get('rank')),
            'playoff_seed': num(st.get('playoff_seed')),
            'wins': num(ot.get('wins')),
            'losses': num(ot.get('losses')),
            'ties': num(ot.get('ties')),
            'percentage': num(ot.get('percentage')),
            'games_back': 0 if st.get('games_back') == '-' else num(st.get('games_back')),
            'moves': num(t.get('number_of_moves')),
            'trades': num(t.get('number_of_trades')),
            'waiver_priority': num(t.get('waiver_priority')),
            'faab_balance': num(t.get('faab_balance')),
        }
        add_stats(row, t.get('team_stats'), cats)
        standings.append(row)
    standings.sort(key=lambda r: r['rank'] if r['rank'] != '' else 99)
    print(f'Standings: {len(standings)} teams')

    # ---- Matchups (one row per team per week)
    matchups = []
    for week in range(start_week, current_week + 1):
        scoreboard = merge(get(sc, f'league/{key}/scoreboard;week={week}')['league']).get('scoreboard') or {}
        for m in items((scoreboard.get('0') or {}).get('matchups'), 'matchup'):
            pair = [merge(t) for t in items((m.get('0') or {}).get('teams'), 'team')]
            for i, t in enumerate(pair):
                opp = pair[1 - i] if len(pair) == 2 else {}
                cat_w = cat_l = cat_t = 0
                for sw in m.get('stat_winners') or []:
                    w = sw.get('stat_winner') or {}
                    if int(w.get('is_tied') or 0):
                        cat_t += 1
                    elif w.get('winner_team_key') == t.get('team_key'):
                        cat_w += 1
                    elif w.get('winner_team_key'):
                        cat_l += 1
                winner = m.get('winner_team_key')
                result = 'T' if int(m.get('is_tied') or 0) else ('W' if winner == t.get('team_key') else 'L') if winner else ''
                games = (t.get('team_remaining_games') or {}).get('total') or {}
                row = {
                    'week': int(m.get('week') or week),
                    'week_start': m.get('week_start', ''),
                    'week_end': m.get('week_end', ''),
                    'status': m.get('status', ''),
                    'is_playoffs': int(m.get('is_playoffs') or 0),
                    'is_consolation': int(m.get('is_consolation') or 0),
                    'team_key': t.get('team_key'),
                    'team_name': t.get('name'),
                    'opp_key': opp.get('team_key', ''),
                    'opp_name': opp.get('name', ''),
                    'cat_wins': cat_w,
                    'cat_losses': cat_l,
                    'cat_ties': cat_t,
                    'result': result,
                    'games_played': num(games.get('completed_games')),
                    'games_remaining': num(games.get('remaining_games')),
                }
                add_stats(row, t.get('team_stats'), cats)
                matchups.append(row)
        time.sleep(0.3)
    print(f'Matchups: {len(matchups)} team-weeks')

    # ---- Transactions (one row per player moved)
    transactions = []
    seen = set()
    for start in range(0, 3000, 25):
        txs = [merge(tx) for tx in items(merge(get(sc, f'league/{key}/transactions;start={start};count=25')['league']).get('transactions'), 'transaction')]
        fresh = 0
        for tx in txs:
            if tx.get('transaction_key') in seen:
                continue
            seen.add(tx.get('transaction_key'))
            fresh += 1
            for p in map(merge, items(tx.get('players'), 'player')):
                td = merge(p.get('transaction_data'))
                # A drop's acting team is the source; everything else is the destination
                if td.get('type') == 'drop':
                    acting_key = td.get('source_team_key') or td.get('destination_team_key')
                    acting_name = td.get('source_team_name') or td.get('destination_team_name')
                else:
                    acting_key, acting_name = td.get('destination_team_key'), td.get('destination_team_name')
                ts = num(tx.get('timestamp'))
                transactions.append({
                    'transaction_id': num(tx.get('transaction_id')),
                    'type': tx.get('type', ''),
                    'status': tx.get('status', ''),
                    'timestamp': ts,
                    'date': time.strftime('%Y-%m-%d', time.gmtime(ts)) if ts != '' else '',
                    'faab_bid': num(tx.get('faab_bid')),
                    'player_action': td.get('type', ''),
                    'player_id': num(p.get('player_id')),
                    'player_name': (p.get('name') or {}).get('full', ''),
                    'nba_team': p.get('editorial_team_abbr', ''),
                    'position': p.get('display_position', ''),
                    'team_key': acting_key or '',
                    'team_name': acting_name or '',
                    'source_type': td.get('source_type', ''),
                    'source_team_key': td.get('source_team_key', ''),
                    'source_team_name': td.get('source_team_name', ''),
                    'destination_type': td.get('destination_type', ''),
                    'destination_team_key': td.get('destination_team_key', ''),
                    'destination_team_name': td.get('destination_team_name', ''),
                })
        # Stop at the last page (or if Yahoo ignores `start` and repeats the same page)
        if not fresh or len(txs) < 25:
            break
        time.sleep(0.3)
    print(f'Transactions: {len(transactions)} player moves')

    # ---- Rosters (current + first week of the season)
    def roster(path, team_key, team_name):
        fc = get(sc, path, tolerant=True)
        if not fc:
            return []
        r = merge(fc['team']).get('roster') or {}
        rows = []
        for p in map(merge, items((r.get('0') or {}).get('players'), 'player')):
            rows.append({
                'team_key': team_key,
                'team_name': team_name,
                'player_id': num(p.get('player_id')),
                'player_name': (p.get('name') or {}).get('full', ''),
                'position': p.get('display_position', ''),
                'selected_position': merge(p.get('selected_position')).get('position', ''),
                'nba_team': p.get('editorial_team_abbr', ''),
                'status': p.get('status', ''),
                'injury_note': p.get('injury_note', ''),
            })
        return rows

    rosters, rosters_draft = [], []
    for s in standings:
        rosters += roster(f"team/{s['team_key']}/roster", s['team_key'], s['team_name'])
        rosters_draft += roster(f"team/{s['team_key']}/roster;week={start_week}", s['team_key'], s['team_name'])
        time.sleep(0.3)
    print(f'Rosters: {len(rosters)} current, {len(rosters_draft)} week {start_week}')

    # ---- Player pool (by Yahoo rank, with ownership)
    pool, seen_players = [], set()
    for start in range(0, POOL_SIZE, 25):
        fc = get(sc, f'league/{key}/players;sort=AR;start={start};count=25/ownership', tolerant=True)
        players = [merge(p) for p in items(merge(fc['league']).get('players'), 'player')] if fc else []
        if not players:
            break
        for idx, p in enumerate(players):
            if p.get('player_key') in seen_players:
                continue
            seen_players.add(p.get('player_key'))
            own = p.get('ownership') or {}
            pool.append({
                'rank': start + idx + 1,
                'player_id': num(p.get('player_id')),
                'player_name': (p.get('name') or {}).get('full', ''),
                'position': p.get('display_position', ''),
                'nba_team': p.get('editorial_team_abbr', ''),
                'status': p.get('status', ''),
                'injury_note': p.get('injury_note', ''),
                'ownership_type': own.get('ownership_type', ''),
                'owner_team_key': own.get('owner_team_key', ''),
                'owner_team_name': own.get('owner_team_name', ''),
            })
        time.sleep(0.3)
    print(f'Player pool: {len(pool)} players')

    meta = {
        'league_key': key,
        'league_name': league.get('name', ''),
        'season': league.get('season', ''),
        'scoring_type': league.get('scoring_type', ''),
        'num_teams': num(league.get('num_teams')),
        'start_week': start_week,
        'end_week': end_week,
        'current_week': current_week,
        'is_finished': int(league.get('is_finished') or 0),
        'last_refresh': int(time.time() * 1000),
        'refresh_seconds': round(time.time() - started),
    }
    return {
        'standings': standings,
        'matchups': matchups,
        'transactions': transactions,
        'rosters': rosters,
        'rosters_draft': rosters_draft,
        'player_pool': pool,
        'stat_categories': cats,
        # meta last, so the web app only sees the new timestamp once everything else is written
        'meta': [{'key': k, 'value': v} for k, v in meta.items()],
    }


# ==================== GOOGLE SHEET ====================

def write_table(ss, name, rows):
    import gspread

    try:
        ws = ss.worksheet(name)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=name, rows=1, cols=1)
    ws.clear()
    if not rows:
        return
    headers = []
    for r in rows:
        for k in r:
            if k not in headers:
                headers.append(k)
    values = [headers] + [['' if r.get(h) is None else r.get(h) for h in headers] for r in rows]
    ws.resize(rows=len(values), cols=len(headers))
    # RAW keeps "412/880" and dates as plain text instead of letting Sheets reinterpret them
    ws.update(values=values, range_name='A1', value_input_option='RAW')
    ws.freeze(rows=1)


def main():
    import gspread

    sc = connect_yahoo()
    key = os.environ.get('LEAGUE_KEY', '').strip() or find_league_key(sc)
    tables = fetch_all(sc, key)

    gc = gspread.service_account_from_dict(json.loads(os.environ['GCP_SA_KEY']))
    ss = gc.open_by_key(os.environ['SHEET_ID'])
    for name, rows in tables.items():
        write_table(ss, name, rows)
        print(f'Wrote {name}: {len(rows)} rows')
    print('Done.')


if __name__ == '__main__':
    main()
