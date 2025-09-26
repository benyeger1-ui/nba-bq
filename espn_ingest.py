#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
espn_ingest.py

Daily: pulls yesterday's per-player box scores via NBA.com Stats (nba_api) and also records games.
Backfill: pulls historical games and player box scores via ESPN site/core endpoints.

- Works on BigQuery Sandbox (no partitions, no DML).
- Robust HTTP headers and region/lang params for ESPN.
- Triple fallback for ESPN player boxes if needed.
- Schema auto-add on load.

Usage:
  python espn_ingest.py --mode daily
  python espn_ingest.py --mode backfill --start 2024-10-22 --end 2024-10-28
"""

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

# nba_api for daily player boxes
from nba_api.stats.endpoints import scoreboardv2, boxscoretraditionalv2

# -----------------------------
# Config via environment
# -----------------------------
PROJECT_ID = os.environ["GCP_PROJECT_ID"]  # set in GitHub Actions secrets
DATASET = os.environ.get("BQ_DATASET", "nba_data")

# Service account JSON stored as a single secret string
SA_INFO = json.loads(os.environ["GCP_SA_KEY"])
CREDS = service_account.Credentials.from_service_account_info(SA_INFO)
BQ = bigquery.Client(project=PROJECT_ID, credentials=CREDS)

# -----------------------------
# ESPN endpoints and http setup
# -----------------------------
ESPN_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
ESPN_SUMMARY = "https://site.web.api.espn.com/apis/site/v2/sports/basketball/nba/summary"
ESPN_SITE_BOXSCORE = "https://site.web.api.espn.com/apis/site/v2/sports/basketball/nba/boxscore"
ESPN_CORE_EVENT_ROOT = "https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0
