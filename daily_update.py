#!/usr/bin/env python3
"""
daily_update.py

- Reads your Google Sheet "Fantasy_NBA_Data" sheets: 'roster' and 'waiver'
- Loads projections from 'player_projections.csv' (in repo)
- Attempts to pull ESPN league data (private league) using ESPN_S2 and SWID (optional)
- Computes z-scores across categories and recommends add/drop:
    - Suggests the worst roster player to drop (lowest aggregate z)
    - Suggests best waiver player to add (highest aggregate z)
- Posts a detailed report to Slack channel '#all-nba-fantasy-bot'

Required env/secrets:
- SLACK_TOKEN
- ESPN_SWID
- ESPN_S2
- GOOGLE_SHEETS_JSON (full JSON contents)
- LEAGUE_ID (optional; default 285626)
"""

import os
import json
import io
import sys
import time
import requests
import pandas as pd
import numpy as np
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Google Sheets auth libraries
from oauth2client.service_account import ServiceAccountCredentials
import gspread

# ---------- Configuration ----------
LEAGUE_ID = int(os.getenv("LEAGUE_ID", "285626"))
SEASON = int(os.getenv("SEASON", "2025"))  # adjust if needed
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL", "#all-nba-fantasy-bot")

# Categories we use (match your league)
CATS = ["PTS", "REB", "AST", "STL", "BLK", "3PM", "FG%", "FT%"]

# Local filenames in repo
PROJ_CSV = "player_projections.csv"
GCREDS_FILE = "gcreds.json"

# ---------- Helpers ----------
def write_google_creds_to_file():
    """Writes GOOGLE_SHEETS_JSON secret to a file gcreds.json for gspread to use."""
    js = os.getenv("GOOGLE_SHEETS_JSON")
    if not js:
        raise RuntimeError("Missing env var GOOGLE_SHEETS_JSON")
    # If it looks like JSON text, write it:
    with open(GCREDS_FILE, "w", encoding="utf-8") as f:
        f.write(js)

def connect_google_sheet(sheet_name="Fantasy_NBA_Data"):
    write_google_creds_to_file()
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GCREDS_FILE, scope)
    client = gspread.authorize(creds)
    sh = client.open(sheet_name)
    return sh

def read_sheet_to_df(sh, worksheet_name):
    try:
        ws = sh.worksheet(worksheet_name)
    except Exception as e:
        print(f"[WARN] Worksheet {worksheet_name} not found: {e}")
        return pd.DataFrame()
    records = ws.get_all_records()
    return pd.DataFrame.from_records(records)

def load_projections(csv_path=PROJ_CSV):
    if not os.path.exists(csv_path):
        print(f"[WARN] Projections file {csv_path} not found. Creating empty dataframe.")
        return pd.DataFrame(columns=["Player"] + CATS)
    df = pd.read_csv(csv_path)
    # Normalize column names (strip)
    df.columns = [c.strip() for c in df.columns]
    return df

def try_fetch_espn_league(league_id=LEAGUE_ID, season=SEASON):
    """
    Attempt to fetch ESPN league JSON using cookies.
    If it fails (no cookies or blocked), return None.
    """
    espn_s2 = os.getenv("ESPN_S2")
    swid = os.getenv("ESPN_SWID")
    if not espn_s2 or not swid:
        print("[INFO] ESPN cookies not found in env; skipping ESPN fetch.")
        return None
    url = f"https://fantasy.espn.com/apis/v3/games/fba/seasons/{season}/segments/0/leagues/{league_id}"
    params = {
        "view": ["mRoster", "mTeam", "mMatchup", "mSchedule"],
    }
    cookies = {"espn_s2": espn_s2, "SWID": swid}
    headers = {
        "User-Agent": "Mozilla/5.0 (fantasy-bot)",
        "Accept": "application/json",
    }
    try:
        resp = requests.get(url, params=params, cookies=cookies, headers=headers, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"[WARN] Could not fetch ESPN league data: {e}")
        return None

def compute_z_scores(proj_df):
    """
    Compute z-scores across the projection dataframe for the categories in CATS.
    Returns a DataFrame with added columns 'z_<CAT>' and 'z_total' (sum across cats).
    """
    df = proj_df.copy()
    # Ensure numeric columns exist; if missing, fill with 0
    for cat in CATS:
        if cat not in df.columns:
            df[cat] = 0.0
    # Convert to numeric (coerce non-numeric -> NaN -> fill 0)
    for cat in CATS:
        df[cat] = pd.to_numeric(df[cat], errors="coerce").fillna(0.0)

    # For FG% and FT% we assume they are in percent form (e.g., 47.5 for 47.5%)
    # Compute z-scores columnwise
    z_cols = []
    for cat in CATS:
        mean = df[cat].mean()
        std = df[cat].std(ddof=0) if df[cat].std(ddof=0) != 0 else 1.0
        zcol = f"z_{cat}"
        df[zcol] = (df[cat] - mean) / std
        z_cols.append(zcol)

    # Aggregate z-score (sum of category z-scores)
    df["z_total"] = df[z_cols].sum(axis=1)
    # Standardize the z_total too (optional)
    return df

def lookup_player_proj(df_proj, player_name):
    """Simple fuzzy lookup; exact match first, otherwise case-insensitive contains."""
    if player_name is None:
        return None
    # Exact match
    found = df_proj[df_proj["Player"].str.lower().str.strip() == player_name.lower().strip()]
    if not found.empty:
        return found.iloc[0]
    # Contains match
    contains = df_proj[df_proj["Player"].str.lower().str.contains(player_name.lower().split()[0])]
    if not contains.empty:
        return contains.iloc[0]
    return None

def prepare_report(roster_df, waiver_df, proj_df, espn_json=None):
    """
    Core logic to compute:
    - Team aggregate totals (projected)
    - Underperformers (bottom 3 by z_total on roster)
    - Top waiver add candidates (top 5 by z_total)
    - Suggested add/drop pair (best waiver vs worst roster)
    """
    # compute z-scores for all projection players:
    proj_z = compute_z_scores(proj_df)
    # ensure Player column exists
    if "Player" not in proj_z.columns:
        raise RuntimeError("Projection file must contain 'Player' column")

    # Map roster players to projection z_total
    roster = roster_df.copy()
    roster["Player"] = roster["Player"].astype(str)
    roster_z_list = []
    for _, row in roster.iterrows():
        p = row["Player"]
        proj_row = lookup_player_proj(proj_z, p)
        if proj_row is None:
            roster_z_list.append({"Player": p, "z_total": -999.0})
        else:
            roster_z_list.append({"Player": p, "z_total": float(proj_row["z_total"])})
    roster_z = pd.DataFrame(roster_z_list)
    roster_z = roster_z.sort_values("z_total", ascending=True).reset_index(drop=True)

    # Map waiver players similarly
    waiver = waiver_df.copy()
    if waiver.empty:
        waiver_z = pd.DataFrame(columns=["Player", "z_total"])
    else:
        waiver["Player"] = waiver["Player"].astype(str)
        waiver_z_list = []
        for _, row in waiver.iterrows():
            p = row["Player"]
            proj_row = lookup_player_proj(proj_z, p)
            if proj_row is None:
                waiver_z_list.append({"Player": p, "z_total": -999.0})
            else:
                waiver_z_list.append({"Player": p, "z_total": float(proj_row["z_total"])})
        waiver_z = pd.DataFrame(waiver_z_list)
        waiver_z = waiver_z.sort_values("z_total", ascending=False).reset_index(drop=True)

    # Underperformers: bottom 3 roster players
    underperformers = roster_z.head(3).to_dict(orient="records")

    # Top waiver adds: top 5
    top_waivers = waiver_z.head(5).to_dict(orient="records")

    suggestion = None
    if not waiver_z.empty and not roster_z.empty:
        candidate_add = waiver_z.iloc[0]
        candidate_drop = roster_z.iloc[0]
        delta = candidate_add["z_total"] - candidate_drop["z_total"]
        if delta > 0:
            suggestion = {
                "add": candidate_add["Player"],
                "add_z": float(candidate_add["z_total"]),
                "drop": candidate_drop["Player"],
                "drop_z": float(candidate_drop["z_total"]),
                "z_gain": float(delta),
            }

    # Team totals (projected) — sum across roster projection rows if available
    team_proj = {}
    for cat in CATS:
        total = 0.0
        for p in roster["Player"].tolist():
            proj_row = lookup_player_proj(proj_z, p)
            if proj_row is not None:
                total += float(proj_row[cat])
        team_proj[cat] = total

    return {
        "underperformers": underperformers,
        "top_waivers": top_waivers,
        "suggestion": suggestion,
        "team_proj": team_proj,
        "proj_count": len(proj_z),
    }

def format_slack_message(report):
    lines = []
    lines.append(":basketball: *Fantasy NBA Daily Update*")
    lines.append("")
    # Team projections summary
    lines.append("*Projected team totals (using projections):*")
    cat_line = " | ".join([f"{cat}: {report['team_proj'].get(cat, 0):.1f}" for cat in CATS])
    lines.append(cat_line)
    lines.append("")
    # Underperformers
    lines.append("*Underperformers (lowest projected z-scores on your roster):*")
    if report["underperformers"]:
        for u in report["underperformers"]:
            lines.append(f"- {u['Player']}: z_total = {u['z_total']:.2f}")
    else:
        lines.append("- None identified (roster empty)")
    lines.append("")
    # Top waiver adds
    lines.append("*Top waiver targets (by aggregate projection z-score):*")
    if report["top_waivers"]:
        for w in report["top_waivers"]:
            lines.append(f"- {w['Player']}: z_total = {w['z_total']:.2f}")
    else:
        lines.append("- No waiver data available")
    lines.append("")
    # Suggestion
    lines.append("*Suggested Add / Drop:*")
    s = report["suggestion"]
    if s:
        lines.append(f"> *Add:* {s['add']} (z={s['add_z']:.2f})")
        lines.append(f"> *Drop:* {s['drop']} (z={s['drop_z']:.2f})")
        lines.append(f"> *Projected z gain:* {s['z_gain']:.2f}")
        lines.append("")
        lines.append("_Reasoning:_ Replace your weakest projected contributor with the highest available projected contributor. This is a projection-driven signal — consider injuries or minutes before acting.")
    else:
        lines.append("- No positive add/drop identified (no waiver players beat roster players by projections).")
    lines.append("")
    lines.append(f"_Data: projections rows={report['proj_count']}_")
    return "\n".join(lines)

def post_to_slack(text):
    token = os.getenv("SLACK_TOKEN")
    if not token:
        print("[ERROR] SLACK_TOKEN missing")
        return False, "Missing SLACK_TOKEN"
    client = WebClient(token=token)
    try:
        client.chat_postMessage(channel=SLACK_CHANNEL, text=text)
        return True, "Posted"
    except SlackApiError as e:
        print(f"[ERROR] Slack post failed: {e.response['error']}")
        return False, str(e)

# ---------- Main ----------
def main():
    print("[INFO] Starting daily_update.py")
    # Load projections
    proj_df = load_projections(PROJ_CSV)
    if proj_df.empty:
        print("[WARN] Projections are empty. Please provide player_projections.csv in repo.")
    else:
        print(f"[INFO] Loaded projections: {len(proj_df)} rows")

    # Connect to Google Sheets and read roster + waiver
    try:
        sh = connect_google_sheet()
        roster_df = read_sheet_to_df(sh, "roster")
        waiver_df = read_sheet_to_df(sh, "waiver")
        print(f"[INFO] Roster rows: {len(roster_df)}, Waiver rows: {len(waiver_df)}")
    except Exception as e:
        print(f"[WARN] Could not read Google Sheets: {e}")
        roster_df = pd.DataFrame(columns=["Player"])
        waiver_df = pd.DataFrame(columns=["Player"])

    # Try ESPN fetch (best-effort)
    espn_json = try_fetch_espn_league()
    if espn_json is not None:
        print("[INFO] Fetched ESPN league JSON (successful).")
    else:
        print("[INFO] ESPN league JSON not available or fetch failed; continuing with sheet data.")

    # Ensure roster_df and waiver_df have Player column
    if "Player" not in roster_df.columns:
        roster_df["Player"] = roster_df.iloc[:, 0] if not roster_df.empty else []
    if "Player" not in waiver_df.columns:
        waiver_df["Player"] = waiver_df.iloc[:, 0] if not waiver_df.empty else []

    # Prepare report
    try:
        report = prepare_report(roster_df, waiver_df, proj_df, espn_json)
    except Exception as e:
        print(f"[ERROR] Preparing report failed: {e}")
        # Minimal fallback report
        report = {
            "underperformers": [],
            "top_waivers": [],
            "suggestion": None,
            "team_proj": {c: 0.0 for c in CATS},
            "proj_count": len(proj_df),
        }

    message = format_slack_message(report)
    success, info = post_to_slack(message)
    if success:
        print("[INFO] Slack message posted successfully.")
    else:
        print(f"[ERROR] Slack post failed: {info}")

if __name__ == "__main__":
    main()
