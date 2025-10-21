import pandas as pd
import numpy as np
import requests
from google.oauth2.service_account import Credentials
import gspread
import os

# --- CONFIGURATION ---
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")  # stored in GitHub Secrets
SHEET_NAME = "Fantasy_NBA_Data"

# Your league’s categories (adjust if needed)
CATS = ["PTS", "REB", "AST", "STL", "BLK", "3PM", "FG%", "FT%"]


# --- GOOGLE SHEETS AUTH ---
def connect_to_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file("credentials.json", scopes=scope)
    client = gspread.authorize(creds)
    return client.open(SHEET_NAME)


def read_sheet_to_df(sheet, tab_name):
    """Reads a tab from Google Sheets into a pandas DataFrame."""
    worksheet = sheet.worksheet(tab_name)
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)
    return df


# --- Z-SCORE CALCULATION ---
def calculate_zscores(df):
    df_z = df.copy()
    for cat in CATS:
        if cat not in df.columns:
            print(f"[WARN] Missing category: {cat}")
            continue
        mean = df[cat].mean()
        std = df[cat].std()
        df_z[cat + "_z"] = (df[cat] - mean) / std if std != 0 else 0
    df_z["z_total"] = df_z[[c + "_z" for c in CATS if c + "_z" in df_z.columns]].sum(axis=1)
    return df_z


# --- MATCH ROSTER AND WAIVER WITH PROJECTIONS ---
def match_players(df_proj, df_players):
    """Merge player list (roster/waiver) with projections by Player name (fuzzy match)."""
    df_players["Player_lower"] = df_players["Player"].str.lower()
    df_proj["Player_lower"] = df_proj["Player"].str.lower()
    merged = pd.merge(df_players, df_proj, on="Player_lower", how="left", suffixes=("_team", ""))
    merged.drop(columns=["Player_lower"], inplace=True)
    merged["Player"] = merged["Player"].fillna(merged["Player_team"])
    return merged


# --- FIND BEST WAIVER MOVE ---
def recommend_add_drop(roster_z, waiver_z):
    """Finds the optimal add/drop move based on total z-scores."""
    roster_z = roster_z.sort_values(by="z_total").reset_index(drop=True)
    waiver_z = waiver_z.sort_values(by="z_total", ascending=False).reset_index(drop=True)

    worst_player = roster_z.iloc[0]
    best_waiver = waiver_z.iloc[0]
    improvement = best_waiver["z_total"] - worst_player["z_total"]

    return {
        "drop": worst_player["Player"],
        "add": best_waiver["Player"],
        "gain": improvement,
    }


# --- SLACK NOTIFICATION ---
def send_to_slack(message):
    payload = {"text": message}
    response = requests.post(SLACK_WEBHOOK_URL, json=payload)
    if response.status_code == 200:
        print("[INFO] Slack message sent successfully.")
    else:
        print(f"[ERROR] Failed to send Slack message: {response.text}")


# --- MAIN WORKFLOW ---
def main():
    print("[INFO] Connecting to Google Sheets...")
    sh = connect_to_sheet()

    print("[INFO] Loading data from Sheets...")
    proj_df = read_sheet_to_df(sh, "projections")
    roster_df = read_sheet_to_df(sh, "roster")
    waiver_df = read_sheet_to_df(sh, "waiver")

    if proj_df.empty or roster_df.empty or waiver_df.empty:
        raise ValueError("One or more sheets are empty. Check your Google Sheet data.")

    print(f"[INFO] Loaded projections: {len(proj_df)} rows")
    print(f"[INFO] Loaded roster: {len(roster_df)} rows")
    print(f"[INFO] Loaded waiver pool: {len(waiver_df)} rows")

    # Compute z-scores
    proj_z = calculate_zscores(proj_df)

    # Merge rosters
    roster_z = match_players(proj_z, roster_df)
    waiver_z = match_players(proj_z, waiver_df)

    # Recommend move
    move = recommend_add_drop(roster_z, waiver_z)

    # Format Slack message
    msg = (
        f"🏀 *Fantasy NBA Daily Report*\n\n"
        f"📊 Total players in projections: {len(proj_df)}\n"
        f"👥 Your roster: {len(roster_df)} | Waiver pool: {len(waiver_df)}\n\n"
        f"💡 *Recommended Move:*\n"
        f"Drop: *{move['drop']}*\n"
        f"Add: *{move['add']}*\n"
        f"Estimated improvement: `{move['gain']:.2f}` total z-score\n\n"
        f"🕒 Updated automatically from Google Sheets"
    )

    print("[INFO] Sending to Slack...")
    send_to_slack(msg)


if __name__ == "__main__":
    main()
