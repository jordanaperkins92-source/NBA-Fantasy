import pandas as pd
import numpy as np
import requests
import json
import os
from google.oauth2.service_account import Credentials
import gspread


# --- CONFIGURATION ---
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")
SHEET_NAME = "Fantasy_NBA_Data"

# Your 8-cat league categories
CATS = ["PTS", "REB", "AST", "STL", "BLK", "3PM", "FG%", "FT%"]


# --- GOOGLE SHEETS CONNECTION ---
def connect_to_sheet():
    """Authenticate and open your Google Sheet."""
    if not GOOGLE_CREDENTIALS:
        raise ValueError("❌ Missing GOOGLE_CREDENTIALS secret in GitHub!")

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds_dict = json.loads(GOOGLE_CREDENTIALS)
    creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
    client = gspread.authorize(creds)
    print("[INFO] Connected to Google Sheets successfully.")
    return client.open(SHEET_NAME)


def read_sheet_to_df(sheet, tab_name):
    """Read a specific tab from Google Sheets into a DataFrame."""
    worksheet = sheet.worksheet(tab_name)
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)
    if df.empty:
        print(f"[WARN] '{tab_name}' sheet is empty.")
    return df


# --- ANALYTICS ---
def calculate_zscores(df):
    """Compute z-scores for all fantasy categories."""
    df_z = df.copy()
    for cat in CATS:
        if cat not in df_z.columns:
            print(f"[WARN] Missing category column: {cat}")
            continue
        mean = df_z[cat].mean()
        std = df_z[cat].std()
        df_z[cat + "_z"] = (df_z[cat] - mean) / std if std != 0 else 0
    z_cols = [c + "_z" for c in CATS if c + "_z" in df_z.columns]
    df_z["z_total"] = df_z[z_cols].sum(axis=1)
    return df_z


def match_players(df_proj, df_players):
    """Merge player list (roster or waiver) with projection stats."""
    df_players["Player_lower"] = df_players["Player"].str.lower()
    df_proj["Player_lower"] = df_proj["Player"].str.lower()
    merged = pd.merge(df_players, df_proj, on="Player_lower", how="left", suffixes=("_team", ""))
    merged.drop(columns=["Player_lower"], inplace=True)
    merged["Player"] = merged["Player"].fillna(merged["Player_team"])
    return merged


def recommend_add_drop(roster_z, waiver_z):
    """Suggest the best single add/drop move based on z-total improvement."""
    roster_z = roster_z.sort_values(by="z_total").reset_index(drop=True)
    waiver_z = waiver_z.sort_values(by="z_total", ascending=False).reset_index(drop=True)

    worst = roster_z.iloc[0]
    best = waiver_z.iloc[0]
    gain = best["z_total"] - worst["z_total"]

    return {
        "drop": worst["Player"],
        "add": best["Player"],
        "gain": gain
    }


# --- SLACK ---
def send_to_slack(message):
    """Post a formatted message to Slack."""
    if not SLACK_WEBHOOK_URL:
        print("[WARN] No Slack webhook found — skipping Slack notification.")
        return
    resp = requests.post(SLACK_WEBHOOK_URL, json={"text": message})
    if resp.status_code == 200:
        print("[INFO] Slack message sent successfully.")
    else:
        print(f"[ERROR] Slack post failed: {resp.text}")


# --- MAIN WORKFLOW ---
def main():
    print("🏀 Starting daily Fantasy NBA update...")

    # Connect to Google Sheets
    sh = connect_to_sheet()

    # Load data
    print("[INFO] Reading data from Google Sheets...")
    proj_df = read_sheet_to_df(sh, "projections")
    roster_df = read_sheet_to_df(sh, "roster")
    waiver_df = read_sheet_to_df(sh, "waiver")

    if proj_df.empty or roster_df.empty or waiver_df.empty:
        raise ValueError("One or more sheets (projections/roster/waiver) are empty!")

    print(f"[INFO] Loaded projections: {len(proj_df)} rows")
    print(f"[INFO] Loaded roster: {len(roster_df)} rows")
    print(f"[INFO] Loaded waiver pool: {len(waiver_df)} rows")

    # Calculate z-scores
    proj_z = calculate_zscores(proj_df)
    roster_z = match_players(proj_z, roster_df)
    waiver_z = match_players(proj_z, waiver_df)

    # Recommend moves
    move = recommend_add_drop(roster_z, waiver_z)

    # Prepare Slack message
    msg = (
        f"🏀 *Fantasy NBA Daily Report*\n\n"
        f"📊 Projections loaded: `{len(proj_df)}` players\n"
        f"👥 Your roster: `{len(roster_df)}` | Waiver pool: `{len(waiver_df)}`\n\n"
        f"💡 *Suggested Move:*\n"
        f"• Drop → *{move['drop']}*\n"
        f"• Add → *{move['add']}*\n"
        f"• Projected gain → `{move['gain']:.2f}` total z-score\n\n"
        f"🕒 Auto-updated from Google Sheets"
    )

    print("[INFO] Sending daily report to Slack...")
    send_to_slack(msg)

    print("✅ Finished Fantasy NBA daily update.")


if __name__ == "__main__":
    main()
