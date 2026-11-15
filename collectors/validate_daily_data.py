# ============================================================
# validate_daily_data.py - HYPER ROBUST V3
# Production-grade data validation for InstitutionalCollector V3
# ============================================================

import pandas as pd
import pytz
import json
import sys
from pathlib import Path
from datetime import datetime, time

# Import your telegram utility
try:
    from scripts.telegram_alert import send_telegram_message
except ImportError:
    def send_telegram_message(msg): print(f"Telegram: {msg}")

# --- CONFIGURATION ---
BASE_DIR = Path("data/live")
OUT_DIR = Path("data/validation")
TZ = pytz.timezone("Asia/Kolkata")

# Nifty/VIX usually have 375-376 1-minute candles per day
EXPECTED_CANDLES = 375 
MARKET_START = time(9, 15)
MARKET_END = time(15, 30)

def load_parquet(path):
    """Safely loads parquet with error handling for corruption."""
    if not path.exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception as e:
        print(f"⚠️ Error loading {path.name}: {e}")
        return None

def validate_candles(df):
    """Validates Nifty Spot and VIX candle data."""
    issues = []
    if df is None or df.empty:
        return ["Candles file missing or empty"]

    # FIX: Robust column detection (handles 'symbol' or 'name')
    sym_col = 'symbol' if 'symbol' in df.columns else ('name' if 'name' in df.columns else None)
    
    if not sym_col:
        return ["Candle file missing identification column (symbol/name)"]

    for sym in df[sym_col].unique():
        d = df[df[sym_col] == sym].copy()
        
        # Ensure timestamp is datetime
        d["timestamp"] = pd.to_datetime(d["timestamp"])
        d = d.sort_values("timestamp")

        # Check count
        count = len(d)
        if count < (EXPECTED_CANDLES - 5): # Allow small buffer for API lag
            issues.append(f"📉 {sym}: Low data density ({count}/{EXPECTED_CANDLES} rows)")

        # Check for time gaps > 1 minute
        d['diff'] = d['timestamp'].diff().dt.total_seconds() / 60
        gaps = d[d['diff'] > 1.5]
        if not gaps.empty:
            issues.append(f"⚠️ {sym}: {len(gaps)} timing gaps detected in stream")

    return issues

def validate_options(df):
    """Validates complex Option Greek data from Collector V3."""
    issues = []
    if df is None or df.empty:
        return ["Options file missing or empty"]

    # 1. Check for Greeks (Collector V3 specific)
    required_cols = ['delta', 'gamma', 'theta', 'vega', 'iv_used', 'market_pcr']
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        issues.append(f"❌ Missing Greek columns: {', '.join(missing_cols)}")

    # 2. Check for Nulls in critical AI features
    for col in ['delta', 'gamma', 'ltp', 'oi']:
        if col in df.columns and df[col].isnull().any():
            null_count = df[col].isnull().sum()
            issues.append(f"❓ {col}: Found {null_count} null values")

    # 3. Strike Price Logic (Ensuring ATM ± Range is healthy)
    unique_strikes = df['strike'].nunique()
    if unique_strikes < 5:
        issues.append(f"🚨 Critical: Only {unique_strikes} strikes collected. Data too thin.")

    # 4. Logical Check: Greeks shouldn't be zero for active options
    if 'delta' in df.columns:
        zero_deltas = (df['delta'] == 0).sum()
        if zero_deltas > (len(df) * 0.1): # If > 10% are zero
            issues.append(f"🧊 Abnormal amount of zero Delta values ({zero_deltas})")

    return issues

def calculate_day_score(all_issues):
    if not all_issues: return "🏆 PERFECT"
    if len(all_issues) <= 2: return "✅ GOOD"
    if any("missing" in i.lower() or "critical" in i.lower() for i in all_issues): return "🔴 BAD"
    return "⚠️ WARNING"

def run_validation(date_str=None):
    """Main execution function."""
    if date_str is None:
        date_str = datetime.now(TZ).strftime("%Y-%m-%d")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"🔍 Starting Hyper-Validation for {date_str}...")

    report = {
        "meta": {
            "date": date_str,
            "validated_at": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
            "version": "V3-Robust"
        },
        "results": {},
        "status": "INCOMPLETE"
    }

    # Load Files
    c_path = BASE_DIR / "candles" / f"{date_str}.parquet"
    o_path = BASE_DIR / "options" / f"{date_str}.parquet"
    
    candles = load_parquet(c_path)
    options = load_parquet(o_path)

    # Validate
    report["results"]["candles"] = validate_candles(candles)
    report["results"]["options"] = validate_options(options)

    # Compile all issues
    all_issues = report["results"]["candles"] + report["results"]["options"]
    
    # Check for "Depth" files if you have them, else ignore
    d_path = BASE_DIR / "depth" / f"{date_str}.parquet"
    if d_path.exists():
        report["results"]["depth"] = ["Depth file exists but validation logic skipped"]
    else:
        report["results"]["depth"] = []

    report["status"] = calculate_day_score(all_issues)

    # --- SAVE REPORT ---
    report_file = OUT_DIR / f"{date_str}_report.json"
    with open(report_file, "w") as f:
        json.dump(report, f, indent=4)

    # --- TELEGRAM NOTIFICATION ---
    issue_text = "\n".join([f"• {i}" for i in all_issues]) if all_issues else "No issues detected."
    msg = (
        f"🏁 *Daily Data Validation: {report['status']}*\n"
        f"📅 Date: `{date_str}`\n"
        f"📊 Options Rows: `{len(options) if options is not None else 0}`\n"
        f"🕯️ Candle Rows: `{len(candles) if candles is not None else 0}`\n\n"
        f"*Issues found:*\n{issue_text}"
    )
    
    try:
        send_telegram_message(msg)
    except:
        print("Telegram notification failed.")

    print(f"📊 Validation Final Score: {report['status']}")
    return report

if __name__ == "__main__":
    # If run via command line, it takes the date as an argument or defaults to Today
    target_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now(TZ).strftime("%Y-%m-%d")
    run_validation(target_date)