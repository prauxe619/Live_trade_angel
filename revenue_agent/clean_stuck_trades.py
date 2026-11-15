import json
import shutil
from pathlib import Path

# The exact path to your trade ledger
LOG_FILE = Path("data/signal_engine/signals_log.json")
BACKUP_FILE = Path("data/signal_engine/signals_log_backup.json")

def cleanup_all_open_trades():
    print("\n" + "═" * 60)
    print(" 🧹 BLACK ENGINE — ALL OPEN TRADES CLEANUP")
    print("═" * 60)

    if not LOG_FILE.exists():
        print(f"❌ Error: Could not find {LOG_FILE}")
        return

    # 1. Create a safe backup first
    try:
        shutil.copy2(LOG_FILE, BACKUP_FILE)
        print(f"✅ Safety backup created at: {BACKUP_FILE.name}")
    except Exception as e:
        print(f"❌ Failed to create backup: {e}")
        return

    # 2. Read the current ledger
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            records = json.load(f)
    except Exception as e:
        print(f"❌ Error reading JSON: {e}")
        return

    # 3. Filter out ALL open trades
    cleaned_records = []
    deleted_count = 0

    for r in records:
        # 🔥 UPDATED: If the trade is OPEN (regardless of type), delete it!
        if r.get("status") == "open":
            print(f"   🗑️ Deleted open trade: {r.get('signal_id', 'Unknown')}")
            deleted_count += 1
        else:
            # Otherwise, keep it (this keeps all closed history intact)
            cleaned_records.append(r)

    if deleted_count == 0:
        print("⚠️ No open trades found. Your ledger is already clean!")
        return

    print(f"\n🔍 Removed {deleted_count} open trades from the ledger.")

    # 4. Save the perfectly formatted JSON back to the file
    try:
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            json.dump(cleaned_records, f, indent=4)
        print("\n✅ Ledger updated successfully! You can now refresh your dashboard.")
    except Exception as e:
        print(f"\n❌ Error saving the updated ledger: {e}")
        print("Don't worry, your backup is safe!")

if __name__ == "__main__":
    cleanup_all_open_trades()