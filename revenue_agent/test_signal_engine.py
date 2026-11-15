import json
from pathlib import Path

# The exact path where your engine saves the trade ledger
LOG_FILE = Path("data/signal_engine/signals_log.json")

def generate_all_time_report():
    print("\n" + "═" * 105)
    print(" 📊 BLACK ENGINE — ALL-TIME TRADE ANALYSIS REPORT")
    print("═" * 105)

    if not LOG_FILE.exists():
        print("❌ Error: signals_log.json not found.")
        return

    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            records = json.load(f)
    except Exception as e:
        print(f"❌ Error reading ledger: {e}")
        return

    if not records:
        print("⚠️ No trades found in the ledger.")
        return

    # Added DATE column and adjusted spacing to fit everything perfectly
    print(f" {'DATE':<10} | {'ENTRY':<8} | {'EXIT':<8} | {'INDEX':<10} | {'TYPE':<7} | {'DIR':<4} | {'BUY AT':<8} | {'SELL AT':<8} | {'OUTCOME':<8} | {'P&L (₹)'}")
    print("-" * 105)

    total_pnl = 0.0
    wins = 0
    losses = 0

    for r in records:
        # Extract Date
        trade_date = r.get("date", "—")
        
        # Extract Time (just the HH:MM:SS part)
        entry_time = r.get("timestamp", "").split(" ")[-1] if r.get("timestamp") else "—"
        exit_time  = r.get("exit_time", "").split(" ")[-1] if r.get("exit_time") else "—"
        
        # Trade metadata
        idx      = r.get("index", "")
        sig_type = r.get("signal_type", "")
        dir_     = r.get("direction", "")
        
        # Exact Premiums
        entry_px = float(r.get("entry_price") or 0.0)
        exit_px  = float(r.get("exit_price") or 0.0)
        
        entry_str = f"₹{entry_px:.1f}" if entry_px > 0 else "—"
        exit_str  = f"₹{exit_px:.1f}" if exit_px > 0 else "—"
        
        # Outcome and P&L
        status   = r.get("status", "open").upper()
        outcome  = r.get("outcome", "") or "—"
        pnl      = float(r.get("pnl_inr") or 0.0)
        
        if status == "OPEN":
            outcome = "OPEN"
            exit_str = "—"
            exit_time = "—"
        else:
            total_pnl += pnl
            if pnl > 0: wins += 1
            elif pnl < 0: losses += 1

        pnl_str = f"₹{pnl:,.0f}" if status != "OPEN" and pnl != 0 else "—"

        # Print the highly detailed row including the Date
        print(f" {trade_date:<10} | {entry_time:<8} | {exit_time:<8} | {idx:<10} | {sig_type:<7} | {dir_:<4} | {entry_str:<8} | {exit_str:<8} | {outcome:<8} | {pnl_str}")

    print("═" * 105)
    pnl_prefix = "🟩" if total_pnl >= 0 else "🟥"
    print(f" {pnl_prefix} Total Realized P&L: ₹{total_pnl:,.2f}  |  Wins: {wins}  |  Losses: {losses}")
    print("═" * 105 + "\n")

if __name__ == "__main__":
    generate_all_time_report()