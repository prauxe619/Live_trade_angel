import time
from datetime import datetime
from unittest.mock import MagicMock, patch
import numpy as np
import pandas as pd

import daily_signal_engine_v11 as eng


def fake_ist(hour: int, minute: int) -> datetime:
    # Wednesday (weekday < 5)
    return datetime(2026, 11, 25, hour, minute, 0, tzinfo=eng.IST)


def make_bull_candles(n: int = 40, start: float = 24000.0) -> pd.DataFrame:
    closes = np.linspace(start, start + 120, n)
    return pd.DataFrame({
        "open": closes - 2.0,
        "high": closes + 4.0,
        "low": closes - 4.0,
        "close": closes,
        "volume": np.full(n, 1000.0),
    })


def build_offline_engine():
    """Instantiate DailySignalEngine without broker login, DB restore, or threads."""
    with patch.object(eng.SignalDataFetcher, "_auth", return_value=False), \
         patch.object(eng.TargetMonitor, "start"), \
         patch.object(eng.DailySignalEngine, "_load_state"), \
         patch.object(eng.DailySignalEngine, "_restore_open_signals"), \
         patch.object(eng.DailySignalEngine, "_save_state"), \
         patch("trading_live.engine_hooks.live_management_requested", return_value=False), \
         patch("trading_live.engine_hooks.live_execution_requested", return_value=False):
        engine = eng.DailySignalEngine()
    engine.monitor.register = MagicMock()
    engine.monitor.close_active_reversal = MagicMock()
    engine._save_state = MagicMock()
    return engine


def run_all_tests():
    print("1. Testing entry windows...")
    for hm, expected in [
        ("09:20", False), ("09:35", True), ("10:05", True), ("10:45", True),
        ("11:05", True), ("11:10", False), ("11:30", False), ("13:00", False),
        ("13:15", False), ("13:30", False), ("13:35", True), ("14:00", True), ("14:30", True),
        ("14:35", True), ("14:40", False), ("15:00", False),
    ]:
        got = eng._is_valid_entry_window(hm)
        assert got == expected, f"Window {hm}: expected {expected}, got {got}"
    print("   PASS")

    print("2. Testing dynamic probability thresholds (scheduled & live)...")
    with patch.object(eng, "_ist_now", return_value=fake_ist(9, 35)):
        assert eng._effective_prob_threshold("NIFTY", is_live=False) == 0.60
        assert eng._effective_prob_threshold("BANKNIFTY", is_live=False) == 0.68
        assert eng._effective_prob_threshold("NIFTY", is_live=True) == 0.65      # floor 0.65
        assert eng._effective_prob_threshold("BANKNIFTY", is_live=True) == 0.71  # 0.68 + 0.03

    with patch.object(eng, "_ist_now", return_value=fake_ist(13, 30)):
        assert eng._effective_prob_threshold("NIFTY", is_live=False) == 0.63
        assert eng._effective_prob_threshold("BANKNIFTY", is_live=False) == 0.71

    with patch.object(eng, "_ist_now", return_value=fake_ist(14, 30)):
        assert eng._effective_prob_threshold("NIFTY", is_live=False) == 0.67
        assert eng._effective_prob_threshold("BANKNIFTY", is_live=False) == 0.75
        assert eng._effective_prob_threshold("BANKNIFTY", is_live=True) == 0.78
    print("   PASS")

    print("3. Testing VIX > 28 single penalty (no 0.45x stacking)...")
    with patch.object(eng, "_persist_vwap_shadow_observation"):
        snap_high_vix = {"spot": 24120.0, "vix": 30.0, "pcr": 1.35, "net_gex": 100.0, "_age_s": 5.0}
        res = eng.TechnicalBrain.analyze(make_bull_candles(), snap_high_vix)
        assert res["prob"] > 0.68, f"VIX>28 prob too low ({res['prob']}); check if/elif!"
    print(f"   PASS (VIX=30 prob={res['prob']:+.4f})")

    print("4. Testing 14:30 scan allowed vs 14:35 blocked (NO_NEW_SNIPER_HM)...")
    engine = build_offline_engine()
    candles = make_bull_candles()
    valid_snap = {"spot": 24120.0, "vix": 15.0, "pcr": 1.35, "net_gex": 80.0, "candles": candles, "_age_s": 5.0}

    def fake_expiry(idx, trade_type="INTRADAY", same_day_only=False, **kw):
        return "" if same_day_only else "03 DEC 26"

    with patch.object(eng, "_ist_now", return_value=fake_ist(14, 30)), \
         patch.object(eng, "_to_vip"), \
         patch.object(eng, "_append_signal_log"), \
         patch.object(eng, "_persist_vwap_shadow_observation"), \
         patch.object(eng, "_get_expiry", side_effect=fake_expiry), \
         patch.object(eng, "select_optional_itm_strike", side_effect=lambda f, i, s, d, e: {"strike": s}), \
         patch.object(engine.fetcher, "fetch_vix", return_value=15.0), \
         patch.object(engine.fetcher, "get_index_data", side_effect=lambda k: valid_snap if k == "NIFTY" else {}), \
         patch.object(engine.fetcher, "get_option_premium", return_value=160.0):
        engine.run_scan("14:30 Final Entry Opportunity", stealth_mode=True)
        assert engine.monitor.register.call_count == 1, "14:30 scan failed to register NIFTY Jackpot!"

    engine.monitor.register.reset_mock()
    engine._last_sniper.clear()
    with patch.object(eng, "_ist_now", return_value=fake_ist(14, 35)), \
         patch.object(eng, "_to_vip"), \
         patch.object(eng, "_append_signal_log"), \
         patch.object(eng, "_persist_vwap_shadow_observation"), \
         patch.object(eng, "_get_expiry", side_effect=fake_expiry), \
         patch.object(engine.fetcher, "fetch_vix", return_value=15.0), \
         patch.object(engine.fetcher, "get_index_data", side_effect=lambda k: valid_snap if k == "NIFTY" else {}), \
         patch.object(engine.fetcher, "get_option_premium", return_value=160.0):
        engine.run_scan("14:35 Cutoff Check", stealth_mode=True)
        assert engine.monitor.register.call_count == 0, "14:35 should block new snipers!"
    print("   PASS")

    print("5. Testing midday chop block (12:00) on scheduled, live, & strong breakout...")
    engine.monitor.register.reset_mock()
    engine._last_sniper.clear()
    with patch.object(eng, "_ist_now", return_value=fake_ist(12, 0)), \
         patch.object(eng, "_to_vip"), \
         patch.object(eng, "_persist_vwap_shadow_observation"), \
         patch.object(engine.fetcher, "fetch_vix", return_value=15.0), \
         patch.object(engine.fetcher, "get_index_data", side_effect=lambda k: valid_snap if k == "NIFTY" else {}):
        res_noon = engine.run_scan("12:00 Chop Check", stealth_mode=True)
        assert res_noon[0]["signal_type"] == "HOLD", "12:00 scheduled scan was not downgraded to HOLD!"
        assert engine.monitor.register.call_count == 0
        bk_ok = engine._register_strong_breakout_signal(
            "NIFTY", eng.INDICES["NIFTY"], valid_snap, candles, 24120.0,
            {"direction": "UP", "level": 24100.0, "volume_ratio": 2.0, "body_ratio": 0.7}
        )
        assert bk_ok is False, "Strong breakout should be blocked at 12:00!"
    print("   PASS")

    print("6. Testing reversal -0.05 relaxation order bug fix...")
    engine = build_offline_engine()
    engine.monitor.get_active_direction = MagicMock(return_value="CE")
    engine._last_prob["BANKNIFTY"] = -0.10
    engine._trade_open_time["BANKNIFTY"] = time.time() - 1200  # > 15 mins open
    engine._last_reversal_time["BANKNIFTY"] = 0

    bnf_snap = {"spot": 52000.0, "vix": 15.0, "candles": candles, "_age_s": 5.0}
    rev_analysis = {
        "prob": -0.65, "direction": "PE", "signal_type": "SNIPER", "atr": 50.0,
        "regime": "BEAR", "vol_surge": True, "bull_candle": False, "bear_candle": True,
        "swing": {}, "breakdown": {"rsi": 40.0, "vix": 15.0},
    }
    with patch.object(eng, "_ist_now", return_value=fake_ist(10, 5)), \
         patch.object(eng, "_to_vip"), \
         patch.object(eng, "_append_signal_log"), \
         patch.object(eng, "_get_expiry", side_effect=fake_expiry), \
         patch.object(eng, "select_optional_itm_strike", side_effect=lambda f, i, s, d, e: {"strike": s}), \
         patch.object(eng.TechnicalBrain, "analyze", return_value=rev_analysis), \
         patch.object(engine.fetcher, "fetch_vix", return_value=15.0), \
         patch.object(engine.fetcher, "get_index_data", side_effect=lambda k: bnf_snap if k == "BANKNIFTY" else {}), \
         patch.object(engine.fetcher, "get_option_premium", return_value=320.0):
        engine.run_scan("10:05 Reversal Test", stealth_mode=True)
        assert engine.monitor.close_active_reversal.called, "Reversal did not close old CE!"
        assert engine.monitor.register.call_count == 1, "Reversal did not enter new PE at 0.65 (floor 0.63)!"
    print("   PASS")

    print("7. Testing live scanner floor (0.65) & BANKNIFTY Jackpot >= 180...")
    engine = build_offline_engine()
    engine._last_live_check = 0
    engine._last_scheduled_scan = 0
    live_analysis = {
        "prob": 0.72, "direction": "CE", "signal_type": "SNIPER", "atr": 40.0,
        "regime": "BULL", "vol_surge": True, "bull_candle": True, "bear_candle": False,
        "swing": {}, "breakdown": {"rsi": 62.0, "vix": 15.0},
    }
    with patch.object(eng, "_ist_now", return_value=fake_ist(10, 10)), \
         patch.object(eng, "_to_vip"), \
         patch.object(eng, "_append_signal_log"), \
         patch.object(eng, "_get_expiry", side_effect=fake_expiry), \
         patch.object(eng, "select_optional_itm_strike", side_effect=lambda f, i, s, d, e: {"strike": s}), \
         patch.object(eng.TechnicalBrain, "analyze", return_value=live_analysis), \
         patch.object(engine.fetcher, "get_index_data", side_effect=lambda k: bnf_snap if k == "BANKNIFTY" else {}), \
         patch.object(engine.fetcher, "get_option_premium", return_value=310.0):
        engine._live_sniper_check()
        assert engine.monitor.register.call_count == 1
        registered_sig = engine.monitor.register.call_args[0][0]
        assert registered_sig.signal_type == "JACKPOT", f"Expected JACKPOT at 180 pts, got {registered_sig.signal_type}"
    print("   PASS")

    print("8. Testing Fmt.vip_no_setup threshold text & daily reset...")
    msg = eng.Fmt.vip_no_setup(eng.INDICES["BANKNIFTY"], "HOLD", 0.64, vix=15.0, threshold=0.71)
    assert "0.71" in msg and "0.55" not in msg, f"Wrong threshold in message: {msg}"

    engine._date = "2026-11-24"
    engine._last_strong_breakout["NIFTY"] = time.time()
    engine._reset_if_new_day()
    assert engine._last_strong_breakout == {}, "_reset_if_new_day() did not clear _last_strong_breakout!"
    print("   PASS")

    print("\nALL 8 OFFLINE TESTS PASSED!")


if __name__ == "__main__":
    run_all_tests()