"""Per-index live lot configuration."""

import os


INDEX_KEYS = {
    "NIFTY": "NIFTY",
    "BANKNIFTY": "BANKNIFTY",
    "FINNIFTY": "FINNIFTY",
    "SENSEX": "SENSEX",
}


def configured_live_lots(index: str, fallback: int = 1) -> int:
    index_key = str(index).strip().upper().replace(" ", "").replace("-", "")

    if index_key not in INDEX_KEYS:
        raise ValueError(f"Unsupported index for live lot configuration: {index}")

    env_key = f"LIVE_LOTS_{INDEX_KEYS[index_key]}"
    raw = os.getenv(env_key)

    if raw is None:
        raw = os.getenv("TRADE_LOTS", str(fallback))

    try:
        lots = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{env_key} must be an integer") from exc

    if lots < 0:
        raise ValueError(f"{env_key} cannot be negative")

    return lots