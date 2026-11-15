"""Refresh Angel One SmartAPI sessions for the four independent accounts.

Account mapping:
  A -> NIFTY      (ANGEL_API_KEY,   ANGEL_CLIENT_ID,   ANGEL_PASSWORD,   ANGEL_TOTP)
  B -> BANKNIFTY   (ANGEL_API_KEY_B, ANGEL_CLIENT_ID_B, ANGEL_PASSWORD_B, ANGEL_TOTP_B)
  C -> FINNIFTY    (ANGEL_API_KEY_C, ANGEL_CLIENT_ID_C, ANGEL_PASSWORD_C, ANGEL_TOTP_C)
  D -> SENSEX      (ANGEL_API_KEY_D, ANGEL_CLIENT_ID_D, ANGEL_PASSWORD_D, ANGEL_TOTP_D)

Usage:
  python scripts/auto_refresh_tokens.py --account A
  python scripts/auto_refresh_tokens.py --account B
  python scripts/auto_refresh_tokens.py --all

Tokens are written to account-specific variables in .env. No credentials or
returned tokens are printed to the console.
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Dict, Iterable, Tuple

import pyotp

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
ENV_FILE = PROJECT_ROOT / ".env"
LOCK_FILE = PROJECT_ROOT / ".env.angel_token_refresh.lock"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from scripts.env_loader import env
except Exception as exc:  # pragma: no cover - project-specific import
    raise RuntimeError(f"Could not load scripts.env_loader.env: {exc}") from exc

try:
    from SmartApi import SmartConnect
except ImportError as exc:  # pragma: no cover - environment-specific
    raise SystemExit("smartapi-python is not installed in this environment.") from exc


ACCOUNT_CONFIG: Dict[str, Dict[str, str]] = {
    "A": {
        "name": "NIFTY",
        "api_key": "ANGEL_API_KEY",
        "client_id": "ANGEL_CLIENT_ID",
        "password": "ANGEL_PASSWORD",
        "totp": "ANGEL_TOTP",
        "jwt_token": "ANGEL_JWT_TOKEN",
        "feed_token": "ANGEL_FEED_TOKEN",
    },
    "B": {
        "name": "BANKNIFTY",
        "api_key": "ANGEL_API_KEY_B",
        "client_id": "ANGEL_CLIENT_ID_B",
        "password": "ANGEL_PASSWORD_B",
        "totp": "ANGEL_TOTP_B",
        "jwt_token": "ANGEL_JWT_TOKEN_B",
        "feed_token": "ANGEL_FEED_TOKEN_B",
    },
    "C": {
        "name": "FINNIFTY",
        "api_key": "ANGEL_API_KEY_C",
        "client_id": "ANGEL_CLIENT_ID_C",
        "password": "ANGEL_PASSWORD_C",
        "totp": "ANGEL_TOTP_C",
        "jwt_token": "ANGEL_JWT_TOKEN_C",
        "feed_token": "ANGEL_FEED_TOKEN_C",
    },
    "D": {
        "name": "SENSEX",
        "api_key": "ANGEL_API_KEY_D",
        "client_id": "ANGEL_CLIENT_ID_D",
        "password": "ANGEL_PASSWORD_D",
        "totp": "ANGEL_TOTP_D",
        "jwt_token": "ANGEL_JWT_TOKEN_D",
        "feed_token": "ANGEL_FEED_TOKEN_D",
    },
}


def _read_env_value(key: str) -> str:
    """Read through the project's env loader, then process environment."""
    value = env(key)
    if value is None or str(value).strip() == "":
        value = os.environ.get(key)
    return str(value).strip() if value is not None else ""


def _quote_env(value: str) -> str:
    """Quote a .env value safely without printing it."""
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


class _FileLock:
    """Small cross-platform exclusive lock for concurrent token writers."""
    def __init__(self, path: Path, timeout: float = 20.0):
        self.path = path
        self.timeout = timeout
        self.handle = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                self.handle = open(self.path, "a+b")
                if os.name == "nt":
                    import msvcrt
                    self.handle.seek(0)
                    # Ensure there is one byte to lock.
                    if self.path.stat().st_size == 0:
                        self.handle.write(b"0")
                        self.handle.flush()
                    self.handle.seek(0)
                    msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self
            except (OSError, BlockingIOError):
                if self.handle:
                    self.handle.close()
                    self.handle = None
                if time.monotonic() >= deadline:
                    raise TimeoutError("Timed out waiting for the .env token-write lock")
                time.sleep(0.2)

    def __exit__(self, exc_type, exc, tb):
        if not self.handle:
            return
        try:
            if os.name == "nt":
                import msvcrt
                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        finally:
            self.handle.close()


def update_env_tokens(jwt_token: str, feed_token: str, account: str) -> None:
    """Atomically update only the selected account's token keys in .env."""
    if not ENV_FILE.exists():
        raise FileNotFoundError(f".env file not found: {ENV_FILE}")

    cfg = ACCOUNT_CONFIG[account]
    replacements = {
        cfg["jwt_token"]: _quote_env(jwt_token),
        cfg["feed_token"]: _quote_env(feed_token),
    }

    with _FileLock(LOCK_FILE):
        original = ENV_FILE.read_text(encoding="utf-8").splitlines()
        output = []
        seen = set()

        for line in original:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in line:
                output.append(line)
                continue
            key = line.split("=", 1)[0].strip()
            if key in replacements:
                if key not in seen:
                    output.append(f"{key}={replacements[key]}")
                    seen.add(key)
                # Drop duplicate definitions for this exact token key.
            else:
                output.append(line)

        for key, value in replacements.items():
            if key not in seen:
                output.append(f"{key}={value}")

        content = "\n".join(output) + "\n"
        fd, temp_name = tempfile.mkstemp(prefix=".env.tokens.", suffix=".tmp", dir=str(PROJECT_ROOT))
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as temp_file:
                temp_file.write(content)
                temp_file.flush()
                os.fsync(temp_file.fileno())
            os.replace(temp_name, ENV_FILE)
        finally:
            if os.path.exists(temp_name):
                os.unlink(temp_name)


def refresh_account(account: str) -> bool:
    cfg = ACCOUNT_CONFIG[account]
    keys = ("api_key", "client_id", "password", "totp")
    values = {name: _read_env_value(cfg[name]) for name in keys}
    missing = [cfg[name] for name in keys if not values[name]]
    if missing:
        print(f"[{cfg['name']}] Missing required .env variables: {', '.join(missing)}")
        return False

    try:
        print(f"[{cfg['name']}] Refreshing Angel One session using account {account}...")
        smart_api = SmartConnect(api_key=values["api_key"])
        current_totp = pyotp.TOTP(values["totp"]).now()
        response = smart_api.generateSession(
            values["client_id"], values["password"], current_totp
        )

        if not isinstance(response, dict) or response.get("status") is not True:
            message = response.get("message", "Unknown login failure") if isinstance(response, dict) else "Invalid API response"
            error_code = response.get("errorcode") if isinstance(response, dict) else None
            print(f"[{cfg['name']}] Login failed: {message}" + (f" (errorcode={error_code})" if error_code else ""))
            return False

        session_data = response.get("data") or {}
        jwt = session_data.get("jwtToken")
        feed = session_data.get("feedToken")
        if not jwt or not feed:
            print(f"[{cfg['name']}] Login response did not contain both jwtToken and feedToken.")
            return False

        # Preserve the original script's Bearer format for the JWT variable.
        jwt_value = str(jwt)
        if not jwt_value.lower().startswith("bearer "):
            jwt_value = f"Bearer {jwt_value}"

        update_env_tokens(jwt_value, str(feed), account)
        print(f"[{cfg['name']}] Tokens refreshed and saved to account-specific .env keys.")
        try:
            smart_api.terminateSession(values["client_id"])
        except Exception:
            pass
        return True

    except Exception as exc:
        # Exception text can contain request details; don't print credentials/tokens.
        print(f"[{cfg['name']}] Refresh failed: {type(exc).__name__}: {exc}")
        return False


def parse_args(argv: Iterable[str] | None = None):
    parser = argparse.ArgumentParser(description="Refresh Angel One tokens for a selected collector account.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--account", choices=tuple(ACCOUNT_CONFIG), help="A=NIFTY, B=BANKNIFTY, C=FINNIFTY, D=SENSEX")
    group.add_argument("--all", action="store_true", help="Refresh all four accounts sequentially")
    return parser.parse_args(argv)


def main() -> int:
    args = parse_args()
    accounts = list(ACCOUNT_CONFIG) if args.all else [args.account]
    results = [refresh_account(account) for account in accounts]
    failed = [account for account, ok in zip(accounts, results) if not ok]
    if failed:
        print(f"Token refresh failed for account(s): {', '.join(failed)}")
        return 1
    print("All requested Angel One account token refreshes completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
