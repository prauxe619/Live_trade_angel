# scripts/env_loader.py
import os
from pathlib import Path
from dotenv import load_dotenv

# ----------------------------------------------------
# 🌟 FIX: Consolidate path check and load immediately 🌟
# ----------------------------------------------------
# Resolve project root reliably (assuming env_loader is in scripts/)
HERE = Path(__file__).resolve().parent
ROOT = HERE.parent

# Search for .env in the project root (most common location)
DOTENV_PATH = ROOT / ".env"

if not DOTENV_PATH.exists():
    # Fallback to check if it's placed inside the scripts directory
    DOTENV_PATH = HERE / ".env"

# Load the environment variables immediately when the module is imported
# This ensures os.getenv has the variables available for subsequent imports in other scripts.
load_dotenv(dotenv_path=str(DOTENV_PATH))


def env(key, default=None):
    """Return environment variable or default."""
    return os.getenv(key, default)

# ----------------------------------------------------