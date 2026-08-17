import os

# ── API credentials ──────────────────────────────────────────────────────────
# The API key obtained from POST /v1/keygen.
# In production set the PSEUDOGRAM_API_KEY environment variable.
API_KEY = os.environ.get(
    "PSEUDOGRAM_API_KEY",
    "a3Jpc2huYWNoYWl0YW55YW1hZGFsYTVAZ21haWwuY29t.10a9fad05ae30400760e",
).strip()

PSEUDOGRAM_BASE_URL = "https://pseudogram-api.onrender.com"

# ── Rate-limit settings ──────────────────────────────────────────────────────
# The mock API allows 10 DM-send requests per rolling 60-second window.
RATE_LIMIT_CALLS = 10          # max calls
RATE_LIMIT_PERIOD = 60         # per N seconds

# ── Retry settings ──────────────────────────────────────────────────────────
MAX_DM_ATTEMPTS = 5            # give up after this many failed attempts
RETRY_BASE_DELAY = 1.0         # initial backoff in seconds (doubles each try)

# ── Reconciliation ──────────────────────────────────────────────────────────
RECONCILE_INTERVAL = 5         # seconds between reconciliation sweeps

# ── Background workers ───────────────────────────────────────────────────────
WORKER_THREADS = 8             # thread-pool size for processing webhook events

# ── Database ─────────────────────────────────────────────────────────────────
DATABASE_PATH = os.environ.get("DATABASE_PATH", "linkplease.db")
