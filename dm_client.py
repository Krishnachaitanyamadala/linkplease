"""
dm_client.py — Mock API client with rate limiting and retry.

The mock API:
  - Allows 10 DM-send requests per rolling 60-second window (429 if exceeded).
  - Returns 500 ~20% of the time (safe to retry).
  - Returns 202 Accepted (not delivered). ~15% of those later become failed.
  - Supports Idempotency-Key header so retrying the same key is safe.

This module owns:
  1. A token-bucket rate limiter (shared across all threads via a lock).
  2. Exponential-backoff retry logic for 500 and 429 responses.
  3. A DM-status-check function for the reconciliation loop.
"""

import time
import threading
import logging
import requests
from config import (
    API_KEY,
    PSEUDOGRAM_BASE_URL,
    RATE_LIMIT_CALLS,
    RATE_LIMIT_PERIOD,
    MAX_DM_ATTEMPTS,
    RETRY_BASE_DELAY,
)

logger = logging.getLogger(__name__)

# ── Token-bucket rate limiter ─────────────────────────────────────────────────
# We keep a list of timestamps of recent calls. Before each DM send we:
#   1. Acquire the lock (mutual exclusion across threads).
#   2. Drop timestamps older than RATE_LIMIT_PERIOD seconds.
#   3. If len(timestamps) >= RATE_LIMIT_CALLS: sleep until the oldest falls off.
#   4. Append now and release the lock.
# This is a simple sliding-window counter, accurate enough for our needs.

_rate_lock = threading.Lock()
_call_timestamps: list[float] = []   # epoch seconds of recent DM-send calls


def _acquire_rate_limit_token():
    """
    Block the calling thread until a rate-limit slot is available, then
    consume one slot. Thread-safe.
    """
    while True:
        with _rate_lock:
            now = time.monotonic()
            # Remove timestamps outside the current rolling window.
            cutoff = now - RATE_LIMIT_PERIOD
            while _call_timestamps and _call_timestamps[0] < cutoff:
                _call_timestamps.pop(0)

            if len(_call_timestamps) < RATE_LIMIT_CALLS:
                # Slot available: claim it and proceed.
                _call_timestamps.append(now)
                return
            else:
                # All slots used: compute how long until the oldest expires.
                sleep_for = (_call_timestamps[0] + RATE_LIMIT_PERIOD) - now

        # Sleep *outside* the lock so other threads can still check.
        time.sleep(max(sleep_for, 0.05))


# ── DM send ───────────────────────────────────────────────────────────────────

def send_dm(
    recipient_user_id: str,
    message: str,
    comment_id: str,
    idempotency_key: str,
    log_id: int,
) -> tuple[str | None, str]:
    """
    Attempt to send a DM, retrying on 500 and 429 up to MAX_DM_ATTEMPTS times.

    Returns (dm_id, final_status) where final_status is one of:
      'delivered_pending' — API accepted (202), delivery not yet confirmed
      'failed'            — gave up after retries or got a 400

    We return 'delivered_pending' rather than 'delivered' because a 202 only
    means the API accepted the message. The reconciler will later poll
    GET /v1/dm/{dm_id} to learn the true final status.
    """
    import db  # import here to avoid circular imports at module load time

    url = f"{PSEUDOGRAM_BASE_URL}/v1/dm/send"
    headers = {
        "X-API-Key": API_KEY,
        "Content-Type": "application/json",
        "Idempotency-Key": idempotency_key,  # Safe to retry same key
    }
    payload = {
        "recipient_user_id": recipient_user_id,
        "message": message,
        "comment_id": comment_id,
    }

    attempt = 0
    delay = RETRY_BASE_DELAY

    while attempt < MAX_DM_ATTEMPTS:
        attempt += 1
        logger.info(
            "DM send attempt %d/%d | log_id=%d | user=%s | idem=%s",
            attempt, MAX_DM_ATTEMPTS, log_id, recipient_user_id, idempotency_key,
        )

        # Consume one rate-limit slot (blocks if we're at the limit).
        _acquire_rate_limit_token()

        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=15)
        except requests.RequestException as exc:
            # Network error — treat like a 500 and retry.
            logger.warning("Network error on DM send (attempt %d): %s", attempt, exc)
            db.update_dm_log(log_id, None, "queued", attempt)
            if attempt < MAX_DM_ATTEMPTS:
                time.sleep(delay)
                delay *= 2
            continue

        if resp.status_code in (200, 202):
            # Accepted. The spec says 202 but the mock API returns 200 in practice.
            # We handle both. Extract dm_id and mark as queued (pending reconciliation).
            data = resp.json()
            dm_id = data.get("dm_id")
            logger.info("DM accepted | dm_id=%s | log_id=%d", dm_id, log_id)
            db.update_dm_log(log_id, dm_id, "queued", attempt)
            return dm_id, "queued"

        elif resp.status_code == 429:
            # Rate limited by the server (shouldn't happen if our limiter works,
            # but handle defensively). The Retry-After header tells us how long.
            retry_after = float(resp.headers.get("Retry-After", delay))
            logger.warning(
                "429 rate limited | log_id=%d | sleeping %.1fs", log_id, retry_after
            )
            db.update_dm_log(log_id, None, "queued", attempt)
            time.sleep(retry_after)
            # Don't count this against our attempt counter — it was our limiter
            # failing, not the DM failing. Decrement so the loop retries.
            attempt -= 1

        elif resp.status_code == 500:
            # Random server error — safe to retry with backoff.
            logger.warning("500 from API | log_id=%d | attempt %d", log_id, attempt)
            db.update_dm_log(log_id, None, "queued", attempt)
            if attempt < MAX_DM_ATTEMPTS:
                time.sleep(delay)
                delay *= 2

        elif resp.status_code == 400:
            # Malformed request — retrying won't help.
            logger.error("400 bad request | log_id=%d | body=%s", log_id, resp.text)
            db.update_dm_log(log_id, None, "failed", attempt)
            return None, "failed"

        else:
            logger.error(
                "Unexpected status %d | log_id=%d | body=%s",
                resp.status_code, log_id, resp.text,
            )
            db.update_dm_log(log_id, None, "queued", attempt)
            if attempt < MAX_DM_ATTEMPTS:
                time.sleep(delay)
                delay *= 2

    # Exhausted all attempts.
    logger.error("Giving up on DM | log_id=%d after %d attempts", log_id, attempt)
    db.update_dm_log(log_id, None, "failed", attempt)
    return None, "failed"


# ── DM status check ───────────────────────────────────────────────────────────

def check_dm_status(dm_id: str) -> str | None:
    """
    Poll GET /v1/dm/{dm_id} to get the current delivery status.
    Returns 'queued', 'delivered', 'failed', or None on network error.
    Note: reads do NOT count against the rate limit per the API spec.
    """
    url = f"{PSEUDOGRAM_BASE_URL}/v1/dm/{dm_id}"
    headers = {"X-API-Key": API_KEY}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            return resp.json().get("status")
        else:
            logger.warning("check_dm_status got %d for dm_id=%s", resp.status_code, dm_id)
            return None
    except requests.RequestException as exc:
        logger.warning("Network error checking DM status %s: %s", dm_id, exc)
        return None
