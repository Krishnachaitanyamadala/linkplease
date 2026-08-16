"""
app.py — Flask application entry point.

Routes:
  POST /webhook  — Receives comment events from the mock API.
  POST /rules    — Creates a keyword → DM-message rule.
  GET  /stats    — Returns live delivery statistics.

Part A: Webhook processing, rule creation, deduplication, retry.
Part B: HMAC signature verification, accurate stats from DB.
Part C: Delivery reconciliation (via reconciler.py), comment.deleted handling.
"""

import hashlib
import hmac
import logging
import uuid

from flask import Flask, request, jsonify, abort
from flask_cors import CORS

import db
import worker
import reconciler
from config import API_KEY

# ── Logging setup ─────────────────────────────────────────────────────────────
# Logs to stdout so they show up in the console and in any hosting platform's
# log viewer (Render, Railway, Fly.io all capture stdout).
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ── Flask app ─────────────────────────────────────────────────────────────────
app = Flask(__name__)
CORS(app)   # Allow cross-origin requests (useful if you add a frontend later)


# ── Startup ───────────────────────────────────────────────────────────────────

def startup():
    """Initialize DB tables and start background threads."""
    db.init_db()
    reconciler.start()
    logger.info("LinkPlease backend started")


# Run startup() at module import time so gunicorn workers also initialise.
# Using a flag to ensure it only runs once per process even if the module
# is imported multiple times (e.g., during testing).
_started = False
if not _started:
    _started = True
    startup()


# ── HMAC signature verification (Part B) ─────────────────────────────────────

def _verify_signature(raw_body: bytes) -> bool:
    """
    Verify the X-PseudoGram-Signature header.

    The mock API signs every webhook using HMAC-SHA256 with the API key as
    the secret, over the raw request body (bytes, before any JSON parsing).

    Header format:  sha256=<lowercase hex digest>

    We compute our own HMAC and compare with constant-time comparison
    (hmac.compare_digest) to prevent timing attacks.
    """
    signature_header = request.headers.get("X-PseudoGram-Signature", "")
    if not signature_header.startswith("sha256="):
        return False

    received_sig = signature_header[len("sha256="):]

    # Compute expected signature using our API key as the HMAC secret.
    # hmac.new(key, msg, digestmod) is the standard Python HMAC API.
    expected_sig = hmac.new(
        API_KEY.encode("utf-8"),
        raw_body,
        hashlib.sha256,
    ).hexdigest()

    # Constant-time comparison prevents timing-oracle attacks.
    return hmac.compare_digest(expected_sig, received_sig)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.post("/webhook")
def webhook():
    """
    Receive a comment event from the mock API.

    Contract:
      - Must return 200 within 5 seconds.
      - Do heavy lifting in the background (we use a ThreadPoolExecutor).

    Processing:
      1. Verify HMAC signature (Part B) — reject forged requests.
      2. Check if we've already processed this event_id — idempotent.
      3. Mark the event_id as seen.
      4. Submit the event to the background worker pool.
      5. Return 200 immediately.
    """
    # Read the raw body BEFORE calling request.json so the bytes are intact
    # for HMAC verification. Flask caches the body, so this is safe to do.
    raw_body = request.get_data()

    # ── Part B: Verify signature ──────────────────────────────────────────
    if not _verify_signature(raw_body):
        logger.warning("Rejected webhook: invalid HMAC signature")
        # Return 200 anyway to prevent the sender from retrying indefinitely.
        # (Some implementations return 401, but 200 is safer for a webhook.)
        return jsonify({"ok": False, "reason": "invalid signature"}), 200

    # Parse JSON after signature check.
    event = request.get_json(silent=True)
    if not event:
        logger.warning("Webhook body is not valid JSON")
        return jsonify({"ok": False, "reason": "invalid JSON"}), 200

    event_id   = event.get("event_id", "")
    event_type = event.get("event_type", "")

    # ── Idempotency: skip already-seen events ─────────────────────────────
    # The mock API redelivers ~8% of events. We check the event_id first.
    # mark_event_seen uses INSERT OR IGNORE, so concurrent threads are safe.
    if db.event_seen(event_id):
        logger.info("Duplicate event ignored | event_id=%s", event_id)
        return jsonify({"ok": True, "duplicate": True}), 200

    db.mark_event_seen(event_id)

    # ── Submit to background workers ──────────────────────────────────────
    worker.submit_event(event)
    logger.info("Event queued | event_id=%s type=%s", event_id, event_type)

    # Return 200 immediately — processing happens in the background.
    return jsonify({"ok": True}), 200


@app.post("/rules")
def create_rule():
    """
    Create a keyword → DM-message rule.

    Request body:
      { "keyword": "PRICE", "dm_message": "Here is the price list ..." }

    Response 201:
      { "rule_id": "<uuid>", "keyword": "PRICE", "dm_message": "..." }

    Keyword matching is case-insensitive substring match.
    """
    body = request.get_json(silent=True)
    if not body:
        return jsonify({"error": "invalid JSON"}), 400

    keyword    = body.get("keyword", "").strip()
    dm_message = body.get("dm_message", "").strip()

    if not keyword:
        return jsonify({"error": "keyword is required"}), 400
    if not dm_message:
        return jsonify({"error": "dm_message is required"}), 400

    # Generate a unique rule ID. UUID4 is random, collision-free in practice.
    rule_id = str(uuid.uuid4())
    db.insert_rule(rule_id, keyword, dm_message)

    logger.info("Rule created | rule_id=%s keyword=%s", rule_id, keyword)
    return jsonify({
        "rule_id":    rule_id,
        "keyword":    keyword,
        "dm_message": dm_message,
    }), 201


@app.get("/stats")
def stats():
    """
    Return live delivery statistics.

    Response:
      {
        "sent":               <int>,   # DMs confirmed delivered by the API
        "failed":             <int>,   # DMs we gave up on after retries
        "queued":             <int>,   # DMs waiting to send or pending reconciliation
        "duplicates_blocked": <int>    # DMs we correctly chose not to send (dedup)
      }

    Stats are computed from the database on every call — no cached counters
    that could drift. This means they're always accurate, even under load.
    """
    return jsonify(db.get_stats()), 200


# ── Health check (optional, useful for deployment platforms) ──────────────────

@app.get("/health")
def health():
    return jsonify({"ok": True}), 200


# ── Main entry point ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    # threaded=True: Flask handles each request in its own thread, which is
    # important so that a slow DM send in one worker doesn't block incoming
    # webhooks. In production, use gunicorn instead.
    app.run(host="0.0.0.0", port=5000, threaded=True, debug=False)
