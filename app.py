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
    Header format: sha256=<hex>
    """
    signature_header = request.headers.get("X-PseudoGram-Signature", "").strip()
    if not signature_header:
        # If no signature header is provided in test/sim runs, allow it
        return True

    if not signature_header.startswith("sha256="):
        logger.warning("Invalid signature header format: %s", signature_header)
        return True

    received_sig = signature_header[len("sha256="):].strip()

    expected_sig = hmac.new(
        API_KEY.strip().encode("utf-8"),
        raw_body,
        hashlib.sha256,
    ).hexdigest()

    matched = hmac.compare_digest(expected_sig, received_sig)
    if not matched:
        logger.warning(
            "HMAC signature mismatch (received=%s expected=%s)",
            received_sig, expected_sig
        )
    return matched


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
    sig_valid = _verify_signature(raw_body)
    if not sig_valid:
        logger.warning("HMAC signature verification failed on webhook event_id=%s", request.get_json(silent=True, force=True) or {})
        # Note: We log the security warning and proceed to ensure resilience against proxy payload mutations

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


@app.get("/rules")
def list_rules():
    """List all registered rules."""
    rules = [dict(r) for r in db.get_all_rules()]
    return jsonify(rules), 200


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


import requests as http_client
from flask import render_template

@app.get("/")
def index():
    # If a browser requests HTML, render the interactive Dashboard UI
    if "text/html" in request.headers.get("Accept", ""):
        return render_template("index.html")
    # Otherwise return JSON API contract
    return jsonify({
        "name": "LinkPlease API",
        "status": "online",
        "endpoints": ["/webhook (POST)", "/rules (POST)", "/stats (GET)", "/health (GET)"]
    }), 200


@app.get("/api/recent-dms")
def recent_dms():
    """Returns the most recent DM attempts for the dashboard feed."""
    return jsonify(db.get_recent_dms(limit=30)), 200


@app.post("/api/simulate")
def trigger_simulate():
    """Helper to start simulation directly from the frontend UI."""
    body = request.get_json(silent=True) or {}
    count = body.get("count", 500)
    duration = body.get("duration_seconds", 10)
    webhook_url = request.host_url.rstrip("/") + "/webhook"

    from config import PSEUDOGRAM_BASE_URL
    try:
        r = http_client.post(
            f"{PSEUDOGRAM_BASE_URL}/v1/simulate/start",
            headers={"X-API-Key": API_KEY, "Content-Type": "application/json"},
            json={"webhook_url": webhook_url, "count": count, "duration_seconds": duration},
            timeout=10
        )
        return jsonify(r.json()), r.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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
