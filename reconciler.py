"""
reconciler.py — Delivery status reconciliation (Part C).

Problem: POST /v1/dm/send returns 202 (Accepted), not "delivered".
Roughly 15% of accepted DMs are later marked failed. We only find out
by polling GET /v1/dm/{dm_id}.

This module runs a background thread that:
  1. Every RECONCILE_INTERVAL seconds, loads all dm_log rows with status='queued'
     that already have a dm_id (i.e., the API accepted them).
  2. For each, calls GET /v1/dm/{dm_id}.
  3. If status is 'delivered' or 'failed', updates the db row.
  4. If 'failed' and attempts < MAX_DM_ATTEMPTS: retry the DM send.

Note: GET /v1/dm/{dm_id} reads do NOT count against the rate limit.
"""

import time
import threading
import logging
from config import RECONCILE_INTERVAL, MAX_DM_ATTEMPTS
import db
import dm_client

logger = logging.getLogger(__name__)

_thread: threading.Thread | None = None
_stop_event = threading.Event()


def start():
    """Start the reconciliation background thread. Called once at app startup."""
    global _thread
    _stop_event.clear()
    _thread = threading.Thread(target=_reconcile_loop, name="Reconciler", daemon=True)
    _thread.start()
    logger.info("Reconciler started (interval=%ds)", RECONCILE_INTERVAL)


def stop():
    """Signal the reconciliation thread to stop."""
    _stop_event.set()


def _reconcile_loop():
    """Main reconciliation loop. Runs forever until stop() is called."""
    while not _stop_event.is_set():
        try:
            _run_sweep()
        except Exception:
            logger.exception("Error in reconciliation sweep")
        # Wait RECONCILE_INTERVAL seconds, but wake up early if stop() is called.
        _stop_event.wait(timeout=RECONCILE_INTERVAL)


def _run_sweep():
    """
    One reconciliation sweep:
      - Load all queued DM log rows that have a dm_id from the API.
      - Poll each one's status.
      - Update the db, or retry if failed.
    """
    rows = db.get_queued_dm_logs()
    if not rows:
        return

    logger.info("Reconciliation sweep: checking %d queued DMs", len(rows))

    for row in rows:
        log_id          = row["id"]
        dm_id           = row["dm_id"]
        rule_id         = row["rule_id"]
        user_id         = row["user_id"]
        comment_id      = row["comment_id"]
        idempotency_key = row["idempotency_key"]
        attempts        = row["attempts"]

        status = dm_client.check_dm_status(dm_id)
        if status is None:
            # Network error — will check again next sweep.
            continue

        if status == "delivered":
            logger.info("DM delivered | dm_id=%s log_id=%d", dm_id, log_id)
            db.update_dm_log(log_id, dm_id, "delivered", attempts)

        elif status == "failed":
            logger.warning(
                "DM failed after API accepted | dm_id=%s log_id=%d attempts=%d",
                dm_id, log_id, attempts,
            )

            if attempts < MAX_DM_ATTEMPTS:
                # Retry: send again with the same idempotency key.
                # The API will return the original dm_id if we already sent it,
                # so the retry is safe.
                logger.info("Retrying DM | log_id=%d", log_id)

                # Check if the comment was later deleted before retrying.
                if db.is_comment_deleted(comment_id):
                    logger.info(
                        "Comment deleted, cancelling retry | comment=%s log_id=%d",
                        comment_id, log_id,
                    )
                    db.update_dm_log(log_id, dm_id, "failed", attempts)
                    continue

                # Load the rule to get the message.
                rules = {r["rule_id"]: r for r in db.get_all_rules()}
                rule = rules.get(rule_id)
                if not rule:
                    logger.error("Rule %s no longer exists, cannot retry DM", rule_id)
                    db.update_dm_log(log_id, dm_id, "failed", attempts)
                    continue

                new_dm_id, new_status = dm_client.send_dm(
                    recipient_user_id=user_id,
                    message=rule["dm_message"],
                    comment_id=comment_id,
                    idempotency_key=idempotency_key,
                    log_id=log_id,
                )
                logger.info(
                    "Retry result | log_id=%d new_dm_id=%s new_status=%s",
                    log_id, new_dm_id, new_status,
                )
            else:
                # Max attempts exhausted — give up.
                logger.error(
                    "Giving up on DM after reconciliation | dm_id=%s log_id=%d",
                    dm_id, log_id,
                )
                db.update_dm_log(log_id, dm_id, "failed", attempts)

        # If status == 'queued', the API hasn't settled yet — check again next sweep.
