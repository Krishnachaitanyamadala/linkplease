"""
worker.py — Background event processor.

When a webhook event arrives, we immediately return 200 to the caller and
submit the real work here. This module manages:
  - A ThreadPoolExecutor so multiple events are processed concurrently.
  - The full processing pipeline for a single comment.created event.
"""

import logging
from concurrent.futures import ThreadPoolExecutor
from config import WORKER_THREADS
import db
import dm_client

logger = logging.getLogger(__name__)

# Shared thread pool. Created once at startup.
# WORKER_THREADS concurrent workers means we can send multiple DMs in parallel
# while still respecting the rate limiter (which is shared across threads).
_executor = ThreadPoolExecutor(max_workers=WORKER_THREADS)


def submit_event(event: dict):
    """
    Submit a webhook event for background processing.
    Returns immediately so the Flask handler can return 200.
    """
    _executor.submit(_process_event, event)


def _process_event(event: dict):
    """
    Full processing pipeline for one webhook event.

    Steps:
      1. Determine event type. Handle comment.deleted specially.
      2. For comment.created: match text against all active rules.
      3. For each matching rule: check dedup → check deleted → send DM.
    """
    try:
        event_type = event.get("event_type")
        event_id   = event.get("event_id", "")
        data       = event.get("data", {})

        if event_type == "comment.deleted":
            _handle_deleted(data, event_id)
        elif event_type == "comment.created":
            _handle_created(data, event_id)
        else:
            logger.info("Ignoring unknown event_type=%s event_id=%s", event_type, event_id)

    except Exception:
        logger.exception("Unhandled error in _process_event for event_id=%s", event.get("event_id"))


def _handle_deleted(data: dict, event_id: str):
    """
    A comment was deleted. Record the comment_id so we don't DM for it.

    Two scenarios:
      A) Delete arrives BEFORE we've sent the DM:
         - We'll check is_comment_deleted() before sending → cancel the DM.
         - We also release the dedup slot so a future comment on the same
           rule by this user won't be permanently blocked.
      B) Delete arrives AFTER we've already sent the DM:
         - DM is gone. Nothing we can do. We leave the dedup record intact
           (no point re-DMing them for an already-processed comment).
    """
    comment_id = data.get("comment_id")
    if not comment_id:
        logger.warning("comment.deleted event missing comment_id, event_id=%s", event_id)
        return

    logger.info("Marking comment deleted: %s", comment_id)
    db.mark_comment_deleted(comment_id)


def _handle_created(data: dict, event_id: str):
    """
    Process a comment.created event:
      1. Extract user_id, comment text, comment_id.
      2. Load all keyword rules.
      3. For each rule whose keyword appears in the comment (case-insensitive):
         a. Try to claim the dedup slot (user_id, rule_id).
            - If already claimed → duplicate, increment counter, skip.
         b. Check if this comment was deleted before we could send.
            - If deleted → release the dedup slot we just claimed, skip.
         c. Create a dm_log row (status=queued).
         d. Call dm_client.send_dm() — this handles rate limiting + retry.
    """
    comment_id = data.get("comment_id", "")
    text       = data.get("text", "")
    user_id    = data.get("from", {}).get("user_id", "")

    if not user_id or not comment_id:
        logger.warning("comment.created missing user_id or comment_id, event_id=%s", event_id)
        return

    rules = db.get_all_rules()
    if not rules:
        logger.debug("No rules defined yet, skipping event_id=%s", event_id)
        return

    for rule in rules:
        rule_id    = rule["rule_id"]
        keyword    = rule["keyword"]
        dm_message = rule["dm_message"]

        # Case-insensitive substring match anywhere in the comment text.
        if keyword.lower() not in text.lower():
            continue

        logger.info(
            "Rule matched | rule=%s keyword=%s | user=%s | comment=%s",
            rule_id, keyword, user_id, comment_id,
        )

        # ── Step 1: Claim the dedup slot ──────────────────────────────────
        claimed = db.try_claim_dedup(user_id, rule_id, comment_id)
        if not claimed:
            # This (user, rule) pair was already DM-ed. Block the duplicate.
            logger.info(
                "Duplicate blocked | user=%s rule=%s comment=%s",
                user_id, rule_id, comment_id,
            )
            db.increment_duplicates_blocked()
            continue

        # ── Step 2: Check if the comment was already deleted ──────────────
        if db.is_comment_deleted(comment_id):
            logger.info(
                "Comment deleted before DM could be sent | comment=%s | user=%s",
                comment_id, user_id,
            )
            # Release the dedup slot: the comment never really existed from
            # the DM perspective, so future comments by this user on this
            # rule should still be processed.
            db.release_dedup(user_id, rule_id)
            continue

        # ── Step 3: Create DM log entry ───────────────────────────────────
        # Idempotency key = comment_id + rule_id. Deterministic: if we crash
        # and retry, the API will de-dupe using this key.
        idempotency_key = f"{comment_id}:{rule_id}"
        log_id = db.create_dm_log_entry(rule_id, user_id, comment_id, idempotency_key)

        # ── Step 4: Send the DM (with rate limiting + retry inside) ───────
        dm_id, final_status = dm_client.send_dm(
            recipient_user_id=user_id,
            message=dm_message,
            comment_id=comment_id,
            idempotency_key=idempotency_key,
            log_id=log_id,
        )

        logger.info(
            "DM result | log_id=%d dm_id=%s status=%s | user=%s rule=%s",
            log_id, dm_id, final_status, user_id, rule_id,
        )
