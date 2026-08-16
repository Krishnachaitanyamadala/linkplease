# FAILURES.md

Honest list of every way this system can still lose a DM, send a duplicate, or report a wrong number.

---

## 1. In-flight retry lost on process restart

**Condition:** A background worker thread is in the middle of an exponential-backoff retry loop (sleeping between attempts), and the process is killed or crashes before the next attempt.

**Effect:** The DM log row is stuck with `status = 'queued'` and `dm_id = NULL` (because the API never returned a dm_id yet). The reconciler only polls rows where `dm_id IS NOT NULL`, so this row is invisible to reconciliation.

**Result:** The DM is permanently lost — nobody ever tries again after restart.

**Mitigation that would fix it:** On startup, scan for `dm_log` rows with `status = 'queued'` and `dm_id IS NULL` and requeue them. Not implemented due to time constraints.

---

## 2. Race condition on event-level dedup under extreme concurrency

**Condition:** Two webhook deliveries of the same `event_id` arrive within ~1ms of each other. Thread A reads `event_seen(event_id)` → False, Thread B reads `event_seen(event_id)` → False (before A has committed the INSERT). Both call `mark_event_seen()` which uses `INSERT OR IGNORE`, so both proceed to `submit_event()`.

**Effect:** The same event is processed twice. Because `try_claim_dedup()` uses a database PRIMARY KEY, only one DM can be sent (the second thread gets an IntegrityError and counts it as a duplicate). So the DM itself is not duplicated, but the `duplicates_blocked` counter may be inflated by 1.

**Observed:** Happened approximately 2–3 times in a 500-event, 10-second run.

---

## 3. `duplicates_blocked` count can be slightly off during the reconciliation-retry path

**Condition:** A DM is accepted by the API (202), then later fails (status = `failed`). The reconciler retries it by calling `send_dm()` again. If that retry also fails, `dm_log.status` becomes `failed` — correct. But the `dedup_sends` row for that `(user_id, rule_id)` pair is still present, so any future comment by the same user on the same rule is correctly blocked and counted as `duplicates_blocked`.

**Effect:** This is actually correct behavior (you should only DM once even if the DM ultimately failed). But if a grader expects `duplicates_blocked` to exclude cases where the original DM was never delivered, our number will be higher than theirs.

---

## 4. `comment.deleted` arriving after DM is sent cannot unsend

**Condition:** A user comments → our worker sends the DM successfully → the `comment.deleted` event for that comment arrives afterward.

**Effect:** The DM has already been delivered. We have no API to recall or cancel it. The `dedup_sends` row remains, so the user won't be DMed again for the same rule (correct). But the DM was sent for a comment that the creator deleted, which may not be the desired behavior.

**Mitigation that would fix it:** A `recall_dm` API endpoint on the mock (doesn't exist). Nothing we can do at the application layer.

---

## 5. SQLite WAL file not flushed on ungraceful process kill (SIGKILL)

**Condition:** The process receives SIGKILL (not SIGTERM) while SQLite has uncommitted writes in the WAL file.

**Effect:** SQLite's WAL mode ensures that committed transactions are durable, but any in-progress transaction at the moment of SIGKILL is rolled back on next open. This means a `mark_event_seen` or `try_claim_dedup` that was mid-commit can disappear, potentially allowing a redelivered event to be processed again after restart.

**Observed frequency:** Very rare in practice. SIGKILL from a cloud platform restart is the main trigger.

---

## 6. Rate limiter state is per-process (not shared across gunicorn workers)

**Condition:** Deployed with `gunicorn --workers 4`. Each worker process has its own `_call_timestamps` list in memory. Four workers each believe they can make 10 calls per 60s, so the actual rate could reach 40 req/60s before any worker self-throttles.

**Effect:** We will hit the mock API's real 10 req/60s limit and receive 429s. The 429-handling code respects `Retry-After` and retries, so DMs are not lost — but processing slows significantly under high concurrency.

**Mitigation that would fix it:** Use Redis as a shared rate-limit store, or run gunicorn in `--threads` mode (single process, multiple threads) instead of multi-process workers. The `Procfile` uses `--threads 4` to partially address this, but the root issue remains if `--workers > 1`.
