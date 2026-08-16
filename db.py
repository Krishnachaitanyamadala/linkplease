"""
db.py — SQLite database helpers.

Why SQLite?
  - Persistent across process restarts (unlike in-memory dicts).
  - Single file, zero infrastructure.
  - The UNIQUE constraint on dedup_sends is enforced by the database engine,
    which means even under concurrent threads, only one INSERT can win.
    The other thread gets an IntegrityError and knows to skip the DM.
"""

import sqlite3
import threading
from config import DATABASE_PATH

# Each thread gets its own SQLite connection (SQLite connections are not
# thread-safe when shared). We use threading.local() for that.
_local = threading.local()


def get_conn() -> sqlite3.Connection:
    """Return the per-thread SQLite connection, creating it if needed."""
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        # Return rows as dict-like objects so we can do row["column"].
        _local.conn.row_factory = sqlite3.Row
        # Enable WAL mode: multiple readers + one writer can coexist without
        # blocking each other. Critical for throughput under 500-event load.
        _local.conn.execute("PRAGMA journal_mode=WAL")
        # Enforce foreign keys (good practice even if we don't have FKs here).
        _local.conn.execute("PRAGMA foreign_keys=ON")
    return _local.conn


def init_db():
    """Create all tables if they do not already exist. Called once at startup."""
    conn = get_conn()
    conn.executescript("""
        -- Keyword → DM rules created by the user via POST /rules.
        CREATE TABLE IF NOT EXISTS rules (
            rule_id     TEXT PRIMARY KEY,
            keyword     TEXT NOT NULL,
            dm_message  TEXT NOT NULL,
            created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        -- Every event_id we have processed. Used for webhook-level dedup.
        -- If the same event_id arrives twice we return 200 immediately without
        -- doing any work, so we never send a duplicate DM due to redelivery.
        CREATE TABLE IF NOT EXISTS seen_events (
            event_id      TEXT PRIMARY KEY,
            processed_at  DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        -- Tracks which (user, rule) pairs have already been DM-ed.
        -- The composite PRIMARY KEY is the dedup guarantee: the DB engine
        -- rejects any second INSERT for the same (user_id, rule_id), which
        -- is exactly what we want. We catch the IntegrityError and count it.
        CREATE TABLE IF NOT EXISTS dedup_sends (
            user_id     TEXT NOT NULL,
            rule_id     TEXT NOT NULL,
            comment_id  TEXT,
            created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, rule_id)
        );

        -- One row per DM we have attempted (or are about to attempt).
        -- dm_id is NULL until the mock API returns a dm_id.
        -- status: queued | delivered | failed
        CREATE TABLE IF NOT EXISTS dm_log (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            dm_id              TEXT,
            rule_id            TEXT NOT NULL,
            user_id            TEXT NOT NULL,
            comment_id         TEXT NOT NULL,
            idempotency_key    TEXT NOT NULL UNIQUE,
            status             TEXT NOT NULL DEFAULT 'queued',
            attempts           INTEGER DEFAULT 0,
            last_attempted_at  DATETIME,
            created_at         DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        -- Comment IDs that arrived with a comment.deleted event.
        -- We check this before sending a DM: if the comment was deleted,
        -- we cancel the send and free the dedup slot.
        CREATE TABLE IF NOT EXISTS deleted_comments (
            comment_id  TEXT PRIMARY KEY,
            deleted_at  DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        -- Simple counter table for duplicates_blocked stat.
        -- We use a single row here so we can do an atomic UPDATE.
        CREATE TABLE IF NOT EXISTS stats_counters (
            id                  INTEGER PRIMARY KEY CHECK (id = 1),
            duplicates_blocked  INTEGER DEFAULT 0
        );
        INSERT OR IGNORE INTO stats_counters (id, duplicates_blocked) VALUES (1, 0);
    """)
    conn.commit()


# ── Helper functions ──────────────────────────────────────────────────────────

def rule_exists(rule_id: str) -> bool:
    conn = get_conn()
    row = conn.execute("SELECT 1 FROM rules WHERE rule_id = ?", (rule_id,)).fetchone()
    return row is not None


def insert_rule(rule_id: str, keyword: str, dm_message: str):
    conn = get_conn()
    conn.execute(
        "INSERT INTO rules (rule_id, keyword, dm_message) VALUES (?, ?, ?)",
        (rule_id, keyword, dm_message),
    )
    conn.commit()


def get_all_rules() -> list[sqlite3.Row]:
    conn = get_conn()
    return conn.execute("SELECT rule_id, keyword, dm_message FROM rules").fetchall()


def event_seen(event_id: str) -> bool:
    """Return True if we have already processed this event_id."""
    conn = get_conn()
    row = conn.execute("SELECT 1 FROM seen_events WHERE event_id = ?", (event_id,)).fetchone()
    return row is not None


def mark_event_seen(event_id: str):
    conn = get_conn()
    # INSERT OR IGNORE: if another thread already wrote it, that's fine.
    conn.execute("INSERT OR IGNORE INTO seen_events (event_id) VALUES (?)", (event_id,))
    conn.commit()


def is_comment_deleted(comment_id: str) -> bool:
    conn = get_conn()
    row = conn.execute(
        "SELECT 1 FROM deleted_comments WHERE comment_id = ?", (comment_id,)
    ).fetchone()
    return row is not None


def mark_comment_deleted(comment_id: str):
    conn = get_conn()
    conn.execute(
        "INSERT OR IGNORE INTO deleted_comments (comment_id) VALUES (?)", (comment_id,)
    )
    conn.commit()


def try_claim_dedup(user_id: str, rule_id: str, comment_id: str) -> bool:
    """
    Attempt to INSERT a (user_id, rule_id) row.
    Returns True if the INSERT succeeded (we own the send slot).
    Returns False if a row already existed (duplicate — skip this DM).
    
    The PRIMARY KEY constraint makes this atomic: even with many threads
    racing, exactly one INSERT wins.
    """
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO dedup_sends (user_id, rule_id, comment_id) VALUES (?, ?, ?)",
            (user_id, rule_id, comment_id),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        # PK violation → already exists → duplicate
        return False


def release_dedup(user_id: str, rule_id: str):
    """
    Remove a dedup row so the slot is freed (used when a comment is deleted
    before the DM was sent, so we won't block future comments on the same rule).
    """
    conn = get_conn()
    conn.execute(
        "DELETE FROM dedup_sends WHERE user_id = ? AND rule_id = ?",
        (user_id, rule_id),
    )
    conn.commit()


def increment_duplicates_blocked():
    conn = get_conn()
    conn.execute(
        "UPDATE stats_counters SET duplicates_blocked = duplicates_blocked + 1 WHERE id = 1"
    )
    conn.commit()


def create_dm_log_entry(rule_id: str, user_id: str, comment_id: str, idempotency_key: str) -> int:
    """Insert a new DM log row with status='queued'. Returns the row id."""
    conn = get_conn()
    cur = conn.execute(
        """INSERT INTO dm_log
               (rule_id, user_id, comment_id, idempotency_key, status, attempts)
           VALUES (?, ?, ?, ?, 'queued', 0)""",
        (rule_id, user_id, comment_id, idempotency_key),
    )
    conn.commit()
    return cur.lastrowid


def update_dm_log(log_id: int, dm_id: str | None, status: str, attempts: int):
    conn = get_conn()
    conn.execute(
        """UPDATE dm_log
              SET dm_id = ?, status = ?, attempts = ?,
                  last_attempted_at = CURRENT_TIMESTAMP
            WHERE id = ?""",
        (dm_id, status, attempts, log_id),
    )
    conn.commit()


def get_queued_dm_logs() -> list[sqlite3.Row]:
    """Return all DM log rows that are still in 'queued' state and have a dm_id."""
    conn = get_conn()
    return conn.execute(
        "SELECT id, dm_id, rule_id, user_id, comment_id, idempotency_key, attempts FROM dm_log WHERE status = 'queued' AND dm_id IS NOT NULL"
    ).fetchall()


def get_stats() -> dict:
    conn = get_conn()
    sent    = conn.execute("SELECT COUNT(*) FROM dm_log WHERE status = 'delivered'").fetchone()[0]
    failed  = conn.execute("SELECT COUNT(*) FROM dm_log WHERE status = 'failed'").fetchone()[0]
    queued  = conn.execute("SELECT COUNT(*) FROM dm_log WHERE status = 'queued'").fetchone()[0]
    blocked = conn.execute("SELECT duplicates_blocked FROM stats_counters WHERE id = 1").fetchone()[0]
    return {
        "sent": sent,
        "failed": failed,
        "queued": queued,
        "duplicates_blocked": blocked,
    }
