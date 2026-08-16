# LinkPlease — Tech Intern Assignment Submission

**Author:** Krishna Chaitanya Madala  
**Email:** krishnachaitanyamadala5@gmail.com  
**Parts Completed:** A + B + C

---

## What this is

A Flask backend that automates Instagram-style DMs. When someone comments a keyword on a creator's post, they automatically receive a DM with the configured message.

Built on top of the mock Pseudogram API, which is deliberately hostile: duplicate events, out-of-order delivery, ~20% 500 errors, ~15% silent DM failures, and a 10 req/60s rate limit.

---

## Architecture

```
POST /webhook  →  HMAC verify  →  dedup event_id  →  ThreadPoolExecutor
                                                            │
                                              ┌─────────────┘
                                              ▼
                                   Match rules (case-insensitive)
                                              │
                                   DB dedup (user_id, rule_id) PRIMARY KEY
                                              │
                                   Rate-limited DM send
                                   (token bucket, shared across threads)
                                              │
                                   Exponential backoff retry (5x max)
                                              │
                                   Reconciler loop (every 30s)
                                   polls GET /v1/dm/{dm_id}
                                   → retries silently failed DMs
```

**Persistence:** SQLite with WAL mode. Survives process restarts for all committed state.

---

## Parts

### Part A — Core
- `POST /rules` — create keyword → DM rules
- `POST /webhook` — receive events, process in background
- Dedup: `(user_id, rule_id)` PRIMARY KEY in SQLite — database enforces atomicity
- Retry: exponential backoff (1s, 2s, 4s, 8s, 16s), max 5 attempts
- No DM silently lost: every send attempt is logged to DB before the call

### Part B — Robustness
- HMAC-SHA256 signature verification (`X-PseudoGram-Signature` header)
- Constant-time comparison (`hmac.compare_digest`) prevents timing attacks
- Stats from live DB queries, never cached — always accurate

### Part C — Show-off
- Reconciliation: background thread polls delivery status every 30s, retries failed DMs
- `comment.deleted` handling: marks comment_id as deleted, cancels pending sends, releases dedup slot
- Rate limiter: sliding-window token bucket prevents exceeding 10 req/60s

---

## Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Set your API key (or it reads from config.py directly)
set PSEUDOGRAM_API_KEY=your_key_here

# Run
python app.py
```

Server starts on `http://localhost:5000`.

### Test it

```bash
# Create a rule
curl -X POST http://localhost:5000/rules \
  -H "Content-Type: application/json" \
  -d '{"keyword": "PRICE", "dm_message": "Here is our price list!"}'

# Check stats
curl http://localhost:5000/stats
```

---

## Deploying to Render

1. Push this repo to GitHub (make sure `.env` and `linkplease.db` are in `.gitignore`)
2. Create a new **Web Service** on [render.com](https://render.com)
3. Connect your GitHub repo
4. Set these settings:
   - **Build command:** `pip install -r requirements.txt`
   - **Start command:** `gunicorn app:app --workers 1 --threads 8 --bind 0.0.0.0:$PORT --timeout 120`
5. Add environment variable: `PSEUDOGRAM_API_KEY` = your key
6. Deploy

> ⚠️ Use `--workers 1 --threads 8` on Render's free tier (single instance). The rate limiter is in-process — multiple workers would split the budget.

---

## Key Design Decisions

### Why SQLite?
Zero infrastructure, survives restarts, and the PRIMARY KEY constraint on `dedup_sends(user_id, rule_id)` gives us atomic deduplication for free — no locks needed in application code.

### Why a token-bucket rate limiter instead of just handling 429s?
Proactive throttling means we never hit the API's limit in the first place. 429 handling exists as a fallback (the API might disagree with our count), but the goal is zero 429s.

### Why idempotency keys on every DM send?
`comment_id:rule_id` is a deterministic key. If we crash mid-retry, the next attempt sends the same key and the API returns the original `dm_id` — no duplicate DM is sent.

---

## Failure Modes

See [FAILURES.md](FAILURES.md) for a detailed, honest list of 6 known failure modes.

---

## File Structure

```
app.py           Flask app + all routes
db.py            SQLite schema + helpers
worker.py        ThreadPoolExecutor + event processing pipeline
dm_client.py     Mock API client: rate limiter + retry
reconciler.py    Background delivery reconciliation
config.py        Constants: API key, retry settings, rate limits
requirements.txt Pinned dependencies
Procfile         gunicorn config for Render/Railway/Heroku
FAILURES.md      Known failure modes (required by assignment)
```
