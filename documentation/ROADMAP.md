# Plan: Static → Dynamic Migration + LLM & Newsletter Features

## Decisions locked
- Hosting: Fly.io (Option A — containerized, Docker-native)
- Deal data: MotherDuck (DuckDB already in requirements.txt, all data is Parquet)
- User/subscription data: Neon (serverless Postgres, free tier)
- LLM: Groq free API (Llama 3 / Mixtral)
- LLM scope: Recipe suggestions (Phase 2), chat as future Phase 4
- Newsletter: Weekly digest + shopping list export
- Email: Resend (3K/month free tier)
- Goal: Zero cost with guardrails to prevent unexpected charges

## Target stack
| Layer | Service | Free tier limit |
|---|---|---|
| Frontend | GitHub Pages (unchanged) | Unlimited |
| API | Fly.io — FastAPI container | 3 shared-cpu VMs, 160GB outbound/mo |
| Deal data | MotherDuck | 10GB storage, shared compute |
| User data | Neon (Postgres) | 500MB, 190 compute hrs/mo |
| LLM | Groq API | 14,400 req/day (Llama 3.1 8B) |
| Email | Resend | 3,000 emails/mo, 100/day |
| Newsletter cron | GitHub Actions | 2,000 min/mo (public repo: unlimited) |

---

## Phase 1: Core Backend + Infrastructure (~2-3 days)

### Goal
Ship a deployed FastAPI service on Fly.io with a health endpoint and Neon DB connected. No features yet — just the skeleton everything else builds on. Frontend is UNTOUCHED in this phase.

### Steps
1. Create `api/` directory with `api/main.py` (FastAPI app, `/health` endpoint, CORS for GitHub Pages domain)
2. Create `api/db.py` — SQLAlchemy async engine connecting to Neon via `DATABASE_URL` env var
3. Create `api/models.py` — SQLAlchemy `Subscription` ORM model (id, email, postal_code, store_codes JSON, confirmed bool, unsubscribe_token UUID, consented_at timestamp)
4. Set up Alembic in `api/migrations/` with initial migration for subscriptions table
5. Add `api` service to `docker-compose.yml` — FastAPI on port 8000, reads `.env` for secrets
6. Create `fly.toml` — single shared-cpu-1x VM, scale-to-zero enabled (guardrail: no idle compute cost), health check on `/health`
7. Create `.github/workflows/deploy_api.yml` — deploys to Fly.io on push to `main` when `api/**` changes
8. Update `requirements.txt`: add `fastapi`, `uvicorn[standard]`, `sqlalchemy[asyncio]`, `asyncpg`, `alembic`, `python-dotenv`
9. Create `.env.example` documenting all required env vars (`DATABASE_URL`, `RESEND_API_KEY`, `GROQ_API_KEY`, `ALLOWED_ORIGIN`)

### Cost guardrails in this phase
- Fly.io `scale-to-zero = true` in fly.toml — VM sleeps when idle, wakes on request
- Neon auto-suspend on inactivity (default 5 min) — no idle compute charges
- No MotherDuck integration yet — pipeline keeps writing Parquet locally; MotherDuck is Phase 1.5

### Files
- NEW: `api/__init__.py`, `api/main.py`, `api/db.py`, `api/models.py`
- NEW: `api/migrations/` (Alembic env.py + first migration)
- NEW: `fly.toml`
- NEW: `.env.example`
- NEW: `.github/workflows/deploy_api.yml`
- MOD: `docker-compose.yml` — add api service
- MOD: `requirements.txt` — add new deps

### Verification
- `docker compose up api` → `GET /health` returns `{"status": "ok"}`
- `alembic upgrade head` runs without error against Neon
- GitHub Actions deploy_api.yml completes successfully
- Live Fly.io URL returns `{"status": "ok"}`

---

## Phase 1.5: MotherDuck Integration (optional, ~1 day, parallel-safe after Phase 1)

### Goal
Replace the `active_scores.json` full-download pattern with an API endpoint that queries MotherDuck, enabling server-side filtering. Frontend can stay static (still downloads full JSON) until Phase 3 frontend work adds the API call.

### Steps
1. Register MotherDuck, create `flyerdeals` database, upload `db/scores/active_scores.parquet` as a table
2. Add `GET /api/deals` endpoint — queries MotherDuck via DuckDB Python driver, accepts `?chains=`, `?category=`, `?postal_prefix=` query params, returns filtered JSON
3. Add `MOTHERDUCK_TOKEN` to env vars and `fly.toml` secrets
4. Update `scripts/run_pipeline.py` to optionally sync `active_scores.parquet` to MotherDuck after build (controlled by `MOTHERDUCK_TOKEN` env var presence — skipped if not set)

### Cost guardrails
- MotherDuck charges per query compute, not per hour — zero cost when idle
- Add query `LIMIT 500` hard cap in the endpoint to prevent runaway result sets
- No write queries from the API — MotherDuck is read-only from the API layer

### Files
- NEW: `api/routes/deals.py`
- MOD: `api/main.py` — include deals router
- MOD: `scripts/run_pipeline.py` — optional MotherDuck sync step
- MOD: `requirements.txt` — duckdb already present, add `motherduck` if separate

---

## Phase 2: LLM Recipe Suggestions (~2-3 days, depends on Phase 1)

### Goal
Add a "Suggest Recipes" button to the deals page. The API sends the user's current visible deals to Groq and streams back recipe suggestions.

### Steps
1. Add `groq` to requirements
2. Create `api/routes/recipes.py` — `POST /api/recipes` endpoint:
   - Accepts `{"deals": [{name, brand, category_l1, sale_price}, ...]}` (max 20 items)
   - Builds structured prompt: deal list as context, asks for 2-3 recipes using the ingredients on sale
   - Calls Groq `llama-3.1-8b-instant` (fastest free model)
   - Returns `{"recipes": "<markdown string>"}`
3. Add rate limiting via `slowapi`: 5 requests/minute per IP (guardrail: stays within Groq's free quota)
4. Add `GROQ_API_KEY` to env vars
5. Frontend — `RecipeSuggestions.vue`: modal component, shows loading spinner then rendered markdown
6. Frontend — modify `DealsView.vue`: add "✨ Suggest Recipes" button that sends `filteredDeals.slice(0, 20)` to `/api/recipes`
7. Frontend — add `VITE_API_BASE_URL` env var so the frontend knows where the API lives

### Cost guardrails
- Hard limit: `deals` payload capped at 20 items server-side (small Groq token budget)
- Rate limit: 5 req/min/IP via slowapi
- Model: `llama-3.1-8b-instant` is the fastest/cheapest Groq model (free tier: 14,400 req/day)
- No streaming to keep implementation simple; full response returned at once

### Files
- NEW: `api/routes/recipes.py`
- MOD: `api/main.py` — include recipes router, add slowapi middleware
- MOD: `requirements.txt` — add groq, slowapi
- NEW: `frontend/src/components/RecipeSuggestions.vue`
- MOD: `frontend/src/views/DealsView.vue` — add recipes button
- NEW: `frontend/.env.example` — document VITE_API_BASE_URL

### Verification
- `POST /api/recipes` with 5 sample deals → returns recipe markdown
- 6th request within 1 minute from same IP → 429 Too Many Requests
- Button in DealsView shows modal with recipe content

---

## Phase 3: Newsletter + Shopping List (~5-7 days, depends on Phase 1)

### Goal
Users can save deals to a local shopping list and email it to themselves. Users can also subscribe to a weekly digest of top deals matching their location and store preferences.

### Steps

#### Backend (steps 1–7)
1. Create `api/routes/subscriptions.py` with four endpoints:
   - `POST /api/subscribe` — validate email (format check), create unconfirmed DB row, generate UUID `unsubscribe_token`, send confirmation email via Resend
   - `GET /api/confirm/{token}` — look up token, set `confirmed = true`, set `consented_at = now()`
   - `GET /api/unsubscribe/{token}` — delete row (CASL requirement)
   - `POST /api/export-deals` — accepts `{email, deals[]}`, sends formatted deal list email immediately (no subscription needed)
2. Add `RESEND_API_KEY` to env vars
3. Create `api/email_templates/` with three Jinja2 HTML templates:
   - `confirmation.html` — double opt-in email with confirm link
   - `weekly_digest.html` — top deals grouped by category, unsubscribe link in footer
   - `deal_export.html` — shopping list formatted as clean HTML table
4. Add rate limiting to subscription endpoints: 3 subscribe requests/hour/IP (prevents email abuse)
5. Add `POST /api/subscribe` input validation: reject disposable email domains (basic list), validate postal code format (`^[A-Za-z]\d[A-Za-z]`)

#### Newsletter delivery (step 6)
6. Create `scripts/send_newsletter.py`:
   - Queries Neon for all `confirmed = true` subscribers
   - For each subscriber: loads `active_scores.json` (or queries MotherDuck if Phase 1.5 done), filters deals by subscriber's postal FSA prefix and store codes
   - Selects top 10 deals by `deal_score`, renders `weekly_digest.html` template
   - Sends via Resend, logs send count
   - Designed to be idempotent — safe to re-run same week (add `last_sent_week` column to check)
7. Create `.github/workflows/newsletter.yml` — cron `0 8 * * 1` (Monday 8am UTC), runs `send_newsletter.py`, requires `DATABASE_URL` and `RESEND_API_KEY` secrets

#### Frontend (steps 8–11, parallel with steps 1–7)
8. Create `frontend/src/stores/shoppingList.js` — Pinia store: `savedDeals` (Map of deal_key → deal object), `toggleDeal(deal)`, persisted to localStorage key `flyerdeals_list`
9. Modify `DealCard.vue` — add bookmark icon toggle button that calls `shoppingList.toggleDeal(deal)`
10. Create `ShoppingList.vue` — slide-in drawer component: lists saved deals, "Email my list" button that opens email input and calls `POST /api/export-deals`
11. Create `SubscribeModal.vue` — modal: email + postal confirmation + store selection summary, calls `POST /api/subscribe`, shows "Check your inbox" confirmation state
12. Add "Subscribe" CTA button to `AppHeader.vue` or `DealsView.vue`

### Cost guardrails
- Resend free tier: 3,000 emails/month, 100/day — newsletter.yml checks subscriber count before sending; aborts with error log if count × 1 > 2,500 (leaves buffer for export emails)
- `last_sent_week` column prevents double-sends if cron retries
- Rate limiting on `/api/subscribe` prevents subscription spam

### CASL compliance
- Double opt-in: no marketing email sent until `confirmed = true`
- `consented_at` timestamp stored in DB
- Every marketing email includes unsubscribe link (`/api/unsubscribe/{token}`)
- Email footer identifies sender (site name + contact info)

### Files
- NEW: `api/routes/subscriptions.py`
- NEW: `api/email_templates/confirmation.html`, `weekly_digest.html`, `deal_export.html`
- NEW: `scripts/send_newsletter.py`
- NEW: `.github/workflows/newsletter.yml`
- MOD: `api/main.py` — include subscriptions router
- MOD: `api/models.py` — add `last_sent_week` column to Subscription
- MOD: `requirements.txt` — add resend, jinja2
- NEW: `frontend/src/stores/shoppingList.js`
- MOD: `frontend/src/components/DealCard.vue` — add save toggle
- NEW: `frontend/src/components/ShoppingList.vue`
- NEW: `frontend/src/components/SubscribeModal.vue`
- MOD: `frontend/src/components/AppHeader.vue` — add Subscribe button

### Verification
- `POST /api/subscribe` → confirmation email arrives, DB row `confirmed = false`
- Confirm link → `confirmed = true`, `consented_at` set
- Unsubscribe link → row deleted
- 4th subscribe request in 1 hour from same IP → 429
- Invalid postal code → 422 with clear error
- Save deal → survives page reload in shopping list
- `POST /api/export-deals` → formatted email arrives
- Manual trigger `newsletter.yml` → confirmed subscribers receive digest, `last_sent_week` updated

---

## Future Phase 4: Chat Interface (not in scope now)
When recipe suggestions are working, a full RAG chat interface over deal data is the natural next step. Would require: vector embeddings of deal text (pgvector on Neon or Chroma), a conversation history endpoint, and a ChatInterface.vue component. MotherDuck integration from Phase 1.5 would be the data layer.

---

## Total effort estimate
| Phase | Scope | Effort |
|---|---|---|
| 1 | Core backend + Fly.io + Neon | 2–3 days |
| 1.5 | MotherDuck integration | 1 day |
| 2 | LLM recipe suggestions | 2–3 days |
| 3 | Newsletter + shopping list | 5–7 days |
| **Total** | | **~10–14 days** |
