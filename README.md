# Instantly Service

Cold email outreach service via [Instantly.ai](https://instantly.ai/) API V2. Handles campaign management, lead upload, email account warmup, and webhook processing. Integrates with runs-service for cost tracking (BLOCKING).

## API Endpoints

### Campaigns
- **POST /campaigns** - Create campaign in Instantly + DB, log run (BLOCKING)
- **GET /campaigns/:campaignId** - Get campaign details
- **GET /campaigns/by-org/:orgId** - List campaigns for org
- **PATCH /campaigns/:campaignId/status** - Update status (active/paused/completed)

### Leads
- **POST /campaigns/:campaignId/leads** - Add leads (bulk), log run + costs (BLOCKING)
- **GET /campaigns/:campaignId/leads** - List leads
- **DELETE /campaigns/:campaignId/leads** - Remove leads

### Accounts
- **GET /accounts** - List email accounts
- **POST /accounts/sync** - Sync from Instantly API to local DB
- **POST /accounts/:email/warmup** - Enable/disable warmup
- **GET /accounts/warmup-analytics** - Warmup stats

### Analytics
- **GET /campaigns/:campaignId/analytics** - Fetch from Instantly API, save snapshot
- **POST /stats** - Aggregated stats by runIds

### Webhooks
- **GET /webhooks/instantly/config** - Returns webhook URL for BYOK customers (public)
- **POST /webhooks/instantly** - Receives Instantly events (verified by campaign_id DB lookup)

### Health
- **GET /** - Service info
- **GET /health** - Health check

## Tech Stack

- **Runtime:** Node.js 20 + TypeScript
- **Framework:** Express
- **Database:** PostgreSQL (Neon) via Drizzle ORM
- **Email API:** Instantly.ai API V2
- **Testing:** Vitest + Supertest

## Database Schema

| Table | Purpose |
|-------|---------|
| `instantly_campaigns` | Campaign records (silver — `delivery_status` reflects 4-stage funnel) |
| `instantly_leads` | Lead records per campaign |
| `instantly_accounts` | Email accounts + warmup status |
| `instantly_events` | Webhook events (silver — source of truth for funnel) |
| `sequence_costs` | Per-step cost lifecycle (provisioned → actual / cancelled) |
| `instantly_webhook_payloads_raw` | Bronze: webhook payloads from Instantly |
| `instantly_analytics_raw` | Bronze: `/campaigns/analytics` responses |
| `instantly_emails_raw` | Bronze: `/emails` records (individual emails with step) |
| `instantly_leads_raw` | Bronze: `/leads/list` per-lead snapshots |
| `instantly_campaigns_config_raw` | Bronze: `GET /campaigns/{id}` full config snapshots (used to derive `not_sending_status`) |

### Funnel stages (4-stage)

| Stage | `delivery_status` | Source of truth |
|-------|-------------------|-----------------|
| 2 — contacted | `contacted` (default) | row exists in `instantly_campaigns` (POST /send success) |
| 3 — sent | `sent` | `instantly_events.event_type='email_sent'` (webhook from Instantly) |
| 4 — delivered | (derived) | sent AND NOT bounced — computed in queries, never stored |
| terminal | `bounced` / `replied` / `unsubscribed` / `failed` / `cancelled` | webhook events; `failed` set by campaign-error-handler; `cancelled` reserved for the stuck-lead retry job (writes leads whose campaign is deliberately killed because Instantly never dispatched — typically `not_sending_status` flagged) |

### Observability — `not_sending_status`

Instantly exposes a per-campaign diagnostic `not_sending_status` (e.g. `4` = capacity-blocked). When set, Instantly will not dispatch emails for that campaign. The reconcile job pulls `GET /campaigns/{id}` per campaign per cycle (bronze: `instantly_campaigns_config_raw`) and promotes the field to silver columns on `instantly_campaigns`:

- `not_sending_status` (integer, NULL = sending normally)
- `not_sending_status_seen_at` (timestamp of last observation)

`GET /stats` surfaces `recipientStats.notSending` = `COUNT(DISTINCT lead_email) FILTER (WHERE c.not_sending_status IS NOT NULL)` scoped to the request's filters.

## Setup

```bash
npm install
cp .env.example .env  # fill in values
npm run dev
```

## Scripts

| Command | Description |
|---------|-------------|
| `npm run dev` | Start dev server (nodemon) |
| `npm run build` | Compile TypeScript |
| `npm start` | Run compiled app |
| `npm test` | Run all tests |
| `npm run test:unit` | Unit tests only |
| `npm run test:integration` | Integration tests only |
| `npm run db:generate` | Generate Drizzle migrations |
| `npm run db:migrate` | Run migrations |
| `npm run db:push` | Push schema to database |

## Cost lifecycle

Costs flow through three states in runs-service: `provisioned` (reserved) →
`actual` (charged) | `cancelled` (refunded).

| Cost | Inserted at /send | Promoted on | Cancelled when |
|------|-------------------|-------------|----------------|
| `instantly-contact-uploaded` | `actual` (1× per send) | n/a | never — lead IS uploaded |
| `instantly-account-email-sent` | `provisioned` (1× per step) | webhook `email_sent` | sequence stop (reply/bounce/unsub) OR retry-stuck |
| `instantly-domain-email-sent` | `provisioned` (1× per step) | webhook `email_sent` | sequence stop OR retry-stuck |

Step 1's email costs are now `provisioned` (previously `actual`). Instantly's
daily sender quota is only consumed on actual dispatch, so charging the
customer at /send time over-bills when Instantly later refuses to send (spam
filter, capacity, esp mismatch). The retry-stuck heartbeat (see below)
sweeps stuck `delivery_status='contacted'` rows older than 72h and re-sends
them onto a fresh account, rolling the reserved spend over.

### Retry-stuck heartbeat

A continuous in-process worker (`src/lib/retry-stuck-worker.ts`) replaces the
old daily GitHub Actions cron. Started from `src/index.ts` right after
`app.listen()` so port-binding never blocks on it, and shut down cleanly on
`SIGTERM`/`SIGINT`.

Selection criteria are **purely local** (no Instantly preflight):

```
delivery_status = 'contacted'
AND status = 'active'
AND created_at < NOW() - INTERVAL '72 hours'
AND NOT EXISTS (
  SELECT 1 FROM instantly_events e
  WHERE e.campaign_id = c.instantly_campaign_id
    AND e.event_type IN (
      'email_sent','email_opened','link_clicked',
      'reply_received','auto_reply_received',
      'email_bounced','lead_unsubscribed'
    )
)
ORDER BY created_at ASC LIMIT 100
```

Belt-and-suspenders: the column gate AND a silver `NOT EXISTS` gate
(catches the rare case where webhook + reconcile both missed a real
`email_sent` and the column stayed stale).

Per row:
1. Pull the live Instantly campaign once to recover the sequence (subject +
   step bodies + delays). `not_sending_status` is NOT consulted — reconcile
   owns that signal for `/stats`.
2. Read the lead profile from local `instantly_leads`.
3. Call `sendLeadToInstantly` (shared with `POST /send`) to create a fresh
   campaign on a different healthy account, weighted-random by warmup score.
4. On success: refund the old cost rows, provision new ones, mirror the lead
   onto the new campaign, mute the local row in place (new
   `instantly_campaign_id`, `metadata.redispatchCount` bumped,
   `metadata.redispatchHistory` appended). `delivery_status` stays
   `'contacted'`.
5. On failure (no healthy account, all 3 send attempts hit NSS, no sequence,
   no local lead, `getCampaign` throws): the row is **left alone**. No
   terminal cancel. The next tick retries — Instantly is free; with 100+
   accounts random sampling converges.

Bounds + concurrency:
- `MAX_ROWS_PER_TICK = 100`. `BATCH_SIZE = 10` per org (the global
  instantly-client throttle paces the rest).
- Tick interval `RETRY_STUCK_TICK_INTERVAL_MS` (default `10 * 60 * 1000`,
  env-overridable for staging / debugging).
- Postgres advisory lock keyed `(8729, 1)` gates each tick so overlapping
  invocations short-circuit with `skipped: "sweep_in_progress"`.

Manual triggers: `POST /internal/campaigns/retry-stuck` calls the same
`runRetryStuck()` (202 + background). Useful for ad-hoc Railway debugging
without waiting for the next heartbeat.

NSS webhook investigation: see
[docs/retry-stuck-event-driven-investigation.md](./docs/retry-stuck-event-driven-investigation.md).
TL;DR — Instantly does not emit a `not_sending_status` webhook, so reconcile
(daily 03:00 UTC) remains the sole NSS observer and feeds `/stats`
independently of retry-stuck.

## BYOK (Bring Your Own Key)

Routes with `clerkOrgId` context use the org's own Instantly API key via key-service's BYOK decrypt endpoint. If the org hasn't configured their key, the request fails with 422 — no fallback to the shared app key.

Account routes (`/accounts/*`) remain on the shared app key for service-level operations.

Webhook verification uses `campaign_id` DB lookup (each campaign UUID is unguessable and stored with its org on creation). No webhook secret is needed.

## Environment Variables

- `INSTANTLY_SERVICE_DATABASE_URL` - PostgreSQL connection string
- `KEY_SERVICE_URL` / `KEY_SERVICE_API_KEY` - Key service for API key decryption
- `INSTANTLY_SERVICE_API_KEY` - Service-to-service auth secret
- `RUNS_SERVICE_URL` / `RUNS_SERVICE_API_KEY` - Runs service integration
- `PORT` - Server port (default: 3011)

## Authentication

All endpoints require `X-API-Key` header except:
- `GET /` and `GET /health` (public)
- `GET /webhooks/instantly/config` and `POST /webhooks/instantly` (public webhook endpoints)

## Project Structure

```
src/
  index.ts              # Express app entry point
  db/
    index.ts            # Database connection
    schema.ts           # All table definitions
  lib/
    instantly-client.ts # Instantly API V2 HTTP client
    key-client.ts       # Key service client (app keys + BYOK)
    runs-client.ts      # Runs service HTTP client (BLOCKING)
  middleware/
    serviceAuth.ts      # API key auth middleware
  routes/
    health.ts           # Health check routes
    campaigns.ts        # Campaign CRUD
    leads.ts            # Lead management
    accounts.ts         # Email accounts + warmup
    analytics.ts        # Campaign analytics
    webhooks.ts         # Instantly webhook receiver
tests/
  unit/                 # Unit tests
  integration/          # Integration tests
  helpers/              # Test utilities
drizzle/                # SQL migrations
```
