# Project: instantly-service

Cold email outreach service via Instantly.ai API V2. Handles campaign management, lead upload, email accounts, warmup, analytics, and webhook processing.

## Commands

- `npm test` — run all tests (Vitest)
- `npm run test:unit` — unit tests only
- `npm run test:integration` — integration tests only
- `npm run build` — compile TypeScript + generate OpenAPI spec
- `npm run dev` — local dev server (nodemon)
- `npm run generate:openapi` — regenerate openapi.json
- `npm run db:generate` — generate Drizzle migrations
- `npm run db:migrate` — run migrations
- `npm run db:push` — push schema to database

## Architecture

- `src/index.ts` — Express app entry point
- `src/schemas.ts` — Zod schemas (source of truth for validation + OpenAPI)
- `src/routes/` — Route handlers (campaigns, leads, accounts, analytics, send, webhooks, health)
- `src/middleware/serviceAuth.ts` — API key auth middleware
- `src/lib/instantly-client.ts` — Instantly API V2 HTTP client
- `src/lib/runs-client.ts` — Runs service HTTP client for cost tracking
- `src/db/schema.ts` — Drizzle ORM table definitions (campaigns, leads, accounts, events, analytics_snapshots)
- `src/db/index.ts` — Database connection
- `tests/unit/` — Unit tests
- `tests/integration/` — Integration tests
- `tests/helpers/` — Test utilities (test app, test DB)
- `openapi.json` — Auto-generated, do NOT edit manually

## Delivery Status Architecture

### Two layers: raw events vs consolidated status

**Layer 1 — Raw Instantly events (`instantly_events` table):**
Append-only log of every webhook received from Instantly. One row per event, never updated, never deleted. This is the single source of truth for all delivery state.

Event types received from Instantly:

| Event type | Category |
|------------|----------|
| `email_sent` | Delivery — Instantly confirms the email was sent |
| `email_opened` | Engagement — recipient opened the email |
| `email_link_clicked` | Engagement — recipient clicked a link |
| `email_bounced` | Delivery — email bounced |
| `campaign_completed` | Delivery — entire sequence finished for this recipient |
| `reply_received` | Reply — recipient replied |
| `lead_interested` | Reply classification — positive reply |
| `lead_meeting_booked` | Reply classification — positive reply |
| `lead_closed` | Reply classification — positive reply |
| `lead_not_interested` | Reply classification — negative reply |
| `lead_wrong_person` | Reply classification — negative reply |
| `lead_unsubscribed` | Reply classification — negative reply |
| `lead_neutral` | Reply classification — neutral reply |
| `auto_reply_received` | Reply classification — auto-reply (not human) |
| `lead_out_of_office` | Reply classification — auto-reply (not human) |

**Layer 2 — Consolidated status (computed at query time, never stored):**
Derived from layer 1 events using implication rules. Returned by `POST /orgs/status`.

### Consolidated status fields (`ScopedStatusFields`)

| Field | Type | Description |
|-------|------|-------------|
| `contacted` | boolean | Row exists in `instantly_campaigns` |
| `sent` | boolean | An `email_sent` event exists, OR implied by downstream events |
| `delivered` | boolean | = `sent` on Instantly (no distinct delivery signal). Exception: `false` if bounced |
| `opened` | boolean | An `email_opened` event exists, OR implied by `clicked`/human reply |
| `clicked` | boolean | An `email_link_clicked` event exists (never implied) |
| `replied` | boolean | A reply event exists (human OR auto-reply) |
| `replyClassification` | string? | `positive`, `negative`, `neutral`, or `auto_reply`. null if no reply |
| `bounced` | boolean | An `email_bounced` event exists |
| `unsubscribed` | boolean | A `lead_unsubscribed` event exists |
| `lastDeliveredAt` | string? | ISO 8601 timestamp of most recent delivery |

### Implication rules

Each event implies parent statuses in the chain. This handles missing webhooks (e.g. `replied` received without `email_sent`):

| Event received | contacted | sent | delivered | opened | clicked | replied |
|----------------|:---------:|:----:|:---------:|:------:|:-------:|:-------:|
| *(row exists)* | **true** | | | | | |
| `email_sent` | true | **true** | true | | | |
| `email_opened` | true | true | true | **true** | | |
| `email_link_clicked` | true | true | true | true | **true** | |
| `reply_received` | true | true | true | true | | **true** |
| `lead_interested` | true | true | true | true | | true |
| `lead_meeting_booked` | true | true | true | true | | true |
| `lead_closed` | true | true | true | true | | true |
| `lead_not_interested` | true | true | true | true | | true |
| `lead_wrong_person` | true | true | true | true | | true |
| `lead_neutral` | true | true | true | true | | true |
| `auto_reply_received` | true | true | true | | | **true** |
| `lead_out_of_office` | true | true | true | | | **true** |
| `email_bounced` | true | true | **false** | | | |
| `lead_unsubscribed` | true | true | true | | | |
| `campaign_completed` | true | true | true | | | |

Key distinctions:
- `auto_reply_received` / `lead_out_of_office`: imply `replied = true` (it IS a reply) but NOT `opened` (not a human open)
- `email_bounced`: implies `sent = true` but `delivered = false` (attempted but failed)
- `clicked` is NEVER implied — only an explicit `email_link_clicked` event sets it

### Reply classification

4 categories:

| Category | Events |
|----------|--------|
| `positive` | `lead_interested`, `lead_meeting_booked`, `lead_closed` |
| `negative` | `lead_not_interested`, `lead_wrong_person`, `lead_unsubscribed` |
| `neutral` | `lead_neutral` |
| `auto_reply` | `auto_reply_received`, `lead_out_of_office` |

**Campaign-level priority:** most recent human classification wins. If no human classification exists, fall back to most recent auto_reply.

**Brand-level priority:** most positive human classification across all campaigns wins. Positivity order: `positive > neutral > negative > auto_reply > null`.

### Status endpoint modes (`POST /orgs/status`)

| Mode | Input | Fields populated |
|------|-------|-----------------|
| Campaign mode | `{ campaignId, items }` | `campaign` + `global` |
| Brand mode | `{ brandId, items }` (no campaignId) | `byCampaign` (per-campaign breakdown) + `brand` (aggregated) + `global` |
| Global only | `{ items }` | `global` only |

Brand aggregation rules:
- Boolean fields (`contacted`, `sent`, `delivered`, `opened`, `clicked`, `replied`, `bounced`, `unsubscribed`): BOOL_OR across campaigns
- `replyClassification`: most positive human classification (see above)
- `lastDeliveredAt`: MAX across campaigns

### Deprecated columns on `instantly_campaigns`

- `delivery_status` — **DEPRECATED.** Do not read or write. All status is derived from `instantly_events`.
- `reply_classification` — **DEPRECATED.** Do not read or write. Derived from events at query time.
