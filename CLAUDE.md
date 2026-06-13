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
- `npm run backfill:inferences` — one-shot CLI: project synthetic predecessor events onto existing silver rows. Manual only — NEVER wired into boot (port-bind hazard).
- `npm run cleanup:stacked-sigs` — one-shot CLI: clean stacked signatures from Instantly campaigns whose lead has been pushed but not yet received any email. Dry-run by default; pass `-- --commit` to actually PATCH Instantly. See [Signature handling](#signature-handling--idempotent-strip-then-append). Manual only.
- `npm run audit:dupes -- <orgId>` — read-only CLI: audit cross-campaign duplicate contacts (same email sitting in ≥2 **active** Instantly campaigns). **Source of truth = Instantly API**, never the local DB (the DB can be stale — the duplicate FACT is derived from Instantly). The local DB is read only to label each collision `same-brand` / `cross-brand` / `unknown-brand` (Instantly has no brand field). Read-only — audits, never heals. Flags: `--json`, `--limit N`, `--severe-only`. Manual only. See [Cross-campaign duplicate audit](#cross-campaign-duplicate-audit--read-only).
- `npm run heal:dupes -- --commit` — MUTATING CLI: pause redundant active Instantly campaigns. For each email in ≥2 active campaigns, **keep the oldest** (`timestamp_created`) and **pause** the rest. Dry-run by default; `--commit` to act; `--limit N` to batch. Pause is reversible (no delete). Instantly-only (no cost refund / DB write — runs-service is internal-DNS-only from a local shell). Run AFTER the retry-stuck fix deploys (else the worker re-spawns dupes). Idempotent/resumable. Manual only. See [Cross-campaign duplicate audit](#cross-campaign-duplicate-audit--read-only).
- `npm run cleanup:finished-contacts -- --commit` — MUTATING CLI: delete contacts of **finished** Instantly campaigns to reclaim the plan's uploaded-contacts quota. Finished = Instantly status **paused (2)** or **completed (3)**; active (1) is never touched. **Source of truth = Instantly API** (status + membership), never the local DB. `DELETE /leads` is campaign-level (the only delete that frees quota — deleting from "Lists" does not). Dry-run by default; `--commit` to act; `--limit N` to batch; `--json`. Idempotent/resumable (already-deleted leads drop out of the next sweep; a 404 delete is counted, not fatal). Instantly-only (no local DB write — the recurring reconcile path marks rows terminal). Selection logic is pure + unit-tested in `src/lib/cleanup-finished.ts` (`selectContactsToDelete`). Manual only. See [Finished-contact cleanup](#finished-contact-cleanup--reclaim-quota).

## Architecture

- `src/index.ts` — Express app entry point
- `src/schemas.ts` — Zod schemas (source of truth for validation + OpenAPI)
- `src/zod-setup.ts` — Side-effect module that extends Zod with `.openapi()`. Import it BEFORE any module that creates `z.object(...).openapi("Name")` schemas.
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

## Shared contract

Cross-provider canonical shapes (`StatusScope`, `RecipientStats`, `EmailStats`, `StepStats`, `RepliesDetail`, `ChannelStats`, `ProviderStatus`, `GlobalStatus`, `ReplyClassification`) live in [`@shamanic-technologies/email-domain-contract`](https://github.com/shamanic-technologies/email-domain-contract). Do NOT redeclare these schemas locally — re-export from the package via `src/schemas.ts`. Instantly-specific schemas that wrap or extend these shapes keep their local declaration.

Two provider-specific fields are **optional in v1** of the contract: `cancelled` and `notSending`. Instantly-service is the source of truth for both — handlers always emit them as populated values. Contract v2 will tighten them to required once postmark-service ships its padding follow-up.

**Contract pinned at `^1.1.0`** (DIS-229). v1.1.0 widened `StatusScope` with 8 optional+nullable first-occurrence (MIN) timestamps — the mirror of `lastDeliveredAt` (MAX): `firstContactedAt, firstSentAt, firstDeliveredAt, firstOpenedAt, firstClickedAt, firstRepliedAt, firstBouncedAt, firstUnsubscribedAt`. `POST /orgs/status` populates all 8 on every StatusScope (`byCampaign[*]`, `campaign`, `brand`) in `src/routes/status.ts`. Rules: each `first*At = MIN(event timestamp in scope)`, null if it never occurred; brand = MIN across the brand's campaigns (mirrors `BOOL_OR` booleans). Each `first*At` agrees with its boolean (non-null iff boolean true). **Two have no engagement event and are derived:** `firstContactedAt = MIN(c.created_at)` (`contacted` = campaign row exists / lead pushed), and `firstDeliveredAt = MIN(sent-timestamp) when (sent AND NOT bounced) else NULL` (consistent with `delivered`). Don't rename the 8, don't add a 9th — the set is locked to the published contract. Guard: the "First-occurrence (MIN) timestamps — DIS-229" tests in `tests/unit/status.test.ts`.

## Zod 4 caveat — contract schemas + `.openapi()`

`@asteasolutions/zod-to-openapi` attaches `.openapi()` to Zod schema instances at the time `extendZodWithOpenApi(z)` runs in the consumer. The contract package's schemas were instantiated before that point in the consumer's module graph, so they do NOT gain `.openapi()` retroactively. Re-export them without `.openapi(name)` and let the generator inline them (no `$ref` name). Local schemas defined in `src/schemas.ts` (after `import "./zod-setup"`) keep their `.openapi(name)` tagging.

## Instantly client — pagination convention

Any helper in `src/lib/instantly-client.ts` that returns "all of X" from Instantly (no caller-driven `skip` / `limit` parameters) MUST paginate via the `next_starting_after` cursor pattern used by `listAccounts`, `listLeadsFull`, and `listEmails`. NEVER call `/path` once and return `response.items` — Instantly's default page size is **10**. Every such helper requires a unit test asserting:
1. multi-page traversal (mock 2+ pages, assert `mockFetch.mock.calls.length` matches and items are concatenated in order),
2. termination on `next_starting_after = null` even when items are still returned,
3. termination on empty `items` (no infinite loop).

Use `limit=100` per page — Instantly silently returns an empty list for any larger value.

Helpers that expose `skip` + `limit` (caller-driven pagination) do NOT exist in this codebase today. If you add one, document why caller-driven pagination is the right shape (e.g. a route that lets a UI page through results); otherwise default to the "all of X" full-pagination helper.

Historic bug 2026-05-28: `listAccounts` shipped without pagination. Only 10 of 156 active senders were ever picked by `pickRandomAccount` (send-lead.ts) → 146 warmed accounts sat idle while the first 10 saturated at 30/day. Fix: paginate. Guard: this section + the pagination unit tests in `tests/unit/instantly-client.test.ts` ("listAccounts: paginates via next_starting_after across 3 pages, ...").

## Signature handling — idempotent strip-then-append

`buildEmailBodyWithSignature` (`src/lib/send-lead.ts`) MUST be idempotent: `f(f(x)) === f(x)`. It ALWAYS calls `stripAccountSignature` on the input body before appending the signature. Guarantees a body re-sent N times (e.g. retry-stuck redispatching the same lead) never accumulates N stacked signatures.

**Signature source priority:**

1. `account.signature` — per-sender override configured in Instantly's UI (account settings). Intentionally empty in prod.
2. `buildDefaultSignature(account)` (`src/lib/send-lead.ts`) — returns the `DEFAULT_SIGNATURE` constant. **Source of truth in prod.**

**One fixed brand line for every sender.** Identical regardless of which account `pickRandomAccount` selected:

```
Kevin Lourd | Founder
Distribute.you | Marketing Agency
```

`buildDefaultSignature(account)` ignores its `account` arg today (kept so per-account derivation can be reintroduced without touching the caller). The brand line renders as **plain text — NOT a clickable `<a>` link**: `buildEmailBodyWithSignature` autolinkifies the prospect body ONLY, appending the signature block verbatim. Do NOT reintroduce sig-wide autolinkify. When the copy/title/brand changes, edit `DEFAULT_SIGNATURE` and ship a hotfix.

The signature is intentionally code-derived (not Instantly-UI driven). When the copy/title/format changes, edit `buildDefaultSignature` and ship a hotfix. Do NOT re-add per-account UI signatures without updating this rule — code prefers the UI value over the derived default.

**HTML-formatted, HTML-separated.** The signature MUST be wrapped in `<p>...</p>` (use `<br>` for in-sig line breaks). Separator is `<p>--</p>`, NOT `\n\n--\n`. Reason: Instantly's HTML sanitizer aggressively strips plain text and bare `--` outside element wrappers on PATCH round-trip — only `<a>` anchors and tag-wrapped content survive. An earlier plain-text signature was reduced to a stray `<a>distribute.you</a>` anchor on every PATCH (historic damage 2026-05-28: ~700 rows shipped with stacked broken anchors and no signature text). Guard: `should append HTML <p>--</p> separator + signature to body` + sibling cumulative-strip tests in `tests/unit/send.test.ts` assert the `<p>--</p>` form.

`stripAccountSignature` is HTML-tolerant. It looks for the EARLIEST occurrence of any of these standalone `--` markers (RFC 3676 sig delimiter, in its plain or HTML-wrapped forms):

- `\n\n--\n` plain (with optional trailing space, e.g. `\n\n-- \n`)
- `<p>--</p>` paragraph-wrapped (with optional `&nbsp;`)
- `<br>--<br>` line-break-wrapped
- `<div>--</div>` div-wrapped

…and slices everything from the marker onward. If no marker matches, body is returned unchanged. Senders whose original body legitimately contains one of these markers will lose content past that point on a re-send — accepted edge-case.

Historic bug 2026-05-28: the original `stripAccountSignature` matched only the plain `\n\n--\n` marker. Bodies stored as HTML never matched, so every retry-stuck re-send appended a fresh signature on top of the existing one. A row redispatched 72 times shipped a body with 72 stacked signatures. Fix: HTML-tolerant marker list + strip-then-append in `buildEmailBodyWithSignature`. Guard: this section + the `stripAccountSignature` and `buildEmailBodyWithSignature` cumulative-stack tests in `tests/unit/send.test.ts`.

If you add a new HTML wrapper form (e.g. `<section>--</section>`), add a regex to `SIG_MARKERS` AND a unit test case covering both the marker shape and a 3-stacked case for that shape.

## Send idempotency — reserve BEFORE the external Instantly call (DIS-148)

`POST /orgs/send` is idempotent under retry/concurrency for `(campaignId, leadEmail)`: a retried or concurrent send creates **at most one** Instantly campaign, and the loser returns **200 duplicate** (`{success:true, added:0, duplicate:true}`), NEVER a fatal 409. Trigger for the original bug: when instantly-service is slow, email-gateway's 10s `AbortSignal.timeout` fires and it retries — the abort only cancels the caller's wait, so the retry races the original. Both used to pass a read-only dedup check, both called Instantly (each picking the next sender in rotation → two real campaigns), and the loser's insert hit the unique index → fatal 409 → email-gateway mapped it 502 → Windmill flow failed.

**The fix is an ORDERING invariant in `src/routes/send.ts` — do NOT revert it to read-then-act.** The atomic claim on the `(campaignId, leadEmail)` unique index happens BEFORE `sendLeadToInstantly` (the external side-effect), not after:

1. **Reserve** — one `INSERT … onConflictDoUpdate(target:[campaignId,leadEmail], setWhere: reservation-is-stale).returning({id})`. The reserved row carries a unique **`reserving:<uuid>` sentinel** in `instantlyCampaignId` (column stays `notNull().unique()` — the sentinel is the "in flight" marker; NO nullable column, NO migration, so the 6 reader services that assume non-null are untouched). Winner ⇔ `RETURNING` non-empty. The single upsert covers all four cases: fresh-insert (winner), already-committed real-id row (loser → 200 dup), fresh concurrent sentinel (loser → 200 dup), and **stale** sentinel older than `STALE_RESERVATION_MS` (30s — crashed mid-send → reclaim → winner).
2. **Winner only** → `sendLeadToInstantly` → **phase-2** `UPDATE` overwrites the sentinel with the real `instantlyCampaignId`.
3. **Release on failure** — `sendLeadToInstantly` `!ok`, or any throw in the inner try → `releaseReservation()` `DELETE`s the row **only while it still matches `reserving:%`** (no-op once phase-2 committed) so a later legit retry can re-claim. Fail loud — no swallow.

`authorize` (credit affordability) + the leadId-conflict 409 stay BEFORE the reserve (they early-return without reserving). Cost provision/actualize (`addCosts`) is winner-only in the step loop — losers declare no cost. The leadId-conflict case (same email, different `lead_id`) is still a real **409**, unchanged. Platform sends (`campaignId=null`) never conflict (Postgres NULLs distinct) → always insert/win, exactly as before. Guard: the "Reservation idempotency (DIS-148)" tests + the rewritten duplicate/concurrent tests in `tests/unit/send.test.ts`. This is idempotency-under-retry for the SAME `(campaignId, leadEmail)` — it does NOT dedup across *different* logical campaigns (that's the separate cross-campaign concern below).

## Cross-campaign duplicate audit — read-only

`POST /send` dedups on `(campaign_id, lead_email)` (`instantly_campaigns_campaign_lead_idx`), **not** on `(org_id, lead_email)` or `(brand, lead_email)`. So the same person reached by two **different** logical campaigns of the same org/brand creates two separate active Instantly campaigns — the same prospect gets double-contacted. DIS-77 healed this once (Phase A cancelled 6 same-wave dups, Phase B cancelled 51 re-contacts) but its **root-cause prevention in `POST /send` was never shipped**, so duplicates re-accumulate.

`npm run audit:dupes -- <orgId>` is the standing read-only check for this:

- **Duplicate fact = Instantly API, authoritative.** Lists all campaigns (active = status `1`), sweeps all leads via `POST /leads/list`, groups by email, flags any email in ≥2 active campaigns. The local DB is deliberately NOT consulted for the fact — it can be stale.
- **Brand/org = local DB, label only.** Instantly has no brand field; `instantly_campaigns.brand_ids` / `org_id` are joined on `instantly_campaign_id` purely to classify each collision:
  - `same-brand` — ≥2 active campaigns share a brand. True redundant outreach (the dangerous case).
  - `cross-brand` — ≥2 active campaigns, all brands known, none repeated.
  - `unknown-brand` — ≥2 active campaigns but a campaign has no DB row (DB gap; duplicate still real).
- Pure detection logic lives in `src/lib/audit-duplicates.ts` (`findDuplicateContacts` / `summarizeDuplicates`), unit-tested in `tests/unit/audit-duplicates.test.ts`. The script `scripts/audit-cross-campaign-dupes.ts` does only the Instantly + DB IO.
- **Read-only — never PATCH/pause/cancel.** This audits; it does not heal. The healing pass is `heal:dupes` (below); the deferred `POST /send` prevention is separate work.

The **heal** (`scripts/heal-pause-dupe-campaigns.ts`, `npm run heal:dupes`):

- For each email in ≥2 active campaigns, **keep the oldest by `timestamp_created`** (NOT `created_at` — the `/campaigns` list does not return `created_at`; relying on it silently falls back to id-sort and keeps the wrong campaign) and **pause** the rest via `POST /campaigns/{id}/pause`. Collapses retry stacks AND distinct logical campaigns → one active per person.
- Selection logic is pure + unit-tested in `src/lib/heal-duplicates.ts` (`selectCampaignsToPause`); the script does only Instantly IO.
- **Pause, not delete** — reversible. **Dry-run by default**; `--commit` to act; `--limit N` to batch (~110ms/call ⇒ ~75 min for the full ~41k backlog). Idempotent + resumable: re-runs re-sweep live state, so already-paused campaigns leave the active set and are never re-touched.
- **Instantly-only.** No cost refund / local-DB `delivery_status` write — `handleCampaignError` / runs-service live at `*.railway.internal`, unreachable from a local shell. Cost reconciliation is a follow-up (DIS-148).
- **Order matters:** deploy the retry-stuck fix (DIS-148) BEFORE running `--commit`, else the worker re-spawns duplicates faster than the heal clears them.
- **Running it locally against prod.** key-service lives at `key-service.railway.internal`, which only resolves INSIDE Railway's network — a laptop `railway run` shell gets `ENOTFOUND`. So set `INSTANTLY_API_KEY` directly (bypasses key-service; audits that key's workspace — the platform key audits the shared workspace) and let `railway run -s instantly-service` inject `INSTANTLY_SERVICE_DATABASE_URL` (Neon, publicly reachable) for the brand labels: `railway run -s instantly-service -- bash -lc 'export INSTANTLY_API_KEY=…; npm run audit:dupes'`. The brand-label DB lookup is chunked + uses `inArray` (drizzle expands `ANY(${jsArray})` into a ROW expression that trips Postgres' 1664-entry limit; never pass a large JS array to `sql\`= ANY(...)\``).

## Finished-contact cleanup — reclaim quota

Instantly's plan limit (e.g. 25,000 "uploaded contacts") counts contacts **currently stored** across all campaigns (active, paused, completed, draft), NOT a lifetime cumulative — **deleting leads from a campaign frees the quota** (~5-10 min sync; confirmed [help.instantly.ai/articles/7918680](https://help.instantly.ai/en/articles/7918680-uploaded-contacts-limits)). Two important facts: the delete MUST be **campaign-level** (`DELETE /leads {campaign_id, delete_list}`) — deleting from "Lists" does not free quota; and **each duplicate counts separately** (same email in 2 campaigns = 2 contacts), so the cross-campaign dupe backlog also costs quota.

Each of our sends is its own per-lead campaign, so once a campaign is **finished** its contact is deletable with no data loss — all engagement history lives in our **local silver DB** (`analytics.ts`/`status.ts` read silver, never Instantly live). "Finished" = Instantly status **paused (2)** (e.g. operator paused after the prospect replied off-Instantly — Instantly never saw that reply) or **completed (3)** (sequence ran out). **Active (1) is never touched. No pause grace period** (locked option A — a pause means "done"; if you ever pause-to-edit-and-resume, the contact gets deleted).

Two paths:

- **One-shot** `npm run cleanup:finished-contacts` — the standing backlog sweep. Pure selection in `src/lib/cleanup-finished.ts` (`selectContactsToDelete` / `countDeletions`, `FINISHED_STATUSES = {2,3}`), unit-tested in `tests/unit/cleanup-finished.test.ts`; the script `scripts/cleanup-finished-contacts.ts` does only Instantly IO (paginated `listAllCampaigns` + `listAllLeads`, then `DELETE /leads` per contact). Dry-run by default; `--commit`, `--limit N`, `--json`. Idempotent/resumable; a 404 delete (already gone) is counted, not fatal. Instantly-only — no local DB write.
- **Recurring** (reconcile) — see [Recurring finished-contact deletion](#recurring-finished-contact-deletion-reconcile) once shipped: the nightly reconcile reads each campaign's Instantly status (already fetched in Phase 0, previously discarded) and, for paused/completed campaigns, deletes the lead **after** the read phases (1-3) backfill all current state, then marks the local row terminal so reconcile stops re-polling it.

**Running it locally against prod** — same as `audit:dupes`/`heal:dupes`: set `INSTANTLY_API_KEY` directly (bypasses key-service, which is internal-DNS-only): `railway run -s instantly-service -- bash -lc 'export INSTANTLY_API_KEY=…; npm run cleanup:finished-contacts'`. Run the dry-run first and read the counts before `--commit`.

### Recurring finished-contact deletion (reconcile)

The nightly reconcile (`reconcileAll` → `reconcileOneCampaign`, `src/lib/reconcile.ts`) deletes finished contacts on an ongoing basis so quota doesn't re-accumulate. Decision logic is pure + unit-tested in `src/lib/finished-contacts.ts` (`tests/unit/finished-contacts.test.ts`); reconcile is the IO glue.

- **Signal, zero extra API calls:** Phase 0 already fetches `GET /campaigns/{id}` for `not_sending_status`. It now also reads `config["status"]` (`parseInstantlyStatus`) — the campaign lifecycle code (1 active / 2 paused / 3 completed). Previously fetched and discarded.
- **ORDERING INVARIANT (load-bearing):** the status is *read* in Phase 0 but the delete *executes last*, via the `finish(result)` closure wrapping every return of `reconcileOneCampaign`. So the delete happens AFTER Phases 1-3 (analytics + `/leads/list` + `/emails` backfill) have captured all current state — a late reply/bounce is never dropped by the delete. Do NOT move the delete into Phase 0.
- **What it does** (`deleteFinishedContact`): for a finished campaign (`isFinishedInstantlyStatus` → status 2 or 3), campaign-level `deleteLeads(apiKey, instantlyCampaignId, [leadEmail])` (frees quota), then sets the local `instantly_campaigns.status` to `completed` (3) / `paused` (2) (`localTerminalStatus`). The leadEmail comes from our own row (1 campaign = 1 lead), not a live fetch.
- **Skip terminal rows:** when deletion is enabled, `reconcileAll` filters out rows already locally terminal (`isLocallyTerminal` → status paused/completed) so a deleted contact is never re-polled — kills the nightly log noise. When disabled, every row is scanned (unchanged).
- **Kill-switch, default OFF:** gated by env `DELETE_FINISHED_CONTACTS_ENABLED` (`isDeleteFinishedEnabled` — exactly `"true"`, anything else incl. unset = OFF). Merge is safe with the var absent (reconcile stays read-only); enable after staging soak. Mirrors the `RETRY_STUCK_WORKER_ENABLED` convention.
- **Fail-loud, 404-tolerant:** a `DELETE /leads` 404 (lead already gone) is tolerated (`isLeadAlreadyGone`) — idempotent. Any other Instantly error propagates → the per-campaign wrapper counts it `failed` and retries next run. No cost/runs declaration (deleteLeads spends nothing).

**⚠️ LIVE IN PROD — `DELETE_FINISHED_CONTACTS_ENABLED=true` (production + staging, 2026-06-13, v0.42.0).** The recurring delete is ARMED. Operational consequence of locked **option A (no pause grace period)**: the instant a campaign is paused — including a **manual pause to edit-then-resume**, or an Instantly auto-pause (bounce/quota) — its contact is **deleted on Instantly at the next reconcile** (daily 03:00 UTC cron), irreversibly. Only pause when you mean "done". To disarm:

```
railway variables -s instantly-service --set "DELETE_FINISHED_CONTACTS_ENABLED=false"
```

## Data layering — Bronze / Silver / Gold

Three layers, doctrine per `~/.claude/skills/data-layering/SKILL.md`:

- **Bronze** — raw external mirrors, append-only, never mutated. Tables: `instantly_webhook_payloads_raw`, `instantly_analytics_raw`, `instantly_emails_raw`, `instantly_leads_raw`, `instantly_campaigns_config_raw`. Each row = one payload from Instantly (webhook OR reconcile poll).
- **Silver** — canonical event log `instantly_events` + state row `instantly_campaigns`. Derived from bronze via `src/lib/silver-promote.ts`. Rebuildable.
- **Gold** — stats views in `src/routes/analytics.ts`, `status.ts`. Read silver only.

### Webhook event_type normalization — canonical `email_link_clicked` (DIS-239)

Silver's canonical `event_type` for a link click is **`email_link_clicked`**. Instantly's webhook, however, emits the raw name **`link_clicked`** (the ONE event Instantly sends without the `email_` prefix — every other webhook event_type is already canonical: `email_sent`, `email_opened`, `email_bounced`, `reply_received`, `lead_*`). `promoteFromWebhookPayload` (`src/lib/silver-promote.ts`) MUST run `payload.event_type` through `normalizeWebhookEventType` (alias map `link_clicked → email_link_clicked`) before `promoteEvent`. Bronze keeps the raw `link_clicked` (append-only, never normalized). The reconcile snapshot path (`promoteClickFromLead`) already emits the canonical name directly, so both ingestion paths converge.

Every silver READER must use `email_link_clicked`, NEVER `link_clicked`: gold queries (`analytics.ts`, `analytics-public.ts`, `status.ts`), the inference rule (`computePredecessors`), `backfill-inferences.ts`, and the retry-stuck progress gate (`retry-stuck.ts`). Historic bug 2026-06-05 (DIS-239): the webhook path passed `link_clicked` verbatim while every reader filtered `email_link_clicked` → 196 real clicks stranded in silver under a name no consumer read; `% Clicks` was 0 everywhere (features-service DIS-132) despite clicks arriving since 2026-05-28 (DIS-58 fixed click *delivery*; this fixed click *visibility*). Guard: the "normalizes webhook 'link_clicked' to canonical 'email_link_clicked'" test in `tests/unit/silver-promote.test.ts`. If Instantly later adds another prefix-less webhook event, add it to `WEBHOOK_EVENT_TYPE_ALIASES` AND a unit case — do NOT pass it verbatim.

### Silver inference (synthetic predecessors)

When a "downstream" event lands (e.g. `email_opened`) without a preceding "upstream" event (e.g. `email_sent`), silver promotion synthesizes the missing predecessor deterministically. Rules:

| Trigger | Inferred predecessors |
|---|---|
| `email_opened` step N | `email_sent` step N |
| `email_link_clicked` step N | `email_opened` step N + `email_sent` step N |
| `reply_received` step N | `email_sent` step N |
| `email_bounced` step N | `email_sent` step N |
| `lead_unsubscribed` step N | `email_sent` step N |
| `email_sent` step N | `email_sent` steps 1..N-1 (sequence cascade) |

Synthetic rows carry `inferred=true`, `source='inferred'`, `inferred_from_event_id` (audit), and `inferred_rule` (rule name). Timestamp = trigger event timestamp.

**One-shot upgrade:** for at-most-1-per-step event types (`email_sent`, `email_bounced`, `lead_unsubscribed`, `reply_received`), partial unique index `instantly_events_one_shot_dedupe_idx` enforces that. When a real webhook arrives after inference projected the event, the synthetic row is upgraded in place (`inferred=true → false`, real timestamp wins). Side effects fire because this is the first time the real signal is observed.

**Side effects on inferred events:** SKIPPED. Synthetic events are projection-only. `delivery_status` / `cost lifecycle` / `reply classification` update only when real external signals arrive.

**Backfill:** `npm run backfill:inferences` re-projects predecessors for every existing real silver event. Idempotent. CLI only — must not run in boot path.
