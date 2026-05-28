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

1. `account.signature` — per-sender override configured in Instantly's UI (account settings).
2. `DEFAULT_SIGNATURE` constant in `src/lib/send-lead.ts` — service-wide fallback. **This is the source of truth in prod**: per-account UI signatures are intentionally empty so every sender shares one canonical signature.

The fallback is intentionally hardcoded (not env-var driven). When the signature copy changes, edit `DEFAULT_SIGNATURE` and ship a hotfix — that's the canonical write-path. Do NOT re-add per-account signatures in Instantly UI without updating this rule — code will prefer the UI value over the hardcoded default.

`stripAccountSignature` is HTML-tolerant. It looks for the EARLIEST occurrence of any of these standalone `--` markers (RFC 3676 sig delimiter, in its plain or HTML-wrapped forms):

- `\n\n--\n` plain (with optional trailing space, e.g. `\n\n-- \n`)
- `<p>--</p>` paragraph-wrapped (with optional `&nbsp;`)
- `<br>--<br>` line-break-wrapped
- `<div>--</div>` div-wrapped

…and slices everything from the marker onward. If no marker matches, body is returned unchanged. Senders whose original body legitimately contains one of these markers will lose content past that point on a re-send — accepted edge-case.

Historic bug 2026-05-28: the original `stripAccountSignature` matched only the plain `\n\n--\n` marker. Bodies stored as HTML never matched, so every retry-stuck re-send appended a fresh signature on top of the existing one. A row redispatched 72 times shipped a body with 72 stacked signatures. Fix: HTML-tolerant marker list + strip-then-append in `buildEmailBodyWithSignature`. Guard: this section + the `stripAccountSignature` and `buildEmailBodyWithSignature` cumulative-stack tests in `tests/unit/send.test.ts`.

If you add a new HTML wrapper form (e.g. `<section>--</section>`), add a regex to `SIG_MARKERS` AND a unit test case covering both the marker shape and a 3-stacked case for that shape.

## Data layering — Bronze / Silver / Gold

Three layers, doctrine per `~/.claude/skills/data-layering/SKILL.md`:

- **Bronze** — raw external mirrors, append-only, never mutated. Tables: `instantly_webhook_payloads_raw`, `instantly_analytics_raw`, `instantly_emails_raw`, `instantly_leads_raw`, `instantly_campaigns_config_raw`. Each row = one payload from Instantly (webhook OR reconcile poll).
- **Silver** — canonical event log `instantly_events` + state row `instantly_campaigns`. Derived from bronze via `src/lib/silver-promote.ts`. Rebuildable.
- **Gold** — stats views in `src/routes/analytics.ts`, `status.ts`. Read silver only.

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
