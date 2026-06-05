# Retry-stuck — event-driven investigation

Date: 2026-05-24
Author: Kevin Lourd

## Question

Can the retry-stuck sweep be driven by an **event** (webhook from Instantly)
instead of by polling local DB state on a heartbeat? In particular, does
Instantly emit a webhook when a campaign's `not_sending_status` changes — i.e.
when Instantly decides it can no longer dispatch on a given account or
campaign?

## Method

1. Inspected the full list of webhook event types Instantly exposes.
2. Cross-checked the existing webhook plumbing in `src/routes/webhooks.ts` and
   the canonical event-type map in `src/lib/silver-promote.ts:DELIVERY_STATUS_MAP`.
3. Confirmed against the public developer docs.

## Findings

Instantly's documented webhook event types (verbatim from
[developer.instantly.ai/webhook-events](https://developer.instantly.ai/webhook-events)
and the
[List available event types](https://developer.instantly.ai/api/v2/webhook/listwebhookeventtypes)
endpoint):

### Email events
- `email_sent`
- `email_opened`
- `reply_received`
- `auto_reply_received`
- `link_clicked` (normalized to canonical `email_link_clicked` in silver — see CLAUDE.md "Webhook event_type normalization"; silver readers must use `email_link_clicked`)
- `email_bounced`
- `lead_unsubscribed`
- `account_error`
- `campaign_completed`

### Lead status events
- `lead_neutral`
- `lead_interested`
- `lead_not_interested`

### Meeting events
- `lead_meeting_booked`
- `lead_meeting_completed`

### Other lead events
- `lead_closed`
- `lead_out_of_office`
- `lead_wrong_person`

### Not emitted
- **No** webhook for `not_sending_status` changes — verified against the
  enumerated list above.
- **No** webhook for workspace daily quota exhaustion.
- **No** webhook for account paused / blocked by Instantly.

The only account-level signal is `account_error`, which has unspecified
triggers in the docs. It is too coarse to drive retry-stuck (no
campaign/lead correlation in the payload — see `src/schemas.ts:WebhookPayloadSchema`).

## Implication

There is no Instantly-side signal that maps 1-to-1 to "this campaign just
flipped from sending to refused-to-send." Any retry-stuck design that wants
to *react* to NSS must derive that state from polling. Two layers in our
codebase already poll:

1. **Reconcile cron** (daily 03:00 UTC, `src/lib/reconcile.ts`) calls
   `getCampaign()` per campaign and writes `not_sending_status` onto
   `instantly_campaigns` via `promoteFromCampaignConfig`. As of 2026-05-24,
   11392 of 12133 active rows had `not_sending_status_seen_at` updated within
   the last 24h (≈94% coverage per cycle).
2. **Webhook silver layer** (`src/lib/silver-promote.ts`) advances
   `delivery_status` on every `email_sent` / `reply_received` / etc. event,
   so a row stays in `contacted` only when *no* "lead-alive" event has been
   observed.

## Recommendation

**Keep polling. Drive retry-stuck purely off local state.**

The new heartbeat worker (`src/lib/retry-stuck-worker.ts`) ticks every
10 minutes and selects rows where:

```sql
WHERE c.delivery_status = 'contacted'
  AND c.status = 'active'
  AND c.created_at < NOW() - INTERVAL '72 hours'
  AND NOT EXISTS (
    SELECT 1 FROM instantly_events e
    WHERE e.campaign_id = c.instantly_campaign_id
      AND e.event_type IN (
        'email_sent', 'email_opened', 'email_link_clicked',
        'reply_received', 'auto_reply_received',
        'email_bounced', 'lead_unsubscribed'
      )
  )
```

This is **belt-and-suspenders**: the column gate catches the column;
the silver `NOT EXISTS` gate catches the rare cases where webhooks +
reconcile both missed a real `email_sent` (≈6% of currently-stuck rows in
prod on 2026-05-24 — 111 of 1781). No Instantly call needed for the
selection itself; the only Instantly call per row is `getCampaign()` to
recover the sequence (subject + step bodies + delays) so we can re-send on
a different account.

## What we'd revisit if Instantly ships an NSS webhook

- Subscribe via the Instantly dashboard webhook config UI (same surface as
  current `email_sent` etc. — see `src/routes/webhooks.ts:/instantly/config`).
- Add the new event type to `WebhookPayloadSchema` (Zod) and the
  `DELIVERY_STATUS_MAP` in silver-promote.
- Bronze-write the raw payload via `insertWebhookPayload`.
- Optionally: trigger an immediate retry-stuck tick on receipt instead of
  waiting for the next 10-min heartbeat.

For now: keep the polling. The heartbeat already converges fast enough
(100 rows × 10 min = 600 rows/hour throughput per worker) and the silver
gate prevents the false-positive class entirely.

## References

- Instantly developer docs: [Webhook events](https://developer.instantly.ai/webhook-events)
- Instantly API: [List webhook event types](https://developer.instantly.ai/api/v2/webhook/listwebhookeventtypes)
- Internal: `src/lib/silver-promote.ts:DELIVERY_STATUS_MAP`
- Internal: `src/schemas.ts:WebhookPayloadSchema`
- Internal: `src/routes/webhooks.ts`
