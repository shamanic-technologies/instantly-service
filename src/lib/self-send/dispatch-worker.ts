/**
 * The dispatch worker — IO glue around the pure decisions in `dispatch.ts`.
 *
 * Reads what is due, sends it, and records what happened. It owns no scheduling
 * rules of its own: which step, when, and how many all come from the pure module,
 * and the consequences of a send come from the existing `promoteEvent` path.
 *
 * Idempotent per step by construction. A step leaves the due set the moment its
 * hold stops being `provisioned`, which `handleEmailSent` does as part of
 * promoting the `email_sent` event — so a worker that dies mid-run re-selects
 * only the steps that never got that far.
 */

import { sql } from "drizzle-orm";

import { db } from "../../db";
import { smtpDispatchRaw } from "../../db/schema";
import { rampCapForAge, IN_PRODUCTION_DAILY_LIMIT } from "../account-lifecycle";
import { promoteEvent } from "../silver-promote";
import type { Account } from "../instantly-client";
import type { CallerInfo } from "../key-client";
import { resolveMailboxCredential, type MailboxCredential } from "./mailbox-credentials";
import { buildMessage } from "./message";
import { dispatchMessage, SmtpDispatchError } from "./smtp";
import {
  classifyPermanentFailure,
  selectDueSteps,
  type AccountCapacity,
  type PendingSequence,
} from "./dispatch";
import { SEND_TRANSPORT_SMTP } from "./transport";

const CALLER: CallerInfo = { method: "POST", path: "/internal/self-send/dispatch" };

export interface DispatchSummary {
  sequencesRead: number;
  due: number;
  sent: number;
  /** Permanent, about the RECIPIENT — promoted as a bounce. */
  bounced: number;
  /** Permanent, about US — recorded, lead untouched, hold left alone. */
  senderBlocked: number;
  /** Retryable; the step stays due for the next run. */
  transient: number;
  failed: number;
}

/**
 * Everything outstanding on the self-send transport.
 *
 * The queue is the still-`provisioned` cost ledger, gated exactly as
 * `loadPendingLeads` gates it (active campaign, delivery_status contacted/sent)
 * plus `send_transport='smtp'` — the DECISION frozen on the campaign row, never
 * the account's live policy, so flipping a mailbox cannot divert sequences
 * already in flight.
 *
 * Delays come back as `jsonb_agg`, not a native array: node-postgres hands back a
 * `numeric[]` as its RAW TEXT form (`"{3,7,0}"`), which is truthy, survives
 * `?? []`, and then throws on `.map`. This exact trap took down the whole fleet
 * forecast once (v0.59.1).
 */
async function loadPendingSequences(): Promise<PendingSequence[]> {
  const result = await db.execute(sql`
    WITH pending AS (
      SELECT
        c.instantly_campaign_id,
        c.lead_email,
        c.account_email,
        COALESCE(
          jsonb_agg(DISTINCT sc.step) FILTER (WHERE sc.status = 'provisioned'),
          '[]'::jsonb
        ) AS provisioned_steps,
        MAX(sc.step) FILTER (WHERE sc.status = 'actual') AS last_sent_step,
        MAX(sc.updated_at) FILTER (WHERE sc.status = 'actual') AS last_sent_at
      FROM sequence_costs sc
      JOIN instantly_campaigns c
        ON c.instantly_campaign_id = sc.instantly_campaign_id
      WHERE c.send_transport = ${SEND_TRANSPORT_SMTP}
        AND c.status = 'active'
        AND c.delivery_status IN ('contacted', 'sent')
        AND c.account_email IS NOT NULL
      GROUP BY c.instantly_campaign_id, c.lead_email, c.account_email
      HAVING COUNT(*) FILTER (WHERE sc.status = 'provisioned') > 0
    )
    SELECT
      p.instantly_campaign_id AS "instantlyCampaignId",
      p.lead_email            AS "leadEmail",
      p.account_email         AS "accountEmail",
      p.provisioned_steps     AS "provisionedSteps",
      p.last_sent_step        AS "lastSentStep",
      p.last_sent_at          AS "lastSentAt",
      (
        SELECT COALESCE(jsonb_agg(s.delay_days ORDER BY s.step), '[]'::jsonb)
        FROM sequence_steps s
        WHERE s.instantly_campaign_id = p.instantly_campaign_id
      ) AS "stepDelays"
    FROM pending p
  `);

  return (result.rows as Record<string, unknown>[]).map((row) => ({
    instantlyCampaignId: String(row.instantlyCampaignId),
    leadEmail: String(row.leadEmail),
    accountEmail: String(row.accountEmail),
    // Guard the shape rather than trust it: a surprise degrades one row instead
    // of throwing the whole sweep.
    provisionedSteps: Array.isArray(row.provisionedSteps)
      ? (row.provisionedSteps as number[])
      : [],
    lastSentStep: row.lastSentStep === null ? null : Number(row.lastSentStep),
    lastSentAt: row.lastSentAt ? new Date(row.lastSentAt as string) : null,
    stepDelays: Array.isArray(row.stepDelays)
      ? (row.stepDelays as (number | null)[])
      : [],
  }));
}

/**
 * Room left on each mailbox today.
 *
 * `cap` is the same `min(daily_limit, rampCapForAge)` the Instantly path
 * enforces, and `sentToday` counts REAL (`inferred=false`) `email_sent` events
 * in the current UTC day — the same definition the account-health table shows,
 * so the two surfaces cannot disagree about how loaded a mailbox is.
 *
 * Accounts absent from this result get no capacity row at all, which
 * `selectDueSteps` reads as no room. That is deliberate: a mailbox whose limits
 * we could not establish must not be sent from.
 */
async function loadSendingAccounts(
  asOf: Date,
): Promise<{ capacities: AccountCapacity[]; accounts: Map<string, Account> }> {
  const result = await db.execute(sql`
    SELECT
      a.email                                   AS "accountEmail",
      a.first_name                              AS "firstName",
      a.last_name                               AS "lastName",
      a.daily_limit                             AS "dailyLimit",
      a.timestamp_created                       AS "timestampCreated",
      COALESCE((
        SELECT COUNT(*)
        FROM instantly_events e
        WHERE e.account_email = a.email
          AND e.event_type = 'email_sent'
          AND e.inferred = false
          AND e.timestamp >= date_trunc('day', now() AT TIME ZONE 'UTC')
      ), 0)                                     AS "sentToday"
    FROM instantly_accounts a
    WHERE a.send_transport = ${SEND_TRANSPORT_SMTP}
      AND a.lifecycle_status = 'in_production'
      AND a.absent_since IS NULL
  `);

  const capacities: AccountCapacity[] = [];
  const accounts = new Map<string, Account>();

  for (const row of result.rows as Record<string, unknown>[]) {
    const email = String(row.accountEmail);
    const dailyLimit = row.dailyLimit === null ? 0 : Number(row.dailyLimit);
    const created = row.timestampCreated ? new Date(row.timestampCreated as string) : null;

    // The age ramp is computed off the CONSTANT, never off the live daily_limit:
    // scaling an already-ramped value by age again halves it every sweep. `min`
    // keeps both enforcement points idempotent while still honouring an
    // operator-set limit BELOW the age cap.
    const rampCap = rampCapForAge(created, IN_PRODUCTION_DAILY_LIMIT, asOf);

    capacities.push({
      accountEmail: email,
      cap: Math.min(dailyLimit, rampCap),
      sentToday: Number(row.sentToday),
    });

    // The real account, so the From display name and the signature agree — the
    // same multi-persona coherence `buildDefaultSignature` exists for. A
    // fabricated account here would sign every email with the fallback name.
    accounts.set(email, {
      email,
      warmup_status: 1,
      status: 1,
      first_name: typeof row.firstName === "string" ? row.firstName : undefined,
      last_name: typeof row.lastName === "string" ? row.lastName : undefined,
    } as Account);
  }

  return { capacities, accounts };
}

/** Body + subject for one step, plus the thread it belongs to. */
async function loadStepContent(
  instantlyCampaignId: string,
  step: number,
): Promise<{ subject: string; bodyHtml: string; priorMessageIds: string[] } | null> {
  const content = await db.execute(sql`
    SELECT
      (SELECT s.body_html FROM sequence_steps s
        WHERE s.instantly_campaign_id = ${instantlyCampaignId} AND s.step = ${step}) AS "bodyHtml",
      (SELECT s.subject FROM sequence_steps s
        WHERE s.instantly_campaign_id = ${instantlyCampaignId} AND s.step = 1) AS "subject",
      COALESCE((
        SELECT jsonb_agg(d.message_id ORDER BY d.step, d.dispatched_at)
        FROM smtp_dispatch_raw d
        WHERE d.instantly_campaign_id = ${instantlyCampaignId}
          AND d.outcome = 'sent'
          AND d.message_id IS NOT NULL
      ), '[]'::jsonb) AS "priorMessageIds"
  `);

  const row = (content.rows as Record<string, unknown>[])[0];
  if (!row || typeof row.bodyHtml !== "string" || row.bodyHtml === "") return null;

  return {
    bodyHtml: row.bodyHtml,
    subject: typeof row.subject === "string" ? row.subject : "",
    priorMessageIds: Array.isArray(row.priorMessageIds)
      ? (row.priorMessageIds as string[])
      : [],
  };
}

async function recordDispatch(values: {
  instantlyCampaignId: string;
  leadEmail: string;
  accountEmail: string;
  step: number;
  outcome: "sent" | "permanent" | "transient";
  messageId?: string | null;
  responseCode?: number | null;
  response?: string | null;
  payload: unknown;
}): Promise<string> {
  const [row] = await db
    .insert(smtpDispatchRaw)
    .values({
      instantlyCampaignId: values.instantlyCampaignId,
      leadEmail: values.leadEmail,
      accountEmail: values.accountEmail,
      step: values.step,
      outcome: values.outcome,
      messageId: values.messageId ?? null,
      responseCode: values.responseCode ?? null,
      response: values.response ?? null,
      payload: values.payload as object,
    })
    .returning({ id: smtpDispatchRaw.id });

  return row.id;
}

/**
 * Send everything due now.
 *
 * Fail-loud PER STEP: one lead's failure is recorded and counted, and the sweep
 * continues — a single dead recipient domain must not stop the fleet's sending
 * for the day. Nothing is swallowed; every outcome lands in bronze.
 */
export async function runDispatch(options: { limit?: number; asOf?: Date } = {}): Promise<DispatchSummary> {
  const asOf = options.asOf ?? new Date();

  const sequences = await loadPendingSequences();
  const { capacities, accounts } = await loadSendingAccounts(asOf);

  const due = selectDueSteps(sequences, capacities, asOf);
  const batch = options.limit ? due.slice(0, options.limit) : due;

  const summary: DispatchSummary = {
    sequencesRead: sequences.length,
    due: due.length,
    sent: 0,
    bounced: 0,
    senderBlocked: 0,
    transient: 0,
    failed: 0,
  };

  // One credential lookup per mailbox per run, not per send: the vendor call
  // returns the whole fleet, so paying it per message would be dozens of
  // identical round-trips for a single sweep.
  const credentials = new Map<string, MailboxCredential>();

  for (const step of batch) {
    try {
      const content = await loadStepContent(step.instantlyCampaignId, step.step);
      if (!content) {
        // No body persisted for this step. Fail loud and leave the hold — sending
        // an empty email, or guessing at content, is worse than not sending.
        console.error(
          `[instantly-service] self-send: no body for campaign=${step.instantlyCampaignId} step=${step.step} — skipped`,
        );
        summary.failed += 1;
        continue;
      }

      let credential = credentials.get(step.accountEmail);
      if (!credential) {
        credential = await resolveMailboxCredential(step.accountEmail, CALLER);
        credentials.set(step.accountEmail, credential);
      }

      const account = accounts.get(step.accountEmail);
      if (!account) {
        // The step's account is not an eligible sender right now (demoted, gone
        // absent, off the smtp transport). Selection should already have skipped
        // it for want of a capacity row; failing loud here rather than
        // fabricating an account keeps the two in agreement.
        console.error(
          `[instantly-service] self-send: no sending account for ${step.accountEmail} — skipped`,
        );
        summary.failed += 1;
        continue;
      }

      const message = buildMessage({
        account,
        leadEmail: step.leadEmail,
        subject: content.subject,
        bodyHtml: content.bodyHtml,
        step: step.step,
        identity: {
          instantlyCampaignId: step.instantlyCampaignId,
          leadEmail: step.leadEmail,
        },
        previousMessageId: content.priorMessageIds.at(-1) ?? null,
        priorMessageIds: content.priorMessageIds,
      });

      const result = await dispatchMessage(credential, message);

      const sourceRowId = await recordDispatch({
        instantlyCampaignId: step.instantlyCampaignId,
        leadEmail: step.leadEmail,
        accountEmail: step.accountEmail,
        step: step.step,
        outcome: "sent",
        messageId: result.messageId,
        response: result.response,
        payload: result,
      });

      // Real, not inferred — this is what actualizes the hold via handleEmailSent.
      await promoteEvent({
        eventType: "email_sent",
        instantlyCampaignId: step.instantlyCampaignId,
        leadEmail: step.leadEmail,
        accountEmail: step.accountEmail,
        step: step.step,
        variant: null,
        timestamp: new Date(),
        source: "self_send",
        sourceRowId,
      });

      summary.sent += 1;
    } catch (error) {
      if (!(error instanceof SmtpDispatchError)) {
        console.error(
          `[instantly-service] self-send: campaign=${step.instantlyCampaignId} step=${step.step} failed: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
        summary.failed += 1;
        continue;
      }

      const sourceRowId = await recordDispatch({
        instantlyCampaignId: step.instantlyCampaignId,
        leadEmail: step.leadEmail,
        accountEmail: step.accountEmail,
        step: step.step,
        outcome: error.kind,
        responseCode: error.responseCode,
        response: error.response,
        payload: { kind: error.kind, responseCode: error.responseCode, response: error.response },
      });

      if (error.kind === "transient") {
        console.warn(
          `[instantly-service] self-send: transient refusal campaign=${step.instantlyCampaignId} step=${step.step} — retried next run: ${error.response}`,
        );
        summary.transient += 1;
        continue;
      }

      const subject = classifyPermanentFailure(error.response, error.responseCode);

      if (subject === "sender") {
        // Refused because of US, not the prospect. Promoting a bounce here would
        // record a fact about our own mailbox on a perfectly reachable lead, and
        // mark them undeliverable forever. The hold stays; account health owns
        // the mailbox side of this.
        console.warn(
          `[instantly-service] self-send: SENDER blocked account=${step.accountEmail} campaign=${step.instantlyCampaignId} step=${step.step} — lead untouched: ${error.response}`,
        );
        summary.senderBlocked += 1;
        continue;
      }

      await promoteEvent({
        eventType: "email_bounced",
        instantlyCampaignId: step.instantlyCampaignId,
        leadEmail: step.leadEmail,
        accountEmail: step.accountEmail,
        step: step.step,
        variant: null,
        timestamp: new Date(),
        source: "self_send",
        sourceRowId,
      });

      console.warn(
        `[instantly-service] self-send: bounced campaign=${step.instantlyCampaignId} step=${step.step}: ${error.response}`,
      );
      summary.bounced += 1;
    }
  }

  console.log(
    `[instantly-service] self-send-dispatch: done ${JSON.stringify(summary)}`,
  );

  return summary;
}
