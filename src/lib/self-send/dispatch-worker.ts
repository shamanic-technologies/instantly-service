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
import { fetchRecentDailyVolume, sustainedForMailbox } from "../recent-send-volume";
import { promoteEvent } from "../silver-promote";
import type { Account } from "../instantly-client";
import type { CallerInfo } from "../key-client";
import {
  loadMailboxLogins,
  resolveMailboxCredential,
  type MailboxCredential,
} from "./mailbox-credentials";
import {
  loadPendingScheduledReplies,
  selectDueScheduledReplies,
} from "../scheduled-replies";
import { buildMessage } from "./message";
import { runPoll } from "./imap-poller";
import { dispatchMessage, SmtpDispatchError } from "./smtp";
import {
  classifyPermanentFailure,
  selectDueSteps,
  type AccountCapacity,
  type PendingSequence,
} from "./dispatch";
import {
  selectSilencedSmtpSenders,
  type SmtpFailureRow,
  type SmtpSenderHealth,
} from "./sender-health";
import { SEND_TRANSPORT_SMTP } from "./transport";
import { dispatchScheduledReplies } from "../scheduled-replies-worker";

const CALLER: CallerInfo = { method: "POST", path: "/internal/self-send/dispatch" };

/** `db.execute` resolves a QueryResult on node-postgres, never a bare array. */
function rowsOf(result: unknown): Record<string, unknown>[] {
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

export interface DispatchSummary {
  sequencesRead: number;
  /** Steps this run will actually send — what is left AFTER capacity clips. */
  due: number;
  /**
   * Steps that were due before capacity clipped them. `dueBeforeCapacity` far
   * above `due` means the fleet is THROTTLED, not idle — a distinction the
   * summary could not make until now, and whose absence hid a backlog of 1,208
   * un-started sequences behind an hourly `due: 0` for eight days.
   */
  dueBeforeCapacity: number;
  /**
   * Steps assigned to a mailbox we hold no credential for. These are not slow,
   * they are stranded: no cap ever grows into them. Needs a credential, not
   * patience.
   */
  blockedNoCapacityRow: number;
  /**
   * Steps whose mailbox the relay has been refusing, skipped this run.
   *
   * Distinct from `blockedNoCapacityRow`: there we hold no credential, here we
   * hold one and it is being refused. A non-zero value that persists means a
   * mailbox needs fixing or retiring — see `sender-health.ts`.
   */
  skippedSilenced: number;
  /**
   * First emails moved off a mailbox that could not send them onto a production
   * mailbox with room — see `selectDueSteps`. Each move is persisted, with its
   * reason, on the campaign row before the email goes out.
   */
  rehomed: number;
  sent: number;
  /** Permanent, about the RECIPIENT — promoted as a bounce. */
  bounced: number;
  /** Permanent, about US — recorded, lead untouched, hold left alone. */
  senderBlocked: number;
  /** Retryable; the step stays due for the next run. */
  transient: number;
  failed: number;
  /**
   * Answers to prospects who wrote back, held until their own business hours.
   *
   * Drained by the SAME run, from the SAME window, deliberately: a reply is the
   * one message where landing at 23:05 local reads worst, and giving it its own
   * schedule would be a second set of rules to keep in step with this one. It
   * is NOT a sequence step — no hold, no step number, no capacity consumed.
   */
  repliesDue: number;
  repliesSent: number;
  repliesFailed: number;
  /**
   * Whether this run read the mailboxes.
   *
   * False when the run found nothing it could send and returned without the
   * poll — see the probe in `runDispatch`. It is NOT a relaxation of the
   * read-before-send ordering: a run that sends anything always polls first.
   */
  polled: boolean;
  /**
   * True when another sweep was already running and this one did nothing.
   *
   * ⚠️ Load-bearing rather than cosmetic. Three things trigger this sweep — the
   * in-process interval, the cron's `POST /internal/self-send/dispatch`, and a
   * hand-run — and two concurrent ones would read the SAME still-`provisioned`
   * ledger and both select the same step, which is how one prospect gets two
   * copies of the same email. The mutex is what makes the extra trigger free.
   */
  skippedConcurrent: boolean;
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
        MAX(sc.updated_at) FILTER (WHERE sc.status = 'actual') AS last_sent_at,
        MIN(c.timezone) AS timezone,
        MIN(c.created_at) AS queued_at
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
      p.timezone              AS "timezone",
      p.queued_at             AS "queuedAt",
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
    // Decides the prospect's business-hours window. Null on a row written before
    // migration 0046; the fleet default then applies, which is the same zone the
    // Instantly schedule degrades to.
    timezone: row.timezone === null || row.timezone === undefined ? null : String(row.timezone),
    // When the lead was handed to us: what a never-sent first email is due from.
    queuedAt: toDate(row.queuedAt),
  }));
}

/**
 * Room left on each mailbox today.
 *
 * `cap` is the same `min(daily_limit, rampCapForVolume)` the Instantly path
 * enforces, and `sentToday` counts REAL (`inferred=false`) `email_sent` events
 * in the current UTC day — the same definition the account-health table shows,
 * so the two surfaces cannot disagree about how loaded a mailbox is.
 *
 * Accounts absent from this result get no capacity row at all, which
 * `selectDueSteps` reads as no room. That is deliberate: a mailbox whose limits
 * we could not establish must not be sent from — and it is now the ONLY reason
 * an account is excluded here, which is the point of the two filters this query
 * used to carry:
 *
 *   - `a.send_transport = 'smtp'` — the ACCOUNT's policy. The decision is
 *     already frozen on the campaign row; re-reading a second column at delivery
 *     time asked the same question twice and got a different answer. Prod
 *     2026-09-06: 1,486 sequences the assignment step had marked `smtp` sat on
 *     accounts whose policy column still said `instantly`, so they matched no
 *     capacity row and were never due — 1,176 prospects who had never received
 *     a first email, silently, with the worker reporting `due: 0` and no errors.
 *
 *   - `a.lifecycle_status = 'in_production'` — the lifecycle governs which
 *     mailbox a NEW sequence is ASSIGNED to (`fetchInProductionAccounts`), not
 *     whether an already-assigned one may finish. Re-reading it here meant every
 *     demotion silently froze the sequences already riding that mailbox: the
 *     50 demotions of 2026-08-29 and the 25 of 09-05 stranded 1,031 of the
 *     sequences above. The Instantly transport has never behaved this way — it
 *     keeps dispatching whatever our lifecycle says — so the two pipes disagreed
 *     about the same mailbox.
 *
 * Capacity is emitted per ACCOUNT but spent per MAILBOX; `selectDueSteps` does
 * the grouping. See {@link AccountCapacity.mailbox}.
 */
/**
 * node-postgres hands a `timestamp` column back as a naive string, which
 * `new Date()` reads as LOCAL time. The container runs UTC so the two agree
 * today, but the comparison below decides whether a mailbox keeps sending —
 * it must not depend on that.
 */
function toDate(value: unknown): Date | null {
  if (value instanceof Date) return value;
  if (typeof value !== "string" || value.trim() === "") return null;
  const iso = value.includes("T") ? value : value.replace(" ", "T");
  const parsed = new Date(/[Zz]|[+-]\d{2}:?\d{2}$/.test(iso) ? iso : `${iso}Z`);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

/**
 * Each real mailbox's recent dispatch record, for the silence rule.
 *
 * Two grouped queries rather than one scan of the rows: the failures collapse
 * to a handful of DISTINCT `(response, response_code)` pairs per mailbox (4,210
 * prod failures over three days were ONE message), so the classification runs
 * over a few rows instead of thousands.
 *
 * Grouped by real MAILBOX, not by address: aliases share one relay login, so a
 * refusal of one is a refusal of all of them. An address the login map does not
 * know is its own mailbox — the same reading every other consumer takes.
 */
async function loadSmtpSenderHealth(
  mailboxLogins: ReadonlyMap<string, string>,
): Promise<SmtpSenderHealth[]> {
  const [sentResult, failureResult] = await Promise.all([
    db.execute(sql`
      SELECT account_email        AS "accountEmail",
             MAX(dispatched_at)   AS "lastSuccessAt"
      FROM smtp_dispatch_raw
      WHERE dispatched_at > now() - interval '7 days'
        AND outcome = 'sent'
      GROUP BY 1
    `),
    db.execute(sql`
      SELECT account_email        AS "accountEmail",
             response             AS "response",
             response_code        AS "responseCode",
             COUNT(*)::int        AS "n",
             MIN(dispatched_at)   AS "firstAt"
      FROM smtp_dispatch_raw
      WHERE dispatched_at > now() - interval '7 days'
        AND outcome = 'permanent'
      GROUP BY 1, 2, 3
    `),
  ]);

  const mailboxOf = (accountEmail: string): string =>
    mailboxLogins.get(accountEmail.trim().toLowerCase()) ??
    accountEmail.trim().toLowerCase();

  const health = new Map<
    string,
    { mailbox: string; lastSuccessAt: Date | null; failures: SmtpFailureRow[] }
  >();
  const at = (accountEmail: string) => {
    const mailbox = mailboxOf(accountEmail);
    const existing = health.get(mailbox);
    if (existing) return existing;
    const fresh = {
      mailbox,
      lastSuccessAt: null as Date | null,
      failures: [] as SmtpFailureRow[],
    };
    health.set(mailbox, fresh);
    return fresh;
  };

  for (const row of rowsOf(sentResult)) {
    const entry = at(String(row.accountEmail));
    const at_ = toDate(row.lastSuccessAt);
    // An alias group takes the LATEST success across its aliases: they share one
    // relay login, so any of them getting through means the login works.
    if (at_ !== null && (entry.lastSuccessAt === null || at_ > entry.lastSuccessAt)) {
      entry.lastSuccessAt = at_;
    }
  }
  for (const row of rowsOf(failureResult)) {
    const firstAt = toDate(row.firstAt);
    // A refusal we cannot date cannot be compared against a success, and
    // treating it as "now" would silence on a stale failure. Skip it loudly
    // rather than guess — the count is what drives the threshold anyway.
    if (firstAt === null) {
      console.warn(
        `[instantly-service] self-send-dispatch: undatable permanent failure for ${String(row.accountEmail)}, not counted toward silencing`,
      );
      continue;
    }
    at(String(row.accountEmail)).failures.push({
      response: row.response === null || row.response === undefined
        ? ""
        : String(row.response),
      responseCode:
        row.responseCode === null || row.responseCode === undefined
          ? null
          : Number(row.responseCode),
      count: Number(row.n ?? 0),
      firstAt,
    });
  }

  return [...health.values()];
}

async function loadSendingAccounts(
  asOf: Date,
  mailboxLogins: ReadonlyMap<string, string>,
): Promise<{ capacities: AccountCapacity[]; accounts: Map<string, Account> }> {
  const [result, volume] = await Promise.all([
    db.execute(sql`
    SELECT
      a.email                                   AS "accountEmail",
      a.first_name                              AS "firstName",
      a.last_name                               AS "lastName",
      a.daily_limit                             AS "dailyLimit",
      a.lifecycle_status                        AS "lifecycleStatus",
      COALESCE((
        SELECT COUNT(*)
        FROM instantly_events e
        WHERE e.account_email = a.email
          AND e.event_type = 'email_sent'
          AND e.inferred = false
          AND e.timestamp >= date_trunc('day', now() AT TIME ZONE 'UTC')
      ), 0)                                     AS "sentToday",
      -- Warmup mail comes out of the SAME Gmail per-user quota as outreach, so
      -- it has to be counted here or the two jobs each spend the full cap and
      -- the mailbox is pushed into the 550-5.4.5 daily-user-sending-limit
      -- refusal the age ramp exists to respect. The mesh applies
      -- the mirror of this rule (it takes what is left AFTER outreach).
      COALESCE((
        SELECT COUNT(*)
        FROM warmup_dispatches w
        WHERE w.sender_email = a.email
          AND w.outcome = 'sent'
          AND w.dispatched_at >= date_trunc('day', now() AT TIME ZONE 'UTC')
      ), 0)                                     AS "warmupToday"
    FROM instantly_accounts a
    WHERE a.absent_since IS NULL
  `),
    fetchRecentDailyVolume(),
  ]);

  const capacities: AccountCapacity[] = [];
  const accounts = new Map<string, Account>();

  // The mailbox's peak is the highest DAILY TOTAL across its aliases, computed
  // once here because this is where the alias map lives. Each alias's capacity
  // row then carries the same mailbox figure, so `selectDueSteps` never has to
  // combine them — summing per-alias peaks would over-state a mailbox whose
  // aliases peaked on different days, and over-stating a quota ramp is the one
  // direction that pushes a mailbox past what the relay accepts.
  const addressesByMailbox = new Map<string, string[]>();
  for (const row of rowsOf(result)) {
    const email = String(row.accountEmail);
    const mailbox = mailboxLogins.get(email.trim().toLowerCase());
    if (mailbox === undefined) continue;
    const group = addressesByMailbox.get(mailbox) ?? [];
    group.push(email);
    addressesByMailbox.set(mailbox, group);
  }
  const peakByMailbox = new Map<string, number>();
  for (const [mailbox, addresses] of addressesByMailbox) {
    peakByMailbox.set(mailbox, sustainedForMailbox(volume, addresses));
  }

  for (const row of rowsOf(result)) {
    const email = String(row.accountEmail);

    // The credential is what makes a mailbox sendable at all, and it is the same
    // positive evidence the assignment step derives the transport from. No
    // credential ⇒ no capacity row ⇒ no room, per the invariant above.
    const mailbox = mailboxLogins.get(email.trim().toLowerCase());
    if (mailbox === undefined) continue;

    // The OPERATOR limit only. The ramp is applied at mailbox grain inside
    // `selectDueSteps`, which is where the aliases are summed — see
    // `AccountCapacity.cap`.
    const dailyLimit = row.dailyLimit === null ? 0 : Number(row.dailyLimit);

    capacities.push({
      accountEmail: email,
      mailbox,
      cap: dailyLimit,
      recentSustainedDaily: peakByMailbox.get(mailbox) ?? 0,
      sentToday: Number(row.sentToday) + Number(row.warmupToday ?? 0),
      // Same gate as the assignment of a NEW sequence: only a production mailbox
      // may take over a first email another mailbox cannot send.
      adoptsFirstEmails: row.lifecycleStatus === "in_production",
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

/**
 * Move a never-sent sequence to another mailbox, recording why.
 *
 * Returns false (and moves nothing) when the row no longer sits on `from` or
 * any email of the sequence has already gone out — both mean a thread may now
 * exist, and a thread must stay on one sender.
 */
async function rehomeFirstEmail(
  instantlyCampaignId: string,
  from: string,
  to: string,
  asOf: Date,
): Promise<boolean> {
  const entry = JSON.stringify({
    from,
    to,
    at: asOf.toISOString(),
    reason: "first_email_stranded",
  });
  const result = await db.execute(sql`
    UPDATE instantly_campaigns c
    SET account_email = ${to},
        metadata = jsonb_set(
          COALESCE(c.metadata, '{}'::jsonb),
          '{rehomed}',
          COALESCE(c.metadata->'rehomed', '[]'::jsonb) || jsonb_build_array(${entry}::jsonb)
        ),
        updated_at = now()
    WHERE c.instantly_campaign_id = ${instantlyCampaignId}
      AND c.account_email = ${from}
      AND c.status = 'active'
      AND NOT EXISTS (
        SELECT 1 FROM smtp_dispatch_raw d
        WHERE d.instantly_campaign_id = c.instantly_campaign_id AND d.outcome = 'sent'
      )
      AND NOT EXISTS (
        SELECT 1 FROM sequence_costs sc
        WHERE sc.instantly_campaign_id = c.instantly_campaign_id AND sc.status = 'actual'
      )
    RETURNING c.id
  `);
  return rowsOf(result).length > 0;
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
        SELECT jsonb_agg(d.message_id ORDER BY d.dispatched_at)
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
/**
 * At most ONE sweep at a time, across every trigger.
 *
 * Module-level rather than per-caller precisely because the callers are plural:
 * the in-process interval, the cron POST and a manual POST all land here, and a
 * guard held by any one of them would not see the others. The queue is the set
 * of `provisioned` holds, and a hold only leaves that set once its `email_sent`
 * has been promoted — so two overlapping sweeps genuinely select the same step
 * and genuinely send it twice.
 */
let dispatchInFlight = false;

/** Exposed for tests only; never call this from application code. */
export function __resetDispatchInFlight(): void {
  dispatchInFlight = false;
}

function emptySummary(): DispatchSummary {
  return {
    sequencesRead: 0,
    due: 0,
    dueBeforeCapacity: 0,
    blockedNoCapacityRow: 0,
    skippedSilenced: 0,
    rehomed: 0,
    sent: 0,
    bounced: 0,
    senderBlocked: 0,
    transient: 0,
    failed: 0,
    repliesDue: 0,
    repliesSent: 0,
    repliesFailed: 0,
    polled: false,
    skippedConcurrent: false,
  };
}

export async function runDispatch(
  options: { limit?: number; asOf?: Date; pollFirst?: boolean } = {},
): Promise<DispatchSummary> {
  if (dispatchInFlight) {
    console.log(
      "[instantly-service] self-send-dispatch: skipped, a sweep is already running",
    );
    return { ...emptySummary(), skippedConcurrent: true };
  }
  dispatchInFlight = true;
  try {
    return await runDispatchExclusive(options);
  } finally {
    dispatchInFlight = false;
  }
}

async function runDispatchExclusive(
  options: { limit?: number; asOf?: Date; pollFirst?: boolean },
): Promise<DispatchSummary> {
  const asOf = options.asOf ?? new Date();

  // Read the mailboxes BEFORE deciding what to send, in the same run and
  // awaited. A prospect who replied since the last sweep has their sequence
  // stopped by the poll, so they are already out of the queue by the time we
  // select — we never email someone who has already answered.
  //
  // This has to happen HERE rather than as an earlier cron step: both endpoints
  // answer 202 and work in the background, so a separate poll step would still
  // be running while dispatch selected, and the ordering would be hoped-for
  // rather than real. A poll failure is logged and does not block the send — the
  // worst case is one extra email to someone who replied within the window,
  // which is the same latency the webhook path already carries.
  // Read once for the whole sweep, not per mailbox: this is a key-service read
  // plus a vendor pagination, and it answers both "may we send from here at all"
  // and "which real mailbox does this alias spend the quota of".
  //
  // Fails LOUD — a sweep that cannot establish the fleet's credentials must stop,
  // not quietly send nothing and report a clean run.
  const mailboxLogins = await loadMailboxLogins(CALLER);

  // Which mailboxes the relay has been refusing outright. Read ONCE for the
  // sweep — it is two grouped queries over a 7-day window, and the answer is a
  // property of the fleet, not of a step.
  const silencedMailboxes = selectSilencedSmtpSenders(
    await loadSmtpSenderHealth(mailboxLogins),
  );
  if (silencedMailboxes.size > 0) {
    console.warn(
      `[instantly-service] self-send-dispatch: ${silencedMailboxes.size} mailbox(es) silenced — the relay refuses them and has accepted nothing in 7 days: ${[...silencedMailboxes].join(", ")}`,
    );
  }

  const plan = async () => {
    const sequences = await loadPendingSequences();
    const { capacities, accounts } = await loadSendingAccounts(asOf, mailboxLogins);
    return {
      sequences,
      accounts,
      selection: selectDueSteps(sequences, capacities, asOf, silencedMailboxes),
    };
  };

  // ── Probe ────────────────────────────────────────────────────────────────
  //
  // Is there anything this run could possibly send? Two local queries and a pure
  // selection answer it, and the answer decides whether we pay for the mailbox
  // read at all.
  //
  // ⚠️ This is what makes a short interval affordable, and it is NOT a weakening
  // of the read-before-send ordering: a run that sends anything still polls
  // first, in this same run, before the selection it acts on. What is skipped is
  // the poll on a run that was going to send NOTHING — a weekend, a fleet at its
  // daily cap, an hour when every prospect's local window is shut. Without it, a
  // 10-minute interval would read 249 mailboxes around the clock to discover
  // there was nothing to do.
  let current = await plan();
  const waitingReplies = selectDueScheduledReplies(
    await loadPendingScheduledReplies(),
    asOf,
  );
  const hasWork = current.selection.selected.length > 0 || waitingReplies.length > 0;

  if (!hasWork) {
    const idle: DispatchSummary = {
      ...emptySummary(),
      sequencesRead: current.sequences.length,
      dueBeforeCapacity: current.selection.dueBeforeCapacity,
      blockedNoCapacityRow: current.selection.blockedNoCapacityRow,
      skippedSilenced: current.selection.skippedSilenced,
      rehomed: current.selection.rehomed,
    };
    console.log(
      `[instantly-service] self-send-dispatch: done ${JSON.stringify(idle)}`,
    );
    return idle;
  }

  // Read the mailboxes BEFORE deciding what to send, in the same run and
  // awaited. A prospect who replied since the last sweep has their sequence
  // stopped by the poll, so they are already out of the queue by the time we
  // select — we never email someone who has already answered.
  //
  // This has to happen HERE rather than as an earlier cron step: both endpoints
  // answer 202 and work in the background, so a separate poll step would still
  // be running while dispatch selected, and the ordering would be hoped-for
  // rather than real. A poll failure is logged and does not block the send — the
  // worst case is one extra email to someone who replied within the window,
  // which is the same latency the webhook path already carries.
  let polled = false;
  if (options.pollFirst) {
    await runPoll({ asOf }).catch((error) => {
      console.error(
        `[instantly-service] self-send: pre-dispatch poll failed, sending anyway: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    });
    polled = true;

    // Re-select against what the poll just learned. The probe above is only a
    // "is this run worth waking for" read; the selection we ACT on is taken
    // after the mailboxes have been read, so a sequence the poll stopped is
    // already gone from it.
    current = await plan();
  }

  const { sequences, accounts, selection } = current;
  const due = selection.selected;
  const batch = options.limit ? due.slice(0, options.limit) : due;

  const summary: DispatchSummary = {
    ...emptySummary(),
    sequencesRead: sequences.length,
    due: due.length,
    dueBeforeCapacity: selection.dueBeforeCapacity,
    blockedNoCapacityRow: selection.blockedNoCapacityRow,
    skippedSilenced: selection.skippedSilenced,
    rehomed: selection.rehomed,
    polled,
  };

  // Answer the prospects who are owed one FIRST. A waiting reply has already
  // had its refusals checked and its moment chosen; a sequence step behind it
  // has not. Fail-soft as a whole (each reply already fails loud on its own
  // row): a queue problem here must not stop the fleet's sending for the hour.
  try {
    const replies = await dispatchScheduledReplies(asOf);
    summary.repliesDue = replies.due;
    summary.repliesSent = replies.sent;
    summary.repliesFailed = replies.failed;
  } catch (error) {
    console.error(
      `[instantly-service] self-send: scheduled-reply drain failed, sending anyway: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }

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

      if (step.rehomedFrom !== undefined) {
        // Persist the move BEFORE the send, guarded on the row still being where
        // we found it and on nothing having gone out yet — a first email is the
        // only step that may change mailbox, and only while there is no thread.
        // The reason is recorded on the row so nobody reads the move as drift.
        const moved = await rehomeFirstEmail(
          step.instantlyCampaignId,
          step.rehomedFrom,
          step.accountEmail,
          asOf,
        );
        if (!moved) {
          console.warn(
            `[instantly-service] self-send: rehome of campaign=${step.instantlyCampaignId} from ${step.rehomedFrom} to ${step.accountEmail} lost its guard — skipped`,
          );
          continue;
        }
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
