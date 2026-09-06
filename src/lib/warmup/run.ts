/**
 * The warmup mesh — sending side (IO glue around the pure plan in `plan.ts`).
 *
 * Replaces the warmup pool bundled with the Instantly Email Outreach
 * subscription. Every credentialed mailbox writes a short, individually
 * generated note to a few of the others each day; the receiving half
 * (`poll.ts`) opens it, rescues it from spam and sometimes answers.
 *
 * ⚠️ WARMUP SPENDS THE SAME DAILY QUOTA AS REAL SENDS, and this module is what
 * makes that true. Gmail's per-user limit does not care which of our jobs put
 * the message on the wire, so warmup that ignored the cap would push mailboxes
 * into the `550-5.4.5 Daily user sending limit exceeded` the age ramp exists to
 * respect — and it would do it to the mailboxes with the LOWEST caps, since a
 * fresh mailbox has the least room to spare. Room is therefore
 * `capForAccount(...) − (real sends today + warmup sends today)`, and warmup
 * takes what is left AFTER outreach, never before it.
 *
 * Declares no cost. The mailbox estate is a fixed cost we absorb (see
 * "Sending declares NO cost"), and the message generation is chat-service's
 * spend, on its own platform run.
 */

import { sql } from "drizzle-orm";

import { db } from "../../db";
import { warmupDispatches } from "../../db/schema";
import { capForAccount } from "../account-lifecycle";
import type { CallerInfo } from "../key-client";
import { loadMailboxLogins, loginFor } from "../self-send/mailbox-credentials";
import { loadSeedCredentialResolver } from "../seed-placement/credentials";
import { selectSeedReceivers } from "../seed-placement/seeds";
import { dispatchMessage, SmtpDispatchError, classifyDispatchFailure } from "../self-send/smtp";
import { buildWarmupMessage } from "./message";
import {
  partnerCandidates,
  planWarmupPairings,
  selectSilencedSenders,
  warmupBudgetFor,
  warmupDayKey,
  type WarmupSenderHealth,
} from "./plan";

const CALLER: CallerInfo = { method: "POST", path: "/internal/audit/warmup/run" };

/** Off by default — a mesh nobody armed must send nothing. */
export function isWarmupMeshEnabled(): boolean {
  return process.env.WARMUP_MESH_ENABLED === "true";
}

export interface WarmupRunSummary {
  dayKey: string;
  /** Mailboxes eligible to take part (credentialed, minus the measurement judges). */
  pool: number;
  /** Mailboxes held out so they can grade without having been trained. */
  judges: number;
  planned: number;
  /** Skipped because the mailbox had no room left today after real outreach. */
  skippedNoRoom: number;
  /** Skipped because this exact edge already went out today (a re-run). */
  skippedAlreadySent: number;
  /** Skipped because the sending mailbox's relay refuses everything it sends. */
  skippedSilenced: number;
  sent: number;
  failed: number;
}

interface SenderCapacity {
  cap: number;
  spent: number;
}

/**
 * Room left on each MAILBOX today, counting real sends AND warmup already made.
 *
 * Keyed on the real mailbox rather than the sending address, for the reason the
 * dispatcher documents: aliases share one login, one relay account and one
 * quota, and the relay enforces it per SASL user.
 */
async function loadRoom(
  mailboxLogins: ReadonlyMap<string, string>,
  dayKey: string,
  asOf: Date,
): Promise<Map<string, SenderCapacity>> {
  const result = await db.execute(sql`
    SELECT
      a.email                AS "accountEmail",
      a.daily_limit          AS "dailyLimit",
      a.timestamp_created    AS "timestampCreated",
      a.lifecycle_updated_at AS "lifecycleUpdatedAt",
      COALESCE((
        SELECT COUNT(*) FROM instantly_events e
        WHERE e.account_email = a.email
          AND e.event_type = 'email_sent'
          AND e.inferred = false
          AND e.timestamp >= date_trunc('day', now() AT TIME ZONE 'UTC')
      ), 0)                  AS "sentToday"
    FROM instantly_accounts a
    WHERE a.absent_since IS NULL
  `);

  const room = new Map<string, SenderCapacity>();

  for (const row of result.rows as Record<string, unknown>[]) {
    const email = String(row.accountEmail).trim().toLowerCase();
    const mailbox = mailboxLogins.get(email);
    if (mailbox === undefined) continue;

    const cap = capForAccount(
      {
        daily_limit: row.dailyLimit === null ? 0 : Number(row.dailyLimit),
        timestamp_created: (row.timestampCreated as string | null) ?? null,
        lifecycle_updated_at: (row.lifecycleUpdatedAt as string | null) ?? null,
      },
      asOf,
    );

    // Several aliases roll up to one mailbox: the lowest cap wins, their sends sum.
    const current = room.get(mailbox);
    room.set(mailbox, {
      cap: current === undefined ? cap : Math.min(current.cap, cap),
      spent: (current?.spent ?? 0) + Number(row.sentToday),
    });
  }

  // Warmup already dispatched today comes out of the same quota.
  const warm = await db.execute(sql`
    SELECT sender_mailbox AS "mailbox", COUNT(*)::int AS "n"
    FROM warmup_dispatches
    WHERE day_key = ${dayKey} AND outcome = 'sent'
    GROUP BY sender_mailbox
  `);
  for (const row of warm.rows as Record<string, unknown>[]) {
    const mailbox = String(row.mailbox);
    const current = room.get(mailbox);
    if (current) current.spent += Number(row.n);
  }

  return room;
}

/**
 * Recent warmup outcomes per mailbox, for the sender-silencing rule.
 *
 * Seven days: long enough that a blacklisted mailbox is not retried daily, short
 * enough that it IS retried without anyone clearing a flag. See
 * {@link selectSilencedSenders}.
 */
async function loadSenderHealth(): Promise<WarmupSenderHealth[]> {
  const result = await db.execute(sql`
    SELECT sender_mailbox                                     AS "mailbox",
           COUNT(*) FILTER (WHERE outcome = 'permanent')::int  AS "permanent",
           COUNT(*) FILTER (WHERE outcome = 'sent')::int       AS "sent"
    FROM warmup_dispatches
    WHERE dispatched_at > now() - interval '7 days'
    GROUP BY sender_mailbox
  `);

  return (result.rows as Record<string, unknown>[]).map((row) => ({
    mailbox: String(row.mailbox),
    permanent: Number(row.permanent),
    sent: Number(row.sent),
  }));
}

/**
 * Warmup already sent today, per REAL mailbox — the budget's running total.
 *
 * Grouped by mailbox rather than address for the reason everything else here is:
 * a domain's aliases share one quota, so their warmup sums.
 */
async function loadWarmupSentToday(dayKey: string): Promise<Map<string, number>> {
  const result = await db.execute(sql`
    SELECT sender_mailbox AS "mailbox", COUNT(*)::int AS "n"
    FROM warmup_dispatches
    WHERE day_key = ${dayKey} AND outcome = 'sent'
    GROUP BY sender_mailbox
  `);
  return new Map(
    (result.rows as Record<string, unknown>[]).map((row) => [
      String(row.mailbox),
      Number(row.n),
    ]),
  );
}

/** Edges already on the wire today, so a re-run is a no-op rather than a double send. */
async function loadSentEdges(dayKey: string): Promise<Set<string>> {
  const result = await db.execute(sql`
    SELECT sender_email AS "s", receiver_email AS "r"
    FROM warmup_dispatches
    WHERE day_key = ${dayKey}
  `);
  return new Set(
    (result.rows as Record<string, unknown>[]).map(
      (row) => `${String(row.s)}->${String(row.r)}`,
    ),
  );
}

export async function runWarmupMesh(
  options: { asOf?: Date; limit?: number } = {},
): Promise<WarmupRunSummary> {
  const asOf = options.asOf ?? new Date();
  const dayKey = warmupDayKey(asOf);

  const mailboxLogins = await loadMailboxLogins(CALLER);
  const resolve = await loadSeedCredentialResolver(CALLER);

  // The judges: the mailboxes the seed harness grades placement with. They are
  // held OUT of the mesh — rescuing mail from a receiver's spam folder teaches
  // its filter, and a receiver that both grades and rescues returns a verdict it
  // has been trained to give. See `partnerCandidates`.
  const addresses = [...mailboxLogins.keys()];
  const judges = selectSeedReceivers(
    addresses
      .map((email) => {
        const credential = resolve(email);
        return credential ? { email, imapHost: credential.imapHost } : null;
      })
      .filter((c): c is { email: string; imapHost: string } => c !== null),
  ).map((r) => r.email);

  const pool = partnerCandidates(addresses, judges, mailboxLogins);
  const pairings = planWarmupPairings(pool, asOf);

  const summary: WarmupRunSummary = {
    dayKey,
    pool: pool.length,
    judges: judges.length,
    planned: pairings.length,
    skippedNoRoom: 0,
    skippedAlreadySent: 0,
    skippedSilenced: 0,
    sent: 0,
    failed: 0,
  };

  const room = await loadRoom(mailboxLogins, dayKey, asOf);
  // ⚠️ THE BUDGET COUNTS THE DAY, NOT THE RUN. Seeded from what warmup already
  // sent today, so a second run cannot spend a second budget. In steady state
  // the pairing is deterministic per day and a re-run plans identical edges that
  // `alreadySent` skips — but that is an argument about the pairing, not a
  // guarantee about the budget, and it stops holding the moment the pairing
  // changes (as it did on 2026-09-06, when three runs with different pairing
  // logic put 17 warmup sends on one mailbox against a budget of 4).
  const sentByMailbox = await loadWarmupSentToday(dayKey);
  const alreadySent = await loadSentEdges(dayKey);
  // Mailboxes whose relay refuses everything: they still RECEIVE, they just stop
  // being asked to send. See `selectSilencedSenders`.
  const silenced = selectSilencedSenders(await loadSenderHealth());

  const batch = options.limit ? pairings.slice(0, options.limit) : pairings;

  for (const pairing of batch) {
    if (alreadySent.has(`${pairing.senderEmail}->${pairing.receiverEmail}`)) {
      summary.skippedAlreadySent += 1;
      continue;
    }

    const mailbox = mailboxLogins.get(pairing.senderEmail);
    if (mailbox === undefined) continue;

    if (silenced.has(mailbox)) {
      summary.skippedSilenced += 1;
      continue;
    }

    const capacity = room.get(mailbox);
    // Warmup takes at most a FRACTION of the day's cap, so it can never starve
    // outreach on the mailboxes with least room — which are exactly the ones the
    // age ramp is protecting. See `warmupBudgetFor`.
    const budget = warmupBudgetFor(capacity?.cap ?? 0);
    if (!capacity || capacity.spent >= capacity.cap || (sentByMailbox.get(mailbox) ?? 0) >= budget) {
      summary.skippedNoRoom += 1;
      continue;
    }

    const credential = resolve(pairing.senderEmail);
    if (!credential) continue;

    const message = await buildWarmupMessage(
      pairing.senderEmail,
      pairing.receiverEmail,
      dayKey,
    );

    // Fail-loud PER EDGE, never per run: one dead receiver domain must not stop
    // the fleet warming for the day. Every outcome lands in bronze, so a refused
    // send stays visible rather than vanishing.
    try {
      const result = await dispatchMessage(credential, {
        from: pairing.senderEmail,
        to: pairing.receiverEmail,
        subject: message.subject,
        html: `<p>${message.text.split("\n\n").join("</p><p>").split("\n").join("<br>")}</p>`,
        headers: {},
      });

      await db.insert(warmupDispatches).values({
        senderEmail: pairing.senderEmail,
        senderMailbox: mailbox,
        receiverEmail: pairing.receiverEmail,
        dayKey,
        messageId: result.messageId,
        subject: message.subject,
        outcome: "sent",
        response: result.response,
      });

      capacity.spent += 1;
      sentByMailbox.set(mailbox, (sentByMailbox.get(mailbox) ?? 0) + 1);
      summary.sent += 1;
    } catch (error) {
      const kind =
        error instanceof SmtpDispatchError
          ? classifyDispatchFailure(error)
          : "transient";
      const detail = error instanceof Error ? error.message : String(error);

      await db
        .insert(warmupDispatches)
        .values({
          senderEmail: pairing.senderEmail,
          senderMailbox: mailbox,
          receiverEmail: pairing.receiverEmail,
          dayKey,
          messageId: `failed:${crypto.randomUUID()}`,
          subject: message.subject,
          outcome: kind,
          response: detail.slice(0, 500),
        })
        .catch(() => undefined);

      summary.failed += 1;
      console.warn(
        `[warmup] ${pairing.senderEmail} -> ${pairing.receiverEmail}: ${kind}: ${detail}`,
      );
    }
  }

  return summary;
}

