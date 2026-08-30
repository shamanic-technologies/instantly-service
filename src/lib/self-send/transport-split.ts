/**
 * Splitting new sequences evenly between the two pipes, to compare them.
 *
 * The Instantly Email Outreach subscription is being cancelled, so we need to
 * know — on real traffic, before the deadline — whether our own sender behaves
 * like the vendor's. That comparison is only worth anything if both arms carry
 * the same kind of work, and the obvious way to arrange it does NOT: assigning
 * half the MAILBOXES to each pipe leaves the arms at different positions in the
 * fill order, which saturates the head before touching the tail, so one arm can
 * legitimately receive nearly all the volume and the other almost none.
 *
 * So the split is per SEQUENCE, on the same mailboxes: each new lead goes to
 * whichever pipe has been assigned fewer sequences in the last 24 hours. Same
 * mailboxes, same days, same brands, same lead source — the pipe is the only
 * thing that differs, which is what makes the arms comparable.
 *
 * ⚠️ Per SEQUENCE, never per EMAIL. Alternating within a sequence would push the
 * lead to Instantly AND have our worker dispatch it, so the prospect receives
 * every followup twice — the exact bug the transport branch in `POST /send`
 * exists to prevent. The decision is frozen on the campaign row and every later
 * step of that lead reads it.
 *
 * It counts ASSIGNMENTS (campaign rows we created), not emails that went out.
 * That is the quantity we control and it is visible immediately, whereas
 * Instantly dispatches on its own schedule — so counting sends would let the
 * vendor's queue depth steer our split. How much each pipe actually manages to
 * dispatch is then a RESULT of the experiment rather than an input to it.
 */

import { sql } from "drizzle-orm";

import { db } from "../../db";
import { getOrSetCachedStats } from "../stats-cache";
import { loadCredentialedMailboxes } from "./mailbox-credentials";
import type { CallerInfo } from "../key-client";
import {
  SEND_TRANSPORT_INSTANTLY,
  SEND_TRANSPORT_SMTP,
  type SendTransport,
} from "./transport";

/**
 * Local row unwrapper — same shape every other module in this repo defines.
 *
 * This service runs on node-postgres, where `db.execute` resolves to a
 * `QueryResult` object (`{ rows, rowCount, … }`), NOT to an array. Casting the
 * result to an array type compiles clean and then throws `rows is not iterable`
 * on the first real query — which is exactly how this counting query took every
 * send down in production. Reading `.rows` is the only correct access; the
 * array branch is kept because every sibling module carries it.
 */
function rowsOf<T = Record<string, unknown>>(result: unknown): T[] {
  if (!result) return [];
  return Array.isArray(result)
    ? (result as T[])
    : (((result as { rows?: T[] }).rows) ?? []);
}

/** Assignments made in the last 24h, per pipe. */
export interface TransportAssignmentCounts {
  instantly: number;
  smtp: number;
}

/**
 * True when the experiment is armed.
 *
 * Off by default and read at USE, never captured at module load: a service
 * deployed without the variable still behaves exactly as before, and turning the
 * experiment off is one env change with no account row touched.
 */
export function isTransportSplitEnabled(): boolean {
  return process.env.SEND_TRANSPORT_AB_ENABLED === "true";
}

/**
 * Pick the pipe with fewer assignments — the minority side.
 *
 * Self-correcting rather than strictly alternating: two concurrent sends can
 * read the same counts and land on the same side, and the next few sends tip
 * back on their own. A strict "look at the last one and take the other" needs no
 * lock either but oscillates under concurrency instead of converging.
 *
 * A tie goes to SMTP. The experiment exists to learn about the newer pipe, and
 * at equal counts either choice is equally balanced.
 */
export function chooseSequenceTransport(
  counts: TransportAssignmentCounts,
): SendTransport {
  return counts.smtp <= counts.instantly ? SEND_TRANSPORT_SMTP : SEND_TRANSPORT_INSTANTLY;
}

/**
 * Count the last 24 hours of assignments per pipe, from our OWN rows.
 *
 * A reservation sentinel is excluded: it is an in-flight claim whose transport
 * has not been frozen yet, so counting it would let a burst of concurrent sends
 * that have not yet decided anything skew the next decision.
 */
export async function fetchTransportAssignmentCounts(): Promise<TransportAssignmentCounts> {
  const rows = rowsOf<{ send_transport: string | null; n: number }>(
    await db.execute(sql`
      SELECT send_transport, COUNT(*)::int AS n
      FROM instantly_campaigns
      WHERE created_at > now() - interval '24 hours'
        AND instantly_campaign_id NOT LIKE 'reserving:%'
      GROUP BY send_transport
    `),
  );

  const counts: TransportAssignmentCounts = { instantly: 0, smtp: 0 };
  for (const row of rows) {
    const n = Number(row.n) || 0;
    if (row.send_transport === SEND_TRANSPORT_SMTP) counts.smtp += n;
    else counts.instantly += n;
  }
  return counts;
}

/**
 * Is this mailbox usable by the self-send pipe at all?
 *
 * The set is cached for the same 60s as the send-selection capacity snapshot,
 * and for the same reason: it is two network reads (key-service plus a vendor
 * pagination) that would otherwise run on every single send. A mailbox we hold
 * no credential for stays on Instantly — that is a capability fact, stated
 * explicitly here rather than discovered later as a failed dispatch.
 */
export async function isSelfSendCapable(
  accountEmail: string,
  caller: CallerInfo,
): Promise<boolean> {
  const addresses = await getOrSetCachedStats("self-send-credentialed-mailboxes", () =>
    loadCredentialedMailboxes(caller),
  );
  return addresses.has(accountEmail.trim().toLowerCase());
}

/**
 * The transport a NEW sequence should use on this mailbox.
 *
 * Precedence, in order:
 *   1. An account explicitly set to `smtp` stays on `smtp`. That column is the
 *      manual override and the rollback lever, so the experiment must not
 *      quietly move a mailbox an operator has pinned.
 *   2. Experiment off, or a mailbox we cannot authenticate → Instantly. The
 *      proven pipe is the safe direction, and a mailbox with no credential
 *      cannot dispatch on the other one at all.
 *   3. Otherwise → the minority side of the last 24 hours.
 */
export async function resolveTransportForNewSequence(
  account: { email: string; sendTransport: string | null },
  caller: CallerInfo,
): Promise<SendTransport> {
  if (account.sendTransport === SEND_TRANSPORT_SMTP) return SEND_TRANSPORT_SMTP;
  if (!isTransportSplitEnabled()) return SEND_TRANSPORT_INSTANTLY;
  if (!(await isSelfSendCapable(account.email, caller))) return SEND_TRANSPORT_INSTANTLY;
  return chooseSequenceTransport(await fetchTransportAssignmentCounts());
}
