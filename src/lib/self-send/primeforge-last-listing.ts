/**
 * Primeforge's mailbox list as we last received it, read from our own bronze.
 *
 * ⚠️ WHY THIS EXISTS (SEND-POOL-VENDOR-402-1030). Primeforge's API answering 402
 * "active subscription required" is a BILLING refusal of its REST API, not the
 * death of the mailboxes: they are Google Workspace accounts and their app
 * passwords keep authenticating smtp.gmail.com / imap.gmail.com. Measured
 * 2026-10-30 from the box, a day into the 402: IMAP LOGIN OK on three of them.
 * Reading "the vendor will not list them" as "we hold no credential" emptied the
 * send pool for every org (the only in-production mailboxes were Primeforge's;
 * every Gandi mailbox sits under the inbox bar) and stopped reading their
 * replies.
 *
 * The infra sync (`infra-sync.ts`, daily) stores every Primeforge listing in
 * `provider_mailboxes_raw` with the vendor's raw payload, app password included.
 * This returns the NEWEST complete listing (one sync run writes every row with
 * the same `fetched_at`), so the credential is the vendor's own, as it last
 * answered. It is not a guess: a password that stopped working fails at the SMTP
 * or IMAP login, loudly, per mailbox, exactly as a live-listed one would.
 *
 * A mailbox whose vendor `validUntil` has passed is dropped: the vendor told us
 * it ends there, and that is the one fact the stored copy cannot refresh.
 */

import { sql } from "drizzle-orm";
import { db } from "../../db";
import type { PrimeforgeRawMailbox } from "../providers/primeforge-client";

export interface LastPrimeforgeListing {
  /** When the stored listing was fetched (ISO), null when we never stored one. */
  fetchedAt: string | null;
  mailboxes: PrimeforgeRawMailbox[];
}

function rowsOf<T>(result: unknown): T[] {
  const r = result as { rows?: T[] } | T[];
  return Array.isArray(r) ? r : (r.rows ?? []);
}

/** Pure: drop the mailboxes the vendor said end before `asOf`. */
export function keepStillValid(
  mailboxes: readonly PrimeforgeRawMailbox[],
  asOf: Date,
): PrimeforgeRawMailbox[] {
  return mailboxes.filter((m) => {
    const until = (m as { validUntil?: unknown }).validUntil;
    if (typeof until !== "string" || until === "") return true;
    const t = new Date(until).getTime();
    return Number.isNaN(t) || t > asOf.getTime();
  });
}

export async function loadLastPrimeforgeListing(
  asOf: Date = new Date(),
): Promise<LastPrimeforgeListing> {
  const result = await db.execute(sql`
    SELECT payload, fetched_at AS "fetchedAt"
    FROM provider_mailboxes_raw
    WHERE provider = 'primeforge'
      AND fetched_at = (
        SELECT max(fetched_at) FROM provider_mailboxes_raw WHERE provider = 'primeforge'
      )
  `);
  const rows = rowsOf<{ payload: PrimeforgeRawMailbox; fetchedAt: string | Date }>(result);
  if (rows.length === 0) return { fetchedAt: null, mailboxes: [] };
  const at = rows[0]!.fetchedAt;
  // `timestamp without time zone`, written as UTC by the sync.
  const fetchedAt =
    at instanceof Date ? at.toISOString() : new Date(`${String(at).replace(" ", "T")}Z`).toISOString();
  return { fetchedAt, mailboxes: keepStillValid(rows.map((r) => r.payload), asOf) };
}
