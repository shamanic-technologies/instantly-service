/**
 * Which sending mailboxes are out of prospect mail, and why: the stored daily
 * series behind the admin sender-health chart, and its staff read
 * `GET /internal/sender-health?days=`. SENDER-HEALTH-1019.
 *
 * Two signals, both decided by the dispatcher on every tick and recorded here:
 *   - REFUSED: the relay refuses the mailbox and accepted nothing from it since
 *     (`selectSilencedSmtpSenders`, over prospect, seed AND warmup mail). It
 *     sends nothing.
 *   - MOSTLY SPAM at a provider (Google, Microsoft 365, Outlook.com): more than
 *     half of at least 10 seeds found in spam there (`provider-placement.ts`).
 *     It sends nothing to that provider's prospects.
 *
 * One row per UTC day, rewritten by every tick of that day: the mailbox lists
 * are the LAST tick's (the state at day end), the held counts the day's MAX
 * (a tick's count, not a sum: the same step is counted again every tick).
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { RECIPIENT_PROVIDERS, type RecipientProvider } from "./provider-placement";
import type { AccountCapacity, DueSelection } from "./self-send/dispatch";

export interface SenderHealthSnapshot {
  day: string;
  /** Real mailboxes (SASL logins) the relay refuses. */
  refused: string[];
  /** Per provider: sending addresses measured mostly in spam there. */
  mostlySpam: Record<RecipientProvider, string[]>;
  /** First emails held this tick for placement (rehomed or waiting). */
  heldPlacement: number;
  /** Follow-ups held this tick: their mailbox is mostly spam at the prospect's provider. */
  heldMostlySpam: number;
  /** Steps skipped this tick on a refused (or blind) mailbox. */
  skippedRefused: number;
}

/** Pure. */
export function senderHealthSnapshot(input: {
  asOf: Date;
  refusedMailboxes: ReadonlySet<string>;
  capacities: readonly AccountCapacity[];
  selection: Pick<DueSelection, "heldPlacement" | "heldMostlySpam" | "skippedSilenced">;
}): SenderHealthSnapshot {
  const mostlySpam = Object.fromEntries(RECIPIENT_PROVIDERS.map((p) => [p, [] as string[]])) as Record<
    RecipientProvider,
    string[]
  >;
  for (const c of input.capacities) {
    for (const provider of RECIPIENT_PROVIDERS) {
      if (c.placement?.[provider]?.mostlySpam) mostlySpam[provider].push(c.accountEmail.trim().toLowerCase());
    }
  }
  for (const provider of RECIPIENT_PROVIDERS) mostlySpam[provider] = [...new Set(mostlySpam[provider])].sort();
  return {
    day: input.asOf.toISOString().slice(0, 10),
    refused: [...input.refusedMailboxes].map((m) => m.trim().toLowerCase()).sort(),
    mostlySpam,
    heldPlacement: input.selection.heldPlacement,
    heldMostlySpam: input.selection.heldMostlySpam,
    skippedRefused: input.selection.skippedSilenced,
  };
}

export async function recordSenderHealth(s: SenderHealthSnapshot): Promise<void> {
  await db.execute(sql`
    INSERT INTO sender_health_daily
      (day, refused, mostly_spam, held_placement, held_mostly_spam, skipped_refused, updated_at)
    VALUES (${s.day}::date, ${JSON.stringify(s.refused)}::jsonb, ${JSON.stringify(s.mostlySpam)}::jsonb,
            ${s.heldPlacement}, ${s.heldMostlySpam}, ${s.skippedRefused}, now())
    ON CONFLICT (day) DO UPDATE SET
      refused = EXCLUDED.refused,
      mostly_spam = EXCLUDED.mostly_spam,
      held_placement = GREATEST(sender_health_daily.held_placement, EXCLUDED.held_placement),
      held_mostly_spam = GREATEST(sender_health_daily.held_mostly_spam, EXCLUDED.held_mostly_spam),
      skipped_refused = GREATEST(sender_health_daily.skipped_refused, EXCLUDED.skipped_refused),
      updated_at = EXCLUDED.updated_at
  `);
}

export interface SenderHealthDay {
  day: string;
  /** Null on a day no dispatch tick ran (no measurement, never a fake 0). */
  refusedMailboxes: number | null;
  mostlySpamMailboxes: Record<RecipientProvider, number> | null;
  heldPlacement: number | null;
  heldMostlySpam: number | null;
  skippedRefused: number | null;
}

interface StoredRow {
  day: string;
  refused: unknown;
  mostlySpam: unknown;
  heldPlacement: unknown;
  heldMostlySpam: unknown;
  skippedRefused: unknown;
}

function listOf(v: unknown): string[] {
  return Array.isArray(v) ? v.map(String) : [];
}

/** Pure: every day of the window emitted, oldest first. */
export function buildSenderHealthDays(rows: readonly StoredRow[], fromDay: string, toDay: string): SenderHealthDay[] {
  const byDay = new Map(rows.map((r) => [r.day, r]));
  const out: SenderHealthDay[] = [];
  for (let t = Date.parse(`${fromDay}T00:00:00Z`); t <= Date.parse(`${toDay}T00:00:00Z`); t += 86_400_000) {
    const day = new Date(t).toISOString().slice(0, 10);
    const r = byDay.get(day);
    if (!r) {
      out.push({ day, refusedMailboxes: null, mostlySpamMailboxes: null, heldPlacement: null, heldMostlySpam: null, skippedRefused: null });
      continue;
    }
    const spam = (r.mostlySpam ?? {}) as Record<string, unknown>;
    out.push({
      day,
      refusedMailboxes: listOf(r.refused).length,
      mostlySpamMailboxes: Object.fromEntries(
        RECIPIENT_PROVIDERS.map((p) => [p, listOf(spam[p]).length]),
      ) as Record<RecipientProvider, number>,
      heldPlacement: Number(r.heldPlacement),
      heldMostlySpam: Number(r.heldMostlySpam),
      skippedRefused: Number(r.skippedRefused),
    });
  }
  return out;
}

export const SENDER_HEALTH_DEFAULT_DAYS = 90;
export const SENDER_HEALTH_MAX_DAYS = 400;

export async function loadSenderHealth(days: number, asOf: Date = new Date()) {
  const toDay = asOf.toISOString().slice(0, 10);
  const fromDay = new Date(Date.parse(`${toDay}T00:00:00Z`) - (days - 1) * 86_400_000).toISOString().slice(0, 10);
  const result = await db.execute(sql`
    SELECT to_char(day, 'YYYY-MM-DD') AS day, refused, mostly_spam AS "mostlySpam",
           held_placement AS "heldPlacement", held_mostly_spam AS "heldMostlySpam",
           skipped_refused AS "skippedRefused", updated_at AS "updatedAt"
    FROM sender_health_daily
    WHERE day >= ${fromDay}::date AND day <= ${toDay}::date
    ORDER BY day
  `);
  const rows = ((result as unknown as { rows?: (StoredRow & { updatedAt: unknown })[] }).rows ?? []);
  const latest = rows[rows.length - 1] ?? null;
  return {
    asOf: asOf.toISOString(),
    from: fromDay,
    to: toDay,
    // The latest tick's lists: WHICH mailboxes are out right now, and why.
    current: latest
      ? {
          day: latest.day,
          updatedAt: new Date(String(latest.updatedAt)).toISOString(),
          refused: listOf(latest.refused),
          mostlySpam: Object.fromEntries(
            RECIPIENT_PROVIDERS.map((p) => [p, listOf(((latest.mostlySpam ?? {}) as Record<string, unknown>)[p])]),
          ) as Record<RecipientProvider, string[]>,
        }
      : null,
    days: buildSenderHealthDays(rows, fromDay, toDay),
  };
}
