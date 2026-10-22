/**
 * Inbox placement per inbox provider against the market bar, and the episodes
 * when it falls and the automatic cut did not bring it back
 * (MAILBOX-MONITORING-1019). Read by dashboard v2 Monitoring > Emails, where a
 * provider below the bar shows red on the hub.
 *
 * NO EMAIL (PLACEMENT-ALERT-NO-EMAIL-1022, owner 2026-10-22: « no email in my
 * inbox please »). The staff email `inbox_placement_below_bar` is gone: the
 * page is the only surface. Do NOT add a send back.
 *
 * The owner learned on 2026-10-19 that seed tests had read 40-60 % spam at
 * Google for weeks and nobody told him. The machine already cuts a bad mailbox
 * (SENDER-HEALTH-1019: refused by its relay = sends nothing; mostly spam at a
 * provider = no mail to that provider's prospects), so a figure about the FLEET
 * would mostly report what was already fixed. The question asked here is
 * the one the cut cannot answer: among the mailboxes STILL ALLOWED to mail a
 * provider's prospects (in production, not refused, not mostly spam there that
 * day), where do their seeds land?
 *
 * THE BAR (market consensus; our history only illustrates it):
 *   - Omni Online Strategies, cold-email inbox placement: below 80 % "indicates
 *     a technical problem", professional target 85-95 %.
 *   - WarmForge, inbox placement tests: 80 % and above strong, 70-79 %
 *     acceptable, below 70 % poor.
 *   - Expandi, email KPIs: below 85 % poor for cold email (domain reputation or
 *     authentication problem).
 *   Validity's benchmark puts the global average at 83-87 %. 80 % is the line
 *   every source agrees is a problem; it is the bar. Measured on seeds FOUND
 *   (inbox / (inbox + spam)), the same basis the dispatcher's provider routing
 *   grades on, so a seed not read yet never reads as spam; a day needs at least
 *   {@link PLACEMENT_ALERT_MIN_FOUND_SEEDS} found seeds to count.
 *   Illustration, prod 2026-10-19 Google: 1,099 inbox of 3,404 seeds fleet-wide,
 *   dominated by legacy relay senders the dispatcher no longer uses.
 *
 * THE FACT is an EPISODE (`placement_alert_episodes`): opened on the first
 * measured day below the bar, closed on the first measured day back at or above
 * it; a day still below extends it. Episodes alerted by email before 1022 keep
 * their `alert_status` / `alerted_at` as history; nothing writes them any more.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { ESP_GOOGLE, ESP_OUTLOOK, ESP_OUTLOOK_CONSUMER } from "./seed-placement/seeds";
import type { RecipientProvider } from "./provider-placement";
import { mailboxOf } from "./ops/entity-scope";

export const PLACEMENT_ALERT_INBOX_PCT_BAR = 80;
export const PLACEMENT_ALERT_MIN_FOUND_SEEDS = 20;
/** Days re-evaluated each tick: seeds of yesterday are still being read today. */
export const PLACEMENT_ALERT_EVAL_DAYS = 3;

export const PLACEMENT_ALERT_SOURCES = [
  { name: "Omni Online Strategies", url: "https://omnionlinestrategies.com/blog/what-is-inbox-placement-rate-cold-email", says: "below 80% indicates a technical problem; target 85-95%" },
  { name: "WarmForge", url: "https://www.warmforge.ai/blog/what-is-an-inbox-placement-test", says: "80%+ strong, 70-79% acceptable, below 70% poor" },
  { name: "Expandi", url: "https://expandi.io/blog/email-marketing-kpis/", says: "below 85% poor for cold email" },
] as const;

export const PROVIDER_LABELS: Record<RecipientProvider, string> = {
  google: "Google",
  microsoft: "Microsoft 365",
  microsoft_consumer: "Outlook.com",
};

const ESP_PROVIDER = new Map<number, RecipientProvider>([
  [ESP_GOOGLE, "google"],
  [ESP_OUTLOOK, "microsoft"],
  [ESP_OUTLOOK_CONSUMER, "microsoft_consumer"],
]);

export interface SeedSenderDay {
  day: string;
  esp: number;
  sender: string;
  seeds: number;
  inbox: number;
  spam: number;
}

export interface HealthDay {
  day: string;
  /** Real mailboxes (logins) the relay refuses. */
  refused: string[];
  mostlySpam: Partial<Record<RecipientProvider, string[]>>;
}

export interface PlacementEvaluation {
  day: string;
  provider: RecipientProvider;
  providerLabel: string;
  /** Every sender, before the cut. */
  fleet: { seeds: number; found: number; inbox: number; inboxPct: number | null };
  /** Production senders the cut still lets mail this provider. */
  allowed: { senders: number; seeds: number; found: number; inbox: number; inboxPct: number | null };
  /** Production mailboxes the cut took off this provider that day. */
  mailboxesCut: number;
  measured: boolean;
  below: boolean;
}

const pct = (n: number, d: number): number | null => (d > 0 ? Math.round((n * 1000) / d) / 10 : null);

/** Pure: one evaluation per (day, provider) present in the seeds. */
export function evaluatePlacement(input: {
  rows: readonly SeedSenderDay[];
  health: readonly HealthDay[];
  inProduction: ReadonlySet<string>;
  loginOf: ReadonlyMap<string, string | null>;
}): PlacementEvaluation[] {
  const healthByDay = new Map(input.health.map((h) => [h.day, h]));
  const groups = new Map<string, PlacementEvaluation & { allowedSenders: Set<string>; cut: Set<string> }>();
  for (const r of input.rows) {
    const provider = ESP_PROVIDER.get(r.esp);
    if (!provider) continue;
    const key = `${r.day}|${provider}`;
    const g =
      groups.get(key) ??
      {
        day: r.day,
        provider,
        providerLabel: PROVIDER_LABELS[provider],
        fleet: { seeds: 0, found: 0, inbox: 0, inboxPct: null },
        allowed: { senders: 0, seeds: 0, found: 0, inbox: 0, inboxPct: null },
        mailboxesCut: 0,
        measured: false,
        below: false,
        allowedSenders: new Set<string>(),
        cut: new Set<string>(),
      };
    const found = r.inbox + r.spam;
    g.fleet.seeds += r.seeds;
    g.fleet.found += found;
    g.fleet.inbox += r.inbox;
    const sender = r.sender.toLowerCase();
    if (input.inProduction.has(sender)) {
      const h = healthByDay.get(r.day);
      const mailbox = mailboxOf(sender, input.loginOf.get(sender));
      const cut =
        (h?.refused ?? []).map((m) => m.toLowerCase()).includes(mailbox) ||
        (h?.mostlySpam[provider] ?? []).map((m) => m.toLowerCase()).includes(sender);
      if (cut) g.cut.add(mailbox);
      else {
        g.allowedSenders.add(sender);
        g.allowed.seeds += r.seeds;
        g.allowed.found += found;
        g.allowed.inbox += r.inbox;
      }
    }
    groups.set(key, g);
  }
  return [...groups.values()]
    .map(({ allowedSenders, cut, ...g }) => {
      const allowedPct = pct(g.allowed.inbox, g.allowed.found);
      const measured = g.allowed.found >= PLACEMENT_ALERT_MIN_FOUND_SEEDS;
      return {
        ...g,
        fleet: { ...g.fleet, inboxPct: pct(g.fleet.inbox, g.fleet.found) },
        allowed: { ...g.allowed, senders: allowedSenders.size, inboxPct: allowedPct },
        mailboxesCut: cut.size,
        measured,
        below: measured && allowedPct !== null && allowedPct < PLACEMENT_ALERT_INBOX_PCT_BAR,
      };
    })
    .sort((a, b) => a.day.localeCompare(b.day) || a.provider.localeCompare(b.provider));
}

export interface Episode {
  provider: RecipientProvider;
  startedOn: string;
  lastBelowOn: string;
  closedOn: string | null;
  inboxPct: number;
  foundSeeds: number;
  thresholdPct: number;
  mailboxesCut: number;
  alertStatus: "sent" | "failed" | null;
}

/**
 * Pure: walk the evaluations oldest first against the open episode of each
 * provider. A measured day below the bar opens an episode (none open) or extends
 * it; a measured day at or above the bar closes it; an unmeasured day changes
 * nothing. Returns the episodes to write (new, extended, closed).
 */
export function applyEvaluations(
  evaluations: readonly PlacementEvaluation[],
  open: ReadonlyMap<RecipientProvider, Episode>,
): Episode[] {
  const current = new Map(open);
  const touched = new Map<string, Episode>();
  for (const e of evaluations) {
    if (!e.measured || e.allowed.inboxPct === null) continue;
    const ep = current.get(e.provider);
    if (e.below) {
      if (ep && e.day < ep.startedOn) continue;
      const next: Episode = ep
        ? {
            ...ep,
            lastBelowOn: e.day > ep.lastBelowOn ? e.day : ep.lastBelowOn,
            ...(e.day >= ep.lastBelowOn ? { inboxPct: e.allowed.inboxPct, foundSeeds: e.allowed.found, mailboxesCut: e.mailboxesCut } : {}),
          }
        : {
            provider: e.provider,
            startedOn: e.day,
            lastBelowOn: e.day,
            closedOn: null,
            inboxPct: e.allowed.inboxPct,
            foundSeeds: e.allowed.found,
            thresholdPct: PLACEMENT_ALERT_INBOX_PCT_BAR,
            mailboxesCut: e.mailboxesCut,
            alertStatus: null,
          };
      current.set(e.provider, next);
      touched.set(`${next.provider}|${next.startedOn}`, next);
    } else if (ep && e.day > ep.lastBelowOn) {
      const closed = { ...ep, closedOn: e.day };
      current.delete(e.provider);
      touched.set(`${closed.provider}|${closed.startedOn}`, closed);
    }
  }
  return [...touched.values()];
}

// ─── IO ──────────────────────────────────────────────────────────────────────

function rowsOf<T>(result: unknown): T[] {
  if (Array.isArray(result)) return result as T[];
  const rows = (result as { rows?: unknown })?.rows;
  return Array.isArray(rows) ? (rows as T[]) : [];
}

function listOf(v: unknown): string[] {
  return Array.isArray(v) ? v.map(String) : [];
}

export async function loadEvaluations(days: number, asOf: Date = new Date()): Promise<PlacementEvaluation[]> {
  const toDay = asOf.toISOString().slice(0, 10);
  const fromDay = new Date(Date.parse(`${toDay}T00:00:00Z`) - (days - 1) * 86_400_000).toISOString().slice(0, 10);
  const [seeds, health, accounts] = await Promise.all([
    db.execute(sql`
      SELECT to_char((d.dispatched_at AT TIME ZONE 'UTC')::date, 'YYYY-MM-DD') AS day, d.recipient_esp AS esp,
             lower(d.sender_email) AS sender, count(*)::int AS seeds,
             count(o.id) FILTER (WHERE o.placement = 'inbox')::int AS inbox,
             count(o.id) FILTER (WHERE o.placement = 'spam')::int AS spam
      FROM seed_placement_dispatches d
      LEFT JOIN seed_placement_observations o ON o.message_id = d.message_id AND o.receiver_email = d.receiver_email
      WHERE d.outcome = 'sent' AND (d.dispatched_at AT TIME ZONE 'UTC')::date BETWEEN ${fromDay}::date AND ${toDay}::date
      GROUP BY 1, 2, 3
    `),
    db.execute(sql`
      SELECT to_char(day, 'YYYY-MM-DD') AS day, refused, mostly_spam FROM sender_health_daily
      WHERE day BETWEEN ${fromDay}::date AND ${toDay}::date
    `),
    db.execute(sql`SELECT lower(email) AS email, lower(mailbox_login) AS mailbox_login, lifecycle_status FROM instantly_accounts`),
  ]);
  const accountRows = rowsOf<{ email: string; mailbox_login: string | null; lifecycle_status: string | null }>(accounts);
  return evaluatePlacement({
    rows: rowsOf<Record<string, unknown>>(seeds).map((r) => ({
      day: String(r.day),
      esp: Number(r.esp),
      sender: String(r.sender),
      seeds: Number(r.seeds),
      inbox: Number(r.inbox),
      spam: Number(r.spam),
    })),
    health: rowsOf<Record<string, unknown>>(health).map((r) => {
      const spam = (r.mostly_spam ?? {}) as Record<string, unknown>;
      return {
        day: String(r.day),
        refused: listOf(r.refused),
        mostlySpam: { google: listOf(spam.google), microsoft: listOf(spam.microsoft), microsoft_consumer: listOf(spam.microsoft_consumer) },
      };
    }),
    inProduction: new Set(accountRows.filter((a) => a.lifecycle_status === "in_production").map((a) => a.email)),
    loginOf: new Map(accountRows.map((a) => [a.email, a.mailbox_login])),
  });
}

function mapEpisode(r: Record<string, unknown>): Episode {
  return {
    provider: String(r.provider) as RecipientProvider,
    startedOn: String(r.started_on),
    lastBelowOn: String(r.last_below_on),
    closedOn: r.closed_on === null || r.closed_on === undefined ? null : String(r.closed_on),
    inboxPct: Number(r.inbox_pct),
    foundSeeds: Number(r.found_seeds),
    thresholdPct: Number(r.threshold_pct),
    mailboxesCut: Number(r.mailboxes_cut),
    alertStatus: r.alert_status === "sent" || r.alert_status === "failed" ? r.alert_status : null,
  };
}

const EPISODE_COLUMNS = sql`provider, to_char(started_on, 'YYYY-MM-DD') AS started_on,
  to_char(last_below_on, 'YYYY-MM-DD') AS last_below_on, to_char(closed_on, 'YYYY-MM-DD') AS closed_on,
  inbox_pct, found_seeds, threshold_pct, mailboxes_cut, alert_status, alert_error, alerted_at`;

export async function loadEpisodes(limit = 50): Promise<(Episode & { alertError: string | null; alertedAt: string | null })[]> {
  return rowsOf<Record<string, unknown>>(
    await db.execute(sql`SELECT ${EPISODE_COLUMNS} FROM placement_alert_episodes ORDER BY started_on DESC, provider LIMIT ${limit}`),
  ).map((r) => ({
    ...mapEpisode(r),
    alertError: r.alert_error === null || r.alert_error === undefined ? null : String(r.alert_error),
    alertedAt: r.alerted_at ? new Date(String(r.alerted_at instanceof Date ? r.alerted_at.toISOString() : r.alerted_at)).toISOString() : null,
  }));
}

async function loadOpenEpisodes(): Promise<Map<RecipientProvider, Episode>> {
  const rows = rowsOf<Record<string, unknown>>(
    await db.execute(sql`SELECT ${EPISODE_COLUMNS} FROM placement_alert_episodes WHERE closed_on IS NULL`),
  );
  return new Map(rows.map((r) => [String(r.provider) as RecipientProvider, mapEpisode(r)]));
}

async function writeEpisode(ep: Episode): Promise<void> {
  await db.execute(sql`
    INSERT INTO placement_alert_episodes
      (provider, started_on, last_below_on, closed_on, inbox_pct, found_seeds, threshold_pct, mailboxes_cut, updated_at)
    VALUES (${ep.provider}, ${ep.startedOn}::date, ${ep.lastBelowOn}::date, ${ep.closedOn}::date, ${ep.inboxPct},
            ${ep.foundSeeds}, ${ep.thresholdPct}, ${ep.mailboxesCut}, now())
    ON CONFLICT (provider, started_on) DO UPDATE SET
      last_below_on = EXCLUDED.last_below_on, closed_on = EXCLUDED.closed_on, inbox_pct = EXCLUDED.inbox_pct,
      found_seeds = EXCLUDED.found_seeds, mailboxes_cut = EXCLUDED.mailboxes_cut, updated_at = now()
  `);
}

export interface PlacementAlertTick {
  evaluations: PlacementEvaluation[];
  written: Episode[];
}

/**
 * One pass: evaluate the last days and record the episodes. Sends nothing, ever.
 * `dryRun` writes nothing either.
 */
export async function runPlacementAlert(opts: { dryRun: boolean; asOf?: Date }): Promise<PlacementAlertTick> {
  const evaluations = await loadEvaluations(PLACEMENT_ALERT_EVAL_DAYS, opts.asOf);
  const open = await loadOpenEpisodes();
  const written = applyEvaluations(evaluations, open);
  if (!opts.dryRun) for (const ep of written) await writeEpisode(ep);
  return { evaluations, written };
}

/** The staff read: the daily evaluation series, the bar and its sources, the episodes. */
export async function readPlacementAlerts(days: number, asOf: Date = new Date()) {
  const [evaluations, episodes] = await Promise.all([loadEvaluations(days, asOf), loadEpisodes()]);
  return {
    asOf: asOf.toISOString(),
    bar: {
      inboxPctOfFound: PLACEMENT_ALERT_INBOX_PCT_BAR,
      minFoundSeeds: PLACEMENT_ALERT_MIN_FOUND_SEEDS,
      basis: "inbox / (inbox + spam) of the seeds sent by production mailboxes the automatic cut still lets mail that provider",
      sources: PLACEMENT_ALERT_SOURCES,
    },
    days: evaluations,
    episodes,
  };
}
