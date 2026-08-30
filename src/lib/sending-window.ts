/**
 * The LEAD's sending window — which day a given prospect's email actually goes
 * out on, in their own timezone, and therefore which day of the sending
 * mailbox's quota it consumes.
 *
 * ── Why the lead's timezone decides a MAILBOX's day ───────────────────────────
 * Every campaign we create carries a Mon-Fri 08:00-17:00 schedule in the
 * RECIPIENT's timezone (`createCampaign` → `campaign_schedule.schedules[0]`,
 * one campaign = one lead), so a send fires inside that prospect's local
 * business hours. The mailbox, however, is billed against a UTC calendar day.
 * The two are not the same day: a New Zealand lead's local Monday 08:00 is
 * SUNDAY 19:00 UTC, so that email spends the mailbox's Sunday, not its Monday.
 *
 * Send selection has to book the day the mail will really leave, or it hands out
 * capacity on a day nothing consumes and over-books the day that does — the same
 * class of error as measuring a Saturday (see `sending-calendar.ts`), one axis
 * over.
 *
 * ── The window is a LOWER BOUND, exactly like the cadence ─────────────────────
 * This resolves the earliest instant the schedule permits. The actual dispatch
 * can still slip later (daily-limit saturation, throttling, a pause) — and on
 * the self-send transport it additionally waits for a UTC sending day, which its
 * own dispatch worker enforces. Both slips run in the same direction: we reserve
 * a slot and the send arrives on it or after it, never before. Do NOT read a
 * booked day as a promise the mail left that day.
 *
 * ── Scope ────────────────────────────────────────────────────────────────────
 * Send SELECTION and the capacity snapshot that feeds it, plus the self-send
 * dispatch worker's per-lead gate. The ops projections (`aggregateQueueBreakdown`,
 * `sending-forecast`) keep bucketing on the RAW nominal UTC day with no snap of
 * any kind, which is what makes those two surfaces agree with each other
 * step-for-step — see CLAUDE.md, "Do NOT reintroduce a weekend snap".
 */

import { canonicalIanaTimezone } from "./timezone";
import { dateKeyUTC } from "./sending-forecast";
import { SENDING_WEEKDAYS } from "./sending-calendar";

/** First hour (local) a campaign may dispatch — `campaign_schedule.timing.from`. */
export const SEND_WINDOW_START_HOUR = 8;

/** Hour (local) the window closes — `campaign_schedule.timing.to` (17:00). */
export const SEND_WINDOW_END_HOUR = 17;

/**
 * Timezone assumed for a lead we hold none for.
 *
 * Deliberately the SAME value `resolveInstantlyTimezone` degrades to, so the day
 * this module books is the day Instantly is actually scheduling against. It is
 * not a guess about the prospect: it is a mirror of the schedule we shipped.
 */
export const DEFAULT_LEAD_TIMEZONE = "America/Chicago";

/**
 * How far forward to look for an open window before giving up. A week always
 * contains a local weekday, so anything past this is a broken zone rather than a
 * long weekend — and a silent infinite loop on the send path is the one outcome
 * worse than a loud throw.
 */
const MAX_WINDOW_SEARCH_DAYS = 14;

interface LocalParts {
  year: number;
  month: number; // 1-based
  day: number;
  hour: number;
  minute: number;
  second: number;
}

const PART_FORMATTERS = new Map<string, Intl.DateTimeFormat>();

/**
 * Memo for the two Intl-backed conversions.
 *
 * This is a throughput fix, not a micro-optimisation. The capacity snapshot runs
 * this over EVERY queued sequence in the fleet (~6,000, several steps each), and
 * the uncached version took ~9 SECONDS at that scale — on the send path, behind
 * a cache email-gateway abandons after ~10s and then retries, so it would have
 * turned one slow snapshot into a retry storm.
 *
 * ⚠️ The dominant cost was NOT `formatToParts`, which is what it looks like.
 * It was `canonicalIanaTimezone`: it constructs a fresh `Intl.DateTimeFormat`
 * on every miss of its legacy-alias table, and every PRIMARY zone name misses —
 * ~290µs per call, paid once per sequence per step. Measured 9,300ms → 86ms.
 * If this ever gets slow again, profile the zone resolution before the
 * formatting.
 *
 * The instants themselves repeat heavily across sequences (every never-contacted
 * lead resolves the same "now", every chained hop lands on the same window-open
 * instant), so plain key-value memos collapse the rest to a few dozen real
 * conversions.
 *
 * Cleared wholesale past a bound rather than evicted one by one: each is a pure
 * function of its key, so dropping everything costs a recompute and never a
 * wrong answer, and an unbounded map in a long-lived process is a leak.
 */
const MAX_MEMO_ENTRIES = 20_000;
const PARTS_MEMO = new Map<string, LocalParts>();
const ZONED_MEMO = new Map<string, number>();
const ZONE_MEMO = new Map<string, string>();
const WINDOW_MEMO = new Map<string, number>();

function memoized<T>(memo: Map<string, T>, key: string, compute: () => T): T {
  const hit = memo.get(key);
  if (hit !== undefined) return hit;
  if (memo.size >= MAX_MEMO_ENTRIES) memo.clear();
  const value = compute();
  memo.set(key, value);
  return value;
}

function formatterFor(timeZone: string): Intl.DateTimeFormat {
  const cached = PART_FORMATTERS.get(timeZone);
  if (cached) return cached;
  const fmt = new Intl.DateTimeFormat("en-US", {
    timeZone,
    hourCycle: "h23",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
  PART_FORMATTERS.set(timeZone, fmt);
  return fmt;
}

/**
 * The wall clock a prospect in `timeZone` reads at `instant`.
 *
 * `Intl` is the only correct source here — it carries the zone's full DST
 * history, which a fixed offset does not. An unknown zone throws from the
 * formatter rather than silently resolving to UTC; callers canonicalize first.
 */
export function localParts(instant: Date, timeZone: string): LocalParts {
  return memoized(PARTS_MEMO, `${timeZone}|${instant.getTime()}`, () =>
    readLocalParts(instant, timeZone),
  );
}

function readLocalParts(instant: Date, timeZone: string): LocalParts {
  const parts = formatterFor(timeZone).formatToParts(instant);
  const read = (type: string): number => {
    const found = parts.find((p) => p.type === type);
    if (!found) {
      throw new Error(
        `[instantly-service] Intl returned no "${type}" part for timezone "${timeZone}"`,
      );
    }
    return Number(found.value);
  };
  return {
    year: read("year"),
    month: read("month"),
    day: read("day"),
    hour: read("hour"),
    minute: read("minute"),
    second: read("second"),
  };
}

/** Offset (ms) of `timeZone` from UTC at `instant` — positive east of Greenwich. */
function offsetMsAt(instant: Date, timeZone: string): number {
  const p = localParts(instant, timeZone);
  return (
    Date.UTC(p.year, p.month - 1, p.day, p.hour, p.minute, p.second) -
    instant.getTime()
  );
}

/**
 * The UTC instant at which a prospect in `timeZone` reads the given wall clock.
 *
 * Two passes: the first guesses the offset from the naive instant, the second
 * re-reads it at the corrected one. That second pass is what keeps a DST
 * transition from landing an hour out — the offset that applies is the offset AT
 * the target instant, not at the guess.
 */
function zonedTimeToUtc(
  year: number,
  month: number,
  day: number,
  hour: number,
  timeZone: string,
): Date {
  const naive = Date.UTC(year, month - 1, day, hour, 0, 0);
  const ms = memoized(ZONED_MEMO, `${timeZone}|${naive}`, () => {
    const firstPass = naive - offsetMsAt(new Date(naive), timeZone);
    return naive - offsetMsAt(new Date(firstPass), timeZone);
  });
  return new Date(ms);
}

/** Day-of-week (0=Sunday) of a local calendar date. */
function weekdayOf(year: number, month: number, day: number): number {
  return new Date(Date.UTC(year, month - 1, day)).getUTCDay();
}

/** True when a local calendar date is one the campaign schedule allows (Mon-Fri). */
export function isLocalSendingDay(year: number, month: number, day: number): boolean {
  return SENDING_WEEKDAYS.includes(weekdayOf(year, month, day));
}

function addLocalDays(
  year: number,
  month: number,
  day: number,
  days: number,
): { year: number; month: number; day: number } {
  const shifted = new Date(Date.UTC(year, month - 1, day + days));
  return {
    year: shifted.getUTCFullYear(),
    month: shifted.getUTCMonth() + 1,
    day: shifted.getUTCDate(),
  };
}

/**
 * Resolve a caller-supplied zone to the primary IANA name, or the fleet default.
 *
 * Memoized because `canonicalIanaTimezone` constructs a fresh
 * `Intl.DateTimeFormat` on every miss of its alias table — which every primary
 * name is — and that construction, not the formatting, was the single largest
 * cost in the whole capacity projection (~290µs per call, ~12,000 calls).
 */
export function resolveLeadTimezone(timeZone: string | null | undefined): string {
  if (!timeZone || !timeZone.trim()) return DEFAULT_LEAD_TIMEZONE;
  const raw = timeZone.trim();
  return memoized(ZONE_MEMO, raw, () => canonicalIanaTimezone(raw));
}

/**
 * The first instant at or after `asOf` at which a lead in `timeZone` can be sent.
 *
 * Three cases, and the middle one is the whole point of the function:
 *   - inside today's local window → `asOf` itself, so nothing shifts for the
 *     overwhelmingly common case of a weekday send during business hours;
 *   - before it opens → 08:00 local the SAME local day;
 *   - after it closes, or a local weekend → 08:00 local on the next local
 *     weekday, which is routinely a different UTC day than the caller's own.
 *
 * Throws rather than looping if no window is found inside `MAX_WINDOW_SEARCH_DAYS`
 * — an unreachable state that must not silently book a send onto an arbitrary day.
 */
export function nextLocalSendInstant(asOf: Date, timeZone: string): Date {
  const tz = resolveLeadTimezone(timeZone);
  return new Date(
    memoized(WINDOW_MEMO, `${tz}|${asOf.getTime()}`, () =>
      resolveWindowInstant(asOf, tz).getTime(),
    ),
  );
}

function resolveWindowInstant(asOf: Date, tz: string): Date {
  const now = localParts(asOf, tz);
  let { year, month, day } = now;

  for (let hop = 0; hop <= MAX_WINDOW_SEARCH_DAYS; hop += 1) {
    if (isLocalSendingDay(year, month, day)) {
      if (hop > 0) return zonedTimeToUtc(year, month, day, SEND_WINDOW_START_HOUR, tz);
      // Today: usable only while its window is still open.
      if (now.hour < SEND_WINDOW_START_HOUR) {
        return zonedTimeToUtc(year, month, day, SEND_WINDOW_START_HOUR, tz);
      }
      if (now.hour < SEND_WINDOW_END_HOUR) return asOf;
      // Closed for today — fall through to the next local day.
    }
    ({ year, month, day } = addLocalDays(year, month, day, 1));
  }

  throw new Error(
    `[instantly-service] no sending window within ${MAX_WINDOW_SEARCH_DAYS} days for timezone "${tz}"`,
  );
}

/**
 * The UTC day key (`YYYY-MM-DD`) a send anchored at `asOf` books on the mailbox.
 *
 * This is the join between the two calendars: the window is resolved in the
 * prospect's local time, the answer is the mailbox's UTC day.
 */
export function bookedDayKey(asOf: Date, timeZone: string): string {
  return dateKeyUTC(nextLocalSendInstant(asOf, timeZone));
}

/**
 * Book a whole sequence: the UTC day keys of a send starting at `anchor` and
 * each subsequent step separated by `gapDays`.
 *
 * Each gap is counted in the prospect's LOCAL calendar and the result snapped
 * forward to their next open window — then the NEXT gap chains off that snapped
 * day, not off the nominal one. Chaining off the snapped day is what the real
 * cadence does: a followup's clock starts when the previous email actually went
 * out, so a step pushed over a weekend pushes everything behind it.
 *
 * Returns one key per step: `[anchor, anchor+gap0, anchor+gap0+gap1, …]`.
 */
export function chainBookedDays(
  anchor: Date,
  timeZone: string,
  gapDays: readonly number[],
): string[] {
  const tz = resolveLeadTimezone(timeZone);
  let instant = nextLocalSendInstant(anchor, tz);
  const keys = [dateKeyUTC(instant)];

  for (const gap of gapDays) {
    const p = localParts(instant, tz);
    const days = Number.isFinite(gap) && gap > 0 ? Math.round(gap) : 0;
    const shifted = addLocalDays(p.year, p.month, p.day, days);
    const nominal = zonedTimeToUtc(
      shifted.year,
      shifted.month,
      shifted.day,
      SEND_WINDOW_START_HOUR,
      tz,
    );
    instant = nextLocalSendInstant(nominal, tz);
    keys.push(dateKeyUTC(instant));
  }

  return keys;
}

/**
 * The UTC days a brand-new sequence would book if it were assigned at `asOf`.
 *
 * This is what send selection checks an account against: not "has this mailbox
 * room today" but "has it room on every day this lead will need it".
 */
export function sequenceFootprintDays(
  asOf: Date,
  timeZone: string | null | undefined,
  gapDays: readonly number[],
): string[] {
  return chainBookedDays(asOf, resolveLeadTimezone(timeZone), gapDays);
}

/**
 * True when `asOf` falls inside the lead's open local window.
 *
 * The self-send dispatch worker's per-lead gate: a step that is due by cadence
 * still waits for its prospect's business hours, exactly as Instantly's schedule
 * makes it wait on the other transport.
 */
export function isWithinLocalSendWindow(asOf: Date, timeZone: string | null | undefined): boolean {
  const tz = resolveLeadTimezone(timeZone);
  const p = localParts(asOf, tz);
  return (
    isLocalSendingDay(p.year, p.month, p.day) &&
    p.hour >= SEND_WINDOW_START_HOUR &&
    p.hour < SEND_WINDOW_END_HOUR
  );
}
