/**
 * An out-of-office that states a return date holds our next email until the
 * person is back (OOO-RETURN-DATE-1101, client 2026-11-01: "they will simply
 * delete and clear inbox. why the system is sending messages before they are
 * back?").
 *
 * Pure: the judging prompt, reading the model's answer, and turning the date the
 * message states into the instant our next email may leave. IO lives in
 * `absence-judgment.ts`; the dispatch reads the stored `hold_until`.
 *
 * Reading a date out of free text in any language is a judgement on text, so the
 * MODEL reads it (never a regex). What the model is NOT asked is the YEAR when
 * the message gives none: "until September 21" received in late October is a
 * stale autoresponder, and a model filling the year in would happily answer next
 * September. The year is resolved here, deterministically, from the message date.
 *
 * An out-of-office never closes the person nor stops their follow-ups: it only
 * moves WHEN the next email leaves. No date, a date already past, or a date we
 * cannot read = nothing changes.
 */

const MS_PER_DAY = 24 * 60 * 60 * 1000;

/**
 * Days after the person's first day back before our next email leaves: the
 * first day back is the day they clear the inbox the absence filled.
 */
export const RETURN_MARGIN_DAYS = 1;

/**
 * A stated date with NO year farther than this after the message is not the
 * coming occurrence of that date: it is a stale autoresponder naming a past one
 * ("until September 21" read in late October would otherwise mean next year).
 */
export const YEARLESS_MAX_AHEAD_DAYS = 120;

/** A return farther than this after the message is not a holiday we wait for. */
export const MAX_ABSENCE_DAYS = 365;

/** The text of the autoresponder sent to the model, at most. */
export const ABSENCE_TEXT_MAX_CHARS = 2000;

export type StatedMeaning = "back_on" | "away_through";

/** The date the message states, as the model read it. */
export interface StatedReturn {
  /** Only when the message WRITES the year (or the date is relative to the message date). */
  year: number | null;
  month: number;
  day: number;
  /** `back_on` = the day they are back; `away_through` = the last day of the absence. */
  meaning: StatedMeaning;
}

export const ABSENCE_SYSTEM_PROMPT = `You read an automatic reply (an out-of-office message) that a person's mailbox sent back to an email. Find the date the message says the person is back.

Answer JSON only: {"date": {"year": <number or null>, "month": <1-12>, "day": <1-31>} or null, "meaning": "back_on" or "away_through" or null}

- "back_on": the message names the day they are back ("I will return on 1st November", "back in the office on Monday 3 November", "de retour le 4 novembre").
- "away_through": the message names the last day of the absence ("out of office until 2nd Nov", "on PTO from 10/16 to 10/26", "hasta el 23 de octubre", "bis zum 5. November", "jusqu'au 4 novembre"). "Until" always means away_through.
- The message can be in any language.
- year: only when the message writes the year. When it does not, answer null; never guess it. When the message only gives a relative day ("back Monday", "back tomorrow"), compute the date from the message date given above the message and give its year.
- Numeric dates: decide the day/month order from the message itself (a number above 12 is the day). When the order cannot be decided, answer {"date": null, "meaning": null}.
- No return date at all, or several dates that disagree: answer {"date": null, "meaning": null}.
- Ignore dates in quoted earlier emails, signatures and legal notices.`;

const WEEKDAYS = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

/** The user message: the message date (to resolve "back Monday"), the subject, their words. */
export function absenceJudgmentMessage(input: {
  receivedAt: Date;
  subject: string | null;
  text: string;
}): string {
  const day = input.receivedAt.toISOString().slice(0, 10);
  const weekday = WEEKDAYS[input.receivedAt.getUTCDay()];
  const subject = input.subject?.replace(/\s+/g, " ").trim();
  return [
    `Message date: ${day} (${weekday})`,
    ...(subject ? [`Subject: ${subject.slice(0, 300)}`] : []),
    "",
    input.text.trim().slice(0, ABSENCE_TEXT_MAX_CHARS),
  ].join("\n");
}

function isInt(v: unknown): v is number {
  return typeof v === "number" && Number.isInteger(v);
}

/** A valid calendar day (UTC midnight), or null. */
function utcDay(year: number, month: number, day: number): Date | null {
  if (month < 1 || month > 12 || day < 1 || day > 31) return null;
  const d = new Date(Date.UTC(year, month - 1, day));
  if (d.getUTCFullYear() !== year || d.getUTCMonth() !== month - 1 || d.getUTCDate() !== day) return null;
  return d;
}

/**
 * Read the model's answer. `undefined` = it answered nothing we can read (stored
 * `unreadable`); `null` = it read the message and found no usable date.
 */
export function parseStatedReturn(result: {
  json?: Record<string, unknown>;
  content?: string;
}): StatedReturn | null | undefined {
  let body: Record<string, unknown> | undefined = result.json;
  if (!body && typeof result.content === "string") {
    try {
      const parsed = JSON.parse(result.content) as unknown;
      if (parsed && typeof parsed === "object") body = parsed as Record<string, unknown>;
    } catch {
      return undefined;
    }
  }
  if (!body || !("date" in body)) return undefined;
  const date = body.date;
  if (date === null) return null;
  if (!date || typeof date !== "object") return undefined;
  const { year, month, day } = date as Record<string, unknown>;
  const meaning = body.meaning;
  if (meaning !== "back_on" && meaning !== "away_through") return undefined;
  if (!isInt(month) || !isInt(day)) return undefined;
  if (year !== null && year !== undefined && !isInt(year)) return undefined;
  return { year: isInt(year) ? year : null, month, day, meaning };
}

export type AbsenceOutcome = "return_date" | "no_date" | "stale" | "unreadable";

export interface ResolvedAbsence {
  outcome: AbsenceOutcome;
  /** YYYY-MM-DD, the first day the person is back. Set only for `return_date`. */
  firstDayBack: string | null;
  /** Our next email to them leaves no earlier than this. Set only for `return_date`. */
  holdUntil: Date | null;
}

/**
 * The instant our next email may leave, from the date the message states.
 *
 * - year written: that day; no year: the coming occurrence after the message
 *   date, and only within {@link YEARLESS_MAX_AHEAD_DAYS} (else stale);
 * - first day back = the day (`back_on`) or the day after (`away_through`);
 * - a first day back at or before the message day = stale (the autoresponder
 *   names an absence that is over); beyond {@link MAX_ABSENCE_DAYS} = no date;
 * - hold = first day back + {@link RETURN_MARGIN_DAYS}, 00:00 UTC. The person's
 *   own business-hours window still decides the hour inside that day.
 */
export function resolveAbsence(stated: StatedReturn | null | undefined, receivedAt: Date): ResolvedAbsence {
  const none = (outcome: AbsenceOutcome): ResolvedAbsence => ({ outcome, firstDayBack: null, holdUntil: null });
  if (stated === undefined) return none("unreadable");
  if (stated === null) return none("no_date");

  const messageDay = Date.UTC(receivedAt.getUTCFullYear(), receivedAt.getUTCMonth(), receivedAt.getUTCDate());
  let date: Date | null;
  if (stated.year !== null) {
    date = utcDay(stated.year, stated.month, stated.day);
    if (!date) return none("no_date");
  } else {
    const year = receivedAt.getUTCFullYear();
    date = utcDay(year, stated.month, stated.day);
    // A day that does not exist this year (29 February) may exist next year.
    if (!date || date.getTime() < messageDay) date = utcDay(year + 1, stated.month, stated.day);
    if (!date) return none("no_date");
    if (date.getTime() - messageDay > YEARLESS_MAX_AHEAD_DAYS * MS_PER_DAY) return none("stale");
  }

  const back = new Date(date.getTime() + (stated.meaning === "away_through" ? MS_PER_DAY : 0));
  if (back.getTime() <= messageDay) return none("stale");
  if (back.getTime() - messageDay > MAX_ABSENCE_DAYS * MS_PER_DAY) return none("no_date");

  return {
    outcome: "return_date",
    firstDayBack: back.toISOString().slice(0, 10),
    holdUntil: new Date(back.getTime() + RETURN_MARGIN_DAYS * MS_PER_DAY),
  };
}
