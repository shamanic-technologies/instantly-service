/**
 * Provider reads of our Units (ENGINE-GET-ASK-1123).
 *
 * This service is the Provider of the outcomes `email_sent` and `email_queued`.
 * A Script asks, before it acts, which Units already exist for a person since a
 * date of its choosing: `GET /orgs/units/email-sent` and
 * `GET /orgs/units/email-queued`. workflow-service finds the two reads by their
 * OpenAPI tag (`provider:<outcome>`) in our served `openapi.json` and calls them
 * with the org's identity headers and the script's query.
 *
 * - A Unit of `email_sent` = one email of ours that really left for the person:
 *   a sequence step (a real, non-withdrawn `email_sent` silver event; an
 *   inferred one counts, a DSN proves it left) or an answer sent in a thread
 *   (`reply_send_outcomes`).
 * - A Unit of `email_queued` = one step we decided to send (`sequence_costs`,
 *   the send queue: one row per step, written at the queue decision), whatever
 *   became of it (`waiting` / `sent` / `cancelled`).
 *
 * The person is named by address, by lead-service lead id, or by company domain
 * (OR'ed; at least one is required: the read never lists an org's whole
 * history). Every brand and campaign of the CALLING org, never another org's.
 *
 * Reads our own tables only: no outbound call, no cost, nothing sent, no rule of
 * the send path. What a script does with the answer is its tactic.
 */
import { sql, type SQL } from "drizzle-orm";

import { db } from "../db";
import { rowsOf } from "./engaged-leads";

export const UNITS_DEFAULT_LIMIT = 100;
export const UNITS_MAX_LIMIT = 1000;
export const UNITS_MAX_FILTER_VALUES = 100;

/** The Engine outcomes scripts name. */
export type ProviderOutcome = "email_sent" | "email_queued";
/** What this service declares to runs-service's ledger for each. */
export const LEDGER_OUTCOME: Record<ProviderOutcome, string> = {
  email_sent: "message_sent",
  email_queued: "email_queued",
};
export const UNIT_SUBJECT_SERVICE = "lead-service";

export const SENT_KINDS = ["first", "follow_up", "answer"] as const;
export const QUEUED_KINDS = ["first", "follow_up"] as const;
export const QUEUED_STATES = ["waiting", "sent", "cancelled"] as const;
export type SentKind = (typeof SENT_KINDS)[number];
export type QueuedKind = (typeof QUEUED_KINDS)[number];
export type QueuedState = (typeof QUEUED_STATES)[number];

/** `sequence_costs.status` -> the state a script reads. Closed set. */
const STATE_BY_HOLD_STATUS: Record<string, QueuedState> = {
  provisioned: "waiting",
  actual: "sent",
  cancelled: "cancelled",
};
const HOLD_STATUS_BY_STATE: Record<QueuedState, string> = {
  waiting: "provisioned",
  sent: "actual",
  cancelled: "cancelled",
};

// ─── Query ──────────────────────────────────────────────────────────────────

/** A query the caller must fix: the route answers 400 `{error: code, message}`. */
export class UnitsQueryError extends Error {
  constructor(
    public readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = "UnitsQueryError";
  }
}

export class InvalidCursorError extends Error {
  constructor(cursor: string) {
    super(`invalid cursor: ${cursor}`);
    this.name = "InvalidCursorError";
  }
}

export interface UnitsFilters {
  orgId: string;
  emails: string[];
  leadIds: string[];
  domains: string[];
  /** ISO 8601 UTC. Inclusive lower bound on the Unit's `at`. */
  since?: string;
  /** ISO 8601 UTC. Exclusive upper bound. */
  until?: string;
  brandId?: string;
  campaignId?: string;
  /** Undefined = every kind. */
  kinds?: string[];
  /** email-queued only. Undefined = every state. */
  states?: QueuedState[];
  limit: number;
  cursor?: string;
}

/** The raw query as Express hands it over (a repeated param is an array). */
export interface RawUnitsQuery {
  emails?: unknown;
  leadIds?: unknown;
  domains?: unknown;
  since?: unknown;
  until?: unknown;
  brandId?: unknown;
  campaignId?: unknown;
  kinds?: unknown;
  states?: unknown;
  limit?: unknown;
  cursor?: unknown;
}

/** `a,b` / `["a","b,c"]` -> trimmed, non-empty, de-duplicated values in order. */
export function splitList(value: unknown): string[] {
  if (value == null) return [];
  const parts = (Array.isArray(value) ? value : [value]).flatMap((v) => String(v).split(","));
  const seen = new Set<string>();
  for (const part of parts) {
    const trimmed = part.trim();
    if (trimmed !== "") seen.add(trimmed);
  }
  return [...seen];
}

function boundedList(name: string, values: string[]): string[] {
  if (values.length > UNITS_MAX_FILTER_VALUES) {
    throw new UnitsQueryError(
      "too_many_values",
      `${name}: at most ${UNITS_MAX_FILTER_VALUES} values per request (got ${values.length})`,
    );
  }
  return values;
}

function closedList<T extends string>(name: string, value: unknown, allowed: readonly T[]): T[] | undefined {
  const values = splitList(value).map((v) => v.toLowerCase());
  if (values.length === 0) return undefined;
  const unknown = values.filter((v) => !(allowed as readonly string[]).includes(v));
  if (unknown.length > 0) {
    throw new UnitsQueryError(
      `invalid_${name}`,
      `${name}: unknown value(s) ${unknown.join(", ")}; allowed: ${allowed.join(", ")}`,
    );
  }
  return [...new Set(values)] as T[];
}

/** An ISO 8601 date or datetime -> ISO UTC. A datetime with no zone is read as UTC. */
function instant(name: string, value: unknown): string | undefined {
  if (value == null || value === "") return undefined;
  const raw = String(Array.isArray(value) ? value[0] : value).trim();
  const shaped = /^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?)?$/i.test(raw);
  const dateOnly = /^\d{4}-\d{2}-\d{2}$/.test(raw);
  const zoned = /(Z|[+-]\d{2}:?\d{2})$/i.test(raw);
  const parsed = shaped ? new Date(dateOnly || zoned ? raw.replace(" ", "T") : `${raw.replace(" ", "T")}Z`) : null;
  if (!parsed || Number.isNaN(parsed.getTime())) {
    throw new UnitsQueryError(`invalid_${name}`, `${name}: not an ISO 8601 datetime (${raw})`);
  }
  return parsed.toISOString();
}

function single(value: unknown): string | undefined {
  if (value == null) return undefined;
  const raw = String(Array.isArray(value) ? value[0] : value).trim();
  return raw === "" ? undefined : raw;
}

/** Normalises and validates a query. Throws `UnitsQueryError` (400). */
export function parseUnitsQuery(outcome: ProviderOutcome, orgId: string, raw: RawUnitsQuery): UnitsFilters {
  const emails = boundedList("emails", [...new Set(splitList(raw.emails).map((e) => e.toLowerCase()))]);
  const notAddresses = emails.filter((e) => !/^[^@\s]+@[^@\s]+$/.test(e));
  if (notAddresses.length > 0) {
    throw new UnitsQueryError("invalid_emails", `emails: not an address: ${notAddresses.join(", ")}`);
  }
  const leadIds = boundedList("leadIds", splitList(raw.leadIds));
  const domains = boundedList(
    "domains",
    [...new Set(splitList(raw.domains).map((d) => d.toLowerCase().replace(/^@+/, "")))].filter((d) => d !== ""),
  );
  const notDomains = domains.filter((d) => /[@\s]/.test(d));
  if (notDomains.length > 0) {
    throw new UnitsQueryError(
      "invalid_domains",
      `domains: a company domain is the part after '@' (got ${notDomains.join(", ")})`,
    );
  }
  if (emails.length === 0 && leadIds.length === 0 && domains.length === 0) {
    throw new UnitsQueryError(
      "person_filter_required",
      "Name who you ask about: at least one of emails, leadIds, domains is required",
    );
  }

  const since = instant("since", raw.since);
  const until = instant("until", raw.until);

  let limit = UNITS_DEFAULT_LIMIT;
  const rawLimit = single(raw.limit);
  if (rawLimit !== undefined) {
    limit = Number(rawLimit);
    if (!Number.isInteger(limit) || limit < 1 || limit > UNITS_MAX_LIMIT) {
      throw new UnitsQueryError("invalid_limit", `limit: an integer from 1 to ${UNITS_MAX_LIMIT}`);
    }
  }

  return {
    orgId,
    emails,
    leadIds,
    domains,
    since,
    until,
    brandId: single(raw.brandId),
    campaignId: single(raw.campaignId),
    kinds: closedList("kinds", raw.kinds, outcome === "email_sent" ? SENT_KINDS : QUEUED_KINDS),
    states: outcome === "email_queued" ? closedList("states", raw.states, QUEUED_STATES) : undefined,
    limit,
    cursor: single(raw.cursor),
  };
}

// ─── Cursor ─────────────────────────────────────────────────────────────────

/** Keyset position: the last Unit's instant at full microsecond precision + its id. */
export interface UnitCursor {
  /** `YYYY-MM-DDTHH:MM:SS.ffffff`, UTC, no zone (as Postgres printed it). */
  at: string;
  unitId: string;
}

const CURSOR_AT = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}$/;

export function encodeUnitCursor(cursor: UnitCursor): string {
  return Buffer.from(JSON.stringify([cursor.at, cursor.unitId]), "utf8").toString("base64url");
}

/** Throws on a cursor this service did not mint: the route answers 400. */
export function decodeUnitCursor(cursor: string): UnitCursor {
  let parsed: unknown;
  try {
    parsed = JSON.parse(Buffer.from(cursor, "base64url").toString("utf8"));
  } catch {
    throw new InvalidCursorError(cursor);
  }
  if (
    !Array.isArray(parsed) ||
    parsed.length !== 2 ||
    typeof parsed[0] !== "string" ||
    typeof parsed[1] !== "string" ||
    !CURSOR_AT.test(parsed[0]) ||
    parsed[1] === ""
  ) {
    throw new InvalidCursorError(cursor);
  }
  return { at: parsed[0], unitId: parsed[1] };
}

// ─── Units ──────────────────────────────────────────────────────────────────

interface UnitBase {
  unitId: string;
  /** The person in lead-service. Null when the sequence holds no lead id. */
  subject: { service: string; id: string } | null;
  email: string;
  domain: string;
  /** ISO 8601 UTC. */
  at: string;
  step: number | null;
  /** The send continued an earlier thread with the person. */
  continuesThread: boolean;
  campaignId: string | null;
  brandIds: string[];
  /** This service's per-person sequence id (`instantly_campaign_id`). */
  sequenceId: string;
}

export interface EmailSentUnit extends UnitBase {
  outcome: "email_sent";
  ledgerOutcome: string;
  kind: SentKind;
  bounced: boolean;
}

export interface EmailQueuedUnit extends UnitBase {
  outcome: "email_queued";
  ledgerOutcome: string;
  kind: QueuedKind;
  state: QueuedState;
  /** The ledger's `email_queued` Unit exists (declared since 2026-11-21 only). */
  declaredOnLedger: boolean;
}

export interface UnitRow {
  unitId: unknown;
  /** `to_char(at, 'YYYY-MM-DD"T"HH24:MI:SS.US')`. */
  atText: unknown;
  step: unknown;
  kind: unknown;
  email: unknown;
  leadId: unknown;
  continuesThread: unknown;
  campaignId: unknown;
  brandIds: unknown;
  sequenceId: unknown;
  bounced?: unknown;
  holdStatus?: unknown;
  declaredOnLedger?: unknown;
}

function atIso(row: UnitRow): string {
  const text = String(row.atText ?? "");
  if (!CURSOR_AT.test(text)) {
    throw new Error(`[instantly-service] provider-units: unit ${String(row.unitId)} has no readable instant (${text})`);
  }
  return new Date(`${text}Z`).toISOString();
}

/** node-postgres hands a `text[]` back as raw text on some paths: never trust it is an array. */
function brandIdsOf(value: unknown): string[] {
  return Array.isArray(value) ? value.map(String) : [];
}

function baseOf(row: UnitRow): UnitBase {
  const email = String(row.email ?? "").toLowerCase();
  const leadId = row.leadId == null || row.leadId === "" ? null : String(row.leadId);
  return {
    unitId: String(row.unitId),
    subject: leadId === null ? null : { service: UNIT_SUBJECT_SERVICE, id: leadId },
    email,
    domain: email.includes("@") ? email.slice(email.lastIndexOf("@") + 1) : "",
    at: atIso(row),
    step: row.step == null ? null : Number(row.step),
    continuesThread: row.continuesThread === true,
    campaignId: row.campaignId == null ? null : String(row.campaignId),
    brandIds: brandIdsOf(row.brandIds),
    sequenceId: String(row.sequenceId),
  };
}

function kindOf<T extends string>(row: UnitRow, allowed: readonly T[]): T {
  const kind = String(row.kind);
  if (!(allowed as readonly string[]).includes(kind)) {
    throw new Error(`[instantly-service] provider-units: unit ${String(row.unitId)} has an unknown kind (${kind})`);
  }
  return kind as T;
}

export function toEmailSentUnit(row: UnitRow): EmailSentUnit {
  const kind = kindOf(row, SENT_KINDS);
  const { unitId, ...rest } = baseOf(row);
  return {
    unitId,
    outcome: "email_sent",
    ledgerOutcome: LEDGER_OUTCOME.email_sent,
    ...rest,
    kind,
    bounced: kind === "answer" ? false : row.bounced === true,
  };
}

/** Throws on a hold status outside the closed set: never a guessed state. */
export function queuedStateOf(holdStatus: unknown): QueuedState {
  const state = STATE_BY_HOLD_STATUS[String(holdStatus)];
  if (state === undefined) {
    throw new Error(`[instantly-service] provider-units: unknown queue status (${String(holdStatus)})`);
  }
  return state;
}

export function toEmailQueuedUnit(row: UnitRow): EmailQueuedUnit {
  const kind = kindOf(row, QUEUED_KINDS);
  const { unitId, ...rest } = baseOf(row);
  return {
    unitId,
    outcome: "email_queued",
    ledgerOutcome: LEDGER_OUTCOME.email_queued,
    ...rest,
    kind,
    state: queuedStateOf(row.holdStatus),
    declaredOnLedger: row.declaredOnLedger === true,
  };
}

export interface UnitsPage<U> {
  units: U[];
  /** Pass back as `cursor`. Null on the last page. */
  nextCursor: string | null;
}

// ─── SQL ────────────────────────────────────────────────────────────────────

const textArray = (values: string[]): SQL => sql`${sql.param(values)}::text[]`;

/**
 * The org's sequences with the person(s) asked about. Each OR branch has its own
 * index (`lower(lead_email)` 0040, `lead_id`, `(org_id, domain)` 0116); an empty
 * list adds no branch at all.
 */
function sequencesCte(filters: UnitsFilters): SQL {
  const person: SQL[] = [];
  if (filters.emails.length > 0) person.push(sql`lower(c.lead_email) = ANY(${textArray(filters.emails)})`);
  if (filters.leadIds.length > 0) person.push(sql`c.lead_id = ANY(${textArray(filters.leadIds)})`);
  if (filters.domains.length > 0) {
    person.push(sql`split_part(lower(c.lead_email), '@', 2) = ANY(${textArray(filters.domains)})`);
  }
  if (person.length === 0) {
    // parseUnitsQuery refuses it; a direct caller must never list a whole org.
    throw new UnitsQueryError("person_filter_required", "at least one of emails, leadIds, domains is required");
  }
  const conditions: SQL[] = [
    sql`c.org_id = ${filters.orgId}`,
    sql`c.lead_email IS NOT NULL`,
    sql`(${sql.join(person, sql` OR `)})`,
  ];
  if (filters.brandId !== undefined) conditions.push(sql`${filters.brandId} = ANY(c.brand_ids)`);
  if (filters.campaignId !== undefined) conditions.push(sql`c.campaign_id = ${filters.campaignId}`);
  return sql`
    seq AS MATERIALIZED (
      SELECT c.instantly_campaign_id,
             c.campaign_id,
             c.lead_email,
             c.lead_id,
             c.follow_up,
             to_jsonb(c.brand_ids) AS brand_ids,
             COALESCE(c.metadata ? 'followUpOf', false) AS continues_thread
      FROM instantly_campaigns c
      WHERE ${sql.join(conditions, sql` AND `)}
    )`;
}

/** Window, kinds and keyset over the union `u` (columns `at`, `unit_id`, `kind`). */
function unitConditions(filters: UnitsFilters, extra: SQL[] = []): SQL {
  const conditions: SQL[] = [sql`TRUE`, ...extra];
  if (filters.since !== undefined) {
    conditions.push(sql`u.at >= (${filters.since}::timestamptz AT TIME ZONE 'UTC')`);
  }
  if (filters.until !== undefined) {
    conditions.push(sql`u.at < (${filters.until}::timestamptz AT TIME ZONE 'UTC')`);
  }
  if (filters.kinds !== undefined) conditions.push(sql`u.kind = ANY(${textArray(filters.kinds)})`);
  if (filters.cursor !== undefined) {
    const after = decodeUnitCursor(filters.cursor);
    conditions.push(sql`(u.at, u.unit_id) < (${after.at}::timestamp, ${after.unitId})`);
  }
  return sql.join(conditions, sql` AND `);
}

const SEQUENCE_COLUMNS = sql.raw(`
             lower(seq.lead_email)      AS email,
             seq.lead_id                AS lead_id,
             seq.continues_thread       AS continues_thread,
             seq.campaign_id            AS campaign_id,
             seq.brand_ids              AS brand_ids,
             seq.instantly_campaign_id  AS sequence_id`);

const OUTPUT_COLUMNS = sql.raw(`
           u.unit_id                                         AS "unitId",
           to_char(u.at, 'YYYY-MM-DD"T"HH24:MI:SS.US')       AS "atText",
           u.step                                            AS "step",
           u.kind                                            AS "kind",
           u.email                                           AS "email",
           u.lead_id                                         AS "leadId",
           u.continues_thread                                AS "continuesThread",
           u.campaign_id                                     AS "campaignId",
           u.brand_ids                                       AS "brandIds",
           u.sequence_id                                     AS "sequenceId"`);

const stepKind = (stepColumn: string): SQL =>
  sql.raw(`CASE WHEN ${stepColumn} = 1 AND NOT seq.follow_up THEN 'first' ELSE 'follow_up' END`);

function pageOf<U extends { unitId: string }>(
  result: unknown,
  limit: number,
  map: (row: UnitRow) => U,
): UnitsPage<U> {
  const rows = rowsOf(result) as unknown as UnitRow[];
  const kept = rows.slice(0, limit);
  const units = kept.map(map);
  const last = kept[kept.length - 1];
  return {
    units,
    nextCursor:
      rows.length > limit && last !== undefined
        ? encodeUnitCursor({ at: String(last.atText), unitId: String(last.unitId) })
        : null,
  };
}

/** Every email of ours that really left for the person(s), newest first. */
export async function fetchEmailSentUnits(filters: UnitsFilters): Promise<UnitsPage<EmailSentUnit>> {
  const result = await db.execute(sql`
    WITH ${sequencesCte(filters)}
    SELECT ${OUTPUT_COLUMNS},
           u.bounced AS "bounced"
    FROM (
      SELECT ev.id                AS unit_id,
             ev."timestamp"       AS at,
             ev.step              AS step,
             ${stepKind("ev.step")} AS kind,
             EXISTS (
               SELECT 1 FROM instantly_events b
               WHERE b.campaign_id = ev.campaign_id
                 AND b.lead_email = ev.lead_email
                 AND b.step = ev.step
                 AND b.event_type = 'email_bounced'
             )                    AS bounced,
             ${SEQUENCE_COLUMNS}
      FROM seq
      JOIN instantly_events ev
        ON ev.campaign_id = seq.instantly_campaign_id
       AND ev.lead_email = seq.lead_email
       AND ev.event_type = 'email_sent'
       AND ev.withdrawn_at IS NULL
      UNION ALL
      SELECT 'answer:' || r.id::text AS unit_id,
             r.sent_at            AS at,
             NULL::integer        AS step,
             'answer'             AS kind,
             false                AS bounced,
             lower(r.lead_email)        AS email,
             seq.lead_id                AS lead_id,
             seq.continues_thread       AS continues_thread,
             seq.campaign_id            AS campaign_id,
             seq.brand_ids              AS brand_ids,
             seq.instantly_campaign_id  AS sequence_id
      FROM seq
      JOIN reply_send_outcomes r ON r.instantly_campaign_id = seq.instantly_campaign_id
    ) u
    WHERE ${unitConditions(filters)}
    ORDER BY u.at DESC, u.unit_id DESC
    LIMIT ${filters.limit + 1}
  `);
  return pageOf(result, filters.limit, toEmailSentUnit);
}

/** Every step we decided to send to the person(s), whatever became of it, newest first. */
export async function fetchEmailQueuedUnits(filters: UnitsFilters): Promise<UnitsPage<EmailQueuedUnit>> {
  const extra: SQL[] = [];
  if (filters.states !== undefined) {
    extra.push(sql`u.hold_status = ANY(${textArray(filters.states.map((s) => HOLD_STATUS_BY_STATE[s]))})`);
  }
  // The second branch below: holds written before migration 0027 carry no
  // sequence id, they are matched by the caller's campaign + address.
  const holdColumns = sql`
             sc.id                AS unit_id,
             sc.created_at        AS at,
             sc.step              AS step,
             ${stepKind("sc.step")} AS kind,
             sc.run_id            AS run_id,
             sc.status            AS hold_status,
             COALESCE(sc.queued_declaration = 'declared', false) AS declared_on_ledger,
             ${SEQUENCE_COLUMNS}`;
  const result = await db.execute(sql`
    WITH ${sequencesCte(filters)}
    SELECT ${OUTPUT_COLUMNS},
           u.hold_status        AS "holdStatus",
           u.declared_on_ledger AS "declaredOnLedger"
    FROM (
      -- One Unit per queued step: holds written before migration 0038 are TWO
      -- rows per step (account + domain cost) on the same run, folded here.
      SELECT DISTINCT ON (h.sequence_id, h.run_id, h.step) h.*
      FROM (
        SELECT ${holdColumns}
        FROM seq
        JOIN sequence_costs sc ON sc.instantly_campaign_id = seq.instantly_campaign_id
        UNION ALL
        SELECT ${holdColumns}
        FROM seq
        JOIN sequence_costs sc
          ON sc.instantly_campaign_id IS NULL
         AND sc.campaign_id = seq.campaign_id
         AND sc.lead_email = seq.lead_email
         AND NOT seq.follow_up
      ) h
      ORDER BY h.sequence_id, h.run_id, h.step, h.unit_id
    ) u
    WHERE ${unitConditions(filters, extra)}
    ORDER BY u.at DESC, u.unit_id DESC
    LIMIT ${filters.limit + 1}
  `);
  return pageOf(result, filters.limit, toEmailQueuedUnit);
}
