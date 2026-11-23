/**
 * GET-BEFORE-WRITE-1123: the Provider reads of the two outcomes this service owns,
 * `email_queued` and `email_sent` (`GET /orgs/emails/queued`, `GET /orgs/emails/sent`).
 *
 * A Script reads Units that EXIST with `ctx.get(outcome, filters)` (workflow-service
 * ENGINE-GET-ASK-1123): the call lands on the operation of our served OpenAPI tagged
 * `provider:<outcome>`, as the Script's run. A cold email Script asks "was this person
 * already emailed?" BEFORE it asks for a written email, so two campaigns never both
 * write to the same person.
 *
 *   - `email_queued`: one Unit per queue decision (one `instantly_campaigns` row: a first
 *     email, or one follow-up send). The row is written when `POST /orgs/send` RESERVES
 *     the send, before anything else happens, so a decision taken a second ago is read.
 *     `state` says what became of it: `queued` (decided, not sent yet), `sent`, `stopped`
 *     (failed, cancelled, paused or completed before any email left).
 *   - `email_sent`: one Unit per email that really left (a real `email_sent` silver
 *     event, never an inferred one), with its step.
 *
 * Reads only. Nothing is declared, reserved or consumed: reading a person's emails
 * changes nothing for the next reader.
 *
 * Scope is the caller's choice, stated in the filters (a tactic of its Script):
 *   - a person is REQUIRED (`email` and/or `leadId`): this is a read about one person;
 *   - `brandId` narrows to one brand; omitted = every brand of the caller's org;
 *   - `acrossOrgs` widens to every org (an inbox does not know which org wrote to it).
 *     A Unit of ANOTHER org carries no brand and no campaign: only that an email was
 *     queued or sent, to whom, and when.
 */
import { sql, type SQL } from "drizzle-orm";
import { db } from "../db";

export const PROVIDER_EMAILS_DEFAULT_LIMIT = 100;
export const PROVIDER_EMAILS_MAX_LIMIT = 500;

export interface ProviderEmailFilters {
  /** The caller's org (`x-org-id`). */
  orgId: string;
  /** The person's address, matched trimmed and case-insensitively. */
  email?: string;
  /** The person's lead-service lead id. */
  leadId?: string;
  brandId?: string;
  campaignId?: string;
  /** ISO instant, inclusive: Units queued / sent at or after it. */
  since?: string;
  /** ISO instant, exclusive: Units queued / sent before it. */
  before?: string;
  /** true = first emails only; false = follow-ups only; omitted = both. */
  firstEmail?: boolean;
  /** true = every org's emails to this person (their Units carry no brand, no campaign). */
  acrossOrgs?: boolean;
  /** `email_queued` only. true = also the decisions that never sent (`state: "stopped"`). */
  includeStopped?: boolean;
  limit?: number;
}

export type QueuedEmailState = "queued" | "sent" | "stopped";

export interface QueuedEmailUnit {
  id: string;
  outcome: "email_queued";
  email: string;
  leadId: string | null;
  queuedAt: string;
  firstEmail: boolean;
  state: QueuedEmailState;
  ownOrg: boolean;
  brandIds: string[] | null;
  campaignId: string | null;
}

export interface SentEmailUnit {
  id: string;
  outcome: "email_sent";
  email: string;
  leadId: string | null;
  sentAt: string;
  /** The step of its sequence (1 = the first email); null when the vendor named none. */
  step: number | null;
  firstEmail: boolean;
  ownOrg: boolean;
  brandIds: string[] | null;
  campaignId: string | null;
}

export class ProviderEmailFilterError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ProviderEmailFilterError";
  }
}

/** UTC ISO text of a `timestamp without time zone` column (stored UTC), built in SQL: never a driver-local Date. */
const iso = (column: SQL) => sql`to_char(${column}, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')`;

/**
 * What became of a queue decision. `delivery_status` starts `contacted` (pushed, nothing
 * sent) and moves on the first real send; `failed` / `cancelled` never sent. A row still
 * `contacted` whose `status` left `active` (paused, completed) is not going out.
 */
const QUEUED_STATE = sql`CASE
    WHEN c.delivery_status IN ('failed', 'cancelled') THEN 'stopped'
    WHEN c.delivery_status <> 'contacted' THEN 'sent'
    WHEN c.status = 'active' THEN 'queued'
    ELSE 'stopped' END`;

function instantOf(name: string, value: string): string {
  const ms = Date.parse(value);
  if (Number.isNaN(ms)) throw new ProviderEmailFilterError(`${name} is not a date: ${value}`);
  return new Date(ms).toISOString();
}

function limitOf(limit: number | undefined): number {
  if (limit === undefined) return PROVIDER_EMAILS_DEFAULT_LIMIT;
  if (!Number.isInteger(limit) || limit < 1 || limit > PROVIDER_EMAILS_MAX_LIMIT) {
    throw new ProviderEmailFilterError(`limit is 1-${PROVIDER_EMAILS_MAX_LIMIT}`);
  }
  return limit;
}

/** The person + scope conditions both reads share, on `instantly_campaigns c`. */
function scopeConditions(f: ProviderEmailFilters): SQL[] {
  const email = f.email?.trim().toLowerCase() ?? "";
  const leadId = f.leadId?.trim() ?? "";
  if (!email && !leadId) {
    throw new ProviderEmailFilterError("name the person: email and/or lead_id is required");
  }
  const where: SQL[] = [];
  // Both stated = either identifies the person (a lead whose address changed keeps its id).
  if (email && leadId) where.push(sql`(lower(c.lead_email) = ${email} OR c.lead_id = ${leadId})`);
  else if (email) where.push(sql`lower(c.lead_email) = ${email}`);
  else where.push(sql`c.lead_id = ${leadId}`);
  if (f.acrossOrgs !== true) where.push(sql`c.org_id = ${f.orgId}`);
  if (f.brandId) where.push(sql`c.brand_ids @> ARRAY[${f.brandId}]::text[]`);
  if (f.campaignId) where.push(sql`c.campaign_id = ${f.campaignId}`);
  if (f.firstEmail === true) where.push(sql`c.follow_up = false`);
  return where;
}

interface QueuedRow {
  id: string;
  email: string;
  leadId: string | null;
  queuedAt: string;
  followUp: boolean;
  state: QueuedEmailState;
  orgId: string | null;
  brandIds: string[] | null;
  campaignId: string | null;
}

/** Every queue decision about one person, newest first. */
export async function fetchQueuedEmails(f: ProviderEmailFilters): Promise<QueuedEmailUnit[]> {
  const where = scopeConditions(f);
  if (f.firstEmail === false) where.push(sql`c.follow_up = true`);
  if (f.since) where.push(sql`c.created_at >= ${instantOf("since", f.since)}::timestamptz AT TIME ZONE 'UTC'`);
  if (f.before) where.push(sql`c.created_at < ${instantOf("before", f.before)}::timestamptz AT TIME ZONE 'UTC'`);
  if (f.includeStopped !== true) where.push(sql`(${QUEUED_STATE}) <> 'stopped'`);
  const limit = limitOf(f.limit);

  const result = await db.execute(sql`
    SELECT c.id AS "id",
           lower(c.lead_email) AS "email",
           c.lead_id AS "leadId",
           ${iso(sql`c.created_at`)} AS "queuedAt",
           c.follow_up AS "followUp",
           (${QUEUED_STATE}) AS "state",
           c.org_id AS "orgId",
           c.brand_ids AS "brandIds",
           c.campaign_id AS "campaignId"
    FROM instantly_campaigns c
    WHERE ${sql.join(where, sql` AND `)}
    ORDER BY c.created_at DESC, c.id DESC
    LIMIT ${limit}
  `);
  return (result.rows as unknown as QueuedRow[]).map((r) => {
    const ownOrg = r.orgId === f.orgId;
    return {
      id: r.id,
      outcome: "email_queued" as const,
      email: r.email,
      leadId: r.leadId,
      queuedAt: r.queuedAt,
      firstEmail: r.followUp !== true,
      state: r.state,
      ownOrg,
      brandIds: ownOrg ? r.brandIds : null,
      campaignId: ownOrg ? r.campaignId : null,
    };
  });
}

interface SentRow {
  id: string;
  email: string;
  leadId: string | null;
  sentAt: string;
  step: number | null;
  followUp: boolean;
  orgId: string | null;
  brandIds: string[] | null;
  campaignId: string | null;
}

/** Every email that really left for one person, newest first. */
export async function fetchSentEmails(f: ProviderEmailFilters): Promise<SentEmailUnit[]> {
  const where = scopeConditions(f);
  // A first email = step 1 of a send that is not a follow-up. A send whose vendor named no step is read by its row alone.
  if (f.firstEmail === true) where.push(sql`COALESCE(NULLIF(e.step, 0), 1) = 1`);
  if (f.firstEmail === false) where.push(sql`(c.follow_up = true OR COALESCE(NULLIF(e.step, 0), 1) > 1)`);
  if (f.since) where.push(sql`e.timestamp >= ${instantOf("since", f.since)}::timestamptz AT TIME ZONE 'UTC'`);
  if (f.before) where.push(sql`e.timestamp < ${instantOf("before", f.before)}::timestamptz AT TIME ZONE 'UTC'`);
  const limit = limitOf(f.limit);

  const result = await db.execute(sql`
    SELECT e.id AS "id",
           lower(c.lead_email) AS "email",
           c.lead_id AS "leadId",
           ${iso(sql`e.timestamp`)} AS "sentAt",
           NULLIF(e.step, 0) AS "step",
           c.follow_up AS "followUp",
           c.org_id AS "orgId",
           c.brand_ids AS "brandIds",
           c.campaign_id AS "campaignId"
    FROM instantly_campaigns c
    JOIN instantly_events e
      ON e.campaign_id = c.instantly_campaign_id
     AND e.event_type = 'email_sent'
     AND e.inferred = false
    WHERE ${sql.join(where, sql` AND `)}
    ORDER BY e.timestamp DESC, e.id DESC
    LIMIT ${limit}
  `);
  return (result.rows as unknown as SentRow[]).map((r) => {
    const ownOrg = r.orgId === f.orgId;
    return {
      id: r.id,
      outcome: "email_sent" as const,
      email: r.email,
      leadId: r.leadId,
      sentAt: r.sentAt,
      step: r.step,
      firstEmail: r.followUp !== true && (r.step === null || r.step === 1),
      ownOrg,
      brandIds: ownOrg ? r.brandIds : null,
      campaignId: ownOrg ? r.campaignId : null,
    };
  });
}
