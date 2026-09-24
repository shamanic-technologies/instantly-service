/**
 * Recorded opt-outs — a person asked a HUMAN to stop contacting them.
 *
 * The clicked unsubscribe link is not how it usually happens. A prospect sends
 * an SMS, calls, replies to a thread somebody forwarded them, or says it in
 * person — and until now none of that could be recorded, so the only way into
 * the opted-out state was a link nobody had clicked.
 *
 * What this is NOT: an inference. Nothing here derives an opt-out from a MOOD or
 * from a SILENCE — not from a negative sentiment, not from a bounce, not from a
 * lead going quiet. A row exists because somebody STATED that a named person
 * asked to stop, through a named channel, and who said it and when stay
 * recoverable forever, because this is a consent record.
 *
 * ⚠️ "Somebody" is usually a staff member and is NOT always one. A prospect who
 * writes "please remove me from your list" has stated it themselves, in words,
 * and `lib/reply-opt-out` records that with `statedBy: "reply-classifier"` and
 * their sentence on the row. That is not the inference this paragraph bans — it
 * is the opposite end of the evidence scale: a sentence the person wrote is
 * stronger than a staff member's recollection of a phone call, which this module
 * accepts without argument. What stays banned is deriving consent from anything
 * they did NOT say.
 *
 * Three properties are load-bearing:
 *
 *  1. **It stops the sending, at the sender.** Recording an opt-out and then
 *     continuing to email is the legally dangerous outcome, so the record does
 *     everything a clicked unsubscribe does: a real `lead_unsubscribed` silver
 *     event through the shared `promoteEvent` (which stops the sequence and
 *     cancels the remaining holds) PLUS the pause at the third party, which the
 *     click path gets from Instantly itself and this path cannot — Instantly
 *     never saw the SMS.
 *  2. **It surfaces where a clicked unsubscribe surfaces.** Same event type,
 *     same gold column, so no consumer needs a second branch to notice it.
 *  3. **It is withdrawable.** A staff member can record one on the wrong
 *     person, and a prospect can come back. Withdrawing marks the silver events
 *     (never deletes them) and the read side stops reporting the opt-out.
 *
 * Scope is the PERSON, not a campaign: "stop contacting me" said once must not
 * be honoured in one campaign while another keeps sending.
 */
import { and, desc, eq, isNull, sql } from "drizzle-orm";

import { db } from "../db";
import {
  instantlyCampaigns,
  instantlyEvents,
  instantlyLeadOptoutWithdrawals,
  instantlyLeadOptoutsRaw,
} from "../db/schema";
import { promoteEvent } from "./silver-promote";
import { refreshLeadStatusCurrent } from "./status-gold";
import { announceEvidenceChanged } from "./evidence-changed";
import { resolveInstantlyApiKey } from "./key-client";
import { updateCampaignStatus } from "./instantly-client";
import { isSelfSendCampaignId } from "./self-send/transport";
import { stopSelfSendSequence } from "./self-send/stop-sequence";
import { normalizeLeadEmail } from "./recontact-window";

/**
 * How the person told us. Required on every record, and deliberately a closed
 * list: "they asked us to stop" with no channel is an assertion nobody can
 * audit later, which is the one thing a consent record must never be.
 */
export const OPT_OUT_CHANNELS = [
  "sms",
  "phone_call",
  "email_reply",
  "forwarded_thread",
  "in_person",
  "web_form",
  "other",
] as const;

export type OptOutChannel = (typeof OPT_OUT_CHANNELS)[number];

const OPT_OUT_CHANNEL_SET = new Set<string>(OPT_OUT_CHANNELS);

export function isOptOutChannel(value: unknown): value is OptOutChannel {
  return typeof value === "string" && OPT_OUT_CHANNEL_SET.has(value);
}

export interface LeadOptOutRow {
  id: string;
  orgId: string;
  email: string;
  channel: OptOutChannel;
  statedBy: string;
  notes: string | null;
  statedAt: Date;
  /** When it was taken back, or null while it still stands. */
  withdrawnAt: Date | null;
  withdrawnBy: string | null;
}

interface RawOptout {
  id: string;
  orgId: string;
  leadEmail: string;
  channel: string;
  statedBy: string;
  notes: string | null;
  statedAt: Date;
}

function toRow(
  raw: RawOptout,
  withdrawal?: { withdrawnAt: Date; withdrawnBy: string } | null,
): LeadOptOutRow {
  return {
    id: raw.id,
    orgId: raw.orgId,
    email: raw.leadEmail,
    channel: raw.channel as OptOutChannel,
    statedBy: raw.statedBy,
    notes: raw.notes,
    statedAt: raw.statedAt,
    withdrawnAt: withdrawal?.withdrawnAt ?? null,
    withdrawnBy: withdrawal?.withdrawnBy ?? null,
  };
}

/**
 * The STANDING opt-out for (org, lead) — the latest record that has not been
 * withdrawn, or null when nobody currently stands behind one.
 *
 * "Latest that is not withdrawn", not "latest": bronze is append-only, so a
 * withdrawn record is still the most recent ROW and reading the plain latest
 * would report an opt-out somebody explicitly took back.
 */
export async function findStandingOptOut(
  orgId: string,
  leadEmail: string,
): Promise<LeadOptOutRow | null> {
  const [row] = await db
    .select({ o: instantlyLeadOptoutsRaw })
    .from(instantlyLeadOptoutsRaw)
    .leftJoin(
      instantlyLeadOptoutWithdrawals,
      eq(instantlyLeadOptoutWithdrawals.optoutId, instantlyLeadOptoutsRaw.id),
    )
    .where(
      and(
        eq(instantlyLeadOptoutsRaw.orgId, orgId),
        // ⚠️ CASE-INSENSITIVE, the same normalization the re-contact window and
        // the serve path apply. `Joe@X.com` and `joe@x.com` are one inbox, and
        // an exact match let a standing opt-out go unseen under the other
        // casing — which fails in the direction that emails somebody who asked
        // us to stop. The row keeps the casing the person used; only the
        // comparison is normalized.
        sql`lower(${instantlyLeadOptoutsRaw.leadEmail}) = ${normalizeLeadEmail(leadEmail)}`,
        isNull(instantlyLeadOptoutWithdrawals.id),
      ),
    )
    .orderBy(desc(instantlyLeadOptoutsRaw.statedAt))
    .limit(1);
  return row ? toRow(row.o) : null;
}

/** Every campaign this org holds for this address. Reservation sentinels are
 *  excluded: an in-flight claim is not a campaign, and asking Instantly about
 *  one 400s on the uuid format. */
async function findOrgCampaignsForLead(orgId: string, leadEmail: string) {
  return db
    .select({
      instantlyCampaignId: instantlyCampaigns.instantlyCampaignId,
      campaignId: instantlyCampaigns.campaignId,
      orgId: instantlyCampaigns.orgId,
      userId: instantlyCampaigns.userId,
      runId: instantlyCampaigns.runId,
      status: instantlyCampaigns.status,
    })
    .from(instantlyCampaigns)
    .where(
      and(
        eq(instantlyCampaigns.orgId, orgId),
        // Same normalization as the standing lookup — an opt-out that stopped
        // only the campaigns whose stored casing happened to match would leave
        // the rest sending.
        sql`lower(${instantlyCampaigns.leadEmail}) = ${normalizeLeadEmail(leadEmail)}`,
        sql`${instantlyCampaigns.instantlyCampaignId} NOT LIKE 'reserving:%'`,
      ),
    );
}

/**
 * Stop one campaign at the SENDER.
 *
 * `promoteEvent` already stopped it on OUR side (delivery status, remaining
 * holds). This is the other half: on the self-send transport the dispatch
 * worker is the sender, so the local stop IS the stop; on the Instantly
 * transport Instantly is the sender and it never saw the SMS, so without the
 * pause it keeps dispatching the remaining steps to somebody who asked us to
 * stop.
 *
 * Fail-soft per campaign: the bronze consent record is already committed when
 * this runs, and one unreachable campaign must not lose the rest. Every failure
 * is logged loudly and reported back in the result.
 */
async function stopSendingForCampaign(
  campaign: {
    instantlyCampaignId: string;
    campaignId: string | null;
    orgId: string | null;
    userId: string | null;
    runId: string | null;
    status: string;
  },
  leadEmail: string,
  orgId: string,
): Promise<boolean> {
  try {
    if (isSelfSendCampaignId(campaign.instantlyCampaignId)) {
      await stopSelfSendSequence(campaign, leadEmail, "recorded opt-out");
      return true;
    }

    if (campaign.status !== "active") return true;

    const { key } = await resolveInstantlyApiKey(orgId, "system", {
      method: "POST",
      path: "/orgs/opt-outs",
    });
    await updateCampaignStatus(key, campaign.instantlyCampaignId, "paused");
    console.log(
      `[instantly-service] opt-out: paused campaign=${campaign.instantlyCampaignId} lead=${leadEmail}`,
    );
    return true;
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(
      `[instantly-service] opt-out: failed to stop campaign=${campaign.instantlyCampaignId} lead=${leadEmail} — ${message}; sequence may continue at the sender`,
    );
    return false;
  }
}

export interface RecordOptOutInput {
  orgId: string;
  leadEmail: string;
  channel: OptOutChannel;
  statedBy: string;
  notes?: string;
  payload: unknown;
}

export interface RecordOptOutResult {
  /** False when a standing opt-out already existed — no new row, no side effects. */
  recorded: boolean;
  optOut: LeadOptOutRow;
  /** Campaigns the opt-out was applied to, and how many could be stopped at the sender. */
  campaignsAffected: number;
  campaignsStopped: number;
}

/**
 * Record an opt-out and honour it everywhere this org could still email the
 * person.
 *
 * Idempotent: a standing record for the same person is returned untouched, with
 * no second bronze row and no repeated side effects.
 *
 * The bronze row is written even when the org holds NO campaign for the address
 * — refusing to record a consent statement because we have nothing to stop
 * would be the wrong direction to be wrong in. The result then reports zero
 * campaigns, which is the honest number.
 */
export async function recordLeadOptOut(
  input: RecordOptOutInput,
): Promise<RecordOptOutResult> {
  const standing = await findStandingOptOut(input.orgId, input.leadEmail);
  if (standing) {
    return { recorded: false, optOut: standing, campaignsAffected: 0, campaignsStopped: 0 };
  }

  const [inserted] = await db
    .insert(instantlyLeadOptoutsRaw)
    .values({
      orgId: input.orgId,
      leadEmail: input.leadEmail,
      channel: input.channel,
      statedBy: input.statedBy,
      notes: input.notes ?? null,
      payload: input.payload as Record<string, unknown>,
    })
    .returning();

  const campaigns = await findOrgCampaignsForLead(input.orgId, input.leadEmail);
  const statedAt = inserted.statedAt;
  let stopped = 0;

  for (const campaign of campaigns) {
    // The SAME event a clicked unsubscribe promotes, through the SAME path — so
    // the sequence stops, the remaining holds are cancelled and the gold status
    // row reports `unsubscribed` with no consumer needing a second branch.
    // `source_row_id` points at the consent record, which is what lets the
    // withdrawal find exactly these events later.
    await promoteEvent({
      eventType: "lead_unsubscribed",
      instantlyCampaignId: campaign.instantlyCampaignId,
      leadEmail: input.leadEmail,
      accountEmail: null,
      // The opt-out is about the whole relationship, not one step of it.
      step: null,
      variant: null,
      timestamp: statedAt,
      rawPayload: input.payload,
      source: "manual",
      sourceRowId: inserted.id,
      inferred: false,
    });

    if (await stopSendingForCampaign(campaign, input.leadEmail, input.orgId)) stopped += 1;
  }

  console.log(
    `[instantly-service] opt-out recorded: org=${input.orgId} lead=${input.leadEmail} channel=${input.channel} campaigns=${campaigns.length} stopped=${stopped}`,
  );

  // A person (or the reply classifier) just stated this — the Leads page must
  // reflect it on the next read, not after lead-service's 5-minute bound.
  await announceEvidenceChanged(input.orgId, [input.leadEmail], "optout_recorded");

  return {
    recorded: true,
    optOut: toRow(inserted),
    campaignsAffected: campaigns.length,
    campaignsStopped: stopped,
  };
}

export interface WithdrawOptOutInput {
  orgId: string;
  leadEmail: string;
  withdrawnBy: string;
  notes?: string;
}

export type WithdrawOptOutResult =
  | { withdrawn: true; optOut: LeadOptOutRow; campaignsAffected: number }
  | { withdrawn: false; reason: "no_standing_optout" };

/**
 * Take a recorded opt-out back.
 *
 * A correction, not an erasure: the record stays byte-identical and a
 * withdrawal row is appended beside it, so both what was stated and the fact
 * that it was retracted stay readable — which is the whole value of a consent
 * log. The silver events it promoted are MARKED withdrawn rather than deleted
 * (silver is the audit of what was asserted, and it is rebuildable), and the
 * gold read excludes a withdrawn `lead_unsubscribed`, so the status read stops
 * reporting the person as opted out.
 *
 * SCOPE, stated rather than papered over: this releases the OPT-OUT. It does
 * not resume the sequences the opt-out stopped — the holds were cancelled and
 * the campaigns paused, and silently restarting outreach at somebody who asked
 * us to stop is the one mistake worth being unable to make by accident. A new
 * send is a new decision, taken deliberately.
 *
 * Refuses with `no_standing_optout` when nothing stands — including a second
 * withdrawal of the same record, which writes nothing. Fail loud otherwise: a
 * withdrawal that cannot release the read side must not report success.
 */
export async function withdrawLeadOptOut(
  input: WithdrawOptOutInput,
): Promise<WithdrawOptOutResult> {
  const standing = await findStandingOptOut(input.orgId, input.leadEmail);
  if (!standing) return { withdrawn: false, reason: "no_standing_optout" };

  const [withdrawal] = await db
    .insert(instantlyLeadOptoutWithdrawals)
    .values({
      optoutId: standing.id,
      orgId: standing.orgId,
      leadEmail: standing.email,
      withdrawnBy: input.withdrawnBy,
      notes: input.notes ?? null,
    })
    .returning();

  // Only the events THIS record promoted, identified by the bronze row they
  // came from. A `lead_unsubscribed` the prospect produced by clicking the link
  // carries a different source and is never touched — nobody withdrew that.
  const marked = await db
    .update(instantlyEvents)
    .set({ withdrawnAt: withdrawal.withdrawnAt })
    .where(
      and(
        eq(instantlyEvents.sourceRowId, standing.id),
        eq(instantlyEvents.source, "manual"),
        eq(instantlyEvents.eventType, "lead_unsubscribed"),
        sql`lower(${instantlyEvents.leadEmail}) = ${normalizeLeadEmail(standing.email)}`,
        isNull(instantlyEvents.withdrawnAt),
      ),
    )
    .returning({ campaignId: instantlyEvents.campaignId });

  const campaignIds = new Set(marked.map((row) => row.campaignId).filter((id): id is string => !!id));
  for (const campaignId of campaignIds) {
    await refreshLeadStatusCurrent(campaignId, standing.email);
  }

  console.log(
    `[instantly-service] opt-out withdrawn: org=${input.orgId} lead=${standing.email} campaigns=${campaignIds.size}`,
  );

  await announceEvidenceChanged(standing.orgId, [standing.email], "optout_withdrawn");

  return {
    withdrawn: true,
    optOut: {
      ...standing,
      withdrawnAt: withdrawal.withdrawnAt,
      withdrawnBy: withdrawal.withdrawnBy,
    },
    campaignsAffected: campaignIds.size,
  };
}

export interface ListLeadOptOutsInput {
  orgId: string;
  leadEmail?: string;
  /** Return only records that still STAND. Default false — the audit is the point. */
  standingOnly?: boolean;
  /**
   * ⚠️ OMIT IT TO GET EVERYTHING. There is no default and no ceiling, and that
   * is deliberate: this list is read by a consent GATE, and a truncated page is
   * indistinguishable from a complete shorter one. A caller that cannot tell
   * them apart can only refuse — so a silent cap here turns into an outage over
   * there the day an org crosses it.
   */
  limit?: number;
}

/**
 * The org's opt-out log, newest first. Withdrawn records are returned too and
 * carry `withdrawnAt` / `withdrawnBy`: hiding them would destroy the audit, and
 * a consumer rendering one as a current opt-out is showing something nobody
 * stands behind.
 *
 * ⚠️ UNBOUNDED BY DEFAULT. human-service gates every serve on this read and
 * refuses to serve anyone when it cannot trust the set is complete — so the old
 * `?? 200` default with a 500 ceiling meant the first org to cross 500 standing
 * opt-outs would have stopped being served at all. Opt-outs never expire, so
 * that was a certainty rather than a risk. The set is small and monotonic (30
 * across 7 orgs on 2026-09-17); a caller who genuinely wants a page asks for one.
 */
export async function listLeadOptOuts(
  input: ListLeadOptOutsInput,
): Promise<LeadOptOutRow[]> {
  const conditions = [eq(instantlyLeadOptoutsRaw.orgId, input.orgId)];
  if (input.leadEmail) {
    conditions.push(
      sql`lower(${instantlyLeadOptoutsRaw.leadEmail}) = ${normalizeLeadEmail(input.leadEmail)}`,
    );
  }
  if (input.standingOnly) {
    conditions.push(isNull(instantlyLeadOptoutWithdrawals.id));
  }

  const query = db
    .select({
      o: instantlyLeadOptoutsRaw,
      w: {
        id: instantlyLeadOptoutWithdrawals.id,
        withdrawnAt: instantlyLeadOptoutWithdrawals.withdrawnAt,
        withdrawnBy: instantlyLeadOptoutWithdrawals.withdrawnBy,
      },
    })
    .from(instantlyLeadOptoutsRaw)
    .leftJoin(
      instantlyLeadOptoutWithdrawals,
      eq(instantlyLeadOptoutWithdrawals.optoutId, instantlyLeadOptoutsRaw.id),
    )
    .where(and(...conditions))
    .orderBy(desc(instantlyLeadOptoutsRaw.statedAt));

  // Applied ONLY when the caller asked for it. `?? 200` used to sit here, and a
  // consumer reading the whole consent log had no way to know it had been given
  // a slice — see the interface note.
  const rows = await (input.limit ? query.limit(input.limit) : query);

  return rows.map((row) =>
    toRow(
      row.o,
      row.w?.withdrawnAt ? { withdrawnAt: row.w.withdrawnAt, withdrawnBy: row.w.withdrawnBy! } : null,
    ),
  );
}


/**
 * Machine-readable refusal code when a send is refused for a standing opt-out.
 *
 * Distinguishable from this route's other refusals: the re-contact window
 * (`recent_brand_contact` — a timing rule that expires). A caller must be
 * able to tell "come back in three months" from "never contact this person
 * again".
 */
export const OPT_OUT_REFUSAL_CODE = "lead_opted_out";

/**
 * The refusal body for a send to somebody who asked us to stop.
 *
 * ⚠️ ORG-SCOPED, NOT BRAND-SCOPED, and that is stricter on purpose. They asked
 * US to stop; the org is the sender, and the consent record has always been
 * keyed on (org, person). Honouring it for one brand while another brand of the
 * same org keeps writing is exactly the outcome the law cares about.
 *
 * ⚠️ IT NEVER EXPIRES. The re-contact window beside it is a three-month timing
 * rule that lapses; this one does not, because nothing about the passage of
 * time withdraws a consent statement. Only an explicit withdrawal does.
 */
export function optOutRefusal(leadEmail: string, optOut: LeadOptOutRow) {
  return {
    error: "Lead has opted out",
    code: OPT_OUT_REFUSAL_CODE,
    details:
      `${leadEmail} asked to stop being contacted (recorded ${optOut.statedAt.toISOString()} ` +
      `via ${optOut.channel}). The opt-out is org-wide and does not expire. ` +
      `No email was sent and nothing was billed.`,
    channel: optOut.channel,
    statedAt: optOut.statedAt.toISOString(),
  };
}
