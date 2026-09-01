/**
 * Reply kind — what KIND of reply arrived, and nothing else.
 *
 * This vocabulary answers exactly one question: what did the prospect send
 * back? It deliberately says NOTHING about how far the deal got. A booked
 * meeting and a closed-won deal are funnel outcomes about a LEAD — they are
 * channel-agnostic (a meeting booked off an ads campaign is the same fact) and
 * they belong to the service that owns lead outcomes, which features-service
 * already reads for funnel counts. This service owns the reply, full stop.
 *
 * Why the split matters, concretely: both facts used to live in ONE statement
 * per lead, and only the latest statement survived the gold projection. So a
 * lead who replied positively and THEN had a meeting booked read as having no
 * reply sentiment at all — stating one fact destroyed the other. With deal
 * progress out of this vocabulary the two axes can no longer overwrite each
 * other, by construction rather than by ordering luck.
 *
 * The positive case carries four distinctions, because "positive" alone cannot
 * separate "interested but not the buyer" from "wants to book a call" — which
 * is the distinction the people reading these replies act on.
 *
 * LEGACY WRITES: the two deal-progress values are still ACCEPTED on the write
 * path, because the staff console is still sending them today. They are
 * RESOLVED to a reply kind at WRITE time (`resolveReplyKind`), never at read
 * time — bronze keeps the raw human intent, silver and every gold reader see
 * the new vocabulary only. Removing them from the write path is a separate,
 * later change, once both dashboards ship their new pickers.
 */

/**
 * A positive reply, split by what the prospect actually asked for.
 *
 *  - `lead_interested`        — personally interested.
 *  - `lead_referral`          — not personally interested, but relevant: they
 *                               are not the buyer and point us at who is.
 *  - `lead_info_requested`    — wants to know more; asks a question about the
 *                               offer without committing.
 *  - `lead_meeting_requested` — wants to book: proposes or accepts a time, or
 *                               sends a booking link.
 *
 * `lead_meeting_requested` is a REPLY fact ("they asked for a call"), NOT the
 * deal fact `lead_meeting_booked` ("a meeting exists on a calendar"). The
 * second one is owned elsewhere and is not in this vocabulary.
 */
export const POSITIVE_REPLY_KINDS = [
  "lead_interested",
  "lead_referral",
  "lead_info_requested",
  "lead_meeting_requested",
] as const;

/**
 * A negative reply, split by whether the "no" is about the MOMENT or about the
 * PERSON — the distinction the sales canon draws between a lead that is
 * recycled and one that is disqualified.
 *
 *  - `lead_not_interested` — they decline today. A commercial judgement about
 *                            this offer at this moment; the person is still
 *                            reachable and the lead stays recyclable.
 *  - `lead_wrong_person`   — they are not the right contact and hand nothing
 *                            back. The mirror of `lead_referral`: no name, no
 *                            relevance, nowhere to go.
 *  - `lead_changed_job`    — they have LEFT the role we were selling to. Like
 *                            `lead_wrong_person` this is an objective fact
 *                            about the person rather than an opinion about the
 *                            offer, and it is permanent for this lead: no
 *                            follow-up and no later re-approach can reach the
 *                            role at this company through them.
 *
 * The last two are DISQUALIFYING (permanent, about the person); the first is
 * not (temporary, about the moment). Collapsing a job change into
 * `lead_not_interested` is exactly the conflation that turns a "no" bucket
 * into a dumping ground and quietly loses recyclable pipeline, so the two are
 * kept apart. `lead_changed_job` is also NOT `lead_wrong_person`: that one
 * says we picked the wrong contact, which a person who once held the role
 * would read back as false.
 */
export const NEGATIVE_REPLY_KINDS = [
  "lead_not_interested",
  "lead_wrong_person",
  "lead_changed_job",
] as const;


/**
 * A real human reply that commits to nothing either way. Kept as its own kind
 * rather than folded into negative: it is the single most-used value on the
 * live console (39 of 84 statements in prod), and calling those leads negative
 * would assert something nobody stated.
 */
export const NEUTRAL_REPLY_KINDS = ["lead_neutral"] as const;

/**
 * Not a reply from a person at all — a machine answered. Deliberately never
 * stops the sequence: the prospect is back at their desk next week and has not
 * engaged (RFC 3834).
 */
export const AUTOMATED_REPLY_KINDS = ["lead_out_of_office", "auto_reply_received"] as const;

/** The complete reply-kind vocabulary. A lead with none of these has no reply. */
export const REPLY_KINDS = [
  ...POSITIVE_REPLY_KINDS,
  ...NEGATIVE_REPLY_KINDS,
  ...NEUTRAL_REPLY_KINDS,
  ...AUTOMATED_REPLY_KINDS,
] as const;

export type ReplyKind = (typeof REPLY_KINDS)[number];
export type PositiveReplyKind = (typeof POSITIVE_REPLY_KINDS)[number];

const REPLY_KIND_SET = new Set<string>(REPLY_KINDS);

/** True iff `value` is a value of the reply-kind vocabulary. */
export function isReplyKind(value: unknown): value is ReplyKind {
  return typeof value === "string" && REPLY_KIND_SET.has(value);
}

/**
 * Deal-progress values that are NO LONGER part of this service's vocabulary,
 * and what each resolves to.
 *
 * The domain fact that licenses the resolution: a person whose reply was
 * qualified as a booked meeting, a completed meeting, or a closed-won deal had
 * by definition replied positively. All three resolve to `lead_interested` —
 * the plain positive — and NOT to `lead_meeting_requested`, deliberately:
 * re-encoding "they wanted to book" out of a calendar fact would drag the deal
 * axis straight back into the reply axis, which is the exact mixing this
 * vocabulary exists to end. `lead_interested` is the strongest claim the
 * domain fact supports on its own.
 */
export const DEAL_PROGRESS_TO_REPLY_KIND: Record<string, ReplyKind> = {
  lead_meeting_booked: "lead_interested",
  lead_closed: "lead_interested",
  lead_meeting_completed: "lead_interested",
};

/**
 * The deal-progress values still accepted on the manual-qualification write
 * path. `lead_meeting_completed` is absent — it was never a hand-set value,
 * only an Instantly-poll event type (`lt_interest_status = 3`).
 */
export const LEGACY_DEAL_PROGRESS_STATUSES = ["lead_meeting_booked", "lead_closed"] as const;

export type LegacyDealProgressStatus = (typeof LEGACY_DEAL_PROGRESS_STATUSES)[number];

/** Everything the write path accepts today: the vocabulary + the two legacy values. */
export const ACCEPTED_QUALIFICATION_STATUSES = [
  ...REPLY_KINDS,
  ...LEGACY_DEAL_PROGRESS_STATUSES,
] as const;

export type AcceptedQualificationStatus = (typeof ACCEPTED_QUALIFICATION_STATUSES)[number];

/**
 * Resolve any accepted statement to a reply kind. Identity for the vocabulary,
 * the documented resolution for a deal-progress value.
 *
 * Throws on anything else — a statement we cannot record is a statement we
 * must not record silently under a fabricated kind.
 */
export function resolveReplyKind(status: string): ReplyKind {
  if (isReplyKind(status)) return status;
  const resolved = DEAL_PROGRESS_TO_REPLY_KIND[status];
  if (resolved) return resolved;
  throw new Error(`Unknown reply qualification status '${status}' — cannot resolve a reply kind`);
}

/**
 * True iff this event type is deal progress that this service no longer
 * records as such, so ingestion must resolve it to a reply kind first.
 */
export function isDealProgressEventType(eventType: string): boolean {
  return eventType in DEAL_PROGRESS_TO_REPLY_KIND;
}

/**
 * The coarse projection kept for the shared `email-domain-contract`
 * (`positive | negative | neutral`). An automated reply keeps projecting to
 * `neutral` there — widening the contract's enum is a cross-repo change, and
 * `repliesAutoReply` already separates it for every consumer that cares.
 *
 * ⚠️ `lead_referral` projects to `neutral`, NOT `positive`, and that is the one
 * place where this map deliberately DIVERGES from `POSITIVE_REPLY_KINDS`.
 * The two answer different questions:
 *
 *  - `POSITIVE_REPLY_KINDS` decides what is worth READING — it drives the
 *    forward to the agency inbox. A referral is very much worth reading.
 *  - this map decides what is REPORTED as the customer's sales interest — it
 *    becomes the brand's positive-reply count, its cost-per-positive-reply and
 *    the learning-threshold gate downstream (email-gateway → lead-service →
 *    features-service).
 *
 * "Not me, but talk to X" is valuable and it is NOT this person's buying
 * interest, which is exactly the distinction the metric exists to make. Pricing
 * a referral as a buying signal made a customer dashboard read "1 sales
 * interest" for a lead its own leads board correctly showed as merely contacted
 * (prod, 2026-08-29). The downstream consumer already excludes `lead_referral`
 * from its interest set; this map was the straggler.
 *
 * Do NOT "restore the lockstep" between this map's positive entries and
 * `POSITIVE_REPLY_KINDS` — the divergence is the fix.
 */
export const REPLY_KIND_CLASSIFICATION: Record<ReplyKind, "positive" | "negative" | "neutral"> = {
  lead_interested: "positive",
  lead_referral: "neutral",
  lead_info_requested: "positive",
  lead_meeting_requested: "positive",
  lead_not_interested: "negative",
  lead_wrong_person: "negative",
  lead_changed_job: "negative",
  lead_neutral: "neutral",
  lead_out_of_office: "neutral",
  auto_reply_received: "neutral",
};

/**
 * The reply kinds that assert the prospect actually ENGAGED — i.e. the
 * sequence must stop and the remaining holds must be cancelled.
 *
 * The automated kinds are excluded: an autoresponder is not a reply, so
 * stopping on one would end the outreach — and refund the spend — for a lead
 * who never answered. Same reasoning as `auto_reply_received` being absent
 * from `SEQUENCE_STOP_EVENTS`.
 */
export const SEQUENCE_STOPPING_REPLY_KINDS = new Set<ReplyKind>([
  ...POSITIVE_REPLY_KINDS,
  ...NEGATIVE_REPLY_KINDS,
  ...NEUTRAL_REPLY_KINDS,
]);

/** True iff a reply of this kind means the sequence must stop. */
export function isSequenceStoppingReplyKind(kind: ReplyKind): boolean {
  return SEQUENCE_STOPPING_REPLY_KINDS.has(kind);
}

/**
 * The negative kinds that are permanent facts about the PERSON rather than a
 * judgement about the offer — i.e. the ones that DISQUALIFY the lead.
 *
 * `lead_not_interested` is deliberately absent, and that absence is the whole
 * point of the set. "Not interested" / "can't buy right now" is a commercial
 * judgement about this offer at this moment: the person is still reachable, the
 * company is still in the audience, and the lead stays recyclable. Filing it as
 * a disqualification is exactly the conflation that turns a "no" bucket into a
 * dumping ground and quietly loses recyclable pipeline.
 *
 * `lead_wrong_person` and `lead_changed_job` are the opposite: each states an
 * objective fact about this contact that no later re-approach can change, so
 * the lead is permanently out.
 *
 * NOTE this is a strictly FINER reading of the same statements — it does NOT
 * change what `REPLY_KIND_CLASSIFICATION` says. All three kinds stay `negative`
 * for every consumer that reads only the coarse classification.
 */
export const DISQUALIFYING_REPLY_KINDS = new Set<ReplyKind>([
  "lead_wrong_person",
  "lead_changed_job",
]);

/**
 * True iff a reply of this kind permanently disqualifies the lead — a fact
 * about the person, not about the moment. False for every other kind,
 * `lead_not_interested` very much included.
 */
export function isDisqualifyingReplyKind(kind: ReplyKind): boolean {
  return DISQUALIFYING_REPLY_KINDS.has(kind);
}
