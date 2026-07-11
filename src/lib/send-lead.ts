/**
 * Shared send helper.
 *
 * Encapsulates "find a healthy Instantly account + create campaign + add
 * lead + activate". Used by:
 *   - POST /send (first-time send)
 *   - retry-stuck heartbeat (re-send when an existing row has stayed in
 *     `delivery_status='contacted'` past STUCK_AGE_HOURS without any silver
 *     proof Instantly actually sent — see lib/retry-stuck.ts)
 *
 * One-shot: picks a healthy account via the capacity-aware policy (fill the
 * account that can absorb one more email soonest under its daily limit — see
 * pickCapacityAwareAccount), creates a fresh Instantly campaign, adds the lead,
 * activates. Returns
 * success regardless of post-activate `not_sending_status` (NSS is pacing
 * diagnostic, not error signal — retry-stuck owns the eventual catch-up
 * 72h later if the campaign never sends).
 */

import linkifyHtml from "linkify-html";
import {
  createCampaign as createInstantlyCampaign,
  updateCampaign as updateInstantlyCampaign,
  getCampaign as getInstantlyCampaign,
  addLeads as addInstantlyLeads,
  updateCampaignStatus,
  type Account,
  type Lead,
  type SequenceStep,
} from "./instantly-client";
import { fetchInProductionAccounts } from "./account-lifecycle-sync";
import {
  fetchAccountCapacityCached,
  type AccountCapacity,
} from "./account-sending-stats";
import { IN_PRODUCTION_DAILY_LIMIT } from "./account-lifecycle";

/** All-zero capacity for an account absent from the snapshot (idle ⇒ preferred). */
const EMPTY_CAPACITY: AccountCapacity = {
  sentToday: 0,
  q0first: 0,
  q0next: 0,
  q1next: 0,
  totalQueue: 0,
};

/**
 * argMIN over `accounts` by `metric`, ties broken by a uniform random pick among
 * ONLY the tied minimum set — so a burst of concurrent sends spreads across
 * equally-preferred accounts rather than always landing on the first one, and a
 * heavier account is NEVER chosen even at the random boundary.
 */
function argMinRandom(
  accounts: Account[],
  metric: (a: Account) => number,
): Account {
  let min = Infinity;
  for (const a of accounts) {
    const m = metric(a);
    if (m < min) min = m;
  }
  const tied = accounts.filter((a) => metric(a) === min);
  return tied[Math.floor(Math.random() * tied.length)];
}

/**
 * Capacity-aware account selection — fill the account that can absorb one more
 * email SOONEST under its own daily limit (MDL), not merely the globally
 * least-loaded one.
 *
 * Per account, from the send-selection snapshot (see fetchAccountCapacity):
 *   MDL         = account.daily_limit (fallback IN_PRODUCTION_DAILY_LIMIT=50)
 *   S0          = sentToday (real dispatches today)
 *   Q0-first    = never-contacted sequences (1 first-email each ≈ today)
 *   Q0-next     = followup steps projected today/overdue
 *   Q1-next     = followup steps projected tomorrow
 *   todayOcc    = S0 + Q0-first + Q0-next          (projected volume today)
 *   tomorrowOcc = max(todayOcc − MDL, 0) + Q1-next (today's overflow O1 + tomorrow's due)
 *
 * Policy (first tier with a candidate wins; argMIN + random tie-break within):
 *   1. Accounts with todayOcc < MDL       → pick argMIN todayOcc (fill emptiest today).
 *   2. Else accounts with tomorrowOcc < MDL → pick argMIN tomorrowOcc (soonest room tomorrow).
 *   3. Else                                → pick argMIN totalQueue (globally emptiest queue).
 *
 * An account absent from `byEmail` is all-zeros ⇒ maximally preferred. Correctness
 * of the today/tomorrow buckets depends on the sending account being persisted on
 * the campaign row at send time (see account-sending-stats.ts) so a just-contacted
 * lead counts against its account immediately, not after the lagging first
 * email_sent webhook.
 */
export function pickCapacityAwareAccount(
  accounts: Account[],
  byEmail: Map<string, AccountCapacity>,
): Account {
  if (accounts.length === 0) {
    throw new Error("No accounts available");
  }

  const capOf = (a: Account): AccountCapacity =>
    byEmail.get(a.email) ?? EMPTY_CAPACITY;
  const mdlOf = (a: Account): number => a.daily_limit ?? IN_PRODUCTION_DAILY_LIMIT;
  const todayOcc = (a: Account): number => {
    const c = capOf(a);
    return c.sentToday + c.q0first + c.q0next;
  };
  const tomorrowOcc = (a: Account): number =>
    Math.max(todayOcc(a) - mdlOf(a), 0) + capOf(a).q1next;

  const roomToday = accounts.filter((a) => todayOcc(a) < mdlOf(a));
  if (roomToday.length > 0) return argMinRandom(roomToday, todayOcc);

  const roomTomorrow = accounts.filter((a) => tomorrowOcc(a) < mdlOf(a));
  if (roomTomorrow.length > 0) return argMinRandom(roomTomorrow, tomorrowOcc);

  return argMinRandom(accounts, (a) => capOf(a).totalQueue);
}

/**
 * Wrap plain-text URLs and bare domains in `<a href>` so Instantly's link
 * tracker rewrites them into redirect URLs (without anchors, Instantly leaves
 * the URL untouched and no `email_link_clicked` webhook ever fires).
 *
 * Mustache placeholders (`{{firstName}}`, `{{user.email}}`) are masked before
 * linkify to avoid wrapping things like `user.email` that look domain-like.
 */
export function autolinkifyHtml(html: string): string {
  const placeholders: string[] = [];
  const masked = html.replace(/\{\{[^}]*\}\}/g, (m) => {
    placeholders.push(m);
    return `XXLINKMUSTACHE${placeholders.length - 1}XX`;
  });
  const linkified = linkifyHtml(masked, { defaultProtocol: "https" });
  return linkified.replace(/XXLINKMUSTACHE(\d+)XX/g, (_, i) => placeholders[Number(i)]);
}

/**
 * Fixed brand line, identical for every sender. The PERSON line above it is
 * derived per-account (see `buildDefaultSignature`).
 *
 * Plain text, no `<a>` link — `buildEmailBodyWithSignature` autolinkifies only
 * the prospect body, never the signature.
 */
const SIGNATURE_BRAND_LINE = "Distribute.you | Marketing Agency";

/** Fallback sender name when an account carries no first/last name. */
const DEFAULT_SENDER_NAME = "Kevin Lourd";

/**
 * Per-account signature (HTML-formatted):
 *
 *   {account first + last name}
 *   Distribute.you | Marketing Agency
 *
 * The PERSON line is the account's own name so the From-name and the signature
 * agree (multi-persona sending: amy@… signs "Amy Moore", not a fixed name). NO
 * title — a fixed "Founder" can't apply across many distinct sender personas.
 * Falls back to `DEFAULT_SENDER_NAME` when the account has no name.
 *
 * Wrapped in `<p>...<br>...</p>` because Instantly's HTML sanitizer aggressively
 * strips plain text and bare `--` outside element wrappers on PATCH round-trip
 * (only tag-wrapped content survives). Historic damage 2026-05-28: a plain-text
 * signature was reduced to a stray `<a>distribute.you</a>` anchor on every PATCH.
 */
export function buildDefaultSignature(account: Account): string {
  const name =
    [account.first_name, account.last_name].filter(Boolean).join(" ").trim() ||
    DEFAULT_SENDER_NAME;
  return `<p>${name}<br>${SIGNATURE_BRAND_LINE}</p>`;
}

/**
 * HTML signature separator. RFC 3676 plain text uses `\n\n--\n`, but Instantly's
 * HTML sanitizer normalizes that to nothing on PATCH round-trip — the `<p>--</p>`
 * form survives and renders as the expected `--` delimiter line in mail clients.
 * Matched in stripAccountSignature via the `<p>--</p>` regex in SIG_MARKERS.
 */
const SIG_SEPARATOR_HTML = "<p>--</p>";

/**
 * Inject the selected account's signature into the email body.
 *
 * `{{accountSignature}}` only resolves in the Instantly UI — campaigns created
 * via the API send it as literal text. Instead we splice the signature directly.
 *
 * Signature resolution priority:
 *   1. `account.signature` — per-sender override configured in Instantly's UI
 *      (intentionally empty in prod for every sender).
 *   2. `buildDefaultSignature(account)` — per-account signature derived from the
 *      sending domain. Source of truth in prod.
 *
 * Idempotent (`f(f(x)) === f(x)`): always strips any pre-existing signature
 * block via `stripAccountSignature` BEFORE appending. Guarantees a body re-sent
 * N times never accumulates N stacked signatures (historic bug 2026-05-28 —
 * see `stripAccountSignature` docstring).
 *
 * Autolinkify is applied to the PROSPECT BODY ONLY. The signature block is our
 * own controlled HTML and is appended verbatim — its brand domain must render
 * as plain text, NOT a clickable `<a>` link.
 */
export function buildEmailBodyWithSignature(body: string, account: Account): string {
  const accountSig = account.signature?.trim() || "";
  const signature = accountSig || buildDefaultSignature(account);
  const stripped = stripAccountSignature(body);

  const linkedBody = autolinkifyHtml(stripped);
  const sigBlock = `${SIG_SEPARATOR_HTML}${signature}`;

  return linkedBody.includes("{{accountSignature}}")
    ? linkedBody.replace("{{accountSignature}}", sigBlock)
    : `${linkedBody}${sigBlock}`;
}

/**
 * Markers that announce a signature block. Each matches a standalone `--`
 * line in one of the common email/HTML forms:
 *   - `\n\n--\n` plain text (RFC 3676 sig delimiter, with optional trailing space)
 *   - `<p>--</p>` paragraph-wrapped
 *   - `<br>--<br>` line-break-wrapped
 *   - `<div>--</div>` div-wrapped
 * `&nbsp;` may appear adjacent to the `--` in HTML forms (HTML-rendered RFC 3676).
 */
const SIG_MARKERS: RegExp[] = [
  /\n\n--\s*\n/,
  /<p[^>]*>\s*--\s*(?:&nbsp;)?\s*<\/p>/i,
  /<br\s*\/?>\s*--\s*(?:&nbsp;)?\s*<br\s*\/?>/i,
  /<div[^>]*>\s*--\s*(?:&nbsp;)?\s*<\/div>/i,
];

/**
 * Strip the first signature block (and everything after) from a body. Used by
 * `buildEmailBodyWithSignature` to keep that function idempotent, and by
 * retry-stuck to recover the original prospect-facing body from an Instantly
 * campaign that already had account A's signature baked in.
 *
 * HTML-tolerant: matches plain `\n\n--\n` AND the HTML variants that Instantly
 * stores after a body has been round-tripped through a rich-text editor.
 *
 * Senders whose original body legitimately contains one of these markers will
 * lose content past that point on a re-send — accepted edge-case rather than
 * introducing a new `sequence_template` table just for this rare path.
 *
 * Historic bug 2026-05-28: the previous implementation matched only the plain
 * `\n\n--\n` marker. Bodies stored as HTML never matched, so every retry-stuck
 * re-send appended a fresh signature on top of the existing one. A row
 * redispatched 72 times shipped 72 stacked signatures. Fix: HTML-tolerant
 * markers + always strip before append (see `buildEmailBodyWithSignature`).
 */
export function stripAccountSignature(body: string): string {
  let earliest = -1;
  for (const re of SIG_MARKERS) {
    const m = re.exec(body);
    if (m && (earliest === -1 || m.index < earliest)) {
      earliest = m.index;
    }
  }
  if (earliest === -1) return body;
  return body.slice(0, earliest);
}

export interface SortedSequenceStep {
  step: number;
  bodyHtml: string;
  daysSinceLastStep: number;
}

/**
 * Build Instantly sequence steps from a normalized sequence array.
 * Injects the chosen account's signature into every step's bodyHtml.
 * All steps share the same subject (Instantly handles Re: threading on follow-ups).
 */
export function buildSequenceSteps(
  subject: string,
  sequence: SortedSequenceStep[],
  account: Account,
): SequenceStep[] {
  return [...sequence]
    .sort((a, b) => a.step - b.step)
    .map((s) => ({
      subject,
      bodyHtml: buildEmailBodyWithSignature(s.bodyHtml, account),
      daysSinceLastStep: s.daysSinceLastStep,
    }));
}

/**
 * Create an Instantly campaign, assign one account, add the lead, activate.
 * Returns the new Instantly campaign ID + the number of leads added.
 *
 * Post-activate `not_sending_status` is logged but never treated as an
 * error — it is pacing diagnostic (daily quota, sending schedule, etc.),
 * not a failure mode. retry-stuck handles the eventual catch-up if the
 * campaign never dispatches.
 */
export async function createAndActivateCampaign(
  apiKey: string,
  campaignName: string,
  account: Account,
  steps: SequenceStep[],
  lead: Lead,
  bcc?: string[],
  timezone?: string,
): Promise<{ instantlyCampaignId: string; added: number }> {
  console.log(
    `[send-lead] Creating Instantly campaign "${campaignName}" with account ${account.email}`,
  );
  const instantlyCampaign = await createInstantlyCampaign(apiKey, {
    name: campaignName,
    steps,
    timezone,
  });
  console.log(
    `[send-lead] Created instantly campaign id=${instantlyCampaign.id} status=${instantlyCampaign.status}`,
  );

  // Assign the selected account via PATCH. When BCC recipients are provided,
  // set the campaign-level `bcc_list` so every step of the sequence BCCs them
  // (the whole editorial team sees the same single thread + follow-ups).
  await updateInstantlyCampaign(apiKey, instantlyCampaign.id, {
    email_list: [account.email],
    ...(bcc && bcc.length > 0 ? { bcc_list: bcc } : {}),
    // Open tracking OFF: the open pixel is an invisible 1x1 tracking image —
    // a recognized bulk-mail spam signal — and Apple Mail Privacy Protection
    // pre-fetches it, so the "open" data is garbage anyway. Link tracking stays
    // ON (functional redirects via the custom tracking domain) for reliable
    // click data. See CLAUDE.md "Account selection" / deliverability notes.
    open_tracking: false,
    link_tracking: true,
    insert_unsubscribe_header: true,
    stop_on_reply: true,
  });

  // Verify accounts were assigned (diagnostic — Instantly occasionally drops
  // the email_list when accounts are concurrently being warmed up, so a sanity
  // read keeps the log trail rich for post-mortem).
  const verified = (await getInstantlyCampaign(
    apiKey,
    instantlyCampaign.id,
  )) as unknown as Record<string, unknown>;
  console.log(
    `[send-lead] Verify after PATCH — email_list=${JSON.stringify(verified.email_list)} not_sending_status=${JSON.stringify(verified.not_sending_status)}`,
  );

  // Add lead.
  const addResult = await addInstantlyLeads(apiKey, {
    campaign_id: instantlyCampaign.id,
    leads: [lead],
  });

  // Activate.
  await updateCampaignStatus(apiKey, instantlyCampaign.id, "active");

  return { instantlyCampaignId: instantlyCampaign.id, added: addResult.added };
}

export interface SendOptions {
  apiKey: string;
  campaignName: string;
  subject: string;
  sortedSequence: SortedSequenceStep[];
  lead: Lead;
  /** Optional BCC recipients — set as the campaign's `bcc_list` (every step). */
  bcc?: string[];
  /**
   * Optional IANA timezone of the recipient (lead). Sets the Instantly campaign
   * sending-schedule timezone so business-hours sends land in the prospect's
   * local time. Falls back to America/Chicago when absent.
   */
  timezone?: string;
}

export interface SendSuccess {
  instantlyCampaignId: string;
  added: number;
  account: Account;
}

export type SendFailureReason = "no_healthy_accounts_available";

export type SendResult =
  | { ok: true; value: SendSuccess }
  | { ok: false; reason: SendFailureReason };

/**
 * Find an eligible Instantly account and send the lead onto a fresh campaign.
 * One-shot — no retry on post-activate NSS (retry-stuck owns the eventual
 * catch-up).
 *
 * ELIGIBILITY = the account's silver `lifecycle_status = 'in_production'` (see
 * lib/account-lifecycle.ts). The pool is read PURELY from silver — no live
 * `listAccounts` on the send hot-path. An account reaches in_production only when
 * BOTH its Instantly health score == 100 AND its latest placement test is 100%
 * inbox across every ESP; the old under-warmed / blacklisted-domain / manual
 * gates are subsumed by that lifecycle.
 *
 * Returns:
 *   - `{ok: true, ...}` on success with the new Instantly campaign ID + chosen account.
 *   - `{ok: false, reason: "no_healthy_accounts_available"}` when zero accounts are
 *     currently in_production — caller surfaces this upstream (no row created).
 */
export async function sendLeadToInstantly(opts: SendOptions): Promise<SendResult> {
  const accounts = await fetchInProductionAccounts();

  if (accounts.length === 0) {
    console.warn(
      `[send-lead] No in_production accounts available for "${opts.campaignName}"`,
    );
    return { ok: false, reason: "no_healthy_accounts_available" };
  }

  const capacityByEmail = await fetchAccountCapacityCached();
  const account = pickCapacityAwareAccount(accounts, capacityByEmail);
  const steps = buildSequenceSteps(opts.subject, opts.sortedSequence, account);

  console.log(
    `[send-lead] Sending "${opts.campaignName}" with account ${account.email}`,
  );

  const result = await createAndActivateCampaign(
    opts.apiKey,
    opts.campaignName,
    account,
    steps,
    opts.lead,
    opts.bcc,
    opts.timezone,
  );

  return {
    ok: true,
    value: { ...result, account },
  };
}
