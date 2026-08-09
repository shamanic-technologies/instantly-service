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
 * One-shot: picks a healthy account via the sequential-fill policy (saturate the
 * first account of a fixed, creation-ordered queue before touching the next — see
 * pickSequentialFillAccount), creates a fresh Instantly campaign, adds the lead,
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
import { IN_PRODUCTION_DAILY_LIMIT, rampCapForAge } from "./account-lifecycle";

/** All-zero capacity for an account absent from the snapshot (idle ⇒ preferred). */
const EMPTY_CAPACITY: AccountCapacity = {
  sentToday: 0,
  q0first: 0,
  q0next: 0,
  q1next: 0,
  totalQueue: 0,
};

/**
 * Today's assignment cap for one account.
 *
 * The age ramp is computed off the LIFECYCLE BASE (IN_PRODUCTION_DAILY_LIMIT),
 * never off the account's live `daily_limit` — lifecycle-limits-sync writes that
 * same ramped value onto Instantly, so scaling the already-scaled value would
 * compound (45 → 23 → 12 → …). Taking the MIN keeps both enforcement points
 * idempotent while still honouring a lower operator-set limit.
 *
 * A mature (or undatable) account keeps its full `daily_limit`; a fresh one is
 * capped by `rampCapForAge` — a young Google mailbox's real Gmail per-user quota
 * is far below 45 for its first weeks, independent of inbox placement.
 */
function capForAccount(a: Account, asOf: Date): number {
  return Math.min(
    a.daily_limit ?? IN_PRODUCTION_DAILY_LIMIT,
    rampCapForAge(a.timestamp_created, IN_PRODUCTION_DAILY_LIMIT, asOf),
  );
}

/** Today's committed load for one account: dispatched + queued-for-today. */
function loadForAccount(
  a: Account,
  byEmail: Map<string, AccountCapacity>,
): number {
  const c = byEmail.get(a.email) ?? EMPTY_CAPACITY;
  return c.sentToday + c.q0first + c.q0next;
}

/**
 * The fleet's fixed fill ORDER: oldest account first, `email` ascending as the
 * tie-break, accounts with no `timestamp_created` last.
 *
 * Stable by construction — an account created later can never move ahead of an
 * older one, so adding mailboxes appends to the tail and never reshuffles the
 * head. That stability is the whole point: the accounts at the tail are the ones
 * that dry up first and become safe to cancel.
 *
 * An undatable account sorts last rather than first: we cannot honestly place it
 * in the sequence, and the tail is the position that risks the least (it only
 * receives volume once everything datable is full).
 */
export function accountFillOrder(accounts: Account[]): Account[] {
  const rank = (a: Account): number => {
    const t = a.timestamp_created ? Date.parse(a.timestamp_created) : NaN;
    return Number.isNaN(t) ? Number.POSITIVE_INFINITY : t;
  };
  return [...accounts].sort((x, y) => {
    const rx = rank(x);
    const ry = rank(y);
    if (rx !== ry) return rx - ry;
    return x.email.localeCompare(y.email);
  });
}

/**
 * Sequential-fill account selection — saturate the first account in the fixed
 * order before touching the second.
 *
 * Per account:
 *   cap   = min(daily_limit, rampCapForAge(timestamp_created, ...))  (capForAccount)
 *   load  = sentToday + Q0-first + Q0-next
 *           real dispatches today + never-contacted sequences (1 first email each)
 *           + followup steps projected today/overdue — so a followup queued days
 *           ago but due today counts against today's cap.
 *   pick  = the FIRST account of `accountFillOrder` whose load < cap.
 *
 * Why sequential and not a fill RATIO: the fleet is deliberately over-provisioned
 * (20 in_production accounts × 45/day ≫ real volume), and spreading volume evenly
 * means every mailbox carries a little traffic — so none can ever be cancelled.
 * Concentrating on the head of a STABLE order leaves the tail idle, which is what
 * makes "this account has sent nothing for N days" a usable delete signal. The
 * starvation of the tail is the OBJECTIVE here, not a side effect.
 *
 * This is deliberately the shape that #543 removed (a mature-before-fresh tier
 * order whose lower tiers were unreachable). The difference is intent: there the
 * starved accounts were meant to be sending and silently were not; here they are
 * meant to go quiet so the fleet can be shrunk. Do NOT "restore" the fill-ratio
 * policy on the strength of that incident without re-reading this paragraph.
 *
 * The AGE CAP is kept intact — a fresh mailbox is filled to ITS ramped cap (5/day
 * at one day old), never to 45, so the sequence never trips Gmail's 550-5.4.5.
 *
 * When every account is at or over its cap the fleet is backlogged; we then fall
 * back to the least-overloaded `load / cap` so a send is never blocked, ties going
 * to the earlier account in the order. Selection is fully deterministic: the same
 * inputs always yield the same account.
 *
 * An account absent from `byEmail` is all-zeros ⇒ load 0 ⇒ has room. Correctness
 * of `load` depends on the sending account being persisted on the campaign row at
 * send time (see account-sending-stats.ts) so a just-contacted lead counts against
 * its account immediately, not after the lagging first email_sent webhook.
 */
export function pickSequentialFillAccount(
  accounts: Account[],
  byEmail: Map<string, AccountCapacity>,
  asOf: Date = new Date(),
): Account {
  if (accounts.length === 0) {
    throw new Error("No accounts available");
  }

  const ordered = accountFillOrder(accounts);

  for (const a of ordered) {
    const cap = capForAccount(a, asOf);
    // A zero/negative cap (an account deliberately pinned to daily_limit 0) can
    // never absorb a lead — skip it entirely rather than dividing by zero.
    if (cap > 0 && loadForAccount(a, byEmail) < cap) return a;
  }

  // Every account is full: pick the least-overloaded one (it is also the one with
  // room soonest). `ordered` is stable, so the first minimum wins on a tie.
  let best = ordered[0];
  let bestRatio = Number.POSITIVE_INFINITY;
  for (const a of ordered) {
    const cap = capForAccount(a, asOf);
    const ratio =
      cap > 0 ? loadForAccount(a, byEmail) / cap : Number.POSITIVE_INFINITY;
    if (ratio < bestRatio) {
      bestRatio = ratio;
      best = a;
    }
  }
  return best;
}

/**
 * Wrap plain-text URLs and bare domains in `<a href>` so Instantly's link
 * tracker rewrites them into redirect URLs (without anchors, Instantly leaves
 * the URL untouched and no `email_link_clicked` webhook ever fires).
 *
 * Mustache placeholders (`{{firstName}}`, `{{user.email}}`) are masked before
 * linkify to avoid wrapping things like `user.email` that look domain-like.
 *
 * The DISPLAY text of every linkified URL is stripped of its query string
 * (`?...`) so a destination link carrying UTM params renders clean
 * (`https://site.com/lp/offer/`, not the giant `?utm_source=...&utm_id=...`).
 * The `href` keeps the FULL URL — Instantly rewrites it into a tracking
 * redirect, and the click still carries every UTM param to the destination.
 */
export function autolinkifyHtml(html: string): string {
  const placeholders: string[] = [];
  const masked = html.replace(/\{\{[^}]*\}\}/g, (m) => {
    placeholders.push(m);
    return `XXLINKMUSTACHE${placeholders.length - 1}XX`;
  });
  const linkified = linkifyHtml(masked, {
    defaultProtocol: "https",
    format: (value, type) => (type === "url" ? value.split("?")[0] : value),
  });
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
 * Unsubscribe footer appended BELOW the signature on every sent email.
 *
 * `{unsubscribe_link}` is Instantly's server-side merge variable (SINGLE braces
 * — the `{{...}}` mustache form does NOT resolve): it renders per-lead at send
 * into a functional opt-out URL. This is a SECOND, VISIBLE opt-out path that
 * complements the RFC-8058 `List-Unsubscribe` header set on the campaign
 * (`insert_unsubscribe_header: true`), which powers the mailbox-native
 * (Gmail/Apple/Outlook) one-click unsubscribe button.
 *
 * A `<p>&nbsp;</p>` spacer paragraph separates it from the signature with a
 * blank-line gap. Rendered small / grey / italic via inline CSS; Instantly's
 * HTML sanitizer MAY strip the `style` attribute on PATCH round-trip (validate
 * the rendered result on a live send), but the anchor + text survive regardless
 * because they are tag-wrapped.
 *
 * Appended as part of the signature block (after the `<p>--</p>` marker) so
 * `stripAccountSignature` removes it together with the signature on re-send —
 * idempotency (`f(f(x)) === f(x)`) is preserved.
 */
export const UNSUBSCRIBE_FOOTER_HTML =
  "<p>&nbsp;</p>" +
  '<p style="font-size:12px;color:#999999;font-style:italic">' +
  "Don't want to hear from me again? " +
  '<a href="{unsubscribe_link}" style="color:#999999">unsubscribe</a></p>';

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
 *
 * The `UNSUBSCRIBE_FOOTER_HTML` block (visible opt-out via `{unsubscribe_link}`)
 * is appended below the signature, INSIDE the strip-and-reappend region, so
 * idempotency holds — a re-sent body never stacks the footer.
 */
export function buildEmailBodyWithSignature(body: string, account: Account): string {
  const accountSig = account.signature?.trim() || "";
  const signature = accountSig || buildDefaultSignature(account);
  const stripped = stripAccountSignature(body);

  const linkedBody = autolinkifyHtml(stripped);
  const sigBlock = `${SIG_SEPARATOR_HTML}${signature}${UNSUBSCRIBE_FOOTER_HTML}`;

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
  /**
   * The feature this send belongs to (x-feature-slug). Selects the sending-account
   * pool: a slug reserved in instantly_account_feature_policy draws from its
   * dedicated accounts (e.g. sales-crm-email-outreach → the CRM accounts); any
   * non-reserved slug or null draws from the shared unreserved fleet.
   */
  featureSlug?: string | null;
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
  const accounts = await fetchInProductionAccounts(opts.featureSlug ?? null);

  if (accounts.length === 0) {
    console.warn(
      `[send-lead] No in_production accounts available for "${opts.campaignName}"` +
        (opts.featureSlug ? ` (feature "${opts.featureSlug}")` : ""),
    );
    return { ok: false, reason: "no_healthy_accounts_available" };
  }

  const capacityByEmail = await fetchAccountCapacityCached();
  const account = pickSequentialFillAccount(accounts, capacityByEmail);
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
