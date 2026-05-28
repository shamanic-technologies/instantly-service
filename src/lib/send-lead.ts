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
 * One-shot: picks a single healthy account (weighted by warmup score),
 * creates a fresh Instantly campaign, adds the lead, activates. Returns
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
  listAccounts,
  type Account,
  type Lead,
  type SequenceStep,
} from "./instantly-client";

/**
 * Accounts whose domain we refuse to send from regardless of Instantly
 * status. Add domains here when a sender's deliverability is so poor that
 * even an "active" status is misleading.
 */
const BLOCKED_DOMAINS = [
  "arcadiaquest.org",
];

/**
 * Pick an account using a single-pool weighted random:
 *   weight = max(1, stat_warmup_score ?? 0)
 *
 * Accounts without a score still get a baseline weight of 1, so they remain
 * eligible while warmer accounts are favoured proportionally. Falls back to
 * uniform random when no account has a score (all weights collapse to 1).
 */
export function pickRandomAccount(accounts: Account[]): Account {
  if (accounts.length === 0) {
    throw new Error("No accounts available");
  }

  const weights = accounts.map((a) => Math.max(1, a.stat_warmup_score ?? 0));
  const total = weights.reduce((sum, w) => sum + w, 0);
  const target = Math.random() * total;

  let acc = 0;
  for (let i = 0; i < accounts.length; i++) {
    acc += weights[i];
    if (target < acc) return accounts[i];
  }

  return accounts[accounts.length - 1];
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
 * Canonical signature (HTML-formatted) appended to every outbound email when
 * the assigned account has no per-sender override in Instantly's UI.
 *
 * Per-account UI signatures are intentionally empty in prod so every sender
 * shares one canonical signature.
 *
 * Wrapped in `<p>...</p>` because Instantly's HTML sanitizer aggressively
 * strips plain text and `--` outside of element wrappers (text loses on PATCH
 * round-trip: only `<a>` anchors survive). Historic bug 2026-05-28: an earlier
 * plain-text version of this constant was stripped down to a stray
 * `<a>distribute.you</a>` anchor each time it round-tripped through Instantly.
 */
const DEFAULT_SIGNATURE =
  "<p>Kevin Lourd | Marketing Representative<br>Distributed with ❤️ from distribute.you</p>";

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
 *   2. `DEFAULT_SIGNATURE` constant above — service-wide fallback, source of
 *      truth in prod.
 *
 * Idempotent (`f(f(x)) === f(x)`): always strips any pre-existing signature
 * block via `stripAccountSignature` BEFORE appending. Guarantees a body re-sent
 * N times never accumulates N stacked signatures (historic bug 2026-05-28 —
 * see `stripAccountSignature` docstring).
 */
export function buildEmailBodyWithSignature(body: string, account: Account): string {
  const accountSig = account.signature?.trim() || "";
  const signature = accountSig || DEFAULT_SIGNATURE;
  const stripped = stripAccountSignature(body);

  const raw = stripped.includes("{{accountSignature}}")
    ? stripped.replace("{{accountSignature}}", `${SIG_SEPARATOR_HTML}${signature}`)
    : `${stripped}${SIG_SEPARATOR_HTML}${signature}`;

  return autolinkifyHtml(raw);
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
 * Filter the raw Instantly account list to senders we can send from now:
 *   - `status > 0` (active in Instantly's account state machine)
 *   - domain not in BLOCKED_DOMAINS
 *
 * The returned list is unsorted; pacing/warmup weighting happens in
 * `pickRandomAccount`.
 */
export function filterHealthyAccounts(accounts: Account[]): Account[] {
  return accounts.filter((a) => {
    if (a.status <= 0) return false;
    const domain = a.email.split("@")[1];
    if (BLOCKED_DOMAINS.includes(domain)) {
      console.log(`[send-lead] Skipping blocked-domain account: ${a.email}`);
      return false;
    }
    return true;
  });
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
): Promise<{ instantlyCampaignId: string; added: number }> {
  console.log(
    `[send-lead] Creating Instantly campaign "${campaignName}" with account ${account.email}`,
  );
  const instantlyCampaign = await createInstantlyCampaign(apiKey, {
    name: campaignName,
    steps,
  });
  console.log(
    `[send-lead] Created instantly campaign id=${instantlyCampaign.id} status=${instantlyCampaign.status}`,
  );

  // Assign the selected account via PATCH.
  await updateInstantlyCampaign(apiKey, instantlyCampaign.id, {
    email_list: [account.email],
    open_tracking: true,
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
 * Find a healthy Instantly account for the given org's API key and send
 * the lead onto a fresh campaign. One-shot — no retry on post-activate
 * NSS (retry-stuck owns the eventual catch-up).
 *
 * Returns:
 *   - `{ok: true, ...}` on success with the new Instantly campaign ID + chosen account.
 *   - `{ok: false, reason: "no_healthy_accounts_available"}` when `listAccounts`
 *     returns zero senders that pass `filterHealthyAccounts` — caller surfaces
 *     this to the upstream (no row created).
 */
export async function sendLeadToInstantly(opts: SendOptions): Promise<SendResult> {
  const allAccounts = await listAccounts(opts.apiKey);
  const accounts = filterHealthyAccounts(allAccounts);

  if (accounts.length === 0) {
    console.warn(
      `[send-lead] No healthy accounts available (raw=${allAccounts.length}) for "${opts.campaignName}"`,
    );
    return { ok: false, reason: "no_healthy_accounts_available" };
  }

  const account = pickRandomAccount(accounts);
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
  );

  return {
    ok: true,
    value: { ...result, account },
  };
}
