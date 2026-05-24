/**
 * Shared send helper.
 *
 * Encapsulates the "find a healthy Instantly account + create campaign + add
 * lead + activate + check not_sending_status" loop. Used by:
 *   - POST /send (first-time send)
 *   - retry-stuck heartbeat (re-send when an existing row has stayed in
 *     `delivery_status='contacted'` past STUCK_AGE_HOURS without any silver
 *     proof Instantly actually sent — see lib/retry-stuck.ts)
 *
 * The retry loop picks a random account each attempt (weighted by warmup score),
 * creates a fresh Instantly campaign, adds the lead, activates, and verifies
 * not_sending_status is null. If set, we discard and try again with a different
 * account — up to MAX_SEND_RETRIES.
 *
 * No exclusion list: with ~100+ accounts per org, random sampling collides
 * rarely enough that maintaining a "failed accounts" list is more bookkeeping
 * than payoff. Each send is independent.
 */

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

export const MAX_SEND_RETRIES = 3;

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
 * Inject the selected account's signature into the email body.
 *
 * `{{accountSignature}}` only resolves in the Instantly UI — campaigns created
 * via the API send it as literal text. Instead we splice the assigned account's
 * `signature` field directly.
 */
export function buildEmailBodyWithSignature(body: string, account: Account): string {
  const signature = account.signature?.trim() || "";

  if (!signature) {
    console.warn(
      `[send-lead] Account ${account.email} has no signature configured — email will be sent without signature`,
    );
    return body.replace(/\n*\{\{accountSignature\}\}/g, "");
  }

  if (body.includes("{{accountSignature}}")) {
    return body.replace("{{accountSignature}}", `--\n${signature}`);
  }

  return `${body}\n\n--\n${signature}`;
}

/**
 * Strip a previously-appended `\n\n--\nSIGNATURE` block from a body. Used by
 * retry-stuck to recover the original prospect-facing body from an Instantly
 * campaign that already had account A's signature baked in, so account B's
 * signature can be re-injected.
 *
 * Heuristic: split on the marker `\n\n--\n` and keep everything before it.
 * If the marker doesn't appear, return the body unchanged. Senders whose
 * original body legitimately contains `\n\n--\n` will lose content past that
 * point on a re-send — accepted edge-case rather than introducing a new
 * `sequence_template` table just for this rare path.
 */
export function stripAccountSignature(body: string): string {
  const marker = "\n\n--\n";
  const idx = body.indexOf(marker);
  if (idx === -1) return body;
  return body.slice(0, idx);
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
 * Create an Instantly campaign, assign one account, add the lead, activate,
 * and verify `not_sending_status` is null post-activation. Returns the new
 * Instantly campaign ID on success, or `null` if `not_sending_status` fired
 * (caller should retry with a different account).
 */
export async function tryCreateAndActivateCampaign(
  apiKey: string,
  campaignName: string,
  account: Account,
  steps: SequenceStep[],
  lead: Lead,
): Promise<{ instantlyCampaignId: string; added: number } | null> {
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

  // Post-activation guard: Instantly sometimes flips not_sending_status the
  // moment a campaign is activated (e.g. account just hit daily quota). If
  // set, caller will retry with a different account.
  const postActivate = (await getInstantlyCampaign(
    apiKey,
    instantlyCampaign.id,
  )) as unknown as Record<string, unknown>;

  if (postActivate.not_sending_status) {
    const reason = `not_sending_status: ${JSON.stringify(postActivate.not_sending_status)}`;
    console.warn(
      `[send-lead] Campaign ${instantlyCampaign.id} ${reason} — will retry with another account`,
    );
    return null;
  }

  return { instantlyCampaignId: instantlyCampaign.id, added: addResult.added };
}

export interface SendOptions {
  apiKey: string;
  campaignName: string;
  subject: string;
  sortedSequence: SortedSequenceStep[];
  lead: Lead;
  maxRetries?: number;
}

export interface SendSuccess {
  instantlyCampaignId: string;
  added: number;
  account: Account;
}

export type SendFailureReason = "no_healthy_account" | "max_retries_exhausted";

export type SendResult =
  | { ok: true; value: SendSuccess }
  | { ok: false; reason: SendFailureReason };

/**
 * Find a healthy Instantly account for the given org's API key and send
 * the lead onto a fresh campaign. Tries up to `maxRetries` accounts before
 * giving up.
 *
 * Returns:
 *   - `{ok: true, ...}` on success with the new Instantly campaign ID + chosen account.
 *   - `{ok: false, reason: "no_healthy_account"}` when `listAccounts` returns
 *     zero senders that pass `filterHealthyAccounts` — caller leaves the row
 *     alone and lets the next tick retry.
 *   - `{ok: false, reason: "max_retries_exhausted"}` when every attempt hit
 *     `not_sending_status` post-activate — caller leaves the row alone and lets
 *     the next tick retry.
 */
export async function sendLeadToInstantly(opts: SendOptions): Promise<SendResult> {
  const maxRetries = opts.maxRetries ?? MAX_SEND_RETRIES;

  const allAccounts = await listAccounts(opts.apiKey);
  const accounts = filterHealthyAccounts(allAccounts);

  if (accounts.length === 0) {
    console.warn(
      `[send-lead] No healthy accounts available (raw=${allAccounts.length}) for "${opts.campaignName}"`,
    );
    return { ok: false, reason: "no_healthy_account" };
  }

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    const account = pickRandomAccount(accounts);
    const steps = buildSequenceSteps(opts.subject, opts.sortedSequence, account);

    console.log(
      `[send-lead] Attempt ${attempt}/${maxRetries} for "${opts.campaignName}" with account ${account.email}`,
    );

    const result = await tryCreateAndActivateCampaign(
      opts.apiKey,
      opts.campaignName,
      account,
      steps,
      opts.lead,
    );

    if (result) {
      return {
        ok: true,
        value: { ...result, account },
      };
    }
  }

  return { ok: false, reason: "max_retries_exhausted" };
}
