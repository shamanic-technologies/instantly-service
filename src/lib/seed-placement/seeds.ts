/**
 * Seed-placement planning (pure — no IO).
 *
 * An inbox-placement test asks one question per sending mailbox: when this
 * mailbox sends, where does the mail land? Answering it needs a set of RECEIVER
 * inboxes we can read. Instantly rents us that set for $47/mo; this module picks
 * it out of the mailboxes we already own.
 *
 * The receivers are deliberately chosen for DOMAIN DIVERSITY rather than count.
 * A verdict is a property of the (sender, receiving-domain) pair, so ten seeds
 * to ten domains says far more than fifty seeds to two — and every extra
 * receiver multiplies the volume every sender has to carry on test day.
 *
 * ⚠️ FIDELITY CAVEAT, stated here because it bounds what the score MEANS.
 * These receivers are our own Google Workspace mailboxes, and Workspace inbound
 * filtering is more permissive than consumer `gmail.com`. A mailbox can score
 * well here and still land in a real prospect's spam. That is why this ships
 * alongside the paid test rather than replacing it on day one: the two run in
 * parallel and the paid subscription is only cancelled once their scores agree.
 * If they do not agree, the fix is consumer seed inboxes (free to create, read
 * over the same IMAP path) — NOT lowering the bar.
 */

/**
 * Marks a placement result this service measured itself.
 *
 * Mirrors the `self:` and `reserving:` sentinels already used on
 * `instantly_campaigns`: the id occupies a column that otherwise holds a vendor
 * id, so it carries a prefix that makes the provenance unmistakable. Silver
 * keeps both kinds side by side, which is exactly what the parallel-run
 * comparison reads.
 */
export const SEED_TEST_ID_PREFIX = "seed:";

/** Recipient-ESP codes, matching the enum Instantly's own analytics rows use. */
export const ESP_GOOGLE = 1;
export const ESP_OUTLOOK = 2;

/** How many receiver mailboxes one test seeds. See the domain-diversity note above. */
export const MAX_SEED_RECEIVERS = 10;

export function mintSeedTestId(): string {
  return `${SEED_TEST_ID_PREFIX}${crypto.randomUUID()}`;
}

export function isSeedTestId(testId: string): boolean {
  return testId.startsWith(SEED_TEST_ID_PREFIX);
}

export function domainOf(email: string): string {
  const at = email.lastIndexOf("@");
  return at === -1 ? "" : email.slice(at + 1).trim().toLowerCase();
}

const OUTLOOK_DOMAINS = new Set([
  "outlook.com",
  "hotmail.com",
  "live.com",
  "msn.com",
]);

/**
 * Hosts that mean "this mailbox is really Google".
 *
 * ⚠️ THE HOST IS THE GROUND TRUTH, NOT THE DOMAIN. Every mailbox in this fleet
 * sits on a custom domain, so the domain name says nothing about who filters its
 * mail: `kevin@growthagency.bio` is Gandi, `k.lourd@growdistribute.com` is
 * Google Workspace, and both look identical from the address alone. The
 * credential we resolved carries the real host, so that is what decides.
 */
const GOOGLE_IMAP_HOSTS = new Set(["imap.gmail.com", "imap.google.com"]);

/**
 * Which ESP a receiving mailbox belongs to, from its resolved IMAP host.
 *
 * Returns null when the host is one we cannot attribute to a consumer-relevant
 * ESP — a Gandi mailbox, a Mailforge relay. Null is NOT "assume Google": the
 * first version of this defaulted every custom domain to Google, which labelled
 * six Gandi receivers as Gmail placement in the very first run. A placement
 * score is a claim about a specific ESP, and claiming the wrong one is worse
 * than declining to claim.
 */
export function espForReceiverHost(email: string, imapHost: string): number | null {
  const domain = domainOf(email);
  if (OUTLOOK_DOMAINS.has(domain)) return ESP_OUTLOOK;
  if (GOOGLE_IMAP_HOSTS.has(imapHost.trim().toLowerCase())) return ESP_GOOGLE;
  if (domain === "gmail.com" || domain === "googlemail.com") return ESP_GOOGLE;
  return null;
}

export interface SeedReceiver {
  email: string;
  recipientEsp: number;
}

/** A candidate receiver: the address plus the host its credential resolved to. */
export interface ReceiverCandidate {
  email: string;
  imapHost: string;
}

/**
 * Pick the receiver set: at most one mailbox per domain, up to `max`.
 *
 * ⚠️ ONLY MAILBOXES ON A CONSUMER-RELEVANT ESP ARE ELIGIBLE. A Gandi-hosted or
 * relay-hosted receiver is filtered by that host, not by Gmail — so a seed
 * landing in its inbox says nothing about whether our mail reaches the prospects
 * we actually email, who are on Google and Microsoft. Including them does not
 * merely add noise, it silently answers a different question under the same
 * label. Such mailboxes remain perfectly good SENDERS; they just cannot grade.
 *
 * Deterministic (sorted by domain) so the same fleet produces the same receivers
 * every week — a moving receiver set would make week-over-week scores
 * incomparable, which is the only thing a placement history is for.
 */
export function selectSeedReceivers(
  candidates: readonly ReceiverCandidate[],
  max: number = MAX_SEED_RECEIVERS,
): SeedReceiver[] {
  const byDomain = new Map<string, SeedReceiver>();

  const sorted = [...candidates]
    .map((c) => ({ email: c.email.trim().toLowerCase(), imapHost: c.imapHost }))
    .sort((a, b) => (a.email < b.email ? -1 : a.email > b.email ? 1 : 0));

  for (const candidate of sorted) {
    const domain = domainOf(candidate.email);
    if (!domain) continue;
    if (byDomain.has(domain)) continue;

    const recipientEsp = espForReceiverHost(candidate.email, candidate.imapHost);
    if (recipientEsp === null) continue;

    byDomain.set(domain, { email: candidate.email, recipientEsp });
  }

  return [...byDomain.entries()]
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .slice(0, max)
    .map(([, receiver]) => receiver);
}

export interface SeedSend {
  senderEmail: string;
  receiverEmail: string;
  recipientEsp: number;
}

/**
 * The full send plan: every sender × every receiver, MINUS self-sends.
 *
 * A mailbox sending to itself never crosses a spam filter — the message is
 * delivered locally — so counting it would inflate every sender that happens to
 * also be a receiver, which under this receiver set is most of them.
 */
export function planSeedSends(
  senders: readonly string[],
  receivers: readonly SeedReceiver[],
): SeedSend[] {
  const out: SeedSend[] = [];

  for (const sender of [...senders].map((s) => s.trim().toLowerCase()).sort()) {
    if (!sender) continue;
    for (const receiver of receivers) {
      if (receiver.email === sender) continue;
      out.push({
        senderEmail: sender,
        receiverEmail: receiver.email,
        recipientEsp: receiver.recipientEsp,
      });
    }
  }

  return out;
}
