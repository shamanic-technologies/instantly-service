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
 * Which ESP a receiving address belongs to.
 *
 * Only the consumer domains can be told apart by name. A custom domain is
 * resolved by the caller (our fleet is Google Workspace throughout), so this
 * defaults to Google rather than guessing — and the default is stated at the
 * call site, not hidden here.
 */
export function espForReceiver(email: string, fallback: number = ESP_GOOGLE): number {
  const domain = domainOf(email);
  if (OUTLOOK_DOMAINS.has(domain)) return ESP_OUTLOOK;
  if (domain === "gmail.com" || domain === "googlemail.com") return ESP_GOOGLE;
  return fallback;
}

export interface SeedReceiver {
  email: string;
  recipientEsp: number;
}

/**
 * Pick the receiver set: at most one mailbox per domain, up to `max`.
 *
 * Deterministic (sorted by domain then address) so the same fleet produces the
 * same receivers every week — a moving receiver set would make week-over-week
 * scores incomparable, which is the only thing a placement history is for.
 */
export function selectSeedReceivers(
  candidates: readonly string[],
  max: number = MAX_SEED_RECEIVERS,
): SeedReceiver[] {
  const byDomain = new Map<string, string>();

  for (const raw of [...candidates].map((c) => c.trim().toLowerCase()).sort()) {
    const domain = domainOf(raw);
    if (!domain) continue;
    if (!byDomain.has(domain)) byDomain.set(domain, raw);
  }

  return [...byDomain.entries()]
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .slice(0, max)
    .map(([, email]) => ({ email, recipientEsp: espForReceiver(email) }));
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
