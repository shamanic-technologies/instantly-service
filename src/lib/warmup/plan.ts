/**
 * The warmup mesh — deciding who writes to whom today. Pure, no IO.
 *
 * Instantly's Email Outreach subscription bundles a warmup pool of tens of
 * thousands of mailboxes that exchange mail, read it, rescue it from spam and
 * reply. We are cancelling that subscription, so the mailboxes have to keep
 * each other warm themselves: every credentialed mailbox writes to a handful of
 * the others each day, and the receiving side opens the message, moves it out of
 * spam if that is where it landed, and sometimes answers.
 *
 * ── What the production data actually says warmup is for ────────────────────
 *
 * Measured 2026-09-06 on the Gandi fleet, by real volume sent over 30 days
 * against the seed placement score:
 *
 *     0 sends (warmup only)   94 accounts   15.9% inbox    4 above the bar
 *     50-199 sends            28 accounts   48.6% inbox    9 above the bar
 *     200+ sends               7 accounts   28.6% inbox    0 above the bar
 *
 * Three things follow, and they shape every constant below:
 *
 *   - Uniform warmup on an idle mailbox buys almost nothing. Those 94 accounts
 *     had been warming at 30/day for six weeks through Instantly with a mean
 *     health score of 95.6, and their delivery was flat at 34-36% the whole
 *     time. So this mesh is deliberately MODEST, not a volume replacement.
 *   - What correlates with inboxing is sending real, varied mail at a moderate
 *     rate. Hence a varied body per message (below) rather than a template.
 *   - Past ~200 sends it turns back down, which is the no-ramp over-send this
 *     fleet was burned by. Warmup volume is small enough not to add to that.
 *
 * ⚠️ The correlation is not causation — the accounts that send were CHOSEN to
 * send because they scored well. Stated so nobody reads these constants as
 * derived from a proven mechanism.
 */

/** Mailboxes each sender writes to per day. */
export const WARMUP_PARTNERS_PER_DAY = 4;

/**
 * The most of a mailbox's daily cap warmup may take.
 *
 * ⚠️ Warmup must never starve outreach, and a flat partner count does exactly
 * that at the bottom of the ramp: a mailbox freshly promoted sits at the
 * `RAMP_FLOOR_PER_DAY` of 5, so 4 partners would leave ONE send for real
 * prospects. Measured on the first live run (2026-09-06): 402 of 784 planned
 * edges were skipped for lack of room, and the mailboxes with least room are
 * exactly the ones the ramp is protecting.
 *
 * The fraction makes the mesh scale with the mailbox instead — 1 send at a cap
 * of 5, 4 at a cap of 15, and the flat maximum above that. Outreach is the job;
 * warmup exists to protect it, not to compete with it.
 */
export const WARMUP_MAX_SHARE_OF_CAP = 0.3;

/** Warmup sends this mailbox may make today, given the cap outreach also draws on. */
export function warmupBudgetFor(
  cap: number,
  partnersPerDay: number = WARMUP_PARTNERS_PER_DAY,
): number {
  if (cap <= 0) return 0;
  return Math.max(1, Math.min(partnersPerDay, Math.floor(cap * WARMUP_MAX_SHARE_OF_CAP)));
}

/**
 * Share of received warmup mail that gets an answer.
 *
 * A reply is the strongest signal in the set — it is what distinguishes a
 * conversation from a broadcast — but a mailbox that answers EVERYTHING is
 * itself a pattern. One in three is enough to make threads exist without making
 * the mesh look mechanical.
 */
export const WARMUP_REPLY_RATE = 1 / 3;

/**
 * Permanent failures within the window that silence a mailbox as a SENDER.
 *
 * Three, not one: mail servers refuse individual messages for ordinary reasons
 * and a single 5xx says nothing about the mailbox. Measured 2026-09-06,
 * `kevin@growthagency.ch` had 1 permanent failure against 5 successes (a blip)
 * while `kevin@growthagency.cloud` had 20 against 0 — its own Gandi relay
 * refusing every send with `550 5.7.1 Blacklisted user`.
 */
export const WARMUP_SILENCE_MIN_FAILURES = 3;

/** One mailbox's warmup outcomes over the recent window. */
export interface WarmupSenderHealth {
  mailbox: string;
  permanent: number;
  sent: number;
}

/**
 * Mailboxes to stop sending warmup FROM.
 *
 * ⚠️ A mailbox whose relay refuses everything must not be hammered daily. Left
 * alone the mesh replans its four edges every morning, every one of them fails
 * `550`, and we spend the day telling a provider that is already blocking us
 * that we would like to send anyway — which is the opposite of what a warmup
 * mesh is for.
 *
 * ⚠️ IT MUST STILL RECEIVE. The blacklist is on SENDING; mail addressed to it
 * arrives normally, and being written to is itself part of looking like a live
 * mailbox. Only the sender side is silenced.
 *
 * Self-healing WITHOUT extra state: the window rolls, so a silenced mailbox's
 * failures age out and it is retried automatically. If the relay is still
 * blocking, that day's failures silence it again — bounded cost, no flag to
 * remember to clear. Requiring ZERO successes is what keeps a mailbox that
 * mostly works from being silenced by a bad afternoon.
 */
export function selectSilencedSenders(
  health: readonly WarmupSenderHealth[],
  minFailures: number = WARMUP_SILENCE_MIN_FAILURES,
): Set<string> {
  const silenced = new Set<string>();
  for (const row of health) {
    if (row.sent === 0 && row.permanent >= minFailures) silenced.add(row.mailbox);
  }
  return silenced;
}

export interface WarmupPairing {
  senderEmail: string;
  receiverEmail: string;
}

/**
 * Deterministic pseudo-random ordering, seeded by (day, mailbox).
 *
 * Deterministic so a run repeated inside the same day picks the SAME partners
 * and the unique index makes it a no-op — the sweep is then idempotent without
 * storing a cursor, exactly as the seed harness is. Varying by day so the mesh
 * is not the same graph every morning: a fixed set of pairs repeating daily is
 * the reciprocal pattern a spam filter is best at spotting.
 */
function seededRank(dayKey: string, sender: string, candidate: string): number {
  const s = `${dayKey}|${sender}|${candidate}`;
  // FNV-1a. Not cryptographic — it only has to be stable and well-mixed.
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i += 1) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h;
}

/** UTC day key — the unit the pairing and the daily cap both use. */
export function warmupDayKey(asOf: Date): string {
  return asOf.toISOString().slice(0, 10);
}

/**
 * Who writes to whom today.
 *
 * ⚠️ NEVER RECIPROCAL ON THE SAME DAY. A pair that writes to each other every
 * morning is the shape of a warmup ring, and it is the one thing a filter can
 * detect about a mesh this small (~200 mailboxes against Instantly's tens of
 * thousands). Once A→B is chosen, B→A is excluded for the day; B still writes
 * to somebody else, and the reverse edge is free to appear tomorrow.
 *
 * ⚠️ A MAILBOX NEVER WRITES TO ANOTHER ON ITS OWN DOMAIN. Same-domain mail
 * frequently skips filtering altogether, so those messages measure nothing and
 * warm nothing — and on a Gandi domain the "other" mailbox is usually the same
 * physical inbox behind an alias, which would be a self-send.
 *
 * `excluded` is the measurement receiver set — see {@link partnerCandidates}.
 */
export function planWarmupPairings(
  mailboxes: readonly string[],
  asOf: Date,
  partnersPerDay: number = WARMUP_PARTNERS_PER_DAY,
): WarmupPairing[] {
  const dayKey = warmupDayKey(asOf);
  const pool = [...new Set(mailboxes.map((m) => m.trim().toLowerCase()).filter(Boolean))].sort();

  const taken = new Set<string>();
  const out: WarmupPairing[] = [];

  for (const sender of pool) {
    const senderDomain = domainOf(sender);

    const partners = pool
      .filter((candidate) => candidate !== sender)
      .filter((candidate) => domainOf(candidate) !== senderDomain)
      // The reverse edge, if it was already chosen today.
      .filter((candidate) => !taken.has(`${candidate}->${sender}`))
      .sort(
        (a, b) => seededRank(dayKey, sender, a) - seededRank(dayKey, sender, b),
      )
      .slice(0, partnersPerDay);

    for (const receiverEmail of partners) {
      taken.add(`${sender}->${receiverEmail}`);
      out.push({ senderEmail: sender, receiverEmail });
    }
  }

  return out;
}

function domainOf(email: string): string {
  const at = email.lastIndexOf("@");
  return at === -1 ? "" : email.slice(at + 1).trim().toLowerCase();
}

/**
 * The mailboxes eligible to take part in the mesh.
 *
 * ⚠️ THE MEASUREMENT RECEIVERS ARE EXCLUDED, AND THIS IS THE INVARIANT THE WHOLE
 * DESIGN RESTS ON. The seed harness grades a sender by reading where its mail
 * landed in a receiver's folders. Rescuing a message from that same receiver's
 * spam folder teaches its filter that this sender is legitimate — so a receiver
 * that both grades and rescues returns a verdict it has been trained to give.
 * The score would drift optimistic over weeks, silently, and the delivery gate
 * (the ONLY demotion path a mailbox has) would stop catching anything.
 *
 * Costing ~10 mailboxes out of ~200 buys a judge that stays independent. Do NOT
 * "simplify" by letting the seed receivers double as warmup partners.
 */
export function partnerCandidates(
  credentialed: readonly string[],
  measurementReceivers: readonly string[],
  mailboxLogins?: ReadonlyMap<string, string>,
): string[] {
  const judges = new Set(
    measurementReceivers.map((r) => r.trim().toLowerCase()),
  );

  const eligible = [...new Set(credentialed.map((m) => m.trim().toLowerCase()))]
    .filter((m) => m && !judges.has(m))
    .sort();

  if (!mailboxLogins) return eligible;

  // ⚠️ ONE ADDRESS PER REAL MAILBOX. The pairing hands each participant a fixed
  // number of partners, but the quota it spends belongs to the MAILBOX — and
  // five Gandi aliases are one mailbox. Pairing per address therefore multiplies
  // the fan-out by the alias count: measured on the first live run
  // (2026-09-06), 196 addresses on 63 real mailboxes produced 5.7 warmup sends
  // per mailbox rather than 4, and a 5-alias domain would have carried 20.
  //
  // Deterministic pick (the sorted-first address) so the participant is stable
  // day to day: a mailbox whose warmup arrives from a different alias every
  // morning looks like several correspondents rather than one.
  const seen = new Set<string>();
  const out: string[] = [];
  for (const address of eligible) {
    const mailbox = mailboxLogins.get(address) ?? address;
    if (seen.has(mailbox)) continue;
    seen.add(mailbox);
    out.push(address);
  }
  return out;
}

/**
 * Does this received message get an answer?
 *
 * Keyed on the message id so the decision is stable: the poller re-reads a
 * window of days, and a message that flips between "reply" and "no reply"
 * across runs would be answered twice.
 */
export function shouldReplyTo(messageId: string, rate: number = WARMUP_REPLY_RATE): boolean {
  let h = 0x811c9dc5;
  for (let i = 0; i < messageId.length; i += 1) {
    h ^= messageId.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h / 0xffffffff < rate;
}
