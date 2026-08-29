/**
 * The seed message (pure — no IO).
 *
 * ⚠️ A SEED IS NOT PROSPECT MAIL, so it deliberately does NOT go through
 * `buildEmailBodyWithSignature`. That pipeline appends a signature and an
 * unsubscribe footer carrying `{unsubscribe_link}` — a merge variable only
 * Instantly resolves — plus routes every link through our click redirect. None
 * of it belongs on a message sent to ourselves, and the opt-out placeholder
 * would ship unresolved. Reusing it would also mean a seed measures the footer
 * as much as the mailbox.
 *
 * What the seed DOES have to be is ORDINARY. A placement test is only
 * meaningful if the probe looks like the mail whose fate we are predicting, so
 * this is a short, plain, link-free business message — no tracking pixel, no
 * anchors, no marketing vocabulary. Anything that makes the seed unusual makes
 * the verdict a fact about the seed rather than about the mailbox.
 *
 * Correlation rides on the RFC 5322 `Message-Id` the server assigns, not on the
 * body: a body marker would be one more thing a filter can score, and Gmail
 * rewrites nothing in the header we key on. The test id travels in a custom
 * header purely so a reader can attribute a message without a database lookup.
 */

import type { BuiltMessage } from "../self-send/message";

/** Custom header carrying the seed test id, for attribution without a DB round-trip. */
export const SEED_TEST_HEADER = "X-Seed-Placement-Test";

/** Custom header naming the sending mailbox, so a receiver can attribute the seed. */
export const SEED_SENDER_HEADER = "X-Seed-Placement-Sender";

export interface SeedMessageInput {
  testId: string;
  senderEmail: string;
  receiverEmail: string;
}

/**
 * Subject line.
 *
 * Carries no test id and no serial number: a subject that looks machine
 * generated is itself a spam signal, which would bias the measurement toward
 * spam for every mailbox equally and make the score useless.
 */
export function seedSubject(): string {
  return "Quick question about your Q3 planning";
}

export function buildSeedMessage(input: SeedMessageInput): BuiltMessage {
  const html = [
    "<p>Hi,</p>",
    "<p>I wanted to check whether you are the right person to speak to about how your team plans capacity for the next quarter.</p>",
    "<p>If it makes sense, I can send over a short summary of what we do. If not, no problem at all.</p>",
    "<p>Best,<br>Kevin</p>",
  ].join("");

  return {
    from: input.senderEmail,
    to: input.receiverEmail,
    subject: seedSubject(),
    html,
    headers: {
      [SEED_TEST_HEADER]: input.testId,
      [SEED_SENDER_HEADER]: input.senderEmail,
    },
  };
}
