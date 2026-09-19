import { describe, it, expect } from "vitest";

import {
  selectDueSteps,
  type AccountCapacity,
  type PendingSequence,
} from "../../src/lib/self-send/dispatch";
import { selectSilencedSmtpSenders } from "../../src/lib/self-send/sender-health";

/** A Friday inside the US business window, so the calendar gates are open. */
const ASOF = new Date("2026-09-18T15:00:00Z");

function sequence(over: Partial<PendingSequence> = {}): PendingSequence {
  return {
    instantlyCampaignId: "self:a",
    leadEmail: "lead@x.com",
    accountEmail: "kevin@live.com",
    provisionedSteps: [1],
    lastSentStep: null,
    lastSentAt: null,
    stepDelays: [3, 7],
    timezone: "America/Chicago",
    ...over,
  };
}

function capacity(over: Partial<AccountCapacity> = {}): AccountCapacity {
  return {
    accountEmail: "kevin@live.com",
    mailbox: "kevin@live.com",
    cap: 50,
    recentSustainedDaily: 50,
    sentToday: 0,
    ...over,
  };
}

/**
 * A mailbox the relay has stopped accepting is retried on every run, forever.
 *
 * Prod 2026-09-16 → 09-18: two Mailforge mailboxes were deprovisioned at the
 * vendor while 56 of their steps were still provisioned. Every SMTP attempt
 * came back `535 5.7.8 authentication failed`, which is sender-side, so the
 * hold correctly stayed provisioned — and the step was re-selected next run.
 * 4,210 attempts at ~13s each; whole hours of the dispatcher produced 240
 * failures and zero emails.
 */
describe("a mailbox whose relay refuses everything stops being tried", () => {
  it("skips its steps and counts them apart from a missing capacity row", () => {
    const result = selectDueSteps(
      [
        sequence({ instantlyCampaignId: "self:dead", accountEmail: "dead@gone.com" }),
        sequence({ instantlyCampaignId: "self:live" }),
      ],
      [
        capacity({ accountEmail: "dead@gone.com", mailbox: "dead@gone.com" }),
        capacity(),
      ],
      ASOF,
      new Set(["dead@gone.com"]),
    );

    expect(result.selected.map((s) => s.instantlyCampaignId)).toEqual(["self:live"]);
    expect(result.skippedSilenced).toBe(1);
    // It HAS a capacity row — calling it "no credential" would send whoever
    // reads the summary looking for a credential that is present.
    expect(result.blockedNoCapacityRow).toBe(0);
  });

  it("silences the MAILBOX, so every alias of it stops too", () => {
    const result = selectDueSteps(
      [
        sequence({ instantlyCampaignId: "self:a", accountEmail: "k@d.com" }),
        sequence({ instantlyCampaignId: "self:b", accountEmail: "k.l@d.com" }),
      ],
      [
        capacity({ accountEmail: "k@d.com", mailbox: "k@d.com" }),
        capacity({ accountEmail: "k.l@d.com", mailbox: "k@d.com" }),
      ],
      ASOF,
      new Set(["k@d.com"]),
    );

    expect(result.selected).toEqual([]);
    expect(result.skippedSilenced).toBe(2);
  });

  it("is byte-identical to the old behaviour when nothing is silenced", () => {
    const sequences = [sequence()];
    const capacities = [capacity()];

    expect(selectDueSteps(sequences, capacities, ASOF, new Set())).toEqual(
      selectDueSteps(sequences, capacities, ASOF),
    );
  });
});

/**
 * ⚠️ ONLY A SENDER-SIDE REFUSAL SILENCES A MAILBOX.
 *
 * `outcome = 'permanent'` on this table also covers a dead PROSPECT address —
 * a prod audit found 9 of 12 deactivated accounts refused over `Recipient
 * address rejected: Domain not found`, a list-hygiene problem misfiled as a
 * sender problem. Silencing a working mailbox because three prospects are
 * unreachable is that mistake, automated.
 */
describe("selectSilencedSmtpSenders", () => {
  it("silences on repeated sender-side refusals with no successes", () => {
    expect(
      selectSilencedSmtpSenders([
        {
          mailbox: "dead@gone.com",
          sent: 0,
          failures: [
            { response: "535 5.7.8 Error: authentication failed", responseCode: 535, count: 4210 },
          ],
        },
      ]),
    ).toEqual(new Set(["dead@gone.com"]));
  });

  it("does NOT silence a mailbox refused over dead PROSPECT addresses", () => {
    expect(
      selectSilencedSmtpSenders([
        {
          mailbox: "fine@live.com",
          sent: 0,
          failures: [
            {
              response: "550 5.1.1 <a@dead.example>: Recipient address rejected: User unknown",
              responseCode: 550,
              count: 40,
            },
          ],
        },
      ]),
    ).toEqual(new Set());
  });

  it("does NOT silence a mailbox that is also sending", () => {
    expect(
      selectSilencedSmtpSenders([
        {
          mailbox: "busy@live.com",
          sent: 31,
          failures: [
            { response: "535 5.7.8 authentication failed", responseCode: 535, count: 9 },
          ],
        },
      ]),
    ).toEqual(new Set());
  });

  it("needs more than one bad afternoon", () => {
    expect(
      selectSilencedSmtpSenders([
        {
          mailbox: "blip@live.com",
          sent: 0,
          failures: [
            { response: "535 5.7.8 authentication failed", responseCode: 535, count: 2 },
          ],
        },
      ]),
    ).toEqual(new Set());
  });

  it("reads a multiline Gmail daily-limit refusal as sender-side", () => {
    expect(
      selectSilencedSmtpSenders([
        {
          mailbox: "capped@live.com",
          sent: 0,
          failures: [
            {
              response: "550-5.4.5 Daily user sending limit exceeded",
              responseCode: 550,
              count: 12,
            },
          ],
        },
      ]),
    ).toEqual(new Set(["capped@live.com"]));
  });
});
