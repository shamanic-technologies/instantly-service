/**
 * SENDER-HEALTH-1019: a mailbox the world refuses or junks stops reaching
 * prospects, on its own. Every fixture below is a prod fact of 2026-10-19.
 */

import { describe, it, expect } from "vitest";

import {
  MOSTLY_SPAM_MIN_FOUND_SEEDS,
  PLACEMENT_EVIDENCE_MIN_SEEDS,
  mayWriteFirstEmail,
  mayWriteFollowUp,
  poolPlacementTests,
  providerPlacement,
  type PlacementTest,
} from "../../src/lib/provider-placement";
import { buildPlacementByProvider } from "../../src/lib/provider-placement-read";
import { deliveryFromSilverRows } from "../../src/lib/account-lifecycle-sync";
import { selectSilencedSmtpSenders } from "../../src/lib/self-send/sender-health";
import { selectDueSteps, type AccountCapacity, type PendingSequence } from "../../src/lib/self-send/dispatch";
import { buildSenderHealthDays, senderHealthSnapshot } from "../../src/lib/sender-health-daily";
import { buildSeedPlacementDays } from "../../src/lib/seed-placement/daily";
import { ESP_GOOGLE, ESP_OUTLOOK, ESP_OUTLOOK_CONSUMER, espForMicrosoftReceiver } from "../../src/lib/seed-placement/seeds";

const DAY = 86_400_000;
const ASOF = new Date("2026-10-19T12:00:00Z");
const test = (testId: string, at: string, seedTotal: number, inboxCount: number, spamCount = seedTotal - inboxCount): PlacementTest => ({
  testId,
  testedAt: new Date(at),
  seedTotal,
  inboxCount,
  spamCount,
});

// klourd@growthagency.agency at Google, newest first (silver, prod).
const KLOURD = [
  test("seed:657a", "2026-10-18T12:28:03Z", 1, 1),
  test("seed:e66c", "2026-10-10T14:16:26Z", 10, 1),
  test("seed:862a", "2026-10-03T13:05:35Z", 10, 1),
  test("seed:4835", "2026-09-19T12:18:27Z", 2, 0),
];

describe("evidence pooled until 30 seeds (one test, small or full, cannot overturn history)", () => {
  it("klourd@growthagency.agency: 1/1 on 10-18 pools with 1/10 and 1/10 = 3/21, not 100 %", () => {
    const pooled = poolPlacementTests(KLOURD, ASOF)!;
    expect(pooled.testIds).toEqual(["seed:657a", "seed:e66c", "seed:862a"]);
    expect([pooled.inboxCount, pooled.seedTotal]).toEqual([3, 21]);
    expect(pooled.testedAt.toISOString()).toBe("2026-10-18T12:28:03.000Z");
  });

  it("the lifecycle gate reads the pooled figure: klourd is below the bar (it was promoted on 1/1)", () => {
    const rows = KLOURD.map((t) => ({ accountEmail: "klourd@growthagency.agency", testId: t.testId, inboxCount: t.inboxCount, spamCount: t.spamCount, seedTotal: t.seedTotal, testedAt: t.testedAt }));
    const d = deliveryFromSilverRows(rows, ASOF).get("klourd@growthagency.agency")!;
    expect(d).toMatchObject({ inboxCount: 3, seedTotal: 21, deliveryPct: 14, atBar: false, testedAt: "2026-10-18T12:28:03.000Z" });
  });

  it("kevin.lourd@growthagency.news, 2026-10-28: one 10/10 Sunday after 5/10, 4/6, 5/10 stays below the bar (it was promoted on 10/10)", () => {
    const asOf = new Date("2026-10-28T12:00:00Z");
    const news = [
      test("seed:969f", "2026-10-25T13:00:00Z", 10, 10),
      test("seed:ad19", "2026-10-19T13:00:00Z", 10, 5),
      test("seed:8179", "2026-10-19T11:00:00Z", 6, 4, 2),
      test("seed:657a", "2026-10-18T12:00:00Z", 10, 5),
      test("seed:e66c", "2026-10-10T14:00:00Z", 10, 0),
    ];
    const rows = news.map((t) => ({ accountEmail: "kevin.lourd@growthagency.news", testId: t.testId, inboxCount: t.inboxCount, spamCount: t.spamCount, seedTotal: t.seedTotal, testedAt: t.testedAt }));
    const d = deliveryFromSilverRows(rows, asOf).get("kevin.lourd@growthagency.news")!;
    expect(d).toMatchObject({ inboxCount: 24, seedTotal: 36, atBar: false });
  });

  it("three good tests in a row pass: 10/10, 10/10, 9/10 = 29/30", () => {
    expect(PLACEMENT_EVIDENCE_MIN_SEEDS).toBe(30);
    const rows = [
      test("a", "2026-10-18T00:00:00Z", 10, 10),
      test("b", "2026-10-11T00:00:00Z", 10, 10),
      test("c", "2026-10-05T00:00:00Z", 10, 9),
    ].map((t) => ({ accountEmail: "x@y.com", testId: t.testId, inboxCount: t.inboxCount, spamCount: t.spamCount, seedTotal: t.seedTotal, testedAt: t.testedAt }));
    expect(deliveryFromSilverRows(rows, ASOF).get("x@y.com")).toMatchObject({ inboxCount: 29, seedTotal: 30, atBar: true });
  });

  it("a newest test holding 30 seeds stands alone; a small test with nothing older still grades (sample size is not a gate)", () => {
    expect(poolPlacementTests([test("a", "2026-10-18T00:00:00Z", 30, 30), test("b", "2026-10-10T00:00:00Z", 10, 0)], ASOF)!.testIds).toEqual(["a"]);
    const alone = poolPlacementTests([test("a", "2026-10-18T00:00:00Z", 1, 1)], ASOF)!;
    expect(alone.seedTotal).toBe(1);
    const rows = [{ accountEmail: "x@y.com", testId: "a", inboxCount: 1, spamCount: 0, seedTotal: 1, testedAt: new Date("2026-10-18T00:00:00Z") }];
    expect(deliveryFromSilverRows(rows, ASOF).get("x@y.com")!.atBar).toBe(true);
  });

  it("an older test past the 16-day evidence limit is never pooled in", () => {
    const pooled = poolPlacementTests([test("a", "2026-10-18T00:00:00Z", 1, 1), test("old", "2026-09-01T00:00:00Z", 10, 0)], ASOF)!;
    expect(pooled.testIds).toEqual(["a"]);
  });

  it("the gate sums every non-Microsoft ESP row of one test", () => {
    const rows = [
      { accountEmail: "a@b.com", testId: "t", inboxCount: 9, spamCount: 1, seedTotal: 10, testedAt: new Date("2026-10-18T00:00:00Z") },
      { accountEmail: "a@b.com", testId: "t", inboxCount: 0, spamCount: 0, seedTotal: 3, testedAt: new Date("2026-10-18T00:00:00Z") },
    ];
    expect(deliveryFromSilverRows(rows, ASOF).get("a@b.com")).toMatchObject({ inboxCount: 9, seedTotal: 13, atBar: false });
  });
});

describe("mostly spam at a provider", () => {
  const p = (inbox: number, spam: number) =>
    providerPlacement({ inboxCount: inbox, spamCount: spam, seedTotal: inbox + spam, testedAt: new Date(ASOF.getTime() - DAY), testIds: ["t"] }, ASOF);

  it("kevin@growdistribute.com at Google, 2/10 on 10-18 and 10-19: mostly spam, nothing to Google prospects", () => {
    expect(p(2, 8)).toEqual({ verdict: "junk", mostlySpam: true });
    expect(mayWriteFollowUp(p(2, 8))).toBe(false);
    expect(mayWriteFirstEmail(p(2, 8), true)).toBe(false);
  });

  it("half or better in Inbox is junk for a FIRST email but follow-ups keep going", () => {
    expect(p(5, 5)).toEqual({ verdict: "junk", mostlySpam: false });
    expect(mayWriteFollowUp(p(5, 5))).toBe(true);
    expect(mayWriteFirstEmail(p(5, 5), false)).toBe(false);
  });

  it(`never on fewer than ${MOSTLY_SPAM_MIN_FOUND_SEEDS} found seeds (one Junk seed is no proof)`, () => {
    expect(p(0, 2).mostlySpam).toBe(false);
    expect(p(0, 2).verdict).toBe("junk");
    expect(mayWriteFollowUp(undefined)).toBe(true);
  });
});

describe("Microsoft 365 and Outlook.com are measured apart", () => {
  it("receivers grade under their own code: consumer = 4, business = 2, unknown kind = not a receiver", () => {
    expect(espForMicrosoftReceiver("consumer")).toBe(ESP_OUTLOOK_CONSUMER);
    expect(espForMicrosoftReceiver("business")).toBe(ESP_OUTLOOK);
    expect(espForMicrosoftReceiver(null)).toBeNull();
  });

  it("fiona-style sender: Inbox at Microsoft 365, Junk at Outlook.com: two verdicts, not one 50 % Junk", () => {
    const at = new Date("2026-10-19T11:22:00Z");
    const t = (testId: string, inbox: number, spam: number) => ({ testId, testedAt: at, seedTotal: inbox + spam, inboxCount: inbox, spamCount: spam });
    const out = buildPlacementByProvider(
      [
        { accountEmail: "Alina@helixmeshio.com", recipientEsp: ESP_OUTLOOK, test: t("seed:8179", 1, 0) },
        { accountEmail: "alina@helixmeshio.com", recipientEsp: ESP_OUTLOOK_CONSUMER, test: t("seed:8179", 0, 1) },
        { accountEmail: "alina@helixmeshio.com", recipientEsp: ESP_GOOGLE, test: t("seed:8179", 10, 0) },
        { accountEmail: "alina@helixmeshio.com", recipientEsp: 999, test: t("x", 0, 9) },
      ],
      ASOF,
    ).get("alina@helixmeshio.com")!;
    expect(out.microsoft).toMatchObject({ verdict: "inbox", mostlySpam: false, inboxPctOfFound: 100 });
    expect(out.microsoft_consumer).toMatchObject({ verdict: "junk", mostlySpam: false, inboxPctOfFound: 0 });
    expect(out.google).toMatchObject({ verdict: "inbox" });
    expect(Object.keys(out).sort()).toEqual(["google", "microsoft", "microsoft_consumer"]);
  });

  it("the daily series keeps Microsoft 365 and Outlook.com apart", () => {
    const [d] = buildSeedPlacementDays(
      [
        { day: "2026-10-19", recipientEsp: ESP_OUTLOOK, senderVendor: "primeforge", seeds: 41, inbox: 41, spam: 0, missing: 0, inboxFocused: 41, inboxOther: 0 },
        { day: "2026-10-19", recipientEsp: ESP_OUTLOOK_CONSUMER, senderVendor: "primeforge", seeds: 105, inbox: 17, spam: 88, missing: 0, inboxFocused: 17, inboxOther: 0 },
      ],
      "2026-10-19",
      "2026-10-19",
    );
    expect(d.microsoft).toMatchObject({ seeds: 41, inboxPct: 100 });
    expect(d.microsoftConsumer).toMatchObject({ seeds: 105, inboxPct: 16.2 });
    expect(d.byVendor[0].microsoftConsumer.seeds).toBe(105);
  });
});

describe("dispatcher: no email to a provider that puts the mailbox mostly in spam", () => {
  // Monday 15:00 UTC = 10:00 in the default zone: inside the send window.
  const NOW = new Date("2026-08-17T15:00:00Z");
  const SPAM = "kevin@growdistribute.com";
  const GOOD = "lyla@alphaflowco.com";
  const spam = { verdict: "junk" as const, mostlySpam: true };
  const inbox = { verdict: "inbox" as const, mostlySpam: false };
  const cap = (accountEmail: string, over: Partial<AccountCapacity> = {}): AccountCapacity => ({
    accountEmail,
    mailbox: accountEmail,
    cap: 50,
    recentSustainedDaily: 50,
    sentToday: 0,
    adoptsFirstEmails: true,
    ...over,
  });
  const seq = (id: string, accountEmail: string, over: Partial<PendingSequence> = {}): PendingSequence => ({
    instantlyCampaignId: id,
    leadEmail: `${id}@prospect.com`,
    accountEmail,
    provisionedSteps: [1, 2, 3],
    lastSentStep: null,
    lastSentAt: null,
    stepDelays: [3, 7, null],
    ...over,
  });
  const followUp = { provisionedSteps: [2, 3], lastSentStep: 1, lastSentAt: new Date(NOW.getTime() - 5 * DAY) };

  it("a follow-up to a Google prospect waits on its thread, never moved, never sent from the spam mailbox", () => {
    const out = selectDueSteps(
      [seq("fu", SPAM, { recipientProvider: "google", ...followUp })],
      [cap(SPAM, { adoptsFirstEmails: false, placement: { google: spam } }), cap(GOOD, { placement: { google: inbox } })],
      NOW,
    );
    expect(out.selected).toEqual([]);
    expect(out.heldMostlySpam).toBe(1);
    expect(out.rehomed).toBe(0);
  });

  it("control: the same mailbox's follow-up to a Microsoft 365 prospect goes out (spam at Google only)", () => {
    const out = selectDueSteps(
      [seq("fu-ms", SPAM, { recipientProvider: "microsoft", ...followUp })],
      [cap(SPAM, { adoptsFirstEmails: false, placement: { google: spam, microsoft: inbox } })],
      NOW,
    );
    expect(out.selected).toEqual([expect.objectContaining({ accountEmail: SPAM, step: 2 })]);
    expect(out.heldMostlySpam).toBe(0);
  });

  it("a FIRST email queued on a mailbox since measured spam at Google is rehomed to one Google puts in Inbox", () => {
    const out = selectDueSteps(
      [seq("first", SPAM, { recipientProvider: "google" })],
      [cap(SPAM, { adoptsFirstEmails: false, placement: { google: spam } }), cap(GOOD, { placement: { google: inbox } })],
      NOW,
    );
    expect(out.heldPlacement).toBe(1);
    expect(out.selected).toEqual([expect.objectContaining({ accountEmail: GOOD, rehomedFrom: SPAM })]);
  });

  it("an Outlook.com prospect reads the consumer verdict, a Microsoft 365 one the business verdict", () => {
    const both = { microsoft: inbox, microsoft_consumer: { verdict: "junk" as const, mostlySpam: false } };
    const consumerOk = { microsoft: inbox, microsoft_consumer: inbox };
    const out = selectDueSteps(
      [seq("m365", GOOD, { recipientProvider: "microsoft" }), seq("olc", GOOD, { recipientProvider: "microsoft_consumer" })],
      [cap(GOOD, { placement: both }), cap("other@b.com", { placement: consumerOk })],
      NOW,
    );
    expect(out.selected).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ instantlyCampaignId: "m365", accountEmail: GOOD }),
        expect.objectContaining({ instantlyCampaignId: "olc", accountEmail: "other@b.com", rehomedFrom: GOOD }),
      ]),
    );
  });

  it("a prospect behind a host we do not measure (gateway, unknown) is not gated", () => {
    const out = selectDueSteps(
      [seq("gw", SPAM, { recipientProvider: null, ...followUp })],
      [cap(SPAM, { placement: { google: spam } })],
      NOW,
    );
    expect(out.selected).toHaveLength(1);
  });
});

describe("refused by the relay: every mail the mailbox sends counts", () => {
  const firstAt = new Date("2026-10-18T12:46:40Z");

  it("kevin@growthagency.cloud: seed refusals '550 5.7.1 Blacklisted user', nothing accepted = silenced", () => {
    const silenced = selectSilencedSmtpSenders([
      {
        mailbox: "kevin@growthagency.cloud",
        lastSuccessAt: null,
        failures: [
          {
            response: "550 5.7.1 <k.lourd@boostdistribute.com>: Recipient address rejected: Blacklisted user: kevin@growthagency.cloud",
            responseCode: 550,
            count: 23,
            firstAt,
          },
        ],
      },
    ]);
    expect([...silenced]).toEqual(["kevin@growthagency.cloud"]);
  });

  it("bailey@fuseconnectio.com: warmup '534-5.7.9 Please log in with your web browser' = silenced", () => {
    const silenced = selectSilencedSmtpSenders([
      {
        mailbox: "bailey@fuseconnectio.com",
        lastSuccessAt: null,
        failures: [
          {
            response: "SMTP dispatch from bailey@fuseconnectio.com to zoey@acuteclusterco.com failed (permanent): 534-5.7.9 Please log in with your web browser and then try again.",
            responseCode: 534,
            count: 38,
            firstAt,
          },
        ],
      },
    ]);
    expect(silenced.has("bailey@fuseconnectio.com")).toBe(true);
  });

  it("control: a seed refused because OUR receiver is gone (5.1.1) never silences the sender", () => {
    const silenced = selectSilencedSmtpSenders([
      {
        mailbox: "a@b.com",
        lastSuccessAt: null,
        failures: [{ response: "550 5.1.1 The email account that you tried to reach does not exist", responseCode: 550, count: 9, firstAt }],
      },
    ]);
    expect(silenced.size).toBe(0);
  });

  it("control: a relay throttle of the box IP (450 4.7.1, transient) is not a refusal record", () => {
    // Transient outcomes are not read by the silence rule at all (only `permanent` rows are loaded).
    const silenced = selectSilencedSmtpSenders([{ mailbox: "klourd@growthagency.cloud", lastSuccessAt: firstAt, failures: [] }]);
    expect(silenced.size).toBe(0);
  });
});

describe("stored daily series", () => {
  it("snapshot lists refused mailboxes and mostly-spam addresses per provider, deduped and sorted", () => {
    const s = senderHealthSnapshot({
      asOf: ASOF,
      refusedMailboxes: new Set(["kevin@growthagency.cloud"]),
      capacities: [
        { accountEmail: "Kevin@GrowDistribute.com", mailbox: "m1", cap: 50, recentSustainedDaily: 0, sentToday: 0, placement: { google: { verdict: "junk", mostlySpam: true } } },
        { accountEmail: "lyla@alphaflowco.com", mailbox: "m2", cap: 50, recentSustainedDaily: 0, sentToday: 0, placement: { google: { verdict: "inbox", mostlySpam: false } } },
      ],
      selection: { heldPlacement: 3, heldMostlySpam: 7, skippedSilenced: 1 },
    });
    expect(s).toEqual({
      day: "2026-10-19",
      refused: ["kevin@growthagency.cloud"],
      mostlySpam: { google: ["kevin@growdistribute.com"], microsoft: [], microsoft_consumer: [] },
      heldPlacement: 3,
      heldMostlySpam: 7,
      skippedRefused: 1,
    });
  });

  it("every day of the window is emitted; a day with no tick is null, never 0", () => {
    const days = buildSenderHealthDays(
      [{ day: "2026-10-19", refused: ["a"], mostlySpam: { google: ["x", "y"] }, heldPlacement: 2, heldMostlySpam: 5, skippedRefused: 0 }],
      "2026-10-18",
      "2026-10-19",
    );
    expect(days[0]).toEqual({ day: "2026-10-18", refusedMailboxes: null, mostlySpamMailboxes: null, heldPlacement: null, heldMostlySpam: null, skippedRefused: null });
    expect(days[1]).toMatchObject({ refusedMailboxes: 1, mostlySpamMailboxes: { google: 2, microsoft: 0, microsoft_consumer: 0 }, heldMostlySpam: 5 });
  });
});
