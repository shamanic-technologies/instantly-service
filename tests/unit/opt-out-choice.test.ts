import { describe, it, expect, vi } from "vitest";

vi.mock("../../src/db", () => ({ db: { execute: vi.fn() } }));

import {
  OPT_OUT_MIN_FOUND_SEEDS,
  chooseOptOutVariant,
  finishCounts,
  foldVariantCounts,
  type VariantCounts,
} from "../../src/lib/seed-placement/opt-out-choice";
import { OPT_OUT_VARIANTS, type OptOutVariant } from "../../src/lib/self-send/opt-out-variant";

/** One variant with `found` seeds at Google of which `inboxPct` % in Inbox. */
function variant(v: OptOutVariant, found: number, inboxPct: number, ms?: { found: number; inboxPct: number }): VariantCounts {
  const counts = (f: number, p: number) => {
    const inbox = Math.round((f * p) / 100);
    return finishCounts({ seeds: f, inbox, spam: f - inbox });
  };
  return {
    variant: v,
    google: counts(found, inboxPct),
    microsoft: ms ? counts(ms.found, ms.inboxPct) : counts(0, 0),
    microsoftConsumer: counts(0, 0),
  };
}

describe("chooseOptOutVariant (UNSUB-3WAY-1103)", () => {
  const N = OPT_OUT_MIN_FOUND_SEEDS;

  it("keeps today's tracker link until EVERY variant has the sample at Google", () => {
    const d = chooseOptOutVariant([
      variant("tracker_link", N, 60),
      variant("mailbox_only", N, 95),
      variant("none", N - 1, 99),
    ]);
    expect(d).toMatchObject({ variant: "tracker_link", reason: "sample_not_reached", scoredProviders: [] });
  });

  it("keeps it when a variant has no row at all", () => {
    expect(chooseOptOutVariant([variant("mailbox_only", N, 95)]).variant).toBe("tracker_link");
  });

  it("switches AWAY from the tracker link when another variant lands better", () => {
    const d = chooseOptOutVariant([
      variant("tracker_link", N, 70),
      variant("mailbox_only", N, 88),
      variant("none", N, 82),
    ]);
    expect(d).toMatchObject({ variant: "mailbox_only", reason: "measured", scoredProviders: ["google"] });
    expect(d.scores).toEqual({ tracker_link: 70, mailbox_only: 88, none: 82 });
  });

  it("KEEPS the tracker link when it is measured best (the other direction)", () => {
    const d = chooseOptOutVariant([
      variant("tracker_link", N, 91),
      variant("mailbox_only", N, 80),
      variant("none", N, 85),
    ]);
    expect(d).toMatchObject({ variant: "tracker_link", reason: "measured" });
  });

  it("picks `none` when no header lands best", () => {
    expect(
      chooseOptOutVariant([variant("tracker_link", N, 70), variant("mailbox_only", N, 75), variant("none", N, 90)]).variant,
    ).toBe("none");
  });

  it("a tie goes to the variant with no outside domain", () => {
    expect(
      chooseOptOutVariant([variant("tracker_link", N, 80), variant("mailbox_only", N, 80), variant("none", N, 80)]).variant,
    ).toBe("mailbox_only");
  });

  it("scores Microsoft 365 too once every variant has the sample there", () => {
    const d = chooseOptOutVariant([
      variant("tracker_link", N, 90, { found: N, inboxPct: 40 }),
      variant("mailbox_only", N, 85, { found: N, inboxPct: 90 }),
      variant("none", N, 86, { found: N, inboxPct: 60 }),
    ]);
    expect(d).toMatchObject({ variant: "mailbox_only", scoredProviders: ["google", "microsoft"] });
  });

  it("ignores Microsoft 365 while it lacks the sample (seeds sent there but never read)", () => {
    const d = chooseOptOutVariant([
      variant("tracker_link", N, 90, { found: 10, inboxPct: 0 }),
      variant("mailbox_only", N, 85, { found: 10, inboxPct: 100 }),
      variant("none", N, 86, { found: 10, inboxPct: 100 }),
    ]);
    expect(d).toMatchObject({ variant: "tracker_link", scoredProviders: ["google"] });
  });

  it("rates on FOUND seeds: an unread seed is not a placement", () => {
    expect(finishCounts({ seeds: 227, inbox: 0, spam: 0 })).toEqual({ seeds: 227, inbox: 0, spam: 0, found: 0, inboxPct: null });
  });
});

describe("foldVariantCounts", () => {
  it("sums stored rows per variant and provider, every variant emitted", () => {
    const out = foldVariantCounts([
      { variant: "none", recipientEsp: 1, seeds: 10, inbox: 6, spam: 3 },
      { variant: "none", recipientEsp: 1, seeds: 10, inbox: 4, spam: 5 },
      { variant: "none", recipientEsp: 2, seeds: 5, inbox: 5, spam: 0 },
      { variant: "tracker_link", recipientEsp: 4, seeds: 5, inbox: 0, spam: 5 },
    ]);
    expect(out.map((v) => v.variant)).toEqual([...OPT_OUT_VARIANTS]);
    const none = out.find((v) => v.variant === "none")!;
    expect(none.google).toEqual({ seeds: 20, inbox: 10, spam: 8, found: 18, inboxPct: 55.6 });
    expect(none.microsoft.inbox).toBe(5);
    expect(out.find((v) => v.variant === "tracker_link")!.microsoftConsumer.spam).toBe(5);
    expect(out.find((v) => v.variant === "mailbox_only")!.google.found).toBe(0);
  });
});
