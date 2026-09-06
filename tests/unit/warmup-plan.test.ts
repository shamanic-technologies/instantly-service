import { describe, it, expect } from "vitest";

import {
  planWarmupPairings,
  partnerCandidates,
  shouldReplyTo,
  warmupDayKey,
  WARMUP_PARTNERS_PER_DAY,
} from "../../src/lib/warmup/plan";

const MONDAY = new Date("2026-09-07T09:00:00Z");
const TUESDAY = new Date("2026-09-08T09:00:00Z");

// Ten mailboxes across ten domains — the shape the real fleet has once aliases
// are collapsed onto their real mailbox.
const FLEET = Array.from({ length: 10 }, (_, i) => `kevin@d${i}.com`);

describe("planWarmupPairings", () => {
  it("gives every mailbox the configured number of partners", () => {
    const pairings = planWarmupPairings(FLEET, MONDAY);
    for (const sender of FLEET) {
      const mine = pairings.filter((p) => p.senderEmail === sender);
      expect(mine).toHaveLength(WARMUP_PARTNERS_PER_DAY);
    }
  });

  // A pair writing to each other every morning is the shape of a warmup ring,
  // and it is the one thing a filter can detect about a mesh this small.
  it("never pairs two mailboxes reciprocally on the same day", () => {
    const pairings = planWarmupPairings(FLEET, MONDAY);
    const edges = new Set(pairings.map((p) => `${p.senderEmail}->${p.receiverEmail}`));

    for (const edge of edges) {
      const [a, b] = edge.split("->");
      expect(edges.has(`${b}->${a}`)).toBe(false);
    }
  });

  it("never writes to a mailbox on the same domain", () => {
    // Same-domain mail frequently skips filtering, so it warms nothing — and on
    // a Gandi domain the "other" mailbox is usually the same inbox behind an
    // alias, which would be a self-send.
    const sameDomain = ["a@x.com", "b@x.com", "c@x.com", "kevin@y.com", "kevin@z.com"];
    const pairings = planWarmupPairings(sameDomain, MONDAY);

    for (const p of pairings) {
      expect(p.senderEmail.split("@")[1]).not.toBe(p.receiverEmail.split("@")[1]);
    }
  });

  it("never writes to itself", () => {
    const pairings = planWarmupPairings(FLEET, MONDAY);
    expect(pairings.some((p) => p.senderEmail === p.receiverEmail)).toBe(false);
  });

  // Idempotence without a cursor: a re-run inside the same day plans the same
  // edges, so the unique index makes it a no-op.
  it("is stable within a day", () => {
    const a = planWarmupPairings(FLEET, MONDAY);
    const b = planWarmupPairings(FLEET, new Date("2026-09-07T21:30:00Z"));
    expect(b).toEqual(a);
  });

  // A fixed graph repeating daily is exactly the pattern the day key exists to
  // break.
  it("draws a different graph the next day", () => {
    const monday = planWarmupPairings(FLEET, MONDAY);
    const tuesday = planWarmupPairings(FLEET, TUESDAY);
    expect(tuesday).not.toEqual(monday);
  });

  it("degrades rather than throwing when the pool is smaller than the fan-out", () => {
    const tiny = ["a@x.com", "b@y.com"];
    const pairings = planWarmupPairings(tiny, MONDAY);
    // One edge only: the reverse is excluded the same day.
    expect(pairings).toHaveLength(1);
  });

  it("plans nothing for an empty or single-mailbox pool", () => {
    expect(planWarmupPairings([], MONDAY)).toEqual([]);
    expect(planWarmupPairings(["only@x.com"], MONDAY)).toEqual([]);
  });

  it("normalises case and blanks out of the pool", () => {
    const pairings = planWarmupPairings(
      ["  KEVIN@D0.com ", "kevin@d0.com", "", "kevin@d1.com"],
      MONDAY,
    );
    expect(pairings.every((p) => p.senderEmail === p.senderEmail.toLowerCase())).toBe(true);
    // The duplicate collapsed, so this is a two-mailbox pool.
    expect(pairings).toHaveLength(1);
  });
});

describe("partnerCandidates — the judge never plays", () => {
  // A receiver that both GRADES a sender and RESCUES its mail from spam returns
  // a verdict it has been trained to give. The delivery gate is the only
  // demotion path a mailbox has, so a drifting judge stops catching anything.
  it("excludes every measurement receiver from the mesh", () => {
    const judges = ["kevin@d0.com", "kevin@d1.com"];
    const pool = partnerCandidates(FLEET, judges);

    expect(pool).not.toContain("kevin@d0.com");
    expect(pool).not.toContain("kevin@d1.com");
    expect(pool).toHaveLength(FLEET.length - judges.length);
  });

  it("matches a judge case-insensitively", () => {
    expect(partnerCandidates(["kevin@d0.com"], ["  KEVIN@D0.COM "])).toEqual([]);
  });

  it("returns the whole fleet when nothing is grading yet", () => {
    expect(partnerCandidates(FLEET, [])).toEqual([...FLEET].sort());
  });
});

describe("shouldReplyTo", () => {
  it("is stable for the same message across runs", () => {
    // The poller re-reads a window of days; a message that flipped between runs
    // would be answered twice.
    const id = "<abc@mail.example>";
    expect(shouldReplyTo(id)).toBe(shouldReplyTo(id));
  });

  it("answers roughly the configured share of a large sample", () => {
    const sample = Array.from({ length: 3000 }, (_, i) => `<msg-${i}@x>`);
    const replied = sample.filter((id) => shouldReplyTo(id)).length;
    const share = replied / sample.length;
    // A mailbox that answers EVERYTHING is itself a pattern; one that answers
    // nothing creates no threads. Wide band — this asserts the shape, not a
    // specific hash.
    expect(share).toBeGreaterThan(0.25);
    expect(share).toBeLessThan(0.42);
  });

  it("answers everything at rate 1 and nothing at rate 0", () => {
    expect(shouldReplyTo("<x@y>", 1)).toBe(true);
    expect(shouldReplyTo("<x@y>", 0)).toBe(false);
  });
});

describe("warmupDayKey", () => {
  it("is the UTC calendar day — the unit the daily cap uses", () => {
    expect(warmupDayKey(new Date("2026-09-07T23:59:59Z"))).toBe("2026-09-07");
    expect(warmupDayKey(new Date("2026-09-08T00:00:01Z"))).toBe("2026-09-08");
  });
});
