import { describe, it, expect } from "vitest";

import {
  planWarmupPairings,
  partnerCandidates,
  shouldReplyTo,
  warmupDayKey,
  WARMUP_PARTNERS_PER_DAY,
  warmupBudgetFor,
  WARMUP_MAX_PER_DAY,
  selectSilencedSenders,
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

// ─── Warmup must never starve outreach ───────────────────────────────────────
//
// Measured on the first live run (2026-09-06): 402 of 784 planned edges were
// skipped for lack of room, and the mailboxes with least room are exactly the
// ones the age ramp is protecting. A flat partner count leaves a freshly
// promoted mailbox (cap 5) ONE send for real prospects.

describe("warmupBudgetFor", () => {
  it("scales with the mailbox at the bottom of the ramp", () => {
    expect(warmupBudgetFor(5)).toBe(1);
    expect(warmupBudgetFor(10)).toBe(3);
  });

  it("never exceeds the flat partner count on a mature mailbox", () => {
    expect(warmupBudgetFor(15)).toBe(WARMUP_PARTNERS_PER_DAY);
    expect(warmupBudgetFor(50)).toBe(WARMUP_PARTNERS_PER_DAY);
  });

  it("always leaves at least one warmup send on a mailbox that can send at all", () => {
    // Starving warmup entirely is the other failure: an idle mailbox that never
    // warms is the 15.9%-inbox cohort.
    expect(warmupBudgetFor(1)).toBe(1);
    expect(warmupBudgetFor(3)).toBe(1);
  });

  it("is zero for a mailbox with no capacity at all", () => {
    expect(warmupBudgetFor(0)).toBe(0);
    expect(warmupBudgetFor(-1)).toBe(0);
  });

  // ─── A mailbox in recovery has no outreach to protect ──────────────────────
  //
  // ⚠️ The share above pins such a mailbox at 1-4 sends a day, and since the cap
  // now ramps on measured volume that pins its CAP at the floor too — it could
  // never build the volume that earns a bigger cap, so it could never arrive in
  // production at a usable rate. Letting warmup fill the headroom is what makes
  // recovery a ramp rather than a waiting room.

  it("fills the mailbox's headroom when it carries no outreach", () => {
    expect(warmupBudgetFor(5, { fillsHeadroom: true })).toBe(5);
    expect(warmupBudgetFor(18, { fillsHeadroom: true })).toBe(18);
  });

  it(`never goes past ${WARMUP_MAX_PER_DAY}/day, however big the cap`, () => {
    // This is mail to our OWN mailboxes, which always accept it — real volume for
    // Gmail, weak reputation signal. Enough to establish a sending rate; not
    // enough to make the mesh the fleet's dominant traffic.
    expect(warmupBudgetFor(50, { fillsHeadroom: true })).toBe(WARMUP_MAX_PER_DAY);
  });

  it("subtracts real outreach, so it can never starve prospect mail", () => {
    expect(warmupBudgetFor(30, { fillsHeadroom: true, outreachToday: 25 })).toBe(5);
    expect(warmupBudgetFor(30, { fillsHeadroom: true, outreachToday: 30 })).toBe(0);
    expect(warmupBudgetFor(30, { fillsHeadroom: true, outreachToday: 40 })).toBe(0);
  });

  it("keeps the modest share for a mailbox that IS doing outreach", () => {
    // Explicitly NOT the headroom: outreach is the job, and a production mailbox
    // handing 20 sends to the mesh before the dispatcher runs would eat into it.
    expect(warmupBudgetFor(50, { fillsHeadroom: false })).toBe(WARMUP_PARTNERS_PER_DAY);
    expect(warmupBudgetFor(50)).toBe(WARMUP_PARTNERS_PER_DAY);
  });
});

describe("partnerCandidates — one address per REAL mailbox", () => {
  // The pairing hands each participant a fixed number of partners, but the quota
  // it spends belongs to the MAILBOX. Five Gandi aliases are one mailbox, so
  // pairing per address multiplies the fan-out by the alias count: 196 addresses
  // on 63 mailboxes produced 5.7 sends per mailbox on the first live run.
  const LOGINS = new Map([
    ["kevin@ga.forum", "kevin@ga.forum"],
    ["kevinl@ga.forum", "kevin@ga.forum"],
    ["klourd@ga.forum", "kevin@ga.forum"],
    ["amy@saviolabsco.com", "amy@saviolabsco.com"],
  ]);

  it("collapses a domain's aliases to a single participant", () => {
    const pool = partnerCandidates([...LOGINS.keys()], [], LOGINS);
    expect(pool).toEqual(["amy@saviolabsco.com", "kevin@ga.forum"]);
  });

  it("picks the same alias every day, so warmup arrives from one correspondent", () => {
    const a = partnerCandidates([...LOGINS.keys()], [], LOGINS);
    const b = partnerCandidates([...LOGINS.keys()].reverse(), [], LOGINS);
    expect(b).toEqual(a);
  });

  it("still excludes the judges before collapsing", () => {
    const pool = partnerCandidates([...LOGINS.keys()], ["amy@saviolabsco.com"], LOGINS);
    expect(pool).toEqual(["kevin@ga.forum"]);
  });

  it("behaves as before when no login map is supplied", () => {
    expect(partnerCandidates([...LOGINS.keys()], [])).toHaveLength(4);
  });
});

// ─── A mailbox whose relay refuses everything stops sending ──────────────────
//
// Measured 2026-09-06: `kevin@growthagency.cloud` had 20 permanent failures and
// 0 successes over seven days — its own Gandi relay answering
// `550 5.7.1 Blacklisted user` to every send. Left alone the mesh replans its
// four edges every morning and spends the day telling a provider that is
// already blocking us that we would like to send anyway.

describe("selectSilencedSenders", () => {
  it("silences a mailbox whose every send is permanently refused", () => {
    expect(
      selectSilencedSenders([{ mailbox: "kevin@ga.cloud", permanent: 20, sent: 0 }]),
    ).toEqual(new Set(["kevin@ga.cloud"]));
  });

  // A single 5xx says nothing about the mailbox — servers refuse individual
  // messages for ordinary reasons. `kevin@growthagency.ch` had 1 against 5.
  it("leaves a mailbox that mostly works alone", () => {
    expect(
      selectSilencedSenders([{ mailbox: "kevin@ga.ch", permanent: 1, sent: 5 }]),
    ).toEqual(new Set());
  });

  it("needs more than one failure even with no successes yet", () => {
    expect(
      selectSilencedSenders([{ mailbox: "new@x.com", permanent: 1, sent: 0 }]),
    ).toEqual(new Set());
    expect(
      selectSilencedSenders([{ mailbox: "new@x.com", permanent: 3, sent: 0 }]),
    ).toEqual(new Set(["new@x.com"]));
  });

  // One success is enough to prove the relay is not blocking us, whatever else
  // failed that week.
  it("never silences a mailbox that has sent at all", () => {
    expect(
      selectSilencedSenders([{ mailbox: "busy@x.com", permanent: 50, sent: 1 }]),
    ).toEqual(new Set());
  });

  it("says nothing about a mailbox with no recent warmup at all", () => {
    expect(selectSilencedSenders([])).toEqual(new Set());
  });
});
