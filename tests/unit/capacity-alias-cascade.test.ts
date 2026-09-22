import { describe, it, expect, vi } from "vitest";

// Both functions under test are pure; the mock only stops the module graph from
// opening a pool at import time.
vi.mock("../../src/db", () => ({
  db: { execute: vi.fn(), select: vi.fn(), insert: vi.fn(), update: vi.fn() },
}));

import { aggregateCapacityByMailbox } from "../../src/lib/account-sending-stats";
import { pickSequentialFillAccount, type FillOrderAccount } from "../../src/lib/send-lead";

/**
 * The two halves of the fix, joined: the snapshot folds a mailbox's aliases into
 * one set of figures, and the selector — unchanged — then cascades off the whole
 * mailbox instead of off each alias in turn.
 *
 * Real prod shape, 2026-09-22: `eric@salesmolt.com` is ONE Gandi relay login with
 * five aliases, two of them `in_production` at 50/day. `accountFillOrder` sorts a
 * domain's aliases ADJACENT (vendor, then domain rank, then age), so a per-address
 * reading saturated the first and walked straight onto its sibling — offering the
 * login 100/day against the ~50 it accepts.
 */
describe("alias cascade — a mailbox's cap is offered ONCE, not once per alias", () => {
  const asOf = new Date("2026-09-22T09:00:00.000Z");
  const ASOF_KEY = "2026-09-22";
  const LOGIN = "eric@salesmolt.com";

  const account = (email: string, domainFillRank: number): FillOrderAccount => ({
    email,
    warmup_status: 1,
    status: 1,
    daily_limit: 50,
    infraProvider: "gandi",
    domainFillRank,
    sendTransport: "smtp",
    // Already at volume, so the ramp is saturated and `daily_limit` is the cap —
    // these cases are about the alias grouping, not about the ramp.
    timestamp_created: "2026-01-01T00:00:00.000Z",
  });

  const ALIAS_A = account("kevinl@salesmolt.com", 0);
  const ALIAS_B = account("klourd@salesmolt.com", 0);
  const NEXT_DOMAIN = account("kevin@othermolt.com", 1);
  const accounts = [ALIAS_A, ALIAS_B, NEXT_DOMAIN];

  /** The snapshot as `fetchAccountCapacity` builds it, for a given alias map. */
  const snapshot = (aliasMap: Map<string, string>, sentToday: Map<string, number>) =>
    aggregateCapacityByMailbox(
      accounts.map((a) => a.email),
      sentToday,
      new Map(),
      // 100/day sustained ⇒ the ramp is saturated everywhere in this fixture.
      new Map(
        accounts.map((a) => [
          a.email,
          new Map([
            ["2026-09-18", 100],
            ["2026-09-19", 100],
          ]),
        ]),
      ),
      aliasMap,
    );

  const aliasMap = new Map([
    [ALIAS_A.email, LOGIN],
    [ALIAS_B.email, LOGIN],
  ]);

  it("cascades PAST a sibling alias to the next domain once the login is full", () => {
    // 50 of the login's 50 already spent, all of it booked against alias A.
    const caps = snapshot(aliasMap, new Map([[ALIAS_A.email, 50]]));
    expect(pickSequentialFillAccount(accounts, caps, asOf, [ASOF_KEY]).email).toBe(
      NEXT_DOMAIN.email,
    );
  });

  it("is the bug when the aliases are read per ADDRESS (control)", () => {
    // Same fixture, empty alias map ⇒ every address is its own mailbox ⇒ alias B
    // reports an untouched 50/day and takes the lead. This is what prod did.
    const caps = snapshot(new Map(), new Map([[ALIAS_A.email, 50]]));
    expect(pickSequentialFillAccount(accounts, caps, asOf, [ASOF_KEY]).email).toBe(
      ALIAS_B.email,
    );
  });

  it("still offers the head alias its mailbox's remaining room", () => {
    // Only 10 spent ⇒ the login has 40 left ⇒ the head of the order keeps it.
    const caps = snapshot(aliasMap, new Map([[ALIAS_A.email, 10]]));
    expect(pickSequentialFillAccount(accounts, caps, asOf, [ASOF_KEY]).email).toBe(
      ALIAS_A.email,
    );
  });

  it("leaves a 1:1 fleet byte-identical (Primeforge / Instantly-DFY)", () => {
    // No alias shares a login, so folding is a no-op and selection is exactly
    // what it was before this change: head first, cascade only when IT is full.
    const solo = [
      account("one@primeforge.com", 0),
      account("two@primeforge.com", 1),
    ];
    const caps = aggregateCapacityByMailbox(
      solo.map((a) => a.email),
      new Map([["one@primeforge.com", 50]]),
      new Map(),
      new Map(
        solo.map((a) => [a.email, new Map([["2026-09-18", 100], ["2026-09-19", 100]])]),
      ),
      new Map(),
    );
    expect(pickSequentialFillAccount(solo, caps, asOf, [ASOF_KEY]).email).toBe(
      "two@primeforge.com",
    );
  });
});
