/**
 * MON-PLACEMENT-STOPPED-1111: no inbox-placement test ran from 2026-11-04 and the
 * hub stayed calm. A 30-seed hand probe of 3 mailboxes on 11-03 read as « the
 * fleet was tested 2 days ago » on Sunday 11-08, so the weekly test of 223
 * mailboxes declined; the tiles kept printing 11-03's figure in calm colours.
 */

import { describe, it, expect, vi } from "vitest";

vi.mock("../../src/db", () => ({ db: { execute: vi.fn() } }));

import { decideSeedTestDue, fleetLastTestedAt, SEED_TEST_OVERDUE_AGE_DAYS } from "../../src/lib/seed-placement/due";
import { providerFreshness, type PlacementEvaluation } from "../../src/lib/placement-alert";

const FULL_TEST = new Date("2026-11-01T15:00:00.000Z"); // Sunday, 223 mailboxes
const PROBE = new Date("2026-11-03T17:43:00.000Z"); // Tuesday, 3 mailboxes
const SUNDAY = new Date("2026-11-08T14:48:00.000Z");

const fleet = Array.from({ length: 223 }, (_, i) => `m${i}@primeforge.test`);
const prodRows = [
  ...fleet.map((sender, i) => ({ sender, lastAt: i < 3 ? PROBE : FULL_TEST })),
];

describe("fleetLastTestedAt: the fleet is as fresh as its least recently tested mailbox", () => {
  it("a probe of 3 mailboxes does not refresh the other 220 (prod 11-03)", () => {
    expect(fleetLastTestedAt(prodRows, fleet, SUNDAY)).toEqual(FULL_TEST);
  });

  it("so the weekly test is due on Sunday 11-08 (it declined as recently_tested)", () => {
    const decision = decideSeedTestDue(fleetLastTestedAt(prodRows, fleet, SUNDAY), SUNDAY);
    expect(decision.due).toBe(true);
    // Control: the old rule (newest seed of any test) declines.
    expect(decideSeedTestDue(PROBE, SUNDAY)).toMatchObject({ due: false, reason: "recently_tested" });
  });

  it("ignores a mailbox no longer in the testable pool", () => {
    const rows = [...prodRows, { sender: "gone@legacy.test", lastAt: new Date("2026-10-25T13:00:00.000Z") }];
    expect(fleetLastTestedAt(rows, fleet, SUNDAY)).toEqual(FULL_TEST);
    // Control: once in the pool it is the oldest.
    expect(fleetLastTestedAt(rows, [...fleet, "GONE@legacy.test"], SUNDAY)).toEqual(new Date("2026-10-25T13:00:00.000Z"));
  });

  it("ignores a seed older than the evidence window (else a mailbox gone uncredentialed forces a test daily)", () => {
    const rows = [...prodRows, { sender: "old@x.test", lastAt: new Date("2026-10-01T00:00:00.000Z") }];
    expect(fleetLastTestedAt(rows, [...fleet, "old@x.test"], SUNDAY)).toEqual(FULL_TEST);
  });

  it("falls back to the newest seed when nothing is inside the window, null when never tested", () => {
    const old = new Date("2026-09-01T00:00:00.000Z");
    expect(fleetLastTestedAt([{ sender: "a@x.test", lastAt: old }], ["a@x.test"], SUNDAY)).toEqual(old);
    expect(fleetLastTestedAt([], fleet, SUNDAY)).toBeNull();
  });
});

const evaluation = (day: string, provider: PlacementEvaluation["provider"], measured: boolean): PlacementEvaluation => ({
  day,
  provider,
  providerLabel: provider,
  fleet: { seeds: 100, found: 90, inbox: 80, inboxPct: 88.9 },
  allowed: { senders: 10, seeds: 50, found: 45, inbox: 40, inboxPct: 88.9 },
  mailboxesCut: 0,
  measured,
  below: false,
});

describe("providerFreshness: a provider not measured for the overdue age is red", () => {
  const evals = [
    evaluation("2026-11-01", "google", true),
    evaluation("2026-11-03", "google", true),
    evaluation("2026-11-08", "google", false), // seeds sent, too few found: not a measurement
    evaluation("2026-11-01", "microsoft_consumer", true),
  ];

  it("prod 2026-11-11: Google last measured 11-03 = 8 days = overdue; Microsoft 365 never = overdue", () => {
    const f = providerFreshness(evals, new Date("2026-11-11T09:00:00.000Z"));
    const by = Object.fromEntries(f.map((p) => [p.provider, p]));
    expect(SEED_TEST_OVERDUE_AGE_DAYS).toBe(8);
    expect(by.google).toMatchObject({ lastMeasuredDay: "2026-11-03", ageDays: 8, overdue: true });
    expect(by.microsoft_consumer).toMatchObject({ lastMeasuredDay: "2026-11-01", ageDays: 10, overdue: true });
    expect(by.microsoft).toMatchObject({ lastMeasuredDay: null, ageDays: null, overdue: true });
  });

  it("control: a weekly test 7 days ago is not overdue", () => {
    const f = providerFreshness(evals, new Date("2026-11-10T09:00:00.000Z"));
    expect(f.find((p) => p.provider === "google")).toMatchObject({ ageDays: 7, overdue: false });
  });
});
