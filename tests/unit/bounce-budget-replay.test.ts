import { describe, it, expect } from "vitest";

import {
  BOUNCE_BUDGET_WINDOWS_HOURS,
  BounceBudget,
  HARD_CEILING_WINDOW_HOURS,
  senderDomain,
  type WindowCounts,
} from "../../src/lib/bounce-budget";
import fixture from "../helpers/bounce-budget-2026-10-14.json";

/**
 * Replay of 2026-10-14 (the day catch-all sending opened) on the four domains
 * that took the bounce burst. Fixture = every REAL `email_sent` from those
 * domains 2026-10-07 -> 2026-10-15 (prod, read-only): [sentAt, mailbox, step,
 * verdict our verifier held at send | null, bouncedAt | null]. No prospect
 * address is in it. No email is sent: the replay only asks the rule.
 *
 * Mechanism replayed as the dispatcher runs it: one selection per 10-minute
 * tick, history = what had happened before the tick (sends, and bounces whose
 * DSN had landed), then the tick spends the budget in memory email by email.
 * A first email the rule holds is removed from the counterfactual (it would
 * have waited or gone to another domain), together with its bounce.
 */
type Row = [string, string, number, string | null, string | null];
const rows = (fixture as Row[]).map(([sentAt, account, step, verdict, bouncedAt]) => ({
  sentAt: new Date(sentAt),
  account,
  domain: senderDomain(account),
  step,
  verdict,
  bouncedAt: bouncedAt ? new Date(bouncedAt) : null,
}));

const REPLAY_FROM = new Date("2026-10-14T00:00:00Z");
// Before 2026-10-14 there was no verifier gate: those sends were the valid-only
// era and carry no verdict. They are history, never decisions.
const WINDOWS = [...new Set<number>([...BOUNCE_BUDGET_WINDOWS_HOURS, HARD_CEILING_WINDOW_HOURS])];
const HOUR = 3_600_000;

function replay() {
  const admitted: { sentAt: Date; domain: string; bouncedAt: Date | null }[] = [];
  const out = new Map<string, { sent: number; bounced: number; held: number; heldBounces: number; firstHeldAt?: Date }>();
  let tick: number | null = null;
  let budget: BounceBudget | null = null;

  for (const row of rows) {
    if (row.sentAt < REPLAY_FROM) {
      admitted.push(row);
      continue;
    }
    const tickStart = Math.floor(row.sentAt.getTime() / (10 * 60_000)) * 10 * 60_000;
    if (tickStart !== tick) {
      tick = tickStart;
      const fleet = new Map<string, Map<number, WindowCounts>>();
      for (const a of admitted) {
        let w = fleet.get(a.domain);
        if (!w) {
          w = new Map(WINDOWS.map((h) => [h, { sends: 0, bounces: 0 }]));
          fleet.set(a.domain, w);
        }
        for (const h of WINDOWS) {
          const lo = tickStart - h * HOUR;
          const c = w.get(h)!;
          if (a.sentAt.getTime() >= lo && a.sentAt.getTime() < tickStart) c.sends += 1;
          if (a.bouncedAt && a.bouncedAt.getTime() >= lo && a.bouncedAt.getTime() < tickStart) c.bounces += 1;
        }
      }
      budget = new BounceBudget(fleet);
    }

    const risky = row.verdict !== "valid";
    const key = `${row.domain}|${risky ? "risky" : "valid"}`;
    const stat = out.get(key) ?? { sent: 0, bounced: 0, held: 0, heldBounces: 0 };
    out.set(key, stat);
    const firstEmail = row.step === 1;
    if (firstEmail && budget!.refusal(row.account, row.verdict) !== null) {
      stat.held += 1;
      if (row.bouncedAt) stat.heldBounces += 1;
      stat.firstHeldAt ??= row.sentAt;
      continue;
    }
    budget!.spend(row.account, row.verdict, firstEmail);
    admitted.push(row);
    stat.sent += 1;
    if (row.bouncedAt) stat.bounced += 1;
  }
  return out;
}

describe("replay of 2026-10-14 against the bounce budget (CATCHALL-BOUNCE-GUARD-1015)", () => {
  const out = replay();
  const get = (domain: string, kind: "risky" | "valid") =>
    out.get(`${domain}|${kind}`) ?? { sent: 0, bounced: 0, held: 0, heldBounces: 0 };

  it("as it happened: 35 bounces on catch-all/unknown/no-verdict emails across the four domains", () => {
    const total = [...out.entries()]
      .filter(([k]) => k.endsWith("|risky"))
      .reduce((n, [, s]) => n + s.bounced + s.heldBounces, 0);
    expect(total).toBe(35);
  });

  it("boostdistribute.com: catch-alls held before the 12:12 burst, its bounces 21 -> at most 2", () => {
    const boost = get("boostdistribute.com", "risky");
    expect(boost.firstHeldAt!.getTime()).toBeLessThan(new Date("2026-10-14T12:12:00Z").getTime());
    expect(boost.bounced).toBeLessThanOrEqual(2);
    expect(boost.heldBounces).toBeGreaterThanOrEqual(19);
  });

  it("agileconsultco.com: catch-alls held from the first one, zero of its bounces sent", () => {
    const agile = get("agileconsultco.com", "risky");
    expect(agile.firstHeldAt!.getTime()).toBeLessThan(new Date("2026-10-14T12:15:00Z").getTime());
    expect(agile.bounced).toBe(0);
  });

  it("across the four domains: 35 -> at most 3 risky bounces", () => {
    const sentBounces = [...out.entries()]
      .filter(([k]) => k.endsWith("|risky"))
      .reduce((n, [, s]) => n + s.bounced, 0);
    expect(sentBounces).toBeLessThanOrEqual(3);
  });

  it("every valid-address email still went out", () => {
    for (const domain of ["boostdistribute.com", "agileconsultco.com", "growdistribute.com", "fuseconnectio.com"]) {
      const valid = get(domain, "valid");
      expect(valid.held).toBe(0);
      expect(valid.sent).toBeGreaterThan(0);
    }
  });
});
