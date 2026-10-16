import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, it, expect } from "vitest";

import { BounceBudget, PROOF_FRESH_DAYS, type WindowCounts } from "../../src/lib/bounce-budget";
import { judgeVerdict } from "../../src/lib/email-verification-gate";

/**
 * CATCHALL-BOUNCE-1016 replay: campaign "Cold Follow-up Replies" (pipe
 * 9a243028-…), the 11 follow-ups it sent on 2026-10-14/15, 4 of which bounced
 * (Mimecast "554 Email rejected due to security policies"). Prod, read-only; no
 * prospect address in the fixture, no email sent.
 *
 * Every one of the 11 had only an April 2026 send behind it: older than
 * PROOF_FRESH_DAYS, so none is "proven" and each first email goes through
 * (1) the send door's verdict rule (valid, catch_all AND unknown are sent:
 * owner rule 2026-10-14, unknown priced x3, reaffirmed 2026-10-16), then
 * (2) its domain's bounce budget, one selection per dispatch tick with the
 * history known at tick start (no DSN of the tick had landed yet).
 */
type Verdict = "valid" | "catch_all" | "unknown";
interface Send {
  who: string;
  verdict: Verdict;
  bounced: boolean;
}
interface Tick {
  mailbox: string;
  /** Domain history at tick start: [sends 24 h, bounces 24 h, sends 14 d, bounces 14 d]. */
  history: [number, number, number, number];
  sends: Send[];
}

const TICKS: Tick[] = [
  {
    mailbox: "raquel@agileconsultco.com", // 2026-10-14 14:27 UTC
    history: [28, 4, 334, 8],
    sends: [{ who: "ionio.gr", verdict: "catch_all", bounced: false }],
  },
  {
    mailbox: "tatiana@fuseconnectio.com", // 2026-10-14 16:31 UTC
    history: [14, 0, 281, 4],
    sends: [
      { who: "forbes.com #1", verdict: "unknown", bounced: true },
      { who: "techcrunch.com", verdict: "unknown", bounced: true },
      { who: "forbes.com #2", verdict: "unknown", bounced: true },
      { who: "superhuman.ai", verdict: "catch_all", bounced: false },
      { who: "forbes.com #3", verdict: "unknown", bounced: true },
    ],
  },
  {
    mailbox: "allie@leansignalio.com", // 2026-10-14 16:31 UTC
    history: [23, 1, 436, 8],
    sends: [
      { who: "indiehackers.com", verdict: "catch_all", bounced: false },
      { who: "ballmüllersolutions.de", verdict: "valid", bounced: false },
    ],
  },
  {
    mailbox: "magnolia@saviolabsco.com", // 2026-10-15 19:10 UTC
    history: [6, 0, 310, 4],
    sends: [
      { who: "tracematters.com", verdict: "catch_all", bounced: false },
      { who: "infogenetica.com", verdict: "catch_all", bounced: false },
      { who: "forbes.com #4", verdict: "unknown", bounced: false },
    ],
  },
];

/** Under the bounce budget (proof older than 90 days = none), the 4 Mimecast bounces are HELD, not refused. */
const EXPECTED: Record<string, string> = {
  "ionio.gr": "held (bounce budget)",
  "forbes.com #1": "held (bounce budget)",
  "techcrunch.com": "held (bounce budget)",
  "forbes.com #2": "held (bounce budget)",
  "superhuman.ai": "sent",
  "forbes.com #3": "held (bounce budget)",
  "indiehackers.com": "held (bounce budget)",
  "ballmüllersolutions.de": "sent",
  "tracematters.com": "sent",
  "infogenetica.com": "held (bounce budget)",
  "forbes.com #4": "held (bounce budget)",
};

type Outcome = "sent" | "held (bounce budget)" | "refused at the door";

function replay(): Map<string, { outcome: Outcome; bounced: boolean }> {
  const out = new Map<string, { outcome: Outcome; bounced: boolean }>();
  for (const tick of TICKS) {
    const domain = tick.mailbox.split("@")[1]!;
    const [s24, b24, s14, b14] = tick.history;
    const windows = new Map<number, WindowCounts>([
      [24, { sends: s24, bounces: b24 }],
      [336, { sends: s14, bounces: b14 }],
    ]);
    const budget = new BounceBudget(new Map([[domain, windows]]));
    for (const send of tick.sends) {
      if (!judgeVerdict(send.verdict).accepted) {
        out.set(send.who, { outcome: "refused at the door", bounced: send.bounced });
        continue;
      }
      if (budget.refusal(tick.mailbox, send.verdict) !== null) {
        out.set(send.who, { outcome: "held (bounce budget)", bounced: send.bounced });
        continue;
      }
      budget.spend(tick.mailbox, send.verdict, true);
      out.set(send.who, { outcome: "sent", bounced: send.bounced });
    }
  }
  return out;
}

describe("CATCHALL-BOUNCE-1016 — the 11 follow-ups of 2026-10-14/15, unknown sent at x3 (owner 2026-10-14, reaffirmed 2026-10-16)", () => {
  const result = replay();

  it("an unknown verdict is accepted at the door (priced -unknown at send), never refused there", () => {
    expect(judgeVerdict("unknown")).toEqual({ accepted: true, reason: "verdict_unknown" });
    for (const r of result.values()) expect(r.outcome).not.toBe("refused at the door");
  });

  it("unknowns and catch-alls are rationed by their domain's bounce budget, the valid one goes out", () => {
    expect(Object.fromEntries([...result].map(([k, v]) => [k, v.outcome]))).toEqual(EXPECTED);
    const sentBounces = [...result.values()].filter((r) => r.outcome === "sent" && r.bounced);
    expect(sentBounces).toHaveLength(0);
  });

  it("proof expires: only an email of ours accepted within 90 days exempts an address from the budget", () => {
    expect(PROOF_FRESH_DAYS).toBe(90);
    const src = readFileSync(join(__dirname, "../../src/lib/self-send/dispatch-worker.ts"), "utf8");
    expect(src).toContain("make_interval(days => ${PROOF_FRESH_DAYS})");
  });
});
