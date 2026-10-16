import { describe, it, expect } from "vitest";

import { BounceBudget, type WindowCounts } from "../../src/lib/bounce-budget";
import { judgeVerdict } from "../../src/lib/email-verification-gate";

/**
 * CATCHALL-BOUNCE-1016 replay: campaign "Cold Follow-up Replies" (pipe
 * 9a243028-…), the 11 follow-ups it sent on 2026-10-14/15, 4 of which bounced
 * (Mimecast "554 Email rejected due to security policies"). Prod, read-only; no
 * prospect address in the fixture, no email sent.
 *
 * Every one of the 11 had only an April 2026 send behind it: older than
 * PROOF_FRESH_DAYS, so under the new rule none is "proven" and each first email
 * goes through (1) the send door's verdict rule, then (2) its domain's bounce
 * budget, one selection per dispatch tick with the history known at tick start
 * (no DSN of the tick had landed yet).
 */
type Verdict = "valid" | "catch_all" | "unknown";
interface Send {
  who: string;
  verdict: Verdict;
  bounced: boolean;
  /** An earlier `unknown` >= 24 h old, or a real email of ours accepted < 90 days ago. */
  unknownConfirmed: boolean;
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
    sends: [{ who: "ionio.gr", verdict: "catch_all", bounced: false, unknownConfirmed: false }],
  },
  {
    mailbox: "tatiana@fuseconnectio.com", // 2026-10-14 16:31 UTC
    history: [14, 0, 281, 4],
    sends: [
      { who: "forbes.com #1", verdict: "unknown", bounced: true, unknownConfirmed: false },
      { who: "techcrunch.com", verdict: "unknown", bounced: true, unknownConfirmed: false },
      { who: "forbes.com #2", verdict: "unknown", bounced: true, unknownConfirmed: false },
      { who: "superhuman.ai", verdict: "catch_all", bounced: false, unknownConfirmed: false },
      { who: "forbes.com #3", verdict: "unknown", bounced: true, unknownConfirmed: false },
    ],
  },
  {
    mailbox: "allie@leansignalio.com", // 2026-10-14 16:31 UTC
    history: [23, 1, 436, 8],
    sends: [
      { who: "indiehackers.com", verdict: "catch_all", bounced: false, unknownConfirmed: false },
      { who: "ballmüllersolutions.de", verdict: "valid", bounced: false, unknownConfirmed: false },
    ],
  },
  {
    mailbox: "magnolia@saviolabsco.com", // 2026-10-15 19:10 UTC
    history: [6, 0, 310, 4],
    sends: [
      { who: "tracematters.com", verdict: "catch_all", bounced: false, unknownConfirmed: false },
      { who: "infogenetica.com", verdict: "catch_all", bounced: false, unknownConfirmed: false },
      { who: "forbes.com #4", verdict: "unknown", bounced: false, unknownConfirmed: false },
    ],
  },
];

type Outcome = "sent" | "held (bounce budget)" | "refused (unknown, re-asked in 48 h)";

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
      if (!judgeVerdict(send.verdict, send.unknownConfirmed).accepted) {
        out.set(send.who, { outcome: "refused (unknown, re-asked in 48 h)", bounced: send.bounced });
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

describe("CATCHALL-BOUNCE-1016 — the 11 follow-ups of 2026-10-14/15 under the new rule", () => {
  const result = replay();

  it("every unknown is refused at the door and re-asked: the 4 bounces never leave", () => {
    for (const who of ["forbes.com #1", "techcrunch.com", "forbes.com #2", "forbes.com #3", "forbes.com #4"]) {
      expect(result.get(who)?.outcome).toBe("refused (unknown, re-asked in 48 h)");
    }
    const sentBounces = [...result.values()].filter((r) => r.outcome === "sent" && r.bounced);
    expect(sentBounces).toHaveLength(0);
  });

  it("catch-alls are rationed by their domain's budget, the valid one goes out", () => {
    expect(Object.fromEntries([...result].map(([k, v]) => [k, v.outcome]))).toEqual({
      "ionio.gr": "held (bounce budget)",
      "forbes.com #1": "refused (unknown, re-asked in 48 h)",
      "techcrunch.com": "refused (unknown, re-asked in 48 h)",
      "forbes.com #2": "refused (unknown, re-asked in 48 h)",
      "superhuman.ai": "sent",
      "forbes.com #3": "refused (unknown, re-asked in 48 h)",
      "indiehackers.com": "held (bounce budget)",
      "ballmüllersolutions.de": "sent",
      "tracematters.com": "sent",
      "infogenetica.com": "held (bounce budget)",
      "forbes.com #4": "refused (unknown, re-asked in 48 h)",
    });
  });

  it("a CONFIRMED unknown (retry answered the same, or fresh proof) is sendable, under the budget", () => {
    expect(judgeVerdict("unknown", true)).toEqual({ accepted: true, reason: "verdict_unknown" });
  });
});
