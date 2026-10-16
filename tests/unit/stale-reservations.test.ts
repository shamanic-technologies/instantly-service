import { describe, it, expect } from "vitest";

import { classifyStepRun, decideStaleReservation } from "../../src/lib/stale-reservations";
import type { RunCost, RunEvent } from "../../src/lib/runs-client";

const event = (e: Partial<RunEvent>): RunEvent => ({ event: "x", detail: null, createdAt: "2026-10-16T14:49:30Z", ...e });
const cost = (id: string, status: RunCost["status"]): RunCost => ({
  id,
  runId: "step-run",
  costName: "instantly-account-email-sent",
  costSource: "platform",
  quantity: "1",
  unitCostInUsdCents: "2.84",
  totalCostInUsdCents: "2.84",
  status,
  createdAt: "2026-10-16T14:49:26Z",
});

describe("decideStaleReservation: never release a row whose email may have left", () => {
  it("releases an interrupted reservation with no trace of a send (the 2026-10-16 OOM rows)", () => {
    expect(decideStaleReservation({ instantlyCreateStartedAt: null }, [event({ event: "send-start" })])).toEqual({ action: "release" });
  });

  it("releases when runs-service no longer has the run (nothing to read, our records hold no send)", () => {
    expect(decideStaleReservation({ instantlyCreateStartedAt: null }, null)).toEqual({ action: "release" });
  });

  it("keeps a row stamped before the Instantly campaign was created", () => {
    const d = decideStaleReservation({ instantlyCreateStartedAt: "2026-10-16T14:49:30Z" }, []);
    expect(d.action).toBe("keep");
  });

  it("keeps a row whose run traced an Instantly campaign creation", () => {
    const d = decideStaleReservation({ instantlyCreateStartedAt: null }, [
      event({ event: "send-campaign-created", detail: "instantlyCampaignId=0fd04c25-8700-4b11, added=1, account=a@b.com" }),
    ]);
    expect(d.action).toBe("keep");
  });

  it("a self-send id minted in memory is not a send (nothing leaves before phase-2)", () => {
    const d = decideStaleReservation({ instantlyCreateStartedAt: null }, [
      event({ event: "send-campaign-created", detail: "instantlyCampaignId=self:36d3873b, added=1, account=a@b.com" }),
    ]);
    expect(d).toEqual({ action: "release" });
  });
});

describe("classifyStepRun: only an orphan step run is settled", () => {
  it("provisioned costs on no queued hold = orphan, all of them cancelled", () => {
    expect(classifyStepRun([cost("a", "provisioned"), cost("b", "provisioned")], new Set())).toEqual({ orphan: true, costIds: ["a", "b"] });
  });

  it("a cost a queued hold carries is never cancelled", () => {
    expect(classifyStepRun([cost("a", "provisioned"), cost("b", "provisioned")], new Set(["b"])).orphan).toBe(false);
  });

  it("a run with an actual cost (an email left) is never touched", () => {
    expect(classifyStepRun([cost("a", "actual"), cost("b", "provisioned")], new Set()).orphan).toBe(false);
  });

  it("nothing provisioned = nothing to give back", () => {
    expect(classifyStepRun([cost("a", "cancelled")], new Set()).orphan).toBe(false);
  });
});
