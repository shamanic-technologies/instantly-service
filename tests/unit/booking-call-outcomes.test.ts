import { describe, it, expect } from "vitest";
import { buildBookingCallDeclaration, matchRing } from "../../src/lib/booking-call-outcomes";

const LEAD = "bc60b495-2482-4972-8162-e296c72df728";
// Prod 2026-10-07: robert.burke stamped 11:23:51.209, ring run started 11:23:51.230.
const STAMPED = Date.parse("2026-10-07T11:23:51.209Z");
const RUN_START = Date.parse("2026-10-07T11:23:51.230Z");

describe("past AI Instant Call rings = booking_call of the lead (LEDGER-HISTORY-1024)", () => {
  it("produces booking_call, consuming the lead's positive reply (conversation)", () => {
    expect(buildBookingCallDeclaration(LEAD)).toEqual({
      produced: [{ outcomeKey: "booking_call", subjectService: "lead-service", subjectId: LEAD }],
      consumed: [{ outcomeKey: "conversation", subjectService: "lead-service", subjectId: LEAD }],
    });
  });

  it("names the ring's lead from the ONE sequence stamped at the run's start", () => {
    expect(matchRing(RUN_START, [{ leadId: LEAD, calledAt: STAMPED }, { leadId: "other", calledAt: STAMPED - 3_600_000 }])).toBe(LEAD);
  });

  it("never guesses: no stamp, or two people stamped together, = no lead", () => {
    expect(matchRing(RUN_START, [{ leadId: "other", calledAt: STAMPED - 60_000 }])).toBeNull();
    expect(matchRing(RUN_START, [{ leadId: LEAD, calledAt: STAMPED }, { leadId: "other", calledAt: STAMPED + 10 }])).toBeNull();
  });
});
