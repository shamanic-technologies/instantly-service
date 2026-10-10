import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const mockExecute = vi.fn();
const mockUpdateSet = vi.fn();
const mockCancelRemainingProvisions = vi.fn();
const mockResolveInstantlyApiKey = vi.fn();
const mockUpdateCampaignStatus = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    execute: (...args: unknown[]) => mockExecute(...args),
    update: () => ({
      set: (v: unknown) => ({ where: () => Promise.resolve(mockUpdateSet(v)) }),
    }),
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: { instantlyCampaignId: "instantly_campaign_id" },
}));

vi.mock("../../src/lib/silver-promote", () => ({
  cancelRemainingProvisions: (...args: unknown[]) => mockCancelRemainingProvisions(...args),
}));

vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: (...args: unknown[]) => mockResolveInstantlyApiKey(...args),
}));

vi.mock("../../src/lib/instantly-client", () => ({
  updateCampaignStatus: (...args: unknown[]) => mockUpdateCampaignStatus(...args),
}));

process.env.LEAD_SERVICE_URL = "http://lead-service";
process.env.LEAD_SERVICE_API_KEY = "lead-key";

const { stopQueuedSequencesOfBookedPeople, isBookedSequence } = await import(
  "../../src/lib/booked-stops-cold"
);
const { listBookedPeople, BOOKED_OUTCOME_EVENTS } = await import("../../src/lib/lead-client");

const CALLER = { method: "POST", path: "/internal/self-send/dispatch" };

// Prod 2026-10-07/08, Doc Dinners.
const DOC_DINNERS = "75d7e3e8-6926-4f85-a557-976895400666";
const FERNANDA = "fernanda@chirohealthspa.com";
const FERNANDA_LEAD = "1d4ae053-ba5a-4685-a5ce-4dae61d1ba67";
const FERNANDA_INSTANTLY = "55d552f2-a0d0-47c3-80da-bec1c74b1079";
const FERNANDA_CAMPAIGN = "f7b1b610-4fa1-4b54-8fec-f7be124dc32b";

function queued(over: Record<string, unknown> = {}) {
  return {
    instantlyCampaignId: FERNANDA_INSTANTLY,
    campaignId: FERNANDA_CAMPAIGN,
    orgId: "org-1",
    userId: null,
    runId: "run-1",
    leadId: FERNANDA_LEAD,
    leadEmail: FERNANDA,
    brandIds: [DOC_DINNERS],
    ...over,
  };
}

/** lead-service's served ledger: `{ event: outcomes[] }` per brand. */
function serveLedger(byBrand: Record<string, Record<string, Array<{ leadId: string | null; email: string | null }>>>) {
  const fetchMock = vi.fn(async (url: string) => {
    const m = /\/internal\/brands\/([^/]+)\/converted-leads\?event=(\w+)$/.exec(url);
    if (!m) return new Response("not found", { status: 404 });
    const outcomes = byBrand[decodeURIComponent(m[1]!)]?.[m[2]!] ?? [];
    return new Response(JSON.stringify({ event: m[2], outcomes }), { status: 200 });
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

beforeEach(() => {
  vi.resetAllMocks();
  mockResolveInstantlyApiKey.mockResolvedValue({ key: "k", keySource: "platform" });
  mockUpdateCampaignStatus.mockResolvedValue({});
  mockCancelRemainingProvisions.mockResolvedValue(undefined);
  mockUpdateSet.mockReturnValue([{}]);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("booked meeting stops the cold sequence (Fernanda, Doc Dinners, 2026-10-08)", () => {
  it("a booking recorded after step 2 means step 3 never sends: Instantly campaign paused, step 3 hold cancelled, row out of the queue", async () => {
    // Steps 1 and 2 went out; step 3 is still a provisioned hold, so the sequence is queued.
    mockExecute.mockResolvedValue({ rows: [queued()] });
    // lead-service recorded meeting_booked (source crm) on 10-07 16:17.
    serveLedger({ [DOC_DINNERS]: { meeting_booked: [{ leadId: FERNANDA_LEAD, email: FERNANDA }] } });

    const { summary, notYetStopped } = await stopQueuedSequencesOfBookedPeople(CALLER);

    expect(summary).toMatchObject({ booked: 1, stoppedInstantly: 1, failed: 0, deferred: 0 });
    expect(mockUpdateCampaignStatus).toHaveBeenCalledWith("k", FERNANDA_INSTANTLY, "paused");
    expect(mockCancelRemainingProvisions).toHaveBeenCalledWith(
      expect.objectContaining({ instantlyCampaignId: FERNANDA_INSTANTLY }),
      FERNANDA,
    );
    expect(mockUpdateSet).toHaveBeenCalledWith(expect.objectContaining({ status: "paused" }));
    // The pause (what stops the email) comes before the local bookkeeping.
    expect(mockUpdateCampaignStatus.mock.invocationCallOrder[0]!).toBeLessThan(
      mockCancelRemainingProvisions.mock.invocationCallOrder[0]!,
    );
    expect(notYetStopped.size).toBe(0);
  });

  it("the same person on a SELF-SEND sequence is stopped locally, no Instantly call", async () => {
    mockExecute.mockResolvedValue({ rows: [queued({ instantlyCampaignId: "self:aaaa" })] });
    serveLedger({ [DOC_DINNERS]: { meeting_booked: [{ leadId: FERNANDA_LEAD, email: FERNANDA }] } });

    const { summary } = await stopQueuedSequencesOfBookedPeople(CALLER);

    expect(summary).toMatchObject({ stoppedSelfSend: 1, stoppedInstantly: 0 });
    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
    expect(mockUpdateSet).toHaveBeenCalledWith(expect.objectContaining({ status: "paused" }));
  });

  it("stops EVERY queued sequence of the booked person at the brand, whichever campaign (platform send included)", async () => {
    mockExecute.mockResolvedValue({
      rows: [
        queued(),
        queued({ instantlyCampaignId: "self:other-campaign", campaignId: "camp-2" }),
        queued({ instantlyCampaignId: "self:platform", campaignId: null, orgId: null }),
      ],
    });
    serveLedger({ [DOC_DINNERS]: { sale: [{ leadId: FERNANDA_LEAD, email: FERNANDA }] } });

    const { summary } = await stopQueuedSequencesOfBookedPeople(CALLER);

    expect(summary).toMatchObject({ booked: 3, stoppedInstantly: 1, stoppedSelfSend: 2, failed: 0 });
  });

  it("a person nobody booked keeps their sequence; a booking at ANOTHER brand stops nothing here", async () => {
    mockExecute.mockResolvedValue({
      rows: [queued({ leadEmail: "other@x.com", leadId: "lead-other", instantlyCampaignId: "self:o" })],
    });
    serveLedger({
      [DOC_DINNERS]: { meeting_booked: [{ leadId: FERNANDA_LEAD, email: FERNANDA }] },
      "brand-other": { meeting_booked: [{ leadId: "lead-other", email: "other@x.com" }] },
    });

    const { summary } = await stopQueuedSequencesOfBookedPeople(CALLER);

    expect(summary).toMatchObject({ booked: 0, stoppedSelfSend: 0, stoppedInstantly: 0 });
    expect(mockUpdateSet).not.toHaveBeenCalled();
  });

  it("FAILS LOUD when the ledger cannot be read: never 'nobody booked'", async () => {
    mockExecute.mockResolvedValue({ rows: [queued()] });
    vi.stubGlobal("fetch", vi.fn(async () => new Response("down", { status: 503 })));

    await expect(stopQueuedSequencesOfBookedPeople(CALLER)).rejects.toThrow(/converted-leads.*503/);
    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
  });

  it("a failed Instantly pause leaves the row queued and held back from the dispatcher", async () => {
    mockExecute.mockResolvedValue({ rows: [queued()] });
    serveLedger({ [DOC_DINNERS]: { meeting_booked: [{ leadId: FERNANDA_LEAD, email: FERNANDA }] } });
    mockUpdateCampaignStatus.mockRejectedValue(new Error("instantly 500"));

    const { summary, notYetStopped } = await stopQueuedSequencesOfBookedPeople(CALLER);

    expect(summary).toMatchObject({ failed: 1, stoppedInstantly: 0 });
    expect(mockCancelRemainingProvisions).not.toHaveBeenCalled();
    expect(notYetStopped.has(FERNANDA_INSTANTLY)).toBe(true);
  });

  it("over the limit, the rest is held back (never sent) for the next tick", async () => {
    mockExecute.mockResolvedValue({
      rows: [queued({ instantlyCampaignId: "self:a" }), queued({ instantlyCampaignId: "self:b" })],
    });
    serveLedger({ [DOC_DINNERS]: { meeting_booked: [{ leadId: FERNANDA_LEAD, email: FERNANDA }] } });

    const { summary, notYetStopped } = await stopQueuedSequencesOfBookedPeople(CALLER, 1);

    expect(summary).toMatchObject({ booked: 2, stoppedSelfSend: 1, deferred: 1 });
    expect(notYetStopped.size).toBe(1);
  });

  it("no queued sequence means no lead-service read at all", async () => {
    mockExecute.mockResolvedValue({ rows: [] });
    const fetchMock = serveLedger({});

    const { summary } = await stopQueuedSequencesOfBookedPeople(CALLER);

    expect(summary.brandsRead).toBe(0);
    expect(fetchMock).not.toHaveBeenCalled();
  });
});

describe("isBookedSequence", () => {
  const ledger = new Map([[DOC_DINNERS, { leadIds: new Set([FERNANDA_LEAD]), emails: new Set([FERNANDA]) }]]);

  it("matches on the address, case- and space-folded, even under a repointed lead id", () => {
    expect(isBookedSequence({ leadId: "new-id", leadEmail: " Fernanda@ChiroHealthSpa.com ", brandIds: [DOC_DINNERS] }, ledger)).toBe(true);
  });

  it("matches on lead-service's lead id when the address we sent to is not its canonical one", () => {
    expect(isBookedSequence({ leadId: FERNANDA_LEAD, leadEmail: "f.alt@chirohealthspa.com", brandIds: [DOC_DINNERS] }, ledger)).toBe(true);
  });

  it("a multi-brand sequence stops when the person is booked at ANY of its brands", () => {
    expect(isBookedSequence({ leadId: null, leadEmail: FERNANDA, brandIds: ["b-x", DOC_DINNERS] }, ledger)).toBe(true);
  });
});

describe("listBookedPeople — lead-service's served ledger", () => {
  it("reads every booked step (meeting_booked, meeting_attended, sale) and merges them", async () => {
    expect([...BOOKED_OUTCOME_EVENTS]).toEqual(["meeting_booked", "meeting_attended", "sale"]);
    const fetchMock = serveLedger({
      [DOC_DINNERS]: {
        meeting_booked: [{ leadId: "l1", email: "A@x.com" }],
        meeting_attended: [{ leadId: "l2", email: null }],
        sale: [{ leadId: null, email: "c@x.com" }],
      },
    });

    const booked = await listBookedPeople(DOC_DINNERS);

    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(booked.leadIds).toEqual(new Set(["l1", "l2"]));
    expect(booked.emails).toEqual(new Set(["a@x.com", "c@x.com"]));
    expect(fetchMock.mock.calls[0]![1]).toMatchObject({ headers: { "x-api-key": "lead-key" } });
  });

  it("throws on a body without an outcomes array", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify({}), { status: 200 })));
    await expect(listBookedPeople(DOC_DINNERS)).rejects.toThrow(/no outcomes array/);
  });
});
