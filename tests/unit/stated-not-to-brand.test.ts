// DUALIST-WA-SILENCE-EMAIL (owner 2026-11-04): a person the brand's team STATED is
// not someone of the brand (crm-service people correction) gets no automated email.
// This file: the crm-service client and the dispatch sweep. The send gate, the
// status read and the reply gate have their blocks in send.test.ts,
// send-refusal.test.ts and reply-to-lead.test.ts.
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const mockLoadQueued = vi.fn();
const mockStopSelfSend = vi.fn();
const mockResolveInstantlyApiKey = vi.fn();
const mockUpdateCampaignStatus = vi.fn();

vi.mock("../../src/lib/booked-stops-cold", () => ({
  loadQueuedBrandSequences: (...a: unknown[]) => mockLoadQueued(...a),
}));
vi.mock("../../src/lib/self-send/stop-sequence", () => ({
  stopSelfSendSequence: (...a: unknown[]) => mockStopSelfSend(...a),
}));
vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: (...a: unknown[]) => mockResolveInstantlyApiKey(...a),
}));
vi.mock("../../src/lib/instantly-client", () => ({
  updateCampaignStatus: (...a: unknown[]) => mockUpdateCampaignStatus(...a),
}));

const { readStatedNotToBrand, CrmStatedReadError } = await import("../../src/lib/crm-client");
const { stopQueuedSequencesOfStatedPeople, findStatedNotToBrand, statedNotToBrandRefusal } = await import(
  "../../src/lib/stated-not-to-brand"
);

const CALLER = { method: "POST", path: "/internal/self-send/dispatch" };
const ORG = "11111111-1111-4111-8111-111111111111";
const BRAND = "22222222-2222-4222-8222-222222222222";
const OTHER_BRAND = "33333333-3333-4333-8333-333333333333";

/** crm-service: `stated` per (brand, email) in the given set. */
function serveCrm(statedPairs: Array<[string, string]>, status = 200) {
  const set = new Set(statedPairs.map(([b, e]) => `${b}|${e}`));
  const fetchMock = vi.fn(async (url: string, init: { body?: string; headers?: Record<string, string> }) => {
    if (!url.endsWith("/internal/people/stated-not-to-brand")) return new Response("nf", { status: 404 });
    if (status !== 200) return new Response("down", { status });
    const body = JSON.parse(init.body ?? "{}") as { orgId: string; brandIds: string[]; emails: string[] };
    const results = body.emails.map((email) => {
      const brandId = body.brandIds.find((b) => set.has(`${b}|${email.toLowerCase()}`)) ?? null;
      return { email, stated: !!brandId, brandId, correctionId: brandId ? `corr-${email}` : null };
    });
    return new Response(JSON.stringify({ orgId: body.orgId, results }), { status: 200 });
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

beforeEach(() => {
  vi.resetAllMocks();
  process.env.CRM_SERVICE_URL = "http://crm/";
  process.env.CRM_SERVICE_API_KEY = "crm-key";
  mockResolveInstantlyApiKey.mockResolvedValue({ key: "k", keySource: "platform" });
  mockUpdateCampaignStatus.mockResolvedValue({});
  mockStopSelfSend.mockResolvedValue(undefined);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("crm-client readStatedNotToBrand", () => {
  it("asks crm-service with the service key and returns the stated addresses, normalised", async () => {
    const fetchMock = serveCrm([[BRAND, "thierry@x.com"]]);
    const out = await readStatedNotToBrand({ orgId: ORG, brandIds: [BRAND], emails: ["Thierry@X.com", "ok@x.com"], runId: "run-9" });

    expect([...out.keys()]).toEqual(["thierry@x.com"]);
    expect(out.get("thierry@x.com")).toEqual({ brandId: BRAND, correctionId: "corr-thierry@x.com" });
    const [url, init] = fetchMock.mock.calls[0]!;
    expect(url).toBe("http://crm/internal/people/stated-not-to-brand");
    expect(init.headers["x-api-key"]).toBe("crm-key");
    expect(init.headers["x-run-id"]).toBe("run-9");
    expect(JSON.parse(init.body!)).toEqual({ orgId: ORG, brandIds: [BRAND], emails: ["thierry@x.com", "ok@x.com"] });
  });

  it("chunks to the producer's caps (1000 emails per call)", async () => {
    const fetchMock = serveCrm([[BRAND, "p1500@x.com"]]);
    const emails = Array.from({ length: 2001 }, (_, i) => `p${i}@x.com`);
    const out = await readStatedNotToBrand({ orgId: ORG, brandIds: [BRAND], emails });
    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(out.has("p1500@x.com")).toBe(true);
  });

  it("asks nothing for no brand or no address", async () => {
    const fetchMock = serveCrm([]);
    expect((await readStatedNotToBrand({ orgId: ORG, brandIds: [], emails: ["a@x.com"] })).size).toBe(0);
    expect((await readStatedNotToBrand({ orgId: ORG, brandIds: [BRAND], emails: [] })).size).toBe(0);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("fails loud naming the env var when CRM_SERVICE_URL is unset (boot is not touched)", async () => {
    delete process.env.CRM_SERVICE_URL;
    await expect(readStatedNotToBrand({ orgId: ORG, brandIds: [BRAND], emails: ["a@x.com"] })).rejects.toThrow(
      /CRM_SERVICE_URL is not set/,
    );
  });

  it("fails loud on a 5xx", async () => {
    serveCrm([], 503);
    await expect(readStatedNotToBrand({ orgId: ORG, brandIds: [BRAND], emails: ["a@x.com"] })).rejects.toBeInstanceOf(
      CrmStatedReadError,
    );
  });

  it("fails loud when the answer does not carry one result per asked address", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify({ results: [] }), { status: 200 })));
    await expect(readStatedNotToBrand({ orgId: ORG, brandIds: [BRAND], emails: ["a@x.com"] })).rejects.toThrow(
      /0 results for 1 emails/,
    );
  });
});

describe("the send gate helpers", () => {
  it("findStatedNotToBrand: the record, or null when no brand is in scope", async () => {
    serveCrm([[BRAND, "a@x.com"]]);
    expect(await findStatedNotToBrand(ORG, "A@x.com", [BRAND])).toEqual({ brandId: BRAND, correctionId: "corr-a@x.com" });
    expect(await findStatedNotToBrand(ORG, "a@x.com", [])).toBeNull();
    expect(await findStatedNotToBrand(ORG, "a@x.com", [OTHER_BRAND])).toBeNull();
  });

  it("the refusal body carries the code crm-service answers on WhatsApp", () => {
    const body = statedNotToBrandRefusal("a@x.com", { brandId: BRAND, correctionId: "c1" });
    expect(body.code).toBe("stated_not_to_brand");
    expect(body.details).toMatch(/No email was sent and nothing was billed/);
  });
});

describe("the dispatch sweep: queued sequences of a stated person stop", () => {
  const seq = (over: Record<string, unknown>) => ({
    instantlyCampaignId: "self:seq-1",
    campaignId: "camp-1",
    orgId: ORG,
    userId: null,
    runId: "run-1",
    leadId: null,
    leadEmail: "thierry@x.com",
    brandIds: [BRAND],
    ...over,
  });

  it("stops the stated person's sequence at THAT brand only; others keep sending", async () => {
    mockLoadQueued.mockResolvedValue([
      seq({ instantlyCampaignId: "self:stated" }),
      seq({ instantlyCampaignId: "self:other-brand", brandIds: [OTHER_BRAND] }),
      seq({ instantlyCampaignId: "self:unstated", leadEmail: "ok@x.com" }),
    ]);
    const fetchMock = serveCrm([[BRAND, "thierry@x.com"]]);

    const { summary, notYetStopped } = await stopQueuedSequencesOfStatedPeople(CALLER);

    expect(summary.stated).toBe(1);
    expect(summary.stoppedSelfSend).toBe(1);
    expect(mockStopSelfSend).toHaveBeenCalledTimes(1);
    expect(mockStopSelfSend.mock.calls[0]![0].instantlyCampaignId).toBe("self:stated");
    expect(notYetStopped.size).toBe(0);
    // One call per (org, brand).
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("pauses an Instantly-transport sequence at Instantly before cancelling its holds", async () => {
    mockLoadQueued.mockResolvedValue([seq({ instantlyCampaignId: "inst-123" })]);
    serveCrm([[BRAND, "thierry@x.com"]]);

    const { summary } = await stopQueuedSequencesOfStatedPeople(CALLER);

    expect(summary.stoppedInstantly).toBe(1);
    expect(mockUpdateCampaignStatus).toHaveBeenCalledWith("k", "inst-123", "paused");
    expect(mockUpdateCampaignStatus.mock.invocationCallOrder[0]!).toBeLessThan(mockStopSelfSend.mock.invocationCallOrder[0]!);
  });

  it("an (org, brand) crm-service cannot answer is HELD this tick (never sent), nothing stopped", async () => {
    mockLoadQueued.mockResolvedValue([seq({ instantlyCampaignId: "self:a" }), seq({ instantlyCampaignId: "self:b", leadEmail: "ok@x.com" })]);
    serveCrm([], 502);

    const { summary, notYetStopped } = await stopQueuedSequencesOfStatedPeople(CALLER);

    expect(summary.groupsUnreadable).toBe(1);
    expect([...notYetStopped].sort()).toEqual(["self:a", "self:b"]);
    expect(mockStopSelfSend).not.toHaveBeenCalled();
  });

  it("a failed stop is held back, not sent", async () => {
    mockLoadQueued.mockResolvedValue([seq({ instantlyCampaignId: "self:stated" })]);
    serveCrm([[BRAND, "thierry@x.com"]]);
    mockStopSelfSend.mockRejectedValue(new Error("db down"));

    const { summary, notYetStopped } = await stopQueuedSequencesOfStatedPeople(CALLER);

    expect(summary.failed).toBe(1);
    expect(notYetStopped.has("self:stated")).toBe(true);
  });

  it("a sequence without an org is left as it was (nothing to ask)", async () => {
    mockLoadQueued.mockResolvedValue([seq({ orgId: null })]);
    const fetchMock = serveCrm([[BRAND, "thierry@x.com"]]);

    const { summary } = await stopQueuedSequencesOfStatedPeople(CALLER);

    expect(summary.orgless).toBe(1);
    expect(fetchMock).not.toHaveBeenCalled();
    expect(mockStopSelfSend).not.toHaveBeenCalled();
  });
});
