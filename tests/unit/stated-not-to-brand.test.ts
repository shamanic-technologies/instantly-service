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

const {
  readStatedNotToBrand,
  readStatedNotToBrandMany,
  clearStatedSetsCache,
  CrmStatedReadError,
  STATED_SETS_TTL_MS,
  STATED_SETS_REFRESH_AFTER_MS,
} = await import("../../src/lib/crm-client");
const {
  stopQueuedSequencesOfStatedPeople,
  findStatedNotToBrand,
  findStatedNotToBrands,
  statedNotToBrandRefusal,
  STATED_SETS_STATUS_MAX_STALE_MS,
} = await import(
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
    if (url.endsWith("/internal/people/stated-not-to-brand/email-sets")) {
      if (status !== 200) return new Response("down", { status });
      const body = JSON.parse(init.body ?? "{}") as { orgId: string; brandIds: string[] };
      const statements = statedPairs
        .filter(([b]) => body.brandIds.includes(b))
        .map(([b, e]) => ({ correctionId: `corr-${e}`, brandId: b, emails: [e] }));
      return new Response(JSON.stringify({ orgId: body.orgId, brandIds: body.brandIds, statements }), { status: 200 });
    }
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
  clearStatedSetsCache();
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

// STATUS-CRM-TIMEOUT-1107: one crm-service request per 100-address status batch timed out 461 times in 3 h under a
// features-service restart burst; the many-address read asks crm-service once per (org, brands) and matches here.
describe("crm-client readStatedNotToBrandMany (status read + sweep)", () => {
  afterEach(() => vi.useRealTimers());

  it("a burst of 300 status batches for one brand = ONE crm-service request, same answers as the per-address read", async () => {
    const fetchMock = serveCrm([[BRAND, "p150@x.com"], [OTHER_BRAND, "p7@x.com"]]);
    const batches = Array.from({ length: 300 }, (_, b) => Array.from({ length: 100 }, (_, i) => `P${(b * 100 + i) % 1000}@X.com`));
    const answers = await Promise.all(batches.map((emails) => readStatedNotToBrandMany({ orgId: ORG, brandIds: [BRAND], emails })));
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, init] = fetchMock.mock.calls[0]!;
    expect(url).toBe("http://crm/internal/people/stated-not-to-brand/email-sets");
    expect(JSON.parse(init.body!)).toEqual({ orgId: ORG, brandIds: [BRAND] });
    expect(answers[1]!.get("p150@x.com")).toEqual({ brandId: BRAND, correctionId: "corr-p150@x.com" });
    expect(answers.reduce((n, a) => n + a.size, 0)).toBe(30); // p150 is in every 10th batch
    // Byte-equal to the per-address read on the same population.
    const one = await readStatedNotToBrand({ orgId: ORG, brandIds: [BRAND], emails: batches[1]! });
    expect([...one.entries()]).toEqual([...answers[1]!.entries()]);
  });

  it("keyed on the brand SET: another brand is its own answer; brand order does not matter", async () => {
    const fetchMock = serveCrm([[OTHER_BRAND, "a@x.com"]]);
    expect((await readStatedNotToBrandMany({ orgId: ORG, brandIds: [BRAND], emails: ["a@x.com"] })).size).toBe(0);
    expect((await readStatedNotToBrandMany({ orgId: ORG, brandIds: [BRAND, OTHER_BRAND], emails: ["a@x.com"] })).size).toBe(1);
    expect((await readStatedNotToBrandMany({ orgId: ORG, brandIds: [OTHER_BRAND, BRAND], emails: ["a@x.com"] })).size).toBe(1);
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("a statement made after the read lands within the TTL (background refresh), never later", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    const t0 = new Date("2026-11-07T15:00:00Z").getTime();
    vi.setSystemTime(t0);
    serveCrm([]);
    expect((await readStatedNotToBrandMany({ orgId: ORG, brandIds: [BRAND], emails: ["a@x.com"] })).size).toBe(0);
    const fetchMock = serveCrm([[BRAND, "a@x.com"]]);
    vi.setSystemTime(t0 + STATED_SETS_REFRESH_AFTER_MS - 1);
    expect((await readStatedNotToBrandMany({ orgId: ORG, brandIds: [BRAND], emails: ["a@x.com"] })).size).toBe(0);
    expect(fetchMock).not.toHaveBeenCalled();
    vi.setSystemTime(t0 + STATED_SETS_REFRESH_AFTER_MS);
    await readStatedNotToBrandMany({ orgId: ORG, brandIds: [BRAND], emails: ["a@x.com"] }); // served, refresh started
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setImmediate(r));
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect((await readStatedNotToBrandMany({ orgId: ORG, brandIds: [BRAND], emails: ["a@x.com"] })).size).toBe(1);
  });

  it("FAIL LOUD: an expired answer crm-service cannot reload throws, never reads as nobody stated", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    const t0 = new Date("2026-11-07T15:00:00Z").getTime();
    vi.setSystemTime(t0);
    serveCrm([]);
    await readStatedNotToBrandMany({ orgId: ORG, brandIds: [BRAND], emails: ["a@x.com"] });
    serveCrm([], 503);
    vi.setSystemTime(t0 + STATED_SETS_TTL_MS);
    await expect(readStatedNotToBrandMany({ orgId: ORG, brandIds: [BRAND], emails: ["a@x.com"] })).rejects.toBeInstanceOf(CrmStatedReadError);
  });

  it("MON-DELIVERY-STATUS-500-1111: the STATUS read serves the last answer while crm-service times out, up to 1 h, then throws", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    const t0 = new Date("2026-11-11T15:00:00Z").getTime();
    vi.setSystemTime(t0);
    serveCrm([[BRAND, "a@x.com"]]);
    expect((await findStatedNotToBrands(ORG, ["a@x.com", "b@x.com"], [BRAND])).has("a@x.com")).toBe(true);
    const timeout = vi.fn().mockRejectedValue(Object.assign(new Error("The operation was aborted due to timeout"), { name: "TimeoutError" }));
    vi.stubGlobal("fetch", timeout);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.setSystemTime(t0 + STATED_SETS_TTL_MS);
    const stale = await findStatedNotToBrands(ORG, ["a@x.com", "b@x.com"], [BRAND]);
    expect([...stale.keys()]).toEqual(["a@x.com"]);
    expect(timeout).toHaveBeenCalled();
    expect(warn.mock.calls.some(([m]) => String(m).includes("serving its last answer"))).toBe(true);
    vi.setSystemTime(t0 + STATED_SETS_STATUS_MAX_STALE_MS);
    await expect(findStatedNotToBrands(ORG, ["a@x.com"], [BRAND])).rejects.toBeInstanceOf(CrmStatedReadError);
    warn.mockRestore();
  });

  it("the sweep path (no maxStaleOnErrorMs) never serves a stale answer: it throws and holds", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    const t0 = new Date("2026-11-11T15:00:00Z").getTime();
    vi.setSystemTime(t0);
    serveCrm([[BRAND, "a@x.com"]]);
    await readStatedNotToBrandMany({ orgId: ORG, brandIds: [BRAND], emails: ["a@x.com"] });
    serveCrm([], 503);
    vi.setSystemTime(t0 + STATED_SETS_TTL_MS + 1);
    await expect(readStatedNotToBrandMany({ orgId: ORG, brandIds: [BRAND], emails: ["a@x.com"] })).rejects.toBeInstanceOf(CrmStatedReadError);
  });

  it("FAIL LOUD: no answer yet and crm-service times out = throws naming the route; a malformed answer throws", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(Object.assign(new Error("The operation was aborted due to timeout"), { name: "TimeoutError" })));
    await expect(readStatedNotToBrandMany({ orgId: ORG, brandIds: [BRAND], emails: ["a@x.com"] })).rejects.toThrow(
      /stated-not-to-brand\/email-sets unreachable/,
    );
    vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify({ statements: [{ brandId: BRAND }] }), { status: 200 })));
    await expect(readStatedNotToBrandMany({ orgId: ORG, brandIds: [BRAND], emails: ["a@x.com"] })).rejects.toThrow(/malformed/);
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
