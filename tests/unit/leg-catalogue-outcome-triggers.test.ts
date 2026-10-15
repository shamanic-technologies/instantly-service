import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  parseOutcomeTriggers,
  outcomeTriggerIdFor,
  getOutcomeTriggers,
  getChannelCatalogue,
} from "../../src/lib/leg-catalogue";
import { clearStatsCache } from "../../src/lib/stats-cache";

// features-service GET /public/channels, trimmed to what this read uses (prod 2026-10-15).
const BODY = {
  channels: [{ slug: "cold-email-outreach", stepTransitions: [] }],
  triggers: [
    { id: "positive_reply_received", type: "outcome", outcomeKey: "conversation", firedBy: "instantly-service", coded: true },
    { id: "website_visited", type: "outcome", outcomeKey: "website_visit", firedBy: "lead-service", coded: false },
    { id: "lead_requested", type: "request", outcomeKey: null },
    { id: "delay", type: "condition" },
  ],
};

describe("parseOutcomeTriggers", () => {
  it("keeps the OUTCOME triggers only, by id and outcomeKey", () => {
    expect(parseOutcomeTriggers(BODY)).toEqual([
      { id: "positive_reply_received", outcomeKey: "conversation" },
      { id: "website_visited", outcomeKey: "website_visit" },
    ]);
  });

  it("throws on a body without a triggers array (a guess would fire the wrong trigger)", () => {
    expect(() => parseOutcomeTriggers({ channels: [] })).toThrow(/no triggers array/);
  });

  it("throws on an outcome trigger missing its outcomeKey", () => {
    expect(() => parseOutcomeTriggers({ triggers: [{ id: "x", type: "outcome" }] })).toThrow(/unreadable/);
  });
});

describe("outcomeTriggerIdFor", () => {
  it("maps website_visit to the trigger the catalogue names, null when none", () => {
    const triggers = parseOutcomeTriggers(BODY);
    expect(outcomeTriggerIdFor(triggers, "website_visit")).toBe("website_visited");
    expect(outcomeTriggerIdFor(triggers, "paid_client")).toBeNull();
  });
});

describe("getOutcomeTriggers", () => {
  const realFetch = globalThis.fetch;
  beforeEach(() => {
    clearStatsCache();
    process.env.FEATURES_SERVICE_URL = "http://features.test";
  });
  afterEach(() => {
    globalThis.fetch = realFetch;
  });

  it("reads /public/channels, cached; the channel read still answers from the same document", async () => {
    const fetchMock = vi.fn(async () => new Response(JSON.stringify(BODY), { status: 200 }));
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    expect(await getOutcomeTriggers()).toHaveLength(2);
    expect(await getOutcomeTriggers()).toHaveLength(2);
    expect(await getChannelCatalogue()).toEqual(BODY.channels);
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(String((fetchMock.mock.calls[0] as unknown[])[0])).toBe("http://features.test/public/channels");
  });

  it("a failed read throws and caches nothing", async () => {
    globalThis.fetch = (async () => new Response("down", { status: 502 })) as unknown as typeof fetch;
    await expect(getOutcomeTriggers()).rejects.toThrow(/502/);
    globalThis.fetch = (async () => new Response(JSON.stringify(BODY), { status: 200 })) as unknown as typeof fetch;
    expect(await getOutcomeTriggers()).toHaveLength(2);
  });
});
