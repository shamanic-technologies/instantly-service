import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  announceEvidenceChanged,
  normalizeEvidenceEmails,
  notifyEvidenceChanged,
} from "../../src/lib/evidence-changed";

const mockFetch = vi.fn();

beforeEach(() => {
  process.env.LEAD_SERVICE_URL = "https://lead.example";
  process.env.LEAD_SERVICE_API_KEY = "lead-key";
  mockFetch.mockReset();
  vi.stubGlobal("fetch", mockFetch);
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("evidence-changed — tells lead-service which addresses moved", () => {
  it("POSTs the locked contract: path, headers, body", async () => {
    mockFetch.mockResolvedValue(new Response(JSON.stringify({ accepted: 1 }), { status: 202 }));
    await notifyEvidenceChanged("org-1", ["a@b.com"]);
    expect(mockFetch).toHaveBeenCalledTimes(1);
    const [url, init] = mockFetch.mock.calls[0];
    expect(url).toBe("https://lead.example/orgs/leads/evidence-changed");
    expect(init.method).toBe("POST");
    expect(init.headers).toMatchObject({
      "x-api-key": "lead-key",
      "x-org-id": "org-1",
      "Content-Type": "application/json",
    });
    expect(JSON.parse(init.body)).toEqual({ emails: ["a@b.com"] });
  });

  it("chunks at the producer's 1000-address ceiling", async () => {
    mockFetch.mockResolvedValue(new Response("{}", { status: 202 }));
    const emails = Array.from({ length: 1500 }, (_, i) => `u${i}@x.com`);
    await notifyEvidenceChanged("org-1", emails);
    expect(mockFetch).toHaveBeenCalledTimes(2);
    expect(JSON.parse(mockFetch.mock.calls[0][1].body).emails).toHaveLength(1000);
    expect(JSON.parse(mockFetch.mock.calls[1][1].body).emails).toHaveLength(500);
  });

  it("the client fails loud on a non-2xx and on missing env", async () => {
    mockFetch.mockResolvedValue(new Response("nope", { status: 500 }));
    await expect(notifyEvidenceChanged("org-1", ["a@b.com"])).rejects.toThrow(/500/);
    delete process.env.LEAD_SERVICE_URL;
    await expect(notifyEvidenceChanged("org-1", ["a@b.com"])).rejects.toThrow(/LEAD_SERVICE_URL/);
  });

  it("normalizes: trimmed, lower-cased, distinct, empties dropped", () => {
    expect(normalizeEvidenceEmails([" A@B.com", "a@b.com", "", null, undefined, "c@d.com"])).toEqual([
      "a@b.com",
      "c@d.com",
    ]);
  });

  it("announce NEVER throws, and WARNS the failure loudly", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    mockFetch.mockRejectedValue(new TypeError("fetch failed"));
    await expect(announceEvidenceChanged("org-1", ["a@b.com"], "optout_recorded")).resolves.toBeUndefined();
    expect(warn).toHaveBeenCalledTimes(1);
    expect(String(warn.mock.calls[0][0])).toMatch(/evidence-changed NOT delivered.*org=org-1.*optout_recorded.*a@b\.com.*fetch failed/);
  });

  it("a platform send (no org) and an empty address list call nothing", async () => {
    await announceEvidenceChanged(null, ["a@b.com"], "event:email_sent");
    await announceEvidenceChanged("org-1", ["", null], "event:email_sent");
    expect(mockFetch).not.toHaveBeenCalled();
  });
});
