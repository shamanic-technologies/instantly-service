import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const ORIGINAL_ENV = { ...process.env };

beforeEach(() => {
  vi.resetModules();
  process.env.LEAD_SERVICE_URL = "https://lead.example";
  process.env.LEAD_SERVICE_API_KEY = "lead-key";
});

afterEach(() => {
  process.env = { ...ORIGINAL_ENV };
  vi.unstubAllGlobals();
});

const RESPONSE = {
  followup: {
    id: "row-1",
    leadId: "lead-1",
    campaignId: "camp-1",
    dueAt: "2026-09-05T10:00:00.000Z",
    claimedAt: null,
    followupCount: 0,
    lastActionAt: null,
    stoppedReason: null,
  },
  leadId: "lead-1",
  email: "prospect@example.com",
};

describe("scheduleFollowupByEmail", () => {
  it("POSTs the deployed path with the org identity, and returns the debt", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => RESPONSE,
    });
    vi.stubGlobal("fetch", fetchMock);

    const { scheduleFollowupByEmail } = await import("../../src/lib/lead-client");
    const result = await scheduleFollowupByEmail({
      orgId: "org-1",
      campaignId: "camp-1",
      email: "prospect@example.com",
      dueAt: "2026-09-05T10:00:00.000Z",
    });

    expect(result).toEqual(RESPONSE);
    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toBe(
      "https://lead.example/orgs/campaigns/camp-1/followups/schedule-by-email",
    );
    expect(init.method).toBe("POST");
    expect(init.headers).toMatchObject({
      "x-api-key": "lead-key",
      "x-org-id": "org-1",
    });
    // The org and the campaign travel in the header and the path; only the person
    // and the due date are the body lead-service documents.
    expect(JSON.parse(String(init.body))).toEqual({
      email: "prospect@example.com",
      dueAt: "2026-09-05T10:00:00.000Z",
    });
  });

  it("encodes the campaign id into the path", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, json: async () => RESPONSE });
    vi.stubGlobal("fetch", fetchMock);

    const { scheduleFollowupByEmail } = await import("../../src/lib/lead-client");
    await scheduleFollowupByEmail({
      orgId: "org-1",
      campaignId: "camp/1 2",
      email: "p@example.com",
      dueAt: "2026-09-05T10:00:00.000Z",
    });

    expect(String((fetchMock.mock.calls[0] as [string])[0])).toContain("camp%2F1%202");
  });

  // Fails loud: the caller is fail-soft and swallows this, but it must arrive with
  // its reason — a client that degraded to silence would reproduce the very bug
  // this closes (a queue nobody wrote to, with nothing anywhere saying so).
  it("throws with the status and the refusal body", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: false,
        status: 409,
        text: async () => '{"code":"ambiguous_lead"}',
      }),
    );

    const { scheduleFollowupByEmail } = await import("../../src/lib/lead-client");
    await expect(
      scheduleFollowupByEmail({
        orgId: "org-1",
        campaignId: "camp-1",
        email: "p@example.com",
        dueAt: "2026-09-05T10:00:00.000Z",
      }),
    ).rejects.toThrow(/409.*ambiguous_lead/);
  });

  it("throws when the service is not configured", async () => {
    delete process.env.LEAD_SERVICE_URL;
    const { scheduleFollowupByEmail } = await import("../../src/lib/lead-client");
    await expect(
      scheduleFollowupByEmail({
        orgId: "org-1",
        campaignId: "camp-1",
        email: "p@example.com",
        dueAt: "2026-09-05T10:00:00.000Z",
      }),
    ).rejects.toThrow(/LEAD_SERVICE_URL/);
  });
});
