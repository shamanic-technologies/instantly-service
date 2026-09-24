import { describe, it, expect, vi, beforeEach } from "vitest";

const mockFetch = vi.fn();
vi.stubGlobal("fetch", mockFetch);

import { sendEmail } from "../../src/lib/email-client";

function ok(body: unknown) {
  return { ok: true, status: 200, json: async () => body, text: async () => JSON.stringify(body) };
}

const PARAMS = {
  appId: "instantly-service",
  eventType: "reply-escalation",
  recipientEmail: "kevin@distribute.you",
};
const IDENTITY = { orgId: "org-1", userId: "user-1", runId: "run-1" };

beforeEach(() => mockFetch.mockReset());

describe("sendEmail — a 2xx is not a send", () => {
  it("resolves when every recipient was sent", async () => {
    mockFetch.mockResolvedValue(ok({ results: [{ email: "kevin@distribute.you", sent: true }] }));
    await expect(sendEmail(PARAMS, IDENTITY)).resolves.toBeUndefined();
  });

  it("forwards the caller's run as x-run-id", async () => {
    mockFetch.mockResolvedValue(ok({ results: [{ email: "kevin@distribute.you", sent: true }] }));
    await sendEmail(PARAMS, IDENTITY);
    const [, init] = mockFetch.mock.calls[0];
    expect(init.headers["x-run-id"]).toBe("run-1");
  });

  it("THROWS when the service answered 2xx but sent nothing", async () => {
    // Measured in prod 2026-09-24: run creation 409'd, the service returned 200
    // with sent:false, and the escalation reported the inbox as notified.
    mockFetch.mockResolvedValue(
      ok({
        results: [
          { email: "kevin@distribute.you", sent: false, reason: "Run creation failed: 409" },
        ],
      }),
    );
    await expect(sendEmail(PARAMS, IDENTITY)).rejects.toThrow(/did not send reply-escalation.*Run creation failed/);
  });

  it("treats a duplicate as already delivered, not as a failure", async () => {
    mockFetch.mockResolvedValue(
      ok({ results: [{ email: "kevin@distribute.you", sent: false, reason: "duplicate" }] }),
    );
    await expect(sendEmail(PARAMS, IDENTITY)).resolves.toBeUndefined();
  });
});
