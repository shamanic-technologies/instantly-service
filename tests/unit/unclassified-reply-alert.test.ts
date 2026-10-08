import { describe, it, expect, vi, beforeEach } from "vitest";

const mockDbExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...a: unknown[]) => mockDbExecute(...a) },
}));

const mockSendEmail = vi.fn();
vi.mock("../../src/lib/email-client", () => ({
  sendEmail: (...a: unknown[]) => mockSendEmail(...a),
}));

import {
  UNCLASSIFIED_ALERT_AFTER_MS,
  alertUnclassifiedReply,
  type UnclassifiedReplyAlertInput,
} from "../../src/lib/unclassified-reply-alert";

/** node-postgres returns a QueryResult OBJECT, never a bare array. */
function pgResult<T>(rows: T[]) {
  return { command: "UPDATE", rowCount: rows.length, oid: null, fields: [], rows };
}

const repliedAt = new Date("2026-10-01T13:49:00.000Z");
const later = new Date(repliedAt.getTime() + UNCLASSIFIED_ALERT_AFTER_MS);

function input(over: Partial<UnclassifiedReplyAlertInput> = {}): UnclassifiedReplyAlertInput {
  return {
    instantlyCampaignId: "ic-1",
    leadEmail: "pam@clinic.com",
    orgId: "org-1",
    userId: "user-1",
    repliedAt,
    reason: "unqualified",
    bodyText: "On … wrote:\n> our pitch\n\nSTOP!",
    ...over,
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  mockSendEmail.mockResolvedValue(undefined);
});

describe("alertUnclassifiedReply", () => {
  it("waits out the alert delay: a late mirror is not yet a give-up", async () => {
    const early = new Date(later.getTime() - 1);
    expect(await alertUnclassifiedReply(input(), early)).toBe("too_early");
    expect(mockDbExecute).not.toHaveBeenCalled();
    expect(mockSendEmail).not.toHaveBeenCalled();
  });

  it("emails the agency inbox once, with the reply's words", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([{ instantly_campaign_id: "ic-1" }]));

    expect(await alertUnclassifiedReply(input(), later)).toBe("sent");

    expect(mockSendEmail).toHaveBeenCalledTimes(1);
    const [params, identity] = mockSendEmail.mock.calls[0];
    expect(params.eventType).toBe("reply-unclassified");
    expect(params.metadata).toMatchObject({
      leadEmail: "pam@clinic.com",
      instantlyCampaignId: "ic-1",
      repliedAt: repliedAt.toISOString(),
    });
    expect(params.metadata.body).toContain("STOP!");
    expect(identity).toMatchObject({ orgId: "org-1", userId: "user-1" });
  });

  it("does not email again for the same reply (claim already held)", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([]));
    expect(await alertUnclassifiedReply(input(), later)).toBe("already_sent");
    expect(mockSendEmail).not.toHaveBeenCalled();
  });

  it("releases the claim and throws when the email cannot be sent", async () => {
    mockDbExecute
      .mockResolvedValueOnce(pgResult([{ instantly_campaign_id: "ic-1" }]))
      .mockResolvedValueOnce(pgResult([]));
    mockSendEmail.mockRejectedValue(new Error("gateway down"));

    await expect(alertUnclassifiedReply(input(), later)).rejects.toThrow("gateway down");
    // Second statement = the release, so the next tick retries.
    expect(mockDbExecute).toHaveBeenCalledTimes(2);
  });

  it("names a no-body reply as such", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([{ instantly_campaign_id: "ic-1" }]));
    await alertUnclassifiedReply(input({ reason: "no_body", bodyText: null, orgId: null, userId: null }), later);
    const [params, identity] = mockSendEmail.mock.calls[0];
    expect(params.metadata.body).toBe("(no body)");
    expect(params.metadata.why).toMatch(/could not read/);
    expect(identity).toMatchObject({ orgId: "system", userId: "system" });
  });
});
