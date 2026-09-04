import { describe, it, expect, vi, beforeEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────

const mockDbExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...a: unknown[]) => mockDbExecute(...a) },
}));

const mockInsertEmailsBatch = vi.fn();
vi.mock("../../src/lib/bronze", () => ({
  insertEmailsBatch: (...a: unknown[]) => mockInsertEmailsBatch(...a),
}));

const mockListEmails = vi.fn();
vi.mock("../../src/lib/instantly-client", () => ({
  listEmails: (...a: unknown[]) => mockListEmails(...a),
}));

const mockResolveOrgKey = vi.fn();
const mockResolvePlatformKey = vi.fn();
vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: (...a: unknown[]) => mockResolveOrgKey(...a),
  resolvePlatformInstantlyApiKey: (...a: unknown[]) => mockResolvePlatformKey(...a),
}));

import {
  MIRRORED_INBOUND_EVENT_TYPES,
  fetchMirroredEmailRecords,
  hasExchangedMailEvidence,
  isInstantlyHeldCampaignId,
  maybeMirrorCampaignEmails,
  mirrorCampaignEmails,
} from "../../src/lib/mirror-emails";
import { REPLY_KINDS } from "../../src/lib/reply-kind";

/** Recursively extract SQL text fragments from a drizzle SQL object. */
function extractSqlText(obj: unknown): string {
  if (typeof obj === "string") return obj;
  if (obj == null) return "";
  if (Array.isArray(obj)) return obj.map(extractSqlText).join("");
  if (typeof obj === "object") {
    const o = obj as Record<string, unknown>;
    if (Array.isArray(o.value)) return o.value.join("");
    if (Array.isArray(o.queryChunks)) return extractSqlText(o.queryChunks);
    return Object.values(o).map(extractSqlText).join("");
  }
  return "";
}

/** node-postgres returns a QueryResult OBJECT, never a bare array. */
function pgResult<T>(rows: T[]) {
  return { command: "SELECT", rowCount: rows.length, oid: null, fields: [], rows };
}

const ORG_CAMPAIGN = {
  instantlyCampaignId: "ic-1",
  orgId: "org-1",
  userId: "user-1",
};

beforeEach(() => {
  vi.clearAllMocks();
  mockResolveOrgKey.mockResolvedValue({ key: "org-key" });
  mockResolvePlatformKey.mockResolvedValue("platform-key");
  mockListEmails.mockResolvedValue([{ id: "e1" }, { id: "e2" }]);
  mockInsertEmailsBatch.mockResolvedValue([{ id: "b1" }]);
});

describe("MIRRORED_INBOUND_EVENT_TYPES", () => {
  it("covers the webhook reply itself AND every reply KIND", () => {
    // A reply whose `reply_received` we never received still arrives as its
    // qualification, and that is the case the drift-gated poll already missed.
    expect(MIRRORED_INBOUND_EVENT_TYPES.has("reply_received")).toBe(true);
    expect(MIRRORED_INBOUND_EVENT_TYPES.has("email_bounced")).toBe(true);
    for (const kind of REPLY_KINDS) {
      expect(MIRRORED_INBOUND_EVENT_TYPES.has(kind)).toBe(true);
    }
  });

  it("does NOT fire on an outbound-only event", () => {
    // An `email_sent` carries no inbound words, and mirroring on every send
    // would read the whole thread once per dispatched step.
    expect(MIRRORED_INBOUND_EVENT_TYPES.has("email_sent")).toBe(false);
    expect(MIRRORED_INBOUND_EVENT_TYPES.has("email_opened")).toBe(false);
    expect(MIRRORED_INBOUND_EVENT_TYPES.has("email_link_clicked")).toBe(false);
  });
});

describe("isInstantlyHeldCampaignId", () => {
  it("excludes a self-send sequence and a reservation sentinel", () => {
    expect(isInstantlyHeldCampaignId("ic-1")).toBe(true);
    expect(isInstantlyHeldCampaignId("self:abc")).toBe(false);
    expect(isInstantlyHeldCampaignId("reserving:abc")).toBe(false);
  });
});

describe("mirrorCampaignEmails", () => {
  it("copies the WHOLE thread, not just the reply", async () => {
    const stored = await mirrorCampaignEmails(ORG_CAMPAIGN);

    expect(mockListEmails).toHaveBeenCalledWith("org-key", { campaignId: "ic-1" });
    expect(mockInsertEmailsBatch).toHaveBeenCalledWith("ic-1", "org-1", [
      { id: "e1" },
      { id: "e2" },
    ]);
    // Only NEW rows are reported — the insert conflicts and does nothing.
    expect(stored).toBe(1);
  });

  it("mirrors a PLATFORM send on the platform key rather than skipping it", async () => {
    await mirrorCampaignEmails({
      instantlyCampaignId: "ic-2",
      orgId: null,
      userId: null,
    });

    expect(mockResolvePlatformKey).toHaveBeenCalled();
    expect(mockResolveOrgKey).not.toHaveBeenCalled();
    expect(mockListEmails).toHaveBeenCalledWith("platform-key", { campaignId: "ic-2" });
    expect(mockInsertEmailsBatch).toHaveBeenCalledWith("ic-2", null, expect.any(Array));
  });
});

describe("maybeMirrorCampaignEmails", () => {
  it("mirrors on a real reply", async () => {
    await maybeMirrorCampaignEmails(ORG_CAMPAIGN, "reply_received");
    expect(mockListEmails).toHaveBeenCalledTimes(1);
  });

  it("no-ops on an event that carries no inbound message", async () => {
    await maybeMirrorCampaignEmails(ORG_CAMPAIGN, "email_sent");
    expect(mockListEmails).not.toHaveBeenCalled();
  });

  it("never asks Instantly about a sequence it never carried", async () => {
    await maybeMirrorCampaignEmails(
      { ...ORG_CAMPAIGN, instantlyCampaignId: "self:abc" },
      "reply_received",
    );
    await maybeMirrorCampaignEmails(
      { ...ORG_CAMPAIGN, instantlyCampaignId: "reserving:abc" },
      "reply_received",
    );
    expect(mockListEmails).not.toHaveBeenCalled();
  });

  it("is fail-soft — a mirror failure never throws into event promotion", async () => {
    // It runs inside promoteEvent, and on the webhook path a throw becomes a
    // 5xx that Instantly counts toward disabling the whole subscription.
    mockListEmails.mockRejectedValue(new Error("Instantly 503"));
    await expect(
      maybeMirrorCampaignEmails(ORG_CAMPAIGN, "reply_received"),
    ).resolves.toBeUndefined();
  });
});

describe("fetchMirroredEmailRecords", () => {
  it("returns the stored payloads, which ARE the Instantly records", async () => {
    mockDbExecute.mockResolvedValue(
      pgResult([{ payload: { id: "e1" } }, { payload: { id: "e2" } }]),
    );

    const records = await fetchMirroredEmailRecords("ic-1");

    expect(records).toEqual([{ id: "e1" }, { id: "e2" }]);
    const text = extractSqlText(mockDbExecute.mock.calls[0][0]);
    expect(text).toContain("instantly_emails_raw");
  });

  it("survives node-postgres handing back a QueryResult, never a bare array", async () => {
    mockDbExecute.mockResolvedValue(pgResult([]));
    await expect(fetchMirroredEmailRecords("ic-1")).resolves.toEqual([]);
  });
});

describe("hasExchangedMailEvidence", () => {
  it("is true when a REAL send or inbound event is on record", async () => {
    mockDbExecute.mockResolvedValue(pgResult([{ present: 1 }]));
    await expect(hasExchangedMailEvidence("ic-1")).resolves.toBe(true);
  });

  it("ignores inferred events — they assert a message nobody witnessed", async () => {
    mockDbExecute.mockResolvedValue(pgResult([]));

    await expect(hasExchangedMailEvidence("ic-1")).resolves.toBe(false);
    const text = extractSqlText(mockDbExecute.mock.calls[0][0]);
    expect(text).toContain("inferred = false");
  });
});
