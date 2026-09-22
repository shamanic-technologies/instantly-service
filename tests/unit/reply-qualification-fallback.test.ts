import { describe, it, expect, vi, beforeEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────

const mockDbExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...a: unknown[]) => mockDbExecute(...a) },
}));

const mockMirror = vi.fn();
vi.mock("../../src/lib/mirror-emails", () => ({
  maybeMirrorCampaignEmails: (...a: unknown[]) => mockMirror(...a),
}));

const mockFetchInbound = vi.fn();
vi.mock("../../src/lib/reply-opt-out", () => ({
  fetchLatestMirroredInbound: (...a: unknown[]) => mockFetchInbound(...a),
}));

const mockPromote = vi.fn();
vi.mock("../../src/lib/silver-promote", () => ({
  promoteEvent: (...a: unknown[]) => mockPromote(...a),
}));

const mockQualify = vi.fn();
vi.mock("../../src/lib/self-send/qualify-reply", async () => {
  const actual = await vi.importActual<
    typeof import("../../src/lib/self-send/qualify-reply")
  >("../../src/lib/self-send/qualify-reply");
  return { ...actual, qualifyReply: (...a: unknown[]) => mockQualify(...a) };
});

import {
  QUALIFICATION_GRACE_MS,
  QUALIFICATION_MAX_AGE_MS,
  qualifyOneReply,
  runReplyQualificationFallback,
  selectUnqualifiedReplies,
  type UnqualifiedReply,
} from "../../src/lib/reply-qualification-fallback";
import { QUALIFICATION_EVENT_TYPES } from "../../src/lib/self-send/qualify-reply";

/** node-postgres returns a QueryResult OBJECT, never a bare array. */
function pgResult<T>(rows: T[]) {
  return { command: "SELECT", rowCount: rows.length, oid: null, fields: [], rows };
}

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

function reply(over: Partial<UnqualifiedReply> = {}): UnqualifiedReply {
  return {
    instantlyCampaignId: "ic-1",
    leadEmail: "jamie@clinic.com",
    accountEmail: "matthew@sender.com",
    orgId: "org-1",
    userId: "user-1",
    repliedAt: new Date("2026-09-21T13:50:00.000Z"),
    ...over,
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  mockMirror.mockResolvedValue(0);
  mockPromote.mockResolvedValue({ promoted: true, silverEventId: "e1" });
});

describe("selectUnqualifiedReplies — which replies are still waiting", () => {
  it("asks for the absence of EVERY reply kind, not of one", async () => {
    mockDbExecute.mockResolvedValue(pgResult([]));

    await selectUnqualifiedReplies(10);

    const text = extractSqlText(mockDbExecute.mock.calls[0]?.[0]);
    // A reply Instantly filed `lead_out_of_office` HAS a verdict — re-reading it
    // would overrule better evidence (it saw the real headers) with worse.
    for (const kind of QUALIFICATION_EVENT_TYPES) {
      expect(text.includes(kind) || JSON.stringify(mockDbExecute.mock.calls[0]).includes(kind)).toBe(
        true,
      );
    }
  });

  it("excludes the self-send and reservation sentinels", async () => {
    mockDbExecute.mockResolvedValue(pgResult([]));

    await selectUnqualifiedReplies(10);

    const text = extractSqlText(mockDbExecute.mock.calls[0]?.[0]);
    expect(text).toContain("NOT LIKE 'self:%'");
    expect(text).toContain("NOT LIKE 'reserving:%'");
  });

  it("only considers a REAL reply, never an inferred one", async () => {
    mockDbExecute.mockResolvedValue(pgResult([]));

    await selectUnqualifiedReplies(10);

    const text = extractSqlText(mockDbExecute.mock.calls[0]?.[0]);
    expect(text).toContain("e.inferred = false");
  });

  it("gives Instantly the grace period before the reply is eligible", async () => {
    mockDbExecute.mockResolvedValue(pgResult([]));
    const asOf = new Date("2026-09-21T15:00:00.000Z");

    await selectUnqualifiedReplies(10, asOf);

    const bound = JSON.stringify(mockDbExecute.mock.calls[0]);
    expect(bound).toContain(
      new Date(asOf.getTime() - QUALIFICATION_GRACE_MS).toISOString(),
    );
  });

  it("will not reach back past the age floor — an old reply must not ring a phone", async () => {
    mockDbExecute.mockResolvedValue(pgResult([]));
    const asOf = new Date("2026-09-21T15:00:00.000Z");

    await selectUnqualifiedReplies(10, asOf);

    // Promoting a kind rings the rep, runs the funded campaign and files a
    // follow-up debt — all of which claim the buyer is waiting NOW. The first
    // production sweep drained a July reply into a POSITIVE kind and only
    // stayed silent because that brand had no rep number.
    const bound = JSON.stringify(mockDbExecute.mock.calls[0]);
    expect(bound).toContain(
      new Date(asOf.getTime() - QUALIFICATION_MAX_AGE_MS).toISOString(),
    );
  });

  it("bounds the window at BOTH ends, so the floor is older than the grace cutoff", () => {
    // The two constants only make sense as a pair: too young is Instantly's
    // turn to answer, too old is nobody's. A floor shorter than the grace
    // period would select nothing at all.
    expect(QUALIFICATION_MAX_AGE_MS).toBeGreaterThan(QUALIFICATION_GRACE_MS);
  });

  it("drops a row whose reply timestamp cannot be read rather than dating it now", async () => {
    mockDbExecute.mockResolvedValue(
      pgResult([
        {
          instantly_campaign_id: "ic-1",
          lead_email: "a@b.com",
          account_email: null,
          org_id: "org-1",
          user_id: null,
          replied_at: "not-a-date",
        },
      ]),
    );

    expect(await selectUnqualifiedReplies(10)).toEqual([]);
  });
});

describe("qualifyOneReply — classifying what Instantly would not", () => {
  it("re-runs the mirror FIRST — the retry the missing verdict never triggered", async () => {
    mockFetchInbound.mockResolvedValue({ instantlyEmailId: "e-9", text: "How much?" });
    mockQualify.mockResolvedValue("lead_info_requested");

    await qualifyOneReply(reply());

    expect(mockMirror).toHaveBeenCalledWith(
      { instantlyCampaignId: "ic-1", orgId: "org-1", userId: "user-1" },
      "reply_received",
    );
    // Order matters: reading before mirroring would find the same empty mirror
    // that the missing qualification left behind.
    expect(mockMirror.mock.invocationCallOrder[0]).toBeLessThan(
      mockFetchInbound.mock.invocationCallOrder[0],
    );
  });

  it("promotes the kind, which is what re-opens every downstream gate", async () => {
    mockFetchInbound.mockResolvedValue({ instantlyEmailId: "e-9", text: "yes please" });
    mockQualify.mockResolvedValue("lead_interested");

    const outcome = await qualifyOneReply(reply());

    expect(outcome).toEqual({ classified: true, eventType: "lead_interested" });
    expect(mockPromote).toHaveBeenCalledTimes(1);
    const promoted = mockPromote.mock.calls[0][0];
    expect(promoted.eventType).toBe("lead_interested");
    expect(promoted.instantlyCampaignId).toBe("ic-1");
    expect(promoted.sourceRowId).toBe("e-9");
  });

  it("carries the REPLY's own timestamp, never the sweep's", async () => {
    mockFetchInbound.mockResolvedValue({ instantlyEmailId: "e-9", text: "sure" });
    mockQualify.mockResolvedValue("lead_interested");

    await qualifyOneReply(reply());

    // A kind timestamped now would sort ahead of a later, better verdict from
    // Instantly, and the gold projection takes the latest.
    expect(mockPromote.mock.calls[0][0].timestamp).toEqual(
      new Date("2026-09-21T13:50:00.000Z"),
    );
  });

  it("says it came from OUR classifier, not from Instantly's verdict", async () => {
    mockFetchInbound.mockResolvedValue({ instantlyEmailId: "e-9", text: "sure" });
    mockQualify.mockResolvedValue("lead_interested");

    await qualifyOneReply(reply());

    // `poll_emails` reads the same table but means Instantly's OWN judgement.
    expect(mockPromote.mock.calls[0][0].source).toBe("emails_backfill");
    expect(mockPromote.mock.calls[0][0].source).not.toBe("poll_emails");
  });

  it("promotes NOTHING when the body cannot be read", async () => {
    mockFetchInbound.mockResolvedValue(null);

    expect(await qualifyOneReply(reply())).toEqual({
      classified: false,
      reason: "no_body",
    });
    expect(mockQualify).not.toHaveBeenCalled();
    expect(mockPromote).not.toHaveBeenCalled();
  });

  it("promotes NOTHING when the classifier returns no usable answer", async () => {
    mockFetchInbound.mockResolvedValue({ instantlyEmailId: "e-9", text: "???" });
    mockQualify.mockResolvedValue(null);

    expect(await qualifyOneReply(reply())).toEqual({
      classified: false,
      reason: "unqualified",
    });
    // Defaulting to neutral would open every gate on a reading we never obtained.
    expect(mockPromote).not.toHaveBeenCalled();
  });
});

describe("runReplyQualificationFallback — the sweep", () => {
  it("one failing reply does not stop the others", async () => {
    mockDbExecute.mockResolvedValue(
      pgResult([
        {
          instantly_campaign_id: "ic-1",
          lead_email: "a@b.com",
          account_email: null,
          org_id: "org-1",
          user_id: null,
          replied_at: "2026-09-21T13:50:00.000Z",
        },
        {
          instantly_campaign_id: "ic-2",
          lead_email: "c@d.com",
          account_email: null,
          org_id: "org-1",
          user_id: null,
          replied_at: "2026-09-21T13:55:00.000Z",
        },
      ]),
    );
    mockFetchInbound
      .mockRejectedValueOnce(new Error("chat-service 502"))
      .mockResolvedValueOnce({ instantlyEmailId: "e-2", text: "STOP!" });
    mockQualify.mockResolvedValue("lead_opt_out_requested");

    const summary = await runReplyQualificationFallback();

    expect(summary.candidates).toBe(2);
    expect(summary.failed).toBe(1);
    expect(summary.classified).toBe(1);
  });
});
