import { describe, it, expect, vi, beforeEach } from "vitest";

const mockDbExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...a: unknown[]) => mockDbExecute(...a) },
}));

import {
  DEFAULT_REPLY_SENDER,
  findHumanTakeover,
  resolveReplySender,
} from "../../src/lib/human-takeover";
import { MANUAL_REPLY_STEP } from "../../src/lib/manual-reply-step";

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

beforeEach(() => {
  vi.resetAllMocks();
});

describe("who asked for a reply to be sent", () => {
  it("resolves an absent sender to automation, not to human", () => {
    // ⚠️ This is the decision that makes the gate live without waiting on
    // workflow-service to declare itself. The automated responder is the only
    // caller of POST /orgs/replies in the fleet, so an undeclared reply is
    // its. Flipping this to "human" would leave the gate inert, which is to
    // say it would not prevent the thing it exists to prevent.
    expect(DEFAULT_REPLY_SENDER).toBe("automation");
    expect(resolveReplySender(undefined)).toBe("automation");
    expect(resolveReplySender(null)).toBe("automation");
  });

  it("keeps a declared sender verbatim", () => {
    expect(resolveReplySender("human")).toBe("human");
    expect(resolveReplySender("automation")).toBe("automation");
  });
});

describe("findHumanTakeover", () => {
  it("reads BOTH places an answer can have gone out from", async () => {
    // Neither source alone is enough, and this is measured rather than
    // reasoned: of the three replies this service had dispatched in
    // production, TWO were absent from Instantly's own mirror — so a gate
    // reading only the mirror would miss our own sends. And every human
    // takeover observed so far happened in Instantly's Unibox, which never
    // touches smtp_dispatch_raw.
    mockDbExecute.mockResolvedValueOnce(pgResult([]));
    await findHumanTakeover("ic-1");

    const sqlText = extractSqlText(mockDbExecute.mock.calls[0][0]);
    expect(sqlText).toContain("smtp_dispatch_raw");
    expect(sqlText).toContain("instantly_emails_raw");
  });

  it("counts a dispatched reply only when it DECLARED itself human", async () => {
    // This predicate is the whole backfill. Every step-0 row written before
    // `sentBy` existed was the automated responder's — it was the only caller
    // — so requiring the declaration correctly leaves all of them out, with no
    // data migration and no guess.
    mockDbExecute.mockResolvedValueOnce(pgResult([]));
    await findHumanTakeover("ic-1");

    const sqlText = extractSqlText(mockDbExecute.mock.calls[0][0]);
    expect(sqlText).toContain("d.payload->>'sentBy' = 'human'");
  });

  it("counts an Instantly manual send only when our dispatcher did not make it", async () => {
    // A ue_type 3/4 row we can tie back to one of our own step-0 dispatches is
    // ours; one we cannot was typed by a person, by construction.
    mockDbExecute.mockResolvedValueOnce(pgResult([]));
    await findHumanTakeover("ic-1");

    const sqlText = extractSqlText(mockDbExecute.mock.calls[0][0]);
    expect(sqlText).toContain("e.payload->>'ue_type' IN ('3', '4')");
    expect(sqlText).toContain("NOT EXISTS");
    expect(sqlText).toContain("d.payload->>'instantlyEmailId' = e.instantly_email_id");
  });

  it("only counts an answer that came AFTER the prospect last wrote", async () => {
    // An answer BEFORE their latest message is part of the conversation they
    // then replied to — the responder is owed a turn. Only an answer since is
    // a takeover.
    mockDbExecute.mockResolvedValueOnce(pgResult([]));
    await findHumanTakeover("ic-1");

    const sqlText = extractSqlText(mockDbExecute.mock.calls[0][0]);
    expect(sqlText).toContain("latest_inbound");
    expect(sqlText).toContain("h.at > li.at");
  });

  it("compares times in UTC explicitly, never against the session zone", async () => {
    // `dispatched_at` / `polled_at` are naive timestamps while Instantly's
    // `timestamp_email` carries a zone. An implicit coercion would resolve the
    // naive ones against whatever TimeZone the session happens to hold, and
    // this comparison decides whether a reply goes out.
    mockDbExecute.mockResolvedValueOnce(pgResult([]));
    await findHumanTakeover("ic-1");

    const sqlText = extractSqlText(mockDbExecute.mock.calls[0][0]);
    expect(sqlText).toContain("d.dispatched_at AT TIME ZONE 'UTC'");
    expect(sqlText).toContain("COALESCE(m.received_at, m.polled_at) AT TIME ZONE 'UTC'");
  });

  it("reads step 0 through the shared constant, not a literal", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([]));
    await findHumanTakeover("ic-1");

    const params = (mockDbExecute.mock.calls[0][0] as { queryChunks?: unknown[] })
      .queryChunks;
    expect(JSON.stringify(params)).toContain(String(MANUAL_REPLY_STEP));
  });

  it("returns null when the only answers since are ours", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([]));
    expect(await findHumanTakeover("ic-1")).toBeNull();
  });

  it("names when a person answered, and where we learned it", async () => {
    mockDbExecute.mockResolvedValueOnce(
      pgResult([
        { at: new Date("2026-09-04T17:51:31.000Z"), source: "instantly_unibox" },
      ]),
    );

    expect(await findHumanTakeover("ic-1")).toEqual({
      at: "2026-09-04T17:51:31.000Z",
      source: "instantly_unibox",
    });
  });

  it("reads a dispatched answer back as dispatched", async () => {
    mockDbExecute.mockResolvedValueOnce(
      pgResult([{ at: "2026-09-21T15:23:10.000Z", source: "dispatched" }]),
    );

    const takeover = await findHumanTakeover("ic-1");
    expect(takeover?.source).toBe("dispatched");
    expect(takeover?.at).toBe("2026-09-21T15:23:10.000Z");
  });

  it("fails loud when it cannot read its own history", async () => {
    // A gate that cannot see what went out must not wave a send through.
    mockDbExecute.mockRejectedValueOnce(new Error("connection terminated"));
    await expect(findHumanTakeover("ic-1")).rejects.toThrow("connection terminated");
  });
});
