import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────

const mockDbExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...a: unknown[]) => mockDbExecute(...a) },
}));

const mockLoadCredentialedMailboxes = vi.fn();
vi.mock("../../src/lib/self-send/mailbox-credentials", () => ({
  loadCredentialedMailboxes: (...a: unknown[]) => mockLoadCredentialedMailboxes(...a),
}));

import {
  chooseSequenceTransport,
  fetchTransportAssignmentCounts,
  isTransportSplitEnabled,
  resolveTransportForNewSequence,
} from "../../src/lib/self-send/transport-split";
import { clearStatsCache } from "../../src/lib/stats-cache";

const CALLER = { method: "POST", path: "/orgs/send" } as const;

/**
 * What `db.execute` ACTUALLY resolves to in this service.
 *
 * instantly-service runs on node-postgres, whose driver returns a `QueryResult`
 * object — not an array. Every mock here goes through this helper so the unit
 * tests exercise the same shape production does; the previous mocks returned a
 * bare array (the postgres.js shape), which is why a query that threw
 * `rows is not iterable` on the very first send shipped green.
 */
function pgResult<T>(rows: T[]) {
  return { command: "SELECT", rowCount: rows.length, oid: null, fields: [], rows };
}

beforeEach(() => {
  vi.resetAllMocks();
  clearStatsCache();
  delete process.env.SEND_TRANSPORT_AB_ENABLED;
  mockDbExecute.mockResolvedValue(pgResult([]));
  mockLoadCredentialedMailboxes.mockResolvedValue(
    new Set(["kevin@boostdistribute.com", "kevinl@marketingagency.life"]),
  );
});

afterEach(() => {
  delete process.env.SEND_TRANSPORT_AB_ENABLED;
  clearStatsCache();
});

// ─── Pure choice ─────────────────────────────────────────────────────────────

describe("chooseSequenceTransport", () => {
  it("sends the next sequence to whichever pipe has fewer assignments", () => {
    expect(chooseSequenceTransport({ instantly: 10, smtp: 4 })).toBe("smtp");
    expect(chooseSequenceTransport({ instantly: 4, smtp: 10 })).toBe("instantly");
  });

  it("gives a tie to smtp — the pipe the experiment exists to learn about", () => {
    expect(chooseSequenceTransport({ instantly: 7, smtp: 7 })).toBe("smtp");
    expect(chooseSequenceTransport({ instantly: 0, smtp: 0 })).toBe("smtp");
  });

  it("converges rather than oscillating: repeated picks even the arms out", () => {
    const counts = { instantly: 0, smtp: 0 };
    for (let i = 0; i < 20; i += 1) {
      const pick = chooseSequenceTransport(counts);
      if (pick === "smtp") counts.smtp += 1;
      else counts.instantly += 1;
    }
    expect(counts.smtp).toBe(10);
    expect(counts.instantly).toBe(10);
  });
});

// ─── Counting ────────────────────────────────────────────────────────────────

describe("fetchTransportAssignmentCounts", () => {
  it("counts each pipe's assignments, treating an unset transport as instantly", async () => {
    mockDbExecute.mockResolvedValue(pgResult([
      { send_transport: "instantly", n: 12 },
      { send_transport: "smtp", n: 5 },
      { send_transport: null, n: 3 },
    ]));

    expect(await fetchTransportAssignmentCounts()).toEqual({ instantly: 15, smtp: 5 });
  });

  it("counts a 24h window and excludes reservation sentinels", async () => {
    await fetchTransportAssignmentCounts();

    const sqlText = JSON.stringify(mockDbExecute.mock.calls[0][0]);
    expect(sqlText).toContain("24 hours");
    expect(sqlText).toContain("reserving:%");
    // Assignments, not dispatches: Instantly's queue must not steer our split.
    expect(sqlText).toContain("instantly_campaigns");
  });

  it("reads a node-postgres QueryResult, not an array — the shape prod returns", async () => {
    // Regression, 2026-08-30: the query result was cast `as unknown as Array<…>`
    // and iterated directly. `tsc` stayed green because the cast lies; every
    // /send in production then threw `rows is not iterable` from 06:46 UTC on,
    // and no lead reached a sending pipe at all until the experiment was
    // disarmed on the box. The counting must read `.rows`.
    mockDbExecute.mockResolvedValue({
      command: "SELECT",
      rowCount: 2,
      oid: null,
      fields: [],
      rows: [
        { send_transport: "instantly", n: 8 },
        { send_transport: "smtp", n: 3 },
      ],
    });

    await expect(fetchTransportAssignmentCounts()).resolves.toEqual({
      instantly: 8,
      smtp: 3,
    });
  });

  it("reports zeroes when nothing has been assigned yet", async () => {
    mockDbExecute.mockResolvedValue(pgResult([]));
    expect(await fetchTransportAssignmentCounts()).toEqual({ instantly: 0, smtp: 0 });
  });
});

// ─── Resolution on the send path ─────────────────────────────────────────────

describe("resolveTransportForNewSequence", () => {
  const account = { email: "kevin@boostdistribute.com", sendTransport: null };

  it("stays on instantly while the experiment is disarmed", async () => {
    mockDbExecute.mockResolvedValue(pgResult([{ send_transport: "instantly", n: 99 }]));

    expect(await resolveTransportForNewSequence(account, CALLER)).toBe("instantly");
    // Disarmed means it does not even look at the counts.
    expect(mockDbExecute).not.toHaveBeenCalled();
  });

  it("balances once armed", async () => {
    process.env.SEND_TRANSPORT_AB_ENABLED = "true";
    mockDbExecute.mockResolvedValue(pgResult([{ send_transport: "instantly", n: 9 }]));

    expect(await resolveTransportForNewSequence(account, CALLER)).toBe("smtp");
  });

  it("resolves a real transport on the armed, capable path — never throws", async () => {
    // The failing branch: armed AND self-send-capable is the only path that
    // reaches the counting query, which is exactly why the outage shipped with
    // a green suite. Driven through the driver's real result shape.
    process.env.SEND_TRANSPORT_AB_ENABLED = "true";
    mockDbExecute.mockResolvedValue({
      command: "SELECT",
      rowCount: 1,
      oid: null,
      fields: [],
      rows: [{ send_transport: "smtp", n: 11 }],
    });

    await expect(resolveTransportForNewSequence(account, CALLER)).resolves.toBe(
      "instantly",
    );
  });

  it("honours an account explicitly pinned to smtp, armed or not", async () => {
    const pinned = { email: "pinned@boostdistribute.com", sendTransport: "smtp" };

    expect(await resolveTransportForNewSequence(pinned, CALLER)).toBe("smtp");
    // The manual override is also the rollback lever: never re-decided here, and
    // never gated on a credential lookup.
    expect(mockLoadCredentialedMailboxes).not.toHaveBeenCalled();
  });

  it("keeps a mailbox we cannot authenticate on instantly", async () => {
    process.env.SEND_TRANSPORT_AB_ENABLED = "true";
    mockDbExecute.mockResolvedValue(pgResult([{ send_transport: "instantly", n: 99 }]));

    // A DFY mailbox: Instantly owns the Workspace, so no credential exists and
    // the self-send pipe could never dispatch from it.
    const dfy = { email: "athena@vibrancesense.com", sendTransport: null };
    expect(await resolveTransportForNewSequence(dfy, CALLER)).toBe("instantly");
  });

  it("matches the credential list case-insensitively", async () => {
    process.env.SEND_TRANSPORT_AB_ENABLED = "true";
    mockDbExecute.mockResolvedValue(pgResult([{ send_transport: "instantly", n: 9 }]));

    const upper = { email: "Kevin@BoostDistribute.com", sendTransport: null };
    expect(await resolveTransportForNewSequence(upper, CALLER)).toBe("smtp");
  });

  it("reads the credential sources once across sends rather than per send", async () => {
    process.env.SEND_TRANSPORT_AB_ENABLED = "true";
    mockDbExecute.mockResolvedValue(pgResult([{ send_transport: "instantly", n: 9 }]));

    await resolveTransportForNewSequence(account, CALLER);
    await resolveTransportForNewSequence(account, CALLER);
    await resolveTransportForNewSequence(account, CALLER);

    // key-service + a vendor pagination on every send would be the cost this
    // cache exists to avoid.
    expect(mockLoadCredentialedMailboxes).toHaveBeenCalledTimes(1);
  });

  it("fails loud when the credential sources cannot be read", async () => {
    process.env.SEND_TRANSPORT_AB_ENABLED = "true";
    mockLoadCredentialedMailboxes.mockRejectedValue(new Error("key-service down"));

    await expect(resolveTransportForNewSequence(account, CALLER)).rejects.toThrow(
      "key-service down",
    );
  });
});

describe("isTransportSplitEnabled", () => {
  it("is off unless the variable is exactly 'true', and is read at use", () => {
    expect(isTransportSplitEnabled()).toBe(false);
    process.env.SEND_TRANSPORT_AB_ENABLED = "yes";
    expect(isTransportSplitEnabled()).toBe(false);
    process.env.SEND_TRANSPORT_AB_ENABLED = "true";
    expect(isTransportSplitEnabled()).toBe(true);
  });
});
