import { describe, it, expect, vi, beforeEach } from "vitest";

// GMAIL-THREADS-RUNS-NEVER-CLOSE-1022: every reader run a pass opens in
// runs-service must END in that pass (14,952 were left `running` for good).

const execute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => execute(...a) } }));

const createRun = vi.fn();
const updateRun = vi.fn();
vi.mock("../../src/lib/runs-client", () => ({
  createRun: (...a: unknown[]) => createRun(...a),
  updateRun: (...a: unknown[]) => updateRun(...a),
}));

const fetchMirroredMailForAddresses = vi.fn();
const fetchOrgMailForAddresses = vi.fn();
vi.mock("../../src/lib/google-client", () => ({
  fetchMirroredMailForAddresses: (...a: unknown[]) => fetchMirroredMailForAddresses(...a),
  fetchOrgMailForAddresses: (...a: unknown[]) => fetchOrgMailForAddresses(...a),
}));

import { foldInGmailThreads } from "../../src/lib/self-send/gmail-threads";

const LEAD = "lead@prospect.com";
let runSeq = 0;

function pg(rows: Record<string, unknown>[]) {
  return { rows, rowCount: rows.length };
}

/** db.execute answers in call order: staff identity, leads by org, the org's identity. */
function seedDb(orgId: string): void {
  execute
    .mockResolvedValueOnce(pg([{ orgId: "staff-org", userId: "staff-user" }]))
    .mockResolvedValueOnce(pg([{ orgId, emails: [LEAD] }]))
    .mockResolvedValueOnce(pg([{ orgId, userId: "org-user" }]));
}

beforeEach(() => {
  execute.mockReset();
  createRun.mockReset();
  updateRun.mockReset();
  fetchMirroredMailForAddresses.mockReset();
  fetchOrgMailForAddresses.mockReset();
  createRun.mockImplementation(async () => ({ id: `run-${++runSeq}` }));
  updateRun.mockResolvedValue({});
});

function closes(): Array<{ runId: string; status: string; error: string | undefined }> {
  return updateRun.mock.calls.map((c) => ({ runId: c[0] as string, status: c[1] as string, error: c[3] as string | undefined }));
}

describe("gmail-threads: every reader run it opens is closed by the pass", () => {
  it("closes the staff run AND the org run as completed when the reads succeed", async () => {
    seedDb("org-ok");
    fetchMirroredMailForAddresses.mockResolvedValue([]);
    fetchOrgMailForAddresses.mockResolvedValue({ ownerAddresses: ["me@client.com"], messages: [] });

    await foldInGmailThreads({ dryRun: true, leads: [LEAD] });

    expect(createRun).toHaveBeenCalledTimes(2);
    const opened = await Promise.all(createRun.mock.results.map((r) => r.value as Promise<{ id: string }>));
    expect(closes()).toEqual(opened.map((r) => ({ runId: r.id, status: "completed", error: undefined })));
  });

  it("closes the org run as completed when the org connected no Gmail (404 = nothing to read, not a failure)", async () => {
    seedDb("org-no-gmail");
    fetchMirroredMailForAddresses.mockResolvedValue([]);
    fetchOrgMailForAddresses.mockResolvedValue(null);

    await foldInGmailThreads({ dryRun: true, leads: [LEAD] });

    expect(closes().map((c) => c.status)).toEqual(["completed", "completed"]);
  });

  it("closes a source's run as FAILED, naming the cause, when its mirror read fails", async () => {
    seedDb("org-read-fails");
    fetchMirroredMailForAddresses.mockRejectedValue(new Error("google-service 502"));
    fetchOrgMailForAddresses.mockResolvedValue({ ownerAddresses: [], messages: [] });

    const summary = await foldInGmailThreads({ dryRun: true, leads: [LEAD] });

    expect(summary.batchesFailed).toBe(1);
    const [staff, org] = closes();
    expect(staff.status).toBe("failed");
    expect(staff.error).toBe("staff: 1 mirror batch(es) could not be read");
    expect(org).toMatchObject({ status: "completed", error: undefined });
  });

  it("closes the run as FAILED with the error when the fold throws, then lets the error through", async () => {
    execute
      .mockResolvedValueOnce(pg([{ orgId: "staff-org", userId: "staff-user" }]))
      .mockRejectedValueOnce(new Error("db down")); // loadCandidates, inside the staff fold
    // A message whose filing lookup throws inside the fold (not a batch read).
    fetchMirroredMailForAddresses.mockResolvedValue([
      {
        mirrorId: "m1",
        gmailMessageId: "g1",
        messageId: "<x@y>",
        inReplyTo: null,
        references: null,
        headers: {},
        from: LEAD,
        to: ["kevin@distribute.you"],
        cc: [],
        subject: "Re: hi",
        date: new Date("2026-10-22T10:00:00Z"),
        text: "Sounds good",
      },
    ]);

    await expect(foldInGmailThreads({ dryRun: true, leads: [LEAD] })).rejects.toThrow("db down");

    expect(createRun).toHaveBeenCalledTimes(1);
    expect(closes()).toEqual([{ runId: (await createRun.mock.results[0].value).id, status: "failed", error: "db down" }]);
  });

  it("a failed close does not abort the pass (logged; the next source still runs and closes)", async () => {
    seedDb("org-close-fails");
    fetchMirroredMailForAddresses.mockResolvedValue([]);
    fetchOrgMailForAddresses.mockResolvedValue({ ownerAddresses: [], messages: [] });
    updateRun.mockRejectedValueOnce(new Error("runs-service 503")).mockResolvedValue({});
    const err = vi.spyOn(console, "error").mockImplementation(() => {});

    await foldInGmailThreads({ dryRun: true, leads: [LEAD] });

    expect(updateRun).toHaveBeenCalledTimes(2);
    expect(err.mock.calls.some((c) => String(c[0]).includes("could not close run"))).toBe(true);
    err.mockRestore();
  });
});
