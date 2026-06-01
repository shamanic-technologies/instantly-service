import { describe, it, expect, vi, beforeEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────
//
// retry-stuck primitives consumed by the worker loop:
//   - selectOneStuckRow(): SELECT ... LIMIT 1 with 72h floor, silver guard,
//                          NULL identifier guard, and lastAttemptAt cooldown.
//   - processRow(row):     stamp lastAttemptAt; resolve parent run identity;
//                          recover sequence + lead; send on fresh account;
//                          refund + recharge; mute row. Cancels via
//                          handleCampaignError on "terminal-for-us" failures
//                          (parent gone, key gone, no sequence, no lead,
//                          runs-service 409). Leaves row alone on transient
//                          send failures.

const mockDbExecute = vi.fn();
const mockDbUpdateSet = vi.fn();
const mockDbInsertValues = vi.fn();
const mockDbSelectQueue: unknown[][] = [];

function nextDbSelectResponse(): unknown[] {
  return mockDbSelectQueue.shift() ?? [];
}

function makeSelectChain() {
  return {
    from: () => ({
      where: () => {
        const rows = nextDbSelectResponse();
        const thenable = Promise.resolve(rows) as Promise<unknown[]> & {
          limit: (n: number) => Promise<unknown[]>;
        };
        thenable.limit = () => Promise.resolve(rows);
        return thenable;
      },
    }),
  };
}

vi.mock("../../src/db", () => ({
  db: {
    execute: (...args: unknown[]) => mockDbExecute(...args),
    select: () => makeSelectChain(),
    update: () => ({
      set: (v: unknown) => ({
        where: () => {
          mockDbUpdateSet(v);
          return Promise.resolve([{}]);
        },
      }),
    }),
    insert: () => ({
      values: (v: unknown) => {
        mockDbInsertValues(v);
        return {
          onConflictDoNothing: () => Promise.resolve([{}]),
        };
      },
    }),
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: { id: "id", instantlyCampaignId: "instantly_campaign_id" },
  instantlyLeads: { instantlyCampaignId: "instantly_campaign_id", email: "email" },
  sequenceCosts: { campaignId: "campaign_id", leadEmail: "lead_email", status: "status" },
}));

const mockResolveKey = vi.fn();

vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: (...args: unknown[]) => mockResolveKey(...args),
}));

const mockGetCampaign = vi.fn();
const mockUpdateCampaignStatus = vi.fn();

vi.mock("../../src/lib/instantly-client", () => ({
  getCampaign: (...args: unknown[]) => mockGetCampaign(...args),
  updateCampaignStatus: (...args: unknown[]) => mockUpdateCampaignStatus(...args),
}));

const mockSendLeadToInstantly = vi.fn();
const mockStripAccountSignature = vi.fn((body: string) => body);

vi.mock("../../src/lib/send-lead", () => ({
  sendLeadToInstantly: (...args: unknown[]) => mockSendLeadToInstantly(...args),
  stripAccountSignature: (b: string) => mockStripAccountSignature(b),
}));

const mockCreateRun = vi.fn();
const mockUpdateRun = vi.fn();
const mockAddCosts = vi.fn();
const mockUpdateCostStatus = vi.fn();
const mockGetRun = vi.fn();

vi.mock("../../src/lib/runs-client", () => ({
  createRun: (...args: unknown[]) => mockCreateRun(...args),
  updateRun: (...args: unknown[]) => mockUpdateRun(...args),
  addCosts: (...args: unknown[]) => mockAddCosts(...args),
  updateCostStatus: (...args: unknown[]) => mockUpdateCostStatus(...args),
  getRun: (...args: unknown[]) => mockGetRun(...args),
}));

const mockHandleCampaignError = vi.fn();

vi.mock("../../src/lib/campaign-error-handler", () => ({
  handleCampaignError: (...args: unknown[]) => mockHandleCampaignError(...args),
}));

import {
  selectOneStuckRow,
  processRow,
  STUCK_AGE_HOURS,
  ATTEMPT_COOLDOWN_MINUTES,
  MAX_REDISPATCHES,
  type StuckCampaignRow,
} from "../../src/lib/retry-stuck";

function row(extra: Record<string, unknown> = {}): StuckCampaignRow {
  return {
    id: "row-1",
    instantlyCampaignId: "inst-camp-1",
    campaignId: "camp-1",
    leadEmail: "lead@test.com",
    orgId: "org-CURRENT",
    userId: "user-CURRENT",
    runId: "parent-run-1",
    brandIds: ["brand-1"],
    metadata: null,
    ...extra,
  };
}

const SINGLE_STEP_LIVE = {
  status: 1, // Instantly "Active" — passes the live-status preflight.
  sequences: [
    {
      steps: [
        { delay: 0, variants: [{ subject: "Hi", body: "Body\n\n--\nold-sig" }] },
      ],
    },
  ],
};

function queueSelectLead() {
  mockDbSelectQueue.push([
    {
      email: "lead@test.com",
      firstName: "Lead",
      lastName: "Doe",
      companyName: "Co",
      customVariables: null,
    },
  ]);
}

function queueSelectCosts(costs: unknown[] = []) {
  mockDbSelectQueue.push(costs);
}

beforeEach(() => {
  vi.resetAllMocks();
  mockDbSelectQueue.length = 0;
  mockDbExecute.mockResolvedValue({ rows: [] });
  mockResolveKey.mockResolvedValue({ key: "fake-api-key", keySource: "platform" });
  mockGetCampaign.mockResolvedValue(SINGLE_STEP_LIVE);
  // Default getRun returns a parent whose identity matches row.runId's lineage.
  // Parent's org is DIFFERENT from row.orgId on purpose — exercises the
  // identity-drift fix (parent identity is the one we MUST use, not row's).
  mockGetRun.mockResolvedValue({
    id: "parent-run-1",
    organizationId: "org-PARENT",
    userId: "user-PARENT",
    parentRunId: null,
  });
  mockSendLeadToInstantly.mockResolvedValue({
    ok: true,
    value: {
      instantlyCampaignId: "inst-camp-NEW",
      added: 1,
      account: { email: "new-sender@test.com", status: 1, warmup_status: 1 },
    },
  });
  mockStripAccountSignature.mockImplementation((b: string) => b);
  mockCreateRun.mockImplementation(async () => ({
    id: `step-run-${Math.random().toString(36).slice(2, 8)}`,
  }));
  mockUpdateRun.mockResolvedValue({});
  mockAddCosts.mockResolvedValue({
    costs: [
      { id: "new-cost-account-id", costName: "instantly-account-email-sent" },
      { id: "new-cost-domain-id", costName: "instantly-domain-email-sent" },
    ],
  });
  mockUpdateCostStatus.mockResolvedValue({});
  mockHandleCampaignError.mockResolvedValue(undefined);
  mockUpdateCampaignStatus.mockResolvedValue({});
});

// ─── Constants ─────────────────────────────────────────────────────────────

describe("constants", () => {
  it("STUCK_AGE_HOURS = 72", () => {
    expect(STUCK_AGE_HOURS).toBe(72);
  });
  it("ATTEMPT_COOLDOWN_MINUTES > 0", () => {
    expect(ATTEMPT_COOLDOWN_MINUTES).toBeGreaterThan(0);
  });
});

// ─── selectOneStuckRow ─────────────────────────────────────────────────────

describe("selectOneStuckRow SQL filter", () => {
  it("contains 72h floor, LIMIT 1, ORDER BY ASC, silver NOT EXISTS, NULL guards, and lastAttemptAt cooldown", async () => {
    await selectOneStuckRow();
    const selectCall = mockDbExecute.mock.calls[0]?.[0];
    expect(selectCall).toBeDefined();
    const text = chunkText(selectCall);
    expect(text).toMatch(/72\s+hours/);
    expect(text).toMatch(/ORDER BY created_at ASC/);
    expect(text).toMatch(/LIMIT 1/);
    expect(text).toMatch(/NOT EXISTS/i);
    expect(text).toMatch(/instantly_events/);
    expect(text).toMatch(/email_sent/);
    expect(text).toMatch(/email_bounced/);
    expect(text).toMatch(/reply_received/);
    expect(text).toMatch(/lead_unsubscribed/);
    expect(text).toMatch(/c\.campaign_id IS NOT NULL/);
    expect(text).toMatch(/c\.lead_email IS NOT NULL/);
    expect(text).toMatch(/c\.org_id IS NOT NULL/);
    expect(text).toMatch(/c\.metadata->>'lastAttemptAt' IS NULL/);
    expect(text).toMatch(/lastAttemptAt.*::timestamptz <.*NOW\(\)/);
    expect(text).toMatch(/4320\s+minutes/);
  });

  it("contains a PERSON-LEVEL opt-out gate keyed on lead_email (reply/auto-reply/unsub/bounce in ANY campaign)", async () => {
    await selectOneStuckRow();
    const text = chunkText(mockDbExecute.mock.calls[0]?.[0]);
    // Two NOT EXISTS blocks: the per-campaign progress gate + the person gate.
    expect((text.match(/NOT EXISTS/gi) ?? []).length).toBeGreaterThanOrEqual(2);
    // Person gate is keyed on the atomic member, not the campaign instance.
    expect(text).toMatch(/e2\.lead_email\s*=\s*c\.lead_email/);
    expect(text).toMatch(/e2\.event_type IN/);
  });

  it("returns null when SELECT yields zero rows", async () => {
    mockDbExecute.mockResolvedValueOnce({ rows: [] });
    const r = await selectOneStuckRow();
    expect(r).toBeNull();
  });

  it("returns the candidate row when SELECT yields one", async () => {
    mockDbExecute.mockResolvedValueOnce({ rows: [row()] });
    const r = await selectOneStuckRow();
    expect(r).not.toBeNull();
    expect(r!.id).toBe("row-1");
  });
});

// ─── processRow happy path ─────────────────────────────────────────────────

describe("processRow — success path", () => {
  it("stamps lastAttemptAt on the row before doing any work", async () => {
    queueSelectLead();
    queueSelectCosts([]);

    await processRow(row());

    const attemptStamp = mockDbUpdateSet.mock.calls.find((c) => {
      const v = c[0] as Record<string, unknown>;
      const m = v.metadata as Record<string, unknown> | undefined;
      return (
        m !== undefined &&
        typeof m.lastAttemptAt === "string" &&
        !("redispatchCount" in m)
      );
    });
    expect(attemptStamp).toBeDefined();
  });

  it("re-sends using the parent run's identity (not the row's current identity)", async () => {
    queueSelectLead();
    queueSelectCosts([
      {
        id: "cost-row-1",
        runId: "step-run-1",
        costId: "cost-id-1",
        status: "provisioned",
        step: 1,
        campaignId: "camp-1",
        leadEmail: "lead@test.com",
      },
    ]);

    const outcome = await processRow(row());

    expect(outcome.kind).toBe("redispatched");

    // Instantly key resolved for PARENT'S org, not row's current org.
    expect(mockResolveKey).toHaveBeenCalledWith(
      "org-PARENT",
      "system",
      expect.any(Object),
    );

    // updateCostStatus identity uses PARENT'S identity.
    expect(mockUpdateCostStatus).toHaveBeenCalledWith(
      "step-run-1",
      "cost-id-1",
      "cancelled",
      expect.objectContaining({ orgId: "org-PARENT" }),
    );

    // createRun for new step run also carries parent identity + parent's runId.
    expect(mockCreateRun).toHaveBeenCalledWith(
      expect.any(Object),
      expect.objectContaining({
        orgId: "org-PARENT",
        runId: "parent-run-1",
      }),
    );
  });

  it("appends redispatchHistory + bumps redispatchCount + persists lastAttemptAt", async () => {
    queueSelectLead();
    queueSelectCosts([]);

    await processRow(
      row({
        metadata: {
          redispatchCount: 1,
          redispatchHistory: [
            { from: "inst-OLD", to: "inst-camp-1", account: "a@test.com", at: "2026-01-01T00:00:00.000Z" },
          ],
        },
      }),
    );

    const muteCall = mockDbUpdateSet.mock.calls.find((c) => {
      const v = c[0] as Record<string, unknown>;
      return "instantlyCampaignId" in v;
    });
    expect(muteCall).toBeDefined();
    const v = muteCall![0] as {
      metadata: {
        redispatchCount: number;
        redispatchHistory: unknown[];
        lastAttemptAt: string;
      };
    };
    expect(v.metadata.redispatchCount).toBe(2);
    expect(v.metadata.redispatchHistory).toHaveLength(2);
    expect(typeof v.metadata.lastAttemptAt).toBe("string");
  });

  it("charges a fresh instantly-contact-uploaded (actual) at step 1 of the re-send", async () => {
    queueSelectLead();
    queueSelectCosts([]);

    await processRow(row());

    // Multi-step would call addCosts multiple times — for single-step LIVE
    // we expect exactly one call. It must include the upload cost as actual.
    expect(mockAddCosts).toHaveBeenCalled();
    const firstCall = mockAddCosts.mock.calls[0];
    const items = firstCall[1] as Array<{ costName: string; status: string }>;
    const uploadCost = items.find((c) => c.costName === "instantly-contact-uploaded");
    expect(uploadCost).toBeDefined();
    expect(uploadCost!.status).toBe("actual");
  });

  it("does NOT persist the contact-uploaded cost into sequence_costs (it is never cancelled)", async () => {
    mockAddCosts.mockResolvedValueOnce({
      costs: [
        { id: "new-cost-account-id", costName: "instantly-account-email-sent" },
        { id: "new-cost-domain-id", costName: "instantly-domain-email-sent" },
        { id: "new-cost-upload-id", costName: "instantly-contact-uploaded" },
      ],
    });
    queueSelectLead();
    queueSelectCosts([]);

    await processRow(row());

    const uploadInsert = mockDbInsertValues.mock.calls.find((c) => {
      const v = c[0] as Record<string, unknown>;
      return v.costId === "new-cost-upload-id";
    });
    expect(uploadInsert).toBeUndefined();
  });

  it("falls back to the row's current identity when row.runId is null (top-level run)", async () => {
    queueSelectLead();
    queueSelectCosts([]);

    await processRow(row({ runId: null }));

    expect(mockGetRun).not.toHaveBeenCalled();
    // Falls back to row.orgId.
    expect(mockResolveKey).toHaveBeenCalledWith(
      "org-CURRENT",
      "system",
      expect.any(Object),
    );
  });
});

// ─── processRow terminal cancel paths ──────────────────────────────────────

describe("processRow — terminal cancel paths", () => {
  it("cancels the row when the parent run is gone (getRun returns null)", async () => {
    mockGetRun.mockResolvedValueOnce(null);

    const outcome = await processRow(row());

    expect(outcome.kind).toBe("failed");
    if (outcome.kind === "failed") expect(outcome.reason).toBe("parent_run_gone");
    expect(mockHandleCampaignError).toHaveBeenCalledWith(
      "inst-camp-1",
      expect.stringContaining("parent_run_gone"),
      expect.objectContaining({ terminalStatus: "cancelled" }),
    );
    expect(mockSendLeadToInstantly).not.toHaveBeenCalled();
  });

  it("cancels the row when Instantly key resolution fails", async () => {
    mockResolveKey.mockRejectedValueOnce(new Error("KEY-SERVICE 404"));

    const outcome = await processRow(row());

    expect(outcome.kind).toBe("failed");
    if (outcome.kind === "failed") expect(outcome.reason).toBe("key_unavailable");
    expect(mockHandleCampaignError).toHaveBeenCalledWith(
      "inst-camp-1",
      expect.stringContaining("key_unavailable"),
      expect.objectContaining({ terminalStatus: "cancelled" }),
    );
    expect(mockSendLeadToInstantly).not.toHaveBeenCalled();
  });

  it("cancels the row when the live campaign has no sequence", async () => {
    mockGetCampaign.mockResolvedValueOnce({ status: 1, sequences: [] });

    const outcome = await processRow(row());

    expect(outcome.kind).toBe("failed");
    if (outcome.kind === "failed") expect(outcome.reason).toBe("no_sequence");
    expect(mockHandleCampaignError).toHaveBeenCalledWith(
      "inst-camp-1",
      expect.stringContaining("no_sequence"),
      expect.objectContaining({ terminalStatus: "cancelled" }),
    );
    expect(mockSendLeadToInstantly).not.toHaveBeenCalled();
  });

  it("cancels the row when the local lead profile is missing", async () => {
    // No queueSelectLead() — lookup returns empty array.

    const outcome = await processRow(row());

    expect(outcome.kind).toBe("failed");
    if (outcome.kind === "failed") expect(outcome.reason).toBe("lead_profile_not_found");
    expect(mockHandleCampaignError).toHaveBeenCalledWith(
      "inst-camp-1",
      expect.stringContaining("lead_profile_not_found"),
      expect.objectContaining({ terminalStatus: "cancelled" }),
    );
    expect(mockSendLeadToInstantly).not.toHaveBeenCalled();
  });

  it("cancels the row when runs-service throws 409 mid-flight", async () => {
    queueSelectLead();
    queueSelectCosts([]);
    // Make addCosts throw a 409 — typical "Parent-child field conflict" path.
    mockAddCosts.mockRejectedValueOnce(new Error("runs-service POST /v1/runs failed: 409 - Parent-child conflict"));

    const outcome = await processRow(row());

    expect(outcome.kind).toBe("failed");
    if (outcome.kind === "failed") expect(outcome.reason).toBe("runs_service_409");
    expect(mockHandleCampaignError).toHaveBeenCalledWith(
      "inst-camp-1",
      expect.stringContaining("runs_service_409"),
      expect.objectContaining({ terminalStatus: "cancelled" }),
    );
  });

  it("cancels the row when required identifiers (campaignId/leadEmail/orgId) are null", async () => {
    const outcome = await processRow(row({ campaignId: null }));

    expect(outcome.kind).toBe("failed");
    if (outcome.kind === "failed") expect(outcome.reason).toBe("missing_identifiers");
    expect(mockHandleCampaignError).toHaveBeenCalledWith(
      "inst-camp-1",
      expect.stringContaining("missing_identifiers"),
      expect.objectContaining({ terminalStatus: "cancelled" }),
    );
  });
});

// ─── processRow transient failure paths (no cancel) ────────────────────────

describe("processRow — transient failure paths (row left alone)", () => {
  it("leaves the row alone when sendLeadToInstantly returns no_healthy_accounts_available", async () => {
    queueSelectLead();
    mockSendLeadToInstantly.mockResolvedValueOnce({ ok: false, reason: "no_healthy_accounts_available" });

    const outcome = await processRow(row());

    expect(outcome).toEqual({ kind: "failed", reason: "no_healthy_accounts_available" });
    expect(mockHandleCampaignError).not.toHaveBeenCalled();
    const muteCall = mockDbUpdateSet.mock.calls.find((c) => {
      const v = c[0] as Record<string, unknown>;
      return v.instantlyCampaignId === "inst-camp-NEW";
    });
    expect(muteCall).toBeUndefined();
  });

  it("leaves the row alone (cooldown) when getCampaign throws non-409", async () => {
    mockGetCampaign.mockRejectedValueOnce(new Error("instantly 500"));

    const outcome = await processRow(row());

    expect(outcome.kind).toBe("failed");
    expect(mockHandleCampaignError).not.toHaveBeenCalled();
  });
});

// ─── DIS-148: live-status preflight, pause-predecessor, retry cap ──────────

describe("processRow — DIS-148 guards", () => {
  it("MAX_REDISPATCHES is 3", () => {
    expect(MAX_REDISPATCHES).toBe(3);
  });

  it("skips redispatch when the live Instantly campaign is paused (status 2) and syncs local status", async () => {
    mockGetCampaign.mockResolvedValueOnce({ status: 2, sequences: SINGLE_STEP_LIVE.sequences });

    const outcome = await processRow(row());

    expect(outcome.kind).toBe("skipped_paused");
    if (outcome.kind === "skipped_paused") expect(outcome.liveStatus).toBe(2);
    // Did NOT redispatch.
    expect(mockSendLeadToInstantly).not.toHaveBeenCalled();
    expect(mockHandleCampaignError).not.toHaveBeenCalled();
    // Synced local status to 'paused'.
    const sync = mockDbUpdateSet.mock.calls.find((c) => {
      const v = c[0] as Record<string, unknown>;
      return v.status === "paused";
    });
    expect(sync).toBeDefined();
  });

  it("syncs local status to 'completed' when the live campaign is completed (status 3)", async () => {
    mockGetCampaign.mockResolvedValueOnce({ status: 3, sequences: SINGLE_STEP_LIVE.sequences });

    const outcome = await processRow(row());

    expect(outcome.kind).toBe("skipped_paused");
    const sync = mockDbUpdateSet.mock.calls.find((c) => {
      const v = c[0] as Record<string, unknown>;
      return v.status === "completed";
    });
    expect(sync).toBeDefined();
    expect(mockSendLeadToInstantly).not.toHaveBeenCalled();
  });

  it("pauses the PREDECESSOR Instantly campaign after a successful redispatch", async () => {
    queueSelectLead();
    queueSelectCosts([]);

    const outcome = await processRow(row());

    expect(outcome.kind).toBe("redispatched");
    expect(mockUpdateCampaignStatus).toHaveBeenCalledWith(
      "fake-api-key",
      "inst-camp-1", // the OLD campaign id
      "paused",
    );
  });

  it("a pause-predecessor failure does NOT abort the redispatch (best-effort)", async () => {
    queueSelectLead();
    queueSelectCosts([]);
    mockUpdateCampaignStatus.mockRejectedValueOnce(new Error("instantly 500"));

    const outcome = await processRow(row());

    expect(outcome.kind).toBe("redispatched");
    // Row still muted onto the new campaign despite the pause failure.
    const muteCall = mockDbUpdateSet.mock.calls.find((c) => {
      const v = c[0] as Record<string, unknown>;
      return v.instantlyCampaignId === "inst-camp-NEW";
    });
    expect(muteCall).toBeDefined();
  });

  it("terminal-cancels a row already redispatched MAX_REDISPATCHES times (no send)", async () => {
    const outcome = await processRow(row({ metadata: { redispatchCount: MAX_REDISPATCHES } }));

    expect(outcome.kind).toBe("failed");
    if (outcome.kind === "failed") expect(outcome.reason).toBe("max_redispatches_exceeded");
    expect(mockHandleCampaignError).toHaveBeenCalledWith(
      "inst-camp-1",
      expect.stringContaining("max_redispatches_exceeded"),
      expect.objectContaining({ terminalStatus: "cancelled" }),
    );
    // Capped before any Instantly work.
    expect(mockGetCampaign).not.toHaveBeenCalled();
    expect(mockSendLeadToInstantly).not.toHaveBeenCalled();
  });

  it("still redispatches a row under the cap (redispatchCount = MAX_REDISPATCHES - 1)", async () => {
    queueSelectLead();
    queueSelectCosts([]);

    const outcome = await processRow(
      row({ metadata: { redispatchCount: MAX_REDISPATCHES - 1 } }),
    );

    expect(outcome.kind).toBe("redispatched");
    expect(mockHandleCampaignError).not.toHaveBeenCalled();
  });
});

/** Recursively concatenate every string fragment in a drizzle SQL query. */
function chunkText(query: unknown): string {
  if (query == null) return "";
  if (typeof query === "string") return query;
  if (typeof query !== "object") return String(query);

  const chunks = (query as { queryChunks?: unknown[] }).queryChunks;
  if (Array.isArray(chunks)) {
    return chunks.map(chunkText).join("");
  }

  const v = (query as { value?: unknown }).value;
  if (typeof v === "string") return v;
  if (Array.isArray(v)) return v.map(chunkText).join("");

  return "";
}
