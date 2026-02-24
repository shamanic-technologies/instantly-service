import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock DB
const mockDbWhere = vi.fn();
const mockDbReturning = vi.fn();
const mockDbInsertValues = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    select: () => ({ from: (table: unknown) => ({ where: mockDbWhere }) }),
    insert: () => ({ values: (v: unknown) => {
      mockDbInsertValues(v);
      return { onConflictDoNothing: () => ({ returning: mockDbReturning }), returning: mockDbReturning };
    }}),
    update: () => ({ set: () => ({ where: vi.fn().mockResolvedValue([{}]) }) }),
  },
}));

vi.mock("../../src/db/schema", () => ({
  organizations: {},
  instantlyCampaigns: {
    id: "id",
    campaignId: "campaign_id",
    leadEmail: "lead_email",
    instantlyCampaignId: "instantly_campaign_id",
  },
  instantlyLeads: { instantlyCampaignId: "instantly_campaign_id", email: "email" },
  sequenceCosts: {},
}));

// Mock instantly-client
const mockAddLeads = vi.fn();
const mockUpdateCampaignStatus = vi.fn();
const mockCreateCampaign = vi.fn();
const mockUpdateCampaign = vi.fn();
const mockGetCampaign = vi.fn();
const mockListAccounts = vi.fn();

vi.mock("../../src/lib/instantly-client", () => ({
  addLeads: (...args: unknown[]) => mockAddLeads(...args),
  createCampaign: (...args: unknown[]) => mockCreateCampaign(...args),
  updateCampaign: (...args: unknown[]) => mockUpdateCampaign(...args),
  getCampaign: (...args: unknown[]) => mockGetCampaign(...args),
  updateCampaignStatus: (...args: unknown[]) => mockUpdateCampaignStatus(...args),
  listAccounts: (...args: unknown[]) => mockListAccounts(...args),
}));

// Mock runs-client
const mockCreateRun = vi.fn();
const mockUpdateRun = vi.fn();
const mockAddCosts = vi.fn();

vi.mock("../../src/lib/runs-client", () => ({
  createRun: (...args: unknown[]) => mockCreateRun(...args),
  updateRun: (...args: unknown[]) => mockUpdateRun(...args),
  addCosts: (...args: unknown[]) => mockAddCosts(...args),
}));

import { buildEmailBodyWithSignature, pickRandomAccount, buildSequenceSteps } from "../../src/routes/send";
import type { Account } from "../../src/lib/instantly-client";
import request from "supertest";
import express from "express";

async function createSendApp() {
  const sendRouter = (await import("../../src/routes/send")).default;
  const app = express();
  app.use(express.json());
  app.use("/send", sendRouter);
  return app;
}

const validBody = {
  to: "test@example.com",
  firstName: "Test",
  lastName: "User",
  company: "TestCo",
  subject: "Hello",
  sequence: [
    { step: 1, bodyHtml: "<p>First email</p>", daysSinceLastStep: 0 },
    { step: 2, bodyHtml: "<p>Follow up</p>", daysSinceLastStep: 3 },
    { step: 3, bodyHtml: "<p>Last chance</p>", daysSinceLastStep: 7 },
  ],
  campaignId: "camp-1",
  leadId: "lead-1",
  runId: "run-1",
  orgId: "org-1",
  brandId: "brand-1",
  appId: "app-1",
};

function acct(overrides: Partial<Account> = {}): Account {
  return { email: "a@test.com", warmup_status: 1, status: 1, ...overrides };
}

/**
 * Helper: set up mocks for a new campaign creation flow (happy path).
 * getCampaign returns no not_sending_status on both verify calls.
 */
function mockNewCampaignFlow() {
  mockDbWhere.mockReset();
  mockDbWhere.mockResolvedValueOnce([{ id: "org-db-1", clerkOrgId: "org-1" }]); // org lookup
  mockDbWhere.mockResolvedValueOnce([]); // findExistingCampaign (not found → create)

  mockCreateCampaign.mockResolvedValue({ id: "inst-camp-new", status: "draft" });
  // getCampaign is called twice per attempt: verify after PATCH + verify after activation
  mockGetCampaign.mockResolvedValueOnce({ email_list: ["sender@example.com"], not_sending_status: null });
  mockGetCampaign.mockResolvedValueOnce({ email_list: ["sender@example.com"], status: "active", not_sending_status: null });

  mockDbReturning.mockResolvedValueOnce([{ id: "sub-camp-1", campaignId: "camp-1", instantlyCampaignId: "inst-camp-new" }]); // campaign insert
  mockDbReturning.mockResolvedValueOnce([{ id: "lead-1" }]); // lead insert
  mockUpdateCampaignStatus.mockResolvedValue({});
}

describe("pickRandomAccount", () => {
  it("should return one of the provided accounts", () => {
    const accounts = [
      acct({ email: "a@test.com" }),
      acct({ email: "b@test.com" }),
      acct({ email: "c@test.com" }),
    ];
    const picked = pickRandomAccount(accounts);
    expect(accounts).toContainEqual(picked);
  });
});

describe("buildEmailBodyWithSignature", () => {
  const sig = "<p>Best,<br>John Doe</p>";

  it("should append separator + signature to body", () => {
    const result = buildEmailBodyWithSignature("<p>Hello</p>", acct({ signature: sig }));
    expect(result).toBe(`<p>Hello</p>\n\n--\n${sig}`);
  });

  it("should replace {{accountSignature}} placeholder with separator + signature", () => {
    const body = "Hello\n\n{{accountSignature}}";
    const result = buildEmailBodyWithSignature(body, acct({ signature: sig }));
    expect(result).toBe(`Hello\n\n--\n${sig}`);
  });

  it("should strip {{accountSignature}} when account has no signature", () => {
    const body = "Hello\n\n{{accountSignature}}";
    const result = buildEmailBodyWithSignature(body, acct({ signature: "" }));
    expect(result).toBe("Hello");
  });

  it("should return body as-is when account has no signature and no placeholder", () => {
    const result = buildEmailBodyWithSignature("Hello", acct());
    expect(result).toBe("Hello");
  });
});

describe("buildSequenceSteps", () => {
  it("should inject signature into every step bodyHtml", () => {
    const sig = "<p>Cheers</p>";
    const sequence = [
      { step: 1, bodyHtml: "<p>First</p>", daysSinceLastStep: 0 },
      { step: 2, bodyHtml: "<p>Second</p>", daysSinceLastStep: 3 },
    ];
    const steps = buildSequenceSteps("Subject", sequence, acct({ signature: sig }));
    expect(steps).toHaveLength(2);
    expect(steps[0].bodyHtml).toContain(sig);
    expect(steps[1].bodyHtml).toContain(sig);
    expect(steps[0].subject).toBe("Subject");
    expect(steps[1].subject).toBe("Subject");
    expect(steps[0].daysSinceLastStep).toBe(0);
    expect(steps[1].daysSinceLastStep).toBe(3);
  });

  it("should sort steps by step number", () => {
    const sequence = [
      { step: 3, bodyHtml: "C", daysSinceLastStep: 7 },
      { step: 1, bodyHtml: "A", daysSinceLastStep: 0 },
      { step: 2, bodyHtml: "B", daysSinceLastStep: 3 },
    ];
    const steps = buildSequenceSteps("Subject", sequence, acct());
    expect(steps[0].bodyHtml).toBe("A");
    expect(steps[1].bodyHtml).toBe("B");
    expect(steps[2].bodyHtml).toBe("C");
  });
});

describe("POST /send", () => {
  let runCounter: number;

  beforeEach(() => {
    vi.clearAllMocks();
    runCounter = 0;

    mockCreateRun.mockImplementation(() => {
      runCounter++;
      return Promise.resolve({ id: `step-run-${runCounter}` });
    });
    mockAddLeads.mockResolvedValue({ added: 1 });
    mockAddCosts.mockImplementation((runId: string) => {
      return Promise.resolve({
        costs: [{ id: `cost-${runId}`, costName: "instantly-email-send" }],
      });
    });
    mockUpdateRun.mockResolvedValue({});
    mockListAccounts.mockResolvedValue([{ email: "sender@example.com", warmup_status: 1, status: 1, signature: "<p>Best,<br>Sender</p>" }]);
    mockUpdateCampaign.mockResolvedValue({});
    mockGetCampaign.mockResolvedValue({ email_list: [], bcc_list: [], not_sending_status: null, status: "active" });
    mockDbReturning.mockResolvedValue([{ id: "lead-1" }]);
    mockDbInsertValues.mockReset();
  });

  it("should reject the old email format", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    const res = await request(app).post("/send").send({
      ...validBody,
      subject: undefined,
      sequence: undefined,
      email: { subject: "Hello", body: "World" },
    });

    expect(res.status).toBe(400);
  });

  it("should create campaign with multi-step sequence", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").send(validBody);

    expect(mockCreateCampaign).toHaveBeenCalledWith({
      name: "Campaign camp-1",
      steps: expect.arrayContaining([
        expect.objectContaining({ subject: "Hello", daysSinceLastStep: 0 }),
        expect.objectContaining({ subject: "Hello", daysSinceLastStep: 3 }),
        expect.objectContaining({ subject: "Hello", daysSinceLastStep: 7 }),
      ]),
    });
  });

  it("should inject signature into all step bodies", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").send(validBody);

    const createCall = mockCreateCampaign.mock.calls[0][0];
    for (const step of createCall.steps) {
      expect(step.bodyHtml).toContain("<p>Best,<br>Sender</p>");
    }
  });

  it("should enable stop_on_reply when patching campaign", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").send(validBody);

    expect(mockUpdateCampaign).toHaveBeenCalledWith(
      "inst-camp-new",
      expect.objectContaining({
        stop_on_reply: true,
        email_list: ["sender@example.com"],
      }),
    );
  });

  it("should store leadId and deliveryStatus in campaign insert", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").send(validBody);

    const campaignInsert = mockDbInsertValues.mock.calls.find(
      ([v]: [any]) => v.campaignId === "camp-1" && v.leadEmail === "test@example.com",
    );
    expect(campaignInsert).toBeDefined();
    expect(campaignInsert![0]).toMatchObject({
      leadId: "lead-1",
      deliveryStatus: "pending",
    });
  });

  it("should create per-step runs with 1 actual + N-1 provisioned costs for a 3-step sequence", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").send(validBody);

    expect(mockCreateRun).toHaveBeenCalledTimes(3);
    expect(mockCreateRun).toHaveBeenCalledWith(expect.objectContaining({ taskName: "email-send-step-1" }));
    expect(mockCreateRun).toHaveBeenCalledWith(expect.objectContaining({ taskName: "email-send-step-2" }));
    expect(mockCreateRun).toHaveBeenCalledWith(expect.objectContaining({ taskName: "email-send-step-3" }));

    expect(mockAddCosts).toHaveBeenCalledTimes(3);
    expect(mockAddCosts).toHaveBeenCalledWith("step-run-1", [{ costName: "instantly-email-send", quantity: 1, status: "actual" }]);
    expect(mockAddCosts).toHaveBeenCalledWith("step-run-2", [{ costName: "instantly-email-send", quantity: 1, status: "provisioned" }]);
    expect(mockAddCosts).toHaveBeenCalledWith("step-run-3", [{ costName: "instantly-email-send", quantity: 1, status: "provisioned" }]);
  });


  it("should store per-step cost IDs in sequence_costs table with distinct runIds", async () => {
    mockNewCampaignFlow();
    // Reset to track sequence_costs inserts
    mockDbReturning.mockReset();
    mockDbReturning.mockResolvedValueOnce([{ id: "sub-camp-1", campaignId: "camp-1", instantlyCampaignId: "inst-camp-new" }]);
    mockDbReturning.mockResolvedValueOnce([{ id: "lead-1" }]);
    mockDbReturning.mockResolvedValue([]); // for sequence_costs inserts

    const app = await createSendApp();

    await request(app).post("/send").send(validBody);

    // Check that sequence_costs were inserted for ALL steps with distinct runIds
    const insertCalls = mockDbInsertValues.mock.calls;
    const sequenceCostInserts = insertCalls.filter(
      ([v]: [any]) => v.costId && v.step,
    );
    expect(sequenceCostInserts).toHaveLength(3);
    expect(sequenceCostInserts[0][0]).toMatchObject({ step: 1, runId: "step-run-1", costId: "cost-step-run-1", status: "actual" });
    expect(sequenceCostInserts[1][0]).toMatchObject({ step: 2, runId: "step-run-2", costId: "cost-step-run-2", status: "provisioned" });
    expect(sequenceCostInserts[2][0]).toMatchObject({ step: 3, runId: "step-run-3", costId: "cost-step-run-3", status: "provisioned" });
  });

  it("should work with a single-step sequence (1 run, completed immediately)", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    const singleStep = {
      ...validBody,
      sequence: [{ step: 1, bodyHtml: "<p>Only email</p>", daysSinceLastStep: 0 }],
    };

    const res = await request(app).post("/send").send(singleStep);

    expect(res.status).toBe(200);
    expect(mockCreateRun).toHaveBeenCalledTimes(1);
    expect(mockCreateRun).toHaveBeenCalledWith(expect.objectContaining({ taskName: "email-send-step-1" }));
    expect(mockAddCosts).toHaveBeenCalledTimes(1);
    expect(mockAddCosts).toHaveBeenCalledWith("step-run-1", [
      { costName: "instantly-email-send", quantity: 1, status: "actual" },
    ]);
    expect(mockUpdateRun).toHaveBeenCalledWith("step-run-1", "completed");
  });

  it("should skip Instantly API call when same lead already processed for campaign", async () => {
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([{ id: "org-db-1", clerkOrgId: "org-1" }]);
    mockDbWhere.mockResolvedValueOnce([{
      id: "sub-camp-1",
      campaignId: "camp-1",
      leadEmail: "test@example.com",
      instantlyCampaignId: "inst-camp-1",
    }]);

    const app = await createSendApp();
    await request(app).post("/send").send(validBody);

    expect(mockCreateCampaign).not.toHaveBeenCalled();
    expect(mockAddLeads).not.toHaveBeenCalled();
  });

  it("should create separate campaigns for different leads in the same campaign", async () => {
    const app = await createSendApp();

    // Send 1: Lead A
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([{ id: "org-db-1", clerkOrgId: "org-1" }]);
    mockDbWhere.mockResolvedValueOnce([]); // findExistingCampaign
    mockCreateCampaign.mockResolvedValueOnce({ id: "inst-camp-A", status: "draft" });
    mockGetCampaign.mockReset();
    mockGetCampaign.mockResolvedValueOnce({ email_list: ["sender@example.com"], not_sending_status: null }); // verify after PATCH
    mockGetCampaign.mockResolvedValueOnce({ email_list: ["sender@example.com"], status: "active", not_sending_status: null }); // post-activate
    mockDbReturning.mockReset();
    mockDbReturning.mockResolvedValueOnce([{ id: "sub-A", campaignId: "camp-1", instantlyCampaignId: "inst-camp-A" }]);
    mockDbReturning.mockResolvedValueOnce([{ id: "lead-A" }]);
    mockDbReturning.mockResolvedValue([]);

    const res1 = await request(app).post("/send").send({
      ...validBody,
      to: "alice@example.com",
      sequence: [{ step: 1, bodyHtml: "<p>Hi Alice</p>", daysSinceLastStep: 0 }],
    });
    expect(res1.status).toBe(200);

    // Send 2: Lead B (same campaignId, different lead)
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([{ id: "org-db-1", clerkOrgId: "org-1" }]);
    mockDbWhere.mockResolvedValueOnce([]); // findExistingCampaign
    mockCreateCampaign.mockResolvedValueOnce({ id: "inst-camp-B", status: "draft" });
    mockGetCampaign.mockReset();
    mockGetCampaign.mockResolvedValueOnce({ email_list: ["sender@example.com"], not_sending_status: null }); // verify after PATCH
    mockGetCampaign.mockResolvedValueOnce({ email_list: ["sender@example.com"], status: "active", not_sending_status: null }); // post-activate
    mockDbReturning.mockReset();
    mockDbReturning.mockResolvedValueOnce([{ id: "sub-B", campaignId: "camp-1", instantlyCampaignId: "inst-camp-B" }]);
    mockDbReturning.mockResolvedValueOnce([{ id: "lead-B" }]);
    mockDbReturning.mockResolvedValue([]);

    const res2 = await request(app).post("/send").send({
      ...validBody,
      to: "bob@example.com",
      sequence: [{ step: 1, bodyHtml: "<p>Hi Bob</p>", daysSinceLastStep: 0 }],
    });
    expect(res2.status).toBe(200);

    expect(mockCreateCampaign).toHaveBeenCalledTimes(2);
    const call1 = mockCreateCampaign.mock.calls[0][0];
    const call2 = mockCreateCampaign.mock.calls[1][0];
    expect(call1.steps[0].bodyHtml).toContain("Hi Alice");
    expect(call2.steps[0].bodyHtml).toContain("Hi Bob");
  });

  it("should retry with new account when not_sending_status detected and succeed on 2nd attempt", async () => {
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([{ id: "org-db-1", clerkOrgId: "org-1" }]); // org lookup
    mockDbWhere.mockResolvedValueOnce([]); // findExistingCampaign

    // Attempt 1: create + verify-PATCH ok + verify-activate fails
    mockCreateCampaign.mockResolvedValueOnce({ id: "inst-camp-fail", status: "draft" });
    mockGetCampaign.mockResolvedValueOnce({ email_list: ["sender@example.com"], not_sending_status: null }); // verify after PATCH
    mockGetCampaign.mockResolvedValueOnce({ email_list: ["sender@example.com"], status: "active", not_sending_status: "account_disconnected" }); // post-activate

    // Attempt 2: create + verify-PATCH ok + verify-activate ok
    mockCreateCampaign.mockResolvedValueOnce({ id: "inst-camp-ok", status: "draft" });
    mockGetCampaign.mockResolvedValueOnce({ email_list: ["sender@example.com"], not_sending_status: null }); // verify after PATCH
    mockGetCampaign.mockResolvedValueOnce({ email_list: ["sender@example.com"], status: "active", not_sending_status: null }); // post-activate

    mockDbReturning.mockResolvedValueOnce([{ id: "sub-camp-1", campaignId: "camp-1", instantlyCampaignId: "inst-camp-ok" }]);
    mockDbReturning.mockResolvedValueOnce([{ id: "lead-1" }]);
    mockDbReturning.mockResolvedValue([]);

    const app = await createSendApp();
    const res = await request(app).post("/send").send(validBody);

    expect(res.status).toBe(200);
    expect(res.body.warning).toBeUndefined();
    expect(mockCreateCampaign).toHaveBeenCalledTimes(2);
    expect(mockCreateRun).toHaveBeenCalledTimes(3); // 3 step runs
    expect(mockAddCosts).toHaveBeenCalledTimes(3);
    expect(mockUpdateRun).toHaveBeenCalledWith("step-run-1", "completed"); // only step 1
  });

  it("should fail after MAX_SEND_RETRIES attempts with not_sending_status and NOT add costs", async () => {
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([{ id: "org-db-1", clerkOrgId: "org-1" }]); // org lookup
    mockDbWhere.mockResolvedValueOnce([]); // findExistingCampaign

    // All 3 attempts fail with not_sending_status
    for (let i = 0; i < 3; i++) {
      mockCreateCampaign.mockResolvedValueOnce({ id: `inst-camp-fail-${i}`, status: "draft" });
      mockGetCampaign.mockResolvedValueOnce({ email_list: ["sender@example.com"], not_sending_status: null }); // verify after PATCH
      mockGetCampaign.mockResolvedValueOnce({ email_list: ["sender@example.com"], status: "active", not_sending_status: "email_gateway_fail" }); // post-activate
    }

    const app = await createSendApp();
    const res = await request(app).post("/send").send(validBody);

    expect(res.status).toBe(500);
    expect(res.body.error).toContain("failed after 3 retry attempts");
    expect(mockCreateCampaign).toHaveBeenCalledTimes(3);
    // No step runs were created (runs are created AFTER successful activation)
    expect(mockCreateRun).not.toHaveBeenCalled();
    expect(mockAddCosts).not.toHaveBeenCalled();
    expect(mockUpdateRun).not.toHaveBeenCalled();
  });

  it("should NOT call handleCampaignError when not_sending_status is null", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    const res = await request(app).post("/send").send(validBody);

    expect(res.status).toBe(200);
    expect(res.body.warning).toBeUndefined();
  });

  it("should only complete step 1 run, leaving follow-up step runs ongoing", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").send(validBody);

    // Only step 1 should be completed
    expect(mockUpdateRun).toHaveBeenCalledTimes(1);
    expect(mockUpdateRun).toHaveBeenCalledWith("step-run-1", "completed");
  });

  it("should return stepRuns array in response", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    const res = await request(app).post("/send").send(validBody);

    expect(res.status).toBe(200);
    expect(res.body.stepRuns).toHaveLength(3);
    expect(res.body.stepRuns[0]).toMatchObject({ step: 1, runId: "step-run-1" });
    expect(res.body.stepRuns[1]).toMatchObject({ step: 2, runId: "step-run-2" });
    expect(res.body.stepRuns[2]).toMatchObject({ step: 3, runId: "step-run-3" });
  });
});
