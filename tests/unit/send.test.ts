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
  runId: "run-1",
  orgId: "org-1",
  brandId: "brand-1",
  appId: "app-1",
};

function acct(overrides: Partial<Account> = {}): Account {
  return { email: "a@test.com", warmup_status: 1, status: 1, ...overrides };
}

/**
 * Helper: set up mocks for a new campaign creation flow.
 */
function mockNewCampaignFlow() {
  mockDbWhere.mockReset();
  mockDbWhere.mockResolvedValueOnce([{ id: "org-db-1", clerkOrgId: "org-1" }]); // org lookup
  mockDbWhere.mockResolvedValueOnce([]); // campaign-for-lead lookup (not found → create)

  mockCreateCampaign.mockResolvedValue({ id: "inst-camp-new", status: "draft" });
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
  beforeEach(() => {
    vi.clearAllMocks();

    mockCreateRun.mockResolvedValue({ id: "run-1" });
    mockAddLeads.mockResolvedValue({ added: 1 });
    mockAddCosts.mockResolvedValue({
      costs: [
        { id: "cost-1", costName: "instantly-email-send", status: "actual" },
        { id: "cost-2", costName: "instantly-email-send", status: "provisioned" },
        { id: "cost-3", costName: "instantly-email-send", status: "provisioned" },
      ],
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

  it("should create 1 actual + N-1 provisioned costs for a 3-step sequence", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").send(validBody);

    expect(mockAddCosts).toHaveBeenCalledWith("run-1", [
      { costName: "instantly-email-send", quantity: 1, status: "actual" },
      { costName: "instantly-email-send", quantity: 1, status: "provisioned" },
      { costName: "instantly-email-send", quantity: 1, status: "provisioned" },
    ]);
  });

  it("should store provisioned cost IDs in sequence_costs table", async () => {
    mockNewCampaignFlow();
    // Reset to track sequence_costs inserts
    mockDbReturning.mockReset();
    mockDbReturning.mockResolvedValueOnce([{ id: "sub-camp-1", campaignId: "camp-1", instantlyCampaignId: "inst-camp-new" }]);
    mockDbReturning.mockResolvedValueOnce([{ id: "lead-1" }]);
    mockDbReturning.mockResolvedValue([]); // for sequence_costs inserts

    const app = await createSendApp();

    await request(app).post("/send").send(validBody);

    // Check that sequence_costs were inserted for steps 2 and 3
    const insertCalls = mockDbInsertValues.mock.calls;
    const sequenceCostInserts = insertCalls.filter(
      ([v]: [any]) => v.costId && v.step,
    );
    expect(sequenceCostInserts).toHaveLength(2);
    expect(sequenceCostInserts[0][0]).toMatchObject({ step: 2, costId: "cost-2", status: "provisioned" });
    expect(sequenceCostInserts[1][0]).toMatchObject({ step: 3, costId: "cost-3", status: "provisioned" });
  });

  it("should work with a single-step sequence (no provisioned costs)", async () => {
    mockNewCampaignFlow();
    mockAddCosts.mockResolvedValue({
      costs: [{ id: "cost-1", costName: "instantly-email-send", status: "actual" }],
    });
    const app = await createSendApp();

    const singleStep = {
      ...validBody,
      sequence: [{ step: 1, bodyHtml: "<p>Only email</p>", daysSinceLastStep: 0 }],
    };

    const res = await request(app).post("/send").send(singleStep);

    expect(res.status).toBe(200);
    expect(mockAddCosts).toHaveBeenCalledWith("run-1", [
      { costName: "instantly-email-send", quantity: 1, status: "actual" },
    ]);
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
    mockDbWhere.mockResolvedValueOnce([]);
    mockCreateCampaign.mockResolvedValueOnce({ id: "inst-camp-A", status: "draft" });
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
    mockDbWhere.mockResolvedValueOnce([]);
    mockCreateCampaign.mockResolvedValueOnce({ id: "inst-camp-B", status: "draft" });
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
});
