import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock DB
const mockDbSelect = vi.fn();
const mockDbInsert = vi.fn();
const mockDbUpdate = vi.fn();
const mockDbFrom = vi.fn();
const mockDbWhere = vi.fn();
const mockDbValues = vi.fn();
const mockDbSet = vi.fn();
const mockDbOnConflictDoNothing = vi.fn();
const mockDbReturning = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    select: () => ({ from: (table: unknown) => ({ where: mockDbWhere }) }),
    insert: () => ({ values: (v: unknown) => ({ onConflictDoNothing: () => ({ returning: mockDbReturning }), returning: mockDbReturning }) }),
    update: () => ({ set: () => ({ where: vi.fn().mockResolvedValue([{}]) }) }),
  },
}));

vi.mock("../../src/db/schema", () => ({
  organizations: {},
  instantlyCampaigns: { id: "id", instantlyCampaignId: "instantly_campaign_id" },
  instantlyLeads: { instantlyCampaignId: "instantly_campaign_id", email: "email" },
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

import { buildEmailBodyWithSignature } from "../../src/routes/send";
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
  email: { subject: "Hello", body: "World" },
  campaignId: "camp-1",
  runId: "run-1",
  orgId: "org-1",
  brandId: "brand-1",
  appId: "app-1",
};

function acct(overrides: Partial<Account> = {}): Account {
  return { email: "a@test.com", warmup_status: 1, status: 1, ...overrides };
}

describe("buildEmailBodyWithSignature", () => {
  const sig = "<p>Best,<br>John Doe</p>";

  it("should append the real signature to plain text body", () => {
    const result = buildEmailBodyWithSignature("Hello world", [acct({ signature: sig })]);
    expect(result).toBe(`Hello world\n\n${sig}`);
  });

  it("should append the real signature to HTML body", () => {
    const result = buildEmailBodyWithSignature("<p>Hello</p>", [acct({ signature: sig })]);
    expect(result).toBe(`<p>Hello</p>\n\n${sig}`);
  });

  it("should replace {{accountSignature}} placeholder with real signature", () => {
    const body = "Hello\n\n{{accountSignature}}";
    const result = buildEmailBodyWithSignature(body, [acct({ signature: sig })]);
    expect(result).toBe(`Hello\n\n${sig}`);
  });

  it("should replace inline {{accountSignature}} in HTML", () => {
    const body = "<p>Hello</p><div>{{accountSignature}}</div>";
    const result = buildEmailBodyWithSignature(body, [acct({ signature: sig })]);
    expect(result).toBe(`<p>Hello</p><div>${sig}</div>`);
  });

  it("should pick the first non-empty signature from multiple accounts", () => {
    const accounts = [
      acct({ email: "a@test.com", signature: "" }),
      acct({ email: "b@test.com", signature: sig }),
      acct({ email: "c@test.com", signature: "<p>Other sig</p>" }),
    ];
    const result = buildEmailBodyWithSignature("Hi", accounts);
    expect(result).toBe(`Hi\n\n${sig}`);
  });

  it("should strip {{accountSignature}} when no account has a signature", () => {
    const body = "Hello\n\n{{accountSignature}}";
    const result = buildEmailBodyWithSignature(body, [acct({ signature: "" }), acct()]);
    expect(result).toBe("Hello");
  });

  it("should return body as-is when no accounts have signatures and no placeholder", () => {
    const result = buildEmailBodyWithSignature("Hello", [acct()]);
    expect(result).toBe("Hello");
  });
});

describe("POST /send", () => {
  beforeEach(() => {
    vi.clearAllMocks();

    // Default: org exists
    mockDbWhere.mockResolvedValue([{ id: "org-db-1", clerkOrgId: "org-1" }]);
    // Default: campaign exists
    mockDbWhere.mockResolvedValueOnce([{ id: "org-db-1", clerkOrgId: "org-1" }]); // org lookup
    mockDbWhere.mockResolvedValueOnce([{ id: "camp-1", instantlyCampaignId: "inst-camp-1", isNew: false }]); // campaign lookup
    mockDbWhere.mockResolvedValueOnce([]); // lead lookup (not found)

    mockCreateRun.mockResolvedValue({ id: "run-1" });
    mockAddLeads.mockResolvedValue({ added: 1 });
    mockAddCosts.mockResolvedValue({ costs: [] });
    mockUpdateRun.mockResolvedValue({});
    mockListAccounts.mockResolvedValue([{ email: "sender@example.com", warmup_status: 1, status: 1, signature: "<p>Best,<br>Sender</p>" }]);
    mockUpdateCampaign.mockResolvedValue({});
    mockGetCampaign.mockResolvedValue({ email_list: [], bcc_list: [], not_sending_status: null });
    mockDbReturning.mockResolvedValue([{ id: "lead-1" }]);
  });

  it("should use 'instantly-email-send' cost name (not 'instantly-lead-add')", async () => {
    const app = await createSendApp();

    await request(app).post("/send").send(validBody);

    // Verify cost name
    expect(mockAddCosts).toHaveBeenCalled();
    const costCalls = mockAddCosts.mock.calls;
    const allCostNames = costCalls.flatMap(
      ([, items]: [string, { costName: string }[]]) => items.map((i) => i.costName)
    );
    expect(allCostNames).toContain("instantly-email-send");
    expect(allCostNames).not.toContain("instantly-lead-add");
  });

  it("should not track 'instantly-campaign-create' cost", async () => {
    const app = await createSendApp();

    await request(app).post("/send").send(validBody);

    const costCalls = mockAddCosts.mock.calls;
    const allCostNames = costCalls.flatMap(
      ([, items]: [string, { costName: string }[]]) => items.map((i) => i.costName)
    );
    expect(allCostNames).not.toContain("instantly-campaign-create");
  });

  it("should fetch accounts and assign them via PATCH when creating a new campaign", async () => {
    // Override: campaign does NOT exist (force creation)
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([{ id: "org-db-1", clerkOrgId: "org-1" }]); // org lookup
    mockDbWhere.mockResolvedValueOnce([]); // campaign lookup (not found → create)
    mockDbWhere.mockResolvedValueOnce([]); // lead lookup (not found)

    mockCreateCampaign.mockResolvedValue({ id: "inst-camp-new", status: "draft" });
    mockDbReturning.mockResolvedValueOnce([{ id: "camp-1", instantlyCampaignId: "inst-camp-new" }]); // campaign insert
    mockDbReturning.mockResolvedValueOnce([{ id: "lead-1" }]); // lead insert
    mockUpdateCampaignStatus.mockResolvedValue({});

    const app = await createSendApp();
    await request(app).post("/send").send(validBody);

    // listAccounts should have been called to fetch available accounts
    expect(mockListAccounts).toHaveBeenCalled();
    // createCampaign should include the real signature (not {{accountSignature}})
    expect(mockCreateCampaign).toHaveBeenCalledWith({
      name: "Campaign camp-1",
      email: { subject: "Hello", body: "World\n\n<p>Best,<br>Sender</p>" },
    });
    // updateCampaign should PATCH the campaign with email_list and bcc_list
    expect(mockUpdateCampaign).toHaveBeenCalledWith(
      "inst-camp-new",
      { email_list: ["sender@example.com"], bcc_list: ["kevin@mcpfactory.org"], open_tracking: true, link_tracking: true, insert_unsubscribe_header: true }
    );
  });

  it("should skip Instantly API call when lead already exists in campaign", async () => {
    // Override: lead already exists
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([{ id: "org-db-1", clerkOrgId: "org-1" }]); // org lookup
    mockDbWhere.mockResolvedValueOnce([{ id: "camp-1", instantlyCampaignId: "inst-camp-1" }]); // campaign lookup
    mockDbWhere.mockResolvedValueOnce([{ id: "lead-existing", email: "test@example.com" }]); // lead lookup (found!)

    const app = await createSendApp();

    await request(app).post("/send").send(validBody);

    // addLeads should NOT have been called since lead already exists
    expect(mockAddLeads).not.toHaveBeenCalled();
  });
});
