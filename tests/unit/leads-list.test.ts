import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";

// Mock DB
const mockDbWhere = vi.fn();
const mockDbLimit = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    select: (selectFields?: unknown) => {
      const isCountQuery = selectFields != null;
      return {
        from: (table: unknown) => {
          const tableName = (table as { _: { name: string } })?._.name ?? null;
          return {
            where: (clause: unknown) => {
              const result = mockDbWhere(tableName, isCountQuery);
              return Object.assign(Promise.resolve(result), {
                offset: (n: number) => {
                  return Object.assign(Promise.resolve(result), {
                    limit: (n: number) => {
                      mockDbLimit(n);
                      return Promise.resolve(result);
                    },
                  });
                },
                limit: (n: number) => {
                  mockDbLimit(n);
                  return Promise.resolve(result);
                },
              });
            },
          };
        },
      };
    },
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: { _: { name: "campaigns" }, id: "id", campaignId: "campaignId", instantlyCampaignId: "instantlyCampaignId" },
  instantlyLeads: { _: { name: "leads" }, instantlyCampaignId: "instantlyCampaignId" },
}));

vi.mock("../../src/lib/runs-client", () => ({
  createRun: vi.fn(),
  updateRun: vi.fn(),
  addCosts: vi.fn(),
}));

vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: vi.fn(),
  KeyServiceError: class extends Error {},
}));

vi.mock("../../src/lib/billing-client", () => ({
  authorizeCreditSpend: vi.fn(),
}));

vi.mock("../../src/lib/instantly-client", () => ({
  getCampaign: vi.fn(),
  updateCampaignStatus: vi.fn(),
}));

vi.mock("../../src/lib/campaign-error-handler", () => ({
  handleCampaignError: vi.fn(),
}));

process.env.INSTANTLY_SERVICE_API_KEY = "test-api-key";

import { createTestApp, getAuthHeaders } from "../helpers/test-app";

const app = createTestApp();

const makeLead = (i: number) => ({
  id: `lead-${i}`,
  instantlyCampaignId: "inst-camp-1",
  email: `lead${i}@example.com`,
  firstName: `First${i}`,
  lastName: `Last${i}`,
});

function setupMock(opts: { campaigns: unknown[]; totalCount: number; leads: unknown[] }) {
  mockDbWhere.mockImplementation((table: string, isCountQuery: boolean) => {
    if (table === "campaigns") return opts.campaigns;
    if (table === "leads" && isCountQuery) return [{ totalCount: opts.totalCount }];
    if (table === "leads") return opts.leads;
    return [];
  });
}

describe("GET /orgs/campaigns/:campaignId/leads", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("returns all leads when limit is not passed (no silent truncation)", async () => {
    const allLeads = Array.from({ length: 150 }, (_, i) => makeLead(i));
    setupMock({
      campaigns: [{ id: "camp-1", campaignId: "camp-1", instantlyCampaignId: "inst-camp-1" }],
      totalCount: 150,
      leads: allLeads,
    });

    const res = await request(app)
      .get("/orgs/campaigns/camp-1/leads")
      .set(getAuthHeaders());

    expect(res.status).toBe(200);
    expect(res.body.leads).toHaveLength(150);
    expect(res.body.count).toBe(150);
    expect(res.body.totalCount).toBe(150);
    // .limit() must NOT be called when no limit param is passed
    expect(mockDbLimit).not.toHaveBeenCalled();
  });

  it("applies limit when explicitly passed", async () => {
    const pagedLeads = Array.from({ length: 10 }, (_, i) => makeLead(i));
    setupMock({
      campaigns: [{ id: "camp-1", campaignId: "camp-1", instantlyCampaignId: "inst-camp-1" }],
      totalCount: 200,
      leads: pagedLeads,
    });

    const res = await request(app)
      .get("/orgs/campaigns/camp-1/leads?limit=10")
      .set(getAuthHeaders());

    expect(res.status).toBe(200);
    expect(res.body.leads).toHaveLength(10);
    expect(res.body.count).toBe(10);
    expect(res.body.totalCount).toBe(200);
    expect(mockDbLimit).toHaveBeenCalledWith(10);
  });

  it("returns totalCount across all records, independent of paginated page", async () => {
    const pagedLeads = Array.from({ length: 50 }, (_, i) => makeLead(i));
    setupMock({
      campaigns: [{ id: "camp-1", campaignId: "camp-1", instantlyCampaignId: "inst-camp-1" }],
      totalCount: 349,
      leads: pagedLeads,
    });

    const res = await request(app)
      .get("/orgs/campaigns/camp-1/leads?limit=50&skip=0")
      .set(getAuthHeaders());

    expect(res.status).toBe(200);
    expect(res.body.count).toBe(50);
    expect(res.body.totalCount).toBe(349);
  });

  it("returns 404 when campaign not found", async () => {
    mockDbWhere.mockImplementation(() => []);

    const res = await request(app)
      .get("/orgs/campaigns/nonexistent/leads")
      .set(getAuthHeaders());

    expect(res.status).toBe(404);
    expect(res.body.error).toBe("Campaign not found");
  });
});
