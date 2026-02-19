import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

const mockSelect = vi.fn();
const mockInsert = vi.fn();
const mockExecute = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    select: () => ({ from: (table: unknown) => ({ where: mockSelect }) }),
    insert: () => ({ values: mockInsert }),
    execute: (...args: unknown[]) => mockExecute(...args),
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: { id: "id", campaignId: "campaignId" },
  instantlyAnalyticsSnapshots: {},
}));

const mockGetCampaignAnalytics = vi.fn();
vi.mock("../../src/lib/instantly-client", () => ({
  getCampaignAnalytics: (...args: unknown[]) =>
    mockGetCampaignAnalytics(...args),
}));

process.env.INSTANTLY_SERVICE_API_KEY = "test-api-key";

async function createAnalyticsApp() {
  const analyticsRouter = (await import("../../src/routes/analytics")).default;
  const app = express();
  app.use(express.json());
  app.use(analyticsRouter);
  return app;
}

describe("GET /:campaignId/analytics", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("should return 404 when campaign not found", async () => {
    mockSelect.mockResolvedValueOnce([]);
    const app = await createAnalyticsApp();

    const response = await request(app).get("/campaign-123/analytics");

    expect(response.status).toBe(404);
    expect(response.body.error).toBe("Campaign not found");
  });

  it("should return null analytics when Instantly API returns nothing", async () => {
    mockSelect.mockResolvedValueOnce([
      { id: "local-1", instantlyCampaignId: "inst-1" },
    ]);
    mockGetCampaignAnalytics.mockResolvedValueOnce(null);
    const app = await createAnalyticsApp();

    const response = await request(app).get("/campaign-123/analytics");

    expect(response.status).toBe(200);
    expect(response.body.analytics).toBeNull();
  });

  it("should use open_count_unique (not open_count) in response", async () => {
    mockSelect.mockResolvedValueOnce([
      { id: "local-1", instantlyCampaignId: "inst-1" },
    ]);
    mockGetCampaignAnalytics.mockResolvedValueOnce({
      campaign_id: "inst-1",
      campaign_name: "Test Campaign",
      leads_count: 100,
      contacted_count: 80,
      emails_sent_count: 80,
      new_leads_contacted_count: 80,
      open_count: 250, // total opens (repeat opens by same recipient)
      open_count_unique: 60, // unique recipients who opened
      reply_count: 5,
      link_click_count: 3,
      bounced_count: 2,
      unsubscribed_count: 1,
      completed_count: 80,
    });
    mockInsert.mockResolvedValueOnce(undefined);
    const app = await createAnalyticsApp();

    const response = await request(app).get("/campaign-123/analytics");

    expect(response.status).toBe(200);
    // opened should be 60 (unique), NOT 250 (total)
    expect(response.body.analytics.opened).toBe(60);
    // Verify it doesn't return raw open_count
    expect(response.body.analytics.open_count).toBeUndefined();
    expect(response.body.analytics.open_count_unique).toBeUndefined();
  });

  it("should aggregate across multiple sub-campaigns", async () => {
    mockSelect.mockResolvedValueOnce([
      { id: "local-1", instantlyCampaignId: "inst-1" },
      { id: "local-2", instantlyCampaignId: "inst-2" },
    ]);
    mockGetCampaignAnalytics
      .mockResolvedValueOnce({
        campaign_id: "inst-1",
        campaign_name: "Sub 1",
        leads_count: 50,
        contacted_count: 40,
        emails_sent_count: 40,
        new_leads_contacted_count: 40,
        open_count: 120,
        open_count_unique: 30,
        reply_count: 3,
        link_click_count: 2,
        bounced_count: 1,
        unsubscribed_count: 0,
        completed_count: 40,
      })
      .mockResolvedValueOnce({
        campaign_id: "inst-2",
        campaign_name: "Sub 2",
        leads_count: 50,
        contacted_count: 40,
        emails_sent_count: 40,
        new_leads_contacted_count: 40,
        open_count: 100,
        open_count_unique: 25,
        reply_count: 2,
        link_click_count: 1,
        bounced_count: 1,
        unsubscribed_count: 1,
        completed_count: 40,
      });
    mockInsert.mockResolvedValue(undefined);
    const app = await createAnalyticsApp();

    const response = await request(app).get("/campaign-123/analytics");

    expect(response.status).toBe(200);
    expect(response.body.analytics).toEqual({
      total_leads: 100,
      contacted: 80,
      opened: 55, // 30 + 25 (unique, not 120 + 100 = 220)
      replied: 5,
      bounced: 2,
      unsubscribed: 1,
    });
  });

  it("should save snapshot with open_count_unique", async () => {
    mockSelect.mockResolvedValueOnce([
      { id: "local-1", instantlyCampaignId: "inst-1" },
    ]);
    mockGetCampaignAnalytics.mockResolvedValueOnce({
      campaign_id: "inst-1",
      campaign_name: "Test",
      leads_count: 10,
      contacted_count: 8,
      emails_sent_count: 8,
      new_leads_contacted_count: 8,
      open_count: 50,
      open_count_unique: 6,
      reply_count: 1,
      link_click_count: 0,
      bounced_count: 0,
      unsubscribed_count: 0,
      completed_count: 8,
    });
    mockInsert.mockResolvedValueOnce(undefined);
    const app = await createAnalyticsApp();

    await request(app).get("/campaign-123/analytics");

    expect(mockInsert).toHaveBeenCalledWith(
      expect.objectContaining({
        campaignId: "inst-1",
        totalLeads: 10,
        contacted: 8,
        opened: 6, // open_count_unique, not open_count (50)
        replied: 1,
        bounced: 0,
        unsubscribed: 0,
      }),
    );
  });

  it("should skip sub-campaigns where Instantly API returns null", async () => {
    mockSelect.mockResolvedValueOnce([
      { id: "local-1", instantlyCampaignId: "inst-1" },
      { id: "local-2", instantlyCampaignId: "inst-2" },
    ]);
    mockGetCampaignAnalytics
      .mockResolvedValueOnce({
        campaign_id: "inst-1",
        campaign_name: "Sub 1",
        leads_count: 10,
        contacted_count: 8,
        emails_sent_count: 8,
        new_leads_contacted_count: 8,
        open_count: 20,
        open_count_unique: 5,
        reply_count: 1,
        link_click_count: 0,
        bounced_count: 0,
        unsubscribed_count: 0,
        completed_count: 8,
      })
      .mockResolvedValueOnce(null); // second sub-campaign returns nothing
    mockInsert.mockResolvedValueOnce(undefined);
    const app = await createAnalyticsApp();

    const response = await request(app).get("/campaign-123/analytics");

    expect(response.status).toBe(200);
    expect(response.body.analytics).toEqual({
      total_leads: 10,
      contacted: 8,
      opened: 5,
      replied: 1,
      bounced: 0,
      unsubscribed: 0,
    });
    // Only one snapshot saved
    expect(mockInsert).toHaveBeenCalledTimes(1);
  });
});
