import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

const mockFetch = vi.fn();

vi.mock("../../src/lib/lead-conversation", async () => {
  const actual = await vi.importActual<
    typeof import("../../src/lib/lead-conversation")
  >("../../src/lib/lead-conversation");
  return {
    ...actual,
    fetchLeadConversation: (...a: unknown[]) => mockFetch(...a),
  };
});

vi.mock("../../src/db", () => ({ db: {} }));
vi.mock("../../src/db/schema", () => ({ smtpDispatchRaw: {} }));

const CONVERSATION = {
  campaignId: "camp-1",
  instantlyCampaignId: "ic-1",
  leadEmail: "alice@media.com",
  accountEmail: "amy@boostdistribute.com",
  transport: "instantly" as const,
  messageCount: 1,
  messages: [
    {
      direction: "inbound" as const,
      from: "alice@media.com",
      to: "amy@boostdistribute.com",
      at: "2026-09-02T09:00:00.000Z",
      subject: "Re: quick question",
      text: "Sure — what does it cost?",
    },
  ],
};

async function createApp(userId: string | null = "user-1") {
  const router = (await import("../../src/routes/lead-conversations")).default;
  const app = express();
  app.use(express.json());
  app.use((req, res, next) => {
    res.locals.orgId = "org-1";
    if (userId) res.locals.userId = userId;
    next();
  });
  app.use(router);
  return app;
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe("GET /orgs/conversations", () => {
  it("returns the conversation for the (campaign, lead) pair the caller names", async () => {
    mockFetch.mockResolvedValue(CONVERSATION);
    const app = await createApp();

    const res = await request(app)
      .get("/")
      .query({ campaign_id: "camp-1", email: "alice@media.com" });

    expect(res.status).toBe(200);
    expect(res.body.success).toBe(true);
    expect(res.body.conversation.messages[0].text).toBe(
      "Sure — what does it cost?",
    );
    expect(mockFetch).toHaveBeenCalledWith({
      orgId: "org-1",
      userId: "user-1",
      campaignId: "camp-1",
      leadEmail: "alice@media.com",
    });
  });

  it("passes the caller's OWN org, so another org's conversation cannot be asked for", async () => {
    mockFetch.mockResolvedValue(CONVERSATION);
    const app = await createApp();

    await request(app)
      .get("/")
      .query({ campaign_id: "camp-1", email: "alice@media.com", org_id: "org-2" });

    expect(mockFetch).toHaveBeenCalledWith(
      expect.objectContaining({ orgId: "org-1" }),
    );
  });

  it("404s with a named code when nobody has this conversation on record", async () => {
    const { LeadConversationError } = await vi.importActual<
      typeof import("../../src/lib/lead-conversation")
    >("../../src/lib/lead-conversation");
    mockFetch.mockRejectedValue(
      new LeadConversationError("campaign_not_found", 404, "no such campaign"),
    );
    const app = await createApp();

    const res = await request(app)
      .get("/")
      .query({ campaign_id: "camp-9", email: "nobody@media.com" });

    expect(res.status).toBe(404);
    expect(res.body.code).toBe("campaign_not_found");
  });

  it("502s with a named code when the thread exists but cannot be read", async () => {
    const { LeadConversationError } = await vi.importActual<
      typeof import("../../src/lib/lead-conversation")
    >("../../src/lib/lead-conversation");
    mockFetch.mockRejectedValue(
      new LeadConversationError("thread_unavailable", 502, "Instantly 503"),
    );
    const app = await createApp();

    const res = await request(app)
      .get("/")
      .query({ campaign_id: "camp-1", email: "alice@media.com" });

    expect(res.status).toBe(502);
    expect(res.body.code).toBe("thread_unavailable");
  });

  it("400s on a missing campaign_id, and never reaches the lookup", async () => {
    const app = await createApp();

    const res = await request(app).get("/").query({ email: "alice@media.com" });

    expect(res.status).toBe(400);
    expect(mockFetch).not.toHaveBeenCalled();
  });

  it("400s without x-user-id — the same identity the reply path needs", async () => {
    const app = await createApp(null);

    const res = await request(app)
      .get("/")
      .query({ campaign_id: "camp-1", email: "alice@media.com" });

    expect(res.status).toBe(400);
    expect(res.body.error).toContain("x-user-id");
    expect(mockFetch).not.toHaveBeenCalled();
  });

  it("an unnamed failure keeps the generic 500 and carries no code", async () => {
    mockFetch.mockRejectedValue(new Error("boom"));
    const app = await createApp();

    const res = await request(app)
      .get("/")
      .query({ campaign_id: "camp-1", email: "alice@media.com" });

    expect(res.status).toBe(500);
    expect(res.body.code).toBeUndefined();
    expect(res.body.error).toBe("boom");
  });
});
