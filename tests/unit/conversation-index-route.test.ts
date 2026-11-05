import { describe, it, expect, vi, beforeAll, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

type LC = typeof import("../../src/lib/lead-conversation");
type LeadConversation = Awaited<ReturnType<LC["fetchLeadConversation"]>>;
type ConversationMessage = LeadConversation["messages"][number];

const mockFetch = vi.fn();

vi.mock("../../src/lib/lead-conversation", async () => {
  const actual = await vi.importActual<
    typeof import("../../src/lib/lead-conversation")
  >("../../src/lib/lead-conversation");
  return { ...actual, fetchLeadConversation: (...a: unknown[]) => mockFetch(...a) };
});
vi.mock("../../src/db", () => ({ db: {} }));
vi.mock("../../src/db/schema", () => ({ smtpDispatchRaw: {} }));

function msg(
  direction: "inbound" | "outbound",
  at: string,
  text: string,
  extra: Partial<ConversationMessage> = {},
): ConversationMessage {
  return {
    direction,
    from: direction === "outbound" ? "amy@ours.com" : "edmund@prospect.com",
    to: direction === "outbound" ? "edmund@prospect.com" : "amy@ours.com",
    at,
    subject: "Re: intro",
    text,
    campaignId: "camp-1",
    instantlyCampaignId: "self:1",
    outreachFact: null,
    ...extra,
  };
}

// Edmund Bartlett's shape (2026-11-04): our cold email, his positive reply, then
// THREE answers of ours that a front-truncated full read hid from Sophie.
const LONG_ANSWER = "Thanks Edmund. ".repeat(2000);
const EDMUND: LeadConversation = {
  campaignId: "camp-1",
  campaignIds: ["camp-1"],
  instantlyCampaignId: "self:1",
  leadEmail: "edmund@prospect.com",
  accountEmail: "amy@ours.com",
  transport: "smtp",
  source: "self_send",
  messageCount: 5,
  messages: [
    msg("outbound", "2026-10-20T09:00:00.000Z", "Hi Edmund, quick idea for Beyond Logic."),
    msg("inbound", "2026-10-21T10:00:00.000Z", "Sounds interesting, tell me more.\n\nOn Mon, Oct 20, 2026 Amy wrote:\n> Hi Edmund"),
    msg("outbound", "2026-10-21T11:00:00.000Z", LONG_ANSWER),
    msg("outbound", "2026-10-23T11:00:00.000Z", "Following up on my last note."),
    msg("outbound", "2026-10-28T11:00:00.000Z", "Here is a time to talk.", {
      attachments: [{ name: "offer.pdf" } as never],
    }),
  ],
  sequences: [],
};

async function createApp(userId: string | null = "user-1") {
  const router = (await import("../../src/routes/lead-conversations")).default;
  const app = express();
  app.use((req, res, next) => {
    res.locals.orgId = "org-1";
    if (userId) res.locals.userId = userId;
    next();
  });
  app.use(router);
  return app;
}

describe("GET /orgs/conversations/index and /messages", () => {
  // The route's import graph is heavy: load it once, outside any test's clock.
  beforeAll(async () => {
    await import("../../src/routes/lead-conversations");
  }, 120_000);
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("index answers the short list through the same lookup", async () => {
    mockFetch.mockResolvedValue(EDMUND);
    const res = await request(await createApp())
      .get("/index")
      .query({ email: "edmund@prospect.com" });
    expect(res.status).toBe(200);
    expect(res.body.conversation.messages).toHaveLength(5);
    expect(res.body.conversation.messages[0].at).toBe("2026-10-28T11:00:00.000Z");
    expect(mockFetch).toHaveBeenCalledWith({
      orgId: "org-1",
      userId: "user-1",
      campaignId: undefined,
      leadEmail: "edmund@prospect.com",
    });
  });

  it("messages returns full text by id", async () => {
    mockFetch.mockResolvedValue(EDMUND);
    const index = await request(await createApp()).get("/index").query({ email: "edmund@prospect.com" });
    const id = index.body.conversation.messages.find((m: { chars: number }) => m.chars === LONG_ANSWER.length).id;
    const res = await request(await createApp())
      .get("/messages")
      .query({ email: "edmund@prospect.com", ids: id });
    expect(res.status).toBe(200);
    expect(res.body.messages).toHaveLength(1);
    expect(res.body.messages[0].text).toBe(LONG_ANSWER);
  });

  it("an unknown id is a 404 naming it, nothing partial", async () => {
    mockFetch.mockResolvedValue(EDMUND);
    const index = await request(await createApp()).get("/index").query({ email: "edmund@prospect.com" });
    const id = index.body.conversation.messages[0].id;
    const res = await request(await createApp())
      .get("/messages")
      .query({ email: "edmund@prospect.com", ids: `${id},msg_nope` });
    expect(res.status).toBe(404);
    expect(res.body.code).toBe("message_not_found");
    expect(res.body.missingIds).toEqual(["msg_nope"]);
  });

  it("refuses no ids, and more than 50", async () => {
    const app = await createApp();
    expect((await request(app).get("/messages").query({ email: "e@x.com" })).status).toBe(400);
    const many = Array.from({ length: 51 }, (_, i) => `msg_${i}`).join(",");
    expect((await request(app).get("/messages").query({ email: "e@x.com", ids: many })).status).toBe(400);
  });

  it("keeps the conversation's own refusals", async () => {
    const { LeadConversationError } = await vi.importActual<
      typeof import("../../src/lib/lead-conversation")
    >("../../src/lib/lead-conversation");
    mockFetch.mockRejectedValue(
      new LeadConversationError("campaign_not_found", 404, "none"),
    );
    const res = await request(await createApp()).get("/index").query({ email: "e@x.com" });
    expect(res.status).toBe(404);
    expect(res.body.code).toBe("campaign_not_found");
  });

  it("needs x-user-id", async () => {
    const res = await request(await createApp(null)).get("/index").query({ email: "e@x.com" });
    expect(res.status).toBe(400);
  });
});
