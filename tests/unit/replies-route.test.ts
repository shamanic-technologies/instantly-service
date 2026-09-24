import { describe, it, expect, vi, beforeEach } from "vitest";
import express from "express";
import request from "supertest";

// `vi.mock` factories are hoisted above every top-level const, so the error
// classes the routes `instanceof`-check have to be minted inside `vi.hoisted` —
// the one top-level form guaranteed to run before them.
const { FakeReplyToLeadError, FakeEscalateReplyError } = vi.hoisted(() => {
  class Named extends Error {
    constructor(
      public readonly code: string,
      public readonly status: number,
      message: string,
    ) {
      super(message);
    }
  }
  return {
    FakeReplyToLeadError: class extends Named {},
    FakeEscalateReplyError: class extends Named {},
  };
});

const mockReplyToLead = vi.fn();
vi.mock("../../src/lib/reply-to-lead", () => ({
  replyToLead: (...a: unknown[]) => mockReplyToLead(...a),
  ReplyToLeadError: FakeReplyToLeadError,
}));

const mockEscalateReply = vi.fn();
vi.mock("../../src/lib/escalate-reply", () => ({
  escalateReply: (...a: unknown[]) => mockEscalateReply(...a),
  EscalateReplyError: FakeEscalateReplyError,
}));

import repliesRoutes from "../../src/routes/replies";

function app() {
  const a = express();
  a.use(express.json());
  a.use(
    "/orgs/replies",
    (req, res, next) => {
      res.locals.orgId = "org-1";
      res.locals.userId = "user-1";
      if (req.headers["x-no-run"] !== "1") res.locals.runId = "run-1";
      next();
    },
    repliesRoutes,
  );
  return a;
}

const REPLY_BODY = {
  campaign_id: "camp-1",
  email: "alice@media.com",
  body_html: "<p>Thursday works.</p>",
};

beforeEach(() => {
  vi.resetAllMocks();
  mockReplyToLead.mockResolvedValue({ status: "sent", reply: { leadEmail: "a@b.c" } });
  mockEscalateReply.mockResolvedValue({
    instantlyCampaignId: "ic-1",
    leadEmail: "alice@media.com",
    threadMessages: 3,
    followupsStopped: true,
  });
});

describe("POST /orgs/replies — who asked reaches the gate", () => {
  it("passes a declared human through verbatim", async () => {
    // ⚠️ THE CALL SITE, not the library. A field the route parses and never
    // forwards leaves the gate correct and the feature absent.
    await request(app())
      .post("/orgs/replies")
      .send({ ...REPLY_BODY, sent_by: "human" })
      .expect(200);

    expect(mockReplyToLead).toHaveBeenCalledWith(
      expect.objectContaining({ sentBy: "human" }),
    );
  });

  it("resolves an undeclared caller to automation, so the gate is live", async () => {
    await request(app()).post("/orgs/replies").send(REPLY_BODY).expect(200);

    expect(mockReplyToLead).toHaveBeenCalledWith(
      expect.objectContaining({ sentBy: "automation" }),
    );
  });

  it("rejects a sender outside the vocabulary rather than defaulting it", async () => {
    await request(app())
      .post("/orgs/replies")
      .send({ ...REPLY_BODY, sent_by: "robot" })
      .expect(400);

    expect(mockReplyToLead).not.toHaveBeenCalled();
  });

  it("surfaces a takeover as its own 409 code", async () => {
    mockReplyToLead.mockRejectedValue(
      new FakeReplyToLeadError("human_took_over", 409, "a person answered"),
    );

    const res = await request(app()).post("/orgs/replies").send(REPLY_BODY).expect(409);
    expect(res.body.code).toBe("human_took_over");
  });
});

describe("POST /orgs/replies/escalate", () => {
  it("hands the thread over and reports what happened", async () => {
    const res = await request(app())
      .post("/orgs/replies/escalate")
      .send({ campaign_id: "camp-1", email: "alice@media.com", question: "How much?" })
      .expect(200);

    expect(res.body).toEqual({
      success: true,
      escalation: {
        instantlyCampaignId: "ic-1",
        leadEmail: "alice@media.com",
        threadMessages: 3,
        followupsStopped: true,
      },
    });
    expect(mockEscalateReply).toHaveBeenCalledWith(
      expect.objectContaining({ question: "How much?", orgId: "org-1", runId: "run-1" }),
    );
  });

  it("refuses an escalation carrying no run, before touching anything", async () => {
    const res = await request(app())
      .post("/orgs/replies/escalate")
      .set("x-no-run", "1")
      .send({ campaign_id: "camp-1", email: "alice@media.com", question: "How much?" })
      .expect(400);

    expect(res.body.error).toMatch(/x-run-id/);
    expect(mockEscalateReply).not.toHaveBeenCalled();
  });

  it("refuses an escalation with nothing to answer", async () => {
    await request(app())
      .post("/orgs/replies/escalate")
      .send({ campaign_id: "camp-1", email: "alice@media.com", question: "" })
      .expect(400);

    expect(mockEscalateReply).not.toHaveBeenCalled();
  });

  it("surfaces a named refusal with its code", async () => {
    mockEscalateReply.mockRejectedValue(
      new FakeEscalateReplyError("campaign_not_found", 404, "nope"),
    );

    const res = await request(app())
      .post("/orgs/replies/escalate")
      .send({ campaign_id: "camp-1", email: "alice@media.com", question: "How much?" })
      .expect(404);
    expect(res.body.code).toBe("campaign_not_found");
  });

  it("does NOT dress an unnamed failure as a refusal the caller can act on", async () => {
    mockEscalateReply.mockRejectedValue(new Error("postmark refused"));

    const res = await request(app())
      .post("/orgs/replies/escalate")
      .send({ campaign_id: "camp-1", email: "alice@media.com", question: "How much?" })
      .expect(500);
    expect(res.body.code).toBeUndefined();
  });
});
