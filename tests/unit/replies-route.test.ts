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

const mockDbExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...a: unknown[]) => mockDbExecute(...a) },
}));

const mockHandThread = vi.fn();
vi.mock("../../src/lib/hand-followups-to-human", () => ({
  handThreadFollowupsToHuman: (...a: unknown[]) => mockHandThread(...a),
}));

const mockCountHumanAnswer = vi.fn();
vi.mock("../../src/lib/count-human-answer", () => ({
  countHumanAnswerAsOurs: (...a: unknown[]) => mockCountHumanAnswer(...a),
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

  function takeover(heldInOwnMailbox: boolean) {
    return Object.assign(new FakeReplyToLeadError("human_took_over", 409, "a person answered"), {
      answer: { instantlyCampaignId: "seq-1", at: "2026-10-15T09:00:00.000Z", heldInOwnMailbox },
    });
  }

  it("a hand-typed answer counts as our touch: the follow-ups go on, nothing is handed (REPLY-ANY-LEAD-1015)", async () => {
    mockReplyToLead.mockRejectedValue(takeover(false));

    const res = await request(app()).post("/orgs/replies").send(REPLY_BODY).expect(409);
    expect(res.body.code).toBe("human_took_over");
    expect(mockCountHumanAnswer).toHaveBeenCalledWith({
      orgId: "org-1",
      campaignId: "camp-1",
      leadEmail: "alice@media.com",
      instantlyCampaignId: "seq-1",
      answeredAt: "2026-10-15T09:00:00.000Z",
    });
    expect(mockHandThread).not.toHaveBeenCalled();
  });

  it("a conversation held in a person's own mailbox is still handed over", async () => {
    mockReplyToLead.mockRejectedValue(takeover(true));

    await request(app()).post("/orgs/replies").send(REPLY_BODY).expect(409);
    expect(mockHandThread).toHaveBeenCalledWith(
      expect.objectContaining({ orgId: "org-1", campaignId: "camp-1", leadEmail: "alice@media.com" }),
    );
    expect(mockCountHumanAnswer).not.toHaveBeenCalled();
  });

  it("prospect_wrote_elsewhere is still handed over", async () => {
    mockReplyToLead.mockRejectedValue(
      new FakeReplyToLeadError("prospect_wrote_elsewhere", 409, "wrote to a person's Gmail"),
    );
    await request(app()).post("/orgs/replies").send(REPLY_BODY).expect(409);
    expect(mockHandThread).toHaveBeenCalled();
    expect(mockCountHumanAnswer).not.toHaveBeenCalled();
  });

  it("any other refusal hands nothing over", async () => {
    mockReplyToLead.mockRejectedValue(
      new FakeReplyToLeadError("no_reply_to_thread", 409, "never wrote back"),
    );
    await request(app()).post("/orgs/replies").send(REPLY_BODY).expect(409);
    expect(mockHandThread).not.toHaveBeenCalled();
    expect(mockCountHumanAnswer).not.toHaveBeenCalled();
  });
});

describe("POST /orgs/replies — the prospect's address alone finds the thread", () => {
  const BY_EMAIL = { email: "alice@media.com", body_html: "<p>Thursday works.</p>", sent_by: "human" };
  const row = (campaignId: string, replied: boolean, at: string | null) => ({
    campaignId,
    replied,
    lastMessageAt: at === null ? null : new Date(at),
  });

  it("replies in the one thread the prospect wrote back in", async () => {
    // Emailed in two campaigns, answered in one: that one is the thread.
    mockDbExecute.mockResolvedValue({
      rows: [row("camp-old", false, "2026-10-01T09:00:00Z"), row("camp-9", true, "2026-10-10T09:00:00Z")],
    });

    await request(app()).post("/orgs/replies").send(BY_EMAIL).expect(200);

    expect(mockReplyToLead).toHaveBeenCalledWith(
      expect.objectContaining({ campaignId: "camp-9", leadEmail: "alice@media.com", sentBy: "human" }),
    );
  });

  it("refuses several threads and lists them, newest first", async () => {
    mockDbExecute.mockResolvedValue({
      rows: [row("camp-b", true, "2026-10-11T09:00:00Z"), row("camp-a", true, "2026-10-02T09:00:00Z")],
    });

    const res = await request(app()).post("/orgs/replies").send(BY_EMAIL).expect(409);

    expect(res.body.code).toBe("ambiguous_thread");
    expect(res.body.candidates).toEqual([
      { campaign_id: "camp-b", last_message_at: "2026-10-11T09:00:00.000Z" },
      { campaign_id: "camp-a", last_message_at: "2026-10-02T09:00:00.000Z" },
    ]);
    expect(mockReplyToLead).not.toHaveBeenCalled();
  });

  it("a prospect we never emailed is a legible 404", async () => {
    mockDbExecute.mockResolvedValue({ rows: [] });
    const res = await request(app()).post("/orgs/replies").send(BY_EMAIL).expect(404);
    expect(res.body.code).toBe("campaign_not_found");
    expect(mockReplyToLead).not.toHaveBeenCalled();
  });

  it("a prospect who never wrote back has no thread", async () => {
    mockDbExecute.mockResolvedValue({ rows: [row("camp-1", false, "2026-10-01T09:00:00Z")] });
    const res = await request(app()).post("/orgs/replies").send(BY_EMAIL).expect(409);
    expect(res.body.code).toBe("no_reply_to_thread");
    expect(mockReplyToLead).not.toHaveBeenCalled();
  });

  it("a named campaign is used as-is, no lookup", async () => {
    await request(app()).post("/orgs/replies").send(REPLY_BODY).expect(200);
    expect(mockDbExecute).not.toHaveBeenCalled();
    expect(mockReplyToLead).toHaveBeenCalledWith(expect.objectContaining({ campaignId: "camp-1" }));
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
