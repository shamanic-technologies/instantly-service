import { describe, it, expect, vi, beforeEach } from "vitest";

const mockLoadHistory = vi.fn();
vi.mock("../../src/lib/prospect-history", () => ({
  loadProspectHistory: (...a: unknown[]) => mockLoadHistory(...a),
}));

import { loadQuotedConversation, ownWords } from "../../src/lib/quoted-conversation";
import { renderQuotedHistory } from "../../src/lib/escalate-reply";
import type { ThreadMessage } from "../../src/lib/forward-positive-reply";

// Doc Dinners, 2026-11-03: the cold email that introduced the brand, then the
// prospect's reply (which carries his mail client's quote of it).
const COLD: ThreadMessage = {
  direction: "outbound",
  from: "klourd@growthagency.agency",
  to: "drfastiggi@wiltonchiro.com",
  date: "2026-10-30T14:00:00.000Z",
  subject: "Dinner for Wilton Chiropractic",
  bodyText: "Hi Thomas,\n\nDoc Dinners hosts private dinners for chiropractors.\n\nKevin",
  sequenceStep: true,
};
const REPLY: ThreadMessage = {
  direction: "inbound",
  from: "drfastiggi@wiltonchiro.com",
  to: "klourd@growthagency.agency",
  date: "2026-11-02T16:00:00.000Z",
  subject: "Re: Dinner for Wilton Chiropractic",
  bodyText:
    "Sounds interesting, tell me more.\n\nOn Thu, Oct 30, 2026 at 10:00 AM Kevin <klourd@growthagency.agency> wrote:\n> Hi Thomas,\n> Doc Dinners hosts private dinners for chiropractors.",
};

beforeEach(() => {
  vi.resetAllMocks();
});

describe("ownWords", () => {
  it("drops the quote a reply already carries, keeps the cold email whole", () => {
    const [cold, reply] = ownWords([COLD, REPLY]);
    expect(cold.bodyText).toBe(COLD.bodyText);
    expect(reply.bodyText).toBe("Sounds interesting, tell me more.");
  });

  it("keeps a message whose every line is quoted, rather than quoting it empty", () => {
    const allQuoted = { ...REPLY, bodyText: "> only a quote" };
    expect(ownWords([allQuoted])[0].bodyText).toBe("> only a quote");
  });
});

describe("loadQuotedConversation", () => {
  it("quotes the lead's whole conversation, cold email included, newest first", async () => {
    mockLoadHistory.mockResolvedValue({ items: [], messages: [COLD, REPLY], notes: [] });

    const html = await loadQuotedConversation({
      orgId: "org-dd",
      userId: "user-1",
      campaignId: "camp-dd",
      instantlyCampaignId: "self:dd-1",
      brandId: "brand-dd",
      leadEmail: "drfastiggi@wiltonchiro.com",
    });

    expect(mockLoadHistory).toHaveBeenCalledWith(
      {
        instantlyCampaignId: "self:dd-1",
        campaignId: null,
        orgId: "org-dd",
        userId: "user-1",
        runId: null,
        brandIds: ["brand-dd"],
        conversationCampaignId: "camp-dd",
      },
      "drfastiggi@wiltonchiro.com",
    );
    expect(html).toBe(renderQuotedHistory(ownWords([COLD, REPLY])));
    expect(html).toContain("Doc Dinners hosts private dinners");
    expect(html.indexOf("Sounds interesting")).toBeLessThan(html.indexOf("Doc Dinners hosts"));
    // The cold email's words appear once, not again inside the reply's own quote.
    expect(html.split("Doc Dinners hosts").length - 1).toBe(1);
  });

  it("is empty when the conversation holds no message", async () => {
    mockLoadHistory.mockResolvedValue({ items: [], messages: [], notes: ["the emails exchanged with this prospect could not be read."] });
    const html = await loadQuotedConversation({
      orgId: "o", userId: "u", campaignId: "c", instantlyCampaignId: "i", brandId: null, leadEmail: "x@y.com",
    });
    expect(html).toBe("");
  });
});
