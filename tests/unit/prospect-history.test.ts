import { describe, it, expect, vi, beforeEach } from "vitest";

const mockExecute = vi.fn();
const mockFetchLeadConversation = vi.fn();
const mockFetchSelfSendThread = vi.fn();

vi.mock("../../src/db", () => ({
  db: { execute: (...a: unknown[]) => mockExecute(...a) },
}));
vi.mock("../../src/lib/lead-conversation", () => ({
  fetchLeadConversation: (...a: unknown[]) => mockFetchLeadConversation(...a),
}));
vi.mock("../../src/lib/self-send/thread", () => ({
  fetchSelfSendThread: (...a: unknown[]) => mockFetchSelfSendThread(...a),
}));
vi.mock("../../src/lib/key-client", () => ({ resolveInstantlyApiKey: vi.fn() }));
vi.mock("../../src/lib/instantly-client", () => ({ listEmails: vi.fn() }));

import {
  buildProspectActions,
  fillThreadSubjects,
  loadProspectHistory,
  mergeHistory,
  readablePage,
  renderProspectHistory,
  soleLinkIn,
  type ActionRow,
} from "../../src/lib/prospect-history";
import type { ThreadMessage } from "../../src/lib/forward-positive-reply";

const LEAD = "jakub@marktize.com";

function msg(date: string, direction: "inbound" | "outbound", body: string): ThreadMessage {
  return {
    direction,
    from: direction === "outbound" ? "kevin@growthagency.dev" : LEAD,
    to: direction === "outbound" ? LEAD : "kevin@growthagency.dev",
    date,
    subject: "Re: Marktize + Doc Dinners partnership?",
    bodyText: body,
  };
}

function row(overrides: Partial<ActionRow>): ActionRow {
  return {
    eventType: "email_link_clicked",
    at: "2026-09-22T10:00:00.000Z",
    step: 1,
    observedUrl: null,
    sentBodyHtml: null,
    ...overrides,
  };
}

describe("fillThreadSubjects", () => {
  it("gives a stored follow-up the Re: subject it was sent with", () => {
    const out = fillThreadSubjects([
      { ...msg("2026-09-18T13:00:00.000Z", "outbound", "one"), subject: "Partnership?" },
      { ...msg("2026-09-21T13:00:00.000Z", "outbound", "two"), subject: "" },
      { ...msg("2026-09-28T13:00:00.000Z", "outbound", "three"), subject: "(no subject)" },
    ]);
    expect(out.map((m) => m.subject)).toEqual(["Partnership?", "Re: Partnership?", "Re: Partnership?"]);
  });

  it("never doubles Re: and leaves a message with its own subject alone", () => {
    const out = fillThreadSubjects([
      { ...msg("2026-09-18T13:00:00.000Z", "inbound", "r"), subject: "Re: Partnership?" },
      { ...msg("2026-09-19T13:00:00.000Z", "outbound", "a"), subject: "" },
    ]);
    expect(out.map((m) => m.subject)).toEqual(["Re: Partnership?", "Re: Partnership?"]);
  });
});

describe("readablePage", () => {
  it("drops our utm tagging and keeps the page", () => {
    expect(readablePage("https://distribute.you/pricing?utm_source=cold_email&utm_medium=cold_email&ref=x")).toBe(
      "https://distribute.you/pricing?ref=x",
    );
  });

  it("never surfaces a provider click-tracking redirect", () => {
    expect(readablePage("https://inst.growthagency.itrackly.com/c/abc")).toBeNull();
    expect(readablePage("https://prox.itrackly.com/abc")).toBeNull();
    expect(readablePage("https://track.instantly.ai/x")).toBeNull();
  });

  it("never surfaces our own opt-out or click redirect", () => {
    process.env.SELF_SEND_PUBLIC_URL = "https://links.example.org";
    expect(readablePage("https://links.example.org/c/payload/sig")).toBeNull();
    delete process.env.SELF_SEND_PUBLIC_URL;
  });

  it("refuses non-web schemes", () => {
    expect(readablePage("mailto:a@b.com")).toBeNull();
    expect(readablePage("not a url")).toBeNull();
  });
});

describe("soleLinkIn", () => {
  it("names the page when the email held exactly one", () => {
    expect(soleLinkIn('<p>See <a href="https://docdinners.com/partners">here</a>.</p>')).toBe(
      "https://docdinners.com/partners",
    );
  });

  it("declines to guess when the email held several", () => {
    expect(soleLinkIn('<a href="https://a.com">a</a> <a href="https://b.com">b</a>')).toBeNull();
  });

  it("counts the same page twice as one", () => {
    expect(soleLinkIn('<a href="https://a.com/x">https://a.com/x</a>')).toBe("https://a.com/x");
  });
});

describe("buildProspectActions", () => {
  it("never lists an open", () => {
    expect(buildProspectActions([row({ eventType: "email_opened" })])).toEqual([]);
  });

  it("an observed destination wins over the deduced one", () => {
    const [a] = buildProspectActions([
      row({ observedUrl: "https://docdinners.com/a", sentBodyHtml: '<a href="https://docdinners.com/b">b</a>' }),
    ]);
    expect(a.page).toBe("https://docdinners.com/a");
  });

  it("deduces the page from the email sent at that step", () => {
    const [a] = buildProspectActions([row({ sentBodyHtml: '<a href="https://docdinners.com/b">b</a>' })]);
    expect(a.page).toBe("https://docdinners.com/b");
  });

  it("collapses repeat clicks on the same page within minutes, keeps a later visit", () => {
    const actions = buildProspectActions([
      row({ at: "2026-09-22T10:00:00.000Z", observedUrl: "https://a.com" }),
      row({ at: "2026-09-22T10:01:00.000Z", observedUrl: "https://a.com" }),
      row({ at: "2026-09-23T10:00:00.000Z", observedUrl: "https://a.com" }),
    ]);
    expect(actions.map((a) => a.at)).toEqual(["2026-09-22T10:00:00.000Z", "2026-09-23T10:00:00.000Z"]);
  });

  it("keeps bounces and unsubscribes, oldest first", () => {
    const actions = buildProspectActions([
      row({ eventType: "lead_unsubscribed", at: "2026-09-25T00:00:00.000Z" }),
      row({ eventType: "email_bounced", at: "2026-09-24T00:00:00.000Z", step: 2 }),
    ]);
    expect(actions.map((a) => a.kind)).toEqual(["bounce", "unsubscribe"]);
  });
});

describe("mergeHistory + renderProspectHistory", () => {
  const messages = [
    msg("2026-09-18T13:09:33.000Z", "outbound", "email one"),
    msg("2026-09-21T13:38:37.000Z", "outbound", "email two"),
    msg("2026-09-28T14:05:55.000Z", "outbound", "email three"),
    msg("2026-09-28T14:13:23.000Z", "inbound", "can you explain?"),
  ];
  const actions = buildProspectActions([
    row({ at: "2026-09-22T09:00:00.000Z", step: 2, observedUrl: "https://docdinners.com/partners" }),
  ]);

  it("places every sent email, the click and the reply in date order", () => {
    const text = renderProspectHistory({ items: mergeHistory(messages, actions), notes: [] }, LEAD);
    const order = ["email one", "email two", "visited https://docdinners.com/partners", "email three", "can you explain?"].map(
      (s) => text.indexOf(s),
    );
    expect(order.every((i) => i >= 0)).toBe(true);
    expect([...order].sort((a, b) => a - b)).toEqual(order);
    expect(text).toContain(`${LEAD} visited https://docdinners.com/partners (clicked the link in email 2)`);
    expect(text).toContain("Date: Sep 22, 2026");
  });

  it("stays clean and forwardable (no internal labels)", () => {
    const text = renderProspectHistory({ items: mergeHistory(messages, actions), notes: [] }, LEAD);
    expect(text).not.toMatch(/instantly-service|qualification|Campaign:/i);
  });

  it("states unreadable parts first", () => {
    const text = renderProspectHistory({ items: mergeHistory(messages, []), notes: ["x could not be read."] }, LEAD);
    expect(text.startsWith("Note: x could not be read.")).toBe(true);
  });

  it("a click with no known page says so", () => {
    const text = renderProspectHistory(
      { items: mergeHistory([], buildProspectActions([row({ step: 3 })])), notes: [] },
      LEAD,
    );
    expect(text).toContain(`${LEAD} clicked a link in email 3 (page not recorded)`);
  });
});

describe("loadProspectHistory", () => {
  const campaign = {
    instantlyCampaignId: "self:1",
    campaignId: null,
    conversationCampaignId: "camp-1",
    orgId: "org-1",
    userId: "user-1",
    runId: "run-1",
  };

  beforeEach(() => {
    mockExecute.mockReset();
    mockFetchLeadConversation.mockReset();
    mockFetchSelfSendThread.mockReset();
    mockExecute.mockResolvedValue({ rows: [] });
  });

  it("reads the whole campaign and the actions across every row of it", async () => {
    mockFetchLeadConversation.mockResolvedValue({
      messages: [
        { direction: "outbound", from: "a", to: LEAD, at: "2026-09-18T13:00:00.000Z", subject: "s", text: "one" },
        { direction: "inbound", from: LEAD, to: "a", at: "2026-09-28T13:00:00.000Z", subject: "Re: s", text: "reply" },
      ],
      sequences: [{ instantlyCampaignId: "self:0" }, { instantlyCampaignId: "self:1" }],
    });
    mockExecute.mockResolvedValue({
      rows: [{ event_type: "email_link_clicked", timestamp: new Date("2026-09-20T10:00:00Z"), step: 1, observed_url: "https://a.com", sent_body_html: null }],
    });

    const history = await loadProspectHistory(campaign, LEAD);

    expect(mockFetchLeadConversation).toHaveBeenCalledWith(
      expect.objectContaining({ orgId: "org-1", campaignId: "camp-1", leadEmail: LEAD }),
    );
    expect(history.notes).toEqual([]);
    expect(history.items.map((i) => i.type)).toEqual(["message", "action", "message"]);
  });

  it("falls back to this sequence and says so when the whole-campaign read fails", async () => {
    mockFetchLeadConversation.mockRejectedValue(new Error("campaign-service down"));
    mockFetchSelfSendThread.mockResolvedValue([msg("2026-09-18T13:00:00.000Z", "outbound", "one")]);

    const history = await loadProspectHistory(campaign, LEAD);

    expect(history.messages).toHaveLength(1);
    expect(history.notes[0]).toMatch(/earlier versions of this campaign could not be read/);
  });

  it("never throws: every unreadable part becomes a note", async () => {
    mockFetchLeadConversation.mockRejectedValue(new Error("down"));
    mockFetchSelfSendThread.mockRejectedValue(new Error("down"));
    mockExecute.mockRejectedValue(new Error("down"));

    const history = await loadProspectHistory(campaign, LEAD);

    expect(history.items).toEqual([]);
    expect(history.notes).toHaveLength(2);
  });
});
