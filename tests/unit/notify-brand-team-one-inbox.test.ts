import { describe, it, expect, vi, beforeEach } from "vitest";

const listOrgMembers = vi.fn();
const sendEmail = vi.fn();

vi.mock("../../src/lib/client-org-client", () => ({
  listOrgMembers: (...args: unknown[]) => listOrgMembers(...args),
}));
vi.mock("../../src/lib/email-client", () => ({
  sendEmail: (...args: unknown[]) => sendEmail(...args),
}));
vi.mock("../../src/lib/celebrate-positive-reply", async (orig) => ({
  ...(await orig<typeof import("../../src/lib/celebrate-positive-reply")>()),
  externalOrgIdOrNull: async () => "org_1",
}));

import { emailBrandTeam, inboxKey, oneMemberPerInbox } from "../../src/lib/notify-brand-team";

const input = {
  reason: "question" as const,
  orgId: "org-uuid",
  userId: "user-uuid",
  runId: "run-uuid",
  brandId: "b-1",
  brandName: "Bio Husk",
  leadEmail: "elena@biopartner.ch",
  history: { items: [], notes: [] },
  latestReply: null,
};

const recipients = () => sendEmail.mock.calls.map((c) => (c[0] as { recipientEmail: string }).recipientEmail);

beforeEach(() => {
  listOrgMembers.mockReset();
  sendEmail.mockReset();
  sendEmail.mockResolvedValue(undefined);
});

describe("one inbox, one escalation copy", () => {
  it("sends ONE copy to the untagged address when 3 members share one Gmail inbox", async () => {
    listOrgMembers.mockResolvedValue([
      { email: "kevin.lourd+mobileaudit@gmail.com", firstName: "Audit" },
      { email: "Kevin.Lourd@gmail.com", firstName: "Kevin" },
      { email: "kevinlourd+speedcheck@googlemail.com", firstName: "Speed" },
    ]);
    const sent = await emailBrandTeam(input);
    expect(recipients()).toEqual(["Kevin.Lourd@gmail.com"]);
    expect(sent).toEqual(["Kevin.Lourd@gmail.com"]);
    const html = (sendEmail.mock.calls[0][0] as { metadata: { html: string } }).metadata.html;
    expect(html).toContain("Hi Kevin,");
  });

  it("sends two copies to two different people", async () => {
    listOrgMembers.mockResolvedValue([
      { email: "ana@gmail.com", firstName: "Ana" },
      { email: "bob@gmail.com", firstName: "Bob" },
    ]);
    await emailBrandTeam(input);
    expect(recipients()).toEqual(["ana@gmail.com", "bob@gmail.com"]);
  });

  it("leaves non-Gmail plus-addresses as two inboxes", async () => {
    listOrgMembers.mockResolvedValue([
      { email: "a+b@corp.com", firstName: "A" },
      { email: "a@corp.com", firstName: "A" },
    ]);
    await emailBrandTeam(input);
    expect(recipients()).toEqual(["a+b@corp.com", "a@corp.com"]);
  });

  it("still throws when the org has no reachable member", async () => {
    listOrgMembers.mockResolvedValue([]);
    await expect(emailBrandTeam(input)).rejects.toThrow(/no member with an email address/);
    expect(sendEmail).not.toHaveBeenCalled();
  });

  it("still throws when every send failed", async () => {
    listOrgMembers.mockResolvedValue([{ email: "ana@gmail.com", firstName: "Ana" }]);
    sendEmail.mockRejectedValue(new Error("boom"));
    await expect(emailBrandTeam(input)).rejects.toThrow(/could be emailed/);
  });
});

describe("inboxKey / oneMemberPerInbox", () => {
  it("folds Gmail dots, +tags and googlemail.com; keeps other domains as typed (lowercased)", () => {
    expect(inboxKey("Kevin.Lourd+x@GoogleMail.com")).toBe("kevinlourd@gmail.com");
    expect(inboxKey("a+b@corp.com")).toBe("a+b@corp.com");
    expect(inboxKey("A.B@Corp.com")).toBe("a.b@corp.com");
  });

  it("keeps the first listed when every copy is tagged", () => {
    const kept = oneMemberPerInbox([
      { email: "k+1@gmail.com", firstName: "One" },
      { email: "k+2@gmail.com", firstName: "Two" },
    ]);
    expect(kept.map((m) => m.email)).toEqual(["k+1@gmail.com"]);
  });
});
