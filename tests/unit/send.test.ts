import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// Mock DB
const mockDbWhere = vi.fn();
const mockDbReturning = vi.fn();
const mockDbInsertValues = vi.fn();
const mockDbDelete = vi.fn();
const mockOnConflictDoUpdate = vi.fn();
const mockRefreshLeadStatusCurrent = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    select: () => ({ from: (table: unknown) => ({ where: (...args: unknown[]) => { const result = mockDbWhere(...args); return Object.assign(result, { limit: () => result }); } }) }),
    insert: () => ({ values: (v: unknown) => {
      mockDbInsertValues(v);
      return {
        // Reservation upsert (onConflictDoUpdate) and the lead insert
        // (onConflictDoNothing) both resolve via the shared returning queue.
        onConflictDoUpdate: (cfg: unknown) => { mockOnConflictDoUpdate(cfg); return { returning: mockDbReturning }; },
        onConflictDoNothing: () => ({ returning: mockDbReturning }),
        returning: mockDbReturning,
      };
    }}),
    update: () => ({ set: () => ({ where: vi.fn().mockResolvedValue([{}]) }) }),
    delete: () => ({ where: (...args: unknown[]) => { mockDbDelete(...args); return Promise.resolve([]); } }),
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: {
    id: "id",
    campaignId: "campaign_id",
    leadEmail: "lead_email",
    instantlyCampaignId: "instantly_campaign_id",
    runId: "run_id",
    status: "status",
  },
  instantlyLeads: { instantlyCampaignId: "instantly_campaign_id", email: "email" },
  sequenceCosts: {},
}));

// Mock key-client
const mockResolveInstantlyApiKey = vi.fn();

vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: (...args: unknown[]) => mockResolveInstantlyApiKey(...args),
  KeyServiceError: class KeyServiceError extends Error {
    statusCode: number;
    constructor(statusCode: number, message: string) {
      super(message);
      this.name = "KeyServiceError";
      this.statusCode = statusCode;
    }
  },
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

// Mock billing-client
const mockAuthorizeCreditSpend = vi.fn();

vi.mock("../../src/lib/billing-client", () => ({
  authorizeCreditSpend: (...args: unknown[]) => mockAuthorizeCreditSpend(...args),
}));

vi.mock("../../src/lib/status-gold", () => ({
  refreshLeadStatusCurrent: (...args: unknown[]) => mockRefreshLeadStatusCurrent(...args),
}));

// Manual-blacklist set: default empty so the send path stays healthy unless a
// test overrides it. Isolates the send route from the DB read.
const mockFetchManuallyBlacklisted = vi.fn(async () => new Set<string>());
vi.mock("../../src/lib/account-blacklist", () => ({
  fetchManuallyBlacklistedEmails: () => mockFetchManuallyBlacklisted(),
}));

import {
  autolinkifyHtml,
  buildEmailBodyWithSignature,
  pickRandomAccount,
  buildSequenceSteps,
  stripAccountSignature,
  classifyAccountBlock,
  filterHealthyAccounts,
} from "../../src/lib/send-lead";
import { requireOrgId } from "../../src/middleware/requireOrgId";
import type { Account } from "../../src/lib/instantly-client";
import request from "supertest";
import express from "express";

async function createSendApp() {
  const sendRouter = (await import("../../src/routes/send")).default;
  const app = express();
  app.use(express.json());
  app.use("/send", requireOrgId, sendRouter);
  return app;
}

const identityHeadersObj = {
  "x-org-id": "org-1",
  "x-user-id": "user-1",
  "x-run-id": "run-1",
  "x-campaign-id": "camp-1",
  "x-brand-id": "brand-1",
};

const validBody = {
  to: "test@example.com",
  firstName: "Test",
  lastName: "User",
  company: "TestCo",
  subject: "Hello",
  sequence: [
    { step: 1, bodyHtml: "<p>First email</p>", daysSinceLastStep: 0 },
    { step: 2, bodyHtml: "<p>Follow up</p>", daysSinceLastStep: 3 },
    { step: 3, bodyHtml: "<p>Last chance</p>", daysSinceLastStep: 7 },
  ],
  leadId: "lead-1",
};

function acct(overrides: Partial<Account> = {}): Account {
  return { email: "a@test.com", warmup_status: 1, status: 1, ...overrides };
}

/**
 * Helper: set up mocks for a new campaign creation flow (happy path).
 * One getCampaign call per attempt (verify-after-PATCH). NSS is no longer
 * checked post-activate.
 */
function mockNewCampaignFlow() {
  mockDbWhere.mockReset();
  mockDbWhere.mockResolvedValueOnce([]); // lead_id conflict check (no conflict)

  mockCreateCampaign.mockResolvedValue({ id: "inst-camp-new", status: "draft" });
  mockGetCampaign.mockResolvedValueOnce({ email_list: ["sender@example.com"], not_sending_status: null }); // verify after PATCH

  mockDbReturning.mockResolvedValueOnce([{ id: "sub-camp-1", campaignId: "camp-1", instantlyCampaignId: "inst-camp-new" }]); // RESERVE upsert → winner
  mockDbReturning.mockResolvedValueOnce([{ id: "lead-1" }]); // lead insert
  mockUpdateCampaignStatus.mockResolvedValue({});
}

describe("pickRandomAccount", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("should throw when no accounts are available", () => {
    expect(() => pickRandomAccount([])).toThrow("No accounts available");
  });

  it("should return the only account when only one is available", () => {
    const a = acct({ email: "only@x.com" });
    expect(pickRandomAccount([a])).toBe(a);
  });

  it("should weight pick by stat_warmup_score", () => {
    const accounts = [
      acct({ email: "low@x.com", stat_warmup_score: 1 }),
      acct({ email: "high@x.com", stat_warmup_score: 99 }),
    ];
    // total weight = 100. target = random * 100. low wins on [0, 1), high on [1, 100).
    const randomSpy = vi.spyOn(Math, "random");

    randomSpy.mockReturnValueOnce(0.005); // 0.5 → low
    expect(pickRandomAccount(accounts).email).toBe("low@x.com");

    randomSpy.mockReturnValueOnce(0.5); // 50 → high
    expect(pickRandomAccount(accounts).email).toBe("high@x.com");
  });

  it("should treat absent stat_warmup_score as weight 1", () => {
    const accounts = [
      acct({ email: "noscore@x.com" }),
      acct({ email: "scored@x.com", stat_warmup_score: 9 }),
    ];
    // weights = [1, 9], total = 10. noscore wins on [0, 1), scored on [1, 10).
    const randomSpy = vi.spyOn(Math, "random");

    randomSpy.mockReturnValueOnce(0.05); // 0.5 → noscore
    expect(pickRandomAccount(accounts).email).toBe("noscore@x.com");

    randomSpy.mockReturnValueOnce(0.5); // 5 → scored
    expect(pickRandomAccount(accounts).email).toBe("scored@x.com");
  });

  it("should fall back to uniform pick when no account has a score", () => {
    const accounts = [
      acct({ email: "a@x.com" }),
      acct({ email: "b@x.com" }),
      acct({ email: "c@x.com" }),
    ];
    // all weights = 1, total = 3. random = 0.5 → target = 1.5 → index 1
    vi.spyOn(Math, "random").mockReturnValue(0.5);
    expect(pickRandomAccount(accounts).email).toBe("b@x.com");
  });

  it("should pick across all accounts regardless of domain", () => {
    const accounts = [
      acct({ email: "alice@growthagency.dev", stat_warmup_score: 1 }),
      acct({ email: "bob@randomdomain.com", stat_warmup_score: 1 }),
    ];
    // equal weights → each domain equally likely; pool whitelisting is gone
    vi.spyOn(Math, "random").mockReturnValue(0.75); // target = 1.5 → bob
    expect(pickRandomAccount(accounts).email).toBe("bob@randomdomain.com");
  });
});

describe("classifyAccountBlock — manual blacklist precedence", () => {
  it("returns 'manual' when the email is in the manual-blacklist set (highest precedence)", () => {
    const set = new Set(["rested@x.com"]);
    // Otherwise fully healthy account still reports manual.
    const a = acct({ email: "rested@x.com", status: 1, stat_warmup_score: 100 });
    expect(classifyAccountBlock(a, set)).toBe("manual");
  });

  it("'manual' wins over inactive / under-warmed / blacklisted-domain", () => {
    const set = new Set(["c@distribute.you"]);
    // distribute.you is a blocked domain, inactive, and under-warmed — manual still wins.
    const a = acct({ email: "c@distribute.you", status: 0, stat_warmup_score: 10 });
    expect(classifyAccountBlock(a, set)).toBe("manual");
  });

  it("falls through to the derived reasons when not manually blacklisted", () => {
    const set = new Set(["someone-else@x.com"]);
    expect(classifyAccountBlock(acct({ email: "y@good.com", status: 0 }), set)).toBe("inactive");
    expect(classifyAccountBlock(acct({ email: "z@good.com", stat_warmup_score: 42 }), set)).toBe("under-warmed");
    expect(classifyAccountBlock(acct({ email: "ok@good.com", status: 1, stat_warmup_score: 100 }), set)).toBeNull();
  });

  it("defaults to no manual blacklist when the set is omitted", () => {
    expect(classifyAccountBlock(acct({ email: "ok@good.com", status: 1, stat_warmup_score: 100 }))).toBeNull();
  });
});

describe("filterHealthyAccounts — excludes manually-blacklisted", () => {
  it("drops a manually-blacklisted account from the eligible pool", () => {
    const accounts = [
      acct({ email: "keep@good.com", status: 1, stat_warmup_score: 100 }),
      acct({ email: "rest@good.com", status: 1, stat_warmup_score: 100 }),
    ];
    const healthy = filterHealthyAccounts(accounts, new Set(["rest@good.com"]));
    expect(healthy.map((a) => a.email)).toEqual(["keep@good.com"]);
  });

  it("keeps everything otherwise-healthy when the manual set is empty", () => {
    const accounts = [
      acct({ email: "a@good.com", status: 1, stat_warmup_score: 100 }),
      acct({ email: "b@good.com", status: 1, stat_warmup_score: 100 }),
    ];
    expect(filterHealthyAccounts(accounts).map((a) => a.email)).toEqual([
      "a@good.com",
      "b@good.com",
    ]);
  });
});

describe("autolinkifyHtml", () => {
  it("wraps plain https URL in anchor tag", () => {
    const out = autolinkifyHtml("<p>visit https://pressbeat.io now</p>");
    expect(out).toContain('<a href="https://pressbeat.io"');
    expect(out).toContain(">https://pressbeat.io</a>");
  });

  it("wraps bare domain with https default protocol", () => {
    const out = autolinkifyHtml("<p>over at pressbeat.io okay</p>");
    expect(out).toContain('href="https://pressbeat.io"');
    expect(out).toContain(">pressbeat.io</a>");
  });

  it("leaves existing <a href> untouched (no double wrap)", () => {
    const out = autolinkifyHtml('<p><a href="https://x.com">x.com</a> and https://y.com</p>');
    expect((out.match(/<a /g) || []).length).toBe(2);
    expect(out).toContain('<a href="https://x.com">x.com</a>');
    expect(out).toContain('href="https://y.com"');
  });

  it("preserves mustache placeholders even when they look domain-like", () => {
    const out = autolinkifyHtml("<p>Hi {{firstName}}, {{user.email}}, see https://z.com</p>");
    expect(out).toContain("{{firstName}}");
    expect(out).toContain("{{user.email}}");
    expect(out).toContain('href="https://z.com"');
  });

  it("returns input unchanged when no URLs and no domains present", () => {
    expect(autolinkifyHtml("<p>Hello world</p>")).toBe("<p>Hello world</p>");
  });

  it("strips trailing punctuation from URL match", () => {
    const out = autolinkifyHtml("<p>(over at pressbeat.io)</p>");
    expect(out).toContain('href="https://pressbeat.io"');
    expect(out).toContain(">pressbeat.io</a>)");
  });
});

describe("buildEmailBodyWithSignature", () => {
  const sig = "<p>Best,<br>John Doe</p>";

  it("should append HTML <p>--</p> separator + signature to body", () => {
    const result = buildEmailBodyWithSignature("<p>Hello</p>", acct({ signature: sig }));
    expect(result).toBe(`<p>Hello</p><p>--</p>${sig}`);
  });

  it("should replace {{accountSignature}} placeholder with HTML separator + signature", () => {
    const body = "Hello\n\n{{accountSignature}}";
    const result = buildEmailBodyWithSignature(body, acct({ signature: sig }));
    expect(result).toBe(`Hello\n\n<p>--</p>${sig}`);
  });

  it("falls back to the canonical Distribute.you signature when account.signature is empty (with placeholder)", () => {
    const body = "Hello\n\n{{accountSignature}}";
    const result = buildEmailBodyWithSignature(body, acct({ email: "kevinl@growthagency.dev", signature: "" }));
    expect(result).toContain("<p>--</p>");
    expect(result).toContain("Kevin Lourd");
    expect(result).not.toContain("Founder");
    expect(result).toContain("Distribute.you | Marketing Agency");
    expect((result.match(/<p>--<\/p>/g) ?? []).length).toBe(1);
  });

  it("falls back to the canonical Distribute.you signature when account.signature is empty (no placeholder)", () => {
    // Brand line is plain text — NOT auto-linkified into an <a>.
    const result = buildEmailBodyWithSignature("Hello", acct({ email: "kevinl@growthagency.dev" }));
    expect(result).toBe(
      "Hello<p>--</p><p>Kevin Lourd<br>Distribute.you | Marketing Agency</p>",
    );
    expect(result).not.toContain("<a ");
  });

  it("uses the same Distribute.you brand line regardless of sending account domain", () => {
    const a = buildEmailBodyWithSignature("Hello", acct({ email: "kevin@marketingagency.forum" }));
    const b = buildEmailBodyWithSignature("Hello", acct({ email: "x@unknownbrand.io" }));
    expect(a).toContain("Distribute.you | Marketing Agency");
    expect(b).toContain("Distribute.you | Marketing Agency");
    expect(a).not.toContain("<a ");
    expect(b).not.toContain("<a ");
  });

  it("signs with the account's OWN name so From-name and signature agree (multi-persona)", () => {
    const result = buildEmailBodyWithSignature(
      "Hello",
      acct({ email: "amy@gildcultivatecoil.com", first_name: "Amy", last_name: "Moore" }),
    );
    expect(result).toBe(
      "Hello<p>--</p><p>Amy Moore<br>Distribute.you | Marketing Agency</p>",
    );
    expect(result).not.toContain("Kevin Lourd");
    expect(result).not.toContain("Founder");
  });

  it("autolinkifies URLs in the body but NOT in the appended signature", () => {
    const sigWithLink = '<p>Best,<br>John — see https://example.com</p>';
    const result = buildEmailBodyWithSignature("<p>hey https://z.com</p>", acct({ signature: sigWithLink }));
    expect(result).toContain('href="https://z.com"');
    // Signature is appended verbatim — its URL stays plain text, no <a> wrap.
    expect(result).not.toContain('href="https://example.com"');
    expect(result).toContain("see https://example.com");
  });

  it("leaves mustache vars in body intact even after autolinkify", () => {
    const result = buildEmailBodyWithSignature(
      "<p>Hi {{firstName}}, visit https://z.com</p>",
      acct({ signature: "<p>--</p>" }),
    );
    expect(result).toContain("{{firstName}}");
    expect(result).toContain('href="https://z.com"');
  });

  it("strips a pre-existing plain-text signature before appending the new one (no cumulative stacking)", () => {
    const newSig = "<p>Best,<br>Jane</p>";
    const body = `<p>Hello world</p>\n\n--\n<p>Old signature from Bob</p>`;
    const result = buildEmailBodyWithSignature(body, acct({ signature: newSig }));
    expect(result).toBe(`<p>Hello world</p><p>--</p>${newSig}`);
    expect(result.match(/--/g)?.length).toBe(1);
  });

  it("strips a pre-existing HTML <p>--</p> signature marker before appending", () => {
    const newSig = "<p>Best,<br>Jane</p>";
    const body = `<p>Hello world</p><p>--</p><p>Old signature</p>`;
    const result = buildEmailBodyWithSignature(body, acct({ signature: newSig }));
    expect(result).not.toContain("Old signature");
    expect(result).toContain(newSig);
    expect((result.match(/Old signature/g) ?? []).length).toBe(0);
  });

  it("strips a pre-existing <br>--<br> signature marker before appending", () => {
    const newSig = "<p>Jane</p>";
    const body = `<p>Hello world</p><br>--<br><p>Old signature</p>`;
    const result = buildEmailBodyWithSignature(body, acct({ signature: newSig }));
    expect(result).not.toContain("Old signature");
    expect(result).toContain(newSig);
  });

  it("collapses 3 stacked signatures into exactly 1 signature (no cumulative)", () => {
    const newSig = "<p>Jane</p>";
    const sig1 = "<p>Sig One</p>";
    const sig2 = "<p>Sig Two</p>";
    const sig3 = "<p>Sig Three</p>";
    const body = `<p>Hello</p>\n\n--\n${sig1}\n\n--\n${sig2}\n\n--\n${sig3}`;
    const result = buildEmailBodyWithSignature(body, acct({ signature: newSig }));
    expect(result).not.toContain("Sig One");
    expect(result).not.toContain("Sig Two");
    expect(result).not.toContain("Sig Three");
    expect(result).toContain(newSig);
    expect(result).toBe(`<p>Hello</p><p>--</p>${newSig}`);
  });

  it("collapses stacked HTML signatures into exactly 1 signature", () => {
    const newSig = "<p>Jane</p>";
    const body = `<p>Hello</p><p>--</p><p>Sig One</p><p>--</p><p>Sig Two</p>`;
    const result = buildEmailBodyWithSignature(body, acct({ signature: newSig }));
    expect(result).not.toContain("Sig One");
    expect(result).not.toContain("Sig Two");
    expect(result).toContain(newSig);
  });

  it("idempotent: f(f(x)) === f(x)", () => {
    const newSig = "<p>Jane</p>";
    const body = "<p>Hello world</p>";
    const once = buildEmailBodyWithSignature(body, acct({ signature: newSig }));
    const twice = buildEmailBodyWithSignature(once, acct({ signature: newSig }));
    expect(twice).toBe(once);
  });

  it("account.signature wins over per-account default signature when present", () => {
    const result = buildEmailBodyWithSignature("<p>Hello</p>", acct({ signature: "<p>Account Sig</p>" }));
    expect(result).toBe("<p>Hello</p><p>--</p><p>Account Sig</p>");
    expect(result).not.toContain("Kevin Lourd");
  });

  it("canonical fallback also strips stacked sigs (idempotence preserved)", () => {
    const body = "<p>Hello</p>\n\n--\n<p>Old Sig 1</p>\n\n--\n<p>Old Sig 2</p>";
    const result = buildEmailBodyWithSignature(body, acct({ email: "kevinl@growthagency.dev", signature: "" }));
    expect(result).toContain("<p>Hello</p><p>--</p><p>Kevin Lourd<br>Distribute.you | Marketing Agency</p>");
    expect(result).not.toContain("Old Sig 1");
    expect(result).not.toContain("Old Sig 2");
  });
});

describe("stripAccountSignature", () => {
  it("returns body unchanged when no marker present", () => {
    expect(stripAccountSignature("<p>Hello world</p>")).toBe("<p>Hello world</p>");
    expect(stripAccountSignature("Just plain text")).toBe("Just plain text");
  });

  it("strips first plain-text `\\n\\n--\\n` marker and everything after", () => {
    const body = "<p>Hello</p>\n\n--\n<p>Bob signature</p>";
    expect(stripAccountSignature(body)).toBe("<p>Hello</p>");
  });

  it("strips HTML `<p>--</p>` marker (paragraph form) and everything after", () => {
    const body = "<p>Hello</p><p>--</p><p>Bob signature</p>";
    expect(stripAccountSignature(body)).toBe("<p>Hello</p>");
  });

  it("strips HTML `<br>--<br>` marker (line-break form) and everything after", () => {
    const body = "<p>Hello world</p><br>--<br><p>Sig</p>";
    expect(stripAccountSignature(body)).toBe("<p>Hello world</p>");
  });

  it("strips HTML `<div>--</div>` marker and everything after", () => {
    const body = "<div>Hello</div><div>--</div><div>Sig</div>";
    expect(stripAccountSignature(body)).toBe("<div>Hello</div>");
  });

  it("strips marker with `-- ` (trailing space, RFC 3676)", () => {
    const body = "<p>Hello</p>\n\n-- \n<p>Sig</p>";
    expect(stripAccountSignature(body)).toBe("<p>Hello</p>");
  });

  it("strips marker with `&nbsp;` adjacent (HTML non-breaking space)", () => {
    const body = "<p>Hello</p><p>--&nbsp;</p><p>Sig</p>";
    expect(stripAccountSignature(body)).toBe("<p>Hello</p>");
  });

  it("strips all of 3 stacked plain-text signatures via first occurrence", () => {
    const body = "<p>Hello</p>\n\n--\nSig1\n\n--\nSig2\n\n--\nSig3";
    expect(stripAccountSignature(body)).toBe("<p>Hello</p>");
  });

  it("strips all of 3 stacked HTML signatures via first occurrence", () => {
    const body = "<p>Hello</p><p>--</p><p>Sig1</p><p>--</p><p>Sig2</p><p>--</p><p>Sig3</p>";
    expect(stripAccountSignature(body)).toBe("<p>Hello</p>");
  });

  it("idempotent: f(f(x)) === f(x)", () => {
    const inputs = [
      "<p>Hello</p>\n\n--\n<p>Sig</p>",
      "<p>Hello</p><p>--</p><p>Sig</p>",
      "<p>Hello</p>",
      "<p>Hello</p><br>--<br><p>Sig</p>",
    ];
    for (const input of inputs) {
      const once = stripAccountSignature(input);
      const twice = stripAccountSignature(once);
      expect(twice).toBe(once);
    }
  });
});

describe("buildSequenceSteps", () => {
  it("should inject signature into every step bodyHtml", () => {
    const sig = "<p>Cheers</p>";
    const sequence = [
      { step: 1, bodyHtml: "<p>First</p>", daysSinceLastStep: 0 },
      { step: 2, bodyHtml: "<p>Second</p>", daysSinceLastStep: 3 },
    ];
    const steps = buildSequenceSteps("Subject", sequence, acct({ signature: sig }));
    expect(steps).toHaveLength(2);
    expect(steps[0].bodyHtml).toContain(sig);
    expect(steps[1].bodyHtml).toContain(sig);
    expect(steps[0].subject).toBe("Subject");
    expect(steps[1].subject).toBe("Subject");
    expect(steps[0].daysSinceLastStep).toBe(0);
    expect(steps[1].daysSinceLastStep).toBe(3);
  });

  it("should sort steps by step number", () => {
    const sequence = [
      { step: 3, bodyHtml: "C", daysSinceLastStep: 7 },
      { step: 1, bodyHtml: "A", daysSinceLastStep: 0 },
      { step: 2, bodyHtml: "B", daysSinceLastStep: 3 },
    ];
    const steps = buildSequenceSteps("Subject", sequence, acct());
    expect(steps[0].bodyHtml).toMatch(/^A\b/);
    expect(steps[1].bodyHtml).toMatch(/^B\b/);
    expect(steps[2].bodyHtml).toMatch(/^C\b/);
  });
});

describe("POST /send", () => {
  let runCounter: number;

  beforeEach(() => {
    vi.clearAllMocks();
    runCounter = 0;

    mockResolveInstantlyApiKey.mockResolvedValue({ key: "test-instantly-key", keySource: "platform" });
    mockAuthorizeCreditSpend.mockResolvedValue({ sufficient: true, balance_cents: 1000 });

    mockCreateRun.mockImplementation(() => {
      runCounter++;
      return Promise.resolve({ id: `step-run-${runCounter}` });
    });
    mockAddLeads.mockResolvedValue({ added: 1 });
    mockAddCosts.mockImplementation((runId: string, items: { costName: string }[]) => {
      return Promise.resolve({
        costs: items.map((item, i) => ({ id: `cost-${runId}-${item.costName}`, costName: item.costName })),
      });
    });
    mockUpdateRun.mockResolvedValue({});
    mockListAccounts.mockResolvedValue([{ email: "sender@example.com", warmup_status: 1, status: 1, stat_warmup_score: 100, signature: "<p>Best,<br>Sender</p>" }]);
    mockUpdateCampaign.mockResolvedValue({});
    mockGetCampaign.mockResolvedValue({ email_list: [], bcc_list: [], not_sending_status: null, status: "active" });
    mockDbReturning.mockResolvedValue([{ id: "lead-1" }]);
    mockDbInsertValues.mockReset();
    mockRefreshLeadStatusCurrent.mockResolvedValue(undefined);
  });

  it("should exclude blocked-domain accounts from new campaign creation", async () => {
    mockListAccounts.mockResolvedValue([
      { email: "blocked@arcadiaquest.org", warmup_status: 1, status: 1, stat_warmup_score: 100, signature: "<p>Sig</p>" },
      { email: "active@example.com", warmup_status: 1, status: 1, stat_warmup_score: 100, signature: "<p>Sig</p>" },
    ]);
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(mockUpdateCampaign).toHaveBeenCalledWith(
      "test-instantly-key",
      "inst-camp-new",
      expect.objectContaining({ email_list: ["active@example.com"] }),
    );
  });

  it("should exclude @distribute.you and @growthagency.dev (pulled-from-cold) accounts", async () => {
    mockListAccounts.mockResolvedValue([
      { email: "k@distribute.you", warmup_status: 1, status: 1, stat_warmup_score: 100, signature: "<p>Sig</p>" },
      { email: "k@growthagency.dev", warmup_status: 1, status: 1, stat_warmup_score: 100, signature: "<p>Sig</p>" },
      { email: "active@example.com", warmup_status: 1, status: 1, stat_warmup_score: 100, signature: "<p>Sig</p>" },
    ]);
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(mockUpdateCampaign).toHaveBeenCalledWith(
      "test-instantly-key",
      "inst-camp-new",
      expect.objectContaining({ email_list: ["active@example.com"] }),
    );
  });

  it("should exclude under-warmed accounts (Health Score < 100)", async () => {
    mockListAccounts.mockResolvedValue([
      { email: "warming@example.com", warmup_status: 1, status: 1, stat_warmup_score: 99 },
      { email: "noscore@example.com", warmup_status: 1, status: 1 },
    ]);
    mockDbWhere.mockResolvedValueOnce([]); // lead_id conflict check
    mockDbWhere.mockResolvedValueOnce([]); // findExistingCampaign

    const app = await createSendApp();
    const res = await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(res.status).toBe(500);
    expect(res.body.details).toContain("No active Instantly accounts available");
    expect(mockCreateCampaign).not.toHaveBeenCalled();
  });

  it("should send from a fully-warmed (score 100) account", async () => {
    mockListAccounts.mockResolvedValue([
      { email: "warming@example.com", warmup_status: 1, status: 1, stat_warmup_score: 95, signature: "<p>Sig</p>" },
      { email: "warmed@example.com", warmup_status: 1, status: 1, stat_warmup_score: 100, signature: "<p>Sig</p>" },
    ]);
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(mockUpdateCampaign).toHaveBeenCalledWith(
      "test-instantly-key",
      "inst-camp-new",
      expect.objectContaining({ email_list: ["warmed@example.com"] }),
    );
  });

  it("sets bcc_list on the campaign PATCH when bcc is provided", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app)
      .post("/send")
      .set(identityHeadersObj)
      .send({ ...validBody, bcc: ["a@x.com", "b@x.com"] });

    expect(mockUpdateCampaign).toHaveBeenCalledWith(
      "test-instantly-key",
      "inst-camp-new",
      expect.objectContaining({ bcc_list: ["a@x.com", "b@x.com"] }),
    );
  });

  it("omits bcc_list from the campaign PATCH when bcc absent", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    const patchArg = mockUpdateCampaign.mock.calls[0][2] as Record<string, unknown>;
    expect(patchArg).not.toHaveProperty("bcc_list");
  });

  it("omits bcc_list from the campaign PATCH when bcc is an empty array", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app)
      .post("/send")
      .set(identityHeadersObj)
      .send({ ...validBody, bcc: [] });

    const patchArg = mockUpdateCampaign.mock.calls[0][2] as Record<string, unknown>;
    expect(patchArg).not.toHaveProperty("bcc_list");
  });

  it("refreshes the Gold status row after attaching the real Instantly campaign id", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    const res = await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(res.status).toBe(200);
    expect(mockRefreshLeadStatusCurrent).toHaveBeenCalledWith(
      "inst-camp-new",
      validBody.to,
    );
  });

  it("should return 500 when only blocked-domain accounts are available", async () => {
    mockListAccounts.mockResolvedValue([
      { email: "a@arcadiaquest.org", warmup_status: 1, status: 1 },
      { email: "b@arcadiaquest.org", warmup_status: 1, status: 2 },
    ]);
    mockDbWhere.mockResolvedValueOnce([]); // lead_id conflict check
    mockDbWhere.mockResolvedValueOnce([]); // findExistingCampaign

    const app = await createSendApp();
    const res = await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(res.status).toBe(500);
    expect(res.body.details).toContain("No active Instantly accounts available");
    expect(mockCreateCampaign).not.toHaveBeenCalled();
  });

  it("should return 500 when all accounts are inactive", async () => {
    mockListAccounts.mockResolvedValue([
      { email: "inactive1@test.com", warmup_status: 0, status: 0 },
      { email: "inactive2@test.com", warmup_status: 0, status: 0 },
    ]);
    mockDbWhere.mockResolvedValueOnce([]); // lead_id conflict check
    mockDbWhere.mockResolvedValueOnce([]); // findExistingCampaign

    const app = await createSendApp();
    const res = await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(res.status).toBe(500);
    expect(res.body.details).toContain("No active Instantly accounts available");
    expect(mockCreateCampaign).not.toHaveBeenCalled();
  });

  it("should treat status 2 (active+warming) accounts as active", async () => {
    mockListAccounts.mockResolvedValue([
      { email: "warming@test.com", warmup_status: 1, status: 2, stat_warmup_score: 100, signature: "<p>Sig</p>" },
    ]);
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(mockUpdateCampaign).toHaveBeenCalledWith(
      "test-instantly-key",
      "inst-camp-new",
      expect.objectContaining({ email_list: ["warming@test.com"] }),
    );
  });

  it("should reject accounts with negative status", async () => {
    mockListAccounts.mockResolvedValue([
      { email: "suspended@test.com", warmup_status: 0, status: -3 },
    ]);
    mockDbWhere.mockResolvedValueOnce([]); // lead_id conflict check
    mockDbWhere.mockResolvedValueOnce([]); // findExistingCampaign

    const app = await createSendApp();
    const res = await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(res.status).toBe(500);
    expect(res.body.details).toContain("No active Instantly accounts available");
  });

  it("should only use active accounts and ignore inactive ones", async () => {
    mockListAccounts.mockResolvedValue([
      { email: "inactive@test.com", warmup_status: 0, status: 0 },
      { email: "active@test.com", warmup_status: 1, status: 1, stat_warmup_score: 100, signature: "<p>Sig</p>" },
    ]);
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    // The campaign should be assigned to the active account only
    expect(mockUpdateCampaign).toHaveBeenCalledWith(
      "test-instantly-key",
      "inst-camp-new",
      expect.objectContaining({ email_list: ["active@test.com"] }),
    );
  });

  it("should reject the old email format", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    const res = await request(app).post("/send").set(identityHeadersObj).send({
      ...validBody,
      subject: undefined,
      sequence: undefined,
      email: { subject: "Hello", body: "World" },
    });

    expect(res.status).toBe(400);
  });

  it("should create campaign with multi-step sequence", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(mockCreateCampaign).toHaveBeenCalledWith(
      "test-instantly-key",
      {
        name: "Campaign camp-1",
        steps: expect.arrayContaining([
          expect.objectContaining({ subject: "Hello", daysSinceLastStep: 0 }),
          expect.objectContaining({ subject: "Hello", daysSinceLastStep: 3 }),
          expect.objectContaining({ subject: "Hello", daysSinceLastStep: 7 }),
        ]),
      },
    );
  });

  it("should inject signature into all step bodies", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    // [0] = apiKey, [1] = params
    const createCall = mockCreateCampaign.mock.calls[0][1];
    for (const step of createCall.steps) {
      expect(step.bodyHtml).toContain("<p>Best,<br>Sender</p>");
    }
  });

  it("should enable stop_on_reply when patching campaign", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(mockUpdateCampaign).toHaveBeenCalledWith(
      "test-instantly-key",
      "inst-camp-new",
      expect.objectContaining({
        stop_on_reply: true,
        email_list: ["sender@example.com"],
      }),
    );
  });

  it("should store leadId and deliveryStatus in campaign insert", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    const campaignInsert = mockDbInsertValues.mock.calls.find(
      ([v]: [any]) => v.campaignId === "camp-1" && v.leadEmail === "test@example.com",
    );
    expect(campaignInsert).toBeDefined();
    expect(campaignInsert![0]).toMatchObject({
      leadId: "lead-1",
      deliveryStatus: "contacted",
    });
  });

  it("should create per-step runs with correct cost items: contact upload (step 1 only) + 2 email costs per step", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(mockCreateRun).toHaveBeenCalledTimes(3);
    expect(mockCreateRun).toHaveBeenCalledWith(expect.objectContaining({ taskName: "email-send-step-1" }), expect.objectContaining({ orgId: "org-1", userId: "user-1" }));
    expect(mockCreateRun).toHaveBeenCalledWith(expect.objectContaining({ taskName: "email-send-step-2" }), expect.objectContaining({ orgId: "org-1", userId: "user-1" }));
    expect(mockCreateRun).toHaveBeenCalledWith(expect.objectContaining({ taskName: "email-send-step-3" }), expect.objectContaining({ orgId: "org-1", userId: "user-1" }));

    expect(mockAddCosts).toHaveBeenCalledTimes(3);
    // Step 1: 2 email costs (provisioned — only actualized on webhook email_sent)
    // + contact upload (actual — lead is uploaded regardless of dispatch)
    expect(mockAddCosts).toHaveBeenCalledWith("step-run-1", [
      { costName: "instantly-account-email-sent", quantity: 1, costSource: "platform", status: "provisioned" },
      { costName: "instantly-domain-email-sent", quantity: 1, costSource: "platform", status: "provisioned" },
      { costName: "instantly-contact-uploaded", quantity: 1, costSource: "platform", status: "actual" },
    ], expect.objectContaining({ orgId: "org-1" }));
    // Steps 2-3: 2 email costs (provisioned), no contact upload
    expect(mockAddCosts).toHaveBeenCalledWith("step-run-2", [
      { costName: "instantly-account-email-sent", quantity: 1, costSource: "platform", status: "provisioned" },
      { costName: "instantly-domain-email-sent", quantity: 1, costSource: "platform", status: "provisioned" },
    ], expect.objectContaining({ orgId: "org-1" }));
    expect(mockAddCosts).toHaveBeenCalledWith("step-run-3", [
      { costName: "instantly-account-email-sent", quantity: 1, costSource: "platform", status: "provisioned" },
      { costName: "instantly-domain-email-sent", quantity: 1, costSource: "platform", status: "provisioned" },
    ], expect.objectContaining({ orgId: "org-1" }));
  });


  it("should store per-step email cost IDs in sequence_costs table (2 per step, excluding contact upload)", async () => {
    mockNewCampaignFlow();
    // Reset to track sequence_costs inserts
    mockDbReturning.mockReset();
    mockDbReturning.mockResolvedValueOnce([{ id: "sub-camp-1", campaignId: "camp-1", instantlyCampaignId: "inst-camp-new" }]);
    mockDbReturning.mockResolvedValueOnce([{ id: "lead-1" }]);
    mockDbReturning.mockResolvedValue([]); // for sequence_costs inserts

    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    // Check that sequence_costs were inserted for ALL steps with 2 rows per step (account + domain)
    // Contact upload cost should NOT be in sequence_costs
    const insertCalls = mockDbInsertValues.mock.calls;
    const sequenceCostInserts = insertCalls.filter(
      ([v]: [any]) => v.costId && v.step,
    );
    expect(sequenceCostInserts).toHaveLength(6); // 2 costs × 3 steps
    // Step 1: 2 provisioned costs (flipped to actual on webhook email_sent)
    expect(sequenceCostInserts[0][0]).toMatchObject({ step: 1, runId: "step-run-1", status: "provisioned" });
    expect(sequenceCostInserts[1][0]).toMatchObject({ step: 1, runId: "step-run-1", status: "provisioned" });
    // Step 2: 2 provisioned costs
    expect(sequenceCostInserts[2][0]).toMatchObject({ step: 2, runId: "step-run-2", status: "provisioned" });
    expect(sequenceCostInserts[3][0]).toMatchObject({ step: 2, runId: "step-run-2", status: "provisioned" });
    // Step 3: 2 provisioned costs
    expect(sequenceCostInserts[4][0]).toMatchObject({ step: 3, runId: "step-run-3", status: "provisioned" });
    expect(sequenceCostInserts[5][0]).toMatchObject({ step: 3, runId: "step-run-3", status: "provisioned" });
  });

  it("should work with a single-step sequence (1 run, 3 costs: account + domain + contact upload)", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    const singleStep = {
      ...validBody,
      sequence: [{ step: 1, bodyHtml: "<p>Only email</p>", daysSinceLastStep: 0 }],
    };

    const res = await request(app).post("/send").set(identityHeadersObj).send(singleStep);

    expect(res.status).toBe(200);
    expect(mockCreateRun).toHaveBeenCalledTimes(1);
    expect(mockCreateRun).toHaveBeenCalledWith(expect.objectContaining({ taskName: "email-send-step-1" }), expect.objectContaining({ orgId: "org-1" }));
    expect(mockAddCosts).toHaveBeenCalledTimes(1);
    expect(mockAddCosts).toHaveBeenCalledWith("step-run-1", [
      { costName: "instantly-account-email-sent", quantity: 1, costSource: "platform", status: "provisioned" },
      { costName: "instantly-domain-email-sent", quantity: 1, costSource: "platform", status: "provisioned" },
      { costName: "instantly-contact-uploaded", quantity: 1, costSource: "platform", status: "actual" },
    ], expect.objectContaining({ orgId: "org-1" }));
    expect(mockUpdateRun).toHaveBeenCalledWith("step-run-1", "completed", expect.objectContaining({ orgId: "org-1" }));
  });

  it("should skip Instantly API call and step runs when same lead already processed for campaign", async () => {
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([]); // lead_id conflict check
    // RESERVE upsert loses the claim (row already committed) → empty RETURNING.
    mockDbReturning.mockReset();
    mockDbReturning.mockResolvedValueOnce([]);

    const app = await createSendApp();
    const res = await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(res.status).toBe(200);
    expect(res.body.duplicate).toBe(true);
    expect(res.body.added).toBe(0);
    expect(mockCreateCampaign).not.toHaveBeenCalled();
    expect(mockAddLeads).not.toHaveBeenCalled();
    // No step runs or costs should be created for duplicates
    expect(mockCreateRun).not.toHaveBeenCalled();
    expect(mockAddCosts).not.toHaveBeenCalled();
    // AC2: no Instantly campaign created before the reservation is won.
    expect(mockDbDelete).not.toHaveBeenCalled(); // nothing to release (we never reserved)
  });

  it("should create separate campaigns for different leads in the same campaign", async () => {
    const app = await createSendApp();

    // Send 1: Lead A
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([]); // lead_id conflict check
    mockDbWhere.mockResolvedValueOnce([]); // findExistingCampaign
    mockCreateCampaign.mockResolvedValueOnce({ id: "inst-camp-A", status: "draft" });
    mockGetCampaign.mockReset();
    mockGetCampaign.mockResolvedValueOnce({ email_list: ["sender@example.com"], not_sending_status: null }); // verify after PATCH
    mockDbReturning.mockReset();
    mockDbReturning.mockResolvedValueOnce([{ id: "sub-A", campaignId: "camp-1", instantlyCampaignId: "inst-camp-A" }]);
    mockDbReturning.mockResolvedValueOnce([{ id: "lead-A" }]);
    mockDbReturning.mockResolvedValue([]);

    const res1 = await request(app).post("/send").set(identityHeadersObj).send({
      ...validBody,
      to: "alice@example.com",
      sequence: [{ step: 1, bodyHtml: "<p>Hi Alice</p>", daysSinceLastStep: 0 }],
    });
    expect(res1.status).toBe(200);

    // Send 2: Lead B (same campaignId, different lead)
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([]); // lead_id conflict check
    mockDbWhere.mockResolvedValueOnce([]); // findExistingCampaign
    mockCreateCampaign.mockResolvedValueOnce({ id: "inst-camp-B", status: "draft" });
    mockGetCampaign.mockReset();
    mockGetCampaign.mockResolvedValueOnce({ email_list: ["sender@example.com"], not_sending_status: null }); // verify after PATCH
    mockDbReturning.mockReset();
    mockDbReturning.mockResolvedValueOnce([{ id: "sub-B", campaignId: "camp-1", instantlyCampaignId: "inst-camp-B" }]);
    mockDbReturning.mockResolvedValueOnce([{ id: "lead-B" }]);
    mockDbReturning.mockResolvedValue([]);

    const res2 = await request(app).post("/send").set(identityHeadersObj).send({
      ...validBody,
      to: "bob@example.com",
      sequence: [{ step: 1, bodyHtml: "<p>Hi Bob</p>", daysSinceLastStep: 0 }],
    });
    expect(res2.status).toBe(200);

    expect(mockCreateCampaign).toHaveBeenCalledTimes(2);
    // [0] = apiKey, [1] = params
    const call1 = mockCreateCampaign.mock.calls[0][1];
    const call2 = mockCreateCampaign.mock.calls[1][1];
    expect(call1.steps[0].bodyHtml).toContain("Hi Alice");
    expect(call2.steps[0].bodyHtml).toContain("Hi Bob");
  });

  it("should ignore not_sending_status post-activate (no retry, no failure)", async () => {
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([]); // lead_id conflict check
    mockDbWhere.mockResolvedValueOnce([]); // findExistingCampaign

    // One-shot create + verify-PATCH (NSS=4 on verify but no longer error-signal).
    mockCreateCampaign.mockResolvedValueOnce({ id: "inst-camp-1", status: "draft" });
    mockGetCampaign.mockResolvedValueOnce({
      email_list: ["sender@example.com"],
      not_sending_status: 4, // daily limit hit on the chosen account — pacing only
    });

    mockDbReturning.mockResolvedValueOnce([{ id: "sub-camp-1", campaignId: "camp-1", instantlyCampaignId: "inst-camp-1" }]);
    mockDbReturning.mockResolvedValueOnce([{ id: "lead-1" }]);
    mockDbReturning.mockResolvedValue([]);

    const app = await createSendApp();
    const res = await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(res.status).toBe(200);
    expect(mockCreateCampaign).toHaveBeenCalledTimes(1);
    expect(mockCreateRun).toHaveBeenCalledTimes(3); // 3 step runs created
    expect(mockUpdateRun).toHaveBeenCalledTimes(3); // all steps completed immediately
  });

  it("should complete all step runs immediately (not just step 1)", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    // All 3 step runs should be completed immediately
    expect(mockUpdateRun).toHaveBeenCalledTimes(3);
    expect(mockUpdateRun).toHaveBeenCalledWith("step-run-1", "completed", expect.objectContaining({ orgId: "org-1" }));
    expect(mockUpdateRun).toHaveBeenCalledWith("step-run-2", "completed", expect.objectContaining({ orgId: "org-1" }));
    expect(mockUpdateRun).toHaveBeenCalledWith("step-run-3", "completed", expect.objectContaining({ orgId: "org-1" }));
  });

  it("should return 200 idempotent duplicate (NOT 409) when a concurrent request already claimed the lead", async () => {
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([]); // lead_id conflict check

    // RESERVE upsert loses the atomic claim — concurrent peer holds it. Empty
    // RETURNING ⇒ idempotent 200 duplicate, NOT a fatal 409.
    mockDbReturning.mockReset();
    mockDbReturning.mockResolvedValueOnce([]);

    const app = await createSendApp();
    const res = await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(res.status).toBe(200);
    expect(res.body.duplicate).toBe(true);
    expect(res.body.added).toBe(0);
    // AC2: the loser creates NO Instantly campaign (claim lost before dispatch).
    expect(mockCreateCampaign).not.toHaveBeenCalled();
    // No step runs/costs for the losing request, and nothing to release.
    expect(mockCreateRun).not.toHaveBeenCalled();
    expect(mockDbDelete).not.toHaveBeenCalled();
  });

  it("should return stepRuns array in response", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    const res = await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(res.status).toBe(200);
    expect(res.body.stepRuns).toHaveLength(3);
    expect(res.body.stepRuns[0]).toMatchObject({ step: 1, runId: "step-run-1" });
    expect(res.body.stepRuns[1]).toMatchObject({ step: 2, runId: "step-run-2" });
    expect(res.body.stepRuns[2]).toMatchObject({ step: 3, runId: "step-run-3" });
  });

  it("should read brandIds and workflowSlug from headers only", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app)
      .post("/send")
      .set({
        ...identityHeadersObj,
        "x-brand-id": "header-brand",
        "x-workflow-slug": "header-workflow",
      })
      .send(validBody);

    // Campaign insert should use header values
    const campaignInsert = mockDbInsertValues.mock.calls.find(
      ([v]: [any]) => v.leadEmail === "test@example.com" && v.instantlyCampaignId,
    );
    expect(campaignInsert).toBeDefined();
    expect(campaignInsert![0].brandIds).toEqual(["header-brand"]);
    expect(campaignInsert![0].workflowSlug).toBe("header-workflow");
  });

  it("should parse multi-brand CSV header into brandIds array", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app)
      .post("/send")
      .set({
        ...identityHeadersObj,
        "x-brand-id": "brand-a,brand-b,brand-c",
      })
      .send(validBody);

    const campaignInsert = mockDbInsertValues.mock.calls.find(
      ([v]: [any]) => v.leadEmail === "test@example.com" && v.instantlyCampaignId,
    );
    expect(campaignInsert).toBeDefined();
    expect(campaignInsert![0].brandIds).toEqual(["brand-a", "brand-b", "brand-c"]);
  });

  it("should forward tracking headers to runs-service", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app)
      .post("/send")
      .set({
        ...identityHeadersObj,
        "x-brand-id": "header-brand",
        "x-campaign-id": "header-camp",
        "x-workflow-slug": "header-wf",
        "x-goal": "signup",
        "x-brand-profile-id": "brand-profile-1",
        "x-audience-id": "audience-1",
      })
      .send(validBody);

    const campaignInsert = mockDbInsertValues.mock.calls.find(
      ([v]: [any]) => v.leadEmail === "test@example.com" && v.instantlyCampaignId,
    );
    expect(campaignInsert).toBeDefined();
    expect(campaignInsert![0].metadata).toEqual({
      goal: "signup",
      brandProfileId: "brand-profile-1",
      audienceId: "audience-1",
    });

    // createRun should receive tracking in identity context
    expect(mockCreateRun).toHaveBeenCalledWith(
      expect.any(Object),
      expect.objectContaining({
        tracking: expect.objectContaining({
          brandId: "header-brand",
          campaignId: "header-camp",
          workflowSlug: "header-wf",
          goal: "signup",
          brandProfileId: "brand-profile-1",
          audienceId: "audience-1",
        }),
      }),
    );
  });

  it("should accept platform sends without campaign context and not pass campaignId to runs-service", async () => {
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([]); // lead_id conflict check
    mockCreateCampaign.mockResolvedValueOnce({ id: "inst-camp-platform", status: "draft" });
    mockGetCampaign.mockResolvedValueOnce({ email_list: ["sender@example.com"], not_sending_status: null });
    mockDbReturning.mockReset();
    mockDbReturning.mockResolvedValueOnce([{ id: "sub-platform", campaignId: null, instantlyCampaignId: "inst-camp-platform" }]);
    mockDbReturning.mockResolvedValueOnce([{ id: "lead-1" }]);
    mockDbReturning.mockResolvedValue([]);

    const app = await createSendApp();
    const res = await request(app)
      .post("/send")
      .set({
        "x-org-id": "org-1",
        "x-user-id": "user-1",
        "x-run-id": "run-1",
        "x-brand-id": "brand-1",
      })
      .send(validBody);

    expect(res.status).toBe(200);
    expect(res.body.campaignId).toBeNull();
    expect(mockDbWhere).toHaveBeenCalledTimes(1);
    expect(mockCreateRun).toHaveBeenCalledTimes(3);
    for (const [params, identity] of mockCreateRun.mock.calls) {
      expect(params.campaignId).toBeUndefined();
      expect(identity.tracking?.campaignId).toBeUndefined();
    }

    const campaignInsert = mockDbInsertValues.mock.calls.find(
      ([v]: [any]) => v.leadEmail === "test@example.com" && v.instantlyCampaignId,
    );
    expect(campaignInsert![0].campaignId).toBeNull();

    const sequenceCostInserts = mockDbInsertValues.mock.calls.filter(
      ([v]: [any]) => v.costId && v.step,
    );
    expect(sequenceCostInserts).toHaveLength(6);
    for (const [value] of sequenceCostInserts) {
      expect(value.campaignId).toBeNull();
    }
  });

  // ── Platform-send idempotency (campaignId NULL) ─────────────────────────────
  // A platform send carries no x-campaign-id, so campaignId is null. The
  // reservation must collide on a retry via the (run_id, lead_email) partial
  // unique index — NOT (campaign_id, lead_email), which never collides on null
  // campaign (Postgres NULLs are DISTINCT). Guards the 2026-06-27 dup incident.
  const platformHeaders = {
    "x-org-id": "org-1",
    "x-user-id": "user-1",
    "x-run-id": "run-1",
    "x-brand-id": "brand-1",
  };

  function mockPlatformWinnerFlow() {
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([]); // lead_id conflict check (no conflict)
    mockCreateCampaign.mockResolvedValue({ id: "inst-camp-plat", status: "draft" });
    mockGetCampaign.mockResolvedValueOnce({ email_list: ["sender@example.com"], not_sending_status: null });
    mockDbReturning.mockReset();
    mockDbReturning.mockResolvedValueOnce([{ id: "sub-plat", campaignId: null, instantlyCampaignId: "inst-camp-plat" }]); // RESERVE → winner
    mockDbReturning.mockResolvedValueOnce([{ id: "lead-1" }]); // lead insert
    mockDbReturning.mockResolvedValue([]);
    mockUpdateCampaignStatus.mockResolvedValue({});
  }

  it("platform send (campaignId null) reserves on the (run_id, lead_email) arbiter", async () => {
    mockPlatformWinnerFlow();
    const app = await createSendApp();

    const res = await request(app).post("/send").set(platformHeaders).send(validBody);

    expect(res.status).toBe(200);
    expect(res.body.campaignId).toBeNull();
    const cfg = mockOnConflictDoUpdate.mock.calls[0][0];
    expect(cfg.target).toEqual(["run_id", "lead_email"]);
    expect(cfg.targetWhere).toBeDefined(); // partial index predicate (campaign_id IS NULL AND status='active')
  });

  it("campaign send (campaignId present) reserves on the (campaign_id, lead_email) arbiter", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    const res = await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(res.status).toBe(200);
    const cfg = mockOnConflictDoUpdate.mock.calls[0][0];
    expect(cfg.target).toEqual(["campaign_id", "lead_email"]);
    expect(cfg.targetWhere).toBeUndefined(); // full unique index, no partial predicate
  });

  it("platform retry (same leadEmail + same runId, campaignId null) is an idempotent duplicate — no 2nd campaign", async () => {
    const app = await createSendApp();

    // 1st send: wins the reservation, creates the Instantly campaign.
    mockPlatformWinnerFlow();
    const res1 = await request(app).post("/send").set(platformHeaders).send(validBody);
    expect(res1.status).toBe(200);
    expect(res1.body.duplicate).toBeUndefined();
    expect(mockCreateCampaign).toHaveBeenCalledTimes(1);

    // 2nd send (the timeout-retry): the (run_id, lead_email) reservation
    // collides → empty RETURNING → idempotent 200 duplicate, NO 2nd campaign.
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([]); // lead_id conflict check
    mockDbReturning.mockReset();
    mockDbReturning.mockResolvedValueOnce([]); // RESERVE upsert loses the claim

    const res2 = await request(app).post("/send").set(platformHeaders).send(validBody);
    expect(res2.status).toBe(200);
    expect(res2.body.duplicate).toBe(true);
    expect(res2.body.added).toBe(0);
    expect(mockCreateCampaign).toHaveBeenCalledTimes(1); // still ONE — no duplicate campaign
  });

  it("should return 402 when credit authorization fails for platform keySource", async () => {
    mockAuthorizeCreditSpend.mockResolvedValue({ sufficient: false, balance_cents: 2, required_cents: 15 });
    const app = await createSendApp();

    const res = await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(res.status).toBe(402);
    expect(res.body.error).toBe("Insufficient credits");
    expect(res.body.balance_cents).toBe(2);
    expect(res.body.required_cents).toBe(15);
    expect(mockCreateCampaign).not.toHaveBeenCalled();
    expect(mockCreateRun).not.toHaveBeenCalled();
  });

  it("should call authorizeCreditSpend with 3 cost items (contact + account + domain)", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(mockAuthorizeCreditSpend).toHaveBeenCalledWith(
      [
        { costName: "instantly-contact-uploaded", quantity: 1 },
        { costName: "instantly-account-email-sent", quantity: 3 },
        { costName: "instantly-domain-email-sent", quantity: 3 },
      ],
      "instantly-send",
      expect.objectContaining({
        orgId: "org-1",
        userId: "user-1",
        runId: "run-1",
      }),
    );
  });

  it("should skip credit authorization when keySource is org (BYOK)", async () => {
    mockResolveInstantlyApiKey.mockResolvedValue({ key: "org-key", keySource: "org" });
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(mockAuthorizeCreditSpend).not.toHaveBeenCalled();
    expect(mockCreateCampaign).toHaveBeenCalled();
  });

  it("should return 409 when email already exists with a different leadId", async () => {
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([{ leadId: "existing-lead-99" }]); // lead_id conflict found

    const app = await createSendApp();
    const res = await request(app).post("/send").set(identityHeadersObj).send({
      ...validBody,
      leadId: "different-lead-1",
    });

    expect(res.status).toBe(409);
    expect(res.body.error).toBe("Lead ID conflict");
    expect(res.body.details).toContain("existing-lead-99");
    expect(res.body.details).toContain("different-lead-1");
    expect(mockCreateCampaign).not.toHaveBeenCalled();
  });

  // ── Reservation idempotency (DIS-148) ──────────────────────────────────────

  it("AC2: reserves the (campaignId, leadEmail) row with a reserving:<uuid> sentinel BEFORE any Instantly call", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    await request(app).post("/send").set(identityHeadersObj).send(validBody);

    // First instantly_campaigns insert is the RESERVE — it carries the sentinel
    // and must precede createCampaign (the external side-effect).
    const reserveInsert = mockDbInsertValues.mock.calls.find(
      ([v]: [any]) => v.campaignId === "camp-1" && v.leadEmail === "test@example.com",
    );
    expect(reserveInsert).toBeDefined();
    expect(reserveInsert![0].instantlyCampaignId).toMatch(/^reserving:/);
    expect(reserveInsert![0].deliveryStatus).toBe("contacted");

    // Ordering: the reserve insert was recorded before createCampaign fired.
    const reserveInsertOrder = mockDbInsertValues.mock.invocationCallOrder[0];
    const createCampaignOrder = mockCreateCampaign.mock.invocationCallOrder[0];
    expect(reserveInsertOrder).toBeLessThan(createCampaignOrder);
  });

  it("winner: overwrites the sentinel with the real Instantly campaign id (phase-2) and 200s", async () => {
    mockNewCampaignFlow();
    const app = await createSendApp();

    const res = await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(res.status).toBe(200);
    expect(res.body.success).toBe(true);
    expect(mockCreateCampaign).toHaveBeenCalled();
    // Winner did NOT release its reservation.
    expect(mockDbDelete).not.toHaveBeenCalled();
  });

  it("AC4: releases the reservation (delete) when sendLeadToInstantly finds no accounts", async () => {
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([]); // lead_id conflict check
    mockDbReturning.mockReset();
    mockDbReturning.mockResolvedValueOnce([{ id: "reserved-1", campaignId: "camp-1", instantlyCampaignId: "reserving:x" }]); // RESERVE → winner
    // No healthy accounts → sendLeadToInstantly returns { ok: false }.
    mockListAccounts.mockResolvedValue([
      { email: "dead@test.com", warmup_status: 0, status: 0 },
    ]);

    const app = await createSendApp();
    const res = await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(res.status).toBe(500);
    expect(res.body.details).toContain("No active Instantly accounts available");
    expect(mockCreateCampaign).not.toHaveBeenCalled();
    // Reservation released so a later legit retry can re-claim.
    expect(mockDbDelete).toHaveBeenCalledTimes(1);
  });

  it("AC4: releases the reservation when a later step throws after the campaign was dispatched", async () => {
    mockDbWhere.mockReset();
    mockDbWhere.mockResolvedValueOnce([]); // lead_id conflict check
    mockDbReturning.mockReset();
    mockDbReturning.mockResolvedValueOnce([{ id: "reserved-2", campaignId: "camp-1", instantlyCampaignId: "reserving:y" }]); // RESERVE → winner
    mockDbReturning.mockResolvedValueOnce([{ id: "lead-1" }]); // lead insert
    mockDbReturning.mockResolvedValue([]);
    mockCreateCampaign.mockResolvedValue({ id: "inst-camp-throw", status: "draft" });
    mockGetCampaign.mockResolvedValue({ email_list: ["sender@example.com"], not_sending_status: null });
    // Step-run creation throws → handler unwinds into the catch.
    mockCreateRun.mockReset();
    mockCreateRun.mockRejectedValue(new Error("runs-service down"));

    const app = await createSendApp();
    const res = await request(app).post("/send").set(identityHeadersObj).send(validBody);

    expect(res.status).toBe(500);
    // releaseReservation runs in the catch (no-op at DB level once phase-2 ran,
    // but the handler still attempts it for any still-open reservation).
    expect(mockDbDelete).toHaveBeenCalledTimes(1);
  });
});
