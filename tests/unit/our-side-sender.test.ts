import { describe, it, expect, vi, beforeEach } from "vitest";

const mockDbExecute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => mockDbExecute(...a) } }));
const mockBrand = vi.fn();
vi.mock("../../src/lib/brand-client", () => ({
  getBrandHandoffContext: (...a: unknown[]) => mockBrand(...a),
}));
const mockMembers = vi.fn();
vi.mock("../../src/lib/notify-brand-team", () => ({
  brandTeamMembers: (...a: unknown[]) => mockMembers(...a),
}));
vi.mock("../../src/lib/self-send/own-mail-domains", () => ({
  loadOwnMailDomains: async () => new Set(["gemkernelio.com", "distribute.you"]),
}));

import {
  isOurSideSender,
  loadOurSide,
  quickSenderSide,
  senderIsOurSide,
} from "../../src/lib/self-send/our-side-sender";

function pgResult<T>(rows: T[]) {
  return { command: "SELECT", rowCount: rows.length, oid: null, fields: [], rows };
}

const JERRY = "jerryclark@alpinespinalrehab.com";
const DOC_DINNERS = {
  addresses: new Set(["dev@docdinners.com", "rep.personal@gmail.com"]),
  domains: new Set(["docdinners.com", "gemkernelio.com", "distribute.you"]),
};

beforeEach(() => {
  vi.resetAllMocks();
});

describe("isOurSideSender — who wrote a message in a prospect's thread", () => {
  it("the rep we Cc'd at the hand-over is our side (Dev, Doc Dinners, 2026-10-14)", () => {
    expect(isOurSideSender('"Dev-Raj Mandroop" <dev@docdinners.com>', JERRY, DOC_DINNERS)).toBe(true);
  });

  it("anyone at the brand's own domain is our side (info@docdinners.com answering Dr. Curran)", () => {
    expect(isOurSideSender("info@docdinners.com", JERRY, DOC_DINNERS)).toBe(true);
  });

  it("another of our personas and our staff are our side", () => {
    expect(isOurSideSender("hanna@gemkernelio.com", JERRY, DOC_DINNERS)).toBe(true);
    expect(isOurSideSender("kevin@distribute.you", JERRY, DOC_DINNERS)).toBe(true);
    expect(quickSenderSide("kevin.lourd@gmail.com", JERRY)).toBe("ours");
  });

  it("the prospect is the prospect", () => {
    expect(isOurSideSender('"Alpine Spinal Rehab" <JerryClark@alpinespinalrehab.com>', JERRY, DOC_DINNERS)).toBe(false);
  });

  it("a colleague or assistant at the prospect's company is the prospect's side", () => {
    expect(isOurSideSender("frontdesk@alpinespinalrehab.com", JERRY, DOC_DINNERS)).toBe(false);
  });

  it("the prospect answering from a second address of their own stays the prospect (jts, 2026-04-09)", () => {
    expect(isOurSideSender("jts@travelnewsgroup.com", "jts@etn.travel", DOC_DINNERS)).toBe(false);
    expect(isOurSideSender("jerry.personal@gmail.com", JERRY, DOC_DINNERS)).toBe(false);
  });

  it("a public mailbox domain is matched by exact address, never by domain", () => {
    const withGmail = { ...DOC_DINNERS, domains: new Set([...DOC_DINNERS.domains, "gmail.com"]) };
    expect(isOurSideSender("stranger@gmail.com", JERRY, withGmail)).toBe(false);
    expect(isOurSideSender("rep.personal@gmail.com", JERRY, withGmail)).toBe(true);
  });

  it("the lead's own domain is never ours, even if the brand shares it", () => {
    const same = { addresses: new Set<string>(), domains: new Set(["alpinespinalrehab.com"]) };
    expect(isOurSideSender("office@alpinespinalrehab.com", JERRY, same)).toBe(false);
  });

  it("an unreadable sender keeps the prospect reading", () => {
    expect(isOurSideSender(null, JERRY, DOC_DINNERS)).toBe(false);
    expect(isOurSideSender("garbage", JERRY, DOC_DINNERS)).toBe(false);
  });
});

describe("loadOurSide — who our side is for one thread", () => {
  it("unions our domains, the brand's domain and rep, the org's people and everyone we copied", async () => {
    mockDbExecute
      .mockResolvedValueOnce(pgResult([{ org_id: "org-1", brand_ids: ["brand-1"] }]))
      .mockResolvedValueOnce(pgResult([{ cc: "kevin@distribute.you,dev@docdinners.com", bcc: null }]));
    mockBrand.mockResolvedValue({ name: "Doc Dinners", domain: "docdinners.com", rep: { email: "Marie@DocDinners.com", firstName: null, role: null } });
    mockMembers.mockResolvedValue([{ email: "owner@docdinners.com", firstName: null }]);

    const side = await loadOurSide("self:d1306a8c");

    expect([...side.domains]).toEqual(expect.arrayContaining(["docdinners.com", "gemkernelio.com", "distribute.you"]));
    expect([...side.addresses]).toEqual(
      expect.arrayContaining(["dev@docdinners.com", "kevin@distribute.you", "marie@docdinners.com", "owner@docdinners.com"]),
    );
    expect(mockBrand).toHaveBeenCalledWith("brand-1", "org-1");
    expect(mockMembers).toHaveBeenCalledWith("org-1");
  });

  it("fails LOUD when brand-service cannot be read (the message stays unfiled, re-read next poll)", async () => {
    mockDbExecute
      .mockResolvedValueOnce(pgResult([{ org_id: "org-1", brand_ids: ["brand-1"] }]))
      .mockResolvedValueOnce(pgResult([]));
    mockBrand.mockRejectedValue(new Error("brand-service down"));

    await expect(loadOurSide("self:x")).rejects.toThrow("brand-service down");
  });

  it("senderIsOurSide never loads anything for the lead's own message", async () => {
    await expect(senderIsOurSide(JERRY, { instantlyCampaignId: "self:x", leadEmail: JERRY })).resolves.toBe(false);
    expect(mockDbExecute).not.toHaveBeenCalled();
    expect(mockBrand).not.toHaveBeenCalled();
  });
});
