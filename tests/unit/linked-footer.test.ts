import { describe, it, expect, vi, beforeEach } from "vitest";

const mockExecute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => mockExecute(...a) } }));

const mockGetCampaign = vi.fn();
const mockUpdateCampaign = vi.fn();
vi.mock("../../src/lib/instantly-client", () => ({
  getCampaign: (...a: unknown[]) => mockGetCampaign(...a),
  updateCampaign: (...a: unknown[]) => mockUpdateCampaign(...a),
}));

import { hasLinkedFooter, replaceLinkedFooter, planFooterFixes } from "../../src/lib/linked-footer";
import { runLinkedFooterCleanup } from "../../src/lib/linked-footer-cleanup";
import { UNSUBSCRIBE_FOOTER_HTML } from "../../src/lib/send-lead";

// The old footer EXACTLY as Instantly stores it (read out of prod bronze config,
// 2026-09-24): its sanitizer turned `&nbsp;` into a real U+00A0.
const STORED_OLD_FOOTER =
  '<p> </p><p style="font-size:12px;color:#999999;font-style:italic">' +
  "Don't want to hear from me again? " +
  '<a href="{unsubscribe_link}" style="color:#999999">unsubscribe</a></p>';
const SIG = "<p>--</p><p>Amy Moore<br>Distribute.you | Marketing Agency</p>";
const body = (text: string) => `<p>${text}</p>${SIG}${STORED_OLD_FOOTER}`;

const pg = (rows: unknown[]) => ({ command: "SELECT", rowCount: rows.length, oid: 0, fields: [], rows });

describe("linked footer rewrite", () => {
  it("recognises the footer as Instantly stores it, and the entity form too", () => {
    expect(hasLinkedFooter(body("Hi"))).toBe(true);
    expect(hasLinkedFooter(body("Hi").replace(" ", "&nbsp;"))).toBe(true);
  });

  it("does not see a footer that is not there (negative control)", () => {
    expect(hasLinkedFooter(`<p>Hi</p>${SIG}`)).toBe(false);
    expect(hasLinkedFooter(`<p>Hi</p>${SIG}${UNSUBSCRIBE_FOOTER_HTML}`)).toBe(false);
  });

  it("swaps the linked footer for the current link-free line and leaves the rest alone", () => {
    const out = replaceLinkedFooter(body("Hi Julio"));
    expect(out).toBe(`<p>Hi Julio</p>${SIG}${UNSUBSCRIBE_FOOTER_HTML}`);
    expect(out).not.toContain("<a ");
    expect(out).not.toContain("{unsubscribe_link}");
    expect(out).not.toContain("hear from me again");
  });

  it("is idempotent", () => {
    const once = replaceLinkedFooter(body("Hi"));
    expect(replaceLinkedFooter(once)).toBe(once);
    expect(hasLinkedFooter(once)).toBe(false);
  });

  it("rewrites only the steps not yet sent, and reports the sent ones", () => {
    const plan = planFooterFixes(
      [
        { index: 0, body: body("step 1") },
        { index: 1, body: body("step 2") },
        { index: 2, body: body("step 3") },
      ],
      1,
    );
    expect(plan.fixes.map((f) => f.index)).toEqual([1, 2]);
    expect(plan.skippedAlreadySent).toEqual([1]);
  });
});

describe("runLinkedFooterCleanup", () => {
  const steps = [1, 2, 3].map((n) => ({ type: "email", delay: 3, variants: [{ subject: "S", body: body(`step ${n}`) }] }));

  beforeEach(() => {
    mockExecute.mockReset();
    mockGetCampaign.mockReset();
    mockUpdateCampaign.mockReset();
    mockExecute.mockResolvedValue(pg([{ instantlyCampaignId: "ic-1", lastSentStep: 1 }]));
    mockGetCampaign.mockResolvedValue({ sequences: [{ steps }] });
  });

  it("dry-run reads live bodies and patches nothing", async () => {
    const s = await runLinkedFooterCleanup("k", { commit: false, log: () => {} });
    expect(mockUpdateCampaign).not.toHaveBeenCalled();
    expect(s).toMatchObject({ candidates: 1, wouldPatch: 1, stepsFixed: 2, skippedAlreadySent: 1, patched: 0 });
  });

  it("commit sends the FULL step array back, with the sent step untouched", async () => {
    const s = await runLinkedFooterCleanup("k", { commit: true, log: () => {} });
    expect(s.patched).toBe(1);
    const [, id, params] = mockUpdateCampaign.mock.calls[0];
    expect(id).toBe("ic-1");
    const sent = (params as { sequences: Array<{ steps: typeof steps }> }).sequences[0].steps;
    expect(sent).toHaveLength(3);
    expect(sent[0].variants[0].body).toBe(body("step 1"));
    expect(sent[1].variants[0].body).not.toContain("<a ");
    expect(sent[2].variants[0].body).toContain(UNSUBSCRIBE_FOOTER_HTML);
  });

  it("counts a failing campaign instead of reading as a clean run", async () => {
    mockGetCampaign.mockRejectedValue(new Error("instantly 500"));
    const s = await runLinkedFooterCleanup("k", { commit: true, log: () => {} });
    expect(s.failed).toBe(1);
    expect(mockUpdateCampaign).not.toHaveBeenCalled();
  });

  it("candidate query excludes non-Instantly ids and requires a step still to send", async () => {
    await runLinkedFooterCleanup("k", { commit: false, log: () => {} });
    const q = JSON.stringify(mockExecute.mock.calls[0][0]);
    expect(q).toContain("NOT LIKE 'reserving:%'");
    expect(q).toContain("NOT LIKE 'self:%'");
    expect(q).toContain("sc.status = 'provisioned'");
    expect(q).toContain("inferred = false");
  });
});
