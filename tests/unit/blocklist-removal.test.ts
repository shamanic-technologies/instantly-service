/**
 * Automated SURBL removal requests (owner 2026-10-16): filled with the owner's
 * own record, once a week per listed domain, bot traps left untouched, every
 * outcome classified. Fixtures are SURBL's real pages (2026-10-16).
 */
import fs from "node:fs";
import path from "node:path";
import { describe, it, expect, vi } from "vitest";

vi.mock("../../src/db", () => ({ db: {} }));

import {
  REMOVAL_COOLDOWN_DAYS,
  classifyProcessorAnswer,
  fileSurblRemoval,
  parseLookupAnswer,
  removalDue,
  removalFormFields,
  type RemovalContact,
} from "../../src/lib/blocklist-removal";
import { contactFromIdentity } from "../../src/lib/owner-identity-client";

const LISTED = fs.readFileSync(path.join(__dirname, "..", "helpers", "surbl-lookup-listed.html"), "utf8");
const FORM = fs.readFileSync(path.join(__dirname, "..", "helpers", "surbl-lookup-form.html"), "utf8");

const CONTACT: RemovalContact = {
  fullName: "Kevin Lourd",
  email: "kevin@distribute.you",
  phone: "+33 1 23 45 67 89",
  organization: "Blooming Generation SASU",
  address: "285 rue de l'Église, 46140 Douelle, France",
  website: "https://distribute.you",
};
const SAMPLE = { headers: "From: x\nTo: y", body: "<p>Hello</p>" };

describe("reading SURBL's lookup answer", () => {
  it("a listed domain shows the removal form, its action and its token", () => {
    const a = parseLookupAnswer(LISTED);
    expect(a.listed).toBe(true);
    expect(a.action).toBe("forms/processor.php");
    expect(a.token).toMatch(/^d2hpdGVsaXN0/);
  });

  it("the lookup page alone is not a listing", () => {
    expect(parseLookupAnswer(FORM)).toMatchObject({ listed: false, action: null });
  });
});

describe("the form's words", () => {
  const fields = removalFormFields({
    domain: "agileconsultco.com",
    contact: CONTACT,
    sample: SAMPLE,
    sendingServers: "Google Workspace outbound servers (smtp.gmail.com).",
    stoppedNewContactsOn: "2026-10-16",
    token: "tok",
  });

  it("leaves every bot trap untouched: hidden fields empty, the hidden reference code never quoted", () => {
    expect(fields.contact_url).toBe("");
    expect(fields.email_confirm).toBe("");
    expect(JSON.stringify(fields)).not.toMatch(/SR-[0-9A-F]+/);
  });

  it("files in the owner's name, ASCII only, our real practice and our published terms", () => {
    expect(fields).toMatchObject({
      name: "Kevin Lourd",
      email: "kevin@distribute.you",
      org: "Blooming Generation SASU",
      address1: "285 rue de l'Eglise, 46140 Douelle, France",
      fp: "agileconsultco.com",
      policy_link: "https://distribute.you/terms",
      fg_token: "tok",
    });
    expect(fields.justify).toMatch(/verifier/);
    expect(fields.justify).toMatch(/2026-10-16/);
    expect(fields.policy).toMatch(/unsolicited bulk email in violation of any anti-spam law/);
    expect(Object.values(fields).join("")).toMatch(/^[\x09\x0A\x0D\x20-\x7E]*$/);
  });
});

describe("once a week", () => {
  const now = new Date("2026-10-23T06:00:00Z");
  it("never asked: due", () => expect(removalDue(null, now)).toBe(true));
  it("asked 6 days ago: not due", () => expect(removalDue(new Date("2026-10-17T07:00:00Z"), now)).toBe(false));
  it(`asked ${REMOVAL_COOLDOWN_DAYS} days ago: due again`, () =>
    expect(removalDue(new Date("2026-10-16T06:00:00Z"), now)).toBe(true));
});

describe("what SURBL answered", () => {
  it("2xx with no complaint: sent", () => expect(classifyProcessorAnswer(200, "<p>Thank you, we will review it.</p>")).toBe("sent"));
  it("a page naming a missing field: refused", () => expect(classifyProcessorAnswer(200, "<p>Error: phone is required</p>")).toBe("refused"));
  it("a server error: failed", () => expect(classifyProcessorAnswer(503, "")).toBe("failed"));
});

describe("the whole exchange", () => {
  it("looks up, then posts the removal form to the processor with the form's own token", async () => {
    const calls: Array<{ url: string; body: string | null }> = [];
    const fake = vi.fn(async (url: string | URL, init?: RequestInit) => {
      calls.push({ url: String(url), body: (init?.body as string) ?? null });
      const html = calls.length === 1 ? FORM : calls.length === 2 ? LISTED : "<p>Thank you</p>";
      return new Response(html, { status: 200 });
    }) as unknown as typeof fetch;
    const out = await fileSurblRemoval({
      domain: "agileconsultco.com",
      contact: CONTACT,
      sample: SAMPLE,
      sendingServers: "Google",
      stoppedNewContactsOn: "2026-10-16",
      fetchImpl: fake,
    });
    expect(out.outcome).toBe("sent");
    expect(calls[2]!.url).toBe("https://www.surbl.org/lookupform/forms/processor.php");
    const posted = new URLSearchParams(calls[2]!.body!);
    expect(posted.get("fp")).toBe("agileconsultco.com");
    expect(posted.get("fg_token")).toMatch(/^d2hpdGVsaXN0/);
    expect(out.request?.phone).toBe("+33 1 23 45 67 89");
  });

  it("a domain SURBL no longer lists sends nothing", async () => {
    let n = 0;
    const fake = (async () => new Response(n++ === 0 ? FORM : FORM, { status: 200 })) as unknown as typeof fetch;
    const out = await fileSurblRemoval({ domain: "x.com", contact: CONTACT, sample: SAMPLE, sendingServers: "g", stoppedNewContactsOn: "d", fetchImpl: fake });
    expect(out).toMatchObject({ outcome: "not_listed", request: null });
  });

  it("a network error is a stored failure, never a throw", async () => {
    const fake = (async () => {
      throw new Error("ECONNRESET");
    }) as unknown as typeof fetch;
    const out = await fileSurblRemoval({ domain: "x.com", contact: CONTACT, sample: SAMPLE, sendingServers: "g", stoppedNewContactsOn: "d", fetchImpl: fake });
    expect(out).toMatchObject({ outcome: "failed", error: "ECONNRESET" });
  });
});

describe("the owner's record decides who files", () => {
  const identity = {
    person: { fullName: "Kevin Lourd", email: "kevin@distribute.you", phone: null, postalAddress: null },
    company: { legalName: "Blooming Generation SASU", siren: "882102775", registeredOffice: "285 rue de l'Église, 46140 Douelle, France", vatNumber: null, brand: "distribute.you", website: "https://distribute.you" },
    updatedAt: null,
  };
  it("an unset phone is named, never guessed", () => {
    expect(() => contactFromIdentity(identity)).toThrow(/not set on kevinlourd.com \(Identity page\): phone/);
  });
  it("every fact set: the contact", () => {
    const c = contactFromIdentity({ ...identity, person: { ...identity.person, phone: "+33 6 00 00 00 00" } });
    expect(c).toMatchObject({ phone: "+33 6 00 00 00 00", organization: "Blooming Generation SASU", address: "285 rue de l'Église, 46140 Douelle, France" });
  });
});
