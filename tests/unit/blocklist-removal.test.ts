/**
 * Automated SURBL removal requests (owner 2026-10-16): sent BY EMAIL to
 * whitelist@surbl.org (the channel SURBL's page offers; its web form refuses
 * automated posts), from the company mailbox, once a week per listed domain,
 * every outcome classified.
 */
import { describe, it, expect, vi } from "vitest";

vi.mock("../../src/db", () => ({ db: {} }));

import {
  REMOVAL_COOLDOWN_DAYS,
  SURBL_REMOVAL_ADDRESS,
  fileSurblRemoval,
  removalDue,
  removalEmail,
  removalFormFields,
  type RemovalContact,
  type RemovalEmail,
} from "../../src/lib/blocklist-removal";
import { contactFromIdentity } from "../../src/lib/owner-identity-client";

const CONTACT: RemovalContact = {
  fullName: "Kevin Lourd",
  email: "kevin@distribute.you",
  phone: "+33 1 23 45 67 89",
  organization: "Blooming Generation SASU",
  address: "285 rue de l'Église, 46140 Douelle, France",
  website: "https://distribute.you",
};
const SAMPLE = { headers: "From: x\nTo: recipient@redacted.invalid", body: "<p>Hello</p>" };
const INPUT = {
  domain: "agileconsultco.com",
  contact: CONTACT,
  sample: SAMPLE,
  sendingServers: "Google Workspace outbound servers (smtp.gmail.com).",
  stoppedNewContactsOn: "2026-10-16",
};

describe("the request's words", () => {
  const fields = removalFormFields(INPUT);

  it("files in the company's name, ASCII only, our real practice and our published terms", () => {
    expect(fields).toMatchObject({
      name: "Kevin Lourd",
      email: "kevin@distribute.you",
      org: "Blooming Generation SASU",
      address1: "285 rue de l'Eglise, 46140 Douelle, France",
      fp: "agileconsultco.com",
      policy_link: "https://distribute.you/terms",
    });
    expect(fields.justify).toMatch(/verifier/);
    expect(fields.justify).toMatch(/2026-10-16/);
    expect(fields.policy).toMatch(/unsolicited bulk email in violation of any anti-spam law/);
    expect(Object.values(fields).join("")).toMatch(/^[\x09\x0A\x0D\x20-\x7E]*$/);
  });

  it("one plaintext email to SURBL's removal address, from the company mailbox, the owner in Bcc, every field of the form", () => {
    const email = removalEmail(fields);
    expect(email).toMatchObject({
      to: SURBL_REMOVAL_ADDRESS,
      from: '"Kevin Lourd" <kevin@distribute.you>',
      bcc: "kevin@distribute.you",
      subject: "Removal request: agileconsultco.com",
    });
    for (const label of ["Your name", "Your email address", "Organization name", "Street address", "Telephone number", "Web site domain name", "Sending IP addresses", "Full headers", "Full source", "Description", "Explanation", "published policy"]) {
      expect(email.text).toContain(label);
    }
    expect(email.text).toContain("+33 1 23 45 67 89");
    expect(email.text).not.toMatch(/[—–]/);
  });
});

describe("once a week", () => {
  const now = new Date("2026-10-23T06:00:00Z");
  it("never asked: due", () => expect(removalDue(null, now)).toBe(true));
  it("asked 6 days ago: not due", () => expect(removalDue(new Date("2026-10-17T07:00:00Z"), now)).toBe(false));
  it(`asked ${REMOVAL_COOLDOWN_DAYS} days ago: due again`, () =>
    expect(removalDue(new Date("2026-10-16T06:00:00Z"), now)).toBe(true));
});

describe("what the mail server answered", () => {
  it("accepted: sent, with the server's answer stored", async () => {
    const sent: RemovalEmail[] = [];
    const out = await fileSurblRemoval({ ...INPUT, send: async (e) => (sent.push(e), { response: "250 2.0.0 Ok: queued", code: 250 }) });
    expect(out).toMatchObject({ outcome: "sent", httpStatus: 250, responseExcerpt: "250 2.0.0 Ok: queued", error: null });
    expect(sent).toHaveLength(1);
    expect(out.request).toMatchObject({ fp: "agileconsultco.com", emailTo: SURBL_REMOVAL_ADDRESS });
  });

  it("a 5xx refusal: refused (it reached the list, so it starts the week)", async () => {
    const out = await fileSurblRemoval({
      ...INPUT,
      send: async () => {
        throw Object.assign(new Error("Message rejected"), { responseCode: 554, response: "554 5.7.1 rejected" });
      },
    });
    expect(out).toMatchObject({ outcome: "refused", httpStatus: 554, responseExcerpt: "554 5.7.1 rejected" });
  });

  it("a network or login failure: failed, never a throw", async () => {
    const out = await fileSurblRemoval({ ...INPUT, send: async () => { throw new Error("ECONNRESET"); } });
    expect(out).toMatchObject({ outcome: "failed", httpStatus: null, error: "ECONNRESET" });
  });
});

describe("the owner's record decides who files", () => {
  // Shape of kevinlourd.com #343: person = personal, company = business.
  const identity = {
    person: { fullName: "Kevin Jonathan Leonard Lourd", email: "kevin.lourd@gmail.com", phone: "+33700000001", postalAddress: "x" },
    company: { legalName: "Blooming Generation SASU", siren: "882102775", registeredOffice: "285 rue de l'Église, 46140 Douelle, France", vatNumber: "FR24882102775", brand: "distribute.you", website: "https://distribute.you", email: "kevin@distribute.you", phone: null as string | null },
    updatedAt: null,
  };
  it("an unset BUSINESS phone is named, never replaced by the personal one", () => {
    expect(() => contactFromIdentity(identity)).toThrow(/not set on kevinlourd.com \(Identity page\): company.phone/);
  });
  it("files as the company: business email and phone, the person's legal name", () => {
    const c = contactFromIdentity({ ...identity, company: { ...identity.company, phone: "+33600000002" } });
    expect(c).toMatchObject({
      fullName: "Kevin Jonathan Leonard Lourd",
      email: "kevin@distribute.you",
      phone: "+33600000002",
      organization: "Blooming Generation SASU",
      address: "285 rue de l'Église, 46140 Douelle, France",
    });
  });
});
