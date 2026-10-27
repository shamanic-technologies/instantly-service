import { describe, it, expect } from "vitest";

import { decideReplyOwnership, type ReplyOwnershipFacts } from "../../src/lib/reply-ownership";
import type { SequenceCandidate } from "../../src/lib/self-send/gmail-threads";

// EMAIL-REPLY-ORPHAN-1027, the real case: pabitrahalder1979@gmail.com answered
// kevin@distribute.you on 2026-10-27 in a thread the owner opened from his Gmail.
// kevin@distribute.you IS in our account list (deactivated_by_user, absent), so
// crm-service skipped the reply as "the email path's", and we held no sequence
// to him: nobody acted.
const ORG = "f0420eb5-8f72-4f0a-a150-f473746df1e6";
const OTHER_ORG = "0d000000-0000-0000-0000-000000000001";

function candidate(extra: Partial<SequenceCandidate> = {}): SequenceCandidate {
  return {
    instantlyCampaignId: "self:seq-1",
    orgId: ORG,
    accountEmail: "allie@leansignalio.com",
    step: 1,
    lastSentAt: new Date("2026-10-10T10:00:00Z"),
    replied: false,
    ...extra,
  };
}

function facts(extra: Partial<ReplyOwnershipFacts> = {}): ReplyOwnershipFacts {
  return {
    ownDomains: new Set(["distribute.you", "leansignalio.com", "polaritycourse.com", "growthagency.store"]),
    readMailboxLogin: new Map([
      ["allie@leansignalio.com", "allie@leansignalio.com"],
      ["kevin.lourd@growthagency.store", "kevin.lourd@growthagency.store"],
    ]),
    mailboxLogin: new Map([
      ["allie@leansignalio.com", "allie@leansignalio.com"],
      ["kevin.lourd@growthagency.store", "kevin.lourd@growthagency.store"],
      ["kevin@distribute.you", "kevin@distribute.you"],
    ]),
    candidates: [],
    referenced: new Set(),
    ...extra,
  };
}

describe("decideReplyOwnership: does the email path act on this mail?", () => {
  it("pabitra: a staff mailbox in our account list but NO sequence to him = not ours (the caller reports it)", () => {
    expect(
      decideReplyOwnership(ORG, { from: "pabitrahalder1979@gmail.com", to: ["kevin@distribute.you"] }, facts()),
    ).toEqual({ owner: "none", reason: "no_sequence" });
  });

  it("lukas: a lead we emailed answering a staff mailbox = ours (the staff Gmail fold files it)", () => {
    expect(
      decideReplyOwnership(
        ORG,
        { from: "Lukas <lukas@stankamera.com>", to: ["kevin@distribute.you"] },
        facts({ candidates: [candidate()] }),
      ),
    ).toEqual({ owner: "email_path", reason: "lead_sequence" });
  });

  it("a staff recipient folds ANY org's sequence (staff mirror reads every lead)", () => {
    expect(
      decideReplyOwnership(ORG, { from: "a@b.com", to: ["kevin@distribute.you"] }, facts({ candidates: [candidate({ orgId: OTHER_ORG })] })),
    ).toEqual({ owner: "email_path", reason: "lead_sequence" });
  });

  it("sequences of two orgs and no reference: the fold declines, so it is NOT ours", () => {
    const two = [candidate(), candidate({ instantlyCampaignId: "self:seq-2", orgId: OTHER_ORG })];
    expect(
      decideReplyOwnership(ORG, { from: "a@b.com", to: ["kevin@distribute.you"] }, facts({ candidates: two })),
    ).toEqual({ owner: "none", reason: "ambiguous_orgs" });
    // A reference naming one of them settles it, as in the fold.
    expect(
      decideReplyOwnership(
        ORG,
        { from: "a@b.com", to: ["kevin@distribute.you"] },
        facts({ candidates: two, referenced: new Set(["self:seq-2"]) }),
      ),
    ).toEqual({ owner: "email_path", reason: "lead_sequence" });
  });

  it("a sending mailbox we READ, with a sequence sent from it = ours (the IMAP poll files it)", () => {
    expect(
      decideReplyOwnership(ORG, { from: "a@b.com", to: ["allie@leansignalio.com"] }, facts({ candidates: [candidate()] })),
    ).toEqual({ owner: "email_path", reason: "sending_mailbox_sequence" });
  });

  it("a sending mailbox we read, but our sequence left from ANOTHER mailbox and no other reader = not ours", () => {
    expect(
      decideReplyOwnership(
        ORG,
        { from: "a@b.com", to: ["kevin.lourd@growthagency.store"] },
        facts({ candidates: [candidate()] }),
      ),
    ).toEqual({ owner: "none", reason: "no_sequence" });
  });

  it("a sending mailbox we do NOT read (absent) is not ownership by itself", () => {
    const f = facts({ readMailboxLogin: new Map(), candidates: [candidate()] });
    expect(decideReplyOwnership(ORG, { from: "a@b.com", to: ["allie@leansignalio.com"] }, f)).toEqual({
      owner: "email_path",
      reason: "org_mailbox_sequence",
    });
    expect(
      decideReplyOwnership(ORG, { from: "a@b.com", to: ["allie@leansignalio.com"] }, facts({ readMailboxLogin: new Map(), candidates: [] })),
    ).toEqual({ owner: "none", reason: "no_sequence" });
  });

  it("the org's own connected Gmail folds only THIS org's sequences", () => {
    expect(
      decideReplyOwnership(ORG, { from: "a@b.com", to: ["ceo@client.com"] }, facts({ candidates: [candidate({ orgId: OTHER_ORG })] })),
    ).toEqual({ owner: "none", reason: "no_sequence" });
    expect(
      decideReplyOwnership(ORG, { from: "a@b.com", to: ["ceo@client.com"] }, facts({ candidates: [candidate()] })),
    ).toEqual({ owner: "email_path", reason: "org_mailbox_sequence" });
  });

  it("our own warmup mesh / staff writing is our own mail, never a person", () => {
    expect(
      decideReplyOwnership(ORG, { from: "maria@polaritycourse.com", to: ["kevin.lourd@growthagency.store"] }, facts()),
    ).toEqual({ owner: "email_path", reason: "our_own_mail" });
    expect(decideReplyOwnership(ORG, { from: "kevin.lourd@gmail.com", to: ["x@y.com"] }, facts())).toEqual({
      owner: "email_path",
      reason: "our_own_mail",
    });
  });
});
