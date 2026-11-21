/**
 * OUTREACH-3WAYS-1121: a cold send copies exactly who its caller named, nobody
 * else (NO-DEFAULT-COPY-1116), on the real SMTP wire.
 */
import { describe, expect, it } from "vitest";
import { copyHeadersFromMetadata, sendCopyFromRequest } from "../../src/lib/send-copy";
import { buildMessage } from "../../src/lib/self-send/message";
import type { Account } from "../../src/lib/instantly-client";

const ACCOUNT = { email: "kevin@sender.test", first_name: "Kevin", last_name: "Lourd" } as Account;

function build(copy: { cc?: string; bcc?: string } = {}) {
  return buildMessage({
    account: ACCOUNT,
    leadEmail: "prospect@lead.test",
    subject: "Quick question",
    bodyHtml: "<p>Hi there</p>",
    step: 1,
    identity: { instantlyCampaignId: "self:x", leadEmail: "prospect@lead.test" },
    unsubscribeUrl: "https://opt.test/u/p/s",
    optOut: "tracker_link",
    ...copy,
  });
}

describe("sendCopyFromRequest", () => {
  it("no Cc and no Bcc = null (the row stays exactly as before)", () => {
    expect(sendCopyFromRequest("p@lead.test", undefined, undefined)).toBeNull();
    expect(sendCopyFromRequest("p@lead.test", [], [])).toBeNull();
    expect(sendCopyFromRequest("p@lead.test", [" "], undefined)).toBeNull();
  });

  it("keeps exactly the caller's addresses, trimmed and de-duplicated", () => {
    expect(sendCopyFromRequest("p@lead.test", [" kevin@distribute.you ", "Kevin@distribute.you"], undefined)).toEqual({
      cc: ["kevin@distribute.you"],
      bcc: [],
    });
  });

  it("never copies the To address on its own email", () => {
    expect(sendCopyFromRequest("P@lead.test", ["p@lead.test"], ["p@LEAD.test"])).toBeNull();
  });

  it("an address in both lists stays in Cc only", () => {
    expect(sendCopyFromRequest("p@lead.test", ["a@x.test"], ["a@x.test", "b@x.test"])).toEqual({
      cc: ["a@x.test"],
      bcc: ["b@x.test"],
    });
  });
});

describe("copyHeadersFromMetadata", () => {
  it("renders the stored lists as comma headers", () => {
    expect(copyHeadersFromMetadata({ cc: ["a@x.test", "b@x.test"], bcc: ["c@x.test"] })).toEqual({
      cc: "a@x.test,b@x.test",
      bcc: "c@x.test",
    });
  });

  it("nothing stored or unreadable = nobody copied", () => {
    expect(copyHeadersFromMetadata(null)).toEqual({ cc: "", bcc: "" });
    expect(copyHeadersFromMetadata({ cc: "a@x.test" })).toEqual({ cc: "", bcc: "" });
  });
});

describe("buildMessage carries the caller's copy", () => {
  it("with a Cc: the message has that Cc", () => {
    const message = build({ cc: "kevin@distribute.you" });
    expect(message.cc).toBe("kevin@distribute.you");
    expect(message.bcc).toBeUndefined();
  });

  it("without one: no Cc and no Bcc key at all", () => {
    const message = build();
    expect("cc" in message).toBe(false);
    expect("bcc" in message).toBe(false);
    expect("cc" in build({ cc: "", bcc: "" })).toBe(false);
  });
});
