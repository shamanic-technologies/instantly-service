import { describe, it, expect } from "vitest";
import { followUpQueuedRefusal, threadSubjectFor } from "../../src/lib/follow-up-send";

describe("threadSubjectFor (NEVER-NO-FOLLOWUP-1014)", () => {
  const thread = { instantlyCampaignId: "ss-1", accountEmail: "Kim@Sender.com", subject: "Quick idea" };

  it("continues the thread from the same mailbox (case-insensitive) with Re: once", () => {
    expect(threadSubjectFor(thread, "kim@sender.com")).toBe("Re: Quick idea");
    expect(threadSubjectFor({ ...thread, subject: "Re: Quick idea" }, "kim@sender.com")).toBe("Re: Quick idea");
  });

  it("a new thread from another mailbox, without a known subject, or with no thread", () => {
    expect(threadSubjectFor(thread, "other@sender.com")).toBeNull();
    expect(threadSubjectFor({ ...thread, subject: null }, "kim@sender.com")).toBeNull();
    expect(threadSubjectFor({ ...thread, accountEmail: null }, "kim@sender.com")).toBeNull();
    expect(threadSubjectFor(null, "kim@sender.com")).toBeNull();
  });

  it("refusal body is explicit and distinct from recent_brand_contact", () => {
    const r = followUpQueuedRefusal("p@x.com", { instantlyCampaignId: "ss-1", brandId: "b-1" });
    expect(r.code).toBe("brand_sequence_queued");
    expect(r.brandId).toBe("b-1");
  });
});
