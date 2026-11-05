import { describe, it, expect } from "vitest";
import {
  followUpQueuedRefusal,
  pickFollowUpThread,
  threadSubjectFor,
  type FollowUpThreadCandidate,
} from "../../src/lib/follow-up-send";

describe("threadSubjectFor (NEVER-NO-FOLLOWUP-1014)", () => {
  const thread = {
    instantlyCampaignId: "ss-1",
    accountEmail: "Kim@Sender.com",
    subject: "Quick idea",
    reason: "our_last_email" as const,
  };

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

  it("refusal body is explicit and with its own code", () => {
    const r = followUpQueuedRefusal("p@x.com", { instantlyCampaignId: "ss-1", brandId: "b-1" });
    expect(r.code).toBe("brand_sequence_queued");
    expect(r.brandId).toBe("b-1");
  });
});

describe("pickFollowUpThread (RIGHT-MOMENT-OR-LATER-1014)", () => {
  // Thread A: we wrote Oct 1 from kim@, they replied in it Oct 12.
  // Thread B: another campaign of the brand wrote Oct 10 from lea@, no answer.
  const threadA: FollowUpThreadCandidate = {
    instantlyCampaignId: "ss-A",
    accountEmail: "kim@sender.com",
    subject: "Quick idea",
    lastSentAt: new Date("2026-10-01T09:00:00Z"),
    lastReplyAt: new Date("2026-10-12T15:00:00Z"),
    replyAccountEmail: "kim@sender.com",
  };
  const threadB: FollowUpThreadCandidate = {
    instantlyCampaignId: "ss-B",
    accountEmail: "lea@sender.com",
    subject: "Another idea",
    lastSentAt: new Date("2026-10-10T09:00:00Z"),
    lastReplyAt: null,
    replyAccountEmail: null,
  };

  it("continues the OLDER thread the person replied in, from the mailbox they wrote to", () => {
    expect(pickFollowUpThread([threadB, threadA])).toEqual({
      instantlyCampaignId: "ss-A",
      accountEmail: "kim@sender.com",
      subject: "Quick idea",
      reason: "their_last_reply",
    });
    expect(threadSubjectFor(pickFollowUpThread([threadA, threadB]), "kim@sender.com")).toBe("Re: Quick idea");
  });

  it("several replied threads: the one holding their LATEST reply wins", () => {
    const repliedB = { ...threadB, lastReplyAt: new Date("2026-10-13T08:00:00Z"), replyAccountEmail: "lea@sender.com" };
    expect(pickFollowUpThread([threadA, repliedB])?.instantlyCampaignId).toBe("ss-B");
  });

  it("the mailbox the reply reached wins over the sequence's own; unknown falls back to it", () => {
    expect(pickFollowUpThread([{ ...threadA, replyAccountEmail: "alias@sender.com" }])?.accountEmail).toBe("alias@sender.com");
    expect(pickFollowUpThread([{ ...threadA, replyAccountEmail: null }])?.accountEmail).toBe("kim@sender.com");
  });

  it("never replied: unchanged, our latest thread", () => {
    const olderUnanswered = { ...threadA, lastReplyAt: null, replyAccountEmail: null };
    expect(pickFollowUpThread([olderUnanswered, threadB])).toEqual({
      instantlyCampaignId: "ss-B",
      accountEmail: "lea@sender.com",
      subject: "Another idea",
      reason: "our_last_email",
    });
  });

  it("no candidate = no thread; a blank subject is no subject", () => {
    expect(pickFollowUpThread([])).toBeNull();
    expect(pickFollowUpThread([{ ...threadB, subject: "  " }])?.subject).toBeNull();
  });
});

describe("the thread they wrote in last carries THEIR reply's subject (REPLY-SAME-THREAD-1105)", () => {
  const base: FollowUpThreadCandidate = {
    instantlyCampaignId: "self:first",
    accountEmail: "kevin.lourd@hellodistribute.com",
    subject: "Launching Jcurve's first campaign",
    lastSentAt: new Date("2026-11-01T22:48:08Z"),
    lastReplyAt: new Date("2026-11-03T23:39:48Z"),
    replyAccountEmail: "kevin.lourd@hellodistribute.com",
    replySubject: "Re: JCurve Solutions <> Distribute.you",
  };

  it("answers under the subject the person wrote under, Re: not doubled", () => {
    const thread = pickFollowUpThread([base]);
    expect(thread?.subject).toBe("Re: JCurve Solutions <> Distribute.you");
    expect(threadSubjectFor(thread, "kevin.lourd@hellodistribute.com")).toBe(
      "Re: JCurve Solutions <> Distribute.you",
    );
  });

  it("CONTROL: a reply we never read (no subject) keeps our first email's subject", () => {
    expect(pickFollowUpThread([{ ...base, replySubject: null }])?.subject).toBe(
      "Launching Jcurve's first campaign",
    );
  });

  it("they never wrote back: our own subject, whatever is stored", () => {
    expect(
      pickFollowUpThread([{ ...base, lastReplyAt: null, replySubject: "Re: something" }])?.subject,
    ).toBe("Launching Jcurve's first campaign");
  });
});
