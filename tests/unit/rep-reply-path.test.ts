import { describe, expect, it } from "vitest";

import { bracketMessageId, capReferences, renderCelebration, repReplyPath, safeDisplayName } from "../../src/lib/celebrate-positive-reply";
import type { ThreadMessage } from "../../src/lib/forward-positive-reply";

// REP-REPLY-1127: the rep opens the celebration, hits Reply, and writes to the
// prospect inside the prospect's thread, our thread mailbox addressed too.
const reply: ThreadMessage = {
  direction: "inbound",
  from: '"Anna Kadro" <drogerie.kadro@bluewin.ch>',
  to: "kevin@leaddistribute.com",
  date: "2026-11-27T10:50:00.000Z",
  subject: "Re: Living Vital für Ihre Drogerie",
  bodyText: "Gerne, schicken Sie mir mehr Infos.",
  messageId: "<their-reply@bluewin.ch>",
};

const thread = {
  mailbox: { name: "Kevin Lourd", email: "kevin@leaddistribute.com" },
  inReplyTo: "<their-reply@bluewin.ch>",
  references: ["our-first@email.amazonses.com", "<their-reply@bluewin.ch>"],
  subject: "Re: Living Vital für Ihre Drogerie",
};

describe("repReplyPath", () => {
  it("Reply addresses the prospect (named) AND the thread's mailbox, threaded on their message", () => {
    const path = repReplyPath({ leadEmail: "drogerie.kadro@bluewin.ch", reply, thread });
    expect(path.replyTo).toEqual([
      { name: "Anna Kadro", email: "drogerie.kadro@bluewin.ch" },
      { name: "Kevin Lourd", email: "kevin@leaddistribute.com" },
    ]);
    expect(path.inReplyTo).toBe("<their-reply@bluewin.ch>");
    expect(path.references).toEqual(["<our-first@email.amazonses.com>", "<their-reply@bluewin.ch>"]);
    // Reply turns this into the prospect's own subject, never "Re: 🎉 ...".
    expect(path.subject).toBe("Re: Living Vital für Ihre Drogerie");
  });

  it("goes where the prospect wrote FROM (another address of theirs), else the address we emailed", () => {
    const other = repReplyPath({
      leadEmail: "info@kadro.ch",
      reply: { ...reply, from: "anna@other.ch" },
      thread,
    });
    expect(other.replyTo[0]).toEqual({ email: "anna@other.ch" });
    expect(repReplyPath({ leadEmail: "info@kadro.ch", reply: null, thread }).replyTo[0]).toEqual({ email: "info@kadro.ch" });
  });

  it("no mailbox on the sequence = the prospect alone, no invented threading, the celebration subject kept", () => {
    const path = repReplyPath({ leadEmail: "drogerie.kadro@bluewin.ch", reply, thread: null });
    expect(path).toEqual({ replyTo: [{ name: "Anna Kadro", email: "drogerie.kadro@bluewin.ch" }] });
  });

  it("a thread we hold no message of theirs in: mailbox named, no threading headers", () => {
    const path = repReplyPath({
      leadEmail: "drogerie.kadro@bluewin.ch",
      reply,
      thread: { mailbox: { name: "", email: "kevin@leaddistribute.com" }, inReplyTo: null, references: [], subject: "" },
    });
    expect(path.replyTo).toEqual([{ name: "Anna Kadro", email: "drogerie.kadro@bluewin.ch" }, { email: "kevin@leaddistribute.com" }]);
    expect(path.inReplyTo).toBeUndefined();
    expect(path.references).toBeUndefined();
    expect(path.subject).toBeUndefined();
  });

  it("a comma in a name stays inside ONE structured address (the producer quotes it)", () => {
    const path = repReplyPath({
      leadEmail: "a@b.ch",
      reply: { ...reply, from: '"Kadro, Anna" <a@b.ch>' },
      thread: null,
    });
    expect(path.replyTo).toEqual([{ name: "Kadro, Anna", email: "a@b.ch" }]);
  });
});

describe("safeDisplayName", () => {
  it("drops what the producer refuses (quote, backslash, line break), never refuses", () => {
    expect(safeDisplayName('Anna "AK" Kadro')).toBe("Anna AK Kadro");
    expect(safeDisplayName("A\\B\nC")).toBe("A B C");
    expect(safeDisplayName("  ")).toBeUndefined();
    expect(safeDisplayName(null)).toBeUndefined();
  });
});

describe("capReferences", () => {
  it("at most 50, keeping the first message and the latest ones, bracketed, no duplicate", () => {
    const ids = Array.from({ length: 60 }, (_, i) => `m${i}@x`);
    const out = capReferences([...ids, "m59@x"]);
    expect(out).toHaveLength(50);
    expect(out[0]).toBe("<m0@x>");
    expect(out[1]).toBe("<m11@x>");
    expect(out[49]).toBe("<m59@x>");
  });
});

describe("bracketMessageId", () => {
  it("one pair of angle brackets, whatever the stored spelling", () => {
    expect(bracketMessageId("x@y")).toBe("<x@y>");
    expect(bracketMessageId("<x@y>")).toBe("<x@y>");
    expect(bracketMessageId(" <<x@y>> ")).toBe("<x@y>");
  });
});

describe("the intro tells the rep Reply reaches the prospect", () => {
  const base = {
    leadEmail: "drogerie.kadro@bluewin.ch",
    brandName: "Living Vital",
    reply,
    history: { items: [], notes: [] },
    conversationUrl: "https://dashboard.distribute.you/v2",
  };
  it("said when Reply reaches them", () => {
    const c = renderCelebration({ ...base, replyReachesProspect: true });
    expect(c.text).toContain("To answer them yourself, hit Reply");
    expect(c.html).toContain("To answer them yourself, hit Reply");
  });
  it("not said otherwise", () => {
    expect(renderCelebration(base).text).not.toContain("hit Reply");
  });
});
