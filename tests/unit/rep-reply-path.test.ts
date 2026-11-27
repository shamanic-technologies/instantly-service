import { describe, expect, it } from "vitest";

import { bracketMessageId, renderCelebration, repReplyPath } from "../../src/lib/celebrate-positive-reply";
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
  mailbox: '"Kevin Lourd" <kevin@leaddistribute.com>',
  inReplyTo: "<their-reply@bluewin.ch>",
  references: ["our-first@email.amazonses.com", "<their-reply@bluewin.ch>"],
  subject: "Re: Living Vital für Ihre Drogerie",
};

describe("repReplyPath", () => {
  it("Reply addresses the prospect (named) AND the thread's mailbox, threaded on their message", () => {
    const path = repReplyPath({ leadEmail: "drogerie.kadro@bluewin.ch", reply, thread });
    expect(path.replyTo).toEqual(['"Anna Kadro" <drogerie.kadro@bluewin.ch>', '"Kevin Lourd" <kevin@leaddistribute.com>']);
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
    expect(other.replyTo[0]).toBe("anna@other.ch");
    expect(repReplyPath({ leadEmail: "info@kadro.ch", reply: null, thread }).replyTo[0]).toBe("info@kadro.ch");
  });

  it("no mailbox on the sequence = the prospect alone, no invented threading, the celebration subject kept", () => {
    const path = repReplyPath({ leadEmail: "drogerie.kadro@bluewin.ch", reply, thread: null });
    expect(path).toEqual({ replyTo: ['"Anna Kadro" <drogerie.kadro@bluewin.ch>'] });
  });

  it("a thread we hold no message of theirs in: mailbox named, no threading headers", () => {
    const path = repReplyPath({
      leadEmail: "drogerie.kadro@bluewin.ch",
      reply,
      thread: { mailbox: "kevin@leaddistribute.com", inReplyTo: null, references: [], subject: "" },
    });
    expect(path.replyTo).toHaveLength(2);
    expect(path.inReplyTo).toBeUndefined();
    expect(path.references).toBeUndefined();
    expect(path.subject).toBeUndefined();
  });

  it("quotes the display name, so a comma in it cannot split the Reply-To list", () => {
    const path = repReplyPath({
      leadEmail: "a@b.ch",
      reply: { ...reply, from: '"Kadro, Anna" <a@b.ch>' },
      thread: null,
    });
    expect(path.replyTo).toEqual(['"Kadro, Anna" <a@b.ch>']);
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
