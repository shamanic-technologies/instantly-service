import { describe, expect, it } from "vitest";

import { bracketMessageId, capReferences, ownWordsOf, renderThreadText, repReplyPath, safeDisplayName, threadReplySubject } from "../../src/lib/celebrate-positive-reply";
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

describe("the thread email reads like a normal email thread (nothing of ours)", () => {
  const cold: ThreadMessage = {
    direction: "outbound",
    from: '"Kevin Lourd" <kevin@leaddistribute.com>',
    to: "drogerie.kadro@bluewin.ch",
    date: "2026-11-25T08:00:00.000Z",
    subject: "Living Vital für Ihre Drogerie",
    bodyText: "Guten Tag Frau Kadro,\n\nkurze Frage.\n\n--\nKevin Lourd",
  };
  const theirs: ThreadMessage = {
    ...reply,
    bodyText: "Gerne, schicken Sie mir mehr Infos.\n\nOn Tue, Nov 25, 2026, Kevin Lourd wrote:\n> Guten Tag Frau Kadro,",
  };

  it("newest first, each message quoted under its own 'On <date>, <from> wrote:', own words only", () => {
    const text = renderThreadText([cold, theirs]);
    const lines = text.split("\n");
    expect(lines[0]).toMatch(/^On .+, "Anna Kadro" <drogerie\.kadro@bluewin\.ch> wrote:$/);
    expect(lines[1]).toBe("> Gerne, schicken Sie mir mehr Infos.");
    // their client's quote of our email is not repeated: our email is quoted once, below
    expect(text.match(/Guten Tag Frau Kadro/g)).toHaveLength(1);
    expect(text).toContain('"Kevin Lourd" <kevin@leaddistribute.com> wrote:\n> Guten Tag Frau Kadro,\n>\n> kurze Frage.');
    // nothing of ours: no brand line, no link, no button, no HTML
    for (const banned of ["distribute.you", "http", "<p", "<div", "Reply"]) expect(text).not.toContain(banned);
  });

  it("subject = Re: + the conversation's subject, any Re:/AW: prefixes collapsed", () => {
    expect(threadReplySubject([cold, { ...theirs, subject: "AW: AW: Living Vital für Ihre Drogerie" }])).toBe(
      "Re: Living Vital für Ihre Drogerie",
    );
  });
});

// Prod 2026-11-27 (Living Vital): Outlook/bluewin replies quote our email under a
// German header block; without a cut, our emails showed twice in the thread.
describe("ownWordsOf cuts an Outlook header block, any language", () => {
  const german = [
    "Lieber Herr Lourd",
    "",
    "Verkaufen Sie ausschliesslich Flohsamenschalen?",
    "",
    "Sonnige Grüsse",
    "",
    "Marianne Wenger",
    "",
    "Von: Kevin Lourd",
    "Gesendet: Freitag, 27. November 2026 11:58",
    "An: drogerie.kadro@bluewin.ch",
    "Betreff: Re: AW: AW: Flohsamenschalen",
    "",
    "Liebe Frau Wenger",
  ].join("\n");

  it("German Von:/Gesendet:", () => {
    const own = ownWordsOf(german);
    expect(own.endsWith("Marianne Wenger")).toBe(true);
    expect(own).not.toContain("Von:");
  });
  it("English From:/Sent: and French De:/Envoyé:", () => {
    expect(ownWordsOf("Yes please.\n\nFrom: Kevin\nSent: Friday\nTo: x\n\nold")).toBe("Yes please.");
    expect(ownWordsOf("Oui.\n\nDe : Kevin\nEnvoyé : vendredi\nÀ : x\n\nold")).toBe("Oui.");
  });
  it("a lone 'Von: ...' sentence is NOT a header block", () => {
    expect(ownWordsOf("Grüsse\nVon: unserem Team")).toBe("Grüsse\nVon: unserem Team");
  });

  it("our sends show 'Name <address>', each email quoted once", () => {
    const ours: ThreadMessage = {
      direction: "outbound",
      from: "lourd@growthagency.media",
      to: "drogerie.kadro@bluewin.ch",
      date: "2026-11-27T10:58:00.000Z",
      subject: "Re: AW: AW: Flohsamenschalen",
      bodyText: "Liebe Frau Wenger\n\nBesten Dank.",
    };
    const theirs: ThreadMessage = { ...reply, date: "2026-11-27T11:06:00.000Z", bodyText: german };
    const text = renderThreadText([ours, theirs], { name: "Kevin Lourd", email: "lourd@growthagency.media" });
    expect(text).toContain("Kevin Lourd <lourd@growthagency.media> wrote:");
    expect(text.match(/Liebe Frau Wenger/g)).toHaveLength(1);
  });
});
