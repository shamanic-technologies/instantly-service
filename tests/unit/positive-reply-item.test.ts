import { describe, it, expect } from "vitest";
import {
  buildPositiveReplyItem,
  buildPriorMessages,
  cleanForSpeech,
  latestInboundText,
  MAX_PRIOR_MESSAGES,
  positiveReplyKey,
  stripSpokenSignature,
} from "../../src/lib/positive-reply-item";

/** A stored outbound body, as it looks AFTER htmlToText has run over it. */
const SENT_BODY = [
  "Hi Colleen,",
  "",
  "We help chiropractic clinics fill their calendar.",
  "--",
  "Amy Moore",
  "Distribute.you | Marketing Agency",
  "",
  " ",
  "Don't want to hear from me again? unsubscribe",
].join("\n");

function msg(
  direction: "outbound" | "inbound",
  bodyText: string,
  date = "2026-09-01T09:00:00.000Z",
) {
  return { direction, from: "a@b.com", to: "c@d.com", date, subject: "s", bodyText };
}

describe("cleanForSpeech — our own words are not read back at the rep", () => {
  it("cuts the signature and the opt-out footer under it", () => {
    const out = cleanForSpeech(SENT_BODY);
    expect(out).toBe(
      "Hi Colleen,\n\nWe help chiropractic clinics fill their calendar.",
    );
    expect(out).not.toContain("Amy Moore");
    expect(out).not.toContain("unsubscribe");
  });

  it("does NOT lean on stripAccountSignature: a <br>-wrapped delimiter leaves ONE newline before the marker", async () => {
    // `<br>--<br>` converts to a single "\n--\n". stripAccountSignature only
    // matches "\n\n--\n", so it would strip nothing here — this asserts the
    // plain-text cut is the one doing the work.
    const { stripAccountSignature } = await import("../../src/lib/send-lead");
    const single = "body\n--\nAmy Moore";
    expect(stripAccountSignature(single)).toBe(single);
    expect(stripSpokenSignature(single)).toBe("body");
  });

  it("cuts quoted history in both client spellings", () => {
    expect(cleanForSpeech("Yes, interested.\n\n> our pitch")).toBe("Yes, interested.");
    expect(
      cleanForSpeech("Sounds good.\n\nOn Mon, Amy Moore wrote:\nour pitch"),
    ).toBe("Sounds good.");
  });

  it("leaves a body carrying neither marker exactly as it is", () => {
    expect(cleanForSpeech("Send me pricing please.")).toBe("Send me pricing please.");
  });
});

describe("buildPriorMessages — the walk back through the thread", () => {
  it("returns what came BEFORE the reply, newest-first, cleaned", () => {
    const out = buildPriorMessages([
      msg("outbound", "first touch", "2026-09-01T09:00:00.000Z"),
      msg("outbound", SENT_BODY, "2026-09-04T09:00:00.000Z"),
      msg("inbound", "Yes, interested.", "2026-09-05T09:00:00.000Z"),
    ]);

    expect(out).toEqual([
      {
        direction: "outbound",
        text: "Hi Colleen,\n\nWe help chiropractic clinics fill their calendar.",
      },
      { direction: "outbound", text: "first touch" },
    ]);
  });

  it("EXCLUDES anything after the reply — that is what happened next, not context", () => {
    const out = buildPriorMessages([
      msg("outbound", "the pitch", "2026-09-01T09:00:00.000Z"),
      msg("inbound", "Yes, interested.", "2026-09-05T09:00:00.000Z"),
      msg("outbound", "our answer", "2026-09-06T09:00:00.000Z"),
    ]);
    expect(out.map((m) => m.text)).toEqual(["the pitch"]);
  });

  it("offers the whole thread when no reply could be read", () => {
    const out = buildPriorMessages([
      msg("outbound", "one", "2026-09-01T09:00:00.000Z"),
      msg("outbound", "two", "2026-09-02T09:00:00.000Z"),
    ]);
    expect(out.map((m) => m.text)).toEqual(["two", "one"]);
  });

  it("caps the walk so a rep is not put on a keypress treadmill", () => {
    const thread = Array.from({ length: MAX_PRIOR_MESSAGES + 4 }, (_, i) =>
      msg("outbound", `body ${i}`, `2026-09-0${(i % 9) + 1}T09:00:00.000Z`),
    );
    thread.push(msg("inbound", "Yes.", "2026-09-30T09:00:00.000Z"));
    expect(buildPriorMessages(thread)).toHaveLength(MAX_PRIOR_MESSAGES);
  });

  it("DROPS a body-less entry rather than speaking an empty pause", () => {
    const out = buildPriorMessages([
      msg("outbound", "(no body)", "2026-09-01T09:00:00.000Z"),
      msg("outbound", "   ", "2026-09-02T09:00:00.000Z"),
      msg("outbound", "real words", "2026-09-03T09:00:00.000Z"),
      msg("inbound", "Yes.", "2026-09-04T09:00:00.000Z"),
    ]);
    expect(out.map((m) => m.text)).toEqual(["real words"]);
  });

  it("keeps an inbound that is not the latest, labelled as theirs", () => {
    const out = buildPriorMessages([
      msg("outbound", "pitch", "2026-09-01T09:00:00.000Z"),
      msg("inbound", "who is this?", "2026-09-02T09:00:00.000Z"),
      msg("outbound", "context", "2026-09-03T09:00:00.000Z"),
      msg("inbound", "Yes, interested.", "2026-09-04T09:00:00.000Z"),
    ]);
    expect(out).toEqual([
      { direction: "outbound", text: "context" },
      { direction: "inbound", text: "who is this?" },
      { direction: "outbound", text: "pitch" },
    ]);
  });
});

describe("latestInboundText: the prospect's own last words, never ours", () => {
  it("reads the last inbound with words", () => {
    const thread = [msg("outbound", "our pitch"), msg("inbound", "Yes, very interested.", "2026-09-02T09:00:00.000Z")];
    expect(latestInboundText(thread)).toBe("Yes, very interested.");
    expect(latestInboundText([thread[0]])).toBeNull();
    expect(latestInboundText([])).toBeNull();
    expect(latestInboundText([msg("inbound", "(no body)")])).toBeNull();
  });
});

describe("buildPositiveReplyItem: the item the AI Instant Call workflow reads (LOCKED shape)", () => {
  it("carries the cleaned reply and the walk back, newest-first", () => {
    const item = buildPositiveReplyItem([
      msg("outbound", SENT_BODY, "2026-09-01T09:00:00.000Z"),
      msg("inbound", "Yes, interested.\n\nOn Mon, Amy Moore wrote:\nour pitch", "2026-09-05T09:00:00.000Z"),
    ]);
    expect(item).toEqual({
      replyText: "Yes, interested.",
      priorMessages: [{ direction: "outbound", text: "Hi Colleen,\n\nWe help chiropractic clinics fill their calendar." }],
    });
    expect(Object.keys(item).sort()).toEqual(["priorMessages", "replyText"]);
  });

  it("no words held: replyText null (the call says they are unavailable)", () => {
    expect(buildPositiveReplyItem([msg("outbound", "pitch")])).toEqual({
      replyText: null,
      priorMessages: [{ direction: "outbound", text: "pitch" }],
    });
  });
});

describe("positiveReplyKey: one occurrence per reply", () => {
  const thread = [msg("outbound", "pitch"), msg("inbound", "Yes.", "2026-09-02T09:00:00.000Z")];
  it("is stable for the same reply, whatever the address casing, and fits campaign-service's 200 chars", () => {
    const k = positiveReplyKey("inst-1", "p@x.com", thread);
    expect(positiveReplyKey("inst-1", " P@X.com", thread)).toBe(k);
    expect(k.length).toBeLessThanOrEqual(200);
  });
  it("differs for a new reply, another thread, or another person", () => {
    const k = positiveReplyKey("inst-1", "p@x.com", thread);
    expect(positiveReplyKey("inst-1", "p@x.com", [...thread, msg("inbound", "Friday?", "2026-09-04T09:00:00.000Z")])).not.toBe(k);
    expect(positiveReplyKey("inst-2", "p@x.com", thread)).not.toBe(k);
    expect(positiveReplyKey("inst-1", "q@x.com", thread)).not.toBe(k);
  });
  it("an unreadable thread keys on the thread, so it still records once", () => {
    expect(positiveReplyKey("inst-1", "p@x.com", [])).toBe(positiveReplyKey("inst-1", "p@x.com", []));
  });
});
