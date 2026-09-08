import { describe, it, expect, vi, beforeEach } from "vitest";

const mockPlatformComplete = vi.fn();
vi.mock("../../src/lib/chat-client", () => ({
  platformComplete: (...a: unknown[]) => mockPlatformComplete(...a),
}));

import {
  buildWarmupMessage,
  buildWarmupReply,
  parseWarmupMessage,
  topicFor,
  warmupSubjectRef,
  WARMUP_SUBJECT_TAG,
} from "../../src/lib/warmup/message";

beforeEach(() => {
  vi.clearAllMocks();
});

describe("parseWarmupMessage", () => {
  it("reads a plain JSON object", () => {
    expect(parseWarmupMessage('{"subject":"Doc","text":"Hi, ready?"}')).toEqual({
      subject: "Doc",
      text: "Hi, ready?",
    });
  });

  it("tolerates a fenced block", () => {
    expect(
      parseWarmupMessage('```json\n{"subject":"Doc","text":"Hi"}\n```'),
    ).toEqual({ subject: "Doc", text: "Hi" });
  });

  it("refuses an incomplete or unparseable answer", () => {
    expect(parseWarmupMessage('{"subject":"Doc"}')).toBeNull();
    expect(parseWarmupMessage('{"subject":"","text":"x"}')).toBeNull();
    expect(parseWarmupMessage("not json at all")).toBeNull();
  });
});

describe("topicFor", () => {
  it("is stable for the same edge on the same day", () => {
    // A re-run inside the day must reuse the topic, so the message is the same
    // message rather than a second one on a new subject.
    const a = topicFor("a@x.com", "b@y.com", "2026-09-07");
    expect(topicFor("a@x.com", "b@y.com", "2026-09-07")).toBe(a);
  });

  it("varies across edges and across days", () => {
    const topics = new Set([
      topicFor("a@x.com", "b@y.com", "2026-09-07"),
      topicFor("a@x.com", "c@z.com", "2026-09-07"),
      topicFor("d@w.com", "e@v.com", "2026-09-08"),
      topicFor("f@u.com", "g@t.com", "2026-09-09"),
      topicFor("h@s.com", "i@r.com", "2026-09-10"),
    ]);
    // An identical prompt pulls the model toward the same handful of openings,
    // which reintroduces the uniformity this module exists to avoid.
    expect(topics.size).toBeGreaterThan(1);
  });
});

describe("buildWarmupMessage", () => {
  it("asks the cheapest model, thinking disabled — this is a two-sentence note", () => {
    mockPlatformComplete.mockResolvedValue({
      content: '{"subject":"Quick one","text":"Hi Bob, ready?"}',
    });

    return buildWarmupMessage("a@x.com", "bob@y.com", "2026-09-07").then((msg) => {
      // The subject carries the filterable tag; the body is verbatim.
      expect(msg.subject).toMatch(/^Quick one \[WRM-[A-Z0-9]+\]$/);
      expect(msg.text).toBe("Hi Bob, ready?");
      expect(mockPlatformComplete).toHaveBeenCalledWith(
        expect.objectContaining({
          provider: "deepseek",
          model: "deepseek-flash",
          disableThinking: true,
        }),
      );
    });
  });

  // ⚠️ Nothing is being ASSERTED by a warmup body — its only job is to be
  // ordinary mail crossing a filter. So unlike the reply qualification (where a
  // missing answer must leave the sentiment unset rather than guess), a
  // chat-service outage must NOT stop the fleet warming.
  it("falls back to a written body when the model is unavailable", async () => {
    mockPlatformComplete.mockRejectedValue(new Error("chat-service 503"));

    const msg = await buildWarmupMessage("a@x.com", "bob@y.com", "2026-09-07");
    expect(msg.subject).not.toBe("");
    expect(msg.text).toContain("Bob");
  });

  it("falls back when the model answers something unusable", async () => {
    mockPlatformComplete.mockResolvedValue({ content: "I'm sorry, I can't." });

    const msg = await buildWarmupMessage("a@x.com", "bob@y.com", "2026-09-07");
    expect(msg.text).not.toBe("");
  });

  // An outage must not collapse the whole run into one repeated body — that is
  // the bulk signal the varied-per-email design exists to avoid.
  it("still varies across edges on the fallback path", async () => {
    mockPlatformComplete.mockRejectedValue(new Error("down"));

    const bodies = await Promise.all([
      buildWarmupMessage("a@x.com", "bob@y.com", "2026-09-07"),
      buildWarmupMessage("a@x.com", "carol@z.com", "2026-09-07"),
      buildWarmupMessage("d@w.com", "dave@v.com", "2026-09-08"),
    ]);

    expect(new Set(bodies.map((b) => b.text)).size).toBeGreaterThan(1);
  });
});

describe("buildWarmupReply", () => {
  it("threads under Re: without doubling an existing one", async () => {
    mockPlatformComplete.mockResolvedValue({ content: '{"subject":"x","text":"Sure."}' });

    expect((await buildWarmupReply("Quick one", "body")).subject).toBe("Re: Quick one");
    expect((await buildWarmupReply("Re: Quick one", "body")).subject).toBe("Re: Quick one");
  });

  it("answers anyway when the model is unavailable", async () => {
    mockPlatformComplete.mockRejectedValue(new Error("down"));

    const reply = await buildWarmupReply("Quick one", "body");
    expect(reply.subject).toBe("Re: Quick one");
    expect(reply.text).not.toBe("");
  });
});

// ─── The filterable tag ──────────────────────────────────────────────────────
//
// Warmup is internal traffic a human should never read, but some fleet mailboxes
// are fetched into a personal Gmail, so it lands in a real inbox. One filter has
// to catch all of it: `subject:WRM`.

describe("the warmup subject tag", () => {
  beforeEach(() => {
    mockPlatformComplete.mockResolvedValue({
      content: '{"subject":"Quick one","text":"Hi Bob, ready?"}',
    });
  });

  it("puts the constant token in every subject", async () => {
    const msg = await buildWarmupMessage("a@x.com", "bob@y.com", "2026-09-07");
    expect(msg.subject).toContain(WARMUP_SUBJECT_TAG);
  });

  it("tags the fallback body too — a filter with a hole is not a filter", async () => {
    mockPlatformComplete.mockRejectedValue(new Error("down"));
    const msg = await buildWarmupMessage("a@x.com", "bob@y.com", "2026-09-07");
    expect(msg.subject).toContain(WARMUP_SUBJECT_TAG);
  });

  // ⚠️ A constant substring is a fingerprint, so the subject as a WHOLE must
  // still vary. Bounded on purpose: three letters plus a per-message code.
  it("still varies the full subject across messages", async () => {
    const subjects = new Set(
      await Promise.all(
        [
          buildWarmupMessage("a@x.com", "bob@y.com", "2026-09-07"),
          buildWarmupMessage("a@x.com", "carol@z.com", "2026-09-07"),
          buildWarmupMessage("a@x.com", "bob@y.com", "2026-09-08"),
        ].map(async (p) => (await p).subject),
      ),
    );
    expect(subjects.size).toBe(3);
  });

  it("keeps the ref stable for one edge on one day", () => {
    expect(warmupSubjectRef("a@x.com", "b@y.com", "2026-09-07")).toBe(
      warmupSubjectRef("a@x.com", "b@y.com", "2026-09-07"),
    );
  });

  it("carries the tag into a reply, since Re: reuses the tagged subject", async () => {
    const reply = await buildWarmupReply("Quick one [WRM-AB12]", "body");
    expect(reply.subject).toBe("Re: Quick one [WRM-AB12]");
  });
});
