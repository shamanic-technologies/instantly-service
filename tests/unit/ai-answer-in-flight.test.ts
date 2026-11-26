import { describe, it, expect, vi, beforeEach } from "vitest";

const mockExecute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => mockExecute(...a) } }));

import {
  answerWords,
  finishAiAnswer,
  findAiAnswerSince,
  startAiAnswer,
  waitForAiAnswer,
} from "../../src/lib/ai-answer-in-flight";

const sqlOf = (call: unknown[]) => JSON.stringify(call[0]);

beforeEach(() => {
  mockExecute.mockReset();
});

describe("one path per reply (ESCALATION-AND-AI-REPLY-1126)", () => {
  it("an automated answer marks the thread ONLY while no escalation is claimed, in one UPDATE", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [{ mark: "2026-11-09 15:30:11.200001" }] });
    expect(await startAiAnswer("self:1")).toBe("2026-11-09 15:30:11.200001");
    const q = sqlOf(mockExecute.mock.calls[0]);
    expect(q).toContain("UPDATE instantly_campaigns");
    expect(q).toContain("escalated_at IS NULL");
    expect(q).toContain("ai_answer_started_at");
  });

  it("a claimed escalation = no mark = the caller refuses (null)", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [] });
    expect(await startAiAnswer("self:1")).toBeNull();
  });

  it("clears only its own mark and never throws", async () => {
    mockExecute.mockRejectedValueOnce(new Error("db down"));
    await expect(finishAiAnswer("self:1", "m")).resolves.toBeUndefined();
    mockExecute.mockResolvedValueOnce({ rows: [] });
    await finishAiAnswer("self:1", "2026-11-09 15:30:11.200001");
    expect(sqlOf(mockExecute.mock.calls[1])).toContain("ai_answer_started_at::text");
  });

  it("waits while an answer is in flight, returns once it is not", async () => {
    mockExecute
      .mockResolvedValueOnce({ rows: [{ inFlight: true }] })
      .mockResolvedValueOnce({ rows: [{ inFlight: true }] })
      .mockResolvedValueOnce({ rows: [{ inFlight: false }] });
    const sleep = vi.fn(async () => {});
    await waitForAiAnswer("self:1", { sleep });
    expect(sleep).toHaveBeenCalledTimes(2);
    // A stale mark (a process that died mid-answer) is not waited on forever.
    expect(sqlOf(mockExecute.mock.calls[0])).toContain("make_interval");
  });

  it("only an AUTOMATED answer counts as the AI's, read from what really left", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [{ sentAt: "2026-11-09T15:30:13.918Z", bodyHtml: "<p>Hoi Markus</p><p>Nein.</p><p>--</p><p>Kevin Lourd</p>" }],
    });
    const answer = await findAiAnswerSince("self:1", new Date("2026-11-09T15:28:51.000Z"));
    expect(answer).toEqual({ sentAt: "2026-11-09T15:30:13.918Z", text: expect.stringContaining("Nein.") });
    expect(answer!.text).not.toContain("Kevin Lourd");
    const q = sqlOf(mockExecute.mock.calls[0]);
    expect(q).toContain("'manual_reply'");
    expect(q).toContain("'automation'");

    mockExecute.mockResolvedValueOnce({ rows: [] });
    expect(await findAiAnswerSince("self:1", new Date())).toBeNull();
  });

  it("answerWords cuts the signature and the quoted conversation (invisible-wrapper signature too)", () => {
    expect(
      answerWords('<p>Yes, it ships Monday.</p><div style="margin:0;padding:0;border:0"><p>Kevin</p></div><blockquote>old</blockquote>'),
    ).toBe("Yes, it ships Monday.");
  });
});
