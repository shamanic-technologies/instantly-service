import { describe, it, expect, vi, beforeEach } from "vitest";

// MIGRATE-1105 S2b: the orphan-reply judgment through the shared service switch (task `orphan-judgment`). No switch =
// our code, its Jev call recorded in the script's words; shadow = ours used, the script asked and compared on the
// decision (not the probabilities of a second call); switched = the script alone, ours never paid for.
const inserted: Array<Record<string, unknown>> = [];
vi.mock("../../src/db", () => ({
  db: {
    insert: () => ({ values: async (v: Record<string, unknown>) => void inserted.push(v) }),
    delete: () => ({ where: async () => undefined }),
  },
}));
vi.mock("../../src/lib/runs-client", () => ({
  createPlatformRun: vi.fn(async (task: string) => ({ id: `run-${task}` })),
  closePlatformRun: vi.fn(async () => undefined),
}));
vi.mock("../../src/lib/reply-judging-run", () => ({ replyJudgingRunForSequence: vi.fn(async () => "judging-run") }));

import { resetServiceScriptSwitchMemo, serviceScriptSwitch, type ServiceScriptSwitch } from "../../src/lib/service-script";
import { decideOrphanReply, orphanDecisionToAct, orphanMaterial, ORPHAN_JUDGMENT_TASK, type Judge, type OrphanDecisionInput } from "../../src/lib/self-send/orphan-decision";
import type { OrphanReplyLead } from "../../src/lib/self-send/orphan-reply";

const drain = async () => {
  for (let i = 0; i < 20; i++) await new Promise((r) => setTimeout(r, 0));
};

const lead: OrphanReplyLead = {
  instantlyCampaignId: "seq-1",
  leadEmail: "stacy.blecher@twinhealth.com",
  firstName: "Stacy",
  lastName: "Blecher",
  companyName: "Twin Health",
  subject: "Metabolic programs",
  excerpt: "Hi Stacy",
  lastStep: 2,
  firstSentAt: new Date("2026-09-20T10:00:00Z"),
  lastSentAt: new Date("2026-09-23T10:00:00Z"),
  orgId: "org-doc",
};
const input = (text: string): OrphanDecisionInput => ({
  message: {
    fromAddress: "Dr Blecher <drblecher@chsmetabolismdoc.com>",
    toAddress: "kevin@mail-dy.com",
    subject: "Re: your note",
    headers: { from: "Dr Blecher <drblecher@chsmetabolismdoc.com>" },
    text,
    receivedAt: new Date("2026-09-24T12:00:00Z"),
    recipients: ["kevin@mail-dy.com"],
  },
  ownMailDomains: new Set(["mail-dy.com"]),
  leads: [lead],
});
const answer = (choice: string, p: number) => ({ model: "jev-1", answers: { orphan_reply: { type: "choice" as const, choice, confidence: 0.8, probabilities: { lead_1: p, none: 1 - p } } }, usage: { inputTokens: 700, outputTokens: 0 } });
const judged: Array<[unknown, string, string]> = [];
const judge = (choice: string, p: number): Judge => async (body, seq, org) => {
  judged.push([body, seq, org]);
  return answer(choice, p);
};
const TEXT = "Stacy here, writing from my practice address. Happy to talk metabolic programs.";
const none = async (): Promise<ServiceScriptSwitch> => ({ switched: false, shadow: false, dynasty: null });
const shadow = async (): Promise<ServiceScriptSwitch> => ({ switched: false, shadow: true, dynasty: "dy_orphan_reply" });
const switched = async (): Promise<ServiceScriptSwitch> => ({ switched: true, shadow: false, dynasty: "dy_orphan_reply" });

beforeEach(() => {
  inserted.length = 0;
  judged.length = 0;
  resetServiceScriptSwitchMemo();
  vi.unstubAllGlobals();
});

describe("our orphan-reply decision is the sweep's code", () => {
  it("excludes on the body's warmup tag and asks nothing", async () => {
    const d = await decideOrphanReply(input("hi WNT6JJB"), judge("lead_1", 0.9));
    expect(d).toEqual({ words: { outcome: "excluded", reason: "instantly_warmup" }, call: null });
    expect(judged).toEqual([]);
  });

  it("judges on the likeliest lead's reply-judging sequence and records the call in the script's words", async () => {
    const d = await decideOrphanReply(input(TEXT), judge("lead_1", 0.9));
    expect(judged[0]![1]).toBe("seq-1");
    expect(judged[0]![2]).toBe("org-doc");
    expect(d.words).toMatchObject({ outcome: "matched", choice: "lead_1", probability: 0.9, inputTokens: 700, instantlyCampaignId: "seq-1", leadEmail: lead.leadEmail });
    expect(d.call).toEqual({ name: "jev.choice", args: judged[0]![0], answer: { ok: true, data: { answers: answer("lead_1", 0.9).answers, model: "jev-1", usage: { inputTokens: 700, outputTokens: 0 } } } });
    expect((await decideOrphanReply(input(TEXT), judge("lead_1", 0.6))).words.outcome).toBe("low_confidence");
  });

  it("states the material with ISO dates and our staff lists", () => {
    const m = orphanMaterial(input(TEXT)) as { message: { receivedAt: string }; leads: Array<{ firstSentAt: string }>; staffAddresses: string[] };
    expect(m.message.receivedAt).toBe("2026-09-24T12:00:00.000Z");
    expect(m.leads[0]!.firstSentAt).toBe("2026-09-20T10:00:00.000Z");
    expect(m.staffAddresses).toContain("kevin.lourd@gmail.com");
  });
});

describe("the orphan-reply decision through the service switch", () => {
  it("no switch: ours, recorded under its task with its call", async () => {
    const d = await orphanDecisionToAct(input(TEXT), { switchState: none, judge: judge("none", 0.1) });
    expect(d.words.outcome).toBe("none");
    await drain();
    expect(inserted).toHaveLength(1);
    expect(inserted[0]).toMatchObject({ outcome: "reply_received", task: ORPHAN_JUDGMENT_TASK, path: "service", runId: "run-orphan-judgment", same: null });
    expect((inserted[0]!.calls as unknown[])).toHaveLength(1);
  });

  it("shadow: ours used, the script asked beside it, same when the decision agrees even if the figures differ", async () => {
    const run = vi.fn(async () => ({ runId: "s1", scriptSlug: "reply_received/dy_orphan_reply/v1", words: { outcome: "none", choice: "none", probability: 0.88, candidates: [{ instantlyCampaignId: "seq-1", leadEmail: lead.leadEmail, score: 7 }] } }));
    const d = await orphanDecisionToAct(input(TEXT), { switchState: shadow, run, judge: judge("none", 0.1) });
    expect(d.words.outcome).toBe("none");
    await drain();
    expect(run).toHaveBeenCalledTimes(1);
    expect(inserted[0]).toMatchObject({ path: "shadow", task: ORPHAN_JUDGMENT_TASK, same: true });
  });

  it("switched: the script alone, our paid judgment never made", async () => {
    const run = vi.fn(async () => ({ runId: "s1", scriptSlug: "x", words: { outcome: "matched", instantlyCampaignId: "seq-1", leadEmail: lead.leadEmail, probability: 0.9, choice: "lead_1", candidates: [] } }));
    const d = await orphanDecisionToAct(input(TEXT), { switchState: switched, run, judge: judge("none", 0.1) });
    expect(judged).toEqual([]);
    expect(d.words).toMatchObject({ outcome: "matched", instantlyCampaignId: "seq-1" });
    expect(inserted[0]).toMatchObject({ path: "script", same: null, ours: {} });
    await expect(orphanDecisionToAct(input(TEXT), { switchState: switched, run: async () => ({ runId: "s", scriptSlug: "x", words: { outcome: "matched" } }) })).rejects.toThrow(/matched no lead/);
  });

  it("asks workflow-service for the switch of its own task", async () => {
    const urls: string[] = [];
    vi.stubEnv("WORKFLOW_SERVICE_URL", "http://wf");
    vi.stubEnv("WORKFLOW_SERVICE_API_KEY", "k");
    vi.stubGlobal("fetch", vi.fn(async (u: string) => {
      urls.push(u);
      return new Response(JSON.stringify({ switched: false, shadow: true, dynasty: "dy_orphan_reply" }));
    }));
    expect(await serviceScriptSwitch("reply_received", ORPHAN_JUDGMENT_TASK)).toEqual({ switched: false, shadow: true, dynasty: "dy_orphan_reply" });
    expect(await serviceScriptSwitch("reply_received", "inbound-decision")).toMatchObject({ shadow: true });
    expect(urls).toEqual([
      "http://wf/internal/scripts/switches/service?outcome=reply_received&callerService=instantly&callerTask=orphan-judgment",
      "http://wf/internal/scripts/switches/service?outcome=reply_received&callerService=instantly&callerTask=inbound-decision",
    ]);
  });
});
