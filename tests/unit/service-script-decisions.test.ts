import { describe, it, expect, vi, beforeEach } from "vitest";

// MIGRATE-1105: message_queued and reply_received asked of a stored script through the shared service switch.
// No switch row = our decision, byte-identical, recorded (path service), nothing asked. Shadow = ours used, the
// script asked beside it and compared. Switched = the script's used. Rolled back = ours again within a minute.
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

import {
  decideThroughScript,
  resetServiceScriptSwitchMemo,
  sameWords,
  serviceScriptSwitch,
  type ServiceScriptSwitch,
} from "../../src/lib/service-script";
import {
  decideQueuedSteps,
  queuedStepsFromWords,
  queuedStepsMaterial,
  queuedStepsToStore,
  queuedStepsWords,
  QUEUE_DECISION_TASK,
  type QueuedStepsInput,
} from "../../src/lib/queued-steps-script";
import { stepRowsFromSendPayload } from "../../src/lib/self-send/sequence-steps";
import { threadSubjectFor } from "../../src/lib/follow-up-send";
import {
  decideInbound,
  inboundDecisionsToAct,
  inboundFromWords,
  inboundMaterial,
  inboundWords,
  needsOurSideLookup,
  INBOUND_DECISION_TASK,
  type InboundDecisionInput,
} from "../../src/lib/self-send/inbound-decision";

const drain = async () => {
  for (let i = 0; i < 20; i++) await new Promise((r) => setTimeout(r, 0));
};
const none = async (): Promise<ServiceScriptSwitch> => ({ switched: false, shadow: false, dynasty: null });
const shadow = async (): Promise<ServiceScriptSwitch> => ({ switched: false, shadow: true, dynasty: "dy_x" });
const switched = async (): Promise<ServiceScriptSwitch> => ({ switched: true, shadow: false, dynasty: "dy_x" });

beforeEach(() => {
  inserted.length = 0;
  resetServiceScriptSwitchMemo();
  vi.unstubAllGlobals();
});

// ─── message_queued ────────────────────────────────────────────────────────────────────────────────────────────

const followUp: QueuedStepsInput = {
  callerSubject: "Quick question",
  sequence: [
    { step: 1, bodyHtml: "<p>a</p>", daysSinceLastStep: 0 },
    { step: 2, bodyHtml: "<p>b</p>", daysSinceLastStep: 3 },
  ],
  followUpThread: { instantlyCampaignId: "self:thread", accountEmail: "Kevin@m.co", subject: "Their words", reason: "their_last_reply" },
  sendingAccountEmail: "kevin@m.co",
  transport: "smtp",
};

describe("message_queued: our decision is the route's old code, byte for byte", () => {
  it("continues the thread from its own mailbox on our sender", () => {
    const d = decideQueuedSteps(followUp);
    const threadSubject = threadSubjectFor(followUp.followUpThread, followUp.sendingAccountEmail);
    expect(threadSubject).toBe("Re: Their words");
    expect(d).toEqual({ followUpOf: "self:thread", threadSubject, steps: stepRowsFromSendPayload(threadSubject!, followUp.sequence) });
  });

  it("the caller's subject when the mailbox differs, or on Instantly, or with no thread", () => {
    for (const input of [
      { ...followUp, sendingAccountEmail: "other@m.co" },
      { ...followUp, transport: "instantly" },
      { ...followUp, followUpThread: null },
    ]) {
      const d = decideQueuedSteps(input);
      expect(d.followUpOf).toBeNull();
      expect(d.threadSubject).toBeNull();
      expect(d.steps).toEqual(stepRowsFromSendPayload("Quick question", input.sequence));
    }
  });

  it("words round-trip to the same decision", () => {
    const d = decideQueuedSteps(followUp);
    expect(queuedStepsFromWords(JSON.parse(JSON.stringify(queuedStepsWords(d))))).toEqual(d);
  });
});

describe("message_queued through the switch", () => {
  it("no switch row: ours, nothing asked, recorded as service on its own run", async () => {
    const run = vi.fn();
    const ours = decideQueuedSteps(followUp);
    expect(await queuedStepsToStore(followUp, { switchState: none, run })).toEqual(ours);
    await drain();
    expect(run).not.toHaveBeenCalled();
    expect(inserted).toHaveLength(1);
    expect(inserted[0]).toMatchObject({ outcome: "message_queued", path: "service", runId: `run-${QUEUE_DECISION_TASK}`, script: null, same: null });
    expect(inserted[0]!.material).toEqual(queuedStepsMaterial(followUp));
    expect(inserted[0]!.ours).toEqual(queuedStepsWords(ours));
  });

  it("no switch row at all (workflow-service unreachable, never read): our code", async () => {
    delete process.env.WORKFLOW_SERVICE_URL;
    expect(await serviceScriptSwitch("message_queued")).toEqual({ switched: false, shadow: false, dynasty: null });
  });

  it("shadow: ours used at once, the script asked beside it and compared", async () => {
    const ours = decideQueuedSteps(followUp);
    const run = vi.fn(async () => ({ runId: "s1", scriptSlug: "message_queued/dy_x/v1", words: queuedStepsWords(ours) }));
    expect(await queuedStepsToStore(followUp, { switchState: shadow, run })).toEqual(ours);
    await drain();
    expect(run).toHaveBeenCalledWith(expect.objectContaining({ outcome: "message_queued", dynasty: "dy_x", consumes: "message_written", parentRunId: `run-${QUEUE_DECISION_TASK}` }));
    expect(inserted[0]).toMatchObject({ path: "shadow", same: true, scriptRunId: "s1" });
  });

  it("shadow, the script differs: ours still used, recorded different", async () => {
    const ours = decideQueuedSteps(followUp);
    const run = vi.fn(async () => ({ runId: "s1", scriptSlug: null, words: { ...queuedStepsWords(ours), threadSubject: null } }));
    expect(await queuedStepsToStore(followUp, { switchState: shadow, run })).toEqual(ours);
    await drain();
    expect(inserted[0]).toMatchObject({ path: "shadow", same: false });
  });

  it("switched: the script's decision is used", async () => {
    const ours = decideQueuedSteps(followUp);
    const theirs = { ...queuedStepsWords(ours), steps: [{ step: 1, subject: "S", bodyHtml: "<p>x</p>", delayDays: null }] };
    const run = vi.fn(async () => ({ runId: "s1", scriptSlug: null, words: theirs }));
    const used = await queuedStepsToStore(followUp, { switchState: switched, run });
    expect(used.steps).toEqual([{ step: 1, subject: "S", bodyHtml: "<p>x</p>", delayDays: null }]);
    expect(inserted[0]).toMatchObject({ path: "script", same: false });
  });

  it("switched, the script fails: the caller fails (never a silent fall back), recorded with the error", async () => {
    const run = vi.fn(async () => {
      throw new Error("boom");
    });
    await expect(queuedStepsToStore(followUp, { switchState: switched, run })).rejects.toThrow("boom");
    expect(inserted[0]).toMatchObject({ path: "script", error: "boom" });
  });

  it("rolled back: the switch read answers not switched within a minute, ours again", async () => {
    process.env.WORKFLOW_SERVICE_URL = "http://wf";
    process.env.WORKFLOW_SERVICE_API_KEY = "k";
    let body = { switched: true, dynasty: "dy_x" } as Record<string, unknown>;
    vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify(body), { status: 200 })));
    const t0 = 1_000_000;
    expect((await serviceScriptSwitch("message_queued", t0)).switched).toBe(true);
    body = { switched: false, shadow: false, dynasty: null };
    expect((await serviceScriptSwitch("message_queued", t0 + 30_000)).switched).toBe(true);
    expect((await serviceScriptSwitch("message_queued", t0 + 61_000)).switched).toBe(false);
    // Per outcome: reply_received is read on its own.
    body = { shadow: true, dynasty: "dy_r" };
    expect(await serviceScriptSwitch("reply_received", t0 + 61_000)).toEqual({ switched: false, shadow: true, dynasty: "dy_r" });
  });
});

// ─── reply_received ────────────────────────────────────────────────────────────────────────────────────────────

const known = new Map([
  ["<s1@m.co>", { instantlyCampaignId: "self:a", leadEmail: "lead@prospect.com", step: 1, orgId: "o1" }],
  ["<s2@m.co>", { instantlyCampaignId: "self:a", leadEmail: "lead@prospect.com", step: 2, orgId: "o1" }],
  ["<t1@m.co>", { instantlyCampaignId: "self:b", leadEmail: "other@else.com", step: 1, orgId: "o1" }],
  ["<far@m.co>", { instantlyCampaignId: "self:c", leadEmail: "nobody@far.com", step: 1, orgId: "o1" }],
]);
const batch = (messages: InboundDecisionInput["messages"]): InboundDecisionInput => ({ accountEmail: "kevin@m.co", knownSends: known, ownMailDomains: new Set(["m.co"]), messages });
const rawReply = { messageId: "<r1@prospect.com>", headers: { from: "Lead <lead@prospect.com>", "in-reply-to": "<s2@m.co>", references: "<s1@m.co> <s2@m.co>" }, body: "", from: "Lead <lead@prospect.com>", subject: "Re: hi" };
// The poller asks `senderIsOurSide` for every correlated reply not from staff (it answers the lead itself at once).
const reply = { ...rawReply, senderOurSide: false };

describe("reply_received: our decision", () => {
  it("a reply from the lead matches the latest step; the our-side fact is asked like before", () => {
    expect(needsOurSideLookup(rawReply, known)?.step).toBe(2);
    const [d] = decideInbound(batch([reply]));
    expect(d).toEqual({ messageId: "<r1@prospect.com>", kind: "reply", referencedMessageIds: ["<s2@m.co>", "<s1@m.co>", "<s2@m.co>"], correlation: { outcome: "matched", instantlyCampaignId: "self:a", leadEmail: "lead@prospect.com", step: 2 }, mailtoUnsubscribe: [], orphanCandidate: false });
  });

  it("a reply from someone else asks the our-side fact, and it refiles to staff_reply", () => {
    const colleague = { ...rawReply, messageId: "<r2@x>", from: "Rep <rep@brand.com>", headers: { ...reply.headers, from: "Rep <rep@brand.com>" } };
    expect(needsOurSideLookup(colleague, known)?.instantlyCampaignId).toBe("self:a");
    expect(() => decideInbound(batch([colleague]))).toThrow("no our-side fact");
    expect(decideInbound(batch([{ ...colleague, senderOurSide: true }]))[0]!.kind).toBe("staff_reply");
    expect(decideInbound(batch([{ ...colleague, senderOurSide: false }]))[0]!.kind).toBe("reply");
  });

  it("staff refile needs no lookup", () => {
    const staff = { ...rawReply, messageId: "<r3@x>", from: "Kevin <kevin@distribute.you>", headers: { ...reply.headers, from: "kevin@distribute.you" } };
    expect(needsOurSideLookup(staff, known)).toBeNull();
    expect(decideInbound(batch([staff]))[0]!.kind).toBe("staff_reply");
  });

  it("two sequences referenced = ambiguous", () => {
    const amb = { ...reply, messageId: "<r4@x>", headers: { ...reply.headers, references: "<s1@m.co> <t1@m.co>" } };
    expect(decideInbound(batch([amb]))[0]!.correlation).toEqual({ outcome: "ambiguous", campaignIds: ["self:a", "self:b"] });
  });

  it("a mailto unsubscribe from the address we wrote to", () => {
    const unsub = { messageId: "<u1@x>", headers: { from: "other@else.com" }, body: "", from: "other@else.com", subject: "unsubscribe" };
    const [d] = decideInbound(batch([unsub]));
    expect(d!.mailtoUnsubscribe).toEqual([{ instantlyCampaignId: "self:b", leadEmail: "other@else.com", step: 1 }]);
    expect(d!.orphanCandidate).toBe(false);
  });

  it("an unthreaded mail: orphan candidate unless from our own domains", () => {
    const stray = { messageId: "<o1@x>", headers: { from: "Stacy <stacy@clinic.com>" }, body: "", from: "Stacy <stacy@clinic.com>", subject: "Following up" };
    const warm = { ...stray, messageId: "<o2@x>", from: "w@m.co", headers: { from: "w@m.co" } };
    const [a, b] = decideInbound(batch([stray, warm]));
    expect(a).toMatchObject({ kind: "unrelated", orphanCandidate: true });
    expect(b).toMatchObject({ kind: "unrelated", orphanCandidate: false });
  });

  it("material carries only the sends the batch can reach, and the words round-trip", () => {
    const input = batch([reply, { messageId: "<u1@x>", headers: { from: "other@else.com" }, body: "", from: "other@else.com", subject: "unsubscribe" }]);
    const m = inboundMaterial(input) as { knownSends: Array<{ messageId: string }>; ownMailDomains: string[] };
    expect(m.knownSends.map((k) => k.messageId)).toEqual(["<s1@m.co>", "<s2@m.co>", "<t1@m.co>"]);
    expect(m.ownMailDomains).toEqual(["m.co"]);
    const ours = decideInbound(input);
    expect(inboundFromWords(JSON.parse(JSON.stringify(inboundWords(ours))))).toEqual(ours);
  });

  it("the pruned material decides like the whole mailbox history", () => {
    const input = batch([reply, { messageId: "<o1@x>", headers: { from: "stacy@clinic.com" }, body: "", from: null, subject: "x" }]);
    const m = inboundMaterial(input) as { knownSends: Array<{ messageId: string; instantlyCampaignId: string; leadEmail: string; step: number }> };
    const pruned = new Map(m.knownSends.map((k) => [k.messageId, { instantlyCampaignId: k.instantlyCampaignId, leadEmail: k.leadEmail, step: k.step }]));
    expect(sameWords(inboundWords(decideInbound({ ...input, knownSends: pruned })), inboundWords(decideInbound(input)))).toBe(true);
  });
});

describe("reply_received through the switch: one decision per poll batch", () => {
  it("no switch row: ours, recorded as service on an inbound-decision run, nothing asked", async () => {
    const run = vi.fn();
    const input = batch([reply]);
    expect(await inboundDecisionsToAct(input, { switchState: none, run })).toEqual(decideInbound(input));
    await drain();
    expect(run).not.toHaveBeenCalled();
    expect(inserted).toHaveLength(1);
    expect(inserted[0]).toMatchObject({ outcome: "reply_received", path: "service", runId: `run-${INBOUND_DECISION_TASK}` });
  });

  it("shadow: ours used, the script asked with the material on message_sent, compared", async () => {
    const input = batch([reply]);
    const ours = decideInbound(input);
    const run = vi.fn(async () => ({ runId: "s", scriptSlug: null, words: inboundWords(ours) }));
    expect(await inboundDecisionsToAct(input, { switchState: shadow, run })).toEqual(ours);
    await drain();
    expect(run).toHaveBeenCalledWith(expect.objectContaining({ outcome: "reply_received", consumes: "message_sent", material: inboundMaterial(input) }));
    expect(inserted[0]).toMatchObject({ path: "shadow", same: true });
  });

  it("switched: the script's decisions are used", async () => {
    const input = batch([reply]);
    const theirs = { decisions: [{ messageId: "<r1@prospect.com>", kind: "unrelated", referencedMessageIds: [], correlation: { outcome: "none" }, mailtoUnsubscribe: [], orphanCandidate: false }] };
    const run = vi.fn(async () => ({ runId: "s", scriptSlug: null, words: theirs }));
    const used = await inboundDecisionsToAct(input, { switchState: switched, run });
    expect(used[0]!.kind).toBe("unrelated");
    expect(inserted[0]).toMatchObject({ path: "script", same: false });
  });
});

describe("decideThroughScript: no Engine-specific branch", () => {
  it("an unrecordable decision never fails the caller", async () => {
    const out = await decideThroughScript({ outcome: "x", task: "t", consumes: "y", fromWords: () => 2 }, 1, () => ({ material: {}, oursWords: {} }), {
      switchState: none,
      openRun: async () => {
        throw new Error("runs down");
      },
      record: async () => {
        throw new Error("db down");
      },
    });
    expect(out).toBe(1);
    await drain();
  });
});
