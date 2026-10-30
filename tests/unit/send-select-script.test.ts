import { describe, it, expect, vi, beforeEach } from "vitest";

// FACTORY-P3 next outcomes (message_sent): the send selection asked of the stored script, in shadow (ours sent, the
// script compared beside it, never delaying a send) or switched (the script's sent, ours compared).
const inserted: Array<Record<string, unknown>> = [];
vi.mock("../../src/db", () => ({
  db: {
    insert: () => ({ values: async (v: Record<string, unknown>) => void inserted.push(v) }),
    delete: () => ({ where: async () => undefined }),
  },
}));
vi.mock("../../src/lib/runs-client", () => ({
  createPlatformRun: vi.fn(async () => ({ id: "sel-run" })),
  closePlatformRun: vi.fn(async () => undefined),
}));

import { selectionToSend, selectionWords, selectionMaterial } from "../../src/lib/self-send/send-select-script";
import type { DueSelection } from "../../src/lib/self-send/dispatch";

const asOf = new Date("2026-10-30T14:00:00.000Z");
const ours: DueSelection = {
  selected: [{ instantlyCampaignId: "c1", leadEmail: "l@x.co", accountEmail: "a@m.co", step: 1, dueAt: asOf }],
  dueBeforeCapacity: 1, blockedNoCapacityRow: 0, skippedSilenced: 0, rehomed: 0, heldBounceBudget: 0, heldBlocklisted: 0, heldPlacement: 0, heldMostlySpam: 0,
};
const input = {
  asOf,
  sequences: [{ instantlyCampaignId: "c1", leadEmail: "l@x.co", accountEmail: "a@m.co", provisionedSteps: [1], lastSentStep: null, lastSentAt: null, stepDelays: [], timezone: "europe/paris", queuedAt: null }],
  capacities: [{ accountEmail: "a@m.co", mailbox: "m", cap: 10, recentSustainedDaily: 2, sentToday: 0 }],
  skippedMailboxes: new Set<string>(),
  bounceHistory: new Map([["m.co", new Map([[24, { sends: 3, bounces: 0 }]])]]),
};
const drain = async () => {
  for (let i = 0; i < 10; i++) await new Promise((r) => setTimeout(r, 0));
};

beforeEach(() => {
  inserted.length = 0;
});

describe("the send selection's material", () => {
  it("is JSON the script reads: instants as ISO text, the time zone resolved, the bounce history by window", () => {
    const m = selectionMaterial(input as never) as { sequences: Array<{ timezone: string }>; bounceHistory: Record<string, Record<string, unknown>>; asOf: string };
    expect(m.asOf).toBe("2026-10-30T14:00:00.000Z");
    expect(m.sequences[0]!.timezone).toBe("Europe/Paris");
    expect(m.bounceHistory).toEqual({ "m.co": { "24": { sends: 3, bounces: 0 } } });
  });
});

describe("selectionToSend", () => {
  it("not switched, no shadow: ours, nothing asked", async () => {
    const run = vi.fn();
    expect(await selectionToSend(ours, input as never, { switchState: async () => ({ switched: false, shadow: false, dynasty: null }), run })).toBe(ours);
    expect(run).not.toHaveBeenCalled();
  });

  it("shadow: ours is sent at once; the script answers beside it and both are recorded, compared", async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const run = vi.fn(async () => {
      await gate;
      return { runId: "script-run", scriptSlug: "message_sent/dy_send_select/v1", words: selectionWords(ours) };
    });
    const chosen = await selectionToSend(ours, input as never, { switchState: async () => ({ switched: false, shadow: true, dynasty: "dy_send_select" }), run });
    expect(chosen).toBe(ours);
    expect(inserted).toHaveLength(0);
    release();
    await drain();
    expect(inserted[0]).toMatchObject({ path: "shadow", runId: "sel-run", scriptRunId: "script-run", same: true, sends: 1, error: null });
  });

  it("switched: the script's selection is sent; a different one is recorded as such", async () => {
    const theirs = { ...selectionWords(ours), selected: [{ ...(selectionWords(ours).selected as object[])[0], accountEmail: "b@m.co" }] };
    const chosen = await selectionToSend(ours, input as never, { switchState: async () => ({ switched: true, shadow: false, dynasty: "dy_send_select" }), run: async () => ({ runId: "r", scriptSlug: "s", words: theirs }) });
    expect(chosen.selected[0]!.accountEmail).toBe("b@m.co");
    expect(chosen.selected[0]!.dueAt.getTime()).toBe(asOf.getTime());
    expect(inserted[0]).toMatchObject({ path: "script", same: false });
  });

  it("switched: a script failure fails this dispatch run (never our own sent silently)", async () => {
    await expect(selectionToSend(ours, input as never, { switchState: async () => ({ switched: true, shadow: false, dynasty: "d" }), run: async () => { throw new Error("boom"); } })).rejects.toThrow("boom");
    expect(inserted[0]).toMatchObject({ path: "script", error: "boom", same: null });
  });
});
