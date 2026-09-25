import { describe, it, expect } from "vitest";

import {
  planScheduledReplies,
  SCHEDULED_REPLY_MAX_DRAFT_AGE_MS,
  type ScheduledReply,
} from "../../src/lib/scheduled-replies";

function reply(over: Partial<ScheduledReply> = {}): ScheduledReply {
  return {
    id: "sr-1",
    orgId: "org-1",
    userId: "user-1",
    campaignId: "camp-1",
    instantlyCampaignId: "ic-1",
    leadEmail: "alice@media.com",
    bodyHtml: "<p>Would tomorrow work?</p>",
    sentBy: "automation",
    timezone: "America/Chicago",
    scheduledFor: new Date("2026-09-25T13:00:00.000Z"),
    createdAt: new Date("2026-09-24T21:43:52.000Z"),
    attempts: 0,
    ...over,
  };
}

// Friday 04:00 UTC = 23:00 Thursday in Chicago — a closed window under the old rule.
const CLOSED_IN_CHICAGO = new Date("2026-09-25T04:00:00.000Z");
// Saturday — the old weekend gate held everything.
const SATURDAY = new Date("2026-09-26T15:00:00.000Z");

describe("planScheduledReplies — no window, only whether the words are still true", () => {
  it("sends a fresh draft at 23:00 in the prospect's day — there is no window any more", () => {
    const fresh = reply({ createdAt: new Date(CLOSED_IN_CHICAGO.getTime() - 60_000) });
    const plan = planScheduledReplies([fresh], CLOSED_IN_CHICAGO);
    expect(plan.send.map((r) => r.id)).toEqual(["sr-1"]);
    expect(plan.redraft).toEqual([]);
  });

  it("sends a fresh draft on a weekend", () => {
    const fresh = reply({ createdAt: new Date(SATURDAY.getTime() - 60_000) });
    expect(planScheduledReplies([fresh], SATURDAY).send).toHaveLength(1);
  });

  it("does NOT send an automated draft written hours ago — 'tomorrow' is no longer true", () => {
    // The 2026-09-24 case: drafted 21:43 UTC, 'tomorrow, September 25th'.
    const plan = planScheduledReplies([reply()], new Date("2026-09-25T12:00:00.000Z"));
    expect(plan.send).toEqual([]);
    expect(plan.redraft.map((r) => r.id)).toEqual(["sr-1"]);
  });

  it("the boundary is the draft age, inclusive", () => {
    const asOf = new Date("2026-09-25T12:00:00.000Z");
    const atLimit = reply({
      createdAt: new Date(asOf.getTime() - SCHEDULED_REPLY_MAX_DRAFT_AGE_MS),
    });
    const past = reply({
      id: "sr-2",
      createdAt: new Date(asOf.getTime() - SCHEDULED_REPLY_MAX_DRAFT_AGE_MS - 1),
    });
    const plan = planScheduledReplies([atLimit, past], asOf);
    expect(plan.send.map((r) => r.id)).toEqual(["sr-1"]);
    expect(plan.redraft.map((r) => r.id)).toEqual(["sr-2"]);
  });

  it("sends a PERSON's old words as written — nobody can redraft them", () => {
    const plan = planScheduledReplies(
      [reply({ sentBy: "human" })],
      new Date("2026-09-25T12:00:00.000Z"),
    );
    expect(plan.send).toHaveLength(1);
    expect(plan.redraft).toEqual([]);
  });

  it("orders oldest draft first, deterministic on a tie", () => {
    const asOf = new Date("2026-09-25T12:00:00.000Z");
    const at = new Date(asOf.getTime() - 60_000);
    const plan = planScheduledReplies(
      [
        reply({ id: "b", createdAt: at }),
        reply({ id: "a", createdAt: at }),
        reply({ id: "c", createdAt: new Date(at.getTime() - 1_000) }),
      ],
      asOf,
    );
    expect(plan.send.map((r) => r.id)).toEqual(["c", "a", "b"]);
  });
});
