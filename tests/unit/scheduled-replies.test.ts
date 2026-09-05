import { describe, it, expect } from "vitest";

import {
  selectDueScheduledReplies,
  type ScheduledReply,
} from "../../src/lib/scheduled-replies";
import { canSendReplyNow } from "../../src/lib/reply-to-lead";

function reply(over: Partial<ScheduledReply> = {}): ScheduledReply {
  return {
    id: "sr-1",
    orgId: "org-1",
    userId: "user-1",
    campaignId: "camp-1",
    instantlyCampaignId: "ic-1",
    leadEmail: "alice@media.com",
    bodyHtml: "<p>Thursday works.</p>",
    timezone: "America/Chicago",
    scheduledFor: new Date("2026-09-02T13:00:00.000Z"),
    attempts: 0,
    ...over,
  };
}

// Wednesday. 14:00 UTC = 09:00 Chicago = 23:00 Tokyo.
const WED_OPEN_IN_CHICAGO = new Date("2026-09-02T14:00:00.000Z");
// Saturday, same local hour.
const SAT_OPEN_IN_CHICAGO = new Date("2026-09-05T14:00:00.000Z");
// Wednesday 04:00 UTC = 23:00 Tuesday in Chicago, 13:00 in Tokyo.
const WED_CLOSED_IN_CHICAGO = new Date("2026-09-03T04:00:00.000Z");

describe("selectDueScheduledReplies", () => {
  it("sends a due reply once the prospect's window is open", () => {
    expect(selectDueScheduledReplies([reply()], WED_OPEN_IN_CHICAGO)).toHaveLength(1);
  });

  it("holds a reply whose window has not opened yet", () => {
    // 23:00 in their day — the whole reason the reply was queued.
    expect(selectDueScheduledReplies([reply()], WED_CLOSED_IN_CHICAGO)).toHaveLength(0);
  });

  it("resolves the window in the LEAD's timezone, not the fleet's", () => {
    // Same instant: closed in Chicago, open in Tokyo.
    expect(
      selectDueScheduledReplies(
        [reply({ timezone: "Asia/Tokyo" })],
        WED_CLOSED_IN_CHICAGO,
      ),
    ).toHaveLength(1);
  });

  it("falls back to the fleet default when we hold no timezone", () => {
    // Null must behave exactly like the default zone — the same value the
    // Instantly campaign schedule degrades to, so both transports agree.
    expect(selectDueScheduledReplies([reply({ timezone: null })], WED_OPEN_IN_CHICAGO))
      .toHaveLength(1);
    expect(
      selectDueScheduledReplies([reply({ timezone: null })], WED_CLOSED_IN_CHICAGO),
    ).toHaveLength(0);
  });

  it("sends nothing on a weekend, exactly like the sequence steps", () => {
    // Both transports run on the SAME mailboxes, and the weekly placement test
    // claims the Saturday slot precisely because they are otherwise empty.
    expect(selectDueScheduledReplies([reply()], SAT_OPEN_IN_CHICAGO)).toHaveLength(0);
  });

  it("holds a reply whose scheduled instant is still in the future", () => {
    expect(
      selectDueScheduledReplies(
        [reply({ scheduledFor: new Date("2026-09-09T13:00:00.000Z") })],
        WED_OPEN_IN_CHICAGO,
      ),
    ).toHaveLength(0);
  });

  it("drains oldest-due first, so a reply held over a weekend goes out first", () => {
    const due = selectDueScheduledReplies(
      [
        reply({ id: "fresh", scheduledFor: new Date("2026-09-02T13:00:00.000Z") }),
        reply({ id: "held", scheduledFor: new Date("2026-08-28T13:00:00.000Z") }),
      ],
      WED_OPEN_IN_CHICAGO,
    );
    expect(due.map((r) => r.id)).toEqual(["held", "fresh"]);
  });

  it("is deterministic on a tie", () => {
    const at = new Date("2026-09-02T13:00:00.000Z");
    const due = selectDueScheduledReplies(
      [reply({ id: "b", scheduledFor: at }), reply({ id: "a", scheduledFor: at })],
      WED_OPEN_IN_CHICAGO,
    );
    expect(due.map((r) => r.id)).toEqual(["a", "b"]);
  });
});

describe("canSendReplyNow", () => {
  it("is the SAME pair of gates the sequence dispatch applies", () => {
    expect(canSendReplyNow(WED_OPEN_IN_CHICAGO, "America/Chicago")).toBe(true);
    expect(canSendReplyNow(WED_CLOSED_IN_CHICAGO, "America/Chicago")).toBe(false);
    expect(canSendReplyNow(SAT_OPEN_IN_CHICAGO, "America/Chicago")).toBe(false);
    expect(canSendReplyNow(WED_OPEN_IN_CHICAGO, null)).toBe(true);
  });

  it("agrees with the drain's own selection, over every case", () => {
    for (const asOf of [WED_OPEN_IN_CHICAGO, WED_CLOSED_IN_CHICAGO, SAT_OPEN_IN_CHICAGO]) {
      for (const tz of ["America/Chicago", "Asia/Tokyo", null]) {
        const selected =
          selectDueScheduledReplies([reply({ timezone: tz })], asOf).length > 0;
        expect(selected).toBe(canSendReplyNow(asOf, tz));
      }
    }
  });
});
