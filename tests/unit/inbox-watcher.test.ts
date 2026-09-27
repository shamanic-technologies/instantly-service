import { describe, it, expect } from "vitest";

import {
  groupAccountsByLogin,
  INBOX_WATCH_REFRESH_MS,
  nextBackoff,
  nextPollQuery,
  RECONNECT_MAX_MS,
  RECONNECT_MIN_MS,
  IDLE_RESTART_MS,
} from "../../src/lib/self-send/inbox-watcher";

describe("inbox watcher — pure pieces", () => {
  it("watches one session per REAL mailbox, not per alias", () => {
    const groups = groupAccountsByLogin(
      ["kevin@salesmolt.com", "klourd@salesmolt.com", "clara@leansignalio.com"],
      new Map([
        ["kevin@salesmolt.com", "eric@salesmolt.com"],
        ["klourd@salesmolt.com", "eric@salesmolt.com"],
      ]),
    );
    expect([...groups.keys()].sort()).toEqual([
      "clara@leansignalio.com",
      "eric@salesmolt.com",
    ]);
    expect(groups.get("eric@salesmolt.com")).toEqual([
      "kevin@salesmolt.com",
      "klourd@salesmolt.com",
    ]);
  });

  it("an address the login map does not know is its own mailbox", () => {
    const groups = groupAccountsByLogin(["Clara@LeanSignalIO.com"], new Map());
    expect(groups.get("clara@leansignalio.com")).toEqual(["Clara@LeanSignalIO.com"]);
  });

  it("reads the whole catch-up window until a read has set the cursor", () => {
    const asOf = new Date("2026-09-25T06:00:00.000Z");
    expect(nextPollQuery(0, asOf)).toEqual({
      since: new Date("2026-09-22T06:00:00.000Z"),
    });
  });

  it("then reads only what came after the last UID", () => {
    expect(nextPollQuery(812, new Date())).toEqual({ uidFrom: 812 });
  });

  it("backs off by doubling, bounded", () => {
    expect(nextBackoff(RECONNECT_MIN_MS)).toBe(RECONNECT_MIN_MS * 2);
    expect(nextBackoff(RECONNECT_MAX_MS)).toBe(RECONNECT_MAX_MS);
    expect(nextBackoff(0)).toBe(RECONNECT_MIN_MS * 2);
  });

  it("states the bound: IDLE re-issued under the socket timeout, fallback within minutes", () => {
    expect(IDLE_RESTART_MS).toBeLessThan(5 * 60_000);
    expect(INBOX_WATCH_REFRESH_MS).toBeLessThanOrEqual(5 * 60_000);
  });
});
