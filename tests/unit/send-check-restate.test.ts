import { describe, it, expect } from "vitest";
import { addressFix } from "../../src/lib/send-check-restate";

describe("addressFix: a past send consumes the address occurrence that exists (LEDGER-HISTORY-1024)", () => {
  it("no check ever produced: take back email_checked, declare email_found", () => {
    expect(addressFix(new Set(["email_checked"]), false)).toEqual({ voidKey: "email_checked", declare: true });
  });
  it("a check existed: a run that consumed email_found moves back to email_checked", () => {
    expect(addressFix(new Set(["email_found"]), true)).toEqual({ voidKey: "email_found", declare: true });
  });
  it("already right: nothing to do (a re-run voids nothing)", () => {
    expect(addressFix(new Set(["email_checked"]), true)).toEqual({ voidKey: null, declare: false });
    expect(addressFix(new Set(["email_found"]), false)).toEqual({ voidKey: null, declare: false });
  });
  it("voided by an earlier pass but never re-declared: declared now", () => {
    expect(addressFix(new Set(), false)).toEqual({ voidKey: null, declare: true });
  });
});

describe("fetchConsumedAddressKeys: the ledger read stays under the server's header limit", () => {
  it("asks 200 runs a call (500 uuids = 431 Request Header Fields Too Large)", async () => {
    const { fetchConsumedAddressKeys, CONSUMED_READ_RUNS } = await import("../../src/lib/send-check-restate");
    const calls: string[] = [];
    const orig = globalThis.fetch;
    globalThis.fetch = (async (url: string) => {
      calls.push(url);
      return new Response(JSON.stringify({ occurrences: [{ runId: "r1", outcomeKey: "email_checked" }, { runId: "r1", outcomeKey: "message_written" }], nextCursor: null }), { status: 200 });
    }) as typeof fetch;
    const ids = Array.from({ length: 450 }, (_, i) => `00000000-0000-4000-8000-${String(i).padStart(12, "0")}`);
    const keys = await fetchConsumedAddressKeys(ids);
    globalThis.fetch = orig;
    expect(CONSUMED_READ_RUNS).toBe(200);
    expect(calls).toHaveLength(3);
    expect(Math.max(...calls.map((u) => u.length))).toBeLessThan(8_500);
    expect([...(keys.get("r1") ?? [])]).toEqual(["email_checked"]);
  });
});
