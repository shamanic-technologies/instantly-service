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
