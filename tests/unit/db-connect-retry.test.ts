import { describe, it, expect, vi } from "vitest";
import { isTransientConnectError, withConnectRetry } from "../../src/db/retry";

describe("isTransientConnectError", () => {
  it("returns true for a raw ETIMEDOUT socket error", () => {
    expect(isTransientConnectError({ code: "ETIMEDOUT" })).toBe(true);
  });

  it("returns true for the Node happy-eyeballs AggregateError (its own code is ETIMEDOUT)", () => {
    // This is the exact prod signature: cold Neon resume > 250ms attempt window.
    const agg = new AggregateError([new Error("connect ETIMEDOUT")], "");
    (agg as { code?: string }).code = "ETIMEDOUT";
    expect(isTransientConnectError(agg)).toBe(true);
  });

  it("returns true for ECONNREFUSED / EHOSTUNREACH / ENETUNREACH", () => {
    expect(isTransientConnectError({ code: "ECONNREFUSED" })).toBe(true);
    expect(isTransientConnectError({ code: "EHOSTUNREACH" })).toBe(true);
    expect(isTransientConnectError({ code: "ENETUNREACH" })).toBe(true);
  });

  it("returns true for pg's connectionTimeoutMillis expiry message", () => {
    expect(isTransientConnectError(new Error("timeout expired"))).toBe(true);
  });

  it("returns false for a real SQL error (undefined_table 42P01)", () => {
    expect(isTransientConnectError({ code: "42P01" })).toBe(false);
  });

  it("returns false for a statement timeout (57014) — query already ran", () => {
    expect(
      isTransientConnectError({
        code: "57014",
        message: "canceling statement due to statement timeout",
      }),
    ).toBe(false);
  });

  it("returns false for null / string / objects without code or matching message", () => {
    expect(isTransientConnectError(null)).toBe(false);
    expect(isTransientConnectError("ETIMEDOUT")).toBe(false);
    expect(isTransientConnectError({ message: "syntax error" })).toBe(false);
  });
});

describe("withConnectRetry", () => {
  const noSleep = vi.fn(async () => {});

  it("resolves on the first attempt without retrying or sleeping", async () => {
    const fn = vi.fn(async () => "ok");
    const onRetry = vi.fn();
    const result = await withConnectRetry(fn, { sleep: noSleep, onRetry });
    expect(result).toBe("ok");
    expect(fn).toHaveBeenCalledTimes(1);
    expect(onRetry).not.toHaveBeenCalled();
    expect(noSleep).not.toHaveBeenCalled();
  });

  it("retries a transient connect error and then succeeds", async () => {
    const transient = Object.assign(new Error("connect ETIMEDOUT"), { code: "ETIMEDOUT" });
    const fn = vi
      .fn()
      .mockRejectedValueOnce(transient)
      .mockRejectedValueOnce(transient)
      .mockResolvedValueOnce("ok");
    const onRetry = vi.fn();
    const result = await withConnectRetry(fn, { sleep: noSleep, onRetry });
    expect(result).toBe("ok");
    expect(fn).toHaveBeenCalledTimes(3);
    expect(onRetry).toHaveBeenCalledTimes(2);
  });

  it("throws the last error after exhausting retries", async () => {
    const transient = Object.assign(new Error("connect ETIMEDOUT"), { code: "ETIMEDOUT" });
    const fn = vi.fn().mockRejectedValue(transient);
    await expect(
      withConnectRetry(fn, { retries: 3, sleep: noSleep }),
    ).rejects.toBe(transient);
    expect(fn).toHaveBeenCalledTimes(4); // initial + 3 retries
  });

  it("does not retry a non-transient error", async () => {
    const sqlError = Object.assign(new Error("relation does not exist"), { code: "42P01" });
    const fn = vi.fn().mockRejectedValue(sqlError);
    await expect(withConnectRetry(fn, { sleep: noSleep })).rejects.toBe(sqlError);
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it("backs off exponentially from the base delay", async () => {
    const transient = Object.assign(new Error("connect ETIMEDOUT"), { code: "ETIMEDOUT" });
    const fn = vi.fn().mockRejectedValue(transient);
    const delays: number[] = [];
    const sleep = vi.fn(async (ms: number) => {
      delays.push(ms);
    });
    await expect(
      withConnectRetry(fn, { retries: 3, baseDelayMs: 250, sleep }),
    ).rejects.toBe(transient);
    expect(delays).toEqual([250, 500, 1000]);
  });
});
