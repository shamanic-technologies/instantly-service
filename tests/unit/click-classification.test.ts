import { describe, it, expect } from "vitest";

import {
  CLICK_DECISION_HOLD_SECONDS,
  PAIRED_UNSUBSCRIBE_WINDOW_SECONDS,
  SCANNER_REASONS,
  classifyClickHit,
  classifyImmediateSignals,
  isScannerUserAgent,
} from "../../src/lib/self-send/click-classification";

const CHROME =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36";
const SAFE_LINKS =
  "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727)";

describe("click classification — request-time signals", () => {
  it("calls a HEAD request a scanner: a browser following a link never HEADs", () => {
    expect(classifyImmediateSignals({ method: "HEAD", userAgent: CHROME })).toEqual({
      verdict: "scanner",
      reason: SCANNER_REASONS.headRequest,
    });
  });

  it("calls the Microsoft Safe Links user-agent a scanner", () => {
    expect(classifyImmediateSignals({ method: "GET", userAgent: SAFE_LINKS })).toEqual({
      verdict: "scanner",
      reason: SCANNER_REASONS.scannerUserAgent,
    });
  });

  it("calls a Go-http-client a scanner", () => {
    expect(isScannerUserAgent("Go-http-client/1.1")).toBe(true);
  });

  it("calls a missing user-agent a scanner: no browser omits it", () => {
    expect(classifyImmediateSignals({ method: "GET", userAgent: null })).toEqual({
      verdict: "scanner",
      reason: SCANNER_REASONS.missingUserAgent,
    });
    expect(classifyImmediateSignals({ method: "GET", userAgent: "   " })).toEqual({
      verdict: "scanner",
      reason: SCANNER_REASONS.missingUserAgent,
    });
  });

  it("returns UNDECIDED — never human — for a plausible browser, because the decisive signal arrives later", () => {
    expect(classifyImmediateSignals({ method: "GET", userAgent: CHROME })).toBeNull();
  });

  it("does NOT treat the dominant fixed Chrome string as a scanner on its own", () => {
    // 213 of 309 prod click hits carry it and it is also a real human UA;
    // blanket-rejecting it would discard every genuine Windows/Chrome click.
    expect(isScannerUserAgent(CHROME)).toBe(false);
  });

  it("does not reject an ordinary mobile browser", () => {
    const iphone =
      "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1";
    expect(isScannerUserAgent(iphone)).toBe(false);
    expect(classifyImmediateSignals({ method: "GET", userAgent: iphone })).toBeNull();
  });
});

describe("click classification — the paired opt-out fetch", () => {
  it("calls a click paired with an opt-out fetch a scanner: a human never fetches both", () => {
    expect(
      classifyClickHit({
        method: "GET",
        userAgent: CHROME,
        hasPairedUnsubscribeFetch: true,
      }),
    ).toEqual({ verdict: "scanner", reason: SCANNER_REASONS.pairedUnsubscribeFetch });
  });

  it("calls an unpaired browser GET a human, with no reason attached", () => {
    expect(
      classifyClickHit({
        method: "GET",
        userAgent: CHROME,
        hasPairedUnsubscribeFetch: false,
      }),
    ).toEqual({ verdict: "human", reason: null });
  });

  it("keeps the request-time reason when both signals fire", () => {
    expect(
      classifyClickHit({
        method: "HEAD",
        userAgent: SAFE_LINKS,
        hasPairedUnsubscribeFetch: true,
      }).reason,
    ).toBe(SCANNER_REASONS.headRequest);
  });
});

describe("click classification — the hold", () => {
  it("holds strictly longer than the pairing window, so a late opt-out fetch is already recorded", () => {
    expect(CLICK_DECISION_HOLD_SECONDS).toBeGreaterThan(PAIRED_UNSUBSCRIBE_WINDOW_SECONDS);
  });

  it("uses NO time-since-send threshold: scanners and phone-reading humans share that window", () => {
    const source = classifyClickHit.toString() + classifyImmediateSignals.toString();
    expect(source).not.toMatch(/sentAt|secondsSinceSend|delayMs/);
  });
});
