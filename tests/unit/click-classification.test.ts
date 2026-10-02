import { describe, it, expect } from "vitest";
import type { SQL } from "drizzle-orm";
import { PgDialect } from "drizzle-orm/pg-core";

import {
  CLICK_DECISION_HOLD_SECONDS,
  PAIRED_UNSUBSCRIBE_WINDOW_SECONDS,
  SCANNER_REASONS,
  classifyClickHit,
  classifyImmediateSignals,
  isScannerUserAgent,
  scannerNetworkEvidenceSql,
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
        sharesScannerNetwork: false,
      }),
    ).toEqual({ verdict: "scanner", reason: SCANNER_REASONS.pairedUnsubscribeFetch });
  });

  it("calls an unpaired browser GET a human, with no reason attached", () => {
    expect(
      classifyClickHit({
        method: "GET",
        userAgent: CHROME,
        hasPairedUnsubscribeFetch: false,
        sharesScannerNetwork: false,
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

// Measured 2026-10-02 (Olive): Microsoft Defender detonated the links from the
// same Azure /24s with an unreduced Windows UA AND an ordinary reduced Mac UA.
const DEFENDER_MAC =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36";

/** Render a drizzle SQL fragment the way node-postgres will receive it. */
function flatten(node: SQL): string {
  return new PgDialect().sqlToQuery(node).sql;
}

describe("click classification — the scanner's network", () => {
  it("calls an ordinary-looking click from a /24 a scanner already used a scanner", () => {
    expect(
      classifyClickHit({
        method: "GET",
        userAgent: DEFENDER_MAC,
        hasPairedUnsubscribeFetch: false,
        sharesScannerNetwork: true,
      }),
    ).toEqual({ verdict: "scanner", reason: SCANNER_REASONS.scannerNetwork });
  });

  it("negative control: the same click from a network with no scanner verdict stays human", () => {
    expect(
      classifyClickHit({
        method: "GET",
        userAgent: DEFENDER_MAC,
        hasPairedUnsubscribeFetch: false,
        sharesScannerNetwork: false,
      }),
    ).toEqual({ verdict: "human", reason: null });
  });

  it("keys the evidence on the /24, a scanner verdict, other clicks only, and a ±7 day window", () => {
    const text = flatten(scannerNetworkEvidenceSql("h"));
    expect(text).toContain("s.classification = 'scanner'");
    expect(text).toContain("s.kind = 'click'");
    expect(text).toContain("s.id <> h.id");
    expect(text).toContain("interval '7 days'");
    expect(text).toContain("(\\d+\\.\\d+\\.\\d+)\\.\\d+$");
  });

  it("never treats a private address as a network: pre-fix hits all carry Caddy's 172.18.x", () => {
    const text = flatten(scannerNetworkEvidenceSql("h"));
    expect(text).toContain("172\\.(1[6-9]|2\\d|3[01])");
    expect(text).toContain("192\\.168");
    expect(text).toContain("(10|127)");
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

describe("click classification — the frozen Chrome version", () => {
  const unreduced =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.7444.175 Safari/537.36";

  it("calls a modern Chrome carrying a full build number a scanner: real Chrome reports X.0.0.0", async () => {
    const { isUnreducedChromeVersion } = await import(
      "../../src/lib/self-send/click-classification"
    );
    expect(isUnreducedChromeVersion(unreduced)).toBe(true);
    expect(classifyImmediateSignals({ method: "GET", userAgent: unreduced })).toEqual({
      verdict: "scanner",
      reason: SCANNER_REASONS.scannerUserAgent,
    });
  });

  it("leaves a real reduced Chrome alone, on every platform that carries the token", async () => {
    const { isUnreducedChromeVersion } = await import(
      "../../src/lib/self-send/click-classification"
    );
    for (const ua of [
      CHROME,
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
      "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Mobile Safari/537.36",
      // Edge reduces its Chrome token and carries its own full version after it.
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.3296.62",
    ]) {
      expect(isUnreducedChromeVersion(ua)).toBe(false);
      expect(classifyImmediateSignals({ method: "GET", userAgent: ua })).toBeNull();
    }
  });

  it("leaves a genuinely OLD Chrome alone — it predates the version freeze", async () => {
    const { isUnreducedChromeVersion } = await import(
      "../../src/lib/self-send/click-classification"
    );
    expect(
      isUnreducedChromeVersion(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36",
      ),
    ).toBe(false);
  });

  it("does not fire on a browser with no Chrome token at all", async () => {
    const { isUnreducedChromeVersion } = await import(
      "../../src/lib/self-send/click-classification"
    );
    expect(
      isUnreducedChromeVersion(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0",
      ),
    ).toBe(false);
  });
});
