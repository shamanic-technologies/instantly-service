import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";

// MON-SEED-MS365-MISSING-1120: on 2026-11-20 the Microsoft 365 test mailbox read
// 0 of 205 seeds (its tenant's trial ended: `451 4.4.4 … no mail-enabled
// subscriptions`), its token was fine, nothing turned red. Then SES suppressed
// the address, so even a renewed tenant would never get an SES seed again.

import { judgeSeedPlacementHealth } from "../../src/lib/seed-placement/health";
import {
  judgeSilentReceivers,
  refusalReason,
  silentReceiverProblem,
  SILENT_RECEIVER_MIN_SEEDS,
} from "../../src/lib/seed-placement/silent-receivers";
import { parseSesTimestamp, shouldLiftSuppression } from "../../src/lib/seed-placement/ses-suppression";

const ASOF = new Date("2026-11-20T12:00:00.000Z");

// Verbatim prod DSN (imap_messages_raw, kevin@growthservice.org, 2026-11-20 06:18 UTC).
const PROD_DSN =
  "Amazon SES delivery failure: Permanent / General\nFinal-Recipient: kevinlourd@distributeyou.onmicrosoft.com\nAction: failed\nStatus: 4.4.7\nDiagnostic-Code: smtp; 554 4.4.7 Message expired: unable to deliver in 840 minutes.<451 4.4.4 Mail received as unauthenticated, incoming to a recipient domain configured in a hosted tenant which has no mail-enabled subscriptions. ATTR5 [PA1PEPF000CC3FB.FRAP264.PROD.OUTLOOK.COM]>";

const MS365_ROW = {
  receiverEmail: "kevinlourd@distributeyou.onmicrosoft.com",
  recipientEsp: 2,
  unfound: 149,
  since: "2026-11-19 11:45:00.21045",
  lastFoundAt: "2026-11-19 08:41:31.640314",
  lastRefusal: PROD_DSN,
};

describe("silent receivers", () => {
  it("flags the Microsoft 365 receiver of 2026-11-20 with its server's own refusal", () => {
    const [r] = judgeSilentReceivers([MS365_ROW]);
    expect(r.provider).toBe("Microsoft 365");
    const sentence = silentReceiverProblem(r);
    expect(sentence).toMatch(/Microsoft 365 test mailbox kevinlourd@distributeyou.onmicrosoft.com received none of the 149 test emails/);
    expect(sentence).toMatch(/no mail-enabled subscriptions/);
    expect(sentence).toMatch(/last one found 2026-11-19/);
  });

  it("a few unfound seeds (normal loss) are not silent (control)", () => {
    expect(judgeSilentReceivers([{ ...MS365_ROW, unfound: SILENT_RECEIVER_MIN_SEEDS - 1 }])).toEqual([]);
  });

  it("labels each provider apart", () => {
    const out = judgeSilentReceivers([
      { ...MS365_ROW, receiverEmail: "a@gmail.com", recipientEsp: 1 },
      { ...MS365_ROW, receiverEmail: "b@outlook.com", recipientEsp: 4 },
    ]);
    expect(out.map((r) => r.provider)).toEqual(["Google", "Outlook.com"]);
  });

  it("reads the Diagnostic-Code, else the first line, else nothing", () => {
    expect(refusalReason(PROD_DSN)).toMatch(/^554 4.4.7 Message expired/);
    expect(refusalReason("Undeliverable: hello")).toBe("Undeliverable: hello");
    expect(refusalReason(null)).toBeNull();
  });

  it("turns the placement check red, even with the weekly test on time and every token fresh", () => {
    const base = {
      lastGateTestAt: new Date("2026-11-20T07:30:00Z"),
      googleReceivers: 10,
      vendorRefusals: [],
      microsoftReceivers: [
        { email: MS365_ROW.receiverEmail, status: "authorized", lastRefreshedAt: "2026-11-20T11:07:48Z", lastError: null },
      ],
      asOf: ASOF,
    };
    expect(judgeSeedPlacementHealth(base).ok).toBe(true);
    const red = judgeSeedPlacementHealth({ ...base, silentReceivers: judgeSilentReceivers([MS365_ROW]) });
    expect(red.ok).toBe(false);
    expect(red.reason).toMatch(/received none of the 149 test emails/);
  });
});

describe("SES suppression of our own receivers", () => {
  const suppressed = { reason: "BOUNCE", lastUpdateTime: new Date("2026-11-20T06:18:33Z") };

  it("stays suppressed while the receiver found nothing since (never lifted on hope)", () => {
    expect(shouldLiftSuppression(suppressed, new Date("2026-11-19T08:41:31Z"))).toBe(false);
    expect(shouldLiftSuppression(suppressed, null)).toBe(false);
  });

  it("is lifted once a seed landed there after the suppression", () => {
    expect(shouldLiftSuppression(suppressed, new Date("2026-11-22T07:10:00Z"))).toBe(true);
  });

  it("not suppressed = nothing to lift", () => {
    expect(shouldLiftSuppression(null, new Date())).toBe(false);
  });

  it("reads SES timestamps as epoch seconds or ISO, refuses junk", () => {
    // Verbatim prod answer for the Microsoft 365 receiver (GET suppression, 2026-11-20).
    expect(parseSesTimestamp(1.79515551363e9)?.toISOString()).toBe("2026-11-20T06:18:33.630Z");
    expect(parseSesTimestamp("2026-11-20T06:18:33Z")?.toISOString()).toBe("2026-11-20T06:18:33.000Z");
    expect(parseSesTimestamp("x")).toBeNull();
    expect(parseSesTimestamp(null)).toBeNull();
  });
});

describe("a retired Microsoft receiver (MON-SEED-MS365-RETIRED-1121)", () => {
  it("is never red: neither as a dead token nor as a silent receiver", () => {
    const h = judgeSeedPlacementHealth({
      lastGateTestAt: new Date("2026-11-20T07:30:00Z"),
      googleReceivers: 10,
      vendorRefusals: [],
      microsoftReceivers: [
        { email: MS365_ROW.receiverEmail, status: "retired", lastRefreshedAt: "2026-11-20T11:07:48Z", lastError: "retired: trial ended, not renewed" },
      ],
      silentReceivers: [],
      asOf: new Date("2026-11-21T12:00:00Z"),
    });
    expect(h.ok).toBe(true);
    const src = readFileSync("src/lib/seed-placement/silent-receivers.ts", "utf8");
    expect(src).toMatch(/microsoft_seed_mailboxes m\s+WHERE m\.email = d\.receiver_email AND m\.status <> 'authorized'/);
  });
});
