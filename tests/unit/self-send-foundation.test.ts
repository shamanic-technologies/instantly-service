import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

import {
  SEND_TRANSPORT_INSTANTLY,
  SEND_TRANSPORT_SMTP,
  resolveTransportForSend,
} from "../../src/lib/self-send/transport";
import {
  GMAIL_IMAP_HOST,
  GMAIL_SMTP_HOST,
  MailboxCredentialError,
  selectMailboxCredential,
} from "../../src/lib/self-send/mailbox-credentials";
import {
  stepDelaysFromRows,
  stepsFromSequenceConfig,
} from "../../src/lib/self-send/sequence-steps";
import { delayForGap, STEP_GAP_CALENDAR_DAYS } from "../../src/lib/sending-forecast";
import { listPrimeforgeRawMailboxes } from "../../src/lib/providers/primeforge-client";

const originalFetch = global.fetch;

function jsonResponse(body: unknown, status = 200): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => body,
    text: async () => JSON.stringify(body),
  } as unknown as Response;
}

beforeEach(() => {
  vi.resetAllMocks();
});

afterEach(() => {
  global.fetch = originalFetch;
});

// ─── Transport resolution (pure) ──────────────────────────────────────────────

describe("resolveTransportForSend", () => {
  it("routes an account explicitly marked smtp to the self-send transport", () => {
    expect(resolveTransportForSend(SEND_TRANSPORT_SMTP)).toBe(SEND_TRANSPORT_SMTP);
  });

  it("routes an account marked instantly to Instantly", () => {
    expect(resolveTransportForSend(SEND_TRANSPORT_INSTANTLY)).toBe(SEND_TRANSPORT_INSTANTLY);
  });

  // The safe default matters: an account whose column was never backfilled, or
  // a value we do not recognise, must keep using the transport that is known to
  // work rather than silently divert live traffic onto the new path.
  it.each([[null], [undefined], [""], ["SMTP "], ["postmark"], ["  "]])(
    "falls back to Instantly for %p rather than diverting traffic",
    (value) => {
      expect(resolveTransportForSend(value as string | null | undefined)).toBe(
        SEND_TRANSPORT_INSTANTLY,
      );
    },
  );
});

// ─── Mailbox credential selection (pure) ──────────────────────────────────────

const MAILBOXES = [
  { address: "kevin@growdistribute.com", appPassword: "abcd efgh ijkl mnop", status: "active" },
  { address: "Michaela@SavioLabsCo.com", appPassword: "qrstuvwxabcdefgh", status: "active" },
  { address: "nopassword@plainsignalco.com", appPassword: "", status: "active" },
];

describe("selectMailboxCredential", () => {
  it("returns the Gmail hosts and a space-stripped app password", () => {
    const credential = selectMailboxCredential("kevin@growdistribute.com", MAILBOXES);

    expect(credential).toEqual({
      address: "kevin@growdistribute.com",
      appPassword: "abcdefghijklmnop",
      smtpHost: GMAIL_SMTP_HOST,
      imapHost: GMAIL_IMAP_HOST,
    });
    expect(credential.appPassword).toHaveLength(16);
  });

  it("matches the address case-insensitively", () => {
    const credential = selectMailboxCredential("michaela@saviolabsco.com", MAILBOXES);
    expect(credential.address).toBe("michaela@saviolabsco.com");
    expect(credential.appPassword).toBe("qrstuvwxabcdefgh");
  });

  // Fail loud: a missing mailbox must never degrade into an anonymous send.
  it("throws when the mailbox is absent from the vendor workspace", () => {
    expect(() => selectMailboxCredential("ghost@nowhere.com", MAILBOXES)).toThrow(
      MailboxCredentialError,
    );
  });

  it("throws when the mailbox carries no app password", () => {
    expect(() => selectMailboxCredential("nopassword@plainsignalco.com", MAILBOXES)).toThrow(
      MailboxCredentialError,
    );
  });
});

// ─── Primeforge mailbox pagination ────────────────────────────────────────────

describe("listPrimeforgeRawMailboxes", () => {
  it("walks every page and concatenates in order", async () => {
    const page1 = Array.from({ length: 100 }, (_, i) => ({
      address: `a${i}@d.com`,
      appPassword: "pw",
    }));
    const page2 = [{ address: "last@d.com", appPassword: "pw" }];

    const mockFetch = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ results: page1 }))
      .mockResolvedValueOnce(jsonResponse({ results: page2 }));
    global.fetch = mockFetch as unknown as typeof fetch;

    const all = await listPrimeforgeRawMailboxes("key");

    expect(mockFetch).toHaveBeenCalledTimes(2);
    expect(all).toHaveLength(101);
    expect(all[0]?.address).toBe("a0@d.com");
    expect(all[100]?.address).toBe("last@d.com");
  });
});

// ─── Sequence steps (pure) ────────────────────────────────────────────────────

const SEQUENCE_CONFIG = {
  sequences: [
    {
      steps: [
        { subject: "Hello", body: "<p>one</p>", delay: 2 },
        { subject: "", body: "<p>two</p>", delay: 5 },
        { subject: "", body: "<p>three</p>" },
      ],
    },
  ],
};

describe("stepsFromSequenceConfig", () => {
  it("emits 1-based steps carrying each step's own delay", () => {
    expect(stepsFromSequenceConfig(SEQUENCE_CONFIG)).toEqual([
      { step: 1, subject: "Hello", bodyHtml: "<p>one</p>", delayDays: 2 },
      { step: 2, subject: null, bodyHtml: "<p>two</p>", delayDays: 5 },
      { step: 3, subject: null, bodyHtml: "<p>three</p>", delayDays: null },
    ]);
  });

  it("returns an empty list for a config with no sequence rather than throwing", () => {
    expect(stepsFromSequenceConfig({})).toEqual([]);
    expect(stepsFromSequenceConfig({ sequences: [] })).toEqual([]);
  });

  it("skips a step with no body — an empty email is never dispatchable", () => {
    const steps = stepsFromSequenceConfig({
      sequences: [{ steps: [{ body: "<p>one</p>" }, { body: "" }, { body: "<p>three</p>" }] }],
    });
    expect(steps.map((s) => s.bodyHtml)).toEqual(["<p>one</p>", "<p>three</p>"]);
    // Re-numbered contiguously so the persisted steps stay 1..N with no hole.
    expect(steps.map((s) => s.step)).toEqual([1, 2]);
  });
});

describe("stepDelaysFromRows", () => {
  // The persisted rows must drop straight into the EXISTING cadence resolver, so
  // the self-send scheduler and the ops forecast cannot drift apart.
  it("produces a 0-based array that delayForGap indexes correctly", () => {
    const rows = [
      { step: 1, delayDays: 2 },
      { step: 2, delayDays: 5 },
      { step: 3, delayDays: null },
    ];

    const delays = stepDelaysFromRows(rows);

    expect(delays).toEqual([2, 5, null]);
    expect(delayForGap(1, delays)).toBe(2);
    expect(delayForGap(2, delays)).toBe(5);
    expect(delayForGap(3, delays)).toBe(STEP_GAP_CALENDAR_DAYS);
  });

  it("orders by step regardless of row order and fills a gap with null", () => {
    const delays = stepDelaysFromRows([
      { step: 3, delayDays: 7 },
      { step: 1, delayDays: 1 },
    ]);

    expect(delays).toEqual([1, null, 7]);
    expect(delayForGap(2, delays)).toBe(STEP_GAP_CALENDAR_DAYS);
  });

  it("returns an empty array for no rows", () => {
    expect(stepDelaysFromRows([])).toEqual([]);
  });
});
