/**
 * In-house seed placement — the pure halves.
 *
 * These guard the two properties the measurement rests on: a seed we could not
 * find counts AGAINST the sender (never silently shrinks the sample), and a
 * folder we cannot classify produces no verdict at all.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";

// The resolver reads key-service + the Primeforge mailbox list ONCE per run.
// Both are stubbed so the test can assert the composition: manual wins, the
// vendor answers next, and a mailbox nobody hosts yields null rather than
// throwing (that null is what keeps ~187 legacy accounts from each attempting
// ten doomed sends).
const mocks = vi.hoisted(() => ({
  resolvePlatformKey: vi.fn(),
  listPrimeforgeRawMailboxes: vi.fn(),
}));

vi.mock("../../src/lib/key-client", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  resolvePlatformKey: mocks.resolvePlatformKey,
}));

vi.mock("../../src/lib/providers/primeforge-client", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  listPrimeforgeRawMailboxes: mocks.listPrimeforgeRawMailboxes,
}));

import {
  ESP_GOOGLE,
  ESP_OUTLOOK,
  MAX_SEED_RECEIVERS,
  SEED_TEST_ID_PREFIX,
  domainOf,
  espForReceiver,
  isSeedTestId,
  mintSeedTestId,
  planSeedSends,
  selectSeedReceivers,
} from "../../src/lib/seed-placement/seeds";
import {
  aggregateSeedPlacement,
  classifySeedFolder,
  parseAuthResults,
} from "../../src/lib/seed-placement/classify";
import {
  MANUAL_CREDENTIALS_PROVIDER,
  loadSeedCredentialResolver,
  parseManualCredentials,
  selectManualCredential,
} from "../../src/lib/seed-placement/credentials";
import { KeyServiceError } from "../../src/lib/key-client";
import {
  SEED_SENDER_HEADER,
  SEED_TEST_HEADER,
  buildSeedMessage,
} from "../../src/lib/seed-placement/message";

describe("seed test id", () => {
  it("carries the seed: prefix so silver keeps our results distinguishable from Instantly's", () => {
    const id = mintSeedTestId();
    expect(id.startsWith(SEED_TEST_ID_PREFIX)).toBe(true);
    expect(isSeedTestId(id)).toBe(true);
  });

  it("does not claim an Instantly test id as its own", () => {
    expect(isSeedTestId("019f9856-1111-4aaa-8bbb-222222222222")).toBe(false);
  });
});

describe("selectSeedReceivers", () => {
  it("takes at most ONE mailbox per domain — diversity is the signal, not count", () => {
    const receivers = selectSeedReceivers([
      "a@one.com",
      "b@one.com",
      "c@two.com",
      "d@three.com",
    ]);

    // Ordered by DOMAIN (one, three, two), not by input order — that stability
    // is what makes week-over-week receiver sets comparable.
    expect(receivers.map((r) => r.email)).toEqual([
      "a@one.com",
      "d@three.com",
      "c@two.com",
    ]);
  });

  it("is deterministic, so week-over-week scores stay comparable", () => {
    const pool = ["z@three.com", "a@one.com", "m@two.com"];
    expect(selectSeedReceivers(pool)).toEqual(selectSeedReceivers([...pool].reverse()));
  });

  it("caps the receiver set", () => {
    const many = Array.from({ length: 30 }, (_, i) => `a@d${i}.com`);
    expect(selectSeedReceivers(many)).toHaveLength(MAX_SEED_RECEIVERS);
    expect(selectSeedReceivers(many, 3)).toHaveLength(3);
  });

  it("skips an address with no domain rather than inventing one", () => {
    expect(selectSeedReceivers(["not-an-email", "ok@d.com"]).map((r) => r.email)).toEqual([
      "ok@d.com",
    ]);
  });
});

describe("espForReceiver", () => {
  it("recognises the consumer ESPs by name", () => {
    expect(espForReceiver("x@gmail.com")).toBe(ESP_GOOGLE);
    expect(espForReceiver("x@outlook.com")).toBe(ESP_OUTLOOK);
    expect(espForReceiver("x@hotmail.com")).toBe(ESP_OUTLOOK);
  });

  it("falls back for a custom domain — our fleet is Workspace, so Google", () => {
    expect(espForReceiver("x@growdistribute.com")).toBe(ESP_GOOGLE);
  });

  it("domainOf lowercases and trims", () => {
    expect(domainOf("  A@Example.COM ")).toBe("example.com");
  });
});

describe("planSeedSends", () => {
  it("NEVER pairs a mailbox with itself — a local delivery crosses no filter", () => {
    const receivers = selectSeedReceivers(["a@one.com", "b@two.com"]);
    const plan = planSeedSends(["a@one.com", "c@three.com"], receivers);

    expect(plan.some((p) => p.senderEmail === p.receiverEmail)).toBe(false);
    expect(plan).toHaveLength(3); // a→b only; c→a and c→b
  });

  it("carries the receiver's ESP onto each planned send", () => {
    const receivers = selectSeedReceivers(["r@outlook.com"]);
    const [send] = planSeedSends(["s@one.com"], receivers);
    expect(send.recipientEsp).toBe(ESP_OUTLOOK);
  });
});

describe("classifySeedFolder", () => {
  it("maps the inbox and the spam folders", () => {
    expect(classifySeedFolder("INBOX")).toBe("inbox");
    expect(classifySeedFolder("[Gmail]/Spam")).toBe("spam");
    expect(classifySeedFolder("Junk")).toBe("spam");
    expect(classifySeedFolder("Junk E-mail")).toBe("spam");
  });

  it("returns null for a folder that carries NO verdict, incl. All Mail", () => {
    // All Mail mirrors both inbox and spam — classifying it would double-count.
    expect(classifySeedFolder("[Gmail]/All Mail")).toBeNull();
    expect(classifySeedFolder("[Gmail]/Sent Mail")).toBeNull();
    expect(classifySeedFolder("Drafts")).toBeNull();
    expect(classifySeedFolder("SomeUserLabel")).toBeNull();
  });
});

describe("parseAuthResults", () => {
  it("reads pass/fail per mechanism", () => {
    const header = "mx.google.com; dkim=pass header.i=@d.com; spf=pass smtp.mailfrom=d.com; dmarc=fail";
    expect(parseAuthResults(header)).toEqual({
      spfPass: true,
      dkimPass: true,
      dmarcPass: false,
    });
  });

  it("reports an ABSENT mechanism as null, not false", () => {
    // "the receiver did not report on SPF" is not "SPF failed".
    expect(parseAuthResults("mx.google.com; dkim=pass")).toEqual({
      spfPass: null,
      dkimPass: true,
      dmarcPass: null,
    });
    expect(parseAuthResults(null)).toEqual({
      spfPass: null,
      dkimPass: null,
      dmarcPass: null,
    });
  });
});

describe("aggregateSeedPlacement", () => {
  const testedAt = new Date("2026-08-29T08:00:00.000Z");

  const dispatch = (messageId: string, senderEmail = "s@one.com", recipientEsp = ESP_GOOGLE) => ({
    messageId,
    senderEmail,
    recipientEsp,
  });

  const observation = (
    messageId: string,
    placement: "inbox" | "spam",
  ) => ({ messageId, placement, spfPass: true, dkimPass: true, dmarcPass: true });

  it("counts a dispatched-but-unobserved seed as MISSING, never as inbox", () => {
    const rows = aggregateSeedPlacement(
      [dispatch("m1"), dispatch("m2"), dispatch("m3"), dispatch("m4")],
      [observation("m1", "inbox"), observation("m2", "spam")],
      "seed:t",
      testedAt,
    );

    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      accountEmail: "s@one.com",
      seedTotal: 4,
      inboxCount: 1,
      spamCount: 1,
      missingCount: 2,
      inboxPct: 25,
      spamPct: 25,
      missingPct: 50,
    });
  });

  it("keeps the DENOMINATOR on the dispatch side — a mailbox whose every seed vanished scores 0%, not null", () => {
    // Deriving seedTotal from the observations would give 0/0 → no row at all →
    // "untested", which would promote a black hole to a passing grade.
    const rows = aggregateSeedPlacement(
      [dispatch("m1"), dispatch("m2")],
      [],
      "seed:t",
      testedAt,
    );

    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ seedTotal: 2, inboxCount: 0, missingCount: 2, inboxPct: 0 });
  });

  it("groups per (sender, ESP)", () => {
    const rows = aggregateSeedPlacement(
      [
        dispatch("m1", "a@x.com", ESP_GOOGLE),
        dispatch("m2", "a@x.com", ESP_OUTLOOK),
        dispatch("m3", "b@x.com", ESP_GOOGLE),
      ],
      [observation("m1", "inbox"), observation("m2", "spam"), observation("m3", "inbox")],
      "seed:t",
      testedAt,
    );

    expect(rows).toHaveLength(3);
    expect(rows.find((r) => r.accountEmail === "a@x.com" && r.recipientEsp === ESP_OUTLOOK))
      .toMatchObject({ spamCount: 1, spamPct: 100 });
  });

  it("stamps every row with the test id and tested-at it was given", () => {
    const [row] = aggregateSeedPlacement([dispatch("m1")], [], "seed:abc", testedAt);
    expect(row.testId).toBe("seed:abc");
    expect(row.testedAt).toBe(testedAt);
  });

  it("AND-folds auth flags across the observed seeds, null when none reported", () => {
    const rows = aggregateSeedPlacement(
      [dispatch("m1"), dispatch("m2")],
      [
        { messageId: "m1", placement: "inbox", spfPass: true, dkimPass: true, dmarcPass: null },
        { messageId: "m2", placement: "inbox", spfPass: false, dkimPass: true, dmarcPass: null },
      ],
      "seed:t",
      testedAt,
    );

    expect(rows[0]).toMatchObject({ spfPass: false, dkimPass: true, dmarcPass: null });
  });

  it("produces no rows at all when nothing was dispatched — never a fabricated zero", () => {
    expect(aggregateSeedPlacement([], [observation("ghost", "inbox")], "seed:t", testedAt))
      .toEqual([]);
  });
});

describe("manual mailbox credentials", () => {
  const entries = parseManualCredentials(
    JSON.stringify([
      {
        address: "KLourd@PressBeat.ai",
        appPassword: "abcd efgh ijkl mnop",
        smtpHost: "mail.gandi.net",
        imapHost: "mail.gandi.net",
      },
    ]),
  );

  it("normalises the address and strips app-password spacing", () => {
    expect(entries[0]).toEqual({
      address: "klourd@pressbeat.ai",
      appPassword: "abcdefghijklmnop",
      smtpHost: "mail.gandi.net",
      imapHost: "mail.gandi.net",
    });
  });

  it("matches case-insensitively", () => {
    expect(selectManualCredential("KLOURD@pressbeat.ai", entries)?.imapHost).toBe(
      "mail.gandi.net",
    );
  });

  it("returns null for an address it does not hold, so Primeforge can answer", () => {
    expect(selectManualCredential("other@primeforge.com", entries)).toBeNull();
  });

  it("fails loud on a malformed entry rather than degrading to 'none configured'", () => {
    expect(() =>
      parseManualCredentials(JSON.stringify([{ address: "a@b.com", appPassword: "x" }])),
    ).toThrow(/smtpHost/);
    expect(() => parseManualCredentials("{}")).toThrow(/array/);
    expect(() => parseManualCredentials("not json")).toThrow(/valid JSON/);
  });
});

describe("buildSeedMessage", () => {
  const message = buildSeedMessage({
    testId: "seed:abc",
    senderEmail: "s@one.com",
    receiverEmail: "r@two.com",
  });

  it("carries the test id and sender in headers, for attribution without a DB read", () => {
    expect(message.headers[SEED_TEST_HEADER]).toBe("seed:abc");
    expect(message.headers[SEED_SENDER_HEADER]).toBe("s@one.com");
  });

  it("does NOT drag in the prospect-mail machinery", () => {
    // No unresolved Instantly merge variable, no signature separator, no anchors
    // — a seed must measure the mailbox, not the footer.
    expect(message.html).not.toContain("{unsubscribe_link}");
    expect(message.html).not.toContain("<p>--</p>");
    expect(message.html).not.toContain("<a ");
  });

  it("keeps the subject free of machine-generated markers", () => {
    expect(message.subject).not.toContain("seed:");
    expect(message.subject).not.toMatch(/[0-9a-f]{8}-[0-9a-f]{4}/);
  });
});

describe("loadSeedCredentialResolver", () => {
  const caller = { method: "POST", path: "/test" };

  const manualPayload = JSON.stringify([
    {
      address: "klourd@pressbeat.ai",
      appPassword: "gandi-pass",
      smtpHost: "mail.gandi.net",
      imapHost: "mail.gandi.net",
    },
  ]);

  beforeEach(() => {
    vi.resetAllMocks();
    mocks.listPrimeforgeRawMailboxes.mockResolvedValue([
      { address: "amy@growdistribute.com", appPassword: "abcd efgh ijkl mnop" },
    ]);
  });

  it("resolves a Primeforge mailbox, and prefers the manual entry when both exist", async () => {
    mocks.resolvePlatformKey.mockImplementation(async (provider: string) =>
      provider === MANUAL_CREDENTIALS_PROVIDER ? manualPayload : "pf-key",
    );

    const resolve = await loadSeedCredentialResolver(caller);

    expect(resolve("amy@growdistribute.com")).toMatchObject({
      appPassword: "abcdefghijklmnop",
      smtpHost: "smtp.gmail.com",
    });
    // The Gandi mailbox exists ONLY in the manual list — without it,
    // klourd@pressbeat.ai becomes unmeasurable the moment the paid test stops.
    expect(resolve("klourd@pressbeat.ai")).toMatchObject({
      appPassword: "gandi-pass",
      imapHost: "mail.gandi.net",
    });
  });

  it("returns null — not a throw — for a mailbox no source hosts", async () => {
    mocks.resolvePlatformKey.mockImplementation(async (provider: string) =>
      provider === MANUAL_CREDENTIALS_PROVIDER ? manualPayload : "pf-key",
    );

    const resolve = await loadSeedCredentialResolver(caller);
    expect(resolve("legacy@growthagency.cloud")).toBeNull();
  });

  it("treats an ABSENT manual key as 'none configured', and still resolves Primeforge", async () => {
    mocks.resolvePlatformKey.mockImplementation(async (provider: string) => {
      if (provider === MANUAL_CREDENTIALS_PROVIDER) {
        throw new KeyServiceError(404, "not found");
      }
      return "pf-key";
    });

    const resolve = await loadSeedCredentialResolver(caller);
    expect(resolve("amy@growdistribute.com")).not.toBeNull();
    expect(resolve("klourd@pressbeat.ai")).toBeNull();
  });

  it("FAILS LOUD when the manual key exists but cannot be read", async () => {
    // 401/5xx means we could not READ the credentials — a different fact from
    // there being none, and degrading it to 'none' would silently unmeasure a
    // mailbox that has a perfectly good password on file.
    mocks.resolvePlatformKey.mockImplementation(async (provider: string) => {
      if (provider === MANUAL_CREDENTIALS_PROVIDER) {
        throw new KeyServiceError(401, "unauthorized");
      }
      return "pf-key";
    });

    await expect(loadSeedCredentialResolver(caller)).rejects.toThrow(/unauthorized/);
  });
});
