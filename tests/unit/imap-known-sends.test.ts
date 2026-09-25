import { describe, it, expect, vi, beforeEach } from "vitest";

const mockDbExecute = vi.fn();
/** Bronze rows the poll wrote, in order — the record of what it chose to keep. */
const mockInserted: Array<Record<string, unknown>> = [];
vi.mock("../../src/db", () => ({
  db: {
    execute: (...a: unknown[]) => mockDbExecute(...a),
    insert: () => ({
      values: (v: Record<string, unknown>) => {
        mockInserted.push(v);
        return {
          onConflictDoNothing: () => ({
            returning: async () => [{ id: `row-${mockInserted.length}` }],
          }),
        };
      },
    }),
  },
}));
vi.mock("../../src/lib/silver-promote", () => ({ promoteEvent: vi.fn() }));
const qualifyReplyMock = vi.fn(async () => null);
vi.mock("../../src/lib/self-send/qualify-reply", () => ({
  qualifyReply: (...a: unknown[]) => qualifyReplyMock(...(a as [])),
}));
// The poll groups its accounts by real mailbox login before reading any of them,
// so the credential map is on the path of every run — including one with no
// accounts at all. Mocked partially: the rest of the module (the IMAP port, the
// per-mailbox resolution) is the real thing.
const mockMailboxLogins = vi.fn(async () => new Map<string, string>());
const mockResolveCredential = vi.fn();
vi.mock("../../src/lib/self-send/mailbox-credentials", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  loadMailboxLogins: (...a: unknown[]) => mockMailboxLogins(...(a as [])),
  resolveMailboxCredential: (...a: unknown[]) =>
    mockResolveCredential(...(a as [string])),
}));

import { loadKnownSends } from "../../src/lib/self-send/imap-poller";

function pgResult<T>(rows: T[]) {
  return { command: "SELECT", rowCount: rows.length, oid: null, fields: [], rows };
}

beforeEach(() => {
  vi.resetAllMocks();
});

/**
 * The correlation key on a mailbox whose sequences INSTANTLY sent.
 *
 * Without this second source there is no `smtp_dispatch_raw` row to match, so
 * every reply on such a mailbox classifies `unrelated` and touches nothing —
 * which is exactly how a "I would be interested" reply reached a customer's
 * personal Gmail and nothing else in the fleet.
 */
describe("loadKnownSends", () => {
  it("unions our own dispatches with what Instantly sent from the same mailbox", async () => {
    mockDbExecute
      .mockResolvedValueOnce(
        pgResult([
          {
            messageId: "<ours@boostdistribute.com>",
            instantlyCampaignId: "self:aaa",
            leadEmail: "a@x.com",
            step: 1,
          },
        ]),
      )
      .mockResolvedValueOnce(
        pgResult([
          {
            messageId: "<theirs@marketingagency.network>",
            instantlyCampaignId: "e1e216ca-635a-4682-92be-f5057f8224ea",
            leadEmail: "jason@uhmedical.com",
            rawStep: "0_1_0",
          },
        ]),
      );

    const sends = await loadKnownSends("kevin.lourd@marketingagency.network");

    expect(sends.size).toBe(2);
    expect(sends.get("<theirs@marketingagency.network>")).toEqual({
      instantlyCampaignId: "e1e216ca-635a-4682-92be-f5057f8224ea",
      leadEmail: "jason@uhmedical.com",
      // Instantly's `0_1_0` is our step 2.
      step: 2,
      // Carried so a reply can pause the sequence on Instantly's side — the
      // mirror row's campaign join supplies it.
      orgId: null,
    });
  });

  it("drops an Instantly send whose step cannot be read rather than defaulting it", async () => {
    // A wrong step makes the inference rule project an `email_sent` for a step
    // nobody sent.
    mockDbExecute.mockResolvedValueOnce(pgResult([])).mockResolvedValueOnce(
      pgResult([
        {
          messageId: "<x@y.com>",
          instantlyCampaignId: "c1",
          leadEmail: "a@x.com",
          rawStep: null,
        },
      ]),
    );

    const sends = await loadKnownSends("kevin@x.com");

    expect(sends.size).toBe(0);
  });

  it("lets our OWN dispatch win a message-id collision", async () => {
    mockDbExecute
      .mockResolvedValueOnce(
        pgResult([
          { messageId: "<dup@x.com>", instantlyCampaignId: "self:aaa", leadEmail: "a@x.com", step: 3 },
        ]),
      )
      .mockResolvedValueOnce(
        pgResult([
          { messageId: "<dup@x.com>", instantlyCampaignId: "other", leadEmail: "b@x.com", rawStep: "0_0_0" },
        ]),
      );

    const sends = await loadKnownSends("kevin@x.com");

    expect(sends.get("<dup@x.com>")).toMatchObject({ instantlyCampaignId: "self:aaa", step: 3 });
  });

  it("keys on Instantly's own message_id — never on the sender address", async () => {
    // The prospect frequently replies from a DIFFERENT address than the one we
    // mailed, and the same prospect can sit in two sequences, so an address
    // match is wrong in both directions.
    mockDbExecute.mockResolvedValue(pgResult([]));

    await loadKnownSends("kevin@x.com");

    const instantlyQuery = JSON.stringify(mockDbExecute.mock.calls[1][0]);
    expect(instantlyQuery).toContain("instantly_emails_raw");
    expect(instantlyQuery).toContain("message_id");
    expect(instantlyQuery).toContain("eaccount");
    expect(instantlyQuery).toContain("ue_type");
    expect(instantlyQuery).not.toContain("from_address_email");
  });
});

/**
 * The catch-up window.
 *
 * A mailbox nobody has ever read holds replies far older than the routine
 * 3-day window, and for those mailboxes the mail exists nowhere else — Instantly
 * could not log in either, so its Unibox never mirrored them.
 */
describe("runPoll window", () => {
  it("clamps sinceDays into [routine, max] and never below the routine window", async () => {
    const { runPoll } = await import("../../src/lib/self-send/imap-poller");
    mockDbExecute.mockResolvedValue(pgResult([]));

    // No accounts on the transport, so the run is a no-op — what is under test
    // is that an absurd or hostile value cannot widen or narrow it wrongly.
    await expect(runPoll({ sinceDays: 100_000 })).resolves.toMatchObject({
      accountsPolled: 0,
    });
    await expect(runPoll({ sinceDays: -5 })).resolves.toMatchObject({ accountsPolled: 0 });
    await expect(runPoll({ sinceDays: Number.NaN })).resolves.toMatchObject({
      accountsPolled: 0,
    });
  });
});

describe("runPoll fan-out", () => {
  it("reads distinct mailboxes in parallel and a mailbox's aliases in sequence", async () => {
    // ⚠️ The unit is the SASL login, not the sending address. A Gandi domain is
    // one mailbox behind several aliases, so a flat fan-out over accounts would
    // open several simultaneous sessions as the SAME user — which is what a
    // relay refuses. Sequentially over 249 accounts this poll measured 45-80
    // MINUTES in production, and it sits on the critical path of every send.
    const { runPoll } = await import("../../src/lib/self-send/imap-poller");

    mockDbExecute.mockResolvedValue(
      pgResult([
        { email: "kevin@molthost.org" },
        { email: "klourd@molthost.org" },
        { email: "amy@saviolabsco.com" },
      ]),
    );
    mockMailboxLogins.mockResolvedValue(
      new Map([
        ["kevin@molthost.org", "kevin@molthost.org"],
        ["klourd@molthost.org", "kevin@molthost.org"],
        ["amy@saviolabsco.com", "amy@saviolabsco.com"],
      ]),
    );

    const live = new Map<string, number>();
    const peak = new Map<string, number>();
    mockResolveCredential.mockImplementation(async (address: string) => {
      const login = address.endsWith("@molthost.org")
        ? "kevin@molthost.org"
        : address;
      const now = (live.get(login) ?? 0) + 1;
      live.set(login, now);
      peak.set(login, Math.max(peak.get(login) ?? 0, now));
      await new Promise((r) => setTimeout(r, 5));
      live.set(login, (live.get(login) ?? 1) - 1);
      // Refusing here keeps the test about the SCHEDULING: each account is
      // counted as failed, never as read, and the sweep continues.
      throw new Error("no IMAP in a unit test");
    });

    const summary = await runPoll();

    expect(summary.accountsFailed).toBe(3);
    // Never two sessions as one login...
    expect(peak.get("kevin@molthost.org")).toBe(1);
    // ...and the two mailboxes genuinely overlapped rather than queueing.
    expect(peak.get("amy@saviolabsco.com")).toBe(1);
    expect(mockResolveCredential).toHaveBeenCalledTimes(3);
  });
});

/**
 * ⚠️ THE POLL FETCHES HEADERS, NOT SOURCE.
 *
 * `source: true` on the window fetch downloads and MIME-parses the FULL BODY of
 * every message in it, on every mailbox, on every run — measured 2026-09-18,
 * 67,098 messages across 214 mailboxes, 314 bodies per mailbox, of which 80,697
 * of 80,891 over three days were ordinary mail we ignore. One sweep then took
 * ~54 minutes of every hour, and because it is awaited INSIDE `runDispatch`
 * (holding the mutex) the dispatcher's 10-minute interval was really one run an
 * hour: sends arrived in hourly bursts with dead hours between them.
 *
 * Only a DSN (whose quoted headers name the bounced message) and a message that
 * correlates to one of our sends earn a body.
 */
describe("runPoll body fetching", () => {
  function imapStub(messages: Array<{ uid: number; headers: string }>) {
    const fetchCalls: unknown[] = [];
    const fetchOneCalls: number[] = [];
    return {
      client: {
        connect: async () => {},
        getMailboxLock: async () => ({ release: () => {} }),
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        fetch: (_range: unknown, query: any) => {
          fetchCalls.push(query);
          return (async function* () {
            for (const m of messages) {
              yield { uid: m.uid, headers: Buffer.from(m.headers) };
            }
          })();
        },
        fetchOne: async (uid: string) => {
          fetchOneCalls.push(Number(uid));
          return { source: Buffer.from("Subject: x\r\n\r\nbody text") };
        },
        logout: async () => {},
      },
      fetchCalls,
      fetchOneCalls,
    };
  }

  async function poll(
    messages: Array<{ uid: number; headers: string }>,
    knownSends: Array<Record<string, unknown>> = [],
  ) {
    vi.resetModules();
    const stub = imapStub(messages);
    vi.doMock("../../src/lib/self-send/imap-client", () => ({
      createImapClient: () => stub.client,
    }));
    const { runPoll } = await import("../../src/lib/self-send/imap-poller");

    // account list, then the two loadKnownSends reads, then everything else.
    mockDbExecute.mockResolvedValueOnce(pgResult([{ email: "kevin@live.com" }]));
    mockDbExecute.mockResolvedValueOnce(pgResult(knownSends));
    mockDbExecute.mockResolvedValue(pgResult([]));
    mockMailboxLogins.mockResolvedValue(new Map([["kevin@live.com", "kevin@live.com"]]));
    mockResolveCredential.mockResolvedValue({
      address: "kevin@live.com",
      appPassword: "pw",
      smtpHost: "smtp.gmail.com",
      imapHost: "imap.gmail.com",
    });

    mockInserted.length = 0;
    const summary = await runPoll();
    return { summary, inserted: mockInserted, ...stub };
  }

  it("asks the server for headers, never for the whole message", async () => {
    const { fetchCalls } = await poll([
      { uid: 1, headers: "Message-ID: <a@x.com>\r\nFrom: someone@x.com\r\nSubject: hi\r\n" },
    ]);

    expect(fetchCalls).toHaveLength(1);
    expect(fetchCalls[0]).toMatchObject({ headers: true, uid: true });
    expect(fetchCalls[0]).not.toHaveProperty("source");
  });

  it("never downloads the body of a message that is not ours", async () => {
    // A newsletter: real mail, on a real mailbox, referencing nothing we sent.
    const { summary, fetchOneCalls, inserted } = await poll([
      { uid: 1, headers: "Message-ID: <news@substack.com>\r\nFrom: a@substack.com\r\n" },
      { uid: 2, headers: "Message-ID: <alert@github.com>\r\nFrom: b@github.com\r\n" },
    ]);

    expect(summary.messagesRead).toBe(2);
    expect(summary.unrelated).toBe(2);
    expect(fetchOneCalls).toEqual([]);
    // The row still exists — it is the dedup key and the record of what we
    // ignored. Only the snippet is absent, and `null` says so rather than
    // claiming we read an empty body.
    expect(inserted).toHaveLength(2);
    for (const row of inserted) {
      expect((row.payload as { textSnippet: unknown }).textSnippet).toBeNull();
    }
  });

  it("DOES download the body of a reply to one of our sends", async () => {
    // The negative control above only proves the poll can decline to fetch. This
    // is the half that proves it still fetches when the words matter — without
    // it, code that never fetched at all would pass the suite.
    qualifyReplyMock.mockResolvedValue(null);
    const { summary, fetchOneCalls, inserted } = await poll(
      [
        {
          uid: 42,
          headers:
            "Message-ID: <their-reply@prospect.com>\r\nFrom: p@prospect.com\r\nIn-Reply-To: <ours@live.com>\r\n",
        },
      ],
      [
        {
          messageId: "<ours@live.com>",
          instantlyCampaignId: "self:abc",
          leadEmail: "p@prospect.com",
          step: 1,
          orgId: null,
        },
      ],
    );

    expect(summary.replies).toBe(1);
    expect(fetchOneCalls).toEqual([42]);
    expect((inserted[0]!.payload as { textSnippet: unknown }).textSnippet).toBe("body text");
  });

  it("downloads a DSN's body, because that is where the bounced id is quoted", async () => {
    const { fetchOneCalls } = await poll([
      {
        uid: 7,
        headers:
          "Message-ID: <dsn@x.com>\r\nFrom: MAILER-DAEMON@x.com\r\nContent-Type: multipart/report; report-type=delivery-status\r\n",
      },
    ]);

    expect(fetchOneCalls).toEqual([7]);
  });
});

/**
 * The inbox watcher's read: through the watcher's OWN session, by UID from the
 * last one seen. A new arrival must cost no fresh login and no window re-read.
 */
describe("pollMailboxGroup — reading a new arrival through the watched session", () => {
  async function group(query: unknown, shared: boolean) {
    vi.resetModules();
    const calls = { connect: 0, logout: 0, created: 0, fetch: [] as unknown[][] };
    const session = {
      connect: async () => {
        calls.connect += 1;
      },
      getMailboxLock: async () => ({ release: () => {} }),
      fetch: (...args: unknown[]) => {
        calls.fetch.push(args);
        return (async function* () {
          yield { uid: 57, headers: Buffer.from("Message-ID: <n@x.com>\r\nFrom: a@x.com\r\n") };
          yield { uid: 58, headers: Buffer.from("Message-ID: <m@x.com>\r\nFrom: b@x.com\r\n") };
        })();
      },
      fetchOne: async () => null,
      logout: async () => {
        calls.logout += 1;
      },
    };
    vi.doMock("../../src/lib/self-send/imap-client", () => ({
      createImapClient: () => {
        calls.created += 1;
        return session;
      },
    }));
    const { pollMailboxGroup } = await import("../../src/lib/self-send/imap-poller");
    mockDbExecute.mockResolvedValue(pgResult([]));
    mockInserted.length = 0;
    const credential = {
      address: "a@live.com",
      appPassword: "pw",
      smtpHost: "smtp.gmail.com",
      imapHost: "imap.gmail.com",
    };
    const result = await pollMailboxGroup(
      ["a@live.com", "b@live.com"],
      credential,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      query as any,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      shared ? (session as any) : undefined,
    );
    return { ...result, calls };
  }

  it("fetches by UID range on the shared session, and never logs in or out", async () => {
    const { calls, maxUid, summary } = await group({ uidFrom: 57 }, true);
    expect(calls.created).toBe(0);
    expect(calls.connect).toBe(0);
    expect(calls.logout).toBe(0);
    // Both aliases read, each by UID — a sequence number shifts on expunge.
    expect(calls.fetch).toHaveLength(2);
    expect(calls.fetch[0]![0]).toBe("57:*");
    expect(calls.fetch[0]![2]).toEqual({ uid: true });
    expect(maxUid).toBe(58);
    expect(summary.accountsPolled).toBe(2);
  });

  it("opens and closes its own session when none is shared (the fallback read)", async () => {
    const { calls } = await group({ since: new Date("2026-09-22T00:00:00Z") }, false);
    expect(calls.created).toBe(2);
    expect(calls.connect).toBe(2);
    expect(calls.logout).toBe(2);
    expect(calls.fetch[0]![0]).toEqual({ since: new Date("2026-09-22T00:00:00Z") });
  });
});
