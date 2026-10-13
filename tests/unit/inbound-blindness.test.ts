import { describe, expect, it, vi } from "vitest";

const mockExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...args: unknown[]) => mockExecute(...args) },
}));

import {
  INBOUND_BLIND_MIN_SENT,
  loadAddressInboundRows,
  mailboxResolver,
  selectInboundBlindMailboxes,
} from "../../src/lib/self-send/inbound-blindness";

// PRESSBEAT-INBOUND-1013: a Gandi forward on the mailbox's own address
// redirected every message away; the aliases sent thousands of emails and the
// INBOX received one message in five months. Nothing failed, so nothing said so.
const logins = new Map([
  ["kevin@pressbeat.ai", "kevin@pressbeat.ai"],
  ["klourd@pressbeat.ai", "kevin@pressbeat.ai"],
  ["lourd@pressbeat.ai", "kevin@pressbeat.ai"],
]);
const mailboxOf = mailboxResolver(logins);

describe("selectInboundBlindMailboxes", () => {
  it("flags a mailbox that sent and received NOTHING, summing its aliases", () => {
    const blind = selectInboundBlindMailboxes(
      [
        { accountEmail: "klourd@pressbeat.ai", sent: 12, hasInbound: false },
        { accountEmail: "lourd@pressbeat.ai", sent: 10, hasInbound: false },
      ],
      mailboxOf,
    );
    expect([...blind]).toEqual(["kevin@pressbeat.ai"]);
  });

  it("one inbound message on ANY alias proves the shared INBOX is read", () => {
    const blind = selectInboundBlindMailboxes(
      [
        { accountEmail: "klourd@pressbeat.ai", sent: 60, hasInbound: false },
        { accountEmail: "lourd@pressbeat.ai", sent: 0, hasInbound: true },
      ],
      mailboxOf,
    );
    expect(blind.size).toBe(0);
  });

  it("does not judge a mailbox that has barely sent", () => {
    const blind = selectInboundBlindMailboxes(
      [{ accountEmail: "new@fresh.com", sent: INBOUND_BLIND_MIN_SENT - 1, hasInbound: false }],
      mailboxOf,
    );
    expect(blind.size).toBe(0);
  });

  it("an address the login map does not know is its own mailbox, case-folded", () => {
    const blind = selectInboundBlindMailboxes(
      [{ accountEmail: "Kevin@GrowthAgency.ch", sent: 122, hasInbound: false }],
      mailboxOf,
    );
    expect([...blind]).toEqual(["kevin@growthagency.ch"]);
  });
});

// Prod 2026-10-13: a correlated EXISTS per sending address re-walked the whole
// 14-day inbound window 120 times (87 s) on the POST /orgs/send path; every send
// took minutes and the gateway gave up. The read must be ONE pass, joined.
describe("loadAddressInboundRows", () => {
  it("reads the inbound window ONCE and joins it, never a correlated EXISTS per address", async () => {
    mockExecute.mockReset().mockResolvedValue({
      rows: [
        { accountEmail: "klourd@pressbeat.ai", sent: 22, hasInbound: false },
        { accountEmail: "paisley@ariacoreco.com", sent: 40, hasInbound: true },
      ],
    });

    const rows = await loadAddressInboundRows();

    expect(rows).toEqual([
      { accountEmail: "klourd@pressbeat.ai", sent: 22, hasInbound: false },
      { accountEmail: "paisley@ariacoreco.com", sent: 40, hasInbound: true },
    ]);
    const { PgDialect } = await import("drizzle-orm/pg-core");
    const text = new PgDialect().sqlToQuery(mockExecute.mock.calls[0]![0] as never).sql;
    expect(text).not.toMatch(/EXISTS/i);
    expect(text).toMatch(/SELECT DISTINCT account_email\s+FROM imap_messages_raw/);
    expect(text).toMatch(/LEFT JOIN inbound/);
    expect(text).toMatch(/NOT \(payload \? 'recoveredFrom'\)/);
  });
});
