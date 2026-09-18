import { describe, it, expect, vi, beforeEach } from "vitest";
import express from "express";
import request from "supertest";

const mockExecute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => mockExecute(...a) } }));
vi.mock("../../src/lib/mailboxes-sync", () => ({ syncMailboxes: vi.fn() }));
vi.mock("../../src/lib/messages-sync", () => ({ syncMessages: vi.fn() }));
vi.mock("../../src/lib/domain-dns-sync", () => ({ syncDomainDns: vi.fn(), summarizeDns: vi.fn() }));
vi.mock("../../src/lib/ops/account-health-read", () => ({ loadAccountHealth: vi.fn() }));
vi.mock("../../src/lib/infra-gold", () => ({ loadEffectiveRates: vi.fn(), loadInventoryDomains: vi.fn() }));
vi.mock("../../src/lib/account-lifecycle-sync", () => ({ fetchLatestDeliveryByAccount: vi.fn(), fetchLifecycleByEmail: vi.fn(), TESTABLE_MIN_AGE_DAYS: 7 }));
vi.mock("../../src/lib/recent-send-volume", () => ({ fetchRecentDailyVolume: vi.fn(), sustainedForMailbox: vi.fn() }));
vi.mock("../../src/lib/self-send/mailbox-credentials", () => ({ loadMailboxLogins: vi.fn() }));

import opsRoutes from "../../src/routes/ops";

function pgResult(rows: Record<string, unknown>[]) {
  return { command: "SELECT", rowCount: rows.length, oid: 0, fields: [], rows };
}

const app = express();
app.use(express.json());
app.use("/internal/ops", opsRoutes);

beforeEach(() => {
  vi.resetAllMocks();
  mockExecute.mockResolvedValue(pgResult([]));
});

describe("GET /internal/ops/lifecycle-rules", () => {
  it("serves the rules with no IO", async () => {
    const res = await request(app).get("/internal/ops/lifecycle-rules");
    expect(res.status).toBe(200);
    expect(res.body.bars.deliveryPctBar).toBe(90);
    expect(mockExecute).not.toHaveBeenCalled();
  });
});

describe("GET /internal/ops/threads — limit is REQUIRED, filters reach the SQL, cursor pages", () => {
  it("400s without a limit — there is no silent default over 140k threads", async () => {
    const res = await request(app).get("/internal/ops/threads");
    expect(res.status).toBe(400);
    expect(mockExecute).not.toHaveBeenCalled();
  });

  it("400s on a bad direction / timestamp / hasInbound", async () => {
    expect((await request(app).get("/internal/ops/threads?limit=10&direction=sideways")).status).toBe(400);
    expect((await request(app).get("/internal/ops/threads?limit=10&since=yesterday")).status).toBe(400);
    expect((await request(app).get("/internal/ops/threads?limit=10&hasInbound=maybe")).status).toBe(400);
  });

  it("threads the filters into the query and pages with an opaque cursor", async () => {
    const rows = Array.from({ length: 3 }, (_, i) => ({
      thread_id: `t${i}`, kind: "outreach", subject: "Hi", account_email: "a@x.com", mailbox_login: "a@x.com",
      counterparty: "p@y.com", transport: "smtp", instantly_campaign_id: `self:${i}`, org_id: "o", campaign_id: "c",
      lead_email: "p@y.com", brand_ids: ["b"], delivery_status: "sent", reply_classification: null, reply_kind: null,
      placement: null, message_count: 2, inbound_count: 0, outbound_count: 2,
      first_at: new Date("2026-09-01"), last_at: new Date(`2026-09-1${i}`),
    }));
    mockExecute.mockResolvedValueOnce(pgResult(rows));
    const res = await request(app).get("/internal/ops/threads?limit=2&kind=outreach&mailbox=A@x.com&hasInbound=false");
    expect(res.status).toBe(200);
    expect(res.body.threads).toHaveLength(2);
    expect(res.body.nextCursor).toBeTypeOf("string");
    const q = JSON.stringify((mockExecute.mock.calls[0][0] as { queryChunks?: unknown[] }).queryChunks);
    expect(q).toContain("m.kind =");
    expect(q).toContain("a@x.com");
    expect(q).toContain("t.inbound_count = 0");
    expect(q).toContain("GROUP BY m.thread_id");
  });
});

describe("GET /internal/ops/messages/:id/body", () => {
  it("404s an unknown message", async () => {
    const res = await request(app).get("/internal/ops/messages/nope/body");
    expect(res.status).toBe(404);
  });

  it("reads an Instantly body from the Unibox mirror", async () => {
    mockExecute
      .mockResolvedValueOnce(pgResult([{ source_table: "instantly_emails_raw", source_row_id: "r1" }]))
      .mockResolvedValueOnce(pgResult([{ text: "hello", html: "<p>hello</p>" }]));
    const res = await request(app).get("/internal/ops/messages/m1/body");
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ text: "hello", html: "<p>hello</p>", source: "instantly_emails_raw" });
  });
});
