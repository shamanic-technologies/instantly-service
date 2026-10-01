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
import { clearStatsCache } from "../../src/lib/stats-cache";
import { mapSentPeriods } from "../../src/lib/ops/sent-per-period";

function pgResult(rows: Record<string, unknown>[]) {
  return { command: "SELECT", rowCount: rows.length, oid: 0, fields: [], rows };
}

const app = express();
app.use(express.json());
app.use("/internal/ops", opsRoutes);

beforeEach(() => {
  vi.resetAllMocks();
  mockExecute.mockResolvedValue(pgResult([]));
  clearStatsCache();
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

describe("GET /internal/ops/sent-per-period — sends per period by purpose", () => {
  const row = (start: string, end: string, inProgress: boolean, n: Partial<Record<string, unknown>> = {}) => ({
    period_start: start, period_end: end, in_progress: inProgress,
    to_leads: 0, manual_replies: 0, warmup: 0, warmup_replies: 0, seeds: 0, leads_emailed: 0, ...n,
  });

  it("400s without a grain, on an unknown grain, and on a bad since", async () => {
    expect((await request(app).get("/internal/ops/sent-per-period")).status).toBe(400);
    expect((await request(app).get("/internal/ops/sent-per-period?grain=year")).status).toBe(400);
    expect((await request(app).get("/internal/ops/sent-per-period?grain=day&since=yesterday")).status).toBe(400);
    expect(mockExecute).not.toHaveBeenCalled();
  });

  it("serves every period with purposes kept apart, zeros included, the current one in progress", async () => {
    mockExecute.mockResolvedValueOnce(pgResult([
      // node-postgres hands bigint/int counts back as strings on some paths
      row("2026-08-01", "2026-09-01", false, { to_leads: "22235", seeds: "1540", leads_emailed: "12178" }),
      row("2026-09-01", "2026-10-01", false, { to_leads: 40066, manual_replies: 33, warmup: 20465, warmup_replies: 8170, seeds: 7648, leads_emailed: 18664 }),
      row("2026-10-01", "2026-11-01", true),
    ]));
    const res = await request(app).get("/internal/ops/sent-per-period?grain=month");
    expect(res.status).toBe(200);
    expect(res.body.grain).toBe("month");
    expect(res.body.timezone).toBe("UTC");
    expect(res.body.since).toBeNull();
    expect(res.body.periods).toHaveLength(3);
    expect(res.body.periods[0]).toEqual({
      periodStart: "2026-08-01", periodEnd: "2026-09-01", inProgress: false,
      toLeads: 22235, manualReplies: 0, warmup: 0, warmupReplies: 0, seeds: 1540, leadsEmailed: 12178,
    });
    expect(res.body.periods[2]).toMatchObject({ inProgress: true, toLeads: 0 });
    expect(res.body.periods.filter((p: { inProgress: boolean }) => p.inProgress)).toHaveLength(1);
    expect(res.body.totals).toEqual({ toLeads: 62301, manualReplies: 33, warmup: 20465, warmupReplies: 8170, seeds: 9188 });
  });

  it("counts only SENT outbound rows per purpose, buckets in UTC, gap-fills with a series, and inlines only the whitelisted grain", async () => {
    await request(app).get("/internal/ops/sent-per-period?grain=week&since=2026-09-01T00:00:00Z");
    const q = JSON.stringify((mockExecute.mock.calls[0][0] as { queryChunks?: unknown[] }).queryChunks);
    expect(q).toContain("m.direction = 'out' AND m.outcome = 'sent'");
    expect(q).toContain("'outreach','manual_reply','warmup','warmup_reply','seed'");
    expect(q).toContain("interval '1 week'");
    expect(q).toContain("AT TIME ZONE 'UTC'");
    expect(q).toContain("generate_series");
    expect(q).toContain("LEFT JOIN counts");
    expect(q).toContain("2026-09-01T00:00:00.000Z");
  });

  it("caches per (grain, since): a second identical call does not re-query", async () => {
    await request(app).get("/internal/ops/sent-per-period?grain=day");
    await request(app).get("/internal/ops/sent-per-period?grain=day");
    expect(mockExecute).toHaveBeenCalledTimes(1);
    await request(app).get("/internal/ops/sent-per-period?grain=month");
    expect(mockExecute).toHaveBeenCalledTimes(2);
  });
});

describe("mapSentPeriods", () => {
  it("reads the in-progress flag in either driver spelling and coerces counts", () => {
    const out = mapSentPeriods(
      [{ period_start: "2026-10-01", period_end: "2026-10-02", in_progress: "t", to_leads: "3" }],
      { grain: "day", since: null, asOf: "x" },
    );
    expect(out.periods[0]).toMatchObject({ inProgress: true, toLeads: 3, warmup: 0 });
  });
});
