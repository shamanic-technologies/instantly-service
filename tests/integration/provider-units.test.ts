/**
 * DB-backed proof of the Provider reads of our Units (ENGINE-GET-ASK-1123):
 * migration 0116 applies; the sent read returns a person's steps and thread
 * answer newest first with kinds / bounce; the queued read returns the queue's
 * states; address, lead id and company domain each find the person; `since`
 * and paging work; another org's Units never come back.
 *
 * Own pool, applies every migration. Skipped when no database is configured:
 * `INSTANTLY_SERVICE_DATABASE_URL=postgres://postgres@localhost:5432/x npx vitest run <file>`.
 */
import path from "node:path";
import { describe, it, expect, beforeAll, afterAll, vi } from "vitest";

const SKIP = !process.env.INSTANTLY_SERVICE_DATABASE_URL;

const { pool } = vi.hoisted(() => {
  const url = process.env.INSTANTLY_SERVICE_DATABASE_URL;
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const { Pool: P } = require("pg");
  return { pool: url ? new P({ connectionString: url }) : null };
});
vi.mock("../../src/db", async () => {
  const { drizzle: d } = await import("drizzle-orm/node-postgres");
  return { db: pool ? d(pool) : null };
});

const { fetchEmailSentUnits, fetchEmailQueuedUnits, parseUnitsQuery } = await import("../../src/lib/provider-units");

const SUFFIX = crypto.randomUUID().slice(0, 8);
const ORG_A = `org-a-${SUFFIX}`;
const ORG_B = `org-b-${SUFFIX}`;
const BRAND_1 = crypto.randomUUID();
const BRAND_2 = crypto.randomUUID();
const DOMAIN = `acme-${SUFFIX}.test`;
const JANE = `Jane@${DOMAIN}`;
const jane = JANE.toLowerCase();
const BOB = `bob@${DOMAIN}`;
const LEAD_JANE = `lead-jane-${SUFFIX}`;
const q = (text: string, params: unknown[] = []) => pool!.query(text, params);

const sequences: string[] = [];

async function sequence(over: {
  org: string;
  email: string;
  leadId?: string | null;
  brand?: string;
  campaign?: string | null;
  followUp?: boolean;
  metadata?: unknown;
}): Promise<string> {
  const icid = `self:${crypto.randomUUID()}`;
  sequences.push(icid);
  await q(
    `INSERT INTO instantly_campaigns (id, campaign_id, lead_email, instantly_campaign_id, name, status, org_id, brand_ids, run_id, lead_id, follow_up, metadata)
     VALUES ($1, $2, $3, $4, 'n', 'active', $5, $6, $7, $8, $9, $10)`,
    [
      crypto.randomUUID(),
      over.campaign === undefined ? `camp-${SUFFIX}` : over.campaign,
      over.email,
      icid,
      over.org,
      [over.brand ?? BRAND_1],
      `run-${crypto.randomUUID()}`,
      over.leadId ?? null,
      over.followUp ?? false,
      over.metadata === undefined ? null : JSON.stringify(over.metadata),
    ],
  );
  return icid;
}

async function event(icid: string, email: string, type: string, step: number, at: string, withdrawn = false): Promise<string> {
  const id = crypto.randomUUID();
  await q(
    `INSERT INTO instantly_events (id, event_type, campaign_id, lead_email, step, "timestamp", withdrawn_at)
     VALUES ($1, $2, $3, $4, $5, $6::timestamp, $7)`,
    [id, type, icid, email, step, at, withdrawn ? new Date() : null],
  );
  return id;
}

async function hold(icid: string | null, campaign: string | null, email: string, step: number, status: string, at: string, declared: string | null = null) {
  const id = crypto.randomUUID();
  await q(
    `INSERT INTO sequence_costs (id, campaign_id, instantly_campaign_id, lead_email, step, run_id, status, created_at, queued_declaration)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8::timestamp, $9)`,
    [id, campaign, icid, email, step, `steprun-${crypto.randomUUID()}`, status, at, declared],
  );
  return id;
}

const sent = (query: Record<string, unknown>, org = ORG_A) => fetchEmailSentUnits(parseUnitsQuery("email_sent", org, query));
const queued = (query: Record<string, unknown>, org = ORG_A) => fetchEmailQueuedUnits(parseUnitsQuery("email_queued", org, query));

describe.skipIf(SKIP)("Provider reads of our Units (DB-backed)", () => {
  let seqJane: string;
  let seqJaneFollowUp: string;
  let seqBob: string;
  let seqOther: string;
  let step1: string;
  let step2: string;
  let answerId: string;
  let holdWaiting: string;
  let holdLegacy: string;

  beforeAll(async () => {
    const { drizzle } = await import("drizzle-orm/node-postgres");
    const { migrate } = await import("drizzle-orm/node-postgres/migrator");
    await migrate(drizzle(pool!), { migrationsFolder: path.join(__dirname, "..", "..", "drizzle") });

    // Org A, Jane: step 1 (Aug) + step 2 (Sep, bounced) + a withdrawn send + an answer (Oct).
    seqJane = await sequence({ org: ORG_A, email: JANE, leadId: LEAD_JANE });
    step1 = await event(seqJane, JANE, "email_sent", 1, "2026-08-10 09:00:00.111111");
    step2 = await event(seqJane, JANE, "email_sent", 2, "2026-09-13 09:00:00.222222");
    await event(seqJane, JANE, "email_bounced", 2, "2026-09-13 09:01:00");
    await event(seqJane, JANE, "email_sent", 3, "2026-09-20 09:00:00", true);
    await event(seqJane, JANE, "reply_received", 2, "2026-09-25 09:00:00");
    const answer = await q(
      `INSERT INTO reply_send_outcomes (org_id, instantly_campaign_id, lead_email, message_id, transport, sent_by, sent_at)
       VALUES ($1, $2, $3, $4, 'smtp', 'automation', '2026-10-01 10:00:00.333333') RETURNING id`,
      [ORG_A, seqJane, JANE, `<${crypto.randomUUID()}@x.test>`],
    );
    answerId = `answer:${answer.rows[0].id}`;
    await hold(seqJane, `camp-${SUFFIX}`, JANE, 1, "actual", "2026-08-10 08:00:00");
    await hold(seqJane, `camp-${SUFFIX}`, JANE, 2, "actual", "2026-08-10 08:00:00.5", "declared");
    await hold(seqJane, `camp-${SUFFIX}`, JANE, 3, "cancelled", "2026-08-10 08:00:01");
    // A hold written before sequence ids were stored: matched by campaign + address.
    holdLegacy = await hold(null, `camp-${SUFFIX}`, JANE, 4, "cancelled", "2026-07-01 08:00:00");

    // Org A, Jane again: a follow-up send on another brand, continuing the thread, still waiting.
    seqJaneFollowUp = await sequence({
      org: ORG_A,
      email: jane,
      leadId: LEAD_JANE,
      brand: BRAND_2,
      campaign: `camp2-${SUFFIX}`,
      followUp: true,
      metadata: { followUpOf: seqJane },
    });
    holdWaiting = await hold(seqJaneFollowUp, `camp2-${SUFFIX}`, jane, 1, "provisioned", "2026-11-20 08:00:00", "declared");

    // Org A, Bob at the same company, no lead id.
    seqBob = await sequence({ org: ORG_A, email: BOB, leadId: null });
    await event(seqBob, BOB, "email_sent", 1, "2026-11-01 09:00:00");

    // Org B wrote to Jane too: must never come back for org A.
    seqOther = await sequence({ org: ORG_B, email: JANE, leadId: LEAD_JANE, campaign: `campb-${SUFFIX}` });
    await event(seqOther, JANE, "email_sent", 1, "2026-11-10 09:00:00");
    await hold(seqOther, `campb-${SUFFIX}`, JANE, 1, "actual", "2026-11-10 08:00:00");
  });

  afterAll(async () => {
    if (!pool) return;
    await q(`DELETE FROM reply_send_outcomes WHERE instantly_campaign_id = ANY($1)`, [sequences]);
    await q(`DELETE FROM sequence_costs WHERE instantly_campaign_id = ANY($1) OR campaign_id = $2`, [sequences, `camp-${SUFFIX}`]);
    await q(`DELETE FROM instantly_events WHERE campaign_id = ANY($1)`, [sequences]);
    await q(`DELETE FROM instantly_campaigns WHERE instantly_campaign_id = ANY($1)`, [sequences]);
    await pool.end();
  });

  it("migration 0116 created the domain index", async () => {
    const { rows } = await q(`SELECT indexdef FROM pg_indexes WHERE indexname = 'instantly_campaigns_org_lead_domain_idx'`);
    expect(rows).toHaveLength(1);
    expect(rows[0].indexdef).toContain("split_part(lower(lead_email), '@'::text, 2)");
  });

  it("email-sent by address: the answer, then the steps, newest first; withdrawn and non-send events never", async () => {
    const page = await sent({ emails: JANE });
    expect(page.units.map((u) => u.unitId)).toEqual([answerId, step2, step1]);
    expect(page.nextCursor).toBeNull();
    expect(page.units[0]).toMatchObject({
      outcome: "email_sent",
      ledgerOutcome: "message_sent",
      kind: "answer",
      step: null,
      bounced: false,
      at: "2026-10-01T10:00:00.333Z",
      email: jane,
      domain: DOMAIN,
      subject: { service: "lead-service", id: LEAD_JANE },
      campaignId: `camp-${SUFFIX}`,
      brandIds: [BRAND_1],
      sequenceId: seqJane,
      continuesThread: false,
    });
    expect(page.units[1]).toMatchObject({ kind: "follow_up", step: 2, bounced: true, at: "2026-09-13T09:00:00.222Z" });
    expect(page.units[2]).toMatchObject({ kind: "first", step: 1, bounced: false });
  });

  it("since is inclusive, until exclusive; kinds narrow", async () => {
    expect((await sent({ emails: jane, since: "2026-09-13T09:00:00Z" })).units.map((u) => u.unitId)).toEqual([answerId, step2]);
    expect((await sent({ emails: jane, since: "2026-09-13T09:00:00.223Z" })).units.map((u) => u.unitId)).toEqual([answerId]);
    expect((await sent({ emails: jane, until: "2026-09-13T09:00:00Z" })).units.map((u) => u.unitId)).toEqual([step1]);
    // Bob's only email left at 2026-11-01 09:00:00 sharp: on the bound = in for since, out for until.
    expect((await sent({ emails: BOB, since: "2026-11-01T09:00:00Z" })).units).toHaveLength(1);
    expect((await sent({ emails: BOB, since: "2026-11-01T10:00:00+01:00" })).units).toHaveLength(1);
    expect((await sent({ emails: BOB, until: "2026-11-01T09:00:00Z" })).units).toEqual([]);
    expect((await sent({ emails: jane, since: "2026-12-01T00:00:00Z" })).units).toEqual([]);
    expect((await sent({ emails: jane, kinds: "first,answer" })).units.map((u) => u.kind)).toEqual(["answer", "first"]);
  });

  it("pages by cursor without dropping or repeating a Unit", async () => {
    const seen: string[] = [];
    let cursor: string | undefined;
    do {
      const page = await sent({ emails: jane, limit: "1", ...(cursor ? { cursor } : {}) });
      seen.push(...page.units.map((u) => u.unitId));
      cursor = page.nextCursor ?? undefined;
    } while (cursor);
    expect(seen).toEqual([answerId, step2, step1]);
  });

  it("the company domain finds everyone there; the lead id finds the person; no lead id = null subject", async () => {
    const byDomain = await sent({ domains: `@${DOMAIN.toUpperCase()}` });
    expect(byDomain.units.map((u) => u.email)).toEqual([BOB, jane, jane, jane]);
    expect(byDomain.units[0]!.subject).toBeNull();

    const byLead = await sent({ leadIds: LEAD_JANE });
    expect(byLead.units.map((u) => u.unitId)).toEqual([answerId, step2, step1]);

    const either = await sent({ leadIds: LEAD_JANE, emails: BOB });
    expect(either.units).toHaveLength(4);
  });

  it("brand and campaign narrow; an unknown person is an empty page", async () => {
    expect((await sent({ emails: jane, brandId: BRAND_2 })).units).toEqual([]);
    expect((await sent({ emails: jane, brandId: BRAND_1 })).units).toHaveLength(3);
    expect((await sent({ emails: jane, campaignId: `camp2-${SUFFIX}` })).units).toEqual([]);
    expect((await sent({ emails: `nobody-${SUFFIX}@nowhere.test` })).units).toEqual([]);
  });

  it("email-queued: every queued step with its state, the legacy hold included", async () => {
    const page = await queued({ emails: jane });
    expect(page.units.map((u) => [u.step, u.state, u.kind, u.declaredOnLedger, u.continuesThread])).toEqual([
      [1, "waiting", "follow_up", true, true],
      [3, "cancelled", "follow_up", false, false],
      [2, "sent", "follow_up", true, false],
      [1, "sent", "first", false, false],
      [4, "cancelled", "follow_up", false, false],
    ]);
    expect(page.units[0]).toMatchObject({
      unitId: holdWaiting,
      outcome: "email_queued",
      ledgerOutcome: "email_queued",
      sequenceId: seqJaneFollowUp,
      brandIds: [BRAND_2],
      at: "2026-11-20T08:00:00.000Z",
    });
    expect(page.units[4]!.unitId).toBe(holdLegacy);
    expect(page.units[0]).not.toHaveProperty("bounced");
  });

  it("email-queued: states, kinds and since narrow", async () => {
    expect((await queued({ emails: jane, states: "waiting" })).units.map((u) => u.unitId)).toEqual([holdWaiting]);
    expect((await queued({ emails: jane, states: "sent,cancelled" })).units).toHaveLength(4);
    expect((await queued({ emails: jane, kinds: "first" })).units.map((u) => u.step)).toEqual([1]);
    expect((await queued({ domains: DOMAIN, since: "2026-11-01T00:00:00Z" })).units.map((u) => u.unitId)).toEqual([holdWaiting]);
  });

  it("two rows of one step on one run (pre-0038 account + domain cost) are ONE Unit; another run of that step is a second", async () => {
    const carol = `carol@other-${SUFFIX}.test`;
    const seq = await sequence({ org: ORG_A, email: carol, campaign: `campc-${SUFFIX}` });
    const insert = async (runId: string, step: number, at: string) => {
      const id = crypto.randomUUID();
      await q(
        `INSERT INTO sequence_costs (id, campaign_id, instantly_campaign_id, lead_email, step, run_id, cost_id, status, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, 'actual', $8::timestamp)`,
        [id, `campc-${SUFFIX}`, seq, carol, step, runId, `cost-${crypto.randomUUID()}`, at],
      );
      return id;
    };
    const runA = `steprun-a-${SUFFIX}`;
    const runB = `steprun-b-${SUFFIX}`;
    const doubled = [await insert(runA, 1, "2026-08-01 08:00:00"), await insert(runA, 1, "2026-08-01 08:00:00")];

    const one = await queued({ emails: carol });
    expect(one.units).toHaveLength(1);
    expect(doubled).toContain(one.units[0]!.unitId);
    expect(one.units[0]).toMatchObject({ step: 1, state: "sent", kind: "first", sequenceId: seq });
    // Stable across reads: the same row stands for the step every time (a cursor never flips).
    expect((await queued({ emails: carol })).units[0]!.unitId).toBe(one.units[0]!.unitId);

    // The same step queued again on ANOTHER run (a re-queue) is its own Unit.
    const requeued = await insert(runB, 1, "2026-08-05 08:00:00");
    const two = await queued({ emails: carol });
    expect(two.units.map((u) => u.unitId)).toEqual([requeued, one.units[0]!.unitId]);

    // Paging over the folded rows neither drops nor repeats.
    const first = await queued({ emails: carol, limit: "1" });
    expect(first.units.map((u) => u.unitId)).toEqual([requeued]);
    const second = await queued({ emails: carol, limit: "1", cursor: first.nextCursor! });
    expect(second.units.map((u) => u.unitId)).toEqual([one.units[0]!.unitId]);
    expect(second.nextCursor).toBeNull();
  });

  it("another org's Units never come back", async () => {
    const mine = [...(await sent({ emails: jane, leadIds: LEAD_JANE, domains: DOMAIN })).units, ...(await queued({ emails: jane, leadIds: LEAD_JANE, domains: DOMAIN })).units];
    expect(mine.some((u) => u.sequenceId === seqOther)).toBe(false);

    const theirs = await sent({ emails: jane }, ORG_B);
    expect(theirs.units.map((u) => u.sequenceId)).toEqual([seqOther]);
    expect((await queued({ leadIds: LEAD_JANE }, ORG_B)).units.map((u) => u.sequenceId)).toEqual([seqOther]);
  });
});
