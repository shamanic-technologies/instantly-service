/**
 * DB-backed proof of EVERY-PERSON-ON-THE-THREAD-1109 on the real schema:
 *  - the triggers mark what our mailboxes send and receive; the drain versions it;
 *    the feed serves Markus's answer FROM Markus, filed on Thomas's thread, and the
 *    thread filter returns the whole thread;
 *  - plain `unrelated` noise is never marked; a refile re-serves the message (same id,
 *    higher version); five alias copies of one message are ONE message;
 *  - a reply can name Markus's message: findThreadMessage resolves Thomas's sequence.
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

const { drainMailboxMessages, readMailboxMessages, imapMessageKey } = await import("../../src/lib/mailbox-messages");
const { findThreadMessage } = await import("../../src/lib/reply-to-lead");
const { readOutreachFacts } = await import("../../src/lib/outreach-facts");
const { MARKUS_IMAP_ROW } = await import("../helpers/markus-reply");

const q = (text: string, params: unknown[] = []) => pool!.query(text, params);
const ORG = crypto.randomUUID();
const ICID = `self:${crypto.randomUUID()}`;
const MAILBOX = `kevin-${ICID.slice(5, 13)}@marketingagency.group`;
const OUR_FIRST = `<first-${ICID}@marketingagency.group>`;
const MARKUS_MSG = `<markus-${ICID}@mail.gmail.com>`;
const THREAD_KEY = OUR_FIRST;

async function drainAll() {
  for (let i = 0; i < 50; i += 1) {
    const d = await drainMailboxMessages(500);
    if (d.drained === 0) return;
  }
}

async function readAll() {
  const out = [];
  let after = "0";
  for (;;) {
    const page = await readMailboxMessages({ after, limit: 500 });
    out.push(...page.messages);
    after = page.nextCursor;
    if (!page.hasMore) return { messages: out, cursor: after };
  }
}

describe.skipIf(SKIP)("mailbox-messages feed (DB-backed)", () => {
  beforeAll(async () => {
    const { drizzle } = await import("drizzle-orm/node-postgres");
    const { migrate } = await import("drizzle-orm/node-postgres/migrator");
    await migrate(drizzle(pool!), { migrationsFolder: path.join(__dirname, "..", "..", "drizzle") });
    await drainAll();

    await q(
      `INSERT INTO instantly_campaigns (id, campaign_id, lead_email, instantly_campaign_id, name, status, org_id, user_id, brand_ids, account_email, send_transport, created_at, updated_at)
       VALUES ($1, 'camp-lv-' || $2, 'thomas@markthalle-luzern.ch', $2, 'n', 'active', $3, $4, $5, $6, 'smtp', now() - interval '1 day', now())`,
      [crypto.randomUUID(), ICID, ORG, crypto.randomUUID(), ["brand-lv"], MAILBOX],
    );
    await q(
      `INSERT INTO sequence_steps (id, instantly_campaign_id, step, subject, body_html) VALUES ($1, $2, 1, 'Flohsamenschalen für die Markthalle', '<p>Hallo Thomas</p>')`,
      [crypto.randomUUID(), ICID],
    );
    await q(
      `INSERT INTO smtp_dispatch_raw (id, instantly_campaign_id, lead_email, account_email, step, outcome, message_id, payload, dispatched_at)
       VALUES ($1, $2, 'thomas@markthalle-luzern.ch', $3, 1, 'sent', $4, '{}'::jsonb, now() - interval '2 hours')`,
      [crypto.randomUUID(), ICID, MAILBOX, OUR_FIRST],
    );
    // A refused attempt is never a message.
    await q(
      `INSERT INTO smtp_dispatch_raw (id, instantly_campaign_id, lead_email, account_email, step, outcome, message_id, payload)
       VALUES ($1, $2, 'thomas@markthalle-luzern.ch', $3, 2, 'transient', NULL, '{}'::jsonb)`,
      [crypto.randomUUID(), ICID, MAILBOX],
    );
    const payload = {
      ...MARKUS_IMAP_ROW.payload,
      headers: { ...MARKUS_IMAP_ROW.payload.headers, references: JSON.stringify([OUR_FIRST]), "message-id": MARKUS_MSG, "in-reply-to": OUR_FIRST },
      referencedMessageIds: [OUR_FIRST],
    };
    // The copy filed on Thomas's thread, and four alias copies the poll stored `unrelated`
    // (one of them an orphan candidate).
    await q(
      `INSERT INTO imap_messages_raw (id, account_email, message_id, from_address, subject, kind, instantly_campaign_id, step, payload, received_at)
       VALUES ($1, $2, $3, $4, $5, 'reply', $6, 1, $7::jsonb, now() - interval '1 hour')`,
      [crypto.randomUUID(), MAILBOX, MARKUS_MSG, MARKUS_IMAP_ROW.from_address, MARKUS_IMAP_ROW.subject, ICID, JSON.stringify(payload)],
    );
    for (const alias of ["lourd", "klourd", "kevinl"]) {
      await q(
        `INSERT INTO imap_messages_raw (id, account_email, message_id, from_address, subject, kind, payload, received_at)
         VALUES ($1, $2, $3, $4, $5, 'unrelated', $6::jsonb, now() - interval '1 hour')`,
        [crypto.randomUUID(), `${alias}-${MAILBOX}`, MARKUS_MSG, MARKUS_IMAP_ROW.from_address, MARKUS_IMAP_ROW.subject, JSON.stringify(payload)],
      );
    }
    await q(
      `INSERT INTO imap_messages_raw (id, account_email, message_id, from_address, subject, kind, payload, received_at)
       VALUES ($1, $2, $3, $4, $5, 'unrelated', $6::jsonb, now() - interval '1 hour')`,
      [crypto.randomUUID(), `x-${MAILBOX}`, MARKUS_MSG, MARKUS_IMAP_ROW.from_address, MARKUS_IMAP_ROW.subject, JSON.stringify({ ...payload, orphanCandidate: true })],
    );
    // A newsletter: never marked.
    await q(
      `INSERT INTO imap_messages_raw (id, account_email, message_id, from_address, subject, kind, payload)
       VALUES ($1, $2, $3, 'news@list.com', 'Weekly', 'unrelated', '{}'::jsonb)`,
      [crypto.randomUUID(), MAILBOX, `<news-${ICID}@list.com>`],
    );
    await drainAll();
  });

  afterAll(async () => {
    await pool?.end();
  });

  it("serves Markus's answer FROM Markus, filed on Thomas's thread, once", async () => {
    const { messages } = await readAll();
    const ours = messages.filter((m) => m.threadKey === THREAD_KEY);
    expect(ours.map((m) => m.kind).sort()).toEqual(["outreach", "reply"]);
    const markus = ours.find((m) => m.kind === "reply")!;
    expect(markus.id).toBe(imapMessageKey(MARKUS_MSG));
    expect(markus.from?.address).toBe("markus@markthalle-luzern.ch");
    expect(markus.mailbox).toBe(MAILBOX);
    expect(markus.leadEmail).toBe("thomas@markthalle-luzern.ch");
    expect(markus.orgId).toBe(ORG);
    expect(markus.participants).toContainEqual({ email: "markus@markthalle-luzern.ch", role: "from" });
    const first = ours.find((m) => m.kind === "outreach")!;
    expect(first.to.map((a) => a.address)).toEqual(["thomas@markthalle-luzern.ch"]);
    expect(first.text).toContain("Hallo Thomas");
    expect(messages.some((m) => m.from?.address === "news@list.com")).toBe(false);
  });

  it("the thread filter returns every message of the thread", async () => {
    const page = await readMailboxMessages({ after: "0", limit: 500, threadKey: THREAD_KEY });
    expect(page.messages.map((m) => m.kind).sort()).toEqual(["outreach", "reply"]);
  });

  it("an unchanged re-mark serves nothing new; a change re-serves the same id at a higher version", async () => {
    const before = await readAll();
    const old = before.messages.find((m) => m.id === imapMessageKey(MARKUS_MSG))!;
    await q(`UPDATE imap_messages_raw SET polled_at = polled_at WHERE message_id = $1`, [MARKUS_MSG]);
    await drainAll();
    const same = await readMailboxMessages({ after: before.cursor, limit: 500 });
    expect(same.messages.filter((m) => m.id === old.id)).toEqual([]);

    await q(
      `UPDATE imap_messages_raw SET payload = jsonb_set(payload, '{textSnippet}', '"Und der Preis?"') WHERE message_id = $1 AND kind = 'reply'`,
      [MARKUS_MSG],
    );
    await drainAll();
    const next = await readMailboxMessages({ after: before.cursor, limit: 500 });
    const again = next.messages.find((m) => m.id === old.id)!;
    expect(BigInt(again.version)).toBeGreaterThan(BigInt(old.version));
    expect(again.text).toBe("Und der Preis?");
  });

  it("a reply naming Markus's message resolves Thomas's sequence and its mailbox", async () => {
    const chosen = await findThreadMessage(ORG, MARKUS_MSG);
    expect(chosen?.campaign.instantlyCampaignId).toBe(ICID);
    expect(chosen?.campaign.leadEmail).toBe("thomas@markthalle-luzern.ch");
    expect(chosen?.mailbox).toBe(MAILBOX);
    expect(await findThreadMessage(crypto.randomUUID(), MARKUS_MSG)).toBeNull();
  });

  it("the reply fact names Markus as `from` (derived at read), the fact stays on Thomas", async () => {
    const row = (await q(`SELECT id FROM imap_messages_raw WHERE message_id = $1 AND kind = 'reply'`, [MARKUS_MSG])).rows[0];
    const replyId = `imap:${row.id}`;
    await q(
      `INSERT INTO replies (id, source_table, source_row_id, instantly_campaign_id, org_id, lead_email, from_email, transport, received_at)
       VALUES ($1, 'imap_messages_raw', $2, $3, $4, 'thomas@markthalle-luzern.ch', 'markus@markthalle-luzern.ch', 'smtp', now())`,
      [replyId, row.id, ICID, ORG],
    );
    await q(
      `INSERT INTO outreach_facts (subject_key, type, occurred_at, lead_email, org_id, instantly_campaign_id, payload)
       VALUES ($1, 'reply', now(), 'thomas@markthalle-luzern.ch', $2, $3, '{"fromEmail":"markus@markthalle-luzern.ch"}'::jsonb)`,
      [`reply:${replyId}`, ORG, ICID],
    );
    const page = await readOutreachFacts({ since: 0, limit: 1000, orgId: ORG });
    const fact = page.facts.find((f) => f.subjectKey === `reply:${replyId}`)!;
    expect(fact.leadEmail).toBe("thomas@markthalle-luzern.ch");
    expect(fact.participants).toEqual([
      { email: "markus@markthalle-luzern.ch", role: "from" },
      { email: "kevin@marketingagency.group", role: "to" },
    ]);
    expect(fact.threadKey).toBe(THREAD_KEY);
  });
});
