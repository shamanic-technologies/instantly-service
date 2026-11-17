/**
 * Postmark's bounce reports, filed like the DSNs our own mailboxes receive
 * (POSTMARK-COLD-1117).
 *
 * Through Postmark a DSN goes to Postmark's Return-Path, never to our mailbox,
 * so the poll would never see it. This reads the cold server's bounces (last 3
 * days, every pass, idempotent on Postmark's bounce id) and files each one on
 * the send it answers (`smtp_dispatch_raw.response = 'postmark:<id>'`):
 *   - a permanent kind = an `imap_messages_raw` row `bounce` with the report's
 *     words, `payload.recoveredFrom = 'postmark_bounce'`; the DSN judgment sweep
 *     then decides address dead vs message refused, exactly as for any DSN
 *     (never a code verdict here);
 *   - a soft / transient kind = a `delay` row (promotes nothing);
 *   - a spam complaint or list unsubscribe = a real `lead_unsubscribed`;
 *   - anything else (auto-responder, subscribe…) = recorded, nothing done.
 * Every report is kept in `postmark_bounces_raw` with what we made of it.
 */

import { sql } from "drizzle-orm";

import { db } from "../../db";
import { errorMessage } from "../db-error.js";
import { promoteEvent } from "../silver-promote";
import type { CallerInfo } from "../key-client";
import { loadPostmarkSeedServerCached } from "../seed-placement/postmark-transport";

const CALLER: CallerInfo = { method: "POST", path: "/internal/postmark-sending/bounces" };
export const POSTMARK_BOUNCE_WINDOW_DAYS = 3;

export type PostmarkBounceFiling = "bounce" | "delay" | "unsubscribe" | "ignored";

const PERMANENT = new Set([
  "HardBounce", "BadEmailAddress", "Blocked", "DnsError", "SpamNotification", "DMARCPolicy",
  "VirusNotification", "ManuallyDeactivated", "Unknown", "AddressChange", "ChallengeVerification",
]);
const TEMPORARY = new Set(["SoftBounce", "Transient"]);
const OPT_OUT = new Set(["SpamComplaint", "Unsubscribe"]);

/** What a Postmark bounce type becomes (pure). */
export function filingForBounceType(type: string): PostmarkBounceFiling {
  if (PERMANENT.has(type)) return "bounce";
  if (TEMPORARY.has(type)) return "delay";
  if (OPT_OUT.has(type)) return "unsubscribe";
  return "ignored";
}

interface PostmarkBounce {
  ID: number;
  Type: string;
  TypeCode?: number;
  MessageID: string;
  Email?: string;
  From?: string;
  BouncedAt?: string;
  Details?: string;
  Description?: string;
  Subject?: string;
}

function rowsOf<T>(result: unknown): T[] {
  return (((result as { rows?: unknown[] }).rows ?? []) as T[]);
}

async function postmarkServer(token: string, path: string): Promise<Record<string, unknown>> {
  const response = await fetch(`https://api.postmarkapp.com${path}`, {
    headers: { Accept: "application/json", "X-Postmark-Server-Token": token },
  });
  const body = (await response.json()) as Record<string, unknown>;
  if (!response.ok) throw new Error(`Postmark GET ${path} ${response.status}: ${JSON.stringify(body)}`);
  return body;
}

export interface PostmarkBounceSummary {
  read: number;
  alreadyFiled: number;
  filed: Record<PostmarkBounceFiling, number>;
  noSendFound: number;
  failed: number;
}

export async function pollPostmarkBounces(asOf: Date = new Date()): Promise<PostmarkBounceSummary> {
  const summary: PostmarkBounceSummary = {
    read: 0,
    alreadyFiled: 0,
    filed: { bounce: 0, delay: 0, unsubscribe: 0, ignored: 0 },
    noSendFound: 0,
    failed: 0,
  };
  const server = await loadPostmarkSeedServerCached(CALLER);
  const from = new Date(asOf.getTime() - POSTMARK_BOUNCE_WINDOW_DAYS * 86_400_000).toISOString().slice(0, 10);

  const bounces: PostmarkBounce[] = [];
  for (let offset = 0; offset < 10_000; offset += 500) {
    const page = await postmarkServer(server.token, `/bounces?count=500&offset=${offset}&fromdate=${from}`);
    const list = (page.Bounces ?? []) as PostmarkBounce[];
    bounces.push(...list);
    if (list.length < 500) break;
  }
  summary.read = bounces.length;
  if (bounces.length === 0) return summary;

  const known = new Set(
    rowsOf<{ id: string }>(
      await db.execute(sql`SELECT bounce_id::text AS id FROM postmark_bounces_raw WHERE bounce_id = ANY(${sql.param(bounces.map((b) => b.ID))}::bigint[])`),
    ).map((r) => r.id),
  );

  for (const bounce of bounces) {
    if (known.has(String(bounce.ID))) {
      summary.alreadyFiled += 1;
      continue;
    }
    try {
      const [send] = rowsOf<{ id: string; campaign: string; step: number; lead: string; account: string }>(
        await db.execute(sql`
          SELECT id, instantly_campaign_id AS campaign, step, lead_email AS lead, account_email AS account
          FROM smtp_dispatch_raw
          WHERE response LIKE 'postmark:%' AND response = ${`postmark:${bounce.MessageID}`}
          LIMIT 1
        `),
      );
      let filing = filingForBounceType(bounce.Type);
      if (!send) {
        // A seed or comparison seed (seed_placement_dispatches), or a send we
        // never recorded: nothing about a lead to file.
        summary.noSendFound += 1;
        filing = "ignored";
      } else if (filing === "bounce" || filing === "delay") {
        let dump = "";
        try {
          const d = await postmarkServer(server.token, `/bounces/${bounce.ID}/dump`);
          dump = typeof d.Body === "string" ? d.Body : "";
        } catch (error) {
          console.warn(`[instantly-service] postmark bounce ${bounce.ID} dump unread: ${errorMessage(error)}`);
        }
        const words = [bounce.Description, bounce.Details, dump].filter(Boolean).join("\n\n").slice(0, 4000);
        await db.execute(sql`
          INSERT INTO imap_messages_raw
            (id, account_email, message_id, from_address, subject, kind, instantly_campaign_id, step, payload, received_at)
          VALUES (${crypto.randomUUID()}, ${send.account}, ${`<postmark-bounce-${bounce.ID}@postmarkapp.com>`},
                  ${"mailer-daemon@postmarkapp.com"}, ${bounce.Subject ?? null}, ${filing}, ${send.campaign}, ${send.step},
                  ${JSON.stringify({ textSnippet: words, recoveredFrom: "postmark_bounce", postmarkBounce: { id: bounce.ID, type: bounce.Type, typeCode: bounce.TypeCode ?? null } })}::jsonb,
                  ${bounce.BouncedAt ? new Date(bounce.BouncedAt) : asOf})
          ON CONFLICT (account_email, message_id) DO NOTHING
        `);
      } else if (filing === "unsubscribe") {
        await promoteEvent({
          eventType: "lead_unsubscribed",
          instantlyCampaignId: send.campaign,
          leadEmail: send.lead,
          accountEmail: send.account,
          step: null,
          variant: null,
          timestamp: bounce.BouncedAt ? new Date(bounce.BouncedAt) : asOf,
          source: "self_send",
          sourceRowId: send.id,
        });
      }
      await db.execute(sql`
        INSERT INTO postmark_bounces_raw
          (bounce_id, postmark_message_id, type, email, bounced_at, payload, filed_as, instantly_campaign_id, step)
        VALUES (${bounce.ID}, ${bounce.MessageID}, ${bounce.Type}, ${bounce.Email ?? null},
                ${bounce.BouncedAt ? new Date(bounce.BouncedAt) : null}, ${JSON.stringify(bounce)}::jsonb, ${filing},
                ${send?.campaign ?? null}, ${send?.step ?? null})
        ON CONFLICT (bounce_id) DO NOTHING
      `);
      summary.filed[filing] += 1;
    } catch (error) {
      summary.failed += 1;
      console.error(`[instantly-service] postmark bounce ${bounce.ID} not filed (retried next pass): ${errorMessage(error)}`);
    }
  }
  return summary;
}
