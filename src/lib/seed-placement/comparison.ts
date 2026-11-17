/**
 * One-off inbox-placement COMPARISON (POSTMARK-PLACEMENT-1117).
 *
 * The same email (subject + body), sent from several sending paths to the SAME
 * seed receivers the weekly test uses, read back by the SAME sync. A sending
 * path is either one of our mailboxes (SMTP, `transport='smtp'`) or an address
 * on a domain we own sent through Postmark (`transport='postmark'`).
 *
 * Rows are bronze like every seed (`seed_placement_dispatches`), tagged
 * `purpose='comparison'`: nothing the fleet decides on reads them (silver and
 * the lifecycle gate, the daily series, the alerts, the test cadence). The
 * answer is the gold read `loadPlacementComparison`, per transport x provider:
 * inbox (primary tab) / promotions (another Gmail tab, or Outlook "Other") /
 * spam / missing.
 *
 * Seeds only: receivers are OUR seed mailboxes, never a person.
 */

import { sql } from "drizzle-orm";

import { db } from "../../db";
import { seedPlacementDispatches } from "../../db/schema";
import type { CallerInfo } from "../key-client";
import type { Account } from "../instantly-client";
import { fetchTestablePoolEmails } from "../account-lifecycle-sync";
import { buildMessage } from "../self-send/message";
import { dispatchMessage, SmtpDispatchError } from "../self-send/smtp";
import { errorMessage } from "../db-error.js";
import { loadSeedCredentialResolver } from "./credentials";
import { loadAuthorizedMicrosoftReceiverEsps } from "./microsoft-oauth";
import { MAX_SEED_RECEIVERS, type SeedReceiver, selectSeedReceivers } from "./seeds";
import {
  PostmarkDispatchError,
  loadPostmarkSeedServer,
  sendThroughPostmark,
  type PostmarkSeedServer,
} from "./postmark-transport";

export const COMPARISON_TEST_ID_PREFIX = "cmp:";
export const MAX_COMPARISON_COPIES = 5;

const CALLER: CallerInfo = { method: "POST", path: "/internal/seed-placement/comparison" };

export function mintComparisonTestId(): string {
  return `${COMPARISON_TEST_ID_PREFIX}${crypto.randomUUID()}`;
}

export interface ComparisonSender {
  email: string;
  firstName?: string;
  lastName?: string;
  transport: "smtp" | "postmark";
}

export interface ComparisonSend {
  sender: ComparisonSender;
  receiver: SeedReceiver;
  copy: number;
}

/**
 * Every sender x every receiver x copies, receivers interleaved so the copies
 * of one pair never leave back to back. A sender never seeds itself. Pure.
 */
export function planComparisonSends(
  senders: readonly ComparisonSender[],
  receivers: readonly SeedReceiver[],
  copies: number,
): ComparisonSend[] {
  const out: ComparisonSend[] = [];
  for (let copy = 1; copy <= copies; copy += 1) {
    for (const receiver of receivers) {
      for (const sender of senders) {
        if (sender.email.toLowerCase() === receiver.email.toLowerCase()) continue;
        out.push({ sender, receiver, copy });
      }
    }
  }
  return out;
}

export interface ComparisonInput {
  testId: string;
  subject: string;
  bodyHtml: string;
  /** Addresses on a domain we own, sent through the cold-placement Postmark server. */
  postmarkSenders: { email: string; firstName?: string; lastName?: string }[];
  /** Our mailboxes (must hold a credential), sent over their own SMTP. */
  mailboxes: string[];
  copies: number;
}

export interface ComparisonRunSummary {
  testId: string;
  receivers: number;
  planned: number;
  sent: number;
  failed: number;
}

async function loadPersonaNames(emails: readonly string[]): Promise<Map<string, { first?: string; last?: string }>> {
  const out = new Map<string, { first?: string; last?: string }>();
  if (emails.length === 0) return out;
  const result = await db.execute(sql`
    SELECT DISTINCT ON (lower(email)) lower(email) AS email, first_name, last_name
    FROM instantly_accounts WHERE lower(email) = ANY(${sql.param(emails.map((e) => e.toLowerCase()))}::text[])
    ORDER BY lower(email), absent_since NULLS FIRST
  `);
  for (const r of ((result as { rows?: unknown[] }).rows ?? []) as { email: string; first_name: string | null; last_name: string | null }[]) {
    out.set(r.email, { first: r.first_name ?? undefined, last: r.last_name ?? undefined });
  }
  return out;
}

export async function runPlacementComparison(input: ComparisonInput): Promise<ComparisonRunSummary> {
  const summary: ComparisonRunSummary = { testId: input.testId, receivers: 0, planned: 0, sent: 0, failed: 0 };

  const resolveCredential = await loadSeedCredentialResolver(CALLER);
  const mailboxes = input.mailboxes.map((m) => m.trim().toLowerCase());
  const missing = mailboxes.filter((m) => !resolveCredential(m));
  if (missing.length > 0) throw new Error(`no credential for mailbox(es): ${missing.join(", ")}`);

  // The weekly test's receivers, exactly: Google from the credentialed pool,
  // plus every authorized Microsoft mailbox.
  const pool = await fetchTestablePoolEmails();
  const google = selectSeedReceivers(
    pool
      .map((email) => ({ email, imapHost: resolveCredential(email)?.imapHost ?? "" }))
      .filter((c) => c.imapHost !== ""),
    MAX_SEED_RECEIVERS,
  );
  const receivers: SeedReceiver[] = [
    ...google,
    ...(await loadAuthorizedMicrosoftReceiverEsps()).filter((r) => !google.some((g) => g.email === r.email)),
  ];
  if (receivers.length === 0) throw new Error("no seed receiver readable: nothing to measure");
  summary.receivers = receivers.length;

  const postmark: PostmarkSeedServer | null =
    input.postmarkSenders.length > 0 ? await loadPostmarkSeedServer(CALLER) : null;

  const personas = await loadPersonaNames(mailboxes);
  const senders: ComparisonSender[] = [
    ...input.postmarkSenders.map((s) => ({ ...s, email: s.email.trim().toLowerCase(), transport: "postmark" as const })),
    ...mailboxes.map((email) => ({
      email,
      firstName: personas.get(email)?.first,
      lastName: personas.get(email)?.last,
      transport: "smtp" as const,
    })),
  ];
  const plan = planComparisonSends(senders, receivers, input.copies);
  summary.planned = plan.length;

  for (const send of plan) {
    const account = {
      email: send.sender.email,
      first_name: send.sender.firstName,
      last_name: send.sender.lastName,
      status: 1,
      warmup_status: 1,
    } as Account;
    // The email a prospect gets today: our signature, the opt-out header prospect
    // mail carries (`mailbox_only`: mailto the sending address).
    const message = buildMessage({
      account,
      signatureHtml: null,
      leadEmail: send.receiver.email,
      subject: input.subject,
      bodyHtml: input.bodyHtml,
      step: 1,
      identity: { instantlyCampaignId: input.testId, leadEmail: send.receiver.email },
      optOut: "mailbox_only",
    });
    try {
      const result =
        send.sender.transport === "postmark"
          ? await sendThroughPostmark(postmark as PostmarkSeedServer, message)
          : await dispatchMessage(resolveCredential(send.sender.email)!, message);
      await db.insert(seedPlacementDispatches).values({
        testId: input.testId,
        senderEmail: send.sender.email,
        receiverEmail: send.receiver.email,
        recipientEsp: send.receiver.recipientEsp,
        messageId: result.messageId,
        optOutVariant: "mailbox_only",
        outcome: "sent",
        response: result.response,
        transport: send.sender.transport,
        purpose: "comparison",
      });
      summary.sent += 1;
    } catch (error) {
      summary.failed += 1;
      const kind =
        error instanceof SmtpDispatchError || error instanceof PostmarkDispatchError ? error.kind : "error";
      const response =
        error instanceof SmtpDispatchError || error instanceof PostmarkDispatchError
          ? error.response
          : errorMessage(error);
      await db.insert(seedPlacementDispatches).values({
        testId: input.testId,
        senderEmail: send.sender.email,
        receiverEmail: send.receiver.email,
        recipientEsp: send.receiver.recipientEsp,
        messageId: `failed:${crypto.randomUUID()}`,
        optOutVariant: "mailbox_only",
        outcome: kind,
        response,
        transport: send.sender.transport,
        purpose: "comparison",
      });
      console.warn(`[seed-placement] comparison ${send.sender.transport} ${send.sender.email} -> ${send.receiver.email} failed (${kind}): ${response}`);
    }
  }
  return summary;
}

// ─── Gold read ───────────────────────────────────────────────────────────────

export const PROVIDER_LABELS: Record<number, string> = {
  1: "Gmail (Google Workspace)",
  2: "Microsoft 365",
  4: "Outlook.com",
};

export interface ComparisonSeedRow {
  transport: string;
  senderEmail: string;
  recipientEsp: number;
  outcome: string;
  placement: string | null;
  gmailCategory: string | null;
  msInference: string | null;
}

export type ComparisonBucket = "inbox" | "promotions" | "spam" | "missing";

/**
 * Where one seed landed, in the owner's four words (pure).
 * Gmail: INBOX in the Primary tab = inbox, any other tab = promotions; an
 * INBOX seed whose tab was never read (before the tab read existed) = inbox.
 * Microsoft: Focused = inbox, Other = promotions. Junk/Spam = spam. Not found = missing.
 */
export function bucketOf(row: ComparisonSeedRow): ComparisonBucket {
  if (row.placement === "spam") return "spam";
  if (row.placement !== "inbox") return "missing";
  if (row.gmailCategory && row.gmailCategory !== "primary") return "promotions";
  if (row.msInference === "other") return "promotions";
  return "inbox";
}

export interface ComparisonCell {
  transport: string;
  recipientEsp: number;
  provider: string;
  sent: number;
  inbox: number;
  promotions: number;
  spam: number;
  missing: number;
  /** Seeds that never left (SMTP/Postmark refusal): outside the sample. */
  failed: number;
}

export function summarizeComparison(rows: readonly ComparisonSeedRow[]): ComparisonCell[] {
  const cells = new Map<string, ComparisonCell>();
  for (const row of rows) {
    const key = `${row.transport}|${row.recipientEsp}`;
    let cell = cells.get(key);
    if (!cell) {
      cell = {
        transport: row.transport,
        recipientEsp: row.recipientEsp,
        provider: PROVIDER_LABELS[row.recipientEsp] ?? `esp ${row.recipientEsp}`,
        sent: 0,
        inbox: 0,
        promotions: 0,
        spam: 0,
        missing: 0,
        failed: 0,
      };
      cells.set(key, cell);
    }
    if (row.outcome !== "sent") {
      cell.failed += 1;
      continue;
    }
    cell.sent += 1;
    cell[bucketOf(row)] += 1;
  }
  return [...cells.values()].sort((a, b) =>
    a.recipientEsp !== b.recipientEsp ? a.recipientEsp - b.recipientEsp : a.transport.localeCompare(b.transport),
  );
}

export interface PlacementComparison {
  testId: string;
  cells: ComparisonCell[];
  senders: (ComparisonCell & { senderEmail: string })[];
}

export async function loadPlacementComparison(testId: string): Promise<PlacementComparison | null> {
  const result = await db.execute(sql`
    SELECT d.transport, lower(d.sender_email) AS "senderEmail", d.recipient_esp::int AS "recipientEsp", d.outcome,
           o.placement, o.gmail_category AS "gmailCategory", o.ms_inference AS "msInference"
    FROM seed_placement_dispatches d
    LEFT JOIN seed_placement_observations o ON o.message_id = d.message_id AND o.receiver_email = d.receiver_email
    WHERE d.test_id = ${testId}
  `);
  const rows = ((result as { rows?: unknown[] }).rows ?? []) as ComparisonSeedRow[];
  if (rows.length === 0) return null;
  const bySender = new Map<string, ComparisonSeedRow[]>();
  for (const r of rows) bySender.set(r.senderEmail, [...(bySender.get(r.senderEmail) ?? []), r]);
  const senders = [...bySender.entries()].flatMap(([senderEmail, list]) =>
    summarizeComparison(list).map((c) => ({ ...c, senderEmail })),
  );
  return { testId, cells: summarizeComparison(rows), senders };
}
