/**
 * Bronze-layer writers — insert raw external payloads into the *_raw tables.
 *
 * Bronze tables are append-only mirrors of source data. Webhook handler writes
 * to instantly_webhook_payloads_raw; reconciler writes to instantly_analytics_raw,
 * instantly_emails_raw, instantly_leads_raw.
 *
 * Silver promotion is a separate concern (see silver-promote.ts).
 */
import { db } from "../db";
import {
  instantlyWebhookPayloadsRaw,
  instantlyAnalyticsRaw,
  instantlyEmailsRaw,
  instantlyLeadsRaw,
} from "../db/schema";
import type { CampaignAnalytics, EmailRecord, LeadFull } from "./instantly-client";

export interface BronzeRowRef {
  id: string;
}

export async function insertWebhookPayload(
  instantlyCampaignId: string,
  orgId: string | null,
  payload: unknown,
): Promise<BronzeRowRef> {
  const [row] = await db
    .insert(instantlyWebhookPayloadsRaw)
    .values({
      orgId,
      instantlyCampaignId,
      payload: payload as Record<string, unknown>,
    })
    .returning({ id: instantlyWebhookPayloadsRaw.id });
  return { id: row.id };
}

export async function insertAnalyticsSnapshot(
  instantlyCampaignId: string,
  orgId: string | null,
  payload: CampaignAnalytics,
): Promise<BronzeRowRef> {
  const [row] = await db
    .insert(instantlyAnalyticsRaw)
    .values({
      orgId,
      instantlyCampaignId,
      payload: payload as unknown as Record<string, unknown>,
    })
    .returning({ id: instantlyAnalyticsRaw.id });
  return { id: row.id };
}

/**
 * Insert /emails records. Idempotent: on conflict (instantly_email_id) do nothing.
 * Returns the IDs of newly inserted rows (excludes rows that already existed).
 */
export async function insertEmailsBatch(
  instantlyCampaignId: string,
  orgId: string | null,
  emails: EmailRecord[],
): Promise<BronzeRowRef[]> {
  if (emails.length === 0) return [];
  const values = emails.map((e) => ({
    orgId,
    instantlyCampaignId,
    instantlyEmailId: e.id,
    payload: e as unknown as Record<string, unknown>,
  }));
  const rows = await db
    .insert(instantlyEmailsRaw)
    .values(values)
    .onConflictDoNothing({ target: instantlyEmailsRaw.instantlyEmailId })
    .returning({ id: instantlyEmailsRaw.id });
  return rows.map((r) => ({ id: r.id }));
}

export async function insertLeadsSnapshot(
  instantlyCampaignId: string,
  orgId: string | null,
  leads: LeadFull[],
): Promise<BronzeRowRef[]> {
  if (leads.length === 0) return [];
  const values = leads.map((l) => ({
    orgId,
    instantlyCampaignId,
    leadEmail: l.email,
    payload: l as unknown as Record<string, unknown>,
  }));
  const rows = await db
    .insert(instantlyLeadsRaw)
    .values(values)
    .returning({ id: instantlyLeadsRaw.id });
  return rows.map((r) => ({ id: r.id }));
}
