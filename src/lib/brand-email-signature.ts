/**
 * A client's own email signature, per brand.
 *
 * Owner 2026-10-14: « Default notre signature. Overwriting possible par l'API ».
 * Every email we send ends with OUR signature (`buildDefaultSignature`: the
 * sending account's name + `Distribute.you | Marketing Agency`) unless the
 * client stated its own for the brand the email is sent for
 * (`PUT /orgs/email-signature`). No row = our default, byte-identical to before.
 *
 * ⚠️ PINNED PER THREAD. The signature is resolved ONCE, at `POST /orgs/send`,
 * and stored on the thread row (`instantly_campaigns.signature_html`). Every
 * later email of that thread (follow-ups, retry-stuck, AI and human replies)
 * reads the pinned value, never the live setting: a client editing its
 * signature changes NEW threads only, so no thread ever mixes two signatures.
 *
 * Plain text in, HTML out: the client writes lines, we escape them and wrap
 * them in the same `<p>…<br>…</p>` shape as our default (Instantly's sanitizer
 * keeps only tag-wrapped content). We add the `--` separator ourselves.
 */
import { and, eq, inArray, sql } from "drizzle-orm";

import { db } from "../db";
import { brandEmailSignatures } from "../db/schema";

export const EMAIL_SIGNATURE_MAX_CHARS = 1000;
export const EMAIL_SIGNATURE_MAX_LINES = 12;

export interface BrandEmailSignature {
  brandId: string;
  signature: string;
  updatedAt: string;
}

/** Two brands of one send state different signatures: we cannot pick one. */
export class EmailSignatureConflictError extends Error {
  readonly code = "email_signature_conflict";
  constructor(readonly brandIds: string[]) {
    super(
      `The brands of this send (${brandIds.join(", ")}) state different email signatures; ` +
        "set the same signature on each, or remove all but one",
    );
  }
}

/**
 * The client's text, normalized: line endings unified, trailing spaces and
 * leading/trailing blank lines dropped. Pure.
 */
export function normalizeSignatureText(text: string): string {
  const lines = text.replace(/\r\n?/g, "\n").split("\n").map((l) => l.replace(/\s+$/, ""));
  while (lines.length > 0 && lines[0] === "") lines.shift();
  while (lines.length > 0 && lines[lines.length - 1] === "") lines.pop();
  return lines.join("\n");
}

/** Why a signature text cannot be stored, or null when it can. Pure. */
export function signatureTextProblem(text: string): string | null {
  const normalized = normalizeSignatureText(text);
  if (normalized === "") return "The signature is empty";
  if (normalized.length > EMAIL_SIGNATURE_MAX_CHARS) {
    return `The signature is longer than ${EMAIL_SIGNATURE_MAX_CHARS} characters`;
  }
  const lines = normalized.split("\n");
  if (lines.length > EMAIL_SIGNATURE_MAX_LINES) {
    return `The signature has more than ${EMAIL_SIGNATURE_MAX_LINES} lines`;
  }
  if (lines.some((l) => l.trim() === "--")) {
    return "Leave out the `--` line: it is added above the signature automatically";
  }
  return null;
}

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

/** The HTML block an email ends with, from the client's text. Pure. */
export function renderSignatureHtml(text: string): string {
  const lines = normalizeSignatureText(text).split("\n").map(escapeHtml);
  return `<p>${lines.join("<br>")}</p>`;
}

function toWire(row: typeof brandEmailSignatures.$inferSelect): BrandEmailSignature {
  return {
    brandId: row.brandId,
    signature: row.signature,
    updatedAt: new Date(row.updatedAt).toISOString(),
  };
}

export async function getBrandEmailSignature(
  orgId: string,
  brandId: string,
): Promise<BrandEmailSignature | null> {
  const [row] = await db
    .select()
    .from(brandEmailSignatures)
    .where(and(eq(brandEmailSignatures.orgId, orgId), eq(brandEmailSignatures.brandId, brandId)))
    .limit(1);
  return row ? toWire(row) : null;
}

/** Upsert. The caller has validated the text with {@link signatureTextProblem}. */
export async function setBrandEmailSignature(
  orgId: string,
  brandId: string,
  text: string,
  userId: string | null,
): Promise<BrandEmailSignature> {
  const signature = normalizeSignatureText(text);
  const [row] = await db
    .insert(brandEmailSignatures)
    .values({ orgId, brandId, signature, updatedByUserId: userId })
    .onConflictDoUpdate({
      target: [brandEmailSignatures.orgId, brandEmailSignatures.brandId],
      set: { signature, updatedByUserId: userId, updatedAt: sql`now()` },
    })
    .returning();
  return toWire(row);
}

/** Back to our default. True when a stated signature was removed. */
export async function deleteBrandEmailSignature(orgId: string, brandId: string): Promise<boolean> {
  const rows = await db
    .delete(brandEmailSignatures)
    .where(and(eq(brandEmailSignatures.orgId, orgId), eq(brandEmailSignatures.brandId, brandId)))
    .returning({ brandId: brandEmailSignatures.brandId });
  return rows.length > 0;
}

/**
 * The signature block a NEW thread is pinned with, for the brands of the send.
 * Null = no brand of the send stated one: our default applies. Brands stating
 * the same text agree; two different texts throw
 * {@link EmailSignatureConflictError} (never a silent pick). Fails loud on a
 * DB error: a thread must not be pinned to a guess.
 */
export async function resolveSignatureHtmlForSend(
  orgId: string,
  brandIds: string[],
): Promise<string | null> {
  if (brandIds.length === 0) return null;
  const rows = await db
    .select({ brandId: brandEmailSignatures.brandId, signature: brandEmailSignatures.signature })
    .from(brandEmailSignatures)
    .where(and(eq(brandEmailSignatures.orgId, orgId), inArray(brandEmailSignatures.brandId, brandIds)));
  if (rows.length === 0) return null;
  const distinct = new Set(rows.map((r) => r.signature));
  if (distinct.size > 1) throw new EmailSignatureConflictError(rows.map((r) => r.brandId).sort());
  return renderSignatureHtml(rows[0].signature);
}
