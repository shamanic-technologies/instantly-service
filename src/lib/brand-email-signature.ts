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
 *
 * SENDER PLACEHOLDERS (owner brief 2026-10-15, DefiniteJobs): the From name is
 * the sending mailbox's persona and mailboxes rotate, so a static name in the
 * client's text would contradict the From on most emails. The text may carry
 * `{first_name}`, `{last_name}`, `{full_name}`, `{sender_email}`: they stay as
 * tokens in the pinned HTML and are filled per email from the account that
 * sends it ({@link fillSenderPlaceholders}, called by `resolveSignatureBlock`).
 * A thread keeps one mailbox, so it keeps one rendered signature. Any other
 * brace is refused at write; an unfillable token throws at send: never a
 * literal brace in an email.
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
  return placeholderProblem(normalized);
}

/** The tokens a signature may carry, each filled from the sending mailbox. */
export const SIGNATURE_PLACEHOLDERS = ["first_name", "last_name", "full_name", "sender_email"] as const;
export type SignaturePlaceholder = (typeof SIGNATURE_PLACEHOLDERS)[number];

const PLACEHOLDER_RE = /\{([a-z_]+)\}/g;

/** The mailbox fields a placeholder reads (an Instantly `Account` fits). */
export interface SignatureSender {
  email: string;
  first_name?: string | null;
  last_name?: string | null;
}

/** An unknown token, or a brace that is not part of a token. Pure. */
function placeholderProblem(text: string): string | null {
  const allowed = SIGNATURE_PLACEHOLDERS.map((p) => `{${p}}`).join(", ");
  for (const m of text.matchAll(PLACEHOLDER_RE)) {
    if (!(SIGNATURE_PLACEHOLDERS as readonly string[]).includes(m[1])) {
      return `Unknown placeholder \`${m[0]}\`. Allowed: ${allowed}`;
    }
  }
  if (/[{}]/.test(text.replace(PLACEHOLDER_RE, ""))) {
    return `Braces are only allowed around a placeholder: ${allowed}`;
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

/**
 * Fill the sender placeholders of a pinned signature block with the mailbox
 * that sends THIS email (values HTML-escaped). A block without tokens is
 * returned unchanged. Throws when a token cannot be filled (a mailbox without
 * a first or last name) or a brace survives: never a literal brace in an
 * email. Pure.
 */
export function fillSenderPlaceholders(html: string, sender: SignatureSender): string {
  if (!/[{}]/.test(html)) return html;
  const first = sender.first_name?.trim() || "";
  const last = sender.last_name?.trim() || "";
  const values: Record<SignaturePlaceholder, string> = {
    first_name: first,
    last_name: last,
    full_name: [first, last].filter(Boolean).join(" "),
    sender_email: sender.email.trim(),
  };
  const filled = html.replace(PLACEHOLDER_RE, (token, name: string) => {
    if (!(SIGNATURE_PLACEHOLDERS as readonly string[]).includes(name)) {
      throw new Error(`Signature placeholder ${token} is not supported (sender ${sender.email})`);
    }
    const value = values[name as SignaturePlaceholder];
    if (!value) throw new Error(`Signature placeholder ${token} has no value for sender ${sender.email}`);
    return escapeHtml(value);
  });
  if (/[{}]/.test(filled)) {
    throw new Error(`Signature still holds a brace after filling placeholders (sender ${sender.email})`);
  }
  return filled;
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
