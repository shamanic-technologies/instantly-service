/**
 * The files a one-to-one reply carries: OFFER DOCUMENTS owned by brand-service
 * (a case study, a one-pager), referenced by id and resolved here, server to
 * server, scoped to the org the reply is sent for.
 *
 * ⚠️ REPLIES ONLY. A cold sequence step never carries a file: an attachment on
 * a first touch is a spam signal, and nothing on `POST /orgs/send` reads this.
 * The prospect who receives one already wrote back, in an engaged thread.
 *
 * ⚠️ FAIL LOUD, NEVER DROP THE FILE. A reply that names a document is a
 * DIFFERENT email than the same words without it ("see the case study
 * attached"). Every document is resolved, bytes included, BEFORE anything
 * leaves; one that is unknown, another org's, unreadable or too big refuses the
 * whole reply with a named code, and nothing is sent.
 *
 * ⚠️ THE CALLER NAMES A DOCUMENT, NEVER A FILE. Bytes, filename and type come
 * from brand-service, never from the request: a caller-supplied file would let
 * anything reach a prospect from the client's mailbox.
 */

import { errorMessage } from "./db-error.js";
import { createHash } from "node:crypto";

import { ReplyToLeadError } from "./reply-to-lead-error";

/** At most this many files on one reply. */
export const MAX_REPLY_ATTACHMENTS = 5;

/**
 * Total bytes one reply may carry. Gmail refuses a message over 25 MB once
 * base64-encoded (about 4/3 of the raw size); 10 MB raw leaves that margin with
 * room for the body, and keeps a sales reply light to open.
 */
export const MAX_REPLY_ATTACHMENT_BYTES = 10 * 1024 * 1024;

/** One file, resolved and ready to put on the wire. */
export interface ResolvedAttachment {
  documentId: string;
  filename: string;
  contentType: string;
  content: Buffer;
  sizeBytes: number;
  /** Hex md5 of the exact bytes we attached. */
  md5: string;
}

/** What we store about a file we sent: everything but the bytes. */
export interface SentAttachment {
  documentId: string;
  filename: string;
  contentType: string;
  sizeBytes: number;
  md5: string;
}

export function describeAttachment(a: ResolvedAttachment): SentAttachment {
  return {
    documentId: a.documentId,
    filename: a.filename,
    contentType: a.contentType,
    sizeBytes: a.sizeBytes,
    md5: a.md5,
  };
}

/** One stored payload's `attachments`, read leniently back into the view shape. */
export function sentAttachmentsOf(value: unknown): SentAttachment[] | undefined {
  if (!Array.isArray(value) || value.length === 0) return undefined;
  return value
    .filter((v): v is Record<string, unknown> => !!v && typeof v === "object")
    .map((v) => ({
      documentId: String(v.documentId ?? ""),
      filename: String(v.filename ?? ""),
      contentType: String(v.contentType ?? ""),
      sizeBytes: Number(v.sizeBytes ?? 0),
      md5: String(v.md5 ?? ""),
    }));
}

function brandServiceEnv(): { url: string; key: string } {
  const url = process.env.BRAND_SERVICE_URL;
  const key = process.env.BRAND_SERVICE_API_KEY;
  if (!url || !key) {
    throw new Error("BRAND_SERVICE_URL or BRAND_SERVICE_API_KEY is not set");
  }
  return { url, key };
}

/**
 * Read ONE offer document from brand-service, metadata and bytes, as the org
 * the reply is sent for. A 404/403 is "no such document in this org", whatever
 * the reason (unknown id, or another org's).
 */
async function fetchOfferDocument(
  orgId: string,
  documentId: string,
): Promise<ResolvedAttachment> {
  const { url, key } = brandServiceEnv();
  const headers = { "x-api-key": key, "x-org-id": orgId };
  const id = encodeURIComponent(documentId);

  const unavailable = (why: string): never => {
    throw new ReplyToLeadError(
      "attachment_unavailable",
      502,
      `Could not read offer document ${documentId} from brand-service: ${why}`,
    );
  };
  const notFound = (): never => {
    throw new ReplyToLeadError(
      "attachment_not_found",
      404,
      `Offer document ${documentId} does not exist in this org; nothing was sent`,
    );
  };

  let response: Response;
  try {
    response = await fetch(`${url}/internal/offer-documents/${id}/content`, { headers });
  } catch (error: unknown) {
    return unavailable(errorMessage(error));
  }
  if (response.status === 404 || response.status === 403) return notFound();
  if (!response.ok) {
    return unavailable(`${response.status} - ${(await response.text()).slice(0, 200)}`);
  }

  const content = Buffer.from(await response.arrayBuffer());
  // brand-service states the sha256 of the bytes it holds; a file that arrives
  // different (truncated, re-encoded on the way) is not the document named.
  const stated = response.headers.get("x-document-sha256");
  const actual = createHash("sha256").update(content).digest("hex");
  if (!stated || stated.toLowerCase() !== actual) {
    return unavailable(`the bytes received (sha256 ${actual}) are not the document's (${stated ?? "none stated"})`);
  }
  const contentType = response.headers.get("content-type") ?? "application/octet-stream";
  const filename = filenameFromDisposition(response.headers.get("content-disposition"));
  if (!filename) return unavailable("the response names no filename");

  return {
    documentId,
    filename,
    contentType: contentType.split(";")[0].trim(),
    content,
    sizeBytes: content.length,
    md5: createHash("md5").update(content).digest("hex"),
  };
}

/** The filename of a `Content-Disposition` header (RFC 6266, `filename*` first). */
export function filenameFromDisposition(header: string | null): string | null {
  if (!header) return null;
  const star = /filename\*\s*=\s*(?:UTF-8|utf-8)''([^;]+)/.exec(header);
  if (star) return decodeURIComponent(star[1].trim());
  const plain = /filename\s*=\s*"([^"]*)"|filename\s*=\s*([^;]+)/.exec(header);
  const name = plain ? (plain[1] ?? plain[2] ?? "").trim() : "";
  return name || null;
}

/**
 * Resolve every referenced document, in the order given. Duplicate ids are one
 * file. Throws `ReplyToLeadError` on the first one that cannot be attached.
 */
export async function resolveReplyAttachments(
  orgId: string,
  documentIds: readonly string[],
): Promise<ResolvedAttachment[]> {
  const ids = [...new Set(documentIds)];
  if (ids.length === 0) return [];
  if (ids.length > MAX_REPLY_ATTACHMENTS) {
    throw new ReplyToLeadError(
      "attachment_too_large",
      422,
      `A reply carries at most ${MAX_REPLY_ATTACHMENTS} documents, ${ids.length} were named; nothing was sent`,
    );
  }

  const resolved: ResolvedAttachment[] = [];
  for (const id of ids) resolved.push(await fetchOfferDocument(orgId, id));

  const total = resolved.reduce((sum, a) => sum + a.sizeBytes, 0);
  if (total > MAX_REPLY_ATTACHMENT_BYTES) {
    throw new ReplyToLeadError(
      "attachment_too_large",
      422,
      `The documents weigh ${total} bytes together, over the ${MAX_REPLY_ATTACHMENT_BYTES}-byte limit of one reply; nothing was sent`,
    );
  }
  return resolved;
}
