/**
 * The Amazon SES account our cold mail leaves through (SES-COLD-1119, owner
 * 2026-11-19: « go with the same current SES account »).
 *
 * ⚠️ ONLY the MARKETING account (key-service platform key `aws-ses-marketing`,
 * JSON {accessKeyId, secretAccessKey, region, account}). NEVER the
 * transactional one (`aws-ses-transactional`: signup codes live there; a
 * complaint on cold mail must never touch its reputation). The transactional
 * key is read only to refuse a marketing key that is the same credential or the
 * same AWS account.
 *
 * The marketing account also carries the newsletter (postmark-service: its own
 * configuration set, SNS topic and contact list). Nothing here touches those:
 * cold mail has its OWN configuration set and topic (`SES_COLD_*` below).
 */
import { KeyServiceError, resolvePlatformKey, type CallerInfo } from "../key-client";
import { sesSmtpPassword, signRequest, type AwsCredentials } from "./sigv4";

export const SES_MARKETING_PROVIDER = "aws-ses-marketing";
export const SES_TRANSACTIONAL_PROVIDER = "aws-ses-transactional";

/** Our own configuration set: every cold identity defaults to it, every send names it. */
export const SES_COLD_CONFIGURATION_SET = "instantly-cold-email";
/** Our own SNS topic: bounces + complaints of cold mail, nothing else. */
export const SES_COLD_TOPIC_NAME = "instantly-cold-email-events";
export const SES_COLD_EVENT_DESTINATION = "instantly-cold-email-sns";
/** Where SNS posts (public, signature-verified, topic-pinned). */
export const SES_COLD_WEBHOOK_URL = "https://instantly.distribute.you/webhooks/ses";

export interface SesAccount extends AwsCredentials {
  account: string;
  smtpUser: string;
  smtpPassword: string;
  smtpHost: string;
  /** The ARN our webhook accepts, and only it. */
  topicArn: string;
}

export class SesAccountError extends Error {}

function parseKey(raw: string, provider: string): { accessKeyId: string; secretAccessKey: string; region: string; account: string } {
  let parsed: Record<string, unknown>;
  try {
    parsed = JSON.parse(raw) as Record<string, unknown>;
  } catch {
    throw new SesAccountError(`key-service "${provider}" is not JSON`);
  }
  const accessKeyId = String(parsed.accessKeyId ?? "").trim();
  const secretAccessKey = String(parsed.secretAccessKey ?? "").trim();
  const region = String(parsed.region ?? "").trim();
  const account = String(parsed.account ?? "").trim();
  if (!accessKeyId || !secretAccessKey || !region || !account) {
    throw new SesAccountError(`key-service "${provider}" lacks accessKeyId/secretAccessKey/region/account`);
  }
  return { accessKeyId, secretAccessKey, region, account };
}

/** Refuse a marketing key that IS the transactional credential or account (pure). */
export function assertNotTransactional(
  marketing: { accessKeyId: string; account: string },
  transactional: { accessKeyId: string; account: string } | null,
): void {
  if (!transactional) return;
  if (marketing.accessKeyId === transactional.accessKeyId || marketing.account === transactional.account) {
    throw new SesAccountError("the SES cold-mail key points at the TRANSACTIONAL account: refused");
  }
}

export function coldTopicArn(region: string, account: string): string {
  return `arn:aws:sns:${region}:${account}:${SES_COLD_TOPIC_NAME}`;
}

export async function loadSesAccount(caller: CallerInfo): Promise<SesAccount> {
  let raw: string;
  try {
    raw = await resolvePlatformKey(SES_MARKETING_PROVIDER, caller);
  } catch (error) {
    if (error instanceof KeyServiceError && error.statusCode === 404) {
      throw new SesAccountError(`no SES account configured (key-service platform key "${SES_MARKETING_PROVIDER}")`);
    }
    throw error;
  }
  const key = parseKey(raw, SES_MARKETING_PROVIDER);
  const transactionalRaw = await resolvePlatformKey(SES_TRANSACTIONAL_PROVIDER, caller).catch((error) => {
    if (error instanceof KeyServiceError && error.statusCode === 404) return null;
    throw error;
  });
  assertNotTransactional(key, transactionalRaw ? parseKey(transactionalRaw, SES_TRANSACTIONAL_PROVIDER) : null);
  return {
    ...key,
    smtpUser: key.accessKeyId,
    smtpPassword: sesSmtpPassword(key.secretAccessKey, key.region),
    smtpHost: `email-smtp.${key.region}.amazonaws.com`,
    topicArn: coldTopicArn(key.region, key.account),
  };
}

const CACHE_MS = 60_000;
let cached: { at: number; account: SesAccount } | null = null;

/** `loadSesAccount`, held 60 s (the send path asks per email). Fails loud. */
export async function loadSesAccountCached(caller: CallerInfo): Promise<SesAccount> {
  if (cached && Date.now() - cached.at < CACHE_MS) return cached.account;
  const account = await loadSesAccount(caller);
  cached = { at: Date.now(), account };
  return account;
}

export function clearSesAccountCache(): void {
  cached = null;
}

export interface AwsResponse {
  status: number;
  text: string;
  json: Record<string, unknown>;
}

async function signedFetch(
  account: SesAccount,
  service: string,
  method: string,
  url: string,
  body: string | undefined,
  contentType: string,
): Promise<AwsResponse> {
  const headers = signRequest({
    method,
    url,
    service,
    credentials: account,
    headers: body !== undefined ? { "content-type": contentType } : {},
    body: body ?? "",
  });
  const response = await fetch(url, { method, headers, ...(body !== undefined ? { body } : {}) });
  const text = await response.text();
  let json: Record<string, unknown> = {};
  try {
    json = text && text.trim().startsWith("{") ? (JSON.parse(text) as Record<string, unknown>) : {};
  } catch {
    json = {};
  }
  return { status: response.status, text, json };
}

/** One SES v2 REST call (`/v2/email/...`). */
export function sesApi(account: SesAccount, method: string, path: string, body?: unknown): Promise<AwsResponse> {
  return signedFetch(
    account,
    "ses",
    method,
    `https://email.${account.region}.amazonaws.com${path}`,
    body === undefined ? undefined : JSON.stringify(body),
    "application/json",
  );
}

/** One SNS query call (`Action=...`), XML answer in `text`. */
export function snsApi(account: SesAccount, params: Record<string, string>): Promise<AwsResponse> {
  const form = new URLSearchParams({ ...params, Version: "2010-03-31" }).toString();
  return signedFetch(
    account,
    "sns",
    "POST",
    `https://sns.${account.region}.amazonaws.com/`,
    form,
    "application/x-www-form-urlencoded; charset=utf-8",
  );
}

/** First `<tag>value</tag>` of an SNS XML answer, or null (pure). */
export function xmlValue(xml: string, tag: string): string | null {
  const match = new RegExp(`<${tag}>([^<]*)</${tag}>`).exec(xml);
  return match ? match[1] : null;
}
