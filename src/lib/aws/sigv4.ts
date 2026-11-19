/**
 * AWS Signature Version 4, by hand (SES-COLD-1119).
 *
 * The service holds no AWS SDK (disk, image size, one more vendor tree to
 * patch): the few SES v2 / SNS calls the cold-mail setup makes are signed here.
 * Pure, so the signing is pinned on AWS's own published test vector.
 */
import { createHash, createHmac } from "node:crypto";

export interface AwsCredentials {
  accessKeyId: string;
  secretAccessKey: string;
  region: string;
}

function sha256Hex(data: string | Buffer): string {
  return createHash("sha256").update(data).digest("hex");
}

function hmac(key: Buffer | string, data: string): Buffer {
  return createHmac("sha256", key).update(data, "utf8").digest();
}

/** RFC 3986 encoding, as SigV4 wants it (`~` kept, space = %20). */
export function awsUriEncode(value: string): string {
  return encodeURIComponent(value).replace(/[!'()*]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
}

export function amzDate(now: Date): string {
  return now.toISOString().replace(/[:-]|\.\d{3}/g, "");
}

export function signingKey(secretAccessKey: string, day: string, region: string, service: string): Buffer {
  return hmac(hmac(hmac(hmac(`AWS4${secretAccessKey}`, day), region), service), "aws4_request");
}

export interface SignInput {
  method: string;
  url: string;
  service: string;
  credentials: AwsCredentials;
  headers?: Record<string, string>;
  body?: string;
  now?: Date;
}

/**
 * The headers to send with the request (`Authorization`, `X-Amz-Date` and the
 * caller's own; `host` is signed and set by fetch), every header signed. Pure.
 */
export function signRequest(input: SignInput): Record<string, string> {
  const url = new URL(input.url);
  const now = input.now ?? new Date();
  const stamp = amzDate(now);
  const day = stamp.slice(0, 8);
  const body = input.body ?? "";

  const headers: Record<string, string> = { ...(input.headers ?? {}), host: url.host, "x-amz-date": stamp };
  const lower = new Map<string, string>();
  for (const [k, v] of Object.entries(headers)) lower.set(k.toLowerCase(), String(v).trim().replace(/\s+/g, " "));
  const names = [...lower.keys()].sort();
  const canonicalHeaders = names.map((n) => `${n}:${lower.get(n)}\n`).join("");
  const signedHeaders = names.join(";");

  const query = [...url.searchParams.entries()]
    .map(([k, v]) => [awsUriEncode(k), awsUriEncode(v)] as const)
    .sort(([a, av], [b, bv]) => (a === b ? (av < bv ? -1 : 1) : a < b ? -1 : 1))
    .map(([k, v]) => `${k}=${v}`)
    .join("&");
  // Non-S3 services: each segment encoded twice. The path the URL holds is
  // already encoded once, so encoding each (decoded) segment once more is it.
  const path = url.pathname === "" ? "/" : url.pathname.split("/").map((s) => awsUriEncode(s)).join("/");

  const canonical = [input.method.toUpperCase(), path, query, canonicalHeaders, signedHeaders, sha256Hex(body)].join("\n");
  const scope = `${day}/${input.credentials.region}/${input.service}/aws4_request`;
  const toSign = ["AWS4-HMAC-SHA256", stamp, scope, sha256Hex(canonical)].join("\n");
  const signature = createHmac("sha256", signingKey(input.credentials.secretAccessKey, day, input.credentials.region, input.service))
    .update(toSign, "utf8")
    .digest("hex");

  return {
    // fetch sets Host itself (to url.host, the value signed above).
    ...(input.headers ?? {}),
    "X-Amz-Date": stamp,
    Authorization: `AWS4-HMAC-SHA256 Credential=${input.credentials.accessKeyId}/${scope}, SignedHeaders=${signedHeaders}, Signature=${signature}`,
  };
}

/**
 * The SES SMTP password for an IAM secret (AWS's documented derivation:
 * version byte 0x04 + HMAC chain over "11111111"/region/"ses"/"aws4_request"/"SendRawEmail").
 */
export function sesSmtpPassword(secretAccessKey: string, region: string): string {
  const key = signingKey(secretAccessKey, "11111111", region, "ses");
  const signature = hmac(key, "SendRawEmail");
  return Buffer.concat([Buffer.from([0x04]), signature]).toString("base64");
}
