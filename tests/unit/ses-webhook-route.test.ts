/**
 * SES-COLD-1119: `POST /webhooks/ses` reads nothing that is not signed by SNS
 * and posted on our pinned cold topic.
 */
import { generateKeyPairSync, createSign } from "node:crypto";
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import request from "supertest";
import express from "express";

const mockFile = vi.fn();
const TOPIC = "arn:aws:sns:us-east-1:168197326881:instantly-cold-email-events";

vi.mock("../../src/db", () => ({ db: {} }));
vi.mock("../../src/lib/aws/ses-account", () => ({
  loadSesAccountCached: async () => ({ topicArn: TOPIC }),
}));
vi.mock("../../src/lib/self-send/ses-events", async (orig) => {
  const actual = await orig<typeof import("../../src/lib/self-send/ses-events")>();
  return { ...actual, fileSesNotification: (...args: unknown[]) => mockFile(...args) };
});

const { default: routes } = await import("../../src/routes/ses-webhook");
const { snsStringToSign } = await import("../../src/lib/self-send/ses-events");

const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
const pem = publicKey.export({ type: "spki", format: "pem" }).toString();

function signed(m: Record<string, string>): Record<string, string> {
  const signer = createSign("RSA-SHA256");
  signer.update(snsStringToSign(m as never));
  return { ...m, Signature: signer.sign(privateKey, "base64") };
}

const notification = {
  Type: "Notification",
  MessageId: "sns-route-1",
  TopicArn: TOPIC,
  Message: JSON.stringify({ eventType: "Complaint", mail: { messageId: "x" } }),
  Timestamp: "2026-11-19T10:00:00.000Z",
  SignatureVersion: "2",
  SigningCertURL: "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-route.pem",
};

function app() {
  const a = express();
  a.use(express.json());
  a.use("/webhooks/ses", routes);
  return a;
}

describe("POST /webhooks/ses", () => {
  const realFetch = globalThis.fetch;
  beforeEach(() => {
    mockFile.mockReset().mockResolvedValue({ filedAs: "unsubscribe", duplicate: false, instantlyCampaignId: "c" });
    globalThis.fetch = vi.fn(async () => new Response(pem, { status: 200 })) as typeof fetch;
  });
  afterEach(() => {
    globalThis.fetch = realFetch;
  });

  it("files a signed notification on the pinned topic (SNS posts text/plain)", async () => {
    const res = await request(app()).post("/webhooks/ses").set("Content-Type", "text/plain; charset=UTF-8").send(JSON.stringify(signed(notification)));
    expect(res.status).toBe(200);
    expect(mockFile).toHaveBeenCalledWith("sns-route-1", { eventType: "Complaint", mail: { messageId: "x" } });
  });

  it("refuses another topic (the newsletter's included) before reading anything", async () => {
    const other = signed({ ...notification, TopicArn: "arn:aws:sns:us-east-1:168197326881:ses-newsletter-events" });
    const res = await request(app()).post("/webhooks/ses").set("Content-Type", "text/plain").send(JSON.stringify(other));
    expect(res.status).toBe(403);
    expect(mockFile).not.toHaveBeenCalled();
  });

  it("refuses a tampered message", async () => {
    const tampered = { ...signed(notification), Message: JSON.stringify({ eventType: "Complaint", mail: { messageId: "victim" } }) };
    const res = await request(app()).post("/webhooks/ses").set("Content-Type", "text/plain").send(JSON.stringify(tampered));
    expect(res.status).toBe(403);
    expect(mockFile).not.toHaveBeenCalled();
  });

  it("a filing failure answers 500 so SNS retries", async () => {
    mockFile.mockRejectedValueOnce(new Error("db down"));
    const res = await request(app()).post("/webhooks/ses").set("Content-Type", "text/plain").send(JSON.stringify(signed({ ...notification, MessageId: "sns-route-2" })));
    expect(res.status).toBe(500);
  });
});
