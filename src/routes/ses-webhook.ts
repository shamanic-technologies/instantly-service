/**
 * `POST /webhooks/ses` (SES-COLD-1119): SNS posts the cold-mail SES events here.
 * Public (SNS cannot carry our key), gated by the SNS SIGNATURE and the PINNED
 * topic (`instantly-cold-email-events` of the marketing account): anything else
 * is 403 and read no further. SNS sends `text/plain`, hence the text parser.
 * A filing failure answers 500 so SNS retries (filing is idempotent on its id).
 */
import express, { Router, type Request, type Response } from "express";

import { errorMessage } from "../lib/db-error.js";
import { loadSesAccountCached } from "../lib/aws/ses-account";
import {
  fileSesNotification,
  isSnsUrl,
  verifySnsSignature,
  type SesEvent,
  type SnsEnvelope,
} from "../lib/self-send/ses-events";

const router = Router();
const CALLER = { method: "POST", path: "/webhooks/ses" };

router.post("/", express.text({ type: "*/*", limit: "1mb" }), async (req: Request, res: Response) => {
  let envelope: SnsEnvelope;
  try {
    envelope = JSON.parse(typeof req.body === "string" ? req.body : JSON.stringify(req.body ?? {})) as SnsEnvelope;
  } catch {
    return res.status(400).json({ error: "not an SNS message" });
  }
  try {
    const account = await loadSesAccountCached(CALLER);
    if (envelope.TopicArn !== account.topicArn) {
      console.warn(`[instantly-service] ses webhook: refused topic ${String(envelope.TopicArn)}`);
      return res.status(403).json({ error: "topic not accepted" });
    }
    if (!(await verifySnsSignature(envelope))) {
      console.warn(`[instantly-service] ses webhook: bad SNS signature on ${String(envelope.MessageId)}`);
      return res.status(403).json({ error: "bad signature" });
    }
    if (envelope.Type === "SubscriptionConfirmation") {
      if (!isSnsUrl(envelope.SubscribeURL, "subscribe")) return res.status(403).json({ error: "bad subscribe url" });
      const confirmed = await fetch(envelope.SubscribeURL as string);
      console.log(`[instantly-service] ses webhook: subscription confirmed (${confirmed.status})`);
      return res.status(confirmed.ok ? 200 : 502).json({ confirmed: confirmed.ok });
    }
    if (envelope.Type !== "Notification") return res.json({ ignored: envelope.Type });
    const event = JSON.parse(envelope.Message) as SesEvent;
    const filed = await fileSesNotification(envelope.MessageId, event);
    return res.json(filed);
  } catch (error) {
    console.error(`[instantly-service] ses webhook ${String(envelope.MessageId)} not filed (SNS retries): ${errorMessage(error)}`);
    return res.status(500).json({ error: "not filed" });
  }
});

export default router;
