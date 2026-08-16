/**
 * Public opt-out endpoint for mail we dispatch ourselves.
 *
 * Unauthenticated by necessity — a prospect clicks it from their inbox — so the
 * HMAC in the URL is the whole gate. It carries no service API key and reveals
 * nothing: an invalid signature and an unknown campaign are indistinguishable
 * from outside (both 404), so the route cannot be used to enumerate campaigns.
 *
 * GET does NOT unsubscribe. Corporate link scanners fetch every URL in an
 * inbound email before the human ever sees it, so acting on GET would opt out
 * prospects who never clicked — and silently kill their sequence. GET renders a
 * confirmation with a POST form; POST performs the opt-out. That also satisfies
 * RFC 8058, whose one-click `List-Unsubscribe-Post` is itself a POST.
 *
 * The opt-out promotes a real `lead_unsubscribed` silver event through the same
 * `promoteEvent` every other ingestion path uses, so the existing machinery
 * handles the consequences for free: the sequence stops, the remaining
 * provisioned holds are cancelled, and the campaign row goes terminal. Nothing
 * about unsubscribing is reimplemented here.
 */

import { Router, type Request, type Response } from "express";

import { db } from "../db";
import { trackingHitsRaw } from "../db/schema";
import { promoteEvent } from "../lib/silver-promote";
import {
  parseSignedUnsubscribe,
  unsubscribeSecret,
  type UnsubscribeIdentity,
} from "../lib/self-send/unsubscribe";

const router = Router();

/** Escape for HTML text context — the email is attacker-influenced input. */
function escapeHtml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function page(title: string, body: string): string {
  return `<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>${escapeHtml(title)}</title>
<style>
body{font:16px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
max-width:32rem;margin:6rem auto;padding:0 1.5rem;color:#111}
button{font:inherit;padding:.6rem 1.2rem;border:1px solid #111;background:#111;
color:#fff;border-radius:.375rem;cursor:pointer}
p{color:#444}
</style></head><body>${body}</body></html>`;
}

/**
 * Record the hit in bronze and hand back its row id.
 *
 * Every hit is stored, including the GET we do not act on: it is a real external
 * request, and when a scanner arrives before the human this row is the evidence
 * of it. The id is also what gives the silver event honest provenance.
 */
async function recordHit(
  identity: UnsubscribeIdentity,
  req: Request,
): Promise<string> {
  const [row] = await db
    .insert(trackingHitsRaw)
    .values({
      kind: "unsubscribe",
      instantlyCampaignId: identity.instantlyCampaignId,
      leadEmail: identity.leadEmail,
      method: req.method,
      userAgent: req.get("user-agent") ?? null,
      payload: {
        method: req.method,
        path: req.originalUrl,
        userAgent: req.get("user-agent") ?? null,
        ip: req.ip ?? null,
      },
    })
    .returning({ id: trackingHitsRaw.id });

  return row.id;
}

/**
 * Resolve the signed path segments, or null.
 *
 * The secret is read here rather than at module load so a service deployed
 * without it still boots and serves every other route; only this endpoint fails,
 * and loudly, which is the correct blast radius for a feature nobody has enabled.
 */
function resolve(req: Request): UnsubscribeIdentity | null {
  const { payload, signature } = req.params;
  if (!payload || !signature) return null;
  return parseSignedUnsubscribe(payload, signature, unsubscribeSecret());
}

router.get("/:payload/:signature", async (req: Request, res: Response) => {
  const identity = resolve(req);
  if (!identity) {
    res.status(404).type("html").send(page("Not found", "<h1>Not found</h1>"));
    return;
  }

  await recordHit(identity, req);

  res
    .status(200)
    .type("html")
    .send(
      page(
        "Unsubscribe",
        `<h1>Unsubscribe</h1>
<p>Confirm and ${escapeHtml(identity.leadEmail)} will not hear from us again.</p>
<form method="post"><button type="submit">Unsubscribe me</button></form>`,
      ),
    );
});

router.post("/:payload/:signature", async (req: Request, res: Response) => {
  const identity = resolve(req);
  if (!identity) {
    res.status(404).type("html").send(page("Not found", "<h1>Not found</h1>"));
    return;
  }

  const sourceRowId = await recordHit(identity, req);

  await promoteEvent({
    eventType: "lead_unsubscribed",
    instantlyCampaignId: identity.instantlyCampaignId,
    leadEmail: identity.leadEmail,
    accountEmail: null,
    // The opt-out is about the whole sequence, not one step of it. A step number
    // here would claim we know which email they were reading, and we do not.
    step: null,
    variant: null,
    timestamp: new Date(),
    source: "self_send",
    sourceRowId,
  });

  console.log(
    `[instantly-service] unsubscribe: campaign=${identity.instantlyCampaignId} lead=${identity.leadEmail}`,
  );

  res
    .status(200)
    .type("html")
    .send(
      page(
        "Unsubscribed",
        `<h1>Unsubscribed</h1>
<p>${escapeHtml(identity.leadEmail)} has been removed. You will not hear from us again.</p>`,
      ),
    );
});

export default router;
