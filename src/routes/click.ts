/**
 * Public click-redirect endpoint for mail we dispatch ourselves.
 *
 * Unauthenticated by necessity — a prospect follows it from their inbox — so the
 * HMAC in the URL is the whole gate. The destination is INSIDE the signed
 * payload, never a query parameter, so a URL we did not mint redirects nowhere.
 * That is what keeps this from being an open redirect, which would let anyone
 * borrow the domain to bounce victims at a phishing page and get it blacklisted.
 *
 * Unlike the opt-out, acting on GET is correct here: following a link IS the
 * action, and the only side effect is recording that it happened. A link scanner
 * that prefetches will inflate click counts slightly — the same tradeoff every
 * click-tracking redirect makes, and the scanner's hit is stored in bronze with
 * its user-agent so it stays visible.
 */

import { Router, type Request, type Response } from "express";

import { db } from "../db";
import { trackingHitsRaw } from "../db/schema";
import { promoteEvent } from "../lib/silver-promote";
import {
  isRedirectableUrl,
  parseSignedClick,
  selfSendLinkSecret,
} from "../lib/self-send/click-tracking";

const router = Router();

router.get("/:payload/:signature", async (req: Request, res: Response) => {
  const { payload, signature } = req.params;

  const target =
    payload && signature ? parseSignedClick(payload, signature, selfSendLinkSecret()) : null;

  // A bad MAC, a malformed payload and an unknown campaign are all the same 404,
  // so the route cannot be probed for which campaigns exist.
  if (!target || !isRedirectableUrl(target.url)) {
    res.status(404).type("html").send("<h1>Not found</h1>");
    return;
  }

  const [row] = await db
    .insert(trackingHitsRaw)
    .values({
      kind: "click",
      instantlyCampaignId: target.instantlyCampaignId,
      leadEmail: target.leadEmail,
      step: target.step,
      method: req.method,
      userAgent: req.get("user-agent") ?? null,
      payload: {
        url: target.url,
        userAgent: req.get("user-agent") ?? null,
        ip: req.ip ?? null,
      },
    })
    .returning({ id: trackingHitsRaw.id });

  // Canonical silver name. Instantly's webhook says `link_clicked` and is
  // normalized at that boundary; every reader keys on `email_link_clicked`, and
  // this is also what `stop-on-click` fires on.
  await promoteEvent({
    eventType: "email_link_clicked",
    instantlyCampaignId: target.instantlyCampaignId,
    leadEmail: target.leadEmail,
    accountEmail: null,
    step: target.step,
    variant: null,
    timestamp: new Date(),
    source: "self_send",
    sourceRowId: row.id,
  });

  // 302, not 301: a permanent redirect would be cached by the browser and every
  // later click on the same link would skip us entirely.
  res.redirect(302, target.url);
});

export default router;
