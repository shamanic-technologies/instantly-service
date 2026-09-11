/**
 * Public click-redirect endpoint for mail we dispatch ourselves.
 *
 * Unauthenticated by necessity — a prospect follows it from their inbox — so the
 * HMAC in the URL is the whole gate. The destination is INSIDE the signed
 * payload, never a query parameter, so a URL we did not mint redirects nowhere.
 * That is what keeps this from being an open redirect, which would let anyone
 * borrow the domain to bounce victims at a phishing page and get it blacklisted.
 *
 * ⚠️ THIS ROUTE NO LONGER PROMOTES ANYTHING. It records the hit in bronze and
 * redirects; a sweep decides later whether it was a person. Corporate mail
 * security fetches every URL in an inbound message before the human sees it, and
 * promoting those made a customer's website-visit stats mostly machines AND
 * paused 131 leads' sequences through `stop-on-click` after a single email. The
 * decisive signal (the same lead fetching the opt-out link seconds either side)
 * can arrive AFTER this request, so the verdict is not available here —
 * and promote-then-retract is no fix, because the pause is the harm.
 *
 * Acting on GET remains correct, unlike the opt-out: following a link IS the
 * action, and the only side effect is recording that it happened.
 */

import { Router, type Request, type Response } from "express";

import { db } from "../db";
import { trackingHitsRaw } from "../db/schema";
import { clientIpOf } from "../lib/client-ip";
import { classifyImmediateSignals } from "../lib/self-send/click-classification";
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

  const userAgent = req.get("user-agent") ?? null;

  // What THIS request gives away — a non-GET method, a non-browser user-agent.
  // Null means undecided, never human: the sweep settles it once the pairing
  // window has closed. Recording the verdict here rather than re-deriving it
  // later keeps the reason attached to the evidence that produced it.
  const immediate = classifyImmediateSignals({ method: req.method, userAgent });

  await db.insert(trackingHitsRaw).values({
    kind: "click",
    instantlyCampaignId: target.instantlyCampaignId,
    leadEmail: target.leadEmail,
    step: target.step,
    method: req.method,
    userAgent,
    clientIp: clientIpOf(req),
    classification: immediate?.verdict ?? null,
    classificationReason: immediate?.reason ?? null,
    payload: {
      url: target.url,
      userAgent,
      ip: clientIpOf(req),
      forwardedFor: req.get("x-forwarded-for") ?? null,
    },
  });

  // 302, not 301: a permanent redirect would be cached by the browser and every
  // later click on the same link would skip us entirely.
  res.redirect(302, target.url);
});

export default router;
