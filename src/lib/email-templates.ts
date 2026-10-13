/**
 * The transactional-email templates this service registers at startup
 * (`deployEmailTemplates` in src/index.ts, PUT /templates).
 *
 * `layout`: transactional-email wraps a template in the distribute.you brand
 * layout (logo, card, footer) unless it is registered `"none"`. A template the
 * client is meant to treat as a plain email from a person (hit Reply and write
 * to the prospect, or forward the thread as is) MUST be `"none"`: wrapped, a
 * retired answer-request reached the client as a branded newsletter (owner 2026-10-08).
 * Omitting `layout` on an existing template keeps whatever is stored, so the
 * value is always stated here.
 *
 * `stream: "transactional"`: delivered person-to-person (Postmark transactional
 * stream, no List-Unsubscribe header, no unsubscribe footer, no sign-off), for
 * a template the client answers with Reply or forwards as is. Omitted = the
 * stored value (default broadcast). Guard: tests/unit/email-templates.test.ts.
 */

import type { TemplateItem } from "./email-client";

export const EMAIL_TEMPLATES: TemplateItem[] = [
    {
      name: "campaign-error",
      layout: "none",
      subject: "[Instantly] Campaign error: {{campaignId}}",
      htmlBody: [
        "<h2>Campaign Error Detected</h2>",
        "<p><strong>Campaign ID:</strong> {{campaignId}}</p>",
        "<p><strong>Lead Email:</strong> {{leadEmail}}</p>",
        "<p><strong>Instantly Campaign ID:</strong> {{instantlyCampaignId}}</p>",
        "<p><strong>Error:</strong></p>",
        "<pre>{{errorReason}}</pre>",
      ].join("\n"),
    },
    {
      // Positive-reply celebration (lib/celebrate-positive-reply): sent to
      // the CLIENT's rep, the agency inbox in Bcc. The whole body is
      // rendered (and escaped) in code — the engine interpolates raw — so
      // the template is only the envelope.
      name: "positive-reply-celebration",
      layout: "brand",
      subject: "{{subject}}",
      htmlBody: "{{html}}",
      textBody: "{{text}}",
    },
    {
      // A reply the AI cannot answer, told to every member of the org that
      // owns the brand (lib/notify-brand-team), never to staff: the
      // prospect's words verbatim and a button into the brand's Unibox.
      // Body rendered and escaped in code; the template is the envelope.
      name: "reply-escalation",
      layout: "brand",
      subject: "{{subject}}",
      htmlBody: "{{html}}",
      textBody: "{{text}}",
    },
];
