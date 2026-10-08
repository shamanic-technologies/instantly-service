/**
 * The transactional-email templates this service registers at startup
 * (`deployEmailTemplates` in src/index.ts, PUT /templates).
 *
 * `layout`: transactional-email wraps a template in the distribute.you brand
 * layout (logo, card, footer) unless it is registered `"none"`. A template the
 * client is meant to treat as a plain email from a person (hit Reply and write
 * to the prospect, or forward the thread as is) MUST be `"none"`: wrapped, the
 * answer-request reached the client as a branded newsletter (owner 2026-10-08).
 * Omitting `layout` on an existing template keeps whatever is stored, so the
 * value is always stated here. Guard: tests/unit/email-templates.test.ts.
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
      // Positive-reply forward: Instantly qualified an inbound reply as
      // positive/interested → we email the full conversation thread here so
      // the agency never depends on the paid Instantly Unibox/CRM. The
      // email is a CLEAN, client-forwardable thread — subject = the
      // conversation's real subject, body = just the conversation (no
      // branding, no notes, no metadata). Rendered plain text into a
      // <pre> with an inherited font + wrapping, so it reads like a normal
      // email (not monospace) and is robust to the engine's escaping.
      name: "positive-reply-forward",
      layout: "none",
      subject: "{{subject}}",
      htmlBody:
        '<pre style="font-family:inherit;white-space:pre-wrap;word-break:break-word;margin:0">{{thread}}</pre>',
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
      // "Answer it yourself" (lib/ask-client-to-answer): a celebrated
      // positive reply on a brand with no AI responder running. Plain
      // text rendered and escaped in code; Reply-To is the prospect. Layout
      // "none": it must read as a plain email from Kevin the client answers
      // with Reply, never as a branded newsletter.
      name: "positive-reply-answer-request",
      layout: "none",
      subject: "{{subject}}",
      htmlBody: "{{html}}",
      textBody: "{{text}}",
    },
    {
      // Reply escalation with NO rep to hand to (lib/escalate-reply): the
      // responder could not answer, the brand names nobody, so the agency
      // inbox answers directly. Same subject as the thread, the history
      // quoted as a forward. No paraphrase of what they asked: their
      // reply is in the thread, verbatim. {{thread}} and {{brandName}}
      // arrive escaped.
      name: "reply-escalation",
      layout: "none",
      subject: "{{subject}}",
      htmlBody: [
        '<p style="margin:0 0 12px 0">The automated responder could not answer this reply and sent the prospect nothing. {{brandName}} has no sales rep email in Brand Settings, so this one is yours to answer. The full conversation is below.</p>',
        '<p style="margin:0 0 8px 0;color:#64748b">---------- Forwarded conversation ----------</p>',
        '<pre style="font-family:inherit;white-space:pre-wrap;word-break:break-word;margin:0">{{thread}}</pre>',
      ].join("\n"),
    },
    {
      // A reply the qualification fallback still has no kind for an hour
      // in (lib/unclassified-reply-alert): no gate opens on it, so a
      // person reads it. Sent once per reply.
      name: "reply-unclassified",
      layout: "none",
      subject: "Unclassified reply: {{leadEmail}}",
      htmlBody: [
        "<p>A reply has no verdict, so nothing acts on it: no opt-out, no forward, no follow-up. Please read it and qualify it by hand.</p>",
        "<p><strong>Why:</strong> {{why}}</p>",
        "<p><strong>Lead:</strong> {{leadEmail}}<br><strong>Instantly campaign:</strong> {{instantlyCampaignId}}<br><strong>Received:</strong> {{repliedAt}}</p>",
        '<pre style="font-family:inherit;white-space:pre-wrap;word-break:break-word;margin:0">{{body}}</pre>',
      ].join("\n"),
    },
    {
      // Off-topic / referral hand-over (lib/escalate-off-topic-reply):
      // why a person is needed leads, then the conversation verbatim.
      // `question` states the reason and quotes their words under
      // "They wrote:", never a paraphrase.
      name: "reply-handover",
      layout: "none",
      subject: "Needs you: {{leadEmail}} replied about something the responder does not handle",
      htmlBody: [
        "<p><strong>Why this needs a person:</strong></p>",
        '<pre style="font-family:inherit;white-space:pre-wrap;word-break:break-word;margin:0">{{question}}</pre>',
        "<p>&nbsp;</p>",
        '<pre style="font-family:inherit;white-space:pre-wrap;word-break:break-word;margin:0">{{thread}}</pre>',
      ].join("\n"),
    },
];
