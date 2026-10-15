/** A refusal a caller can act on, rather than a bare 500. */
export class ReplyToLeadError extends Error {
  constructor(
    public readonly code:
      | "campaign_not_found"
      | "ambiguous_thread"
      | "no_reply_to_thread"
      | "sending_account_unresolved"
      | "mailbox_credential_unavailable"
      | "human_took_over"
      | "prospect_wrote_elsewhere"
      | "handed_over"
      | "reply_dispatch_failed"
      | "attachment_not_found"
      | "attachment_too_large"
      | "attachment_unavailable"
      | "attachments_unsupported_on_transport",
    public readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "ReplyToLeadError";
  }
}

/**
 * `409 human_took_over`, carrying WHICH answer refused it, so the route can count
 * a hand-typed answer as one of our touches (lib/count-human-answer) instead of
 * handing the thread over for good (owner 2026-10-15, REPLY-ANY-LEAD-1015).
 */
export class HumanTookOverError extends ReplyToLeadError {
  constructor(
    message: string,
    public readonly answer: {
      /** The sequence the answer sits on. */
      instantlyCampaignId: string;
      /** When the person sent it, ISO 8601 UTC. */
      at: string;
      /** True when the conversation lives in that person's own mailbox (never counted). */
      heldInOwnMailbox: boolean;
    },
  ) {
    super("human_took_over", 409, message);
    this.name = "HumanTookOverError";
  }
}
