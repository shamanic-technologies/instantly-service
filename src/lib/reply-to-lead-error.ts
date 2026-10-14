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
