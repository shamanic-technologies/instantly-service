/**
 * The reasons the off-topic / referral escalation states when it hands a thread
 * to a person in lead-service. They name OUR hand-over: a later sales-interest
 * verdict on the thread withdraws exactly these (`lift-wrong-handoff.ts`) and
 * nothing else, so do not reword one without keeping the old text listed until
 * no handed row carries it.
 */
export const OFF_TOPIC_HANDOFF_REASON =
  "The reply is about something other than the offer; handed to a person, the automated responder does not answer it";
export const REFERRAL_HANDOFF_REASON =
  "The prospect referred us to someone else; handed to a person, the automated responder does not answer it";

export const ESCALATION_HANDOFF_REASONS = [OFF_TOPIC_HANDOFF_REASON, REFERRAL_HANDOFF_REASON] as const;
